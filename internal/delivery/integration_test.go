//go:build integration

// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package delivery

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/oracle"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/sorrell-consulting/amends/internal/worker"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type environment struct {
	Options
	pool *pgxpool.Pool
	cl   *kgo.Client
}

func setup(t *testing.T, partitions int) *environment {
	t.Helper()
	url, seeds := os.Getenv("AMENDS_TEST_DATABASE_URL"), os.Getenv("AMENDS_TEST_BROKERS")
	if url == "" || seeds == "" {
		t.Fatal("requires PostgreSQL and Redpanda; use make test-delivery")
	}
	e := &environment{Options: Options{Brokers: strings.Split(seeds, ","), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}}
	var err error
	e.pool, err = pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.pool.Close)
	if err = postgres.Migrate(t.Context(), e.pool); err != nil {
		t.Fatal(err)
	}
	e.cl, err = broker.Client(e.Brokers, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.cl.Close)
	var nonce [8]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	name := "amends-delivery-" + hex.EncodeToString(nonce[:])
	var topics []string
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		req := kmsg.NewDeleteTopicsRequest()
		for _, name := range topics {
			topic := kmsg.NewDeleteTopicsRequestTopic()
			topic.Topic = kmsg.StringPtr(name)
			req.Topics = append(req.Topics, topic)
		}
		if len(topics) > 0 {
			response, err := req.RequestWith(ctx, e.cl)
			if err != nil {
				t.Error(err)
				return
			}
			for _, topic := range response.Topics {
				if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
					t.Error(err)
				}
			}
		}
	})
	source, err := broker.CreateSource(t.Context(), e.cl, name+"-source", partitions)
	if err != nil {
		t.Fatal(err)
	}
	topics = append(topics, source.Topic)
	output, err := broker.CreateSource(t.Context(), e.cl, name+"-output", partitions)
	if err != nil {
		t.Fatal(err)
	}
	topics = append(topics, output.Topic)
	c, err := fixtures.Config(name)
	if err != nil {
		t.Fatal(err)
	}
	c.Source, c.Partitions = source.ID, partitions
	for p := 1; p < partitions; p++ {
		k := c.Keys[0]
		k.Item += fmt.Sprint(p)
		k.Partition = p
		c.Keys = append(c.Keys, k)
	}
	e.Manifest = broker.Manifest{Topic: source.Topic, Config: c, Output: &store.OutputBinding{Topic: output.Topic, Incarnation: output.ID, Partitions: partitions}}
	e.Store, err = postgres.New(e.pool, c)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Store.Ensure(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		tx, err := e.pool.Begin(ctx)
		if err != nil {
			t.Error(err)
			return
		}
		defer tx.Rollback(ctx)
		ns, _ := json.Marshal(c.Namespace)
		for _, table := range []string{"amends_view_rows", "amends_view_progress", "amends_quarantine", "amends_outbox", "amends_output_state", "amends_key_state", "amends_partitions", "amends_namespaces"} {
			if _, err = tx.Exec(ctx, "DELETE FROM "+table+" WHERE namespace_id=$1", string(ns)); err != nil {
				t.Error(err)
				return
			}
		}
		if err = tx.Commit(ctx); err != nil {
			t.Error(err)
		}
	})
	if err = e.Store.BindOutput(t.Context(), *e.Manifest.Output); err != nil {
		t.Fatal(err)
	}
	return e
}

func eventually(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.NewTimer(25 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if f() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("condition did not become true")
		case <-tick.C:
		}
	}
}

type running struct {
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
}

func start(t *testing.T, run func(context.Context) error) *running {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	r := &running{cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- run(ctx) }()
	t.Cleanup(func() { r.stop(t) })
	return r
}
func (r *running) stop(t *testing.T) {
	t.Helper()
	r.once.Do(func() {
		r.cancel()
		select {
		case err := <-r.done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("service exit: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("service shutdown timed out")
		}
	})
}
func (e *environment) worker(ctx context.Context) error {
	return worker.Run(ctx, worker.Options{Brokers: e.Brokers, Topic: e.Manifest.Topic, Config: e.Manifest.Config, State: e.Store, Logger: e.Logger})
}

func (e *environment) publish(t *testing.T, name string) []ledger.Record {
	t.Helper()
	c := e.Manifest.Config
	var history []ledger.Record
	for p := 0; p < c.Partitions; p++ {
		records, err := fixtures.Records(c, name)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range records {
			transaction, err := ledger.Decode(r.Value)
			if err != nil {
				t.Fatal(err)
			}
			transaction.StockKey = c.Keys[p].StockKey
			raw, err := json.Marshal(transaction)
			if err != nil {
				t.Fatal(err)
			}
			kr := &kgo.Record{Topic: e.Manifest.Topic, Partition: int32(p), Key: broker.EncodeKey(transaction.StockKey), Value: raw}
			if err = e.cl.ProduceSync(t.Context(), kr).FirstErr(); err != nil {
				t.Fatal(err)
			}
			history = append(history, broker.DecodeRecord(c.Source, kr))
		}
	}
	return history
}
func (e *environment) awaitView(t *testing.T, next int64) {
	t.Helper()
	eventually(t, func() bool {
		for p := 0; p < e.Manifest.Config.Partitions; p++ {
			position, err := e.Store.ViewPosition(t.Context(), p)
			if err != nil {
				t.Fatal(err)
			}
			if position.Error != "" {
				t.Fatal(position.Error)
			}
			if position.Next != next {
				return false
			}
		}
		return true
	})
}
func (e *environment) check(t *testing.T, history []ledger.Record, expected string) {
	t.Helper()
	v, err := e.Store.View(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	projection, err := fixtures.Expected(expected)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range e.Manifest.Config.Keys {
		want := projection.Clone()
		want.Key = key.StockKey
		if got := v.Projection(key.StockKey); !reflect.DeepEqual(got, want) {
			t.Fatalf("hand fixture mismatch: %+v want %+v", got, want)
		}
	}
	full, err := oracle.Compute(e.Manifest.Config, history)
	if err != nil {
		t.Fatal(err)
	}
	if len(full.Rejections) != 0 || len(full.Conflicts) != 0 {
		t.Fatal("unexpected source qualifications")
	}
	for _, want := range full.Keys {
		if got := v.Projection(want.Key); !reflect.DeepEqual(got, want) {
			t.Fatalf("source oracle mismatch: %+v want %+v", got, want)
		}
	}
}

func TestPipelineFixtureStagesAndDeletionAfterRestart(t *testing.T) {
	e := setup(t, 2)
	start(t, e.worker)
	start(t, e.worker)
	relay := start(t, func(ctx context.Context) error { return RunRelay(ctx, e.Options) })
	view := start(t, func(ctx context.Context) error { return RunView(ctx, e.Options) })
	var history []ledger.Record
	for _, stage := range []struct {
		input, expected string
		next            int64
	}{{"before.jsonl", "expected-before.json", 5}, {"correction.jsonl", "expected-after.json", 10}, {"cancel-last-day.jsonl", "expected-after-cancel.json", 11}} {
		history = append(history, e.publish(t, stage.input)...)
		e.awaitView(t, stage.next)
		e.check(t, history, stage.expected)
	}
	eventually(t, func() bool {
		for p := 0; p < 2; p++ {
			_, pending, err := e.Store.Pending(t.Context(), p)
			if err != nil {
				t.Fatal(err)
			}
			if pending {
				return false
			}
		}
		return true
	})
	relay.stop(t)
	view.stop(t)
	snapshot, err := e.Store.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for p := 0; p < 2; p++ {
		for _, row := range snapshot.Outbox {
			if row.Partition == p && row.Envelope.ID.Family == "balance" && row.Envelope.ID.Day == "2026-01-03" && row.Envelope.Version == 1 {
				raw, err := json.Marshal(row.Envelope)
				if err != nil {
					t.Fatal(err)
				}
				if err = e.cl.ProduceSync(t.Context(), &kgo.Record{Topic: e.Manifest.Output.Topic, Partition: int32(p), Key: broker.EncodeKey(row.Envelope.ID.Key), Value: raw}).FirstErr(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	start(t, func(ctx context.Context) error { return RunView(ctx, e.Options) })
	e.awaitView(t, 12)
	e.check(t, history, "expected-after-cancel.json")
	v, err := e.Store.View(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range e.Manifest.Config.Keys {
		id := ledger.OutputID{Namespace: e.Manifest.Config.Namespace, Family: "balance", Key: key.StockKey, Day: "2026-01-03"}
		row := v.Rows[id]
		if row.Version != 3 || row.Payload.Present {
			t.Fatalf("late output resurrected withdrawn balance: %+v", row)
		}
	}
}

type publishLoss struct {
	e       *environment
	lose    bool
	offsets []int64
}

func (p *publishLoss) Publish(ctx context.Context, d store.Delivery) (int64, error) {
	r := &kgo.Record{Topic: p.e.Manifest.Output.Topic, Partition: int32(d.Partition), Key: broker.EncodeKey(d.Envelope.ID.Key), Value: d.Bytes}
	if err := p.e.cl.ProduceSync(ctx, r).FirstErr(); err != nil {
		return 0, err
	}
	p.offsets = append(p.offsets, r.Offset)
	if p.lose {
		p.lose = false
		return 0, errors.New("controlled lost publication observation")
	}
	return r.Offset, nil
}
func (e *environment) seedDecision(t *testing.T) store.Delivery {
	t.Helper()
	history := e.publish(t, "before.jsonl")
	token, err := e.Store.Claim(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Store.Process(t.Context(), token, 0, history[0]); err != nil {
		t.Fatal(err)
	}
	d, ok, err := e.Store.Pending(t.Context(), 0)
	if err != nil || !ok {
		t.Fatal(err)
	}
	return d
}
func (e *environment) readOutputs(t *testing.T, n int) []*kgo.Record {
	t.Helper()
	cl, err := broker.Client(e.Brokers, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{e.Manifest.Output.Topic: {0: kgo.NewOffset().At(0)}}), kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var records []*kgo.Record
	for len(records) < n {
		fetches := cl.PollRecords(ctx, n-len(records))
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatal(errs)
		}
		records = append(records, fetches.Records()...)
	}
	return records
}
func TestRelayLostPublishObservationRetainsExactEnvelope(t *testing.T) {
	e := setup(t, 1)
	d := e.seedDecision(t)
	publisher := &publishLoss{e: e, lose: true}
	if worked, err := RelayStep(t.Context(), e.Store, publisher, 0); err != nil || !worked {
		t.Fatalf("relay: %v", err)
	}
	if !reflect.DeepEqual(publisher.offsets, []int64{0, 1}) {
		t.Fatalf("not redelivered: %v", publisher.offsets)
	}
	for _, r := range e.readOutputs(t, 2) {
		if !bytes.Equal(r.Value, d.Bytes) || !bytes.Equal(r.Key, broker.EncodeKey(d.Envelope.ID.Key)) {
			t.Fatal("retry changed committed envelope or key")
		}
	}
	receipt, err := e.Store.Receipt(t.Context(), 0, d.Sequence)
	if err != nil || receipt == nil || *receipt != 1 {
		t.Fatalf("receipt=%v error=%v", receipt, err)
	}
	start(t, func(ctx context.Context) error { return RunView(ctx, e.Options) })
	e.awaitView(t, 2)
	v, err := e.Store.View(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Rows) != 1 || !v.Rows[d.Envelope.ID].Equal(d.Envelope) {
		t.Fatal("duplicate publication changed materialization")
	}
}

type unknownState struct {
	*postgres.Store
	committed         bool
	ackLost, viewLost bool
	reads             int
}

func (s *unknownState) Acknowledge(ctx context.Context, p int, seq, offset int64) error {
	if !s.ackLost {
		s.ackLost = true
		if s.committed {
			if err := s.Store.Acknowledge(ctx, p, seq, offset); err != nil {
				return err
			}
		}
		return store.ErrCommitUnknown
	}
	return s.Store.Acknowledge(ctx, p, seq, offset)
}
func (s *unknownState) Receipt(ctx context.Context, p int, seq int64) (*int64, error) {
	s.reads++
	return s.Store.Receipt(ctx, p, seq)
}
func (s *unknownState) ApplyViewInput(ctx context.Context, next int64, input store.ViewInput) error {
	if !s.viewLost {
		s.viewLost = true
		if s.committed {
			if err := s.Store.ApplyViewInput(ctx, next, input); err != nil {
				return err
			}
		}
		return store.ErrCommitUnknown
	}
	return s.Store.ApplyViewInput(ctx, next, input)
}
func (s *unknownState) ViewPosition(ctx context.Context, p int) (store.ViewPosition, error) {
	s.reads++
	return s.Store.ViewPosition(ctx, p)
}
func TestDeliveryUnknownDatabaseOutcomes(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprint(committed), func(t *testing.T) {
			e := setup(t, 1)
			d := e.seedDecision(t)
			state := &unknownState{Store: e.Store, committed: committed}
			publisher := &publishLoss{e: e}
			if worked, err := RelayStep(t.Context(), state, publisher, 0); err != nil || !worked {
				t.Fatalf("relay %v", err)
			}
			if len(publisher.offsets) != 1 {
				t.Fatal("SQL acknowledgement loss caused unnecessary publication")
			}
			input := store.ViewInput{Partition: 0, Offset: 0, Envelope: d.Envelope, Key: broker.EncodeKey(d.Envelope.ID.Key), Value: d.Bytes}
			if err := ViewStep(t.Context(), state, 0, input); err != nil {
				t.Fatal(err)
			}
			if state.reads != 2 {
				t.Fatalf("unknown outcomes not reobserved: %d", state.reads)
			}
			position, err := e.Store.ViewPosition(t.Context(), 0)
			if err != nil || position.Next != 1 {
				t.Fatalf("view position %+v %v", position, err)
			}
		})
	}
}
