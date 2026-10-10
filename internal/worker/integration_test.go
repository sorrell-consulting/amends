//go:build integration

// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package worker

import (
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
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/oracle"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type environment struct {
	c       ledger.Config
	topic   string
	brokers []string
	pool    *pgxpool.Pool
	db      *postgres.Store
	cl      *kgo.Client
}

func setup(t *testing.T, partitions int) *environment {
	t.Helper()
	url, brokers := os.Getenv("AMENDS_TEST_DATABASE_URL"), os.Getenv("AMENDS_TEST_BROKERS")
	if url == "" || brokers == "" {
		t.Fatal("real lifecycle tests require AMENDS_TEST_DATABASE_URL and AMENDS_TEST_BROKERS; use make test-worker")
	}
	e := &environment{brokers: strings.Split(brokers, ",")}
	var err error
	e.pool, err = pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.pool.Close)
	if err = postgres.Migrate(t.Context(), e.pool); err != nil {
		t.Fatal(err)
	}
	e.cl, err = broker.Client(e.brokers, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.cl.Close)
	var nonce [8]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	e.topic = "amends-test-" + hex.EncodeToString(nonce[:])
	source, err := broker.CreateSource(t.Context(), e.cl, e.topic, partitions)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("source=%s incarnation=%s partitions=%d", source.Topic, source.ID, source.Partitions)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		req := kmsg.NewDeleteTopicsRequest()
		x := kmsg.NewDeleteTopicsRequestTopic()
		x.Topic = kmsg.StringPtr(e.topic)
		req.Topics = append(req.Topics, x)
		r, err := req.RequestWith(ctx, e.cl)
		if err != nil {
			t.Error(err)
			return
		}
		for _, topic := range r.Topics {
			if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
				t.Error(err)
			}
		}
	})
	e.c, err = fixtures.Config(e.topic)
	if err != nil {
		t.Fatal(err)
	}
	e.c.Source, e.c.Partitions = source.ID, partitions
	for p := 1; p < partitions; p++ {
		k := e.c.Keys[0]
		k.Item += fmt.Sprint(p)
		k.Partition = p
		e.c.Keys = append(e.c.Keys, k)
	}
	e.db, err = postgres.New(e.pool, e.c)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.db.Ensure(t.Context()); err != nil {
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
		ns, _ := json.Marshal(e.c.Namespace)
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
	return e
}

func (e *environment) records(t *testing.T, p int) []ledger.Record {
	t.Helper()
	records, err := fixtures.Records(e.c, "before.jsonl", "correction.jsonl", "cancel-last-day.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	for i := range records {
		x, err := ledger.Decode(records[i].Value)
		if err != nil {
			t.Fatal(err)
		}
		x.StockKey = e.c.Keys[p].StockKey
		records[i].Key, records[i].Partition = x.StockKey, p
		records[i].Value, err = json.Marshal(x)
		if err != nil {
			t.Fatal(err)
		}
	}
	return records
}

func (e *environment) publish(t *testing.T, records ...ledger.Record) []ledger.Record {
	t.Helper()
	var out []ledger.Record
	for _, r := range records {
		key := r.RawKey
		if key == nil {
			key = broker.EncodeKey(r.Key)
		}
		kr := &kgo.Record{Topic: e.topic, Partition: int32(r.Partition), Key: key, Value: r.Value}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		err := e.cl.ProduceSync(ctx, kr).FirstErr()
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, broker.DecodeRecord(e.c.Source, kr))
	}
	return out
}

type running struct {
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
}

func (e *environment) start(t *testing.T, state State) *running {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	r := &running{cancel: cancel, done: make(chan error, 1)}
	go func() {
		r.done <- Run(ctx, Options{Brokers: e.brokers, Topic: e.topic, Config: e.c, State: state, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	}()
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
				t.Errorf("worker stopped: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("worker shutdown did not join callback work")
		}
	})
}
func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(25 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("condition did not become true")
		case <-tick.C:
		}
	}
}
func (e *environment) await(t *testing.T, next int64) {
	t.Helper()
	eventually(t, func() bool {
		for p := 0; p < e.c.Partitions; p++ {
			_, s, err := e.db.Observe(t.Context(), p)
			if err != nil {
				t.Fatal(err)
			}
			if s.Next != next {
				return false
			}
		}
		return true
	})
}

type tracked struct {
	*postgres.Store
	mu        sync.Mutex
	claims    map[int]store.Token
	processed []int64
}

func track(s *postgres.Store) *tracked { return &tracked{Store: s, claims: map[int]store.Token{}} }
func (s *tracked) Claim(ctx context.Context, p int) (store.Token, error) {
	t, err := s.Store.Claim(ctx, p)
	if err == nil {
		s.mu.Lock()
		s.claims[p] = t
		s.mu.Unlock()
	}
	return t, err
}
func (s *tracked) Process(ctx context.Context, token store.Token, next int64, r ledger.Record) error {
	err := s.Store.Process(ctx, token, next, r)
	if err == nil {
		s.mu.Lock()
		s.processed = append(s.processed, r.Offset)
		s.mu.Unlock()
	}
	return err
}
func (s *tracked) latest(p int) store.Token { s.mu.Lock(); defer s.mu.Unlock(); return s.claims[p] }

func TestWorkerTwoMembersCorrectionAndReplacement(t *testing.T) {
	e := setup(t, 2)
	a, b := e.records(t, 0), e.records(t, 1)
	history := e.publish(t, slices.Concat(a[:3], b[:3])...)
	s1, s2 := track(e.db), track(e.db)
	w1 := e.start(t, s1)
	e.await(t, 3)
	old := []store.Token{s1.latest(0), s1.latest(1)}
	w2 := e.start(t, s2)
	eventually(t, func() bool {
		claimed := s2.latest(0).Epoch > 0 || s2.latest(1).Epoch > 0
		for p := 0; p < 2; p++ {
			token, _, err := e.db.Observe(t.Context(), p)
			if err != nil {
				t.Fatal(err)
			}
			claimed = claimed && token.Epoch > old[p].Epoch
		}
		return claimed
	})
	history = append(history, e.publish(t, slices.Concat(a[3:], b[3:])...)...)
	e.await(t, 5)
	for p := 0; p < 2; p++ {
		r := a[3]
		r.Partition = p
		if err := e.db.Process(t.Context(), old[p], 3, r); !errors.Is(err, store.ErrFence) {
			t.Fatalf("old owner accepted: %v", err)
		}
	}
	checkBusiness(t, e, history, 22)
	before0, before1 := s2.latest(0), s2.latest(1)
	w1.stop(t)
	eventually(t, func() bool { return s2.latest(0).Epoch > before0.Epoch && s2.latest(1).Epoch > before1.Epoch })
	history = append(history, e.publish(t, a[3], a[0], b[3], b[0])...)
	e.await(t, 7)
	checkBusiness(t, e, history, 22)
	w2.stop(t)
	s3 := track(e.db)
	w3 := e.start(t, s3)
	eventually(t, func() bool { return s3.latest(0).Epoch > 0 && s3.latest(1).Epoch > 0 })
	e.await(t, 7)
	w3.stop(t)
}

func checkBusiness(t *testing.T, e *environment, history []ledger.Record, outbox int) {
	t.Helper()
	snap, err := e.db.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Outbox) != outbox {
		t.Fatalf("outbox=%d want %d", len(snap.Outbox), outbox)
	}
	for _, key := range e.c.Keys {
		got := snap.Keys[key.StockKey].Projection
		if !reflect.DeepEqual(got.Balances, []ledger.Balance{{Day: "2026-01-01", Close: 70}, {Day: "2026-01-02", Close: 50}}) || !reflect.DeepEqual(got.Alerts, []ledger.Alert{{Day: "2026-01-02", Threshold: 60, Close: 50}}) {
			t.Fatalf("hand-derived correction/cancellation mismatch: %+v", got)
		}
	}
	want, err := oracle.Compute(e.c, history)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range want.Keys {
		if !reflect.DeepEqual(p, snap.Keys[p.Key].Projection) {
			t.Fatalf("full-source oracle mismatch: %+v", p)
		}
	}
}

func TestWorkerExternalFrontierOverridesGroupCommit(t *testing.T) {
	e := setup(t, 1)
	history := e.publish(t, e.records(t, 0)...)
	token, err := e.db.Claim(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.db.Process(t.Context(), token, 0, history[0]); err != nil {
		t.Fatal(err)
	}
	req := kmsg.NewOffsetCommitRequest()
	req.Group = Group(e.c.Namespace)
	topic := kmsg.NewOffsetCommitRequestTopic()
	topic.Topic = e.topic
	p := kmsg.NewOffsetCommitRequestTopicPartition()
	p.Partition, p.Offset = 0, 5
	topic.Partitions = append(topic.Partitions, p)
	req.Topics = append(req.Topics, topic)
	response, err := req.RequestWith(t.Context(), e.cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range response.Topics {
		for _, p := range topic.Partitions {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				t.Fatal(err)
			}
		}
	}
	s := track(e.db)
	w := e.start(t, s)
	e.await(t, 5)
	w.stop(t)
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.processed) != 4 || s.processed[0] != 1 {
		t.Fatalf("processed offsets %v; expected 1..4 despite group offset 5", s.processed)
	}
	checkBusiness(t, e, history, 11)
}

// These wrappers control caller observations around actual SQL decisions. The
// store suite separately drops actual PostgreSQL wire acknowledgements.
type unknownState struct {
	*postgres.Store
	claim, process atomic.Bool
	committed      bool
	observations   atomic.Int32
}

func (s *unknownState) Claim(ctx context.Context, p int) (store.Token, error) {
	if s.claim.CompareAndSwap(false, true) {
		if s.committed {
			if _, err := s.Store.Claim(ctx, p); err != nil {
				return store.Token{}, err
			}
		}
		return store.Token{}, store.ErrCommitUnknown
	}
	return s.Store.Claim(ctx, p)
}
func (s *unknownState) Process(ctx context.Context, t store.Token, n int64, r ledger.Record) error {
	if s.process.CompareAndSwap(false, true) {
		if s.committed {
			if err := s.Store.Process(ctx, t, n, r); err != nil {
				return err
			}
		}
		return store.ErrCommitUnknown
	}
	return s.Store.Process(ctx, t, n, r)
}
func (s *unknownState) Observe(ctx context.Context, p int) (store.Token, store.Partition, error) {
	s.observations.Add(1)
	return s.Store.Observe(ctx, p)
}
func TestWorkerUnknownOutcomes(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprint(committed), func(t *testing.T) {
			e := setup(t, 1)
			history := e.publish(t, e.records(t, 0)...)
			s := &unknownState{Store: e.db, committed: committed}
			w := e.start(t, s)
			e.await(t, 5)
			w.stop(t)
			if s.observations.Load() < 4 {
				t.Fatal("unknown outcomes did not reobserve durable state")
			}
			checkBusiness(t, e, history, 11)
		})
	}
}

type waitingClaim struct {
	*postgres.Store
	entered chan struct{}
	once    sync.Once
}

func (s *waitingClaim) Claim(ctx context.Context, p int) (store.Token, error) {
	s.once.Do(func() { close(s.entered) })
	return s.Store.Claim(ctx, p)
}
func TestWorkerShutdownCancelsBlockedSQLClaim(t *testing.T) {
	e := setup(t, 1)
	e.publish(t, e.records(t, 0)...)
	tx, err := e.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	ns, _ := json.Marshal(e.c.Namespace)
	if _, err = tx.Exec(t.Context(), "SELECT epoch FROM amends_partitions WHERE namespace_id=$1 AND partition=0 FOR UPDATE", string(ns)); err != nil {
		t.Fatal(err)
	}
	s := &waitingClaim{Store: e.db, entered: make(chan struct{})}
	w := e.start(t, s)
	select {
	case <-s.entered:
	case <-time.After(15 * time.Second):
		t.Fatal("assignment never claimed")
	}
	eventually(t, func() bool {
		var blocked bool
		err := e.pool.QueryRow(t.Context(), "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)))", tx.Conn().PgConn().PID()).Scan(&blocked)
		if err != nil {
			t.Fatal(err)
		}
		return blocked
	})
	w.stop(t)
	if err = tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, state, err := e.db.Observe(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if state.Epoch != 0 || state.Next != 0 {
		t.Fatalf("cancelled callback installed ownership: %+v", state)
	}
	replacement := e.start(t, e.db)
	e.await(t, 5)
	replacement.stop(t)
}

func TestWorkerQuarantinesRawBrokerKeys(t *testing.T) {
	e := setup(t, 2)
	records := e.records(t, 0)
	bad := records[0]
	bad.RawKey = []byte{0xff, 0, 0x01}
	mismatch := records[1]
	mismatch.Key = e.c.Keys[1].StockKey
	misroute := records[2]
	misroute.Partition = 1
	e.publish(t, bad, mismatch, records[0], misroute)
	w := e.start(t, e.db)
	eventually(t, func() bool {
		_, a, err := e.db.Observe(t.Context(), 0)
		if err != nil {
			t.Fatal(err)
		}
		_, b, err := e.db.Observe(t.Context(), 1)
		if err != nil {
			t.Fatal(err)
		}
		return a.Next == 3 && b.Next == 1
	})
	w.stop(t)
	snap, err := e.db.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Rejections) != 3 {
		t.Fatalf("rejections=%+v", snap.Rejections)
	}
	if snap.Rejections[0].Reason != "KEY_MISMATCH" || !reflect.DeepEqual(snap.Rejections[0].Record.RawKey, bad.RawKey) || snap.Rejections[1].Reason != "KEY_MISMATCH" || snap.Rejections[2].Reason != "ROUTING" {
		t.Fatalf("raw-key evidence/reasons: %+v", snap.Rejections)
	}
}
