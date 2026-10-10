//go:build integration

// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (e *environment) sendEnvelope(t *testing.T, p int, envelope ledger.Envelope) *kgo.Record {
	t.Helper()
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	r := &kgo.Record{Topic: e.Manifest.Output.Topic, Partition: int32(p), Key: broker.EncodeKey(envelope.ID.Key), Value: raw}
	if err = e.cl.ProduceSync(t.Context(), r).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestViewProtocolConflictPersistsAndOtherPartitionContinues(t *testing.T) {
	e := setup(t, 2)
	start(t, e.worker)
	relay := start(t, func(ctx context.Context) error { return RunRelay(ctx, e.Options) })
	materializer := start(t, func(ctx context.Context) error { return RunView(ctx, e.Options) })
	e.publish(t, "before.jsonl")
	e.awaitView(t, 5)
	relay.stop(t)
	v, err := e.Store.View(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	status := func(p int) ledger.Envelope {
		return v.Rows[ledger.OutputID{Namespace: e.Manifest.Config.Namespace, Family: "key-status", Key: e.Manifest.Config.Keys[p].StockKey}]
	}
	conflicting := status(0)
	conflicting.Cause.Offset++ // Same version/payload, different cause is still a violation.
	bad := e.sendEnvelope(t, 0, conflicting)
	e.sendEnvelope(t, 0, status(0)) // Buffered later good data cannot move the stopped frontier.
	e.sendEnvelope(t, 1, status(1))
	eventually(t, func() bool {
		a, err := e.Store.ViewPosition(t.Context(), 0)
		if err != nil {
			t.Fatal(err)
		}
		b, err := e.Store.ViewPosition(t.Context(), 1)
		if err != nil {
			t.Fatal(err)
		}
		return a.Error != "" && a.Next == 5 && b.Next == 6
	})
	materializer.stop(t)
	var offset int64
	var key, value []byte
	ns, _ := json.Marshal(e.Manifest.Config.Namespace)
	if err = e.pool.QueryRow(t.Context(), "SELECT error_offset,error_key,error_value FROM amends_view_progress WHERE namespace_id=$1 AND partition=0", string(ns)).Scan(&offset, &key, &value); err != nil {
		t.Fatal(err)
	}
	if offset != bad.Offset || !bytes.Equal(key, bad.Key) || !bytes.Equal(value, bad.Value) {
		t.Fatal("protocol evidence changed or disappeared")
	}
	start(t, func(ctx context.Context) error { return RunView(ctx, e.Options) })
	e.sendEnvelope(t, 1, status(1))
	eventually(t, func() bool {
		b, err := e.Store.ViewPosition(t.Context(), 1)
		if err != nil {
			t.Fatal(err)
		}
		return b.Next == 7
	})
	after, err := e.Store.View(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if after.Next[0] != 5 || after.Errors[0] == "" || !after.Rows[status(0).ID].Equal(status(0)) {
		t.Fatal("restart cleared protocol stop or selected conflicting payload")
	}
}

func TestMalformedOutputStopsWithoutQuarantineOrProgress(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"invalid UTF-8", []byte{255, 0}},
		{"NUL in unknown member", []byte(`{"\u0000":1}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := setup(t, 1)
			bad := &kgo.Record{Topic: e.Manifest.Output.Topic, Partition: 0, Key: []byte{0, 255}, Value: tc.raw}
			if err := e.cl.ProduceSync(t.Context(), bad).FirstErr(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if err := RunView(ctx, e.Options); !errors.Is(err, store.ErrProtocol) {
				t.Fatalf("malformed output accepted: %v", err)
			}
			position, err := e.Store.ViewPosition(t.Context(), 0)
			if err != nil {
				t.Fatal(err)
			}
			if position.Next != 0 || position.Error == "" {
				t.Fatalf("invalid progress: %+v", position)
			}
			s, err := e.Store.Snapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if len(s.Rejections) != 0 {
				t.Fatal("output protocol failure was source quarantine")
			}
			var key, value []byte
			ns, _ := json.Marshal(e.Manifest.Config.Namespace)
			if err = e.pool.QueryRow(t.Context(), "SELECT error_key,error_value FROM amends_view_progress WHERE namespace_id=$1 AND partition=0", string(ns)).Scan(&key, &value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(key, bad.Key) || !bytes.Equal(value, bad.Value) {
				t.Fatal("malformed output bytes lost")
			}
			if err := RunView(ctx, e.Options); !errors.Is(err, store.ErrProtocol) {
				t.Fatalf("protocol stop lost on restart: %v", err)
			}
		})
	}
}

type stopAfterPublish struct {
	publishLoss
	cancel context.CancelFunc
}

func (p *stopAfterPublish) Publish(ctx context.Context, d store.Delivery) (int64, error) {
	_, err := p.publishLoss.Publish(ctx, d)
	if err != nil {
		return 0, err
	}
	p.cancel()
	return 0, ctx.Err()
}
func TestRelayRestartAfterPublicationBeforeReceipt(t *testing.T) {
	e := setup(t, 1)
	d := e.seedDecision(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	publisher := &stopAfterPublish{publishLoss: publishLoss{e: e}, cancel: cancel}
	if _, err := RelayStep(ctx, e.Store, publisher, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled relay reported success: %v", err)
	}
	receipt, err := e.Store.Receipt(t.Context(), 0, d.Sequence)
	if err != nil || receipt != nil {
		t.Fatalf("unacknowledged publication was marked sent: %v %v", receipt, err)
	}
	relay := start(t, func(ctx context.Context) error { return RunRelay(ctx, e.Options) })
	start(t, func(ctx context.Context) error { return RunView(ctx, e.Options) })
	e.awaitView(t, 4)
	relay.stop(t)
	records := e.readOutputs(t, 4)
	if !bytes.Equal(records[0].Value, d.Bytes) || !bytes.Equal(records[1].Value, d.Bytes) {
		t.Fatal("restart retry changed envelope bytes")
	}
	s, err := e.Store.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Outbox) != 3 {
		t.Fatal("transport retry allocated new output versions")
	}
}

func TestOutputTopicRecreationRefusesResume(t *testing.T) {
	e := setup(t, 1)
	req := kmsg.NewDeleteTopicsRequest()
	topic := kmsg.NewDeleteTopicsRequestTopic()
	topic.Topic = kmsg.StringPtr(e.Manifest.Output.Topic)
	req.Topics = append(req.Topics, topic)
	response, err := req.RequestWith(t.Context(), e.cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range response.Topics {
		if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
			t.Fatal(err)
		}
	}
	created, err := broker.CreateSource(t.Context(), e.cl, e.Manifest.Output.Topic, 1)
	if err != nil {
		t.Fatal(err)
	}
	if created.ID == e.Manifest.Output.Incarnation {
		t.Fatal("topic UUID did not change")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := RunRelay(ctx, e.Options); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("relay adopted recreated output: %v", err)
	}
	if err := RunView(ctx, e.Options); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("view adopted recreated output: %v", err)
	}
	changed := *e.Manifest.Output
	changed.Incarnation = created.ID
	if err := e.Store.BindOutput(t.Context(), changed); !errors.Is(err, store.ErrFence) {
		t.Fatalf("rebound output: %v", err)
	}
}
