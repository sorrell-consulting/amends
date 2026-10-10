//go:build integration

// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package worker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func rejectRun(t *testing.T, e *environment, message string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	err := Run(ctx, Options{Brokers: e.brokers, Topic: e.topic, Config: e.c, State: e.db, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err == nil || !strings.Contains(err.Error(), message) {
		t.Fatalf("expected %q, got %v", message, err)
	}
	s, err := e.db.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Outbox) != 0 || len(s.Rejections) != 0 {
		t.Fatal("invalid source boundary produced a processing decision")
	}
}

func TestWorkerRejectsMissingRequiredHistory(t *testing.T) {
	e := setup(t, 1)
	e.publish(t, e.records(t, 0)...)
	req := kmsg.NewDeleteRecordsRequest()
	topic := kmsg.NewDeleteRecordsRequestTopic()
	topic.Topic = e.topic
	p := kmsg.NewDeleteRecordsRequestTopicPartition()
	p.Partition, p.Offset = 0, 2
	topic.Partitions = append(topic.Partitions, p)
	req.Topics = append(req.Topics, topic)
	r, err := req.RequestWith(t.Context(), e.cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range r.Topics {
		for _, p := range topic.Partitions {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				t.Fatal(err)
			}
		}
	}
	rejectRun(t, e, "required history missing")
	_, state, err := e.db.Observe(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if state.Epoch != 0 || state.Next != 0 {
		t.Fatalf("invalid boundary installed owner or moved progress: %+v", state)
	}
}

func TestWorkerRejectsRecreatedTopic(t *testing.T) {
	e := setup(t, 1)
	req := kmsg.NewDeleteTopicsRequest()
	topic := kmsg.NewDeleteTopicsRequestTopic()
	topic.Topic = kmsg.StringPtr(e.topic)
	req.Topics = append(req.Topics, topic)
	r, err := req.RequestWith(t.Context(), e.cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range r.Topics {
		if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
			t.Fatal(err)
		}
	}
	s, err := broker.CreateSource(t.Context(), e.cl, e.topic, 1)
	if err != nil {
		t.Fatal(err)
	}
	if s.ID == e.c.Source {
		t.Fatal("recreation did not change broker UUID")
	}
	rejectRun(t, e, "incarnation/partition count differs")
}

func TestWorkerRejectsPartitionCountChange(t *testing.T) {
	e := setup(t, 1)
	req := kmsg.NewCreatePartitionsRequest()
	topic := kmsg.NewCreatePartitionsRequestTopic()
	topic.Topic, topic.Count = e.topic, 2
	req.Topics = append(req.Topics, topic)
	r, err := req.RequestWith(t.Context(), e.cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range r.Topics {
		if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, func() bool {
		source, err := broker.Inspect(t.Context(), e.cl, e.topic)
		return err == nil && source.Partitions == 2
	})
	rejectRun(t, e, "incarnation/partition count differs")
}

func TestWorkerRejectsFiniteRetention(t *testing.T) {
	e := setup(t, 1)
	req := kmsg.NewIncrementalAlterConfigsRequest()
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType, resource.ResourceName = kmsg.ConfigResourceTypeTopic, e.topic
	value := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	value.Name, value.Value = "retention.ms", kmsg.StringPtr("60000")
	resource.Configs = append(resource.Configs, value)
	req.Resources = append(req.Resources, resource)
	r, err := req.RequestWith(t.Context(), e.cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, resource := range r.Resources {
		if err := kerr.ErrorForCode(resource.ErrorCode); err != nil {
			t.Fatal(err)
		}
	}
	rejectRun(t, e, "retention.ms=-1")
}

func TestWorkerRejectsFrontierBeyondBrokerEnd(t *testing.T) {
	e := setup(t, 1)
	ns, _ := json.Marshal(e.c.Namespace)
	if _, err := e.pool.Exec(t.Context(), "UPDATE amends_partitions SET next_offset=100 WHERE namespace_id=$1", string(ns)); err != nil {
		t.Fatal(err)
	}
	rejectRun(t, e, "frontier/epoch incompatible")
	_, state, err := e.db.Observe(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if state.Next != 100 {
		t.Fatal("unavailable position was reset")
	}
}

func TestWorkerRejectsStoreManifestMismatch(t *testing.T) {
	e := setup(t, 1)
	e.c.Keys[0].Opening++ // A miswired State adapter must not certify another manifest.
	rejectRun(t, e, "frontier/epoch incompatible")
}

func TestWorkerReadCommittedGapsAndTrailingControls(t *testing.T) {
	e := setup(t, 1)
	records := e.records(t, 0)
	first := e.publish(t, records[0])[0]
	producer, err := broker.Client(e.brokers, kgo.TransactionalID(e.topic+"-transaction"), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err = producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	kr := &kgo.Record{Topic: e.topic, Partition: 0, Key: broker.EncodeKey(records[1].Key), Value: records[1].Value}
	if err = producer.ProduceSync(t.Context(), kr).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err = producer.EndTransaction(t.Context(), kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	ends, err := broker.Offsets(t.Context(), e.cl, e.topic, 1, -1)
	if err != nil {
		t.Fatal(err)
	}
	if ends[0] <= first.Offset+1 {
		t.Fatal("aborted transaction did not leave a broker offset gap")
	}
	s := track(e.db)
	w := e.start(t, s)
	e.await(t, first.Offset+1)
	// Observe through several idle poll periods: neither the fetch cursor nor
	// trailing filtered/control positions may be committed as processed input.
	deadline := time.NewTimer(2200 * time.Millisecond)
	defer deadline.Stop()
	<-deadline.C
	_, state, err := e.db.Observe(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if state.Next != first.Offset+1 {
		t.Fatalf("trailing controls advanced frontier: %+v", state)
	}
	w.stop(t)
	last := e.publish(t, records[2])[0]
	if last.Offset <= first.Offset+1 {
		t.Fatal("expected a coordinate gap")
	}
	replacement := e.start(t, e.db)
	e.await(t, last.Offset+1)
	replacement.stop(t)
	snapshot, err := e.db.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	key := snapshot.Keys[e.c.Keys[0].StockKey]
	if len(key.Identities) != 2 || len(key.Projection.Balances) != 2 || key.Projection.Balances[1].Close != 100 {
		t.Fatalf("aborted input leaked into authority or gap lost a record: %+v", key)
	}
	t.Logf("visible offsets %d and %d; aborted data %d; trailing end %d; final durable frontier %d", first.Offset, last.Offset, kr.Offset, ends[0], last.Offset+1)
}
