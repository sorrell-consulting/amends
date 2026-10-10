//go:build integration

// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package delivery

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/sorrell-consulting/amends/internal/verify"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (e *environment) verify(t *testing.T, timeout time.Duration) verify.Report {
	t.Helper()
	return verify.Run(t.Context(), verify.Options{Brokers: e.Brokers, Manifest: e.Manifest, Store: e.Store, Quiesce: true, Timeout: timeout})
}
func (e *environment) awaitSource(t *testing.T, next int64) {
	t.Helper()
	eventually(t, func() bool {
		p, err := e.Store.Progress(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		return p.Partitions[0].SourceNext == next
	})
}
func (e *environment) awaitReceipts(t *testing.T) {
	t.Helper()
	eventually(t, func() bool {
		p, err := e.Store.Progress(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range p.Partitions {
			if r.Pending != 0 {
				return false
			}
		}
		return true
	})
}
func (e *environment) sourceRecord(t *testing.T, key, value []byte) ledger.Record {
	t.Helper()
	r := &kgo.Record{Topic: e.Manifest.Topic, Partition: 0, Key: key, Value: value}
	if err := e.cl.ProduceSync(t.Context(), r).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return broker.DecodeRecord(e.Manifest.Config.Source, r).Clone()
}
func requireStatus(t *testing.T, r verify.Report, status string) {
	t.Helper()
	if r.Status != status {
		raw, _ := json.MarshalIndent(r, "", "  ")
		t.Fatalf("want %s: %s", status, raw)
	}
	if status != "PASS" && r.Accepts(nil) {
		t.Fatal("negative/qualified result accepted without exact fixture")
	}
}

// SC16/17/25: the command's source scan, never worker tables, supplies the oracle.
func TestVerificationFixtureBoundaries(t *testing.T) {
	e := setup(t, 2)
	start(t, e.worker)
	start(t, e.worker)
	start(t, func(ctx context.Context) error { return RunView(ctx, e.Options) })
	for i, stage := range []struct {
		file   string
		view   int64
		source int64
	}{{"before.jsonl", 5, 3}, {"correction.jsonl", 10, 4}, {"cancel-last-day.jsonl", 11, 5}} {
		relay := start(t, func(ctx context.Context) error { return RunRelay(ctx, e.Options) })
		e.publish(t, stage.file)
		e.awaitView(t, stage.view)
		e.awaitReceipts(t)
		relay.stop(t)
		r := e.verify(t, 5*time.Second)
		requireStatus(t, r, "PASS")
		if !r.Accepts(nil) || !r.Checks.BoundariesStable || r.SourceEnd[0] != stage.source || r.SourceEnd[1] != stage.source || r.OutputEnd[0] != stage.view || r.SourceRecords != int(stage.source*2) || len(r.Differences) != 0 {
			t.Fatalf("stage %d incomplete report: %+v", i, r)
		}
	}
}

// SC08 with SC25: INCOMPLETE retains BLOCKED authority, then overall BLOCKED
// after drain; a unique repair yields qualified agreement with exact evidence.
func TestVerificationConflictIncompleteAndRepair(t *testing.T) {
	e := setup(t, 1)
	start(t, e.worker)
	start(t, func(ctx context.Context) error { return RunView(ctx, e.Options) })
	history := e.publish(t, "before.jsonl")
	tx, err := ledger.Decode(history[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	tx.Qty = -40
	raw, _ := json.Marshal(tx)
	conflict := e.sourceRecord(t, history[0].RawKey, raw)
	e.awaitSource(t, 4)
	r := e.verify(t, 5*time.Second)
	requireStatus(t, r, "INCOMPLETE")
	if r.Progress.Partitions[0].Pending != 6 || r.Progress.Partitions[0].OldestPending == nil || len(r.KnownConflicts) != 1 || r.Keys[0].AuthorityStatus != "BLOCKED" || r.Keys[0].Compared || !r.Checks.SourceScanned || r.Checks.PublicationDrained {
		t.Fatalf("lost incomplete/conflict distinction: %+v", r)
	}
	relay := start(t, func(ctx context.Context) error { return RunRelay(ctx, e.Options) })
	e.awaitView(t, 6)
	e.awaitReceipts(t)
	relay.stop(t)
	requireStatus(t, e.verify(t, 5*time.Second), "BLOCKED")
	tx.Revision = 2
	tx.Qty = -30
	raw, _ = json.Marshal(tx)
	e.sourceRecord(t, history[0].RawKey, raw)
	relay = start(t, func(ctx context.Context) error { return RunRelay(ctx, e.Options) })
	e.awaitView(t, 12)
	e.awaitReceipts(t)
	relay.stop(t)
	r = e.verify(t, 5*time.Second)
	requireStatus(t, r, "PASS_WITH_EXCLUSIONS")
	// Hand-authored fixture uses records published by this test, not verifier output.
	expected := &verify.ExpectedDiagnostics{Name: "repaired-source-conflict", Namespace: e.Manifest.Config.Namespace, ConfigDigest: e.Manifest.Config.Digest(), SourceEnd: []int64{5}, Diagnostics: verify.Diagnostics{Conflicts: []verify.ConflictEvidence{{Conflict: ledger.Conflict{StockKey: tx.StockKey, TxnID: tx.TxnID, Revision: 1}, Records: []ledger.Record{history[0], conflict}}}}}
	if !r.Accepts(expected) || r.Status != "PASS_WITH_EXCLUSIONS" {
		t.Fatalf("exact repaired fixture refused: %+v", r)
	}
	bad := e.sourceRecord(t, []byte{255, 0}, []byte{0, 255})
	e.awaitSource(t, 6)
	r = e.verify(t, 5*time.Second)
	requireStatus(t, r, "PASS_WITH_EXCLUSIONS")
	if r.Accepts(expected) {
		t.Fatal("additional poison record was suppressed")
	}
	expected.SourceEnd = []int64{6}
	expected.Diagnostics.Rejections = []ledger.Rejection{{Record: bad, Reason: "MALFORMED"}}
	if !r.Accepts(expected) {
		t.Fatal("exact poison fixture refused")
	}
	expected.Diagnostics.Rejections[0].Reason = "KIND"
	if r.Accepts(expected) {
		t.Fatal("wrong expected reason accepted")
	}
	ns, _ := json.Marshal(e.Manifest.Config.Namespace)
	if _, err = e.pool.Exec(t.Context(), "UPDATE amends_quarantine SET reason='KIND' WHERE namespace_id=$1", string(ns)); err != nil {
		t.Fatal(err)
	}
	r = e.verify(t, 5*time.Second)
	requireStatus(t, r, "DRIFT")
	if r.Accepts(expected) {
		t.Fatal("expected manifest masked incorrect durable evidence")
	}
}

func TestVerificationViewLagDriftAndProtocolFailure(t *testing.T) {
	e := setup(t, 1)
	start(t, e.worker)
	relay := start(t, func(ctx context.Context) error { return RunRelay(ctx, e.Options) })
	e.publish(t, "before.jsonl")
	e.awaitSource(t, 3)
	e.awaitReceipts(t)
	relay.stop(t)
	r := e.verify(t, 700*time.Millisecond)
	requireStatus(t, r, "INCOMPLETE")
	if !r.Checks.PublicationDrained || r.Checks.ViewReached {
		t.Fatalf("view not included in boundary: %+v", r)
	}
	materializer := start(t, func(ctx context.Context) error { return RunView(ctx, e.Options) })
	e.awaitView(t, 5)
	materializer.stop(t)
	requireStatus(t, e.verify(t, 5*time.Second), "PASS")
	ns, _ := json.Marshal(e.Manifest.Config.Namespace)
	id := ledger.OutputID{Namespace: e.Manifest.Config.Namespace, Family: "balance", Key: e.Manifest.Config.Keys[0].StockKey, Day: "2026-01-02"}
	if _, err := e.pool.Exec(t.Context(), "DELETE FROM amends_view_rows WHERE namespace_id=$1 AND output_id=$2", string(ns), id.String()); err != nil {
		t.Fatal(err)
	}
	r = e.verify(t, 5*time.Second)
	requireStatus(t, r, "DRIFT")
	if len(r.Differences) == 0 || r.Differences[0].Day != "2026-01-02" || len(r.Differences[0].SourceReferences) == 0 {
		t.Fatalf("missing drift explanation: %+v", r)
	}
	if _, err := e.pool.Exec(t.Context(), "UPDATE amends_view_progress SET protocol_error='synthetic protocol failure' WHERE namespace_id=$1", string(ns)); err != nil {
		t.Fatal(err)
	}
	requireStatus(t, e.verify(t, 5*time.Second), "ERROR")
}

func TestVerificationWriterGuards(t *testing.T) {
	e := setup(t, 1)
	for _, role := range []string{"producer", "relay"} {
		t.Run(role, func(t *testing.T) {
			service, err := e.Store.AcquireService(t.Context(), role)
			if err != nil {
				t.Fatal(err)
			}
			defer service.Close()
			r := e.verify(t, 350*time.Millisecond)
			requireStatus(t, r, "INCOMPLETE")
			if r.Checks.WritersStopped {
				t.Fatal("active writer treated as stopped")
			}
		})
	}
	requireStatus(t, e.verify(t, 3*time.Second), "PASS")
}

// Force expiry inside the second real guard query, after the first attempt
// observed the live producer. Expiry during the retry pause must have the same
// boundary status. This tracer changes timing only; PostgreSQL decides locks.
type expireGuardRetry struct{ attempts int }

func (p *expireGuardRetry) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if data.SQL == "SELECT pg_try_advisory_lock($1)" {
		p.attempts++
		if p.attempts == 2 {
			<-ctx.Done()
		}
	}
	return ctx
}
func (*expireGuardRetry) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestVerificationProducerDeadlineDuringGuardRetry(t *testing.T) {
	e := setup(t, 1)
	producer, err := e.Store.AcquireService(t.Context(), "producer")
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	tracer := &expireGuardRetry{}
	config := e.pool.Config()
	config.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	state, err := postgres.New(pool, e.Manifest.Config)
	if err != nil {
		t.Fatal(err)
	}
	r := verify.Run(t.Context(), verify.Options{Brokers: e.Brokers, Manifest: e.Manifest, Store: state, Quiesce: true, Timeout: time.Second})
	if tracer.attempts != 2 {
		t.Fatalf("deadline did not occur in the controlled retry: attempts=%d report=%+v", tracer.attempts, r)
	}
	requireStatus(t, r, "INCOMPLETE")
	if r.Checks.WritersStopped || !strings.Contains(r.Reason, "owned source loader has not stopped") {
		t.Fatalf("producer wait lost its boundary reason: %+v", r)
	}
	if err = producer.Check(t.Context()); err != nil {
		t.Fatalf("verification disturbed the producer guard: %v", err)
	}
}

func TestVerificationRecreatedSourceRefusesComparison(t *testing.T) {
	e := setup(t, 1)
	req := kmsg.NewDeleteTopicsRequest()
	topic := kmsg.NewDeleteTopicsRequestTopic()
	topic.Topic = kmsg.StringPtr(e.Manifest.Topic)
	req.Topics = append(req.Topics, topic)
	if _, err := req.RequestWith(t.Context(), e.cl); err != nil {
		t.Fatal(err)
	}
	if _, err := broker.CreateSource(t.Context(), e.cl, e.Manifest.Topic, 1); err != nil {
		t.Fatal(err)
	}
	r := e.verify(t, 3*time.Second)
	requireStatus(t, r, "INCOMPLETE")
	if !strings.Contains(r.Reason, "incarnation") {
		t.Fatalf("wrong refusal: %+v", r)
	}
}

func TestVerificationReadCommittedGapsAndTrailingControls(t *testing.T) {
	e := setup(t, 1)
	before := e.publish(t, "before.jsonl")
	producer, err := broker.Client(e.Brokers, kgo.TransactionalID(e.Manifest.Topic+"-aborted"), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err = producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	tx, err := ledger.Decode(before[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	tx.Revision = 99
	tx.Qty = -90
	raw, _ := json.Marshal(tx)
	if err = producer.ProduceSync(t.Context(), &kgo.Record{Topic: e.Manifest.Topic, Partition: 0, Key: before[0].RawKey, Value: raw}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err = producer.EndTransaction(t.Context(), kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	r := e.verify(t, 700*time.Millisecond)
	requireStatus(t, r, "INCOMPLETE")
	if r.Checks.SourceScanned {
		t.Fatal("trailing invisible positions treated as scanned records")
	}
	start(t, e.worker)
	relay := start(t, func(ctx context.Context) error { return RunRelay(ctx, e.Options) })
	start(t, func(ctx context.Context) error { return RunView(ctx, e.Options) })
	history := append(before, e.publish(t, "correction.jsonl")...)
	e.awaitView(t, 10)
	e.awaitReceipts(t)
	relay.stop(t)
	e.check(t, history, "expected-after.json")
	r = e.verify(t, 5*time.Second)
	requireStatus(t, r, "PASS")
	if r.SourceRecords != 4 || r.SourceEnd[0] <= 4 {
		t.Fatalf("aborted input included or gap lost: %+v", r)
	}
}

func TestVerificationMissingHistory(t *testing.T) {
	e := setup(t, 1)
	e.publish(t, "before.jsonl")
	req := kmsg.NewDeleteRecordsRequest()
	topic := kmsg.NewDeleteRecordsRequestTopic()
	topic.Topic = e.Manifest.Topic
	p := kmsg.NewDeleteRecordsRequestTopicPartition()
	p.Partition = 0
	p.Offset = 1
	topic.Partitions = append(topic.Partitions, p)
	req.Topics = append(req.Topics, topic)
	if _, err := req.RequestWith(t.Context(), e.cl); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		starts, err := broker.Offsets(t.Context(), e.cl, e.Manifest.Topic, 1, -2)
		if err != nil {
			t.Fatal(err)
		}
		return starts[0] == 1
	})
	r := e.verify(t, 3*time.Second)
	requireStatus(t, r, "INCOMPLETE")
	if !strings.Contains(r.Reason, "history missing") || r.Checks.SourceScanned {
		t.Fatalf("lost prefix was certified: %+v", r)
	}
}

func TestVerificationDetectsAdvancingUncooperativeSource(t *testing.T) {
	e := setup(t, 1)
	before := e.publish(t, "before.jsonl")
	// Bypass the application writer guard to exercise the final
	// defense against changing input. There is no hostile-writer guarantee.
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		for ctx.Err() == nil {
			record := &kgo.Record{Topic: e.Manifest.Topic, Partition: 0, Key: before[0].RawKey, Value: before[0].Value}
			if err := e.cl.ProduceSync(ctx, record).FirstErr(); err != nil {
				done <- err
				return
			}
			if err := wait(ctx, 20*time.Millisecond); err != nil {
				done <- err
				return
			}
		}
		done <- ctx.Err()
	}()
	r := e.verify(t, 3*time.Second)
	cancel()
	<-done
	requireStatus(t, r, "INCOMPLETE")
	if !strings.Contains(r.Reason, "source advanced") && !strings.Contains(r.Reason, "source changed") {
		t.Fatalf("advance was not observed: %+v", r)
	}
}

func TestVerificationLostWriterSessionNeverCertifies(t *testing.T) {
	e := setup(t, 1)
	e.publish(t, "before.jsonl") // No worker: keep the verifier waiting at H.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan verify.Report, 1)
	go func() {
		done <- verify.Run(ctx, verify.Options{Brokers: e.Brokers, Manifest: e.Manifest, Store: e.Store, Quiesce: true, Timeout: 4 * time.Second})
	}()
	sum := sha256.Sum256([]byte(e.Manifest.Config.Source + "/producer"))
	bits := binary.BigEndian.Uint64(sum[:8])
	var pid *int32
	eventually(t, func() bool {
		err := e.pool.QueryRow(t.Context(), `SELECT MIN(pid) FROM pg_locks WHERE locktype='advisory' AND classid::bigint=$1 AND objid::bigint=$2 AND objsubid=1 AND granted`, int64(bits>>32), int64(uint32(bits))).Scan(&pid)
		if err != nil {
			t.Fatal(err)
		}
		return pid != nil
	})
	if _, err := e.pool.Exec(t.Context(), "SELECT pg_terminate_backend($1)", *pid); err != nil {
		t.Fatal(err)
	}
	r := <-done
	requireStatus(t, r, "ERROR")
	if r.Checks.BoundariesStable {
		t.Fatal("lost writer guard certified")
	}
	// The session is not silently reconnected. A fresh owned guard can start.
	guard, err := e.Store.AcquireService(t.Context(), "producer")
	if err != nil {
		t.Fatal(err)
	}
	guard.Close()
}

func TestVerificationProducerAdmissionWaitsThenCompletes(t *testing.T) {
	for _, schedule := range []string{"normal", "delayed-disconnect"} {
		t.Run(schedule, func(test *testing.T) {
			environment := setup(test, 1)
			state := environment.Store
			var delay *producerDisconnect
			if schedule == "delayed-disconnect" {
				state, delay = delayedProducerDisconnect(test, environment)
			}
			guard, err := environment.Store.AcquireService(test.Context(), "producer")
			if err != nil {
				test.Fatal(err)
			}
			test.Cleanup(guard.Close)
			ctx, cancel := context.WithCancel(test.Context())
			defer cancel()
			done := make(chan verify.Report, 1)
			go func() {
				done <- verify.Run(ctx, verify.Options{Brokers: environment.Brokers, Manifest: environment.Manifest, Store: state, Quiesce: true, Timeout: 3 * time.Second})
			}()
			select {
			case report := <-done:
				guard.Close()
				test.Fatalf("active producer not awaited: %+v", report)
			case <-time.After(150 * time.Millisecond):
			}
			guard.Close()
			requireStatus(test, <-done, "PASS")
			if delay != nil {
				socket := delay.selected.Load()
				if socket == nil || !socket.terminateHeld.Load() || !socket.closeRequested.Load() {
					test.Fatal("did not hold the verifier's producer disconnect after local close")
				}
			}
			var replacement *postgres.Service
			observedBusy := false
			eventually(test, func() bool {
				next, err := environment.Store.AcquireService(test.Context(), "producer")
				if errors.Is(err, postgres.ErrServiceBusy) {
					observedBusy = true
					if delay != nil {
						if err := delay.release(); err != nil {
							test.Fatal(err)
						}
					}
					return false
				}
				if err != nil {
					test.Fatal(err)
				}
				replacement = next
				return true
			})
			defer replacement.Close()
			if delay != nil && !observedBusy {
				test.Fatal("delayed disconnect did not preserve the real server-side guard")
			}
			if err := replacement.Check(test.Context()); err != nil {
				test.Fatal(err)
			}
			if next, err := environment.Store.AcquireService(test.Context(), "producer"); err != postgres.ErrServiceBusy {
				if next != nil {
					next.Close()
				}
				test.Fatalf("duplicate loader allowed: %v", err)
			}
		})
	}
}
