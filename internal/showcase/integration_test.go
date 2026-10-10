//go:build integration

// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package showcase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestShowcase(t *testing.T) {
	base := showcaseTestOptions(t)
	for _, name := range []string{"correction-only", "crash-mid-correction", "cancel-after-baseline"} {
		crash := name == "crash-mid-correction"
		scenario := CorrectionOnly
		if crash {
			scenario = CrashMidCorrection
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			var transcript bytes.Buffer
			cancellation := newCheckpointCancellation(t)
			if name == "cancel-after-baseline" {
				cancellation.point, cancellation.cancel = BaselineVerified, cancel
			}
			options := base
			options.Scenario, options.Output, options.OnCheckpoint = scenario, &transcript, cancellation.observe
			report, err := Run(ctx, options)
			retainTestRun(t, base, report, &transcript)
			for _, child := range report.Processes {
				wantExit := 0
				if report.Crash != nil && child.PID == report.Crash.WorkerPID {
					wantExit = -1
				}
				checkChildExit(t, child, wantExit)
				if name == "cancel-after-baseline" && (child.Name == "relay-after" || child.Name == "load-correction") {
					t.Fatal("canceled baseline continued the correction")
				}
			}

			if name == "cancel-after-baseline" {
				if err == nil || report.Status != "ERROR" || len(report.Stages) != 1 {
					t.Fatalf("canceled pipeline incorrectly accepted: %+v error=%v", report, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if report.Status != "PASS" || !report.TwoMembersAssigned || len(report.Stages) != 2 {
				t.Fatalf("incomplete run: %+v", report)
			}
			checkInspectionArtifacts(t, report.Directory, "before", "")
			checkInspectionArtifacts(t, report.Directory, "after", "")
			for i, stage := range report.Stages {
				if stage.Status != "PASS" || !slices.Equal(stage.SourceEnd, []int64{int64(3 + i), 0}) || !slices.Equal(stage.OutputEnd, []int64{int64(5 * (i + 1)), 0}) || stage.BinarySHA256 == "" {
					t.Fatalf("unexpected stage: %+v", stage)
				}
			}
			if crash {
				c := report.Crash
				if c == nil || c.Signal != "SIGKILL" || !c.BlockedAfterKill || !c.TransactionEnded || !c.DurableStateUnchanged || !c.OutputUnchanged || c.RecoveredEpoch <= c.PreviousEpoch || c.WorkerPID <= 0 || c.BackendPID <= 0 || c.BlockerPID <= 0 {
					t.Fatalf("crash not established: %+v", c)
				}
				before, e := os.ReadFile(filepath.Join(report.Directory, "rollback-before.json"))
				if e != nil {
					t.Fatal(e)
				}
				after, e := os.ReadFile(filepath.Join(report.Directory, "rollback-after.json"))
				if e != nil {
					t.Fatal(e)
				}
				if !bytes.Equal(before, after) {
					t.Fatal("partial decision escaped")
				}
			} else if report.Crash != nil {
				t.Fatal("no-fault scenario recorded a crash")
			}
			// Fixed semantic versions establish that retry did not commit another
			// transition. Expectations are hand-stated, not made by diff/fold.
			raw, e := os.ReadFile(filepath.Join(report.Directory, "manifest.json"))
			if e != nil {
				t.Fatal(e)
			}
			m, e := broker.ReadManifest(bytes.NewReader(raw))
			if e != nil {
				t.Fatal(e)
			}
			pool, e := pgxpool.New(ctx, base.DatabaseURL)
			if e != nil {
				t.Fatal(e)
			}
			defer pool.Close()
			state, e := postgres.New(pool, m.Config)
			if e != nil {
				t.Fatal(e)
			}
			v, e := state.View(ctx)
			if e != nil {
				t.Fatal(e)
			}
			if len(v.Rows) != 6 {
				t.Fatalf("expected 3 balances, 2 alert versions, 1 key status; got %d", len(v.Rows))
			}
			for _, row := range []struct {
				family  ledger.OutputFamily
				day     string
				version int64
				present bool
			}{
				{"balance", "2026-01-01", 2, true}, {"balance", "2026-01-02", 2, true}, {"balance", "2026-01-03", 2, true},
				{"alert", "2026-01-01", 2, false}, {"alert", "2026-01-02", 1, true}, {"key-status", "", 1, true},
			} {
				e := v.Rows[ledger.OutputID{Namespace: m.Config.Namespace, Key: m.Config.Keys[0].StockKey, Family: row.family, Day: row.day}]
				if e.Version != row.version || e.Payload.Present != row.present {
					t.Errorf("unexpected version/tombstone: %+v want %+v", e, row)
				}
			}
		})
	}
}

func cleanupRun(t *testing.T, dsn string, brokers []string, ns string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Error(err)
		return
	}
	defer pool.Close()
	key, _ := json.Marshal(ns)
	for _, table := range []string{"amends_view_rows", "amends_view_progress", "amends_outbox", "amends_quarantine", "amends_output_state", "amends_key_state", "amends_partitions", "amends_output_binding", "amends_namespaces"} {
		if _, err = pool.Exec(ctx, "DELETE FROM "+table+" WHERE namespace_id=$1", string(key)); err != nil {
			t.Error(err)
		}
	}
	cl, err := broker.Client(brokers)
	if err != nil {
		t.Error(err)
		return
	}
	defer cl.Close()
	names := []string{ns + "-source", ns + "-output"}
	q := kmsg.NewDeleteTopicsRequest()
	// The pinned broker negotiates v6, which does not encode TopicNames.
	for _, name := range names {
		topic := kmsg.NewDeleteTopicsRequestTopic()
		topic.Topic = kmsg.StringPtr(name)
		q.Topics = append(q.Topics, topic)
	}
	response, err := q.RequestWith(ctx, cl)
	if err != nil {
		t.Error(err)
		return
	}
	if len(response.Topics) != len(names) {
		t.Errorf("incomplete cleanup response: %+v", response.Topics)
		return
	}
	pending := map[string]bool{names[0]: true, names[1]: true}
	for _, topic := range response.Topics {
		if topic.Topic == nil || !pending[*topic.Topic] {
			t.Errorf("unexpected cleanup topic: %+v", topic)
			return
		}
		delete(pending, *topic.Topic)
		if err := kerr.ErrorForCode(topic.ErrorCode); err != nil && !errors.Is(err, kerr.UnknownTopicOrPartition) {
			t.Errorf("cleanup topic: %+v", topic)
			return
		}
	}
	// Deletion can finish asynchronously. Inspect without auto-creating topics
	// so empty or ineffective requests cannot silently leak test fixtures.
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for _, name := range names {
		for {
			_, err := broker.Inspect(ctx, cl, name)
			if errors.Is(err, kerr.UnknownTopicOrPartition) {
				break
			}
			if err != nil {
				t.Errorf("observe cleanup of %s: %v", name, err)
				return
			}
			select {
			case <-ctx.Done():
				t.Errorf("topic %s still exists after cleanup: %v", name, ctx.Err())
				return
			case <-ticker.C:
			}
		}
	}
}
