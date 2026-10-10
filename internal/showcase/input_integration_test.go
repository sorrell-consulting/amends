//go:build integration

package showcase

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/fold"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/sorrell-consulting/amends/internal/verify"
)

func TestInputDrills(test *testing.T) {
	base := showcaseTestOptions(test)
	for _, name := range []string{"duplicate-flood", "stale-amendment-storm", "cancel-duplicate-ready", "cancel-stale-first"} {
		test.Run(name, func(test *testing.T) {
			ctx, cancel := context.WithTimeout(test.Context(), 2*time.Minute)
			defer cancel()
			var transcript bytes.Buffer
			cancellation := newCheckpointCancellation(test)
			drill := name
			if name == "cancel-duplicate-ready" {
				drill, cancellation.point, cancellation.cancel = "duplicate-flood", InputReady, cancel
			} else if name == "cancel-stale-first" {
				drill, cancellation.point, cancellation.cancel = "stale-amendment-storm", StaleFirstVerified, cancel
			}
			options := base
			options.Scenario, options.Output, options.OnCheckpoint = Scenario(drill), &transcript, cancellation.observe
			report, runErr := Run(ctx, options)
			retainTestRun(test, base, report, &transcript)
			read := func(name string, target any) {
				test.Helper()
				readRunJSON(test, report.Directory, name, target)
			}
			for _, child := range report.Processes {
				checkChildExit(test, child, 0)
				if cancellation.cancel != nil && child.Name == "load-input-burst" {
					test.Error("cancellation still launched the burst loader")
				}
			}
			stale := drill == "stale-amendment-storm"
			stages, sourceEnds, outputEnds := []string{"before", "after", "duplicates"}, []int64{3, 4, 132}, []int64{5, 10, 10}
			if stale {
				stages, sourceEnds, outputEnds = []string{"before", "after", "protected", "stale-first", "stale-storm"}, []int64{3, 4, 6, 8, 136}, []int64{5, 10, 11, 11, 11}
			}
			if cancellation.cancel != nil {
				stages, sourceEnds, outputEnds = stages[:len(stages)-1], sourceEnds[:len(sourceEnds)-1], outputEnds[:len(outputEnds)-1]
				if runErr == nil || report.Status != "ERROR" {
					test.Fatalf("canceled drill incorrectly succeeded: %+v error=%v", report, runErr)
				}
			} else if runErr != nil || report.Status != "PASS" {
				test.Fatalf("input drill failed: %+v error=%v", report, runErr)
			}
			if report.FinalVerification != "PASS" || report.Mode != drill || !report.TwoMembersAssigned || len(report.Stages) != len(stages) {
				test.Fatalf("incomplete or hidden stage results: %+v", report)
			}
			for index, stage := range report.Stages {
				checks := verify.Checks{WritersStopped: true, SourceScanned: true, WorkersReached: true, PublicationDrained: true, ViewReached: true, BoundariesStable: true}
				if stage.Status != "PASS" || stage.Checks != checks || !slices.Equal(stage.SourceEnd, []int64{sourceEnds[index], 0}) || !slices.Equal(stage.OutputEnd, []int64{outputEnds[index], 0}) || len(stage.Differences) != 0 || stage.BinarySHA256 == "" {
					test.Fatalf("wrong complete-boundary verification: %+v", stage)
				}
				checkInspectionArtifacts(test, report.Directory, stages[index], drill)
			}
			inspectionCtx, done := context.WithTimeout(test.Context(), 20*time.Second)
			defer done()
			var manifest broker.Manifest
			read("manifest.json", &manifest)
			pool, err := pgxpool.New(inspectionCtx, base.DatabaseURL)
			if err != nil {
				test.Fatal(err)
			}
			defer pool.Close()
			state, err := postgres.New(pool, manifest.Config)
			if err != nil {
				test.Fatal(err)
			}
			snapshot, err := state.Snapshot(inspectionCtx)
			if err != nil {
				test.Fatal(err)
			}
			sourceEnd, outputEnd := sourceEnds[len(stages)-1], outputEnds[len(stages)-1]
			if snapshot.Partitions[0].Next != sourceEnd || snapshot.Partitions[0].Sequence != outputEnd || int64(len(snapshot.Outbox)) != outputEnd || len(snapshot.Rejections) != 0 {
				test.Fatal("source evidence/progress changed output intent or quarantine")
			}
			view, err := state.View(inspectionCtx)
			if err != nil || len(view.Rows) != 6 || view.Next[0] != outputEnd || view.Next[1] != 0 {
				test.Fatalf("view lost retained guards/progress: %+v error=%v", view, err)
			}
			dayVersion, dayPresent, expectedFile := int64(2), true, "expected-after.json"
			if stale {
				dayVersion, dayPresent, expectedFile = 3, false, "expected-after-cancel.json"
			}
			expected, err := fixtures.Expected(expectedFile)
			if err != nil || !reflect.DeepEqual(view.Projection(manifest.Config.Keys[0].StockKey), expected) {
				test.Fatalf("view differs from hand-derived fixture: %v", err)
			}
			for _, expected := range []struct {
				family  ledger.OutputFamily
				day     string
				version int64
				present bool
			}{
				{"balance", "2026-01-01", 2, true}, {"balance", "2026-01-02", 2, true}, {"balance", "2026-01-03", dayVersion, dayPresent},
				{"alert", "2026-01-01", 2, false}, {"alert", "2026-01-02", 1, true}, {"key-status", "", 1, true},
			} {
				row := view.Rows[ledger.OutputID{Namespace: report.Namespace, Family: expected.family, Key: manifest.Config.Keys[0].StockKey, Day: expected.day}]
				if row.Version != expected.version || row.Payload.Present != expected.present || !row.Equal(snapshot.Outputs[row.ID]) {
					test.Fatalf("wrong output version/tombstone: %+v want %+v", row, expected)
				}
			}
			repetitions := 64
			if stale {
				repetitions = 65
			}
			if cancellation.cancel != nil {
				repetitions -= 64
			}
			history, err := fixtures.Records(manifest.Config, inputFixtureNames(stale, repetitions)...)
			if err != nil {
				test.Fatal(err)
			}
			actual, err := broker.Scan(inspectionCtx, base.Brokers, manifest, []int64{sourceEnd, 0})
			if err != nil || len(actual) != len(history) {
				test.Fatalf("source history unavailable: %v", err)
			}
			for index := range history {
				history[index].RawKey = broker.EncodeKey(history[index].Key)
			}
			if !reflect.DeepEqual(actual, history) {
				test.Fatal("retained source bytes/coordinates differ from declared deliveries")
			}
			checkInputReferences(test, snapshot.Keys[manifest.Config.Keys[0].StockKey], history, stale)
			if cancellation.cancel != nil {
				return
			}
			burst, err := os.ReadFile(filepath.Join(report.Directory, "input-burst.jsonl"))
			if err != nil || bytes.Count(burst, []byte{'\n'}) != 128 {
				test.Fatalf("finite burst is not retained: %v", err)
			}
			sum := sha256.Sum256(burst)
			if report.SHA256["input-burst.jsonl"] != hex.EncodeToString(sum[:]) {
				test.Fatal("generated input hash differs from retained bytes")
			}
			var baseline map[string]json.RawMessage
			read("input-state-baseline.json", &baseline)
			checkedStages := stages[len(stages)-1:]
			if stale {
				checkedStages = stages[3:]
			}
			for _, stage := range checkedStages {
				var after map[string]json.RawMessage
				read("input-state-"+stage+".json", &after)
				for _, table := range []string{"outputs", "outbox", "quarantine", "view_rows", "view_progress"} {
					if len(baseline[table]) == 0 || !bytes.Equal(baseline[table], after[table]) {
						test.Fatalf("%s changed durable %s bytes/receipts", stage, table)
					}
				}
				var evidence inputEvidence
				read("input-checks-"+stage+".json", &evidence)
				from, next, duplicate := int64(4), int64(132), true
				if stale {
					from, next = 6, 136
				}
				if stage == "stale-first" {
					next, duplicate = 8, false
				}
				measurement := evidence.LastProcessing
				if evidence.SourceFrom != from || evidence.SourceNext != next || evidence.AppendedRecords != next-from || evidence.RetainedReferences != next || !evidence.AuthorityUnchanged || !evidence.OutputUnchanged || !evidence.ViewUnchanged || !slices.Equal(evidence.OutputEnd, []int64{outputEnd, 0}) || measurement == nil {
					test.Fatalf("incomplete no-op evidence: %+v", evidence)
				}
				if measurement.Source.Offset != next-1 || measurement.Work != (fold.Work{Duplicate: duplicate, Stale: stale}) || measurement.BusinessOutputs != 0 || measurement.StatusOutputs != 0 {
					test.Fatalf("wrong last-decision work: %+v", measurement)
				}
			}
		})
	}
}

func checkInputReferences(test *testing.T, state fold.State, history []ledger.Record, stale bool) {
	test.Helper()
	highest := map[string]int64{"ISSUE-001": 2, "ISSUE-002": 1, "RECEIPT-003": 1}
	revisions := map[string]int{"ISSUE-001": 2, "ISSUE-002": 1, "RECEIPT-003": 1}
	if stale {
		highest["ISSUE-001"], highest["RECEIPT-003"] = 4, 3
		revisions["ISSUE-001"], revisions["RECEIPT-003"] = 4, 3
	}
	if len(state.Identities) != 3 {
		test.Fatal("wrong identity count")
	}
	seen := map[ledger.Coordinates]bool{}
	for name, identity := range state.Identities {
		if identity.Highest != highest[name] || len(identity.Revisions) != revisions[name] {
			test.Fatalf("arrival replaced source revision authority: %s %+v", name, identity)
		}
		for revision, candidates := range identity.Revisions {
			if len(candidates) != 1 || len(candidates[0].Sources) == 0 {
				test.Fatal("lost or conflicting canonical candidate")
			}
			candidate := candidates[0]
			for index, source := range candidate.Sources {
				if seen[source] || source.Offset < 0 || source.Offset >= int64(len(history)) || source != history[source.Offset].Coordinates {
					test.Fatalf("lost/misrouted source reference: %+v", source)
				}
				seen[source] = true
				transaction, err := ledger.Decode(history[source.Offset].Value)
				if err != nil || transaction.TxnID != name || transaction.Revision != revision {
					test.Fatalf("candidate refers to another assertion: %+v error=%v", candidate, err)
				}
				if index != 0 {
					transaction.SourceTime = candidate.Transaction.SourceTime
				}
				if !reflect.DeepEqual(transaction, candidate.Transaction) {
					test.Fatal("candidate canonical fields or first informational timestamp changed")
				}
			}
		}
	}
	if len(seen) != len(history) {
		test.Fatal("source progress advanced without complete revision evidence")
	}
}
