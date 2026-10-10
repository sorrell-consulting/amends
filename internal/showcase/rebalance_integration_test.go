//go:build integration

package showcase

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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

func TestRebalanceUnderLoad(test *testing.T) {
	base := showcaseTestOptions(test)
	for _, outcome := range []string{"recovered", "cancel-ready", "cancel-preparing", "cancel-reassigned"} {
		test.Run(outcome, func(test *testing.T) {
			ctx, cancel := context.WithTimeout(test.Context(), 2*time.Minute)
			defer cancel()
			var transcript bytes.Buffer
			cancellation := newCheckpointCancellation(test)
			switch outcome {
			case "cancel-ready":
				cancellation.point, cancellation.cancel = RebalanceReady, cancel
			case "cancel-preparing":
				cancellation.point, cancellation.cancel = RebalancePreparing, cancel
			case "cancel-reassigned":
				cancellation.point, cancellation.cancel = RebalanceReassigned, cancel
			}
			options := base
			options.Scenario, options.Output, options.OnCheckpoint = RebalanceUnderLoad, &transcript, cancellation.observe
			report, runErr := Run(ctx, options)
			retainTestRun(test, base, report, &transcript)
			read := func(name string, target any) {
				test.Helper()
				readRunJSON(test, report.Directory, name, target)
			}
			if report.Mode != "rebalance-under-load" || report.Rebalance == nil {
				test.Fatalf("lost retained run evidence: %+v error=%v", report, runErr)
			}
			canceled, loaded := outcome != "recovered", outcome != "cancel-ready"
			reassigned := outcome == "recovered" || outcome == "cancel-reassigned"
			if canceled {
				if !errors.Is(runErr, context.Canceled) || report.Status != "ERROR" {
					test.Fatalf("canceled drill did not retain ERROR: %+v error=%v", report, runErr)
				}
			} else if runErr != nil || report.Status != "PASS" {
				test.Fatalf("rebalance failed: %+v error=%v", report, runErr)
			}
			for _, child := range report.Processes {
				wantExit := 0
				if child.Name == "verify-preparing" || child.Name == "verify-reassigned" {
					wantExit = 1
				}
				checkChildExit(test, child, wantExit)
				if (canceled && child.Name == "relay-after") || (!loaded && child.Name == "load-rebalance-burst") {
					test.Fatal("cancellation continued the remaining schedule")
				}
			}
			stages, statuses := []string{"before", "preparing", "reassigned", "after"}, []string{"PASS", "INCOMPLETE", "INCOMPLETE", "PASS"}
			count := 4
			switch outcome {
			case "cancel-ready":
				count = 1
			case "cancel-preparing":
				count = 2
			case "cancel-reassigned":
				count = 3
			}
			stages, statuses = stages[:count], statuses[:count]
			if len(report.Stages) != count || report.FinalVerification != statuses[count-1] {
				test.Fatalf("lost original verification status: %+v", report)
			}
			for index, stage := range report.Stages {
				checks := verify.Checks{WritersStopped: true, SourceScanned: true, WorkersReached: true, PublicationDrained: true, ViewReached: true, BoundariesStable: true}
				sourceEnd, outputEnd := []int64{35, 35}, []int64{10, 10}
				if index == 0 {
					sourceEnd, outputEnd = []int64{3, 3}, []int64{5, 5}
				} else if stages[index] != "after" {
					checks = verify.Checks{WritersStopped: true, SourceScanned: true}
					outputEnd = nil
					if stage.Accepts(nil) {
						test.Fatal("incomplete rebalance was accepted")
					}
					for _, key := range stage.Keys {
						if key.Compared || key.Agrees {
							test.Fatal("incomplete key was certified")
						}
					}
				}
				if stage.Status != statuses[index] || stage.Checks != checks || !slices.Equal(stage.SourceEnd, sourceEnd) || !slices.Equal(stage.OutputEnd, outputEnd) || len(stage.Diagnostics.Rejections) != 0 || len(stage.Diagnostics.Conflicts) != 0 {
					test.Fatalf("incorrect complete-boundary checks: %+v", stage)
				}
				checkInspectionArtifacts(test, report.Directory, stages[index], "rebalance-under-load")
			}
			evidence := report.Rebalance
			if evidence.Released != !canceled || evidence.Completed != !canceled || report.TwoMembersAssigned != reassigned || evidence.PreparingUnchanged != loaded || len(evidence.Baseline) != 2 || !slices.Equal(evidence.ExpectedSourceEnd, []int64{35, 35}) {
				test.Fatalf("lost rebalance schedule evidence: %+v", evidence)
			}
			wantWorkers := 2
			if !loaded {
				wantWorkers = 1
			}
			if len(evidence.Workers) != wantWorkers {
				test.Fatal("wrong owned worker count")
			}
			if loaded {
				if evidence.Preparing.State != "PreparingRebalance" || evidence.Held.Expected != 3 || evidence.Held.Record.Offset != 3 || evidence.Held.Token.Epoch != evidence.Baseline[evidence.Held.Record.Partition].Epoch {
					test.Fatal("rebalance was not observed while the old polled decision was held")
				}
				before, firstErr := os.ReadFile(filepath.Join(report.Directory, "rebalance-before.json"))
				after, secondErr := os.ReadFile(filepath.Join(report.Directory, "rebalance-preparing.json"))
				if firstErr != nil || secondErr != nil || !bytes.Equal(before, after) {
					test.Fatal("held rebalance changed exact durable state", firstErr, secondErr)
				}
			}
			if reassigned {
				if evidence.Reassigned.State != "Stable" || len(evidence.Reassigned.Members) != 2 || len(evidence.Successors) != 2 {
					test.Fatal("missing actual two-member assignment")
				}
				var assigned []int
				for _, partitions := range evidence.Reassigned.Members {
					if len(partitions) != 1 {
						test.Fatal("two-member assignment left one member idle")
					}
					assigned = append(assigned, partitions...)
				}
				slices.Sort(assigned)
				if !slices.Equal(assigned, []int{0, 1}) || evidence.Successors[0].Worker == evidence.Successors[1].Worker {
					test.Fatal("both workers did not receive populated partitions")
				}
				for _, attempt := range evidence.Successors {
					if attempt.Expected != attempt.Record.Offset || attempt.Expected < 3 || attempt.Expected >= 35 || attempt.Token.Epoch <= evidence.Baseline[attempt.Token.Partition].Epoch {
						test.Fatal("reassignment did not retain a valid durable frontier with backlog")
					}
				}
			}
			burst, err := os.ReadFile(filepath.Join(report.Directory, "rebalance-burst.jsonl"))
			if err != nil || bytes.Count(burst, []byte("\n")) != 64 {
				test.Fatal("lost finite burst", err)
			}
			digest := sha256.Sum256(burst)
			if report.SHA256["rebalance-burst.jsonl"] != hex.EncodeToString(digest[:]) {
				test.Fatal("finite burst hash mismatch")
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
			view, err := state.View(inspectionCtx)
			if err != nil {
				test.Fatal(err)
			}
			history := rebalanceFixtureRecords(test, manifest.Config, loaded)
			sourceEnd := int64(3)
			if loaded {
				sourceEnd = 35
			}
			actual, err := broker.Scan(inspectionCtx, base.Brokers, manifest, []int64{sourceEnd, sourceEnd})
			slices.SortFunc(actual, func(left, right ledger.Record) int {
				if order := cmp.Compare(left.Partition, right.Partition); order != 0 {
					return order
				}
				return cmp.Compare(left.Offset, right.Offset)
			})
			if err != nil || !reflect.DeepEqual(actual, history) {
				test.Fatal("complete retained broker input differs from the declared two-key fixture", err)
			}
			decisions := map[ledger.Coordinates]RebalanceAttempt{}
			for _, actor := range evidence.Workers {
				if !actor.Joined {
					test.Fatal("source lifecycle was not joined")
				}
				for _, attempt := range actor.Committed {
					_, duplicate := decisions[attempt.Record]
					if duplicate || attempt.Worker != actor.Name || attempt.Expected != attempt.Record.Offset || !slices.Contains(actor.Claims, attempt.Token) || attempt.Token.Partition != attempt.Record.Partition || attempt.Token.Namespace != report.Namespace || attempt.Token.Digest != manifest.Config.Digest() {
						test.Fatal("duplicate, unclaimed, or misattributed acknowledged decision")
					}
					decisions[attempt.Record] = attempt
				}
			}
			progress, err := state.Progress(inspectionCtx)
			if err != nil || len(progress.Partitions) != 2 {
				test.Fatal("missing populated partition observations", err)
			}
			var totalNext, totalOutput int64
			for _, key := range manifest.Config.Keys {
				partition := snapshot.Partitions[key.Partition]
				if (!loaded && partition.Next != 3) || (!reassigned && partition.Next != 3) || (!canceled && partition.Next != 35) {
					test.Fatalf("incorrect source frontier after schedule: %+v", partition)
				}
				if outcome == "cancel-reassigned" {
					for _, attempt := range evidence.Successors {
						if attempt.Record.Partition == key.Partition && (partition.Next != attempt.Expected || partition.Epoch != attempt.Token.Epoch) {
							test.Fatal("canceled reassignment performed gated work")
						}
					}
				}
				stage, sequence := "before", int64(5)
				if partition.Next > 3 {
					stage, sequence = "after", 10
				}
				prefix := "expected-"
				if key.Partition == 1 {
					prefix = "expected-rebalance-other-"
				}
				expected, err := fixtures.Expected(prefix + stage + ".json")
				if err != nil || partition.Sequence != sequence || !reflect.DeepEqual(snapshot.Keys[key.StockKey].Projection, expected) {
					test.Fatal("processing differs from hand-derived fixture", err)
				}
				viewStage, viewNext := "before", int64(5)
				if !canceled {
					viewStage, viewNext = "after", 10
				}
				expected, err = fixtures.Expected(prefix + viewStage + ".json")
				if err != nil || view.Next[key.Partition] != viewNext || !reflect.DeepEqual(view.Projection(key.StockKey), expected) {
					test.Fatal("view differs from declared delivery boundary", err)
				}
				lastEpoch := int64(0)
				for offset := int64(0); offset < partition.Next; offset++ {
					coordinate := ledger.Coordinates{Source: manifest.Config.Source, Partition: key.Partition, Offset: offset}
					attempt, ok := decisions[coordinate]
					if !ok || attempt.Token.Epoch < lastEpoch {
						test.Fatal("ownership regressed or a committed source decision is missing")
					}
					lastEpoch = attempt.Token.Epoch
				}
				if partition.Next > 3 {
					start := int64(key.Partition) * sourceEnd
					checkInputReferences(test, snapshot.Keys[key.StockKey], history[start:start+partition.Next], false)
				}
				measurement := progress.Partitions[key.Partition].LastProcessing
				if measurement == nil || measurement.Source.Offset != partition.Next-1 || (!canceled && (measurement.Work != (fold.Work{Duplicate: true}) || measurement.BusinessOutputs != 0 || measurement.StatusOutputs != 0)) {
					test.Fatal("lost per-partition committed no-op measurement")
				}
				totalNext += partition.Next
				totalOutput += sequence
			}
			if int64(len(decisions)) != totalNext || int64(len(snapshot.Outbox)) != totalOutput || len(snapshot.Rejections) != 0 {
				test.Fatal("progress disagrees with source decisions or output intent")
			}
			if !canceled {
				for _, key := range manifest.Config.Keys {
					for _, expected := range []struct {
						family  ledger.OutputFamily
						day     string
						version int64
						present bool
					}{
						{"balance", "2026-01-01", 2, true}, {"balance", "2026-01-02", 2, true}, {"balance", "2026-01-03", 2, true},
						{"alert", "2026-01-01", 2, false}, {"alert", "2026-01-02", 1, true}, {"key-status", "", 1, true},
					} {
						row := view.Rows[ledger.OutputID{Namespace: report.Namespace, Family: expected.family, Key: key.StockKey, Day: expected.day}]
						if row.Version != expected.version || row.Payload.Present != expected.present || !row.Equal(snapshot.Outputs[row.ID]) {
							test.Fatal("rebalance allocated extra versions or lost an alert withdrawal")
						}
					}
				}
			}
		})
	}
}
