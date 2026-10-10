//go:build integration

package showcase

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
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

func TestZombieWorkerDrill(test *testing.T) {
	base := showcaseTestOptions(test)
	for _, name := range []string{"recovered", "cancel-before-replacement", "cancel-after-fence"} {
		test.Run(name, func(test *testing.T) {
			ctx, cancel := context.WithTimeout(test.Context(), 2*time.Minute)
			defer cancel()
			var transcript bytes.Buffer
			cancellation := newCheckpointCancellation(test)
			if name == "cancel-before-replacement" {
				cancellation.point, cancellation.cancel = ZombieReady, cancel
			} else if name == "cancel-after-fence" {
				cancellation.point, cancellation.cancel = ZombieFenced, cancel
			}
			options := base
			options.Scenario, options.Output, options.OnCheckpoint = ZombieWorker, &transcript, cancellation.observe
			report, runErr := Run(ctx, options)
			retainTestRun(test, base, report, &transcript)
			read := func(name string, target any) {
				test.Helper()
				readRunJSON(test, report.Directory, name, target)
			}
			if report.Mode != "zombie-worker" || !report.TwoMembersAssigned || report.Zombie == nil {
				test.Fatalf("missing retained run evidence: %+v error=%v", report, runErr)
			}
			fenced := name != "cancel-before-replacement"
			for _, child := range report.Processes {
				wantExit := 0
				if fenced && (child.Name == "worker-stale" || child.Name == "verify-fenced") {
					wantExit = 1
				}
				checkChildExit(test, child, wantExit)
				if cancellation.cancel != nil && (strings.HasPrefix(child.Name, "worker-recovery-") || child.Name == "relay-after") {
					test.Error("canceled drill continued recovery")
				}
			}
			stages, statuses, sourceEnds, outputEnds := []string{"before", "reowned", "fenced", "after"}, []string{"PASS", "PASS", "INCOMPLETE", "PASS"}, []int64{3, 6, 7, 7}, []int64{5, 5, -1, 10}
			if cancellation.cancel != nil {
				if !errors.Is(runErr, context.Canceled) || report.Status != "ERROR" {
					test.Fatalf("cancellation did not fail the overall drill: %+v error=%v", report, runErr)
				}
				count := 2
				if fenced {
					count = 3
				}
				stages, statuses, sourceEnds, outputEnds = stages[:count], statuses[:count], sourceEnds[:count], outputEnds[:count]
			} else if runErr != nil || report.Status != "PASS" {
				test.Fatalf("zombie recovery failed: %+v error=%v", report, runErr)
			}
			if len(report.Stages) != len(stages) || report.FinalVerification != statuses[len(statuses)-1] {
				test.Fatalf("lost original stage results: %+v", report)
			}
			for index, stage := range report.Stages {
				checks := verify.Checks{WritersStopped: true, SourceScanned: true, WorkersReached: true, PublicationDrained: true, ViewReached: true, BoundariesStable: true}
				if stages[index] == "fenced" {
					checks = verify.Checks{WritersStopped: true, SourceScanned: true}
					if len(stage.OutputEnd) != 0 || !strings.Contains(stage.Reason, "workers have not reached source boundary") || stage.Accepts(nil) {
						test.Fatalf("fenced stage certified an incomplete boundary: %+v", stage)
					}
					for _, key := range stage.Keys {
						if key.Compared || key.Agrees {
							test.Fatal("fenced key was certified")
						}
					}
				} else if !slices.Equal(stage.OutputEnd, []int64{outputEnds[index], 0}) {
					test.Fatalf("unexpected complete output boundary: %+v", stage)
				}
				if stage.Status != statuses[index] || stage.Checks != checks || !slices.Equal(stage.SourceEnd, []int64{sourceEnds[index], 0}) || stage.BinarySHA256 == "" || len(stage.Diagnostics.Rejections) != 0 || len(stage.Diagnostics.Conflicts) != 0 {
					test.Fatalf("wrong stage result: %+v", stage)
				}
				checkInspectionArtifacts(test, report.Directory, stages[index], "zombie-worker")
			}
			evidence := report.Zombie
			if evidence.WorkerPID <= 0 || evidence.MemberID == "" || evidence.Previous.Namespace != report.Namespace || evidence.Previous.Epoch < 1 || evidence.Before.Epoch != evidence.Previous.Epoch || evidence.Before.Next != 6 || evidence.Before.Sequence != 5 {
				test.Fatalf("real worker's saved ownership was not established: %+v", evidence)
			}
			if fenced {
				installed := evidence.Previous
				installed.Epoch++
				partition := evidence.Before
				partition.Epoch++
				if evidence.Installed != installed || evidence.AfterFence != partition || !evidence.WorkerExited || evidence.FencedAttempts != 1 || !evidence.StateUnchanged || !slices.Equal(evidence.OutputEnd, []int64{5, 0}) {
					test.Fatalf("old-epoch rejection or durable preservation not established: %+v", evidence)
				}
				before, err := os.ReadFile(filepath.Join(report.Directory, "zombie-before.json"))
				if err != nil {
					test.Fatal(err)
				}
				after, err := os.ReadFile(filepath.Join(report.Directory, "zombie-after-fence.json"))
				if err != nil || !bytes.Equal(before, after) {
					test.Fatal("fenced worker changed exact durable table snapshots")
				}
				log, err := os.ReadFile(filepath.Join(report.Directory, "worker-stale.log"))
				if err != nil {
					test.Fatal(err)
				}
				if count, err := zombieFenceAttempts(log, report.Namespace, evidence.Previous.Epoch); err != nil || count != 1 {
					test.Fatalf("real worker did not retain exactly one rejected attempt: count=%d error=%v", count, err)
				}
			} else if evidence.Installed.Epoch != 0 || evidence.FencedAttempts != 0 || evidence.WorkerExited {
				test.Fatal("canceled pre-replacement drill installed ownership or attempted stale processing")
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
			frontier, sequence, rows, expectedFile := int64(6), int64(5), 5, "expected-before.json"
			if cancellation.cancel == nil {
				frontier, sequence, rows, expectedFile = 7, 10, 6, "expected-after.json"
				if evidence.Recovered.Epoch <= evidence.Installed.Epoch || snapshot.Partitions[0].Epoch != evidence.Recovered.Epoch {
					test.Fatal("normal workers did not recover under a higher epoch")
				}
			} else if evidence.Recovered.Epoch != 0 {
				test.Fatal("canceled run claimed successful recovery")
			}
			if snapshot.Partitions[0].Next != frontier || snapshot.Partitions[0].Sequence != sequence || int64(len(snapshot.Outbox)) != sequence || len(snapshot.Rejections) != 0 || len(view.Rows) != rows || view.Next[0] != sequence || view.Next[1] != 0 {
				test.Fatalf("invalid durable state after zombie drill: partition=%+v view=%+v", snapshot.Partitions[0], view)
			}
			expected, err := fixtures.Expected(expectedFile)
			if err != nil || !reflect.DeepEqual(view.Projection(manifest.Config.Keys[0].StockKey), expected) {
				test.Fatal("view differs from hand-derived fixture", err)
			}
			if cancellation.cancel == nil {
				for _, expected := range []struct {
					family  ledger.OutputFamily
					day     string
					version int64
					present bool
				}{
					{"balance", "2026-01-01", 2, true}, {"balance", "2026-01-02", 2, true}, {"balance", "2026-01-03", 2, true},
					{"alert", "2026-01-01", 2, false}, {"alert", "2026-01-02", 1, true}, {"key-status", "", 1, true},
				} {
					row := view.Rows[ledger.OutputID{Namespace: report.Namespace, Family: expected.family, Key: manifest.Config.Keys[0].StockKey, Day: expected.day}]
					if row.Version != expected.version || row.Payload.Present != expected.present || !row.Equal(snapshot.Outputs[row.ID]) {
						test.Fatalf("stale attempt allocated extra versions or lost a tombstone: %+v", row)
					}
				}
			}
			fixtureNames := []string{"before.jsonl", "before.jsonl"}
			if fenced {
				fixtureNames = append(fixtureNames, "correction.jsonl")
			}
			history, err := fixtures.Records(manifest.Config, fixtureNames...)
			if err != nil {
				test.Fatal(err)
			}
			for index := range history {
				history[index].RawKey = broker.EncodeKey(history[index].Key)
			}
			actual, err := broker.Scan(inspectionCtx, base.Brokers, manifest, []int64{int64(len(history)), 0})
			if err != nil || !reflect.DeepEqual(actual, history) {
				test.Fatal("retained source differs from declared repeated baseline and correction", err)
			}
			progress, err := state.Progress(inspectionCtx)
			if err != nil || len(progress.Partitions) != 2 || progress.Partitions[0].LastProcessing == nil {
				test.Fatal("lost committed processing measurement", err)
			}
			measurement := progress.Partitions[0].LastProcessing
			work, outputs := fold.Work{Duplicate: true}, int64(0)
			if cancellation.cancel == nil {
				work, outputs = fold.Work{Refold: true, FromDay: "2026-01-01", RewindDays: 2, RefoldedRecords: 3}, 5
			}
			if measurement.Source.Offset != frontier-1 || measurement.Work != work || measurement.BusinessOutputs != outputs || measurement.StatusOutputs != 0 {
				test.Fatalf("failed stale attempt changed committed work or recovery repeated it: %+v", measurement)
			}
		})
	}
}
