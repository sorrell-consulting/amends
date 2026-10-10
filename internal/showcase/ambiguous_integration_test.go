//go:build integration

package showcase

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/sorrell-consulting/amends/internal/verify"
	"github.com/sorrell-consulting/amends/internal/worker"
)

func TestAmbiguousCommitDrill(test *testing.T) {
	base := showcaseTestOptions(test)
	for _, mode := range []string{"request", "reply"} {
		for _, outcome := range []string{"recovered", "cancel-ready", "cancel-unknown"} {
			test.Run(mode+"/"+outcome, func(test *testing.T) {
				ctx, cancel := context.WithTimeout(test.Context(), 2*time.Minute)
				defer cancel()
				var transcript bytes.Buffer
				cancellation := newCheckpointCancellation(test)
				if outcome == "cancel-ready" {
					cancellation.point, cancellation.cancel = CommitReady, cancel
				} else if outcome == "cancel-unknown" {
					cancellation.point, cancellation.cancel = CommitUnknown, cancel
				}
				options := base
				options.Scenario, options.Output, options.OnCheckpoint = Scenario("ambiguous-commit-"+mode), &transcript, cancellation.observe
				report, runErr := Run(ctx, options)
				retainTestRun(test, base, report, &transcript)
				read := func(name string, target any) {
					test.Helper()
					readRunJSON(test, report.Directory, name, target)
				}
				if report.Mode != "ambiguous-commit-"+mode || report.Ambiguous == nil {
					test.Fatalf("lost retained evidence: %+v error=%v", report, runErr)
				}
				canceled, faulted := cancellation.cancel != nil, outcome != "cancel-ready"
				if canceled {
					if !errors.Is(runErr, context.Canceled) || report.Status != "ERROR" {
						test.Fatalf("canceled drill claimed success: %+v error=%v", report, runErr)
					}
				} else if runErr != nil || report.Status != "PASS" {
					test.Fatalf("recovery failed: %+v error=%v", report, runErr)
				}
				for _, child := range report.Processes {
					wantExit := 0
					if child.Name == "verify-commit-unknown" {
						wantExit = 1
					}
					checkChildExit(test, child, wantExit)
					if (canceled && child.Name == "relay-after") || (!faulted && child.Name == "load-correction") {
						test.Fatal("cancellation did not stop the remaining schedule")
					}
				}
				stages, statuses := []string{"before", "commit-unknown", "after"}, []string{"PASS", "INCOMPLETE", "PASS"}
				if outcome == "cancel-ready" {
					stages, statuses = stages[:1], statuses[:1]
				} else if canceled {
					stages, statuses = stages[:2], statuses[:2]
				}
				if len(report.Stages) != len(stages) || report.FinalVerification != statuses[len(statuses)-1] {
					test.Fatalf("lost stage status: %+v", report)
				}
				for index, stage := range report.Stages {
					checks := verify.Checks{WritersStopped: true, SourceScanned: true, WorkersReached: true, PublicationDrained: true, ViewReached: true, BoundariesStable: true}
					sourceEnd, outputEnd := []int64{4, 0}, []int64{10, 0}
					if index == 0 {
						sourceEnd, outputEnd = []int64{3, 0}, []int64{5, 0}
					} else if index == 1 {
						outputEnd = nil
						checks = verify.Checks{WritersStopped: true, SourceScanned: true, WorkersReached: mode == "reply"}
						if stage.Accepts(nil) {
							test.Fatal("incomplete commit stage was accepted")
						}
						for _, key := range stage.Keys {
							if key.Compared || key.Agrees {
								test.Fatal("incomplete key was certified")
							}
						}
					}
					if stage.Status != statuses[index] || stage.Checks != checks || !slices.Equal(stage.SourceEnd, sourceEnd) || !slices.Equal(stage.OutputEnd, outputEnd) || len(stage.Diagnostics.Rejections) != 0 || len(stage.Diagnostics.Conflicts) != 0 {
						test.Fatalf("wrong verification boundary: %+v", stage)
					}
					checkInspectionArtifacts(test, report.Directory, stages[index], "ambiguous-commit")
				}
				evidence := report.Ambiguous
				if !evidence.Joined || evidence.Loss != mode || evidence.FaultFired != faulted || evidence.RecoveryReleased != !canceled || evidence.Recovered != !canceled {
					test.Fatalf("incorrect fault/lifecycle evidence: %+v", evidence)
				}
				if faulted {
					next, sequence := int64(3), int64(5)
					if mode == "reply" {
						next, sequence = 4, 10
					}
					if evidence.ServerCommitObserved != (mode == "reply") || evidence.Connections < 2 || evidence.BackendBefore <= 0 || evidence.BackendAfter <= 0 || evidence.BackendBefore == evidence.BackendAfter || !strings.Contains(evidence.OriginalError, store.ErrCommitUnknown.Error()) || evidence.Token.Namespace != report.Namespace || evidence.Token.Epoch <= 1 || evidence.Observed.Next != next || evidence.Observed.Sequence != sequence || evidence.Observed.Epoch != evidence.Token.Epoch {
						test.Fatalf("lost request/reply or fresh recovery read not proven: %+v", evidence)
					}
					wantOutcomes := []string{"unknown"}
					if !canceled && mode == "request" {
						wantOutcomes = append(wantOutcomes, "acknowledged")
					}
					if !slices.Equal(evidence.AttemptOutcomes, wantOutcomes) {
						test.Fatalf("unknown was relabeled or retried incorrectly: %+v", evidence)
					}
					log, err := os.ReadFile(filepath.Join(report.Directory, "worker-ambiguous.log"))
					if err != nil {
						test.Fatal(err)
					}
					var actualOutcomes []string
					for _, line := range bytes.Split(bytes.TrimSpace(log), []byte("\n")) {
						var event struct {
							Event     string `json:"event"`
							Namespace string `json:"namespace"`
							Partition int    `json:"partition"`
							Offset    int64  `json:"offset"`
							Epoch     int64  `json:"epoch"`
							Outcome   string `json:"outcome"`
							Elapsed   *int64 `json:"storage_call_elapsed_ns"`
						}
						if err = json.Unmarshal(line, &event); err != nil {
							test.Fatal(err)
						}
						if event.Event != worker.ProcessingAttemptEvent {
							continue
						}
						if event.Namespace != report.Namespace || event.Partition != 0 || event.Offset != 3 || event.Epoch != evidence.Token.Epoch || event.Elapsed == nil || *event.Elapsed < 0 {
							test.Fatalf("attempt lost original attribution/timing: %s", line)
						}
						actualOutcomes = append(actualOutcomes, event.Outcome)
					}
					if !slices.Equal(actualOutcomes, wantOutcomes) {
						test.Fatal("retained logs differ from the declared attempt sequence", actualOutcomes)
					}
					first, second := "commit-before.json", "commit-observed.json"
					if mode == "reply" {
						first, second = "commit-observed.json", "commit-recovered.json"
					}
					if mode == "request" || !canceled {
						before, firstErr := os.ReadFile(filepath.Join(report.Directory, first))
						after, secondErr := os.ReadFile(filepath.Join(report.Directory, second))
						if firstErr != nil || secondErr != nil || !bytes.Equal(before, after) {
							test.Fatal("partial aborted writes or repeated committed transition", firstErr, secondErr)
						}
					}
				} else if evidence.ServerCommitObserved || len(evidence.AttemptOutcomes) != 0 {
					test.Fatal("canceled before fault but still attempted processing")
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
				corrected := !canceled || (faulted && mode == "reply")
				frontier, sequence, fixture := int64(3), int64(5), "expected-before.json"
				if corrected {
					frontier, sequence, fixture = 4, 10, "expected-after.json"
				}
				expected, err := fixtures.Expected(fixture)
				key := manifest.Config.Keys[0].StockKey
				if err != nil || snapshot.Partitions[0].Next != frontier || snapshot.Partitions[0].Sequence != sequence || int64(len(snapshot.Outbox)) != sequence || len(snapshot.Rejections) != 0 || !reflect.DeepEqual(snapshot.Keys[key].Projection, expected) {
					test.Fatal("processing differs from hand-derived actual commit outcome", err)
				}
				viewNext, viewFixture := int64(5), "expected-before.json"
				if !canceled {
					viewNext, viewFixture = 10, "expected-after.json"
				}
				expected, err = fixtures.Expected(viewFixture)
				if err != nil || view.Next[0] != viewNext || view.Next[1] != 0 || !reflect.DeepEqual(view.Projection(key), expected) {
					test.Fatal("view differs from hand-derived delivery boundary", err)
				}
				if corrected {
					for _, expected := range []struct {
						family  ledger.OutputFamily
						day     string
						version int64
						present bool
					}{
						{"balance", "2026-01-01", 2, true}, {"balance", "2026-01-02", 2, true}, {"balance", "2026-01-03", 2, true},
						{"alert", "2026-01-01", 2, false}, {"alert", "2026-01-02", 1, true}, {"key-status", "", 1, true},
					} {
						row := snapshot.Outputs[ledger.OutputID{Namespace: report.Namespace, Family: expected.family, Key: key, Day: expected.day}]
						if row.Version != expected.version || row.Payload.Present != expected.present || (!canceled && !row.Equal(view.Rows[row.ID])) {
							test.Fatalf("recovery duplicated versions or lost a withdrawal: %+v", row)
						}
					}
				}
				files := []string{"before.jsonl"}
				if faulted {
					files = append(files, "correction.jsonl")
				}
				history, err := fixtures.Records(manifest.Config, files...)
				if err != nil {
					test.Fatal(err)
				}
				for index := range history {
					history[index].RawKey = broker.EncodeKey(history[index].Key)
				}
				actual, err := broker.Scan(inspectionCtx, base.Brokers, manifest, []int64{int64(len(history)), 0})
				if err != nil || !reflect.DeepEqual(actual, history) {
					test.Fatal("retained source differs from original declared fixture", err)
				}
				progress, err := state.Progress(inspectionCtx)
				if err != nil || len(progress.Partitions) != 2 || progress.Partitions[0].LastProcessing == nil {
					test.Fatal("missing processing measurement", err)
				}
				measurement := progress.Partitions[0].LastProcessing
				if measurement.Source.Offset != frontier-1 || (corrected && (measurement.Work != (fold.Work{Refold: true, FromDay: "2026-01-01", RewindDays: 2, RefoldedRecords: 3}) || measurement.BusinessOutputs != 5 || measurement.StatusOutputs != 0)) {
					test.Fatalf("unknown correction duplicated or lost its committed sample: %+v", measurement)
				}
			})
		}
	}
}
