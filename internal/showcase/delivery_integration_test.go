//go:build integration

package showcase

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/sorrell-consulting/amends/internal/verify"
)

func TestDeliveryDrills(test *testing.T) {
	base := showcaseTestOptions(test)
	for _, name := range []string{"relay-stall", "disappearing-day", "materializer-crash", "cancel-stalled", "cancel-withdrawn", "cancel-materializer-crashed", "cancel-materializer-committed"} {
		test.Run(name, func(test *testing.T) {
			ctx, cancel := context.WithTimeout(test.Context(), 2*time.Minute)
			defer cancel()
			var transcript bytes.Buffer
			cancellation := newCheckpointCancellation(test)
			drill := name
			if name == "cancel-stalled" {
				drill, cancellation.point, cancellation.cancel = "relay-stall", RelayStalled, cancel
			} else if name == "cancel-withdrawn" {
				drill, cancellation.point, cancellation.cancel = "disappearing-day", WithdrawalVerified, cancel
			} else if name == "cancel-materializer-crashed" {
				drill, cancellation.point, cancellation.cancel = "materializer-crash", MaterializerKilled, cancel
			} else if name == "cancel-materializer-committed" {
				drill, cancellation.point, cancellation.cancel = "materializer-crash", MaterializerCommitKilled, cancel
			}
			options := base
			options.Scenario, options.Output, options.OnCheckpoint = Scenario(drill), &transcript, cancellation.observe
			report, err := Run(ctx, options)
			retainTestRun(test, base, report, &transcript)
			read := func(name string, target any) {
				test.Helper()
				readRunJSON(test, report.Directory, name, target)
			}
			for _, child := range report.Processes {
				wantExit := 0
				if child.Name == "verify-stalled" || child.Name == "verify-materializer-crashed" {
					wantExit = 1
				}
				if drill == "materializer-crash" && (child.Name == "view" || child.Name == "view-recovered") {
					wantExit = -1
				}
				checkChildExit(test, child, wantExit)
				if name == "cancel-stalled" && child.Name == "relay-after" {
					test.Fatal("canceled stall restarted delivery")
				}
				if name == "cancel-withdrawn" && child.Name == "versions-withdrawn" {
					test.Fatal("canceled withdrawal continued the redelivery schedule")
				}
			}
			if drill == "materializer-crash" {
				checkMaterializerCrash(test, report, name)
			} else if report.MaterializerCrash != nil {
				test.Fatal("non-crash drill recorded a materializer crash")
			}
			if cancellation.cancel != nil {
				if _, statErr := os.Stat(filepath.Join(report.Directory, "redelivery.json")); !os.IsNotExist(statErr) {
					test.Fatalf("canceled delivery recorded a later redelivery: %v", statErr)
				}
				lastStatus, stages := "INCOMPLETE", 2
				if name == "cancel-withdrawn" {
					lastStatus, stages = "PASS", 3
				} else if name == "cancel-materializer-crashed" {
					stages = 3
				} else if name == "cancel-materializer-committed" {
					lastStatus, stages = "PASS", 4
				}
				if err == nil || report.Status != "ERROR" || report.FinalVerification != lastStatus || len(report.Stages) != stages {
					test.Fatalf("cancellation hid last stage or reported success: %+v error=%v", report, err)
				}
				return
			}
			if err != nil {
				test.Fatal(err)
			}
			if report.Status != "PASS" || report.FinalVerification != "PASS" || report.Mode != name || !report.TwoMembersAssigned {
				test.Fatalf("incomplete run: %+v", report)
			}
			sourceEnds, outputEnds := []int64{3, 4, 4}, []int64{5, -1, 10}
			if name == "disappearing-day" {
				sourceEnds, outputEnds = []int64{3, 4, 5, 5}, []int64{5, 10, 11, 12}
			} else if name == "materializer-crash" {
				sourceEnds, outputEnds = []int64{3, 4, 5, 5, 5}, []int64{5, 10, 11, 11, 12}
			}
			if len(report.Stages) != len(sourceEnds) {
				test.Fatalf("wrong number of stages: %+v", report.Stages)
			}
			for index, stage := range report.Stages {
				status := "PASS"
				checks := verify.Checks{WritersStopped: true, SourceScanned: true, WorkersReached: true, PublicationDrained: true, ViewReached: true, BoundariesStable: true}
				if outputEnds[index] == -1 {
					status = "INCOMPLETE"
					checks = verify.Checks{WritersStopped: true, SourceScanned: true, WorkersReached: true}
					if len(stage.OutputEnd) != 0 || !strings.Contains(stage.Reason, "pending outbox") {
						test.Fatal("stall invented a complete output boundary")
					}
					for _, key := range stage.Keys {
						if key.Compared || key.Agrees {
							test.Fatal("stall certified an incomparable view")
						}
					}
				} else if !slices.Equal(stage.OutputEnd, []int64{outputEnds[index], 0}) {
					test.Fatalf("wrong output boundary: %+v", stage)
				}
				if name == "materializer-crash" && index == 2 {
					status = "INCOMPLETE"
					checks = verify.Checks{WritersStopped: true, SourceScanned: true, WorkersReached: true, PublicationDrained: true}
				}
				if stage.Status != status || stage.Checks != checks || !slices.Equal(stage.SourceEnd, []int64{sourceEnds[index], 0}) || stage.BinarySHA256 == "" || len(stage.Differences) != 0 {
					test.Fatalf("wrong verification status/boundary: %+v", stage)
				}
			}
			var manifest broker.Manifest
			read("manifest.json", &manifest)
			pool, err := pgxpool.New(ctx, base.DatabaseURL)
			if err != nil {
				test.Fatal(err)
			}
			defer pool.Close()
			state, err := postgres.New(pool, manifest.Config)
			if err != nil {
				test.Fatal(err)
			}
			view, err := state.View(ctx)
			if err != nil {
				test.Fatal(err)
			}
			expectedFile, sourceEnd, sequence, dayVersion, dayPresent := "expected-after.json", int64(4), int64(10), int64(2), true
			if name == "disappearing-day" || name == "materializer-crash" {
				expectedFile, sourceEnd, sequence, dayVersion, dayPresent = "expected-after-cancel.json", 5, 11, 3, false
			}
			expected, err := fixtures.Expected(expectedFile)
			if err != nil || !reflect.DeepEqual(view.Projection(manifest.Config.Keys[0].StockKey), expected) {
				test.Fatalf("view differs from hand-derived fixture: %+v error=%v", view, err)
			}
			if len(view.Rows) != 6 {
				test.Fatal("lost retained output guards")
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
				if row.Version != expected.version || row.Payload.Present != expected.present {
					test.Fatalf("wrong output guard: %+v want %+v", row, expected)
				}
			}
			snapshot, err := state.Snapshot(ctx)
			if err != nil || snapshot.Partitions[0].Next != sourceEnd || snapshot.Partitions[0].Sequence != sequence || int64(len(snapshot.Outbox)) != sequence {
				test.Fatalf("retry changed semantic transitions/progress: %+v error=%v", snapshot, err)
			}
			if name == "relay-stall" {
				var evidence stallEvidence
				read("stall.json", &evidence)
				for _, progress := range []postgres.Progress{evidence.Before, evidence.After, report.Stages[1].Progress} {
					partition := progress.Partitions[0]
					if partition.SourceNext != 4 || partition.ViewNext != 5 || partition.Pending != 5 || partition.EmissionSequence != 10 || partition.OldestPending == nil || partition.LastReceipt == nil || *partition.LastReceipt != 4 {
						test.Fatalf("stall not observed: %+v", progress)
					}
				}
				if !evidence.ViewUnchanged || !slices.Equal(evidence.OutputEnd, []int64{5, 0}) || !evidence.Before.Partitions[0].OldestPending.Equal(*evidence.After.Partitions[0].OldestPending) || !evidence.After.ObservedAt.After(evidence.Before.ObservedAt) {
					test.Fatal("stall did not preserve output/view and aging pending intent")
				}
				checkInspectionArtifacts(test, report.Directory, "stalled", name)
				return
			}
			var evidence redeliveryEvidence
			read("redelivery.json", &evidence)
			if evidence.Sequence != 10 || evidence.Offset != 11 || evidence.Tombstone.Version != 3 || evidence.Tombstone.Payload.Present || !evidence.ProcessingUnchanged || !evidence.ViewRowsUnchanged {
				test.Fatalf("redelivery assertions missing: %+v", evidence)
			}
			var committed []byte
			namespace, err := json.Marshal(report.Namespace)
			if err != nil {
				test.Fatal(err)
			}
			if err = pool.QueryRow(ctx, "SELECT envelope FROM amends_outbox WHERE namespace_id=$1 AND partition=0 AND sequence=10", string(namespace)).Scan(&committed); err != nil {
				test.Fatal(err)
			}
			outputManifest := manifest
			outputManifest.Topic, outputManifest.Config.Source = manifest.Output.Topic, manifest.Output.Incarnation
			records, err := broker.Scan(ctx, base.Brokers, outputManifest, []int64{12, 0})
			if err != nil || len(records) != 12 {
				test.Fatalf("cannot check exact broker redelivery: %d records, error=%v", len(records), err)
			}
			if !bytes.Equal(committed, evidence.Bytes) || !bytes.Equal(records[9].Value, committed) || !bytes.Equal(records[11].Value, committed) || !bytes.Equal(records[11].RawKey, records[9].RawKey) || !bytes.Equal(records[11].RawKey, evidence.Key) {
				test.Fatal("redelivery did not preserve exact committed wire bytes/key")
			}
			for _, stage := range []string{"withdrawn", "redelivered"} {
				checkInspectionArtifacts(test, report.Directory, stage, name)
				if _, err = os.Stat(filepath.Join(report.Directory, "versions-"+stage+".log")); err != nil {
					test.Fatal(err)
				}
			}
		})
	}
}
