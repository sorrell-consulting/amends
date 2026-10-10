//go:build integration

package showcase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/admin"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/sorrell-consulting/amends/internal/verify"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestHistoryMissingDrill(test *testing.T) {
	base := showcaseTestOptions(test)
	for _, name := range []string{"refused", "cancel-before-removal", "cancel-after-incomplete"} {
		test.Run(name, func(test *testing.T) {
			ctx, cancel := context.WithTimeout(test.Context(), 2*time.Minute)
			defer cancel()
			var transcript bytes.Buffer
			cancellation := newCheckpointCancellation(test)
			if name == "cancel-before-removal" {
				cancellation.point, cancellation.cancel = HistoryReady, cancel
			} else if name == "cancel-after-incomplete" {
				cancellation.point, cancellation.cancel = HistoryLost, cancel
			}
			options := base
			options.Scenario, options.Output, options.OnCheckpoint = HistoryMissing, &transcript, cancellation.observe
			report, runErr := Run(ctx, options)
			retainTestRun(test, base, report, &transcript)
			read := func(name string, target any) {
				test.Helper()
				readRunJSON(test, report.Directory, name, target)
			}
			if report.Mode != "history-missing" || !report.TwoMembersAssigned {
				test.Fatal("missing or inconsistent retained run evidence")
			}
			for _, child := range report.Processes {
				exitCode := 0
				if child.Name == "verify-history-missing" {
					exitCode = 1
				}
				checkChildExit(test, child, exitCode)
			}
			if cancellation.cancel != nil {
				if !errors.Is(runErr, context.Canceled) || report.Status != "ERROR" {
					test.Fatalf("cancellation was hidden: %+v error=%v", report, runErr)
				}
			} else if runErr != nil || report.Status != "PASS" {
				test.Fatalf("missing-history assertions failed: %+v error=%v", report, runErr)
			}
			removed := name != "cancel-before-removal"
			wantStages, wantFinal, retainedStart := 2, "PASS", []int64{0, 0}
			if removed {
				wantStages, wantFinal, retainedStart = 3, "INCOMPLETE", []int64{2, 0}
			}
			if len(report.Stages) != wantStages || report.FinalVerification != wantFinal {
				test.Fatalf("lost original verification statuses: %+v", report)
			}
			for index, stage := range report.Stages[:2] {
				if stage.Status != "PASS" || !slices.Equal(stage.SourceEnd, []int64{int64(index + 3), 0}) || !slices.Equal(stage.OutputEnd, []int64{int64((index + 1) * 5), 0}) {
					test.Fatalf("baseline was not certified at the expected boundary: %+v", stage)
				}
			}
			if removed {
				stage := report.Stages[2]
				if stage.Status != "INCOMPLETE" || stage.Checks != (verify.Checks{WritersStopped: true}) || stage.AuthorityBasis != "worker-observation" || stage.BinarySHA256 == "" || len(stage.SourceStart) != 0 || len(stage.SourceEnd) != 0 || len(stage.OutputEnd) != 0 || stage.SourceRecords != 0 || !strings.Contains(stage.Reason, "required history missing: partition 0 starts at 2") || stage.Accepts(nil) {
					test.Fatalf("missing-history verification certified an invalid boundary: %+v", stage)
				}
				for _, key := range stage.Keys {
					if key.Compared || key.Agrees || key.ViewStatus != "READY" {
						test.Fatalf("intact READY view was confused with certification: %+v", key)
					}
				}
				checkInspectionArtifacts(test, report.Directory, "history-missing", "history-missing")
				var inspection struct {
					Errors     []string `json:"observation_errors"`
					Partitions []struct {
						SourceNext int64  `json:"source_next"`
						SourceEnd  *int64 `json:"observed_source_end"`
						InputLag   *int64 `json:"input_offset_distance"`
						OutputEnd  *int64 `json:"observed_output_end"`
						ViewLag    *int64 `json:"view_offset_distance"`
					} `json:"partitions"`
				}
				read("inspect-history-missing.log", &inspection)
				if len(inspection.Errors) != 1 || !strings.Contains(inspection.Errors[0], "required history missing") || len(inspection.Partitions) != 2 {
					test.Fatalf("inspection hid unavailable history: %+v", inspection)
				}
				for index, partition := range inspection.Partitions {
					sourceNext, outputEnd := int64(4), int64(10)
					if index == 1 {
						sourceNext, outputEnd = 0, 0
					}
					if partition.SourceNext != sourceNext || partition.SourceEnd != nil || partition.InputLag != nil || partition.OutputEnd == nil || *partition.OutputEnd != outputEnd || partition.ViewLag == nil || *partition.ViewLag != 0 {
						test.Fatalf("inspection fabricated a complete source boundary: %+v", partition)
					}
				}
			}
			inspectionCtx, done := context.WithTimeout(test.Context(), 30*time.Second)
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
			client, err := broker.Client(base.Brokers)
			if err != nil {
				test.Fatal(err)
			}
			defer client.Close()
			starts, err := broker.Offsets(inspectionCtx, client, manifest.Topic, 2, -2)
			if err != nil || !slices.Equal(starts, retainedStart) {
				test.Fatalf("wrong real retained source boundary: %v %v", starts, err)
			}
			ends, err := broker.Offsets(inspectionCtx, client, manifest.Topic, 2, -1)
			if err != nil || !slices.Equal(ends, []int64{4, 0}) {
				test.Fatalf("removal changed the source end: %v %v", ends, err)
			}
			if removed {
				if _, err = broker.Validate(inspectionCtx, client, manifest.Topic, manifest.Config); !errors.Is(err, broker.ErrBoundary) {
					test.Fatalf("source validation accepted the missing prefix: %v", err)
				}
				if _, err = broker.Scan(inspectionCtx, base.Brokers, manifest, ends); !errors.Is(err, kerr.OffsetOutOfRange) {
					test.Fatalf("raw source scan did not refuse the deleted offset: %v", err)
				}
			}
			snapshot, err := state.Snapshot(inspectionCtx)
			if err != nil || len(snapshot.Partitions) != 2 || snapshot.Partitions[0].Next != 4 || snapshot.Partitions[0].Sequence != 10 || len(snapshot.Outbox) != 10 || len(snapshot.Rejections) != 0 {
				test.Fatalf("history refusal changed durable input/output state: %+v %v", snapshot, err)
			}
			view, err := state.View(inspectionCtx)
			if err != nil || len(view.Rows) != 6 || view.Next[0] != 10 || view.Next[1] != 0 {
				test.Fatalf("history refusal lost view guards or progress: %+v %v", view, err)
			}
			expected, err := fixtures.Expected("expected-after.json")
			if err != nil || !reflect.DeepEqual(view.Projection(manifest.Config.Keys[0].StockKey), expected) {
				test.Fatalf("retained view differs from hand-derived corrected fixture: %v", err)
			}
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
					test.Fatalf("changed retained version/tombstone: %+v", row)
				}
			}
			if !removed {
				if _, err := os.Stat(filepath.Join(report.Directory, "history-request.json")); !errors.Is(err, os.ErrNotExist) {
					test.Fatal("canceled drill still attempted history removal")
				}
				return
			}
			var evidence historyEvidence
			read("history.json", &evidence)
			var request kmsg.DeleteRecordsRequest
			read("history-request.json", &request)
			if len(request.Topics) != 1 || request.Topics[0].Topic != manifest.Topic || len(request.Topics[0].Partitions) != 1 || request.Topics[0].Partitions[0].Partition != 0 || request.Topics[0].Partitions[0].Offset != 2 {
				test.Fatal("removal request exceeded owned fixture scope")
			}
			if evidence.Source.Topic != manifest.Topic || evidence.Source.ID != manifest.Config.Source || evidence.Source.Partitions != 2 || !slices.Equal(evidence.RequiredStart, []int64{0, 0}) || !slices.Equal(evidence.RetainedStart, []int64{2, 0}) || !slices.Equal(evidence.SourceEnd, []int64{4, 0}) {
				test.Fatalf("retained loss evidence is incomplete: %+v", evidence)
			}
			if cancellation.cancel != nil {
				if evidence.Replay != nil || evidence.Rebuild != nil || strings.Contains(transcript.String(), "history-refused:") {
					test.Fatal("cancellation continued into administrative operations")
				}
				return
			}
			if !evidence.StateUnchanged || !evidence.DestinationAbsent || !reflect.DeepEqual(evidence.PartitionsBefore, evidence.PartitionsAfter) || !reflect.DeepEqual(evidence.PartitionsAfter, snapshot.Partitions) || !slices.Equal(evidence.OutputEnd, []int64{10, 0}) || evidence.Replay == nil || evidence.Replay.Status != "INCOMPLETE" || evidence.Replay.From != 2 || evidence.Replay.Until != 4 || evidence.Rebuild == nil || evidence.Rebuild.Status != "INCOMPLETE" || evidence.Rebuild.Verification != nil {
				test.Fatalf("refused operations lost their evidence: %+v", evidence)
			}
			before, err := os.ReadFile(filepath.Join(report.Directory, "history-state-before.json"))
			if err != nil {
				test.Fatal(err)
			}
			after, err := os.ReadFile(filepath.Join(report.Directory, "history-state-after.json"))
			if err != nil || !bytes.Equal(before, after) {
				test.Fatal("administrative refusal changed raw SQL state")
			}
			checkHistoryRefusalCLI(test, inspectionCtx, base.ControlBinary, base.DatabaseURL, base.Brokers, report, evidence)
			afterSnapshot, err := state.Snapshot(inspectionCtx)
			if err != nil || !reflect.DeepEqual(snapshot, afterSnapshot) {
				test.Fatal("public CLI refusal changed state or ownership epochs")
			}
			afterView, err := state.View(inspectionCtx)
			if err != nil || !reflect.DeepEqual(view, afterView) {
				test.Fatal("public CLI refusal changed the view")
			}
			config := manifest.Config.Clone()
			config.Namespace = evidence.DestinationNamespace
			destination, err := postgres.New(pool, config)
			if err != nil {
				test.Fatal(err)
			}
			if exists, err := destination.Exists(inspectionCtx); err != nil || exists {
				test.Fatalf("refused rebuild created durable destination: %v", err)
			}
			if _, err = broker.Inspect(inspectionCtx, client, evidence.DestinationOutput); !errors.Is(err, kerr.UnknownTopicOrPartition) {
				test.Fatalf("refused rebuild created output topic: %v", err)
			}
		})
	}
}

func checkHistoryRefusalCLI(test *testing.T, ctx context.Context, binary, databaseURL string, brokers []string, run Report, evidence historyEvidence) {
	test.Helper()
	for _, execute := range []bool{false, true} {
		for _, command := range []string{"replay", "rebuild"} {
			args := []string{command, "-manifest", filepath.Join(run.Directory, "manifest.json"), "-brokers", strings.Join(brokers, ","), "-timeout", "5s"}
			if execute {
				args = append(args, "--execute", "--quiesce")
			}
			if command == "replay" {
				args = append(args, "-partition", "0", "-from-offset", "2", "-until-offset", "4")
			} else {
				args = append(args, "-new-namespace", evidence.DestinationNamespace, "-output-topic", evidence.DestinationOutput, "-artifacts", filepath.Join(run.Directory, "refused-rebuild"))
			}
			child := exec.CommandContext(ctx, binary, args...)
			child.Env = append(os.Environ(), "AMENDS_DATABASE_URL="+databaseURL)
			var stdout, stderr bytes.Buffer
			child.Stdout, child.Stderr = &stdout, &stderr
			var exitError *exec.ExitError
			if err := child.Run(); !errors.As(err, &exitError) || exitError.ExitCode() != 1 {
				test.Fatalf("public %s execute=%t did not refuse: stdout=%s stderr=%s error=%v", command, execute, stdout.String(), stderr.String(), err)
			}
			if command == "replay" {
				var report admin.ReplayReport
				if err := json.Unmarshal(stdout.Bytes(), &report); err != nil || report.Status != "INCOMPLETE" || report.Epoch != 0 || !strings.Contains(report.Reason, "required history missing") {
					test.Fatalf("public replay hid history refusal: %s %v", stdout.String(), err)
				}
			} else {
				var report admin.RebuildReport
				if err := json.Unmarshal(stdout.Bytes(), &report); err != nil || report.Status != "INCOMPLETE" || report.Manifest.Config.Namespace != "" || report.Verification != nil || !strings.Contains(report.Reason, "required history missing") {
					test.Fatalf("public rebuild hid history refusal: %s %v", stdout.String(), err)
				}
			}
		}
	}
	manifests, err := filepath.Glob(filepath.Join(run.Directory, "refused-rebuild", "run-*", "manifest.json"))
	if err != nil || len(manifests) != 0 {
		test.Fatal("public rebuild saved an unusable destination manifest")
	}
}
