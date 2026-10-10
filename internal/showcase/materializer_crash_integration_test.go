//go:build integration

package showcase

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/verify"
)

func checkMaterializerCrash(test *testing.T, report Report, name string) {
	test.Helper()
	evidence := report.MaterializerCrash
	if evidence == nil || evidence.Signal != "SIGKILL" || !evidence.BlockedAfterKill || !evidence.TransactionEnded || !evidence.ViewUnchanged || evidence.ViewPID <= 0 || evidence.BackendPID <= 0 || evidence.BlockerPID <= 0 || evidence.TransactionID == "" || evidence.ApplicationName != report.Namespace+"-view" || !strings.HasPrefix(evidence.Query, "INSERT INTO amends_view_rows") {
		test.Fatalf("precommit materializer crash not established: %+v", evidence)
	}
	checkSnapshots := func(prefix string, next int64, version int64, present bool) {
		test.Helper()
		before, err := os.ReadFile(filepath.Join(report.Directory, prefix+"-before.json"))
		if err != nil {
			test.Fatal(err)
		}
		after, err := os.ReadFile(filepath.Join(report.Directory, prefix+"-after.json"))
		if err != nil || !bytes.Equal(before, after) {
			test.Fatalf("view rows/guards/progress changed around crash: %s error=%v", prefix, err)
		}
		var snapshot struct {
			Rows []struct {
				Envelope string `json:"envelope"`
			} `json:"rows"`
			Progress []struct {
				Partition int     `json:"partition"`
				Next      int64   `json:"next_offset"`
				Error     *string `json:"protocol_error"`
			} `json:"progress"`
		}
		if err = json.Unmarshal(after, &snapshot); err != nil || len(snapshot.Rows) != 6 || len(snapshot.Progress) != 2 {
			test.Fatalf("incomplete view snapshot: %s error=%v", after, err)
		}
		for index, partition := range snapshot.Progress {
			want := int64(0)
			if index == 0 {
				want = next
			}
			if partition.Partition != index || partition.Next != want || partition.Error != nil {
				test.Fatalf("unexpected crash frontier/diagnostic: %+v", partition)
			}
		}
		guards := map[string]ledger.Envelope{}
		for _, row := range snapshot.Rows {
			raw, err := hex.DecodeString(strings.TrimPrefix(row.Envelope, "\\x"))
			if err != nil {
				test.Fatal(err)
			}
			var envelope ledger.Envelope
			if err = json.Unmarshal(raw, &envelope); err != nil {
				test.Fatal(err)
			}
			guards[string(envelope.ID.Family)+"/"+envelope.ID.Day] = envelope
		}
		day := guards["balance/2026-01-03"]
		alert := guards["alert/2026-01-01"]
		if day.Version != version || day.Payload.Present != present || day.ID != evidence.OutputID || alert.Version != 2 || alert.Type != "AlertRetracted" || alert.Payload.Present {
			test.Fatalf("crash lost a withdrawal guard or changed Day 3: %+v", guards)
		}
	}
	checkSnapshots("view-precommit", 10, 2, true)
	if len(report.Stages) < 3 {
		test.Fatal("missing materializer-crashed verification")
	}
	stage := report.Stages[2]
	checks := verify.Checks{WritersStopped: true, SourceScanned: true, WorkersReached: true, PublicationDrained: true}
	if stage.Status != "INCOMPLETE" || stage.Checks != checks || !strings.Contains(stage.Reason, "view has not reached output boundary") || !slices.Equal(stage.SourceEnd, []int64{5, 0}) || !slices.Equal(stage.OutputEnd, []int64{11, 0}) || len(stage.Progress.Partitions) != 2 || stage.Progress.Partitions[0].ViewNext != 10 || stage.Progress.Partitions[0].Pending != 0 {
		test.Fatalf("crashed view's incomplete boundary lost: %+v", stage)
	}
	for _, key := range stage.Keys {
		if key.Compared || key.Agrees {
			test.Fatal("view lag certified as a comparison")
		}
	}
	checkInspectionArtifacts(test, report.Directory, "materializer-crashed", "materializer-crash")
	if name == "cancel-materializer-crashed" {
		if evidence.RecoveryPID != 0 || evidence.CommittedSignal != "" || evidence.RestartPID != 0 {
			test.Fatal("canceled precommit crash continued recovery")
		}
	} else {
		if evidence.RecoveryPID <= 0 || evidence.CommittedSignal != "SIGKILL" || evidence.CommittedNext != 11 || evidence.CommittedTombstone.Version != 3 || evidence.CommittedTombstone.Type != "BalanceRetracted" || evidence.CommittedTombstone.Payload.Present || !evidence.CommittedStateUnchanged {
			test.Fatalf("postcommit crash not established: %+v", evidence)
		}
		checkSnapshots("view-postcommit", 11, 3, false)
		if name == "cancel-materializer-committed" {
			if evidence.RestartPID != 0 {
				test.Fatal("canceled postcommit crash restarted materializer")
			}
		} else if evidence.RestartPID <= 0 || evidence.RestartPID == evidence.RecoveryPID || evidence.RestartPID == evidence.ViewPID {
			test.Fatal("missing fresh materializer after committed withdrawal")
		}
	}
	killed := 0
	for _, child := range report.Processes {
		if child.Name == "view" || child.Name == "view-recovered" {
			if child.ExitCode != -1 {
				test.Fatalf("crash child did not die by signal: %+v", child)
			}
			killed++
		} else if child.Name != "verify-materializer-crashed" && child.ExitCode != 0 {
			test.Fatalf("unexpected child failure: %+v", child)
		}
	}
	wantKills := 2
	if name == "cancel-materializer-crashed" {
		wantKills = 1
	}
	if killed != wantKills {
		test.Fatalf("killed %d children, want %d", killed, wantKills)
	}
}
