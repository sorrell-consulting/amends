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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/sorrell-consulting/amends/internal/verify"
)

func TestDiagnosticDrills(t *testing.T) {
	base := showcaseTestOptions(t)
	for _, drill := range []string{"poison-event", "source-conflict", "cancel-blocked"} {
		t.Run(drill, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			var transcript bytes.Buffer
			cancellation := newCheckpointCancellation(t)
			name := drill
			if drill == "cancel-blocked" {
				name, cancellation.point, cancellation.cancel = "source-conflict", DiagnosticVerified, cancel
			}
			options := base
			options.Scenario, options.Output, options.OnCheckpoint = Scenario(name), &transcript, cancellation.observe
			report, err := Run(ctx, options)
			retainTestRun(t, base, report, &transcript)
			for _, child := range report.Processes {
				wantExit := 0
				if strings.HasPrefix(child.Name, "verify-diagnostic") || strings.HasPrefix(child.Name, "verify-repaired") {
					wantExit = 1
				}
				// Qualified repaired/poison verification accepts the declared exclusions.
				if child.Name == "verify-repaired" || (name == "poison-event" && child.Name == "verify-diagnostic") {
					wantExit = 0
				}
				checkChildExit(t, child, wantExit)
				if drill == "cancel-blocked" && (child.Name == "relay-repaired" || child.Name == "load-conflict-repair") {
					t.Fatal("canceled diagnostic stage continued repair")
				}
			}
			if drill == "cancel-blocked" {
				if err == nil || report.Status != "ERROR" || report.FinalVerification != "BLOCKED" || len(report.Stages) != 3 {
					t.Fatalf("cancellation lost its negative state: %+v error=%v", report, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if report.Status != "PASS" || report.FinalVerification != "PASS_WITH_EXCLUSIONS" || report.Scope == "" || !report.TwoMembersAssigned || len(report.Stages) != 4 {
				t.Fatalf("incomplete or unqualified run: %+v", report)
			}
			checkInspectionArtifacts(t, report.Directory, "diagnostic", drill)
			checkInspectionArtifacts(t, report.Directory, "repaired", drill)
			sourceEnd, faultStatus, emissionCount := int64(6), "PASS_WITH_EXCLUSIONS", 11
			if drill == "source-conflict" {
				sourceEnd, faultStatus, emissionCount = 7, "BLOCKED", 18
			}
			if report.Stages[2].Status != faultStatus || report.Stages[3].Status != "PASS_WITH_EXCLUSIONS" || !slices.Equal(report.Stages[3].SourceEnd, []int64{sourceEnd, 0}) {
				t.Fatalf("wrong stage boundaries/statuses: %+v", report.Stages)
			}
			if drill == "source-conflict" {
				for _, key := range report.Stages[2].Keys {
					if key.Key.Item == "DEMO-PART" && (key.Compared || key.Agrees || key.ViewStatus != "BLOCKED") {
						t.Fatal("blocked key certified")
					}
					if key.Key.Item == "DEMO-OTHER" && (!key.Compared || !key.Agrees || key.ViewStatus != "READY") {
						t.Fatal("other key did not continue in the same partition")
					}
				}
			}
			manifestPath := filepath.Join(report.Directory, "manifest.json")
			raw, err := os.ReadFile(manifestPath)
			if err != nil {
				t.Fatal(err)
			}
			manifest, err := broker.ReadManifest(bytes.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			pool, err := pgxpool.New(ctx, base.DatabaseURL)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			state, err := postgres.New(pool, manifest.Config)
			if err != nil {
				t.Fatal(err)
			}
			checkReviewCommands(t, ctx, base.ControlBinary, base.DatabaseURL, manifestPath, state, drill)
			snapshot, err := state.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Outbox) != emissionCount || snapshot.Partitions[0].Next != sourceEnd {
				t.Fatalf("unexpected semantic transitions/frontier: outbox=%d frontier=%d", len(snapshot.Outbox), snapshot.Partitions[0].Next)
			}
			view, err := state.View(ctx)
			if err != nil {
				t.Fatal(err)
			}
			key := manifest.Config.Keys[0].StockKey
			statusRow := view.Rows[ledger.OutputID{Namespace: manifest.Config.Namespace, Key: key, Family: "key-status"}]
			wantVersion := int64(1)
			if drill == "source-conflict" {
				wantVersion = 3
				last := snapshot.Outbox[len(snapshot.Outbox)-1].Envelope
				if last.Type != "KeyStatusStated" || last.Payload.Status != "READY" || last.Cause.Revision != 3 {
					t.Fatal("READY did not follow the business repair")
				}
			}
			if statusRow.Version != wantVersion || statusRow.Payload.Status != "READY" {
				t.Fatalf("unexpected status version: %+v", statusRow)
			}
			for _, day := range []string{"2026-01-01", "2026-01-02", "2026-01-03"} {
				wantVersion := int64(3)
				if drill == "poison-event" && day != "2026-01-03" {
					wantVersion = 2
				}
				balance := view.Rows[ledger.OutputID{Namespace: manifest.Config.Namespace, Key: key, Family: "balance", Day: day}]
				if balance.Version != wantVersion || !balance.Payload.Present {
					t.Fatalf("wrong balance version: %+v", balance)
				}
			}
			for _, day := range []string{"2026-01-01", "2026-01-02"} {
				alert := view.Rows[ledger.OutputID{Namespace: manifest.Config.Namespace, Key: key, Family: "alert", Day: day}]
				present := drill == "poison-event" && day == "2026-01-02"
				wantVersion := int64(2)
				if present {
					wantVersion = 1
				}
				if alert.Version != wantVersion || alert.Payload.Present != present {
					t.Fatalf("lost alert withdrawal or unexpected restatement: %+v", alert)
				}
			}
			raw, err = os.ReadFile(filepath.Join(report.Directory, "expected-repaired.json"))
			if err != nil {
				t.Fatal(err)
			}
			expected, err := verify.ReadExpected(bytes.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			if drill == "poison-event" {
				expected.Diagnostics.Rejections[0].Reason = "MALFORMED"
			} else {
				expected.Diagnostics.Conflicts[0].Records[0].Offset++
			}
			wrongPath := filepath.Join(report.Directory, "expected-wrong.json")
			raw, err = json.Marshal(expected)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(wrongPath, raw, 0600); err != nil {
				t.Fatal(err)
			}
			command := exec.CommandContext(ctx, base.ControlBinary, "verify", "--quiesce", "-manifest", manifestPath, "-brokers", strings.Join(base.Brokers, ","), "-expected-diagnostics", wrongPath)
			command.Env = append(os.Environ(), "AMENDS_DATABASE_URL="+base.DatabaseURL)
			raw, err = command.CombinedOutput()
			var exitError *exec.ExitError
			if !errors.As(err, &exitError) || exitError.ExitCode() != 1 {
				t.Fatalf("wrong diagnostics accepted or command failed: %s error=%v", raw, err)
			}
			var rejected verify.Report
			if err = json.Unmarshal(raw, &rejected); err != nil || rejected.Status != "PASS_WITH_EXCLUSIONS" || rejected.ExpectedDiagnosticsMatch {
				t.Fatalf("mismatch lost qualified status: %s error=%v", raw, err)
			}
		})
	}
}
