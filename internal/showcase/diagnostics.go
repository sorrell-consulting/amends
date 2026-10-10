package showcase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/verify"
)

func (runner *runner) prepareSource() error {
	raw, err := os.ReadFile(filepath.Join(runner.report.Directory, "source.log"))
	if err != nil {
		return err
	}
	manifest, err := broker.ReadManifest(bytes.NewReader(raw))
	if err != nil {
		return err
	}
	if runner.options.Scenario == SourceConflict || runner.options.Scenario == RebalanceUnderLoad {
		name := "conflict-other-key.json"
		if runner.options.Scenario == RebalanceUnderLoad {
			name = "rebalance-other-key.json"
		}
		raw, err = fixtures.Files.ReadFile(name)
		if err != nil {
			return err
		}
		var key ledger.KeyConfig
		if err = json.Unmarshal(raw, &key); err != nil {
			return err
		}
		manifest.Config.Keys = append(manifest.Config.Keys, key)
		if err = manifest.Config.Validate(); err != nil {
			return err
		}
	}
	return runner.save("source.json", manifest)
}

func diagnosticExpectation(config ledger.Config, drill string, sourceEnd int64) (verify.ExpectedDiagnostics, error) {
	expected := verify.ExpectedDiagnostics{Name: drill, Namespace: config.Namespace, ConfigDigest: config.Digest(), SourceEnd: []int64{sourceEnd, 0}}
	var names []string
	var offset int64
	switch drill {
	case "poison-event":
		names, offset = []string{"poison-event.jsonl"}, 4
	case "source-conflict":
		names, offset = []string{"correction.jsonl", "source-conflict.jsonl"}, 3
	default:
		return expected, fmt.Errorf("unknown diagnostic fixture %q", drill)
	}
	records, err := fixtures.Records(config, names...)
	if err != nil {
		return expected, err
	}
	for index := range records {
		records[index].Offset = offset + int64(index)
		records[index].RawKey = broker.EncodeKey(records[index].Key)
	}
	if drill == "poison-event" {
		expected.Diagnostics.Rejections = []ledger.Rejection{{Record: records[0], Reason: "REVISION"}}
	} else {
		expected.Diagnostics.Conflicts = []verify.ConflictEvidence{{
			Conflict: ledger.Conflict{StockKey: config.Keys[0].StockKey, TxnID: "ISSUE-001", Revision: 2},
			Records:  records[:2],
		}}
	}
	return expected, nil
}

func (runner *runner) diagnosticDrill(ctx context.Context) error {
	drill := string(runner.options.Scenario)
	sourceEnd, quarantined := int64(5), int64(1)
	status, repair := "PASS_WITH_EXCLUSIONS", "poison-repair"
	if drill == "source-conflict" {
		sourceEnd, quarantined = 6, 0
		status, repair = "BLOCKED", "conflict-repair"
	}
	baseline, err := runner.state.Snapshot(ctx)
	if err != nil {
		return err
	}
	if _, err = fmt.Fprintf(runner.options.Output, "%s: inject the declared source fixture; require %s, never an unqualified PASS.\n", drill, status); err != nil {
		return err
	}
	if runner.relay, err = runner.role("relay-diagnostic", "relay"); err != nil {
		return err
	}
	if err = runner.load(ctx, drill); err != nil {
		return err
	}
	if err = runner.awaitSourceOutbox(ctx, sourceExpectation{Next: sourceEnd, Quarantined: quarantined}, sourceExpectation{}); err != nil {
		return err
	}
	if err = runner.relay.stop(); err != nil {
		return err
	}
	if err = runner.diagnosticStage(ctx, "diagnostic", sourceEnd, status, "expected-after.json"); err != nil {
		return err
	}
	if err = runner.checkpoint(ctx, DiagnosticVerified); err != nil {
		return err
	}
	fault, err := runner.state.Snapshot(ctx)
	if err != nil {
		return err
	}
	key := runner.manifest.Config.Keys[0].StockKey
	for identity, envelope := range fault.Outputs {
		if identity.Key == key && identity.Family != "key-status" && !envelope.Equal(baseline.Outputs[identity]) {
			return fmt.Errorf("diagnostic changed frozen business output %s", identity.String())
		}
	}
	for identity, envelope := range baseline.Outputs {
		if identity.Key == key && identity.Family != "key-status" && !envelope.Equal(fault.Outputs[identity]) {
			return fmt.Errorf("diagnostic removed frozen business output %s", identity.String())
		}
	}
	if drill == "poison-event" && (!reflect.DeepEqual(baseline.Keys, fault.Keys) || !reflect.DeepEqual(baseline.Outputs, fault.Outputs) || !reflect.DeepEqual(baseline.Outbox, fault.Outbox)) {
		return fmt.Errorf("rejected record changed authority, outputs, or publication intent")
	}
	if _, err = fmt.Fprintf(runner.options.Output, "Recovery: publish %s.jsonl as new source input; retain all diagnostic evidence.\n", repair); err != nil {
		return err
	}
	if runner.relay, err = runner.role("relay-repaired", "relay"); err != nil {
		return err
	}
	if err = runner.load(ctx, repair); err != nil {
		return err
	}
	expectedFile := "expected-poison-repaired.json"
	if drill == "source-conflict" {
		expectedFile = "expected-conflict-repaired.json"
	}
	if err = runner.awaitSourceOutbox(ctx, sourceExpectation{Next: sourceEnd + 1, Quarantined: quarantined}, sourceExpectation{}); err != nil {
		return err
	}
	if err = runner.relay.stop(); err != nil {
		return err
	}
	return runner.diagnosticStage(ctx, "repaired", sourceEnd+1, "PASS_WITH_EXCLUSIONS", expectedFile)
}

func (runner *runner) diagnosticStage(ctx context.Context, name string, sourceEnd int64, status, projectionFile string) error {
	expected, err := diagnosticExpectation(runner.manifest.Config, string(runner.options.Scenario), sourceEnd)
	if err != nil {
		return err
	}
	if err = runner.save("expected-"+name+".json", expected); err != nil {
		return err
	}
	if _, err = runner.verifyDiagnostic(ctx, name+"-default", status, expected, false); err != nil {
		return err
	}
	report, err := runner.verifyDiagnostic(ctx, name, status, expected, true)
	if err != nil {
		return err
	}
	runner.report.Stages = append(runner.report.Stages, report)
	actual, err := runner.state.View(ctx)
	if err != nil {
		return err
	}
	want, err := fixtures.Expected(projectionFile)
	if err != nil {
		return err
	}
	if status == "BLOCKED" {
		want.Status = ledger.Blocked
		want.Reasons = []string{expected.Diagnostics.Conflicts[0].Conflict.Ref()}
	}
	projections := []ledger.Projection{want}
	if runner.options.Scenario == SourceConflict {
		other, err := fixtures.Expected("expected-conflict-other.json")
		if err != nil {
			return err
		}
		projections = append(projections, other)
	}
	for _, want := range projections {
		got := actual.Projection(want.Key)
		if got.Status != want.Status || !slices.Equal(got.Reasons, want.Reasons) || !slices.Equal(got.Balances, want.Balances) || !slices.Equal(got.Alerts, want.Alerts) {
			return fmt.Errorf("%s key %v differs from hand-derived projection: got %+v; want %+v; see %s",
				name, want.Key, got, want, filepath.Join(runner.report.Directory, "verify-"+name+".log"))
		}
	}
	if err = runner.inspectStage(ctx, name); err != nil {
		return err
	}
	if err = runner.command(ctx, "versions-"+name, "view-show", "-manifest", filepath.Join(runner.report.Directory, "manifest.json")); err != nil {
		return err
	}
	_, err = fmt.Fprintf(runner.options.Output, "%s: verifier %s; complete H=%v O=%v; exact diagnostics matched; hand-derived values checked (BLOCKED values remain uncertified).\n", name, report.Status, report.SourceEnd, report.OutputEnd)
	return err
}

func (runner *runner) verifyDiagnostic(ctx context.Context, name, status string, expected verify.ExpectedDiagnostics, qualified bool) (verify.Report, error) {
	var report verify.Report
	args := []string{"verify", "--quiesce", "-manifest", filepath.Join(runner.report.Directory, "manifest.json"), "-timeout", "30s", "-brokers", strings.Join(runner.options.Brokers, ",")}
	exitCode := 1
	if qualified {
		args = append(args, "-expected-diagnostics", filepath.Join(runner.report.Directory, "expected-"+name+".json"))
		if status == "PASS_WITH_EXCLUSIONS" {
			exitCode = 0
		}
	}
	child, err := runner.start("verify-"+name, runner.options.ControlBinary, true, args...)
	if err != nil {
		return report, err
	}
	waitErr := child.wait(ctx)
	var processError *exec.ExitError
	if waitErr != nil && (!errors.As(waitErr, &processError) || processError.ExitCode() != exitCode) {
		return report, fmt.Errorf("verify-%s failed: %w", name, waitErr)
	}
	if waitErr == nil && exitCode != 0 {
		return report, fmt.Errorf("verify-%s unexpectedly accepted qualified or blocked history", name)
	}
	report, err = runner.readVerificationReport(name)
	if err != nil {
		return report, err
	}
	checks := verify.Checks{WritersStopped: true, SourceScanned: true, WorkersReached: true, PublicationDrained: true, ViewReached: true, BoundariesStable: true}
	if report.Status != status || report.Checks != checks || report.BinarySHA256 == "" || len(report.Differences) != 0 || !slices.Equal(report.SourceEnd, expected.SourceEnd) {
		return report, fmt.Errorf("verify-%s unexpected status, boundary, or differences: %s H=%v", name, report.Status, report.SourceEnd)
	}
	if qualified && (report.ExpectedDiagnostics != expected.Name || !report.ExpectedDiagnosticsMatch) {
		return report, fmt.Errorf("verify-%s did not match exact declared diagnostics", name)
	}
	return report, nil
}
