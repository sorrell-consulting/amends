// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Package showcase owns a small, finite pipeline and its process-crash drill.
// It does not implement authority, folding, or verification decisions.
package showcase

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/sorrell-consulting/amends/internal/verify"
	"github.com/sorrell-consulting/amends/internal/worker"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type Options struct {
	DatabaseURL                 string
	Brokers                     []string
	ControlBinary, WorkerBinary string
	ArtifactParent              string
	Scenario                    Scenario
	OnCheckpoint                func(Checkpoint)
	Output                      io.Writer
}

type Process struct {
	Name            string `json:"name"`
	PID             int    `json:"pid"`
	ApplicationName string `json:"application_name"`
	Joined          bool   `json:"joined"`
	ExitCode        int    `json:"exit_code"`
}

type Report struct {
	Status             string             `json:"status"`
	Scope              string             `json:"scope"`
	FinalVerification  string             `json:"final_verification_status,omitempty"`
	Reason             string             `json:"reason,omitempty"`
	Namespace          string             `json:"namespace"`
	Directory          string             `json:"directory"`
	Mode               string             `json:"mode"`
	StartedAt          time.Time          `json:"started_at"`
	ElapsedMillis      int64              `json:"elapsed_ms"`
	GoVersion          string             `json:"go_version"`
	Platform           string             `json:"platform"`
	SHA256             map[string]string  `json:"sha256"`
	Processes          []Process          `json:"processes"`
	TwoMembersAssigned bool               `json:"two_members_assigned"`
	Crash              *Crash             `json:"crash,omitempty"`
	MaterializerCrash  *MaterializerCrash `json:"materializer_crash,omitempty"`
	Zombie             *ZombieEvidence    `json:"zombie_worker,omitempty"`
	Ambiguous          *AmbiguousEvidence `json:"ambiguous_commit,omitempty"`
	Rebalance          *RebalanceEvidence `json:"rebalance,omitempty"`
	Stages             []verify.Report    `json:"stages"`
}

type runner struct {
	options  Options
	report   *Report
	children []*process
	manifest broker.Manifest
	pool     *pgxpool.Pool
	state    *postgres.Store
	client   *kgo.Client
	relay    *process
	view     *process
}

// Run creates fresh resources and retains them for inspection. It always joins
// its children, including on failure. It neither adopts existing pipelines nor
// starts/stops the database or broker. The caller supplies an overall deadline.
func Run(ctx context.Context, o Options) (report Report, err error) {
	if o.DatabaseURL == "" || len(o.Brokers) == 0 || o.ControlBinary == "" || o.WorkerBinary == "" || o.ArtifactParent == "" {
		return report, fmt.Errorf("showcase requires database, brokers, binaries, and artifact directory")
	}
	if o.Scenario == "" {
		o.Scenario = CorrectionOnly
	}
	if !o.Scenario.valid() {
		return report, fmt.Errorf("unsupported showcase scenario %q", o.Scenario)
	}
	if o.Scenario == AmbiguousRequest || o.Scenario == AmbiguousReply {
		if _, err = ambiguousConfig(o.DatabaseURL); err != nil {
			return report, err
		}
	}
	if o.Output == nil {
		o.Output = io.Discard
	}
	if err = os.MkdirAll(o.ArtifactParent, 0755); err != nil {
		return report, err
	}
	dir, err := os.MkdirTemp(o.ArtifactParent, "run-")
	if err != nil {
		return report, err
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		return report, err
	}
	report = Report{Status: "ERROR", Namespace: "amends-demo-" + strings.TrimPrefix(filepath.Base(dir), "run-"), Directory: dir, Mode: string(o.Scenario), StartedAt: time.Now().UTC(), GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH, SHA256: map[string]string{}}
	r := &runner{options: o, report: &report}
	report.Scope = "owned-pipeline assertions; nested stages retain their verification status"
	defer func() {
		err = errors.Join(err, r.cleanup())
		for i, p := range r.children {
			select {
			case <-p.done:
				report.Processes[i].Joined = true
				report.Processes[i].ExitCode = p.cmd.ProcessState.ExitCode()
			default:
			}
		}
		report.ElapsedMillis = time.Since(report.StartedAt).Milliseconds()
		if len(report.Stages) > 0 {
			report.FinalVerification = report.Stages[len(report.Stages)-1].Status
		}
		if err == nil {
			report.Status = "PASS"
		} else {
			report.Status = "ERROR"
			report.Reason = err.Error()
		}
		err = errors.Join(err, r.save("run.json", report))
	}()
	if _, err = fmt.Fprintf(o.Output, "\n%s, namespace %s\nArtifacts: %s\n", report.Mode, report.Namespace, dir); err != nil {
		return report, err
	}
	for name, path := range map[string]string{"amends": o.WorkerBinary, "amendsctl": o.ControlBinary} {
		raw, e := os.ReadFile(path)
		if e != nil {
			return report, e
		}
		sum := sha256.Sum256(raw)
		report.SHA256[name] = hex.EncodeToString(sum[:])
	}
	names := scenarioFixtures(o.Scenario)
	for _, name := range names {
		raw, e := fixtures.Files.ReadFile(name)
		if e != nil {
			return report, e
		}
		sum := sha256.Sum256(raw)
		report.SHA256[name] = hex.EncodeToString(sum[:])
		if e = os.WriteFile(filepath.Join(dir, name), raw, 0600); e != nil {
			return report, e
		}
	}
	// Setup uses the same public commands as the manual path. Logs containing
	// manifests are copied only after successful command completion.
	if err = r.command(ctx, "source", "source-create", "-namespace", report.Namespace, "-topic", report.Namespace+"-source"); err != nil {
		return report, err
	}
	if err = r.prepareSource(); err != nil {
		return report, err
	}
	if err = r.command(ctx, "manifest", "output-create", "-manifest", filepath.Join(dir, "source.json"), "-topic", report.Namespace+"-output"); err != nil {
		return report, err
	}
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.log"))
	if err != nil {
		return report, err
	}
	r.manifest, err = broker.ReadManifest(strings.NewReader(string(raw)))
	if err != nil {
		return report, err
	}
	if err = os.WriteFile(filepath.Join(dir, "manifest.json"), raw, 0600); err != nil {
		return report, err
	}
	r.pool, err = pgxpool.New(ctx, o.DatabaseURL)
	if err != nil {
		return report, err
	}
	defer r.pool.Close()
	r.state, err = postgres.New(r.pool, r.manifest.Config)
	if err != nil {
		return report, err
	}
	r.client, err = broker.Client(o.Brokers)
	if err != nil {
		return report, err
	}
	defer r.client.Close()
	if o.Scenario == RebalanceUnderLoad {
		err = r.rebalanceUnderLoad(ctx)
	} else {
		err = r.correctionScenario(ctx)
	}
	if err != nil {
		return report, err
	}
	qualification := ""
	if o.Scenario == PoisonEvent || o.Scenario == SourceConflict {
		qualification = " -expected-diagnostics " + shellQuote(filepath.Join(dir, "expected-repaired.json"))
	}
	_, err = fmt.Fprintf(o.Output, "Reverify retained state (set AMENDS_DATABASE_URL for this stack):\n  %s verify --quiesce -brokers %s -manifest %s%s\n", shellQuote(o.ControlBinary), shellQuote(strings.Join(o.Brokers, ",")), shellQuote(filepath.Join(dir, "manifest.json")), qualification)
	return report, err
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func (r *runner) save(name string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(r.report.Directory, name), append(raw, '\n'), 0600)
}

// readVerificationReport decodes a saved report; each drill checks its own
// expected status, boundary, and diagnostics separately.
func (r *runner) readVerificationReport(name string) (verify.Report, error) {
	var report verify.Report
	raw, err := os.ReadFile(filepath.Join(r.report.Directory, "verify-"+name+".log"))
	if err != nil {
		return report, err
	}
	err = json.Unmarshal(raw, &report)
	return report, err
}

func (r *runner) command(ctx context.Context, name string, args ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	args = append(args, "-brokers", strings.Join(r.options.Brokers, ","))
	p, err := r.start(name, r.options.ControlBinary, true, args...)
	if err != nil {
		return err
	}
	if err = p.wait(ctx); err != nil {
		return fmt.Errorf("%s failed (see %s.log): %w", name, name, err)
	}
	return nil
}

func (r *runner) role(name, role string) (*process, error) {
	return r.start(name, r.options.WorkerBinary, false, "-role", role, "-log-format", "json", "-brokers", strings.Join(r.options.Brokers, ","), "-manifest", filepath.Join(r.report.Directory, "manifest.json"))
}

func (r *runner) load(ctx context.Context, name string) error {
	return r.command(ctx, "load-"+name, "source-load", "-manifest", filepath.Join(r.report.Directory, "manifest.json"), "-file", filepath.Join(r.report.Directory, name+".jsonl"))
}

func (r *runner) until(ctx context.Context, label string, check func() (bool, error)) error {
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		if err := r.healthy(); err != nil {
			return err
		}
		done, err := check()
		if err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", label, ctx.Err())
		case <-tick.C:
		}
	}
}

func (r *runner) twoMembers(ctx context.Context) error {
	return r.until(ctx, "two assigned workers", func() (bool, error) {
		q := kmsg.NewDescribeGroupsRequest()
		q.Groups = []string{worker.Group(r.report.Namespace)}
		response, err := q.RequestWith(ctx, r.client)
		if err != nil {
			return false, err
		}
		if len(response.Groups) != 1 {
			return false, nil
		}
		g := response.Groups[0]
		if g.ErrorCode != 0 || g.State != "Stable" || len(g.Members) != 2 {
			return false, nil
		}
		seen := map[int32]bool{}
		for _, m := range g.Members {
			var assignment kmsg.ConsumerMemberAssignment
			if err = assignment.ReadFrom(m.MemberAssignment); err != nil {
				return false, err
			}
			if len(assignment.Topics) != 1 || assignment.Topics[0].Topic != r.manifest.Topic || len(assignment.Topics[0].Partitions) != 1 {
				return false, nil
			}
			seen[assignment.Topics[0].Partitions[0]] = true
		}
		return seen[0] && seen[1], nil
	})
}

func (r *runner) stage(ctx context.Context, name string, sourceEnd int64) error {
	return r.fixtureStage(ctx, name, sourceEnd, "expected-"+name+".json")
}

func (r *runner) fixtureStage(ctx context.Context, name string, sourceEnd int64, expectedFile string) error {
	// The scenario has already awaited source/outbox progress and joined the
	// relay. Verification observes that handoff; it never changes child ownership.
	if err := r.command(ctx, "verify-"+name, "verify", "--quiesce", "-manifest", filepath.Join(r.report.Directory, "manifest.json"), "-timeout", "30s"); err != nil {
		return err
	}
	report, err := r.readVerificationReport(name)
	if err != nil {
		return err
	}
	if report.Status != "PASS" || !slices.Equal(report.SourceEnd, []int64{sourceEnd, 0}) {
		return fmt.Errorf("%s verification: got status=%s H=%v; want status=PASS H=%v; see %s",
			name, report.Status, report.SourceEnd, []int64{sourceEnd, 0}, filepath.Join(r.report.Directory, "verify-"+name+".log"))
	}
	r.report.Stages = append(r.report.Stages, report)
	v, err := r.state.View(ctx)
	if err != nil {
		return err
	}
	want, err := fixtures.Expected(expectedFile)
	if err != nil {
		return err
	}
	got := v.Projection(r.manifest.Config.Keys[0].StockKey)
	if got.Key != want.Key || got.Status != want.Status || !slices.Equal(got.Reasons, want.Reasons) || !slices.Equal(got.Balances, want.Balances) || !slices.Equal(got.Alerts, want.Alerts) {
		return fmt.Errorf("%s differs from hand-derived fixture %s: got %+v; want %+v",
			name, filepath.Join(r.report.Directory, expectedFile), got, want)
	}
	if err = r.save("view-"+name+".json", got); err != nil {
		return err
	}
	if err = r.inspectStage(ctx, name); err != nil {
		return err
	}
	if _, err = fmt.Fprintf(r.options.Output, "%s: %s; source stopped; H=%v O=%v; verifier PASS; hand-derived fixture agrees\n", name, got.Status, report.SourceEnd, report.OutputEnd); err != nil {
		return err
	}
	for _, b := range got.Balances {
		if _, err = fmt.Fprintf(r.options.Output, "  %s close %d\n", b.Day, b.Close); err != nil {
			return err
		}
	}
	for _, a := range got.Alerts {
		if _, err = fmt.Fprintf(r.options.Output, "  crossing %s: %d < %d\n", a.Day, a.Close, a.Threshold); err != nil {
			return err
		}
	}
	return nil
}

// sourceExpectation states the complete frontier and quarantine count for one
// partition. Waiting never changes a child process's lifecycle.
type sourceExpectation struct {
	Next        int64
	Quarantined int64
}

func (runner *runner) awaitSourceOutbox(ctx context.Context, expected ...sourceExpectation) error {
	return runner.until(ctx, "source/outbox drain", func() (bool, error) {
		progress, err := runner.state.Progress(ctx)
		if err != nil {
			return false, err
		}
		if len(progress.Partitions) != len(expected) {
			return false, fmt.Errorf("expected %d partition observations, got %d", len(expected), len(progress.Partitions))
		}
		seen := make([]bool, len(expected))
		for _, partition := range progress.Partitions {
			index := int(partition.Partition)
			if index < 0 || index >= len(expected) || seen[index] {
				return false, fmt.Errorf("unexpected partition observation: %+v", partition)
			}
			seen[index] = true
			want := expected[index]
			if partition.SourceNext > want.Next || partition.ProtocolError != "" || partition.Quarantined > want.Quarantined {
				return false, fmt.Errorf("unexpected source boundary or diagnostics: %+v; want %+v", partition, want)
			}
			if partition.SourceNext != want.Next || partition.Pending != 0 || partition.Quarantined != want.Quarantined {
				return false, nil
			}
		}
		return true, nil
	})
}

// correctionScenario keeps the baseline, fault, delivery handoff, and recovery
// in execution order. Each verification stage observes an already stopped relay.
func (r *runner) correctionScenario(ctx context.Context) error {
	var err error
	for _, name := range []string{"worker-1", "worker-2"} {
		if _, err = r.role(name, "source"); err != nil {
			return err
		}
	}
	if r.view, err = r.role("view", "view"); err != nil {
		return err
	}
	if err = r.twoMembers(ctx); err != nil {
		return err
	}
	r.report.TwoMembersAssigned = true
	if r.relay, err = r.role("relay-before", "relay"); err != nil {
		return err
	}
	if err = r.load(ctx, "before"); err != nil {
		return err
	}
	if err = r.awaitSourceOutbox(ctx, sourceExpectation{Next: 3}, sourceExpectation{}); err != nil {
		return err
	}
	if err = r.relay.stop(); err != nil {
		return err
	}
	if err = r.stage(ctx, "before", 3); err != nil {
		return err
	}
	if err = r.checkpoint(ctx, BaselineVerified); err != nil {
		return err
	}
	correctedEnd := int64(4)
	switch r.options.Scenario {
	case RelayStall:
		err = r.relayStall(ctx)
	case ZombieWorker:
		err = r.zombieWorker(ctx)
		correctedEnd = 7
	case AmbiguousRequest, AmbiguousReply:
		err = r.ambiguousCorrection(ctx)
	}
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if r.relay, err = r.role("relay-after", "relay"); err != nil {
		return err
	}
	switch r.options.Scenario {
	case CrashMidCorrection:
		err = r.crashCorrection(ctx)
	case RelayStall, ZombieWorker, AmbiguousRequest, AmbiguousReply:
		// These scenarios already loaded the correction while delivery was stopped.
	default:
		err = r.load(ctx, "correction")
	}
	if err != nil {
		return err
	}
	if err = r.awaitSourceOutbox(ctx, sourceExpectation{Next: correctedEnd}, sourceExpectation{}); err != nil {
		return err
	}
	if err = r.relay.stop(); err != nil {
		return err
	}
	if err = r.stage(ctx, "after", correctedEnd); err != nil {
		return err
	}
	if r.options.Scenario == ZombieWorker {
		if err = r.zombieRecovered(ctx); err != nil {
			return err
		}
	}
	if r.options.Scenario == CrashMidCorrection {
		_, p, e := r.state.Observe(ctx, 0)
		if e != nil {
			return e
		}
		r.report.Crash.RecoveredEpoch = p.Epoch
		if p.Epoch <= r.report.Crash.PreviousEpoch {
			return fmt.Errorf("recovery did not install a higher ownership epoch")
		}
	}
	switch r.options.Scenario {
	case PoisonEvent, SourceConflict:
		return r.diagnosticDrill(ctx)
	case DisappearingDay, MaterializerCrashDrill:
		return r.disappearingDay(ctx)
	case DuplicateFlood, StaleAmendmentStorm:
		return r.inputDrill(ctx)
	case HistoryMissing:
		return r.historyMissing(ctx)
	default:
		return nil
	}
}
