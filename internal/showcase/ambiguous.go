package showcase

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/sorrell-consulting/amends/internal/verify"
	"github.com/sorrell-consulting/amends/internal/worker"
)

type AmbiguousEvidence struct {
	Loss                 string          `json:"loss"`
	Worker               string          `json:"worker"`
	Joined               bool            `json:"worker_joined"`
	FaultFired           bool            `json:"fault_fired"`
	ServerCommitObserved bool            `json:"server_commit_observed"`
	Connections          int32           `json:"connections"`
	BackendBefore        int32           `json:"backend_before"`
	BackendAfter         int32           `json:"backend_after"`
	OriginalError        string          `json:"original_storage_error"`
	Token                store.Token     `json:"recovery_token"`
	Observed             store.Partition `json:"recovery_partition"`
	AttemptOutcomes      []string        `json:"attempt_outcomes"`
	RecoveryReleased     bool            `json:"recovery_released"`
	Recovered            bool            `json:"recovered"`
}

type commitObservation struct {
	token         store.Token
	partition     store.Partition
	backendBefore int32
	backendAfter  int32
	originalError string
}

type ambiguousState struct {
	*postgres.Store
	pool          *pgxpool.Pool
	loss          *commitWireLoss
	mode          string
	attempted     bool
	unknown       error
	backendBefore int32
	observed      chan commitObservation
	release       chan struct{}
	paused        bool
}

func (state *ambiguousState) Process(ctx context.Context, token store.Token, expected int64, record ledger.Record) error {
	if !state.attempted {
		if record.Partition != 0 || record.Offset != 3 || expected != 3 {
			return fmt.Errorf("commit fault refuses an unexpected processing boundary")
		}
		if err := state.pool.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&state.backendBefore); err != nil {
			return err
		}
		state.attempted = true
		mode := int32(1)
		if state.mode == "request" {
			mode = 2
		}
		state.loss.armed.Store(mode)
	}
	err := state.Store.Process(ctx, token, expected, record)
	if errors.Is(err, store.ErrCommitUnknown) {
		state.unknown = err
	}
	return err
}

func (state *ambiguousState) Observe(ctx context.Context, partition int) (store.Token, store.Partition, error) {
	token, progress, err := state.Store.Observe(ctx, partition)
	if err != nil || state.unknown == nil || state.paused {
		return token, progress, err
	}
	observation := commitObservation{token: token, partition: progress, backendBefore: state.backendBefore, originalError: state.unknown.Error()}
	if err = state.pool.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&observation.backendAfter); err != nil {
		return store.Token{}, store.Partition{}, err
	}
	state.paused = true
	state.observed <- observation
	select {
	case <-state.release:
		return token, progress, ctx.Err()
	case <-ctx.Done():
		return store.Token{}, store.Partition{}, ctx.Err()
	}
}

func (runner *runner) ambiguousCorrection(ctx context.Context) (err error) {
	for _, child := range runner.children {
		if child.name == "worker-1" || child.name == "worker-2" {
			if err = child.stop(); err != nil {
				return err
			}
		}
	}
	before, err := runner.durableState(ctx)
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(runner.report.Directory, "commit-before.json"), before, 0600); err != nil {
		return err
	}
	baselineView, err := runner.state.View(ctx)
	if err != nil {
		return err
	}
	_, baselinePartition, err := runner.state.Observe(ctx, 0)
	if err != nil {
		return err
	}
	config, err := ambiguousConfig(runner.options.DatabaseURL)
	if err != nil {
		return err
	}
	config.ConnConfig.RuntimeParams["application_name"] = runner.report.Namespace + "-worker-ambiguous"
	loss := &commitWireLoss{}
	loss.configure(config)
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return err
	}
	defer pool.Close()
	state, err := postgres.New(pool, runner.manifest.Config)
	if err != nil {
		return err
	}
	logPath := filepath.Join(runner.report.Directory, "worker-ambiguous.log")
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, log.Close()) }()
	controlled := &ambiguousState{Store: state, pool: pool, loss: loss, mode: strings.TrimPrefix(string(runner.options.Scenario), "ambiguous-commit-"), observed: make(chan commitObservation, 1), release: make(chan struct{})}
	evidence := &AmbiguousEvidence{Loss: controlled.mode, Worker: "production worker.Lifecycle in the controller; controlled assignment, real retained broker input and PostgreSQL; socket fault and recovery-read gate"}
	runner.report.Ambiguous = evidence
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	lifecycle, err := worker.NewLifecycle(workerCtx, worker.LifecycleOptions{Config: runner.manifest.Config, State: controlled, Logger: slog.New(slog.NewJSONHandler(log, nil)), Validate: func(ctx context.Context) ([]int64, error) {
		return broker.Validate(ctx, runner.client, runner.manifest.Topic, runner.manifest.Config)
	}})
	if err != nil {
		return err
	}
	defer lifecycle.Stop(context.Canceled)
	if err = lifecycle.Assign(lifecycle.Context(), []int{0}); err != nil {
		return err
	}
	positions, err := lifecycle.Seek(lifecycle.Context(), []int{0})
	if err != nil || positions[0] != 3 {
		return fmt.Errorf("controlled source worker did not seek baseline frontier: %v", err)
	}
	input := make(chan ledger.Record, 1)
	done := make(chan struct{})
	var workerErr error
	go func() {
		select {
		case record := <-input:
			workerErr = lifecycle.Process(lifecycle.Context(), record)
		case <-workerCtx.Done():
			workerErr = workerCtx.Err()
		}
		close(done)
	}()
	defer func() {
		stop()
		<-done
		evidence.Joined = true
		evidence.FaultFired, evidence.ServerCommitObserved, evidence.Connections = loss.fired.Load(), loss.committed.Load(), loss.dials.Load()
		lifecycle.Revoke()
		if workerErr != nil && !errors.Is(workerErr, context.Canceled) {
			err = errors.Join(err, fmt.Errorf("controlled source worker failed: %w", workerErr))
		}
		raw, readErr := os.ReadFile(logPath)
		if readErr == nil {
			evidence.AttemptOutcomes, readErr = commitAttemptOutcomes(raw, runner.report.Namespace)
		}
		err = errors.Join(err, readErr)
	}()
	if _, err = fmt.Fprintln(runner.options.Output, "commit-ready: normal source owners joined; a controlled production lifecycle will lose only this correction's COMMIT request or reply."); err != nil {
		return err
	}
	if err = runner.checkpoint(ctx, CommitReady); err != nil {
		return err
	}
	if err = runner.load(ctx, "correction"); err != nil {
		return err
	}
	records, err := broker.Scan(ctx, runner.options.Brokers, runner.manifest, []int64{4, 0})
	if err != nil || len(records) != 4 || records[3].Partition != 0 || records[3].Offset != 3 {
		return fmt.Errorf("correction source boundary unavailable: %v", err)
	}
	input <- records[3]
	var observation commitObservation
	select {
	case observation = <-controlled.observed:
	case <-done:
		return fmt.Errorf("source worker stopped before unknown-outcome recovery: %v", workerErr)
	case <-ctx.Done():
		return ctx.Err()
	}
	evidence.BackendBefore, evidence.BackendAfter, evidence.OriginalError = observation.backendBefore, observation.backendAfter, observation.originalError
	evidence.Token, evidence.Observed = observation.token, observation.partition
	committed := controlled.mode == "reply"
	if !loss.fired.Load() || loss.armed.Load() != 0 || loss.committed.Load() != committed || evidence.BackendBefore == evidence.BackendAfter || loss.dials.Load() < 2 || observation.token.Epoch <= baselinePartition.Epoch {
		return fmt.Errorf("socket loss and fresh-connection recovery were not established")
	}
	if err = runner.until(ctx, "faulted PostgreSQL backend ended", func() (bool, error) {
		var alive bool
		err := runner.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)", evidence.BackendBefore).Scan(&alive)
		return !alive, err
	}); err != nil {
		return err
	}
	token, partition, err := runner.state.Observe(ctx, 0)
	if err != nil {
		return err
	}
	wantNext, wantSequence := int64(3), int64(5)
	if committed {
		wantNext, wantSequence = 4, 10
	}
	if token != evidence.Token || partition != evidence.Observed || partition.Next != wantNext || partition.Sequence != wantSequence {
		return fmt.Errorf("independent durable observation disagrees with the selected COMMIT outcome")
	}
	observed, err := runner.durableState(ctx)
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(runner.report.Directory, "commit-observed.json"), observed, 0600); err != nil {
		return err
	}
	if !committed && !bytes.Equal(before, observed) {
		return fmt.Errorf("lost COMMIT request left partial durable state")
	}
	view, err := runner.state.View(ctx)
	if err != nil || !reflect.DeepEqual(view, baselineView) {
		return fmt.Errorf("view changed before the stopped relay resumed: %v", err)
	}
	outputEnd, err := broker.ValidateOutput(ctx, runner.client, *runner.manifest.Output, runner.manifest.Config)
	if err != nil || !slices.Equal(outputEnd, []int64{5, 0}) {
		return fmt.Errorf("output advanced before recovery: %v %v", outputEnd, err)
	}
	if committed {
		if err = runner.checkCommittedCorrection(ctx); err != nil {
			return err
		}
	}
	report, err := runner.verifyIncomplete(ctx, "commit-unknown", "5s")
	if err != nil {
		return err
	}
	checks := verify.Checks{WritersStopped: true, SourceScanned: true, WorkersReached: committed}
	reason := "workers have not reached source boundary"
	if committed {
		reason = "outbox"
	}
	if report.Status != "INCOMPLETE" || report.Checks != checks || !slices.Equal(report.SourceEnd, []int64{4, 0}) || len(report.OutputEnd) != 0 || !strings.Contains(report.Reason, reason) {
		return fmt.Errorf("unknown-outcome stage did not preserve its incomplete boundary: %+v", report)
	}
	for _, key := range report.Keys {
		if key.Compared || key.Agrees {
			return fmt.Errorf("unknown-outcome stage certified an incomplete key")
		}
	}
	if err = runner.inspectStage(ctx, "commit-unknown"); err != nil {
		return err
	}
	if _, err = fmt.Fprintf(runner.options.Output, "commit-unknown: original storage outcome unknown; independent durable frontier %d and sequence %d after %s loss. Verifier INCOMPLETE; recovery read is paused without a SQL transaction.\n", wantNext, wantSequence, controlled.mode); err != nil {
		return err
	}
	if err = runner.checkpoint(ctx, CommitUnknown); err != nil {
		return err
	}
	evidence.RecoveryReleased = true
	close(controlled.release)
	select {
	case <-done:
		if workerErr != nil {
			return workerErr
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	stop()
	raw, err := os.ReadFile(logPath)
	if err != nil {
		return err
	}
	outcomes, err := commitAttemptOutcomes(raw, runner.report.Namespace)
	if err != nil || !slices.Equal(outcomes, commitExpectedOutcomes(controlled.mode)) {
		return fmt.Errorf("recovery changed the original unknown attempt or retried incorrectly: %v %v", outcomes, err)
	}
	if err = runner.checkCommittedCorrection(ctx); err != nil {
		return err
	}
	recovered, err := runner.durableState(ctx)
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(runner.report.Directory, "commit-recovered.json"), recovered, 0600); err != nil {
		return err
	}
	if committed && !bytes.Equal(observed, recovered) {
		return fmt.Errorf("recovery repeated an already committed decision")
	}
	evidence.Recovered = true
	_, err = fmt.Fprintln(runner.options.Output, "commit-recovered: production recovery kept the original unknown observation, inspected durable progress, and committed only one correction transition; delivery and strict verification follow.")
	return err
}

func commitExpectedOutcomes(mode string) []string {
	if mode == "request" {
		return []string{"unknown", "acknowledged"}
	}
	return []string{"unknown"}
}

func commitAttemptOutcomes(raw []byte, namespace string) ([]string, error) {
	events, err := readWorkerEvents(raw)
	if err != nil {
		return nil, err
	}
	var outcomes []string
	for _, event := range events {
		if event.Event != worker.ProcessingAttemptEvent {
			continue
		}
		if event.Namespace != namespace || *event.Partition != 0 || *event.Offset != 3 {
			return nil, fmt.Errorf("unexpected processing attempt in commit drill")
		}
		outcomes = append(outcomes, event.Outcome)
	}
	return outcomes, nil
}

func (runner *runner) checkCommittedCorrection(ctx context.Context) error {
	snapshot, err := runner.state.Snapshot(ctx)
	if err != nil {
		return err
	}
	expected, err := fixtures.Expected("expected-after.json")
	if err != nil {
		return err
	}
	key := runner.manifest.Config.Keys[0].StockKey
	if got := snapshot.Partitions[0].Next; got != 4 {
		return fmt.Errorf("unknown correction source frontier: got %d; want 4", got)
	}
	if got := snapshot.Partitions[0].Sequence; got != 10 {
		return fmt.Errorf("unknown correction emission sequence: got %d; want 10", got)
	}
	if got := len(snapshot.Outbox); got != 10 {
		return fmt.Errorf("unknown correction outbox rows: got %d; want 10", got)
	}
	if got := len(snapshot.Outputs); got != 6 {
		return fmt.Errorf("unknown correction output identities: got %d; want 6", got)
	}
	if got := len(snapshot.Rejections); got != 0 {
		return fmt.Errorf("unknown correction quarantined records: got %d; want 0", got)
	}
	if got := snapshot.Keys[key].Projection; !reflect.DeepEqual(got, expected) {
		return fmt.Errorf("unknown correction key %v differs from expected-after.json: got %+v; want %+v", key, got, expected)
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
		id := ledger.OutputID{Namespace: runner.report.Namespace, Family: expected.family, Key: key, Day: expected.day}
		row := snapshot.Outputs[id]
		if row.Version != expected.version || row.Payload.Present != expected.present {
			return fmt.Errorf("correction output guard differs from fixed expectation for %s: got version=%d present=%t; want version=%d present=%t",
				id.String(), row.Version, row.Payload.Present, expected.version, expected.present)
		}
	}
	return nil
}
