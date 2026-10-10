package showcase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/sorrell-consulting/amends/internal/verify"
)

type MaterializerCrash struct {
	Boundary                string          `json:"boundary"`
	OutputID                ledger.OutputID `json:"locked_output_id"`
	ViewPID                 int             `json:"view_process_pid"`
	BackendPID              int32           `json:"view_backend_pid"`
	BlockerPID              int32           `json:"blocker_backend_pid"`
	ApplicationName         string          `json:"application_name"`
	TransactionID           string          `json:"transaction_id"`
	Query                   string          `json:"blocked_query"`
	Signal                  string          `json:"signal"`
	BlockedAfterKill        bool            `json:"transaction_still_blocked_after_kill"`
	TransactionEnded        bool            `json:"transaction_ended"`
	ViewUnchanged           bool            `json:"precommit_view_unchanged"`
	RecoveryPID             int             `json:"recovery_process_pid"`
	CommittedNext           int64           `json:"committed_view_next"`
	CommittedTombstone      ledger.Envelope `json:"committed_tombstone"`
	CommittedSignal         string          `json:"postcommit_signal,omitempty"`
	CommittedStateUnchanged bool            `json:"postcommit_view_unchanged"`
	RestartPID              int             `json:"postcommit_restart_pid"`
}

func (runner *runner) crashMaterializer(ctx context.Context, id ledger.OutputID) (err error) {
	if err = ctx.Err(); err != nil {
		return err
	}
	evidence := &MaterializerCrash{Boundary: "Day 3 view-row upsert before withdrawal COMMIT, then process death after independently verified COMMIT", OutputID: id, ViewPID: runner.view.cmd.Process.Pid, ApplicationName: runner.report.Namespace + "-" + runner.view.name}
	runner.report.MaterializerCrash = evidence
	before, err := runner.viewSnapshot(ctx, "view-precommit-before.json")
	if err != nil {
		return err
	}
	barrier, err := runner.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rollbackErr := barrier.Rollback(cleanup)
		if !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			err = errors.Join(err, rollbackErr)
		}
	}()
	namespace, err := json.Marshal(runner.report.Namespace)
	if err != nil {
		return err
	}
	var version int64
	var present bool
	if err = barrier.QueryRow(ctx, "SELECT version,present FROM amends_view_rows WHERE namespace_id=$1 AND output_id=$2 FOR UPDATE", string(namespace), id.String()).Scan(&version, &present); err != nil {
		return err
	}
	if version != 2 || !present {
		return fmt.Errorf("materializer crash requires the corrected Day 3 balance at version 2")
	}
	if err = barrier.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&evidence.BlockerPID); err != nil {
		return err
	}
	loader, err := runner.start("load-cancel-last-day", runner.options.ControlBinary, true, "source-load", "-brokers", strings.Join(runner.options.Brokers, ","), "-manifest", filepath.Join(runner.report.Directory, "manifest.json"), "-file", filepath.Join(runner.report.Directory, "cancel-last-day.jsonl"))
	if err != nil {
		return err
	}
	if err = runner.until(ctx, "withdrawal at view database barrier", func() (bool, error) {
		select {
		case <-loader.done:
			if loader.err != nil {
				return false, fmt.Errorf("cancellation producer: %w", loader.err)
			}
		default:
		}
		observeErr := runner.pool.QueryRow(ctx, `SELECT pid,backend_xid::text,query FROM pg_stat_activity
		 WHERE application_name=$1 AND $2=ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock' AND backend_xid IS NOT NULL`, evidence.ApplicationName, evidence.BlockerPID).Scan(&evidence.BackendPID, &evidence.TransactionID, &evidence.Query)
		if errors.Is(observeErr, pgx.ErrNoRows) {
			return false, nil
		}
		if observeErr != nil {
			return false, observeErr
		}
		if !strings.HasPrefix(evidence.Query, "INSERT INTO amends_view_rows") {
			return false, fmt.Errorf("view blocked at unexpected SQL: %s", evidence.Query)
		}
		if killErr := runner.view.kill(); killErr != nil {
			return false, killErr
		}
		evidence.Signal = "SIGKILL"
		return true, nil
	}); err != nil {
		return err
	}
	if err = runner.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
	 WHERE pid=$1 AND backend_xid::text=$2 AND $3=ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock')`, evidence.BackendPID, evidence.TransactionID, evidence.BlockerPID).Scan(&evidence.BlockedAfterKill); err != nil {
		return err
	}
	if !evidence.BlockedAfterKill {
		return fmt.Errorf("materializer crash timing inconclusive: transaction no longer blocked after SIGKILL")
	}
	if err = loader.wait(ctx); err != nil {
		return fmt.Errorf("cancellation producer: %w", err)
	}
	if err = runner.until(ctx, "killed materializer transaction ends", func() (bool, error) {
		var active bool
		observeErr := runner.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND backend_xid::text=$2)", evidence.BackendPID, evidence.TransactionID).Scan(&active)
		return !active, observeErr
	}); err != nil {
		return err
	}
	evidence.TransactionEnded = true
	after, err := runner.viewSnapshot(ctx, "view-precommit-after.json")
	if err != nil {
		return err
	}
	evidence.ViewUnchanged = bytes.Equal(before, after)
	if !evidence.ViewUnchanged {
		return fmt.Errorf("precommit materializer crash changed rows, deletion guards, diagnostics, or progress")
	}
	if err = barrier.Rollback(ctx); err != nil {
		return err
	}
	if err = runner.awaitSourceOutbox(ctx, sourceExpectation{Next: 5}, sourceExpectation{}); err != nil {
		return err
	}
	if err = runner.relay.stop(); err != nil {
		return err
	}
	report, err := runner.verifyIncomplete(ctx, "materializer-crashed", "5s")
	if err != nil {
		return err
	}
	checks := verify.Checks{WritersStopped: true, SourceScanned: true, WorkersReached: true, PublicationDrained: true}
	if report.Status != "INCOMPLETE" || report.Checks != checks || report.BinarySHA256 == "" || !strings.Contains(report.Reason, "view has not reached output boundary") || !slices.Equal(report.SourceEnd, []int64{5, 0}) || !slices.Equal(report.OutputEnd, []int64{11, 0}) || len(report.Progress.Partitions) != 2 {
		return fmt.Errorf("crashed materializer did not retain incomplete view boundary: %+v", report)
	}
	for _, partition := range report.Progress.Partitions {
		viewNext := int64(0)
		if partition.Partition == 0 {
			viewNext = 10
		}
		if partition.ViewNext != viewNext || partition.Pending != 0 || partition.ProtocolError != "" {
			return fmt.Errorf("unexpected crashed view progress: %+v", partition)
		}
	}
	for _, key := range report.Keys {
		if key.Compared || key.Agrees {
			return fmt.Errorf("crashed materializer's incomplete view was certified")
		}
	}
	if err = runner.inspectStage(ctx, "materializer-crashed"); err != nil {
		return err
	}
	if _, err = fmt.Fprintf(runner.options.Output, "materializer-crashed: verifier INCOMPLETE; SIGKILL PID %d at observed SQL barrier; view rows/guards/progress unchanged at [10 0], H=[5 0] O=[11 0].\n", evidence.ViewPID); err != nil {
		return err
	}
	if err = runner.checkpoint(ctx, MaterializerKilled); err != nil {
		return err
	}
	if err = runner.restartMaterializer(ctx, "view-recovered"); err != nil {
		return err
	}
	evidence.RecoveryPID = runner.view.cmd.Process.Pid
	return nil
}

func (runner *runner) crashCommittedMaterializer(ctx context.Context, id ledger.OutputID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	evidence := runner.report.MaterializerCrash
	view, err := runner.state.View(ctx)
	if err != nil {
		return err
	}
	evidence.CommittedNext, evidence.CommittedTombstone = view.Next[0], view.Rows[id]
	if evidence.CommittedNext != 11 || view.Next[1] != 0 || evidence.CommittedTombstone.Version != 3 || evidence.CommittedTombstone.Type != "BalanceRetracted" || evidence.CommittedTombstone.Payload.Present {
		return fmt.Errorf("postcommit materializer crash requires durable withdrawal and view frontier 11")
	}
	before, err := runner.viewSnapshot(ctx, "view-postcommit-before.json")
	if err != nil {
		return err
	}
	if err = runner.view.kill(); err != nil {
		return err
	}
	evidence.CommittedSignal = "SIGKILL"
	if err = runner.awaitViewRelease(ctx); err != nil {
		return err
	}
	after, err := runner.viewSnapshot(ctx, "view-postcommit-after.json")
	if err != nil {
		return err
	}
	evidence.CommittedStateUnchanged = bytes.Equal(before, after)
	if !evidence.CommittedStateUnchanged {
		return fmt.Errorf("postcommit materializer crash changed durable view state")
	}
	_, err = fmt.Fprintf(runner.options.Output, "materializer-committed: SIGKILL PID %d after verified withdrawal COMMIT; deletion version 3 and view frontier 11 retained.\n", evidence.RecoveryPID)
	if err != nil {
		return err
	}
	return runner.checkpoint(ctx, MaterializerCommitKilled)
}

func (runner *runner) viewSnapshot(ctx context.Context, name string) ([]byte, error) {
	namespace, err := json.Marshal(runner.report.Namespace)
	if err != nil {
		return nil, err
	}
	var raw []byte
	if err = runner.pool.QueryRow(ctx, `SELECT jsonb_build_object(
	 'rows',(SELECT jsonb_agg(to_jsonb(t) ORDER BY output_id) FROM amends_view_rows t WHERE namespace_id=$1),
	 'progress',(SELECT jsonb_agg(to_jsonb(t) ORDER BY partition) FROM amends_view_progress t WHERE namespace_id=$1))`, string(namespace)).Scan(&raw); err != nil {
		return nil, err
	}
	return raw, os.WriteFile(filepath.Join(runner.report.Directory, name), raw, 0600)
}

func (runner *runner) awaitViewRelease(ctx context.Context) error {
	return runner.until(ctx, "stopped materializer releases its service guard", func() (bool, error) {
		guard, err := runner.state.AcquireService(ctx, "view")
		if errors.Is(err, postgres.ErrServiceBusy) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		guard.Close()
		return true, nil
	})
}

func (runner *runner) restartMaterializer(ctx context.Context, name string) error {
	if err := runner.awaitViewRelease(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var err error
	runner.view, err = runner.role(name, "view")
	return err
}
