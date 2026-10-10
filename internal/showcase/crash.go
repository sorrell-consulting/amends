// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

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
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/ledger"
)

type Crash struct {
	Boundary              string          `json:"boundary"`
	OutputID              ledger.OutputID `json:"locked_output_id"`
	BlockerPID            int32           `json:"blocker_backend_pid"`
	WorkerPID             int             `json:"worker_process_pid"`
	BackendPID            int32           `json:"worker_backend_pid"`
	ApplicationName       string          `json:"application_name"`
	TransactionID         string          `json:"transaction_id"`
	Query                 string          `json:"blocked_query"`
	Signal                string          `json:"signal"`
	BlockedAfterKill      bool            `json:"transaction_still_blocked_after_kill"`
	TransactionEnded      bool            `json:"transaction_ended"`
	DurableStateUnchanged bool            `json:"durable_state_unchanged_except_epochs"`
	OutputUnchanged       bool            `json:"output_boundary_unchanged"`
	PreviousEpoch         int64           `json:"previous_epoch"`
	RecoveredEpoch        int64           `json:"recovered_epoch"`
}

func (r *runner) crashCorrection(ctx context.Context) (err error) {
	c := &Crash{Boundary: "output-state update of Day 2 balance, before correction COMMIT", OutputID: ledger.OutputID{Namespace: r.report.Namespace, Family: "balance", Key: r.manifest.Config.Keys[0].StockKey, Day: "2026-01-02"}}
	r.report.Crash = c
	_, p, err := r.state.Observe(ctx, 0)
	if err != nil {
		return err
	}
	c.PreviousEpoch = p.Epoch
	before, err := r.durableState(ctx)
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(r.report.Directory, "rollback-before.json"), before, 0600); err != nil {
		return err
	}
	// A separate connection owns the barrier, never the processing partition
	// row. ProcessingTx has written key state and earlier output/outbox rows
	// before it reaches this particular output ID (see processing.go/diff.go).
	barrier, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e := barrier.Rollback(cleanup)
		if !errors.Is(e, pgx.ErrTxClosed) {
			err = errors.Join(err, e)
		}
	}()
	ns, _ := json.Marshal(r.report.Namespace)
	var version int64
	if err = barrier.QueryRow(ctx, "SELECT version FROM amends_output_state WHERE namespace_id=$1 AND output_id=$2 FOR UPDATE", string(ns), c.OutputID.String()).Scan(&version); err != nil {
		return err
	}
	if err = barrier.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&c.BlockerPID); err != nil {
		return err
	}
	// Start, but do not wait for, the finite producer: observe and kill inside
	// the worker's 3-second SQL lock budget even if producer shutdown is slow.
	loader, err := r.start("load-correction", r.options.ControlBinary, true, "source-load", "-brokers", strings.Join(r.options.Brokers, ","), "-manifest", filepath.Join(r.report.Directory, "manifest.json"), "-file", filepath.Join(r.report.Directory, "correction.jsonl"))
	if err != nil {
		return err
	}
	var victim *process
	err = r.until(ctx, "correction at database barrier", func() (bool, error) {
		select {
		case <-loader.done:
			if loader.err != nil {
				return false, fmt.Errorf("correction producer: %w", loader.err)
			}
		default:
		}
		// Calling this in autocommit gets a fresh statistics snapshot. The
		// blocker relation and unique application name identify the owned child.
		rows, e := r.pool.Query(ctx, `SELECT pid,application_name,backend_xid::text,query FROM pg_stat_activity
		 WHERE $1=ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock' AND backend_xid IS NOT NULL`, c.BlockerPID)
		if e != nil {
			return false, e
		}
		defer rows.Close()
		for rows.Next() {
			var pid int32
			var app, xid, query string
			if e = rows.Scan(&pid, &app, &xid, &query); e != nil {
				return false, e
			}
			for _, child := range r.children {
				if child.stopped || !strings.HasPrefix(child.name, "worker-") || app != r.report.Namespace+"-"+child.name {
					continue
				}
				if !strings.HasPrefix(query, "INSERT INTO amends_output_state") {
					return false, fmt.Errorf("worker blocked at unexpected SQL: %s", query)
				}
				c.BackendPID, c.ApplicationName, c.TransactionID, c.Query = pid, app, xid, query
				c.WorkerPID = child.cmd.Process.Pid
				victim = child
				// Kill immediately, while the observed transaction is blocked.
				if e = child.kill(); e != nil {
					return false, e
				}
				c.Signal = "SIGKILL"
				return true, nil
			}
		}
		return false, rows.Err()
	})
	if err != nil {
		return err
	}
	// Confirm the same server transaction is still waiting after the child is
	// dead. This refuses an inconclusive observation near the lock deadline.
	if err = r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
	 WHERE pid=$1 AND backend_xid::text=$2 AND $3=ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock')`, c.BackendPID, c.TransactionID, c.BlockerPID).Scan(&c.BlockedAfterKill); err != nil {
		return err
	}
	if !c.BlockedAfterKill {
		return fmt.Errorf("crash timing inconclusive: transaction no longer blocked after SIGKILL")
	}
	if err = loader.wait(ctx); err != nil {
		return fmt.Errorf("correction producer: %w", err)
	}
	// The database may notice the dead client only when its lock timeout fires.
	// Keep the barrier through this observation; a replacement cannot commit
	// the correction while we compare the complete durable state.
	if err = r.until(ctx, "killed worker transaction ends", func() (bool, error) {
		var active bool
		e := r.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND backend_xid::text=$2)", c.BackendPID, c.TransactionID).Scan(&active)
		return !active, e
	}); err != nil {
		return err
	}
	c.TransactionEnded = true
	after, err := r.durableState(ctx)
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(r.report.Directory, "rollback-after.json"), after, 0600); err != nil {
		return err
	}
	if !bytes.Equal(before, after) {
		return fmt.Errorf("pre-commit worker crash changed durable state; inspect rollback snapshots")
	}
	c.DurableStateUnchanged = true
	ends, err := broker.ValidateOutput(ctx, r.client, *r.manifest.Output, r.manifest.Config)
	if err != nil {
		return err
	}
	if !slices.Equal(ends, r.report.Stages[0].OutputEnd) {
		return fmt.Errorf("output log advanced before correction committed: %v", ends)
	}
	c.OutputUnchanged = true
	if err = barrier.Rollback(ctx); err != nil {
		return err
	}
	if _, err = fmt.Fprintf(r.options.Output, "SIGKILL worker PID %d at observed SQL barrier; transaction ended; state, versions, outbox, quarantine, source frontier, output boundary, and view unchanged.\n", c.WorkerPID); err != nil {
		return err
	}
	if _, err = r.role(victim.name+"-replacement", "source"); err != nil {
		return err
	}
	return r.twoMembers(ctx)
}

// One SQL statement observes every correction-owned table at one MVCC
// snapshot. Only ownership epochs are omitted: surviving workers may claim a
// partition while the barrier prevents the correction from committing. Binary
// state and envelope columns are retained exactly as PostgreSQL bytea text.
func (r *runner) durableState(ctx context.Context) ([]byte, error) {
	ns, _ := json.Marshal(r.report.Namespace)
	const query = `SELECT jsonb_build_object(
	 'partitions',(SELECT jsonb_agg(to_jsonb(t)-'epoch' ORDER BY partition) FROM amends_partitions t WHERE namespace_id=$1),
	 'keys',(SELECT jsonb_agg(to_jsonb(t) ORDER BY key_id) FROM amends_key_state t WHERE namespace_id=$1),
	 'outputs',(SELECT jsonb_agg(to_jsonb(t) ORDER BY output_id) FROM amends_output_state t WHERE namespace_id=$1),
	 'outbox',(SELECT jsonb_agg(to_jsonb(t) ORDER BY partition,sequence) FROM amends_outbox t WHERE namespace_id=$1),
	 'quarantine',(SELECT jsonb_agg(to_jsonb(t) ORDER BY partition,source_offset) FROM amends_quarantine t WHERE namespace_id=$1),
	 'view_rows',(SELECT jsonb_agg(to_jsonb(t) ORDER BY output_id) FROM amends_view_rows t WHERE namespace_id=$1),
	 'view_progress',(SELECT jsonb_agg(to_jsonb(t) ORDER BY partition) FROM amends_view_progress t WHERE namespace_id=$1))`
	var raw []byte
	err := r.pool.QueryRow(ctx, query, string(ns)).Scan(&raw)
	return raw, err
}
