// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sorrell-consulting/amends/internal/diff"
	"github.com/sorrell-consulting/amends/internal/fold"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
)

// ProcessingTx owns the partition row lock until Commit or Rollback. It accepts
// one record and is not safe for concurrent use. Always defer Rollback.
type ProcessingTx struct {
	tx          pgx.Tx
	ctx         context.Context
	cancel      context.CancelFunc
	store       *Store
	token       store.Token
	partition   store.Partition
	applied     bool
	failed      error
	closed      bool
	started     time.Time
	measurement ProcessingMeasurement
}

// Begin locks token's partition and checks its namespace, configuration, epoch,
// and expected exclusive source frontier. Incompatible guards return
// [store.ErrFence]. The returned transaction accepts one record; callers must
// defer [ProcessingTx.Rollback] and finish within the caller and adapter deadlines.
func (s *Store) Begin(ctx context.Context, token store.Token, expected int64) (*ProcessingTx, error) {
	started := time.Now()
	tx, ctx, cancel, err := begin(ctx, s.pool, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	lockStarted := time.Now()
	p, err := s.lock(ctx, tx, token.Partition)
	lockAcquire := time.Since(lockStarted)
	if err == nil && (token.Namespace != s.config.Namespace || token.Digest != s.digest || token.Epoch < 1 || token.Epoch != p.Epoch || expected != p.Next) {
		err = store.ErrFence
	}
	if err != nil {
		rollback(tx)
		cancel()
		return nil, err
	}
	return &ProcessingTx{tx: tx, ctx: ctx, cancel: cancel, store: s, token: token, partition: p, started: started,
		measurement: ProcessingMeasurement{Epoch: token.Epoch, LockAcquireNS: lockAcquire.Nanoseconds()}}, nil
}

// Apply writes one record's decision inside the open transaction without
// committing it. Accepted input updates authority, projection, and output intent;
// invalid business input writes quarantine instead. Both advance source progress
// and measurements together on commit. Rejection is a successful decision.
//
// r must belong to the claimed source partition and have an offset at or beyond
// the guarded frontier. Any error poisons the transaction, so [ProcessingTx.Commit]
// will roll it back. Apply may succeed only once per transaction.
func (t *ProcessingTx) Apply(r ledger.Record) (err error) {
	if t.closed {
		return pgx.ErrTxClosed
	}
	if t.failed != nil {
		return t.failed
	}
	defer func() {
		if err != nil {
			t.failed = err
		}
	}()
	if t.applied {
		return fmt.Errorf("processing transaction already has a record")
	}
	s := t.store
	p := t.token.Partition
	if r.Source != s.config.Source || r.Partition != p || r.Offset < t.partition.Next || r.Offset == math.MaxInt64 {
		return fmt.Errorf("incompatible source coordinates")
	}
	transaction, reason := fold.Validate(s.config, r)
	t.measurement.Source = r.Coordinates
	t.measurement.QuarantineReason = reason
	if reason != "" {
		raw, err := json.Marshal(r)
		if err != nil {
			return err
		}
		if _, err = t.tx.Exec(t.ctx, "INSERT INTO amends_quarantine(namespace_id,partition,source_offset,record,reason) VALUES($1,$2,$3,$4,$5)", s.ns, p, r.Offset, raw, reason); err != nil {
			return err
		}
	} else {
		var raw []byte
		if err = t.tx.QueryRow(t.ctx, "SELECT state FROM amends_key_state WHERE namespace_id=$1 AND key_id=$2 AND partition=$3", s.ns, keyID(transaction.StockKey), p).Scan(&raw); err != nil {
			return err
		}
		previous, err := decodeKeyState(raw)
		if err != nil {
			return err
		}
		key, _ := s.config.Key(transaction.StockKey)
		next, work, err := fold.ApplyMeasured(previous, transaction, r.Coordinates, key)
		if err != nil {
			return err
		}
		t.measurement.Work = work
		//TODO: Do we actually need to load every output in this key's partition? I don't think so.
		outputs, err := loadEnvelopes(t.ctx, t.tx, s.ns, p, "amends_output_state")
		if err != nil {
			return err
		}
		messages, err := diff.Reconcile(s.config.Namespace, outputs, next.Projection, ledger.Cause{Coordinates: r.Coordinates, TxnID: transaction.TxnID, Revision: transaction.Revision})
		if err != nil {
			return err
		}
		if int64(len(messages)) > math.MaxInt64-t.partition.Sequence {
			return fmt.Errorf("emission sequence overflow")
		}
		raw, err = encodeKeyState(next)
		if err != nil {
			return err
		}
		if _, err = t.tx.Exec(t.ctx, "UPDATE amends_key_state SET state=$3 WHERE namespace_id=$1 AND key_id=$2", s.ns, keyID(transaction.StockKey), raw); err != nil {
			return err
		}
		for _, e := range messages {
			if e.Type == "KeyStatusStated" {
				t.measurement.StatusOutputs++
			} else {
				t.measurement.BusinessOutputs++
			}
			raw, err = json.Marshal(e)
			if err != nil {
				return err
			}
			t.partition.Sequence++
			if _, err = t.tx.Exec(t.ctx, `INSERT INTO amends_output_state(namespace_id,output_id,partition,version,envelope) VALUES($1,$2,$3,$4,$5)
				ON CONFLICT(namespace_id,output_id) DO UPDATE SET version=EXCLUDED.version,envelope=EXCLUDED.envelope`, s.ns, e.ID.String(), p, e.Version, raw); err != nil {
				return err
			}
			if _, err = t.tx.Exec(t.ctx, "INSERT INTO amends_outbox(namespace_id,partition,sequence,output_id,version,envelope) VALUES($1,$2,$3,$4,$5,$6)", s.ns, p, t.partition.Sequence, e.ID.String(), e.Version, raw); err != nil {
				return err
			}
		}
	}
	if err = t.recordProgress(r.Offset + 1); err != nil {
		return err
	}
	t.applied = true
	return nil
}

// recordProgress finalizes the source frontier and measurements in the same
// transaction as the decision. The partition row lock is still held; a failed
// read, encoding, or write propagates through Apply's transaction poison guard.
func (t *ProcessingTx) recordProgress(next int64) error {
	s, p := t.store, t.token.Partition
	var statisticsRaw []byte
	if err := t.tx.QueryRow(t.ctx, "SELECT processing_statistics FROM amends_partitions WHERE namespace_id=$1 AND partition=$2", s.ns, p).Scan(&statisticsRaw); err != nil {
		return err
	}
	var statistics ProcessingStatistics
	if statisticsRaw != nil {
		if err := json.Unmarshal(statisticsRaw, &statistics); err != nil {
			return err
		}
	}
	t.measurement.ElapsedBeforeCommitNS = time.Since(t.started).Nanoseconds()
	t.measurement.SampledAt = time.Now().UTC()
	measurement, err := json.Marshal(t.measurement)
	if err != nil {
		return err
	}
	statistics.record(t.measurement)
	statisticsRaw, err = json.Marshal(statistics)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec(t.ctx, `UPDATE amends_partitions SET next_offset=$3,emission_sequence=$4,last_processing_measurement=$5,processing_statistics=$6
		WHERE namespace_id=$1 AND partition=$2`, s.ns, p, next, t.partition.Sequence, measurement, statisticsRaw)
	return err
}

// Commit commits the applied decision and closes the transaction. A failed or
// missing Apply causes rollback. An error matching [store.ErrCommitUnknown]
// does not establish rollback: observe durable ownership and source progress
// before deciding whether to process the record again. The transaction itself
// cannot be reused, and calling Rollback cannot resolve an unknown commit.
func (t *ProcessingTx) Commit() error {
	if t.closed {
		return pgx.ErrTxClosed
	}
	if t.failed != nil || !t.applied {
		err := t.failed
		if err == nil {
			err = fmt.Errorf("no processing decision")
		}
		t.Rollback()
		return err
	}
	t.closed = true
	defer t.cancel()
	return commit(t.ctx, t.tx)
}

// Rollback aborts an open transaction and releases its resources. It is safe to
// defer after Begin and call after Commit; it cannot undo committed effects.
func (t *ProcessingTx) Rollback() {
	if t.closed {
		return
	}
	t.closed = true
	rollback(t.tx)
	t.cancel()
}

// Process applies and commits one record under token and the expected exclusive
// source frontier. It wraps [Store.Begin], [ProcessingTx.Apply], and
// [ProcessingTx.Commit], including rollback cleanup. A nil error may represent
// durable quarantine rather than accepted business input. On
// [store.ErrCommitUnknown], observe durable ownership and progress before retrying.
func (s *Store) Process(ctx context.Context, token store.Token, expected int64, r ledger.Record) error {
	tx, err := s.Begin(ctx, token, expected)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = tx.Apply(r); err != nil {
		return err
	}
	return tx.Commit()
}

// table is an adapter-owned constant, never caller input.
func loadEnvelopes(ctx context.Context, tx pgx.Tx, ns string, partition int, table string) (map[ledger.OutputID]ledger.Envelope, error) {
	if table != "amends_output_state" && table != "amends_view_rows" {
		return nil, fmt.Errorf("invalid envelope relation")
	}
	rows, err := tx.Query(ctx, "SELECT envelope FROM "+table+" WHERE namespace_id=$1 AND partition=$2", ns, partition)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[ledger.OutputID]ledger.Envelope{}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		var e ledger.Envelope
		if err = json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		result[e.ID] = e
	}
	return result, rows.Err()
}
