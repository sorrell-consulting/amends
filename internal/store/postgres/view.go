// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/view"
)

// ApplyView serializes a partition's version decision and progress on its own
// row. Protocol diagnostics commit without advancing rows or the frontier.
func (s *Store) ApplyView(ctx context.Context, partition int, expected, offset int64, envelope ledger.Envelope) error {
	return s.ApplyViewInput(ctx, expected, store.ViewInput{Partition: partition, Offset: offset, Envelope: envelope})
}

// ApplyViewInput commits an output version decision and progress together.
// expected is the saved exclusive output frontier; input.Offset may be greater
// when broker positions have gaps. Valid duplicates and older versions advance
// progress while preserving the retained envelope, including any withdrawal.
//
// Invalid input or equal-version conflicts commit a stopped-partition diagnostic
// and return [store.ErrProtocol] without advancing rows or progress. Stale caller
// guards return [store.ErrFence] without installing a diagnostic. If the commit
// outcome is [store.ErrCommitUnknown], call [Store.ViewPosition] before retrying.
func (s *Store) ApplyViewInput(ctx context.Context, expected int64, input store.ViewInput) error {
	partition, offset, envelope := input.Partition, input.Offset, input.Envelope
	tx, ctx, cancel, err := begin(ctx, s.pool, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer cancel()
	defer rollback(tx)
	if err = s.compatible(ctx, tx); err != nil {
		return err
	}
	state := view.New(s.config)
	var stopped *string
	var next int64
	if err = tx.QueryRow(ctx, "SELECT next_offset,protocol_error FROM amends_view_progress WHERE namespace_id=$1 AND partition=$2 FOR UPDATE", s.ns, partition).Scan(&next, &stopped); err != nil {
		return err
	}
	state.Next[partition] = next
	if stopped != nil {
		return fmt.Errorf("%w: %s", store.ErrProtocol, *stopped)
	}
	if partition < 0 || partition >= s.config.Partitions || next != expected || offset < expected || offset == math.MaxInt64 {
		return store.ErrFence
	}
	// Only this output ID is needed to make the version decision.
	var raw []byte
	err = tx.QueryRow(ctx, "SELECT envelope FROM amends_view_rows WHERE namespace_id=$1 AND output_id=$2", s.ns, envelope.ID.String()).Scan(&raw)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if err == nil {
		var old ledger.Envelope
		if err = json.Unmarshal(raw, &old); err != nil {
			return err
		}
		state.Rows[old.ID] = old
	}
	if input.WireError != "" {
		state.Errors[partition] = input.WireError
		err = errors.New(input.WireError)
	} else {
		err = state.Apply(partition, expected, offset, envelope)
	}
	if err != nil {
		message := state.Errors[partition]
		if message == "" {
			return err
		} // A stale caller/frontier is not poison output.
		if _, err = tx.Exec(ctx, "UPDATE amends_view_progress SET protocol_error=$3,error_offset=$4,error_key=$5,error_value=$6 WHERE namespace_id=$1 AND partition=$2", s.ns, partition, message, offset, input.Key, input.Value); err != nil {
			return err
		}
		if err = commit(ctx, tx); err != nil {
			return err
		}
		return fmt.Errorf("%w: %s", store.ErrProtocol, message)
	}
	kept := state.Rows[envelope.ID]
	raw, err = json.Marshal(kept)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO amends_view_rows(namespace_id,output_id,partition,version,present,envelope) VALUES($1,$2,$3,$4,$5,$6)
		ON CONFLICT(namespace_id,output_id) DO UPDATE SET version=EXCLUDED.version,present=EXCLUDED.present,envelope=EXCLUDED.envelope`, s.ns, kept.ID.String(), partition, kept.Version, kept.Payload.Present, raw); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "UPDATE amends_view_progress SET next_offset=$3 WHERE namespace_id=$1 AND partition=$2", s.ns, partition, state.Next[partition]); err != nil {
		return err
	}
	return commit(ctx, tx)
}

// ViewPosition reads a partition's durable next output offset and protocol stop.
// It is the recovery observation after an unknown [Store.ApplyViewInput] outcome.
func (s *Store) ViewPosition(ctx context.Context, partition int) (store.ViewPosition, error) {
	var position store.ViewPosition
	tx, ctx, cancel, err := begin(ctx, s.pool, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return position, err
	}
	defer cancel()
	defer rollback(tx)
	if err = s.compatible(ctx, tx); err != nil {
		return position, err
	}
	err = tx.QueryRow(ctx, "SELECT next_offset,COALESCE(protocol_error,'') FROM amends_view_progress WHERE namespace_id=$1 AND partition=$2", s.ns, partition).Scan(&position.Next, &position.Error)
	return position, err
}

// View returns values, deletion guards, diagnostics, and progress from one
// repeatable-read snapshot. It does not by itself establish a broker boundary.
func (s *Store) View(ctx context.Context) (view.State, error) {
	state := view.New(s.config)
	tx, ctx, cancel, err := begin(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return state, err
	}
	defer cancel()
	defer rollback(tx)
	if err = s.compatible(ctx, tx); err != nil {
		return state, err
	}
	for p := 0; p < s.config.Partitions; p++ {
		var stopped *string
		var next int64
		if err = tx.QueryRow(ctx, "SELECT next_offset,protocol_error FROM amends_view_progress WHERE namespace_id=$1 AND partition=$2", s.ns, p).Scan(&next, &stopped); err != nil {
			return state, err
		}
		state.Next[p] = next
		if stopped != nil {
			state.Errors[p] = *stopped
		}
		rows, err := loadEnvelopes(ctx, tx, s.ns, p, "amends_view_rows")
		if err != nil {
			return state, err
		}
		for id, e := range rows {
			state.Rows[id] = e
		}
	}
	return state, nil
}
