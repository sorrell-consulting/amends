// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/sorrell-consulting/amends/internal/fold"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
)

// Snapshot reads processing state at one consistent database snapshot.
// The oracle must never use this as its source history.
func (s *Store) Snapshot(ctx context.Context) (store.Snapshot, error) {
	result := store.Snapshot{Config: s.config.Clone(), Partitions: map[int]store.Partition{}, Keys: map[ledger.StockKey]fold.State{}, Outputs: map[ledger.OutputID]ledger.Envelope{}}
	tx, ctx, cancel, err := begin(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return result, err
	}
	defer cancel()
	defer rollback(tx)
	if err = s.compatible(ctx, tx); err != nil {
		return result, err
	}
	for p := 0; p < s.config.Partitions; p++ {
		var state store.Partition
		if err = tx.QueryRow(ctx, "SELECT epoch,next_offset,emission_sequence FROM amends_partitions WHERE namespace_id=$1 AND partition=$2", s.ns, p).Scan(&state.Epoch, &state.Next, &state.Sequence); err != nil {
			return result, err
		}
		result.Partitions[p] = state
		envelopes, err := loadEnvelopes(ctx, tx, s.ns, p, "amends_output_state")
		if err != nil {
			return result, err
		}
		for id, e := range envelopes {
			result.Outputs[id] = e
		}
	}
	for _, k := range s.config.Keys {
		var raw []byte
		if err = tx.QueryRow(ctx, "SELECT state FROM amends_key_state WHERE namespace_id=$1 AND key_id=$2", s.ns, keyID(k.StockKey)).Scan(&raw); err != nil {
			return result, err
		}
		state, err := decodeKeyState(raw)
		if err != nil {
			return result, err
		}
		result.Keys[k.StockKey] = state
	}
	rows, err := tx.Query(ctx, "SELECT partition,sequence,envelope,acknowledged_at IS NOT NULL FROM amends_outbox WHERE namespace_id=$1 ORDER BY partition,sequence", s.ns)
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var emission store.Emission
		var raw []byte
		if err = rows.Scan(&emission.Partition, &emission.Sequence, &raw, &emission.Acknowledged); err != nil {
			rows.Close()
			return result, err
		}
		if err = json.Unmarshal(raw, &emission.Envelope); err != nil {
			rows.Close()
			return result, err
		}
		result.Outbox = append(result.Outbox, emission)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	rows, err = tx.Query(ctx, "SELECT record,reason FROM amends_quarantine WHERE namespace_id=$1 ORDER BY partition,source_offset", s.ns)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var rejection ledger.Rejection
		var raw []byte
		if err = rows.Scan(&raw, &rejection.Reason); err != nil {
			return result, err
		}
		if err = json.Unmarshal(raw, &rejection.Record); err != nil {
			return result, err
		}
		result.Rejections = append(result.Rejections, rejection)
	}
	return result, rows.Err()
}
