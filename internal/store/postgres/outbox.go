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
	"github.com/sorrell-consulting/amends/internal/store"
)

// Pending returns the exact committed wire bytes. A relay must publish Bytes,
// rather than deriving a new envelope or reserializing a changed cause.
// It selects the earliest unacknowledged sequence in partition. The boolean is
// false with a nil error when nothing is pending; the read does not reserve a row.
func (s *Store) Pending(ctx context.Context, partition int) (store.Delivery, bool, error) {
	d := store.Delivery{Partition: partition}
	if partition < 0 || partition >= s.config.Partitions {
		return d, false, store.ErrFence
	}
	tx, ctx, cancel, err := begin(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return d, false, err
	}
	defer cancel()
	defer rollback(tx)
	if err = s.compatible(ctx, tx); err != nil {
		return d, false, err
	}
	err = tx.QueryRow(ctx, "SELECT sequence,envelope,created_at FROM amends_outbox WHERE namespace_id=$1 AND partition=$2 AND acknowledged_at IS NULL ORDER BY sequence LIMIT 1", s.ns, partition).Scan(&d.Sequence, &d.Bytes, &d.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return d, false, nil
	}
	if err != nil {
		return d, false, err
	}
	if err = json.Unmarshal(d.Bytes, &d.Envelope); err != nil {
		return d, false, err
	}
	return d, true, nil
}

// Acknowledge records an observed publication coordinate, not view application.
// Repeating the same receipt is safe after an unknown database commit outcome.
// A new receipt must match the earliest pending sequence. On
// [store.ErrCommitUnknown], use [Store.Receipt] before retrying the same receipt.
func (s *Store) Acknowledge(ctx context.Context, partition int, sequence, outputOffset int64) error {
	if sequence < 1 || outputOffset < 0 || outputOffset == math.MaxInt64 {
		return fmt.Errorf("invalid publication receipt")
	}
	tx, ctx, cancel, err := begin(ctx, s.pool, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer cancel()
	defer rollback(tx)
	if _, err = s.lock(ctx, tx, partition); err != nil {
		return err
	}
	var old *int64
	if err = tx.QueryRow(ctx, "SELECT output_offset FROM amends_outbox WHERE namespace_id=$1 AND partition=$2 AND sequence=$3", s.ns, partition, sequence).Scan(&old); err != nil {
		return err
	}
	if old != nil {
		if *old != outputOffset {
			return fmt.Errorf("conflicting publication receipt")
		}
		return commit(ctx, tx)
	}
	var first int64
	if err = tx.QueryRow(ctx, "SELECT sequence FROM amends_outbox WHERE namespace_id=$1 AND partition=$2 AND acknowledged_at IS NULL ORDER BY sequence LIMIT 1", s.ns, partition).Scan(&first); err != nil {
		return err
	}
	if first != sequence {
		return fmt.Errorf("outbox acknowledgement out of order")
	}
	var last int64
	if err = tx.QueryRow(ctx, "SELECT COALESCE(MAX(output_offset),-1) FROM amends_outbox WHERE namespace_id=$1 AND partition=$2", s.ns, partition).Scan(&last); err != nil {
		return err
	}
	if outputOffset <= last {
		return fmt.Errorf("publication receipt does not advance")
	}
	if _, err = tx.Exec(ctx, "UPDATE amends_outbox SET output_offset=$4,acknowledged_at=clock_timestamp() WHERE namespace_id=$1 AND partition=$2 AND sequence=$3", s.ns, partition, sequence, outputOffset); err != nil {
		return err
	}
	return commit(ctx, tx)
}
