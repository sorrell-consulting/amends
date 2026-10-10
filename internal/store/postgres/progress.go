// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
)

// PartitionProgress reports source and view next offsets, committed emission
// sequence, pending publication, and retained diagnostics for one partition.
// SourceNext and ViewNext are exclusive broker positions; LastReceipt is the
// greatest acknowledged publication offset. A nil OldestPending or LastReceipt
// means no pending row or publication receipt. Nil measurements/statistics mean
// no instrumented decision has been retained, not zero processing cost.
type PartitionProgress struct {
	Partition        int                    `json:"partition"`
	Epoch            int64                  `json:"epoch"`
	SourceNext       int64                  `json:"source_next"`
	EmissionSequence int64                  `json:"emission_sequence"`
	Pending          int64                  `json:"pending_outbox"`
	OldestPending    *time.Time             `json:"oldest_pending_at"`
	LastReceipt      *int64                 `json:"last_publication_offset"`
	ViewNext         int64                  `json:"view_next"`
	ProtocolError    string                 `json:"protocol_error,omitempty"`
	Quarantined      int64                  `json:"quarantined"`
	LastProcessing   *ProcessingMeasurement `json:"last_processing"`
	Statistics       *ProcessingStatistics  `json:"processing_statistics"`
}

// Progress groups partition observations from one SQL snapshot. ObservedAt is
// the sampling time; broker ends and independent verification are separate reads.
type Progress struct {
	ObservedAt time.Time           `json:"observed_at"`
	Partitions []PartitionProgress `json:"partitions"`
}

// Progress observes processing, receipts, and view positions in one SQL
// snapshot. Broker end positions are separate observations, not part of it.
func (s *Store) Progress(ctx context.Context) (Progress, error) {
	var result Progress
	tx, ctx, cancel, err := begin(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return result, err
	}
	defer cancel()
	defer rollback(tx)
	if err = s.compatible(ctx, tx); err != nil {
		return result, err
	}
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&result.ObservedAt); err != nil {
		return result, err
	}
	for p := 0; p < s.config.Partitions; p++ {
		r := PartitionProgress{Partition: p}
		var measurement, statistics []byte
		err = tx.QueryRow(ctx, `SELECT p.epoch,p.next_offset,p.emission_sequence,v.next_offset,COALESCE(v.protocol_error,''),
		 (SELECT COUNT(*) FROM amends_outbox o WHERE o.namespace_id=p.namespace_id AND o.partition=p.partition AND o.acknowledged_at IS NULL),
		 (SELECT MIN(created_at) FROM amends_outbox o WHERE o.namespace_id=p.namespace_id AND o.partition=p.partition AND o.acknowledged_at IS NULL),
		 (SELECT MAX(output_offset) FROM amends_outbox o WHERE o.namespace_id=p.namespace_id AND o.partition=p.partition),
		 (SELECT COUNT(*) FROM amends_quarantine q WHERE q.namespace_id=p.namespace_id AND q.partition=p.partition),
		 p.last_processing_measurement,p.processing_statistics
		 FROM amends_partitions p JOIN amends_view_progress v USING(namespace_id,partition)
		 WHERE p.namespace_id=$1 AND p.partition=$2`, s.ns, p).Scan(&r.Epoch, &r.SourceNext, &r.EmissionSequence, &r.ViewNext, &r.ProtocolError, &r.Pending, &r.OldestPending, &r.LastReceipt, &r.Quarantined, &measurement, &statistics)
		if err != nil {
			return result, err
		}
		if measurement != nil {
			if err = json.Unmarshal(measurement, &r.LastProcessing); err != nil {
				return result, err
			}
		}
		if statistics != nil {
			if err = json.Unmarshal(statistics, &r.Statistics); err != nil {
				return result, err
			}
		}
		result.Partitions = append(result.Partitions, r)
	}
	return result, nil
}
