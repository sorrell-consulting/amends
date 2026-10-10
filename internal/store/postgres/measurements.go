package postgres

import (
	"math"
	"time"

	"github.com/sorrell-consulting/amends/internal/fold"
	"github.com/sorrell-consulting/amends/internal/ledger"
)

// ProcessingMeasurement describes one decision committed with its source frontier.
// It excludes relay and view work. LockAcquireNS measures client time acquiring
// the row lock and checking configuration, not isolated server wait.
// ElapsedBeforeCommitNS ends before the final progress write and COMMIT; SampledAt
// is that sampling instant, not a confirmed commit time. Failed attempts are absent.
type ProcessingMeasurement struct {
	Source                ledger.Coordinates `json:"source"`
	Epoch                 int64              `json:"epoch"`
	Work                  fold.Work          `json:"work"`
	BusinessOutputs       int64              `json:"business_outputs"`
	StatusOutputs         int64              `json:"status_outputs"`
	QuarantineReason      string             `json:"quarantine_reason,omitempty"`
	LockAcquireNS         int64              `json:"lock_acquire_ns"`
	ElapsedBeforeCommitNS int64              `json:"elapsed_before_commit_ns"`
	SampledAt             time.Time          `json:"sampled_at"`
}

// ProcessingHistoryLimit bounds retained per-partition measurement samples.
const ProcessingHistoryLimit = 32

// ProcessingStatistics covers only decisions made after statistics collection
// began. It is committed with source progress, not updated on attempt/replay.
// Timing fields sum the corresponding measurement fields. Counters saturate at
// MaxInt64 and set CountersSaturated; Recent retains the newest samples in order.
type ProcessingStatistics struct {
	FirstSource           ledger.Coordinates      `json:"first_source"`
	StartedAt             time.Time               `json:"started_at"`
	Decisions             int64                   `json:"decisions"`
	Duplicates            int64                   `json:"duplicates"`
	Stale                 int64                   `json:"stale"`
	Quarantined           int64                   `json:"quarantined"`
	Refolds               int64                   `json:"refolds"`
	RefoldedRecords       int64                   `json:"refolded_records"`
	BusinessOutputs       int64                   `json:"business_outputs"`
	StatusOutputs         int64                   `json:"status_outputs"`
	MaxRewindDays         int64                   `json:"max_rewind_days"`
	LockAcquireNS         int64                   `json:"lock_acquire_ns"`
	ElapsedBeforeCommitNS int64                   `json:"elapsed_before_commit_ns"`
	CountersSaturated     bool                    `json:"counters_saturated"`
	Recent                []ProcessingMeasurement `json:"recent"`
}

func (s *ProcessingStatistics) record(m ProcessingMeasurement) {
	if s.Decisions == 0 {
		s.FirstSource, s.StartedAt = m.Source, m.SampledAt
	}
	add := func(total *int64, n int64) {
		if n > math.MaxInt64-*total {
			*total, s.CountersSaturated = math.MaxInt64, true
		} else {
			*total += n
		}
	}
	add(&s.Decisions, 1)
	if m.Work.Duplicate {
		add(&s.Duplicates, 1)
	}
	if m.Work.Stale {
		add(&s.Stale, 1)
	}
	if m.QuarantineReason != "" {
		add(&s.Quarantined, 1)
	}
	if m.Work.Refold {
		add(&s.Refolds, 1)
	}
	add(&s.RefoldedRecords, m.Work.RefoldedRecords)
	add(&s.BusinessOutputs, m.BusinessOutputs)
	add(&s.StatusOutputs, m.StatusOutputs)
	add(&s.LockAcquireNS, m.LockAcquireNS)
	add(&s.ElapsedBeforeCommitNS, m.ElapsedBeforeCommitNS)
	s.MaxRewindDays = max(s.MaxRewindDays, m.Work.RewindDays)
	s.Recent = append(s.Recent, m)
	if len(s.Recent) > ProcessingHistoryLimit {
		copy(s.Recent, s.Recent[len(s.Recent)-ProcessingHistoryLimit:])
		s.Recent = s.Recent[:ProcessingHistoryLimit]
	}
}
