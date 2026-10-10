package postgres

import (
	"math"
	"testing"
	"time"

	"github.com/sorrell-consulting/amends/internal/fold"
	"github.com/sorrell-consulting/amends/internal/ledger"
)

func TestProcessingStatisticsBoundHistoryAndQualifyOverflow(t *testing.T) {
	var statistics ProcessingStatistics
	for offset := int64(0); offset <= ProcessingHistoryLimit; offset++ {
		statistics.record(ProcessingMeasurement{Source: ledger.Coordinates{Source: "retained", Offset: offset}, SampledAt: time.Unix(offset, 0), BusinessOutputs: 1})
	}
	if statistics.Decisions != 33 || statistics.BusinessOutputs != 33 || statistics.FirstSource.Offset != 0 || len(statistics.Recent) != 32 || statistics.Recent[0].Source.Offset != 1 || statistics.Recent[31].Source.Offset != 32 {
		t.Fatalf("history bound lost cumulative counts or retained wrong suffix: %+v", statistics)
	}
	statistics.ElapsedBeforeCommitNS = math.MaxInt64 - 1
	statistics.record(ProcessingMeasurement{Source: ledger.Coordinates{Offset: 33}, ElapsedBeforeCommitNS: 2, Work: fold.Work{Duplicate: true, Stale: true}})
	if !statistics.CountersSaturated || statistics.ElapsedBeforeCommitNS != math.MaxInt64 || statistics.Decisions != 34 || statistics.Duplicates != 1 || statistics.Stale != 1 || statistics.Recent[31].Source.Offset != 33 {
		t.Fatalf("counter overflow wrapped or stopped retaining history: %+v", statistics)
	}
}
