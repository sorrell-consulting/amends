//go:build integration

package postgres

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/sorrell-consulting/amends/internal/fold"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
)

func measured(test *testing.T, state *Store) *ProcessingMeasurement {
	test.Helper()
	progress, err := state.Progress(test.Context())
	if err != nil {
		test.Fatal(err)
	}
	measurement := progress.Partitions[0].LastProcessing
	if measurement != nil && (measurement.Source.Offset+1 != progress.Partitions[0].SourceNext || measurement.Epoch > progress.Partitions[0].Epoch || measurement.LockAcquireNS <= 0 || measurement.ElapsedBeforeCommitNS < measurement.LockAcquireNS || measurement.SampledAt.IsZero()) {
		test.Fatalf("measurement and progress disagree: %+v", progress.Partitions[0])
	}
	return measurement
}

func statistics(test *testing.T, state *Store) *ProcessingStatistics {
	test.Helper()
	progress, err := state.Progress(test.Context())
	if err != nil {
		test.Fatal(err)
	}
	return progress.Partitions[0].Statistics
}

func TestProcessingMeasurementsCommitWithProgress(test *testing.T) {
	state, records := fixtureStore(test)
	token := claim(test, state, 0)
	if measured(test, state) != nil {
		test.Fatal("untouched partition fabricated a measurement")
	}
	if statistics(test, state) != nil {
		test.Fatal("untouched partition fabricated processing statistics")
	}
	for _, record := range records[:4] {
		process(test, state, token, record)
	}
	measurement := measured(test, state)
	if measurement == nil || measurement.Work != (fold.Work{Refold: true, FromDay: "2026-01-01", RewindDays: 2, RefoldedRecords: 3}) || measurement.BusinessOutputs != 5 || measurement.StatusOutputs != 0 || measurement.Epoch != token.Epoch {
		test.Fatalf("correction measurement differs from fixture: %+v", measurement)
	}
	noOp := records[3].Clone()
	transaction, err := ledger.Decode(noOp.Value)
	if err != nil {
		test.Fatal(err)
	}
	transaction.Revision++
	noOp.Value, err = json.Marshal(transaction)
	if err != nil {
		test.Fatal(err)
	}
	noOp.Offset = 4
	process(test, state, token, noOp)
	measurement = measured(test, state)
	if !measurement.Work.Refold || measurement.Work.RefoldedRecords != 3 || measurement.BusinessOutputs != 0 || measurement.StatusOutputs != 0 {
		test.Fatalf("no-op revision confused refold cost with changed outputs: %+v", measurement)
	}
	duplicate := noOp.Clone()
	duplicate.Offset = 5
	process(test, state, token, duplicate)
	measurement = measured(test, state)
	if measurement.Work != (fold.Work{Duplicate: true}) || measurement.BusinessOutputs != 0 || measurement.StatusOutputs != 0 {
		test.Fatalf("duplicate fabricated work: %+v", measurement)
	}
	stale := records[0].Clone()
	stale.Offset = 6
	process(test, state, token, stale)
	measurement = measured(test, state)
	if measurement.Work != (fold.Work{Duplicate: true, Stale: true}) || measurement.BusinessOutputs != 0 || measurement.StatusOutputs != 0 {
		test.Fatalf("stale duplicate fabricated work: %+v", measurement)
	}
	malformed := records[0].Clone()
	malformed.Offset, malformed.Value = 7, []byte("{")
	process(test, state, token, malformed)
	measurement = measured(test, state)
	if measurement.QuarantineReason != "MALFORMED" || measurement.Work != (fold.Work{}) || measurement.BusinessOutputs != 0 || measurement.StatusOutputs != 0 {
		test.Fatalf("quarantine fabricated work: %+v", measurement)
	}
	cancellation := records[4].Clone()
	cancellation.Offset = 8
	process(test, state, token, cancellation)
	measurement = measured(test, state)
	if measurement.Work != (fold.Work{Refold: true, FromDay: "2026-01-03"}) || measurement.BusinessOutputs != 1 || measurement.StatusOutputs != 0 {
		test.Fatalf("empty suffix lost withdrawal measurement: %+v", measurement)
	}
	saved := *measurement
	counts := statistics(test, state)
	if counts == nil || counts.Decisions != 9 || counts.Duplicates != 2 || counts.Stale != 1 || counts.Quarantined != 1 || counts.Refolds != 6 || counts.RefoldedRecords != 9 || counts.BusinessOutputs != 10 || counts.StatusOutputs != 1 || counts.MaxRewindDays != 2 || counts.CountersSaturated || len(counts.Recent) != 9 || counts.FirstSource != records[0].Coordinates || !reflect.DeepEqual(counts.Recent[8], saved) {
		test.Fatalf("committed fixture statistics differ from hand-stated counts: %+v", counts)
	}
	if _, err := state.Replay(test.Context(), token, 9, 8, 9, []ledger.Record{cancellation}, true); err != nil {
		test.Fatal(err)
	}
	if !reflect.DeepEqual(measured(test, state), &saved) {
		test.Fatal("administrative replay rewrote processing measurements")
	}
	if !reflect.DeepEqual(statistics(test, state), counts) {
		test.Fatal("administrative replay counted processing work again")
	}
}

func TestProcessingMeasurementsAbortAndFence(test *testing.T) {
	state, records := fixtureStore(test)
	token := claim(test, state, 0)
	process(test, state, token, records[0])
	before := measured(test, state)
	beforeCounts := statistics(test, state)
	transaction, err := state.Begin(test.Context(), token, 1)
	if err != nil {
		test.Fatal(err)
	}
	defer transaction.Rollback()
	if err = transaction.Apply(records[1]); err != nil {
		test.Fatal(err)
	}
	if !reflect.DeepEqual(before, measured(test, state)) {
		test.Fatal("uncommitted decision changed visible measurement")
	}
	if !reflect.DeepEqual(beforeCounts, statistics(test, state)) {
		test.Fatal("uncommitted decision changed processing statistics")
	}
	transaction.cancel()
	if err = transaction.Commit(); err == nil {
		test.Fatal("canceled decision committed")
	}
	if !reflect.DeepEqual(before, measured(test, state)) {
		test.Fatal("aborted decision overwrote the measurement")
	}
	if !reflect.DeepEqual(beforeCounts, statistics(test, state)) {
		test.Fatal("aborted decision changed processing statistics")
	}
	claim(test, state, 0)
	if err = state.Process(test.Context(), token, 1, records[1]); !errors.Is(err, store.ErrFence) {
		test.Fatalf("stale owner was not fenced: %v", err)
	}
	if !reflect.DeepEqual(before, measured(test, state)) {
		test.Fatal("stale owner overwrote the measurement")
	}
	if !reflect.DeepEqual(beforeCounts, statistics(test, state)) {
		test.Fatal("stale owner changed processing statistics")
	}
}

func TestProcessingMeasurementIncludesActualPartitionLockWait(test *testing.T) {
	state, records := fixtureStore(test)
	token := claim(test, state, 0)
	blocker, err := state.pool.Begin(test.Context())
	if err != nil {
		test.Fatal(err)
	}
	defer rollback(blocker)
	var epoch int64
	if err = blocker.QueryRow(test.Context(), "SELECT epoch FROM amends_partitions WHERE namespace_id=$1 AND partition=0 FOR UPDATE", state.ns).Scan(&epoch); err != nil {
		test.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- state.Process(test.Context(), token, 0, records[0]) }()
	waitBlocked(test, state.pool, blocker.Conn().PgConn().PID())
	confirmed := time.Now()
	if measured(test, state) != nil {
		test.Fatal("blocked processing published a measurement")
	}
	held := time.Since(confirmed)
	if err = blocker.Commit(test.Context()); err != nil {
		test.Fatal(err)
	}
	select {
	case err = <-result:
		if err != nil {
			test.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		test.Fatal("processing did not finish after lock release")
	}
	measurement := measured(test, state)
	if measurement == nil || measurement.LockAcquireNS < held.Nanoseconds() || measurement.BusinessOutputs != 2 || measurement.StatusOutputs != 1 {
		test.Fatalf("measurement missed observed SQL wait (%s) or initial outputs: %+v", held, measurement)
	}
}

func TestProcessingStatisticsRetainBoundedHistoryAcrossOwnership(test *testing.T) {
	state, records := fixtureStore(test)
	token := claim(test, state, 0)
	process(test, state, token, records[0])
	for offset := int64(1); offset < 40; offset++ {
		duplicate := records[0].Clone()
		duplicate.Offset = offset
		process(test, state, token, duplicate)
	}
	reopened, err := New(state.pool, state.config)
	if err != nil {
		test.Fatal(err)
	}
	token = claim(test, reopened, 0)
	counts := statistics(test, reopened)
	if counts == nil || counts.Decisions != 40 || counts.Duplicates != 39 || counts.Refolds != 1 || counts.FirstSource.Offset != 0 || len(counts.Recent) != 32 || counts.Recent[0].Source.Offset != 8 || counts.Recent[31].Source.Offset != 39 {
		test.Fatalf("restart or ownership transfer lost durable aggregates/history: %+v", counts)
	}
	duplicate := records[0].Clone()
	duplicate.Offset = 45 // Five absent source positions are not processing decisions.
	process(test, reopened, token, duplicate)
	counts = statistics(test, reopened)
	if counts.Decisions != 41 || counts.Duplicates != 40 || counts.Recent[0].Source.Offset != 9 || counts.Recent[31].Source.Offset != 45 || counts.Recent[31].Epoch != token.Epoch {
		test.Fatalf("offset gap counted as decisions or history lost the new epoch: %+v", counts)
	}
}
