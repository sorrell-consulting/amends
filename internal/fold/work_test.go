package fold

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/ledger"
)

func workFixture(test *testing.T) (State, ledger.KeyConfig, []ledger.Record) {
	test.Helper()
	config, err := fixtures.Config("fold-work")
	if err != nil {
		test.Fatal(err)
	}
	config.Origin = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	records, err := fixtures.Records(config, "before.jsonl", "correction.jsonl", "cancel-last-day.jsonl")
	if err != nil {
		test.Fatal(err)
	}
	state := Empty(config.Keys[0].StockKey)
	for _, record := range records[:3] {
		transaction, reason := Validate(config, record)
		if reason != "" {
			test.Fatal(reason)
		}
		next, work, err := ApplyMeasured(state, transaction, record.Coordinates, config.Keys[0])
		if err != nil || work != (Work{Refold: true, FromDay: transaction.ValidTime.Format("2006-01-02"), RefoldedRecords: 1}) {
			test.Fatalf("append work = %+v, error = %v", work, err)
		}
		state = next
	}
	return state, config.Keys[0], records
}

func TestMeasuredSuffixWork(test *testing.T) {
	previous, key, records := workFixture(test)
	correction, err := ledger.Decode(records[3].Value)
	if err != nil {
		test.Fatal(err)
	}
	saved := previous.Clone()
	corrected, work, err := ApplyMeasured(previous, correction, records[3].Coordinates, key)
	want := Work{Refold: true, FromDay: "2026-01-01", RewindDays: 2, RefoldedRecords: 3}
	if err != nil || work != want {
		test.Fatalf("correction work = %+v, want %+v, error = %v", work, want, err)
	}
	if !reflect.DeepEqual(previous, saved) {
		test.Fatal("measurement mutated previous state")
	}
	for _, scenario := range []struct {
		name string
		edit func(*ledger.Transaction)
		want Work
	}{
		{"duplicate", func(transaction *ledger.Transaction) { transaction.SourceTime = transaction.SourceTime.Add(time.Hour) }, Work{Duplicate: true}},
		{"stale_duplicate", func(transaction *ledger.Transaction) {
			transaction.Revision = 1
			transaction.Kind = "NEW"
			transaction.Qty = -50
		}, Work{Duplicate: true, Stale: true}},
		{"stale_candidate", func(transaction *ledger.Transaction) { transaction.Revision = 1; transaction.Qty = -44 }, Work{Stale: true}},
		{"same_day_noop", func(transaction *ledger.Transaction) {
			transaction.Revision++
			transaction.ValidTime = transaction.ValidTime.Add(time.Hour)
		}, want},
		{"move_later", func(transaction *ledger.Transaction) {
			transaction.Revision++
			transaction.ValidTime = transaction.ValidTime.AddDate(0, 0, 3)
		}, want},
		{"move_earlier", func(transaction *ledger.Transaction) {
			transaction.Revision++
			transaction.ValidTime = transaction.ValidTime.AddDate(0, 0, -1)
		}, Work{Refold: true, FromDay: "2025-12-31", RewindDays: 3, RefoldedRecords: 3}},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			transaction := correction
			scenario.edit(&transaction)
			_, got, err := ApplyMeasured(corrected, transaction, records[3].Coordinates, key)
			if err != nil || got != scenario.want {
				test.Fatalf("work = %+v, want %+v, error = %v", got, scenario.want, err)
			}
		})
	}
	cancellation, err := ledger.Decode(records[4].Value)
	if err != nil {
		test.Fatal(err)
	}
	_, work, err = ApplyMeasured(corrected, cancellation, records[4].Coordinates, key)
	if err != nil || work != (Work{Refold: true, FromDay: "2026-01-03"}) {
		test.Fatalf("empty suffix withdrawal work = %+v, error = %v", work, err)
	}
}

func TestMeasuredBlockedResolutionAndCancellation(test *testing.T) {
	state, key, records := workFixture(test)
	transaction, err := ledger.Decode(records[0].Value)
	if err != nil {
		test.Fatal(err)
	}
	transaction.Qty = -40
	blocked, work, err := ApplyMeasured(state, transaction, records[3].Coordinates, key)
	if err != nil || blocked.Projection.Status != "BLOCKED" || work != (Work{}) {
		test.Fatalf("blocked work = %+v, error = %v", work, err)
	}
	_, work, err = ApplyMeasured(blocked, transaction, records[3].Coordinates, key)
	if err != nil || work != (Work{Duplicate: true}) {
		test.Fatalf("blocked duplicate work = %+v, error = %v", work, err)
	}
	transaction.Revision = 2
	_, work, err = ApplyMeasured(blocked, transaction, records[4].Coordinates, key)
	if err != nil || work != (Work{Refold: true, FullRefold: true, FromDay: "2026-01-01", RewindDays: 2, RefoldedRecords: 3}) {
		test.Fatalf("full resolution work = %+v, error = %v", work, err)
	}
	transaction.Kind, transaction.Qty = "CANCEL", 0
	cancelled, work, err := ApplyMeasured(Empty(key.StockKey), transaction, records[4].Coordinates, key)
	if err != nil || work != (Work{}) {
		test.Fatalf("cancel-before-new work = %+v, error = %v", work, err)
	}
	transaction.Kind, transaction.Qty, transaction.Revision = "NEW", -50, 1
	_, work, err = ApplyMeasured(cancelled, transaction, records[0].Coordinates, key)
	if err != nil || work != (Work{Stale: true}) {
		test.Fatalf("cancelled stale work = %+v, error = %v", work, err)
	}
	transaction.Revision = 3
	_, work, err = ApplyMeasured(cancelled, transaction, records[0].Coordinates, key)
	if err != nil || work != (Work{Refold: true, FromDay: "2026-01-01", RefoldedRecords: 1}) {
		test.Fatalf("reactivation work = %+v, error = %v", work, err)
	}
}

func TestMeasuredWorkOverflowAndCalendarSpan(test *testing.T) {
	state, key, records := workFixture(test)
	transaction, err := ledger.Decode(records[3].Value)
	if err != nil {
		test.Fatal(err)
	}
	transaction.Qty = math.MaxInt64
	_, work, err := ApplyMeasured(state, transaction, records[3].Coordinates, key)
	if err == nil || work != (Work{}) {
		test.Fatalf("overflow returned successful work: %+v, error = %v", work, err)
	}
	transaction.Qty = -30
	transaction.ValidTime = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	state = Empty(key.StockKey)
	state.Projection.Balances = []ledger.Balance{{Day: "9999-12-31", Close: 100}}
	_, work, err = ApplyMeasured(state, transaction, records[3].Coordinates, key)
	if err != nil || work.RewindDays != 3652058 {
		test.Fatalf("calendar span overflowed time.Duration: %+v, error = %v", work, err)
	}
}
