//go:build locality

// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package scenarios_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/sorrell-consulting/amends/internal/diff"
	"github.com/sorrell-consulting/amends/internal/fold"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/oracle"
	"github.com/sorrell-consulting/amends/internal/view"
)

// This opt-in example measures derivation work, not elapsed time or storage I/O.
// Each case starts from its own 2,000-day history with one movement per day.
func TestRepairLocality(test *testing.T) {
	const days = 2000
	for _, correctionDay := range []int{10, 1990} {
		test.Run(fmt.Sprintf("day_%d", correctionDay), func(test *testing.T) {
			c := config()
			c.Keys = c.Keys[:1]
			c.Namespace = fmt.Sprintf("locality-%d", correctionDay)
			transactions := make([]ledger.Transaction, days)
			for day := 1; day <= days; day++ {
				quantity := int64(0)
				switch day {
				case correctionDay:
					quantity = -50
				case correctionDay + 1:
					quantity = -20
				case correctionDay + 2:
					quantity = 50
				}
				transactions[day-1] = txn(fmt.Sprintf("movement-%04d", day), 1, "NEW", day, quantity)
			}
			amendment := txn(transactions[correctionDay-1].TxnID, 2, "AMEND", correctionDay, -30)
			cancellation := txn(transactions[days-1].TxnID, 2, "CANCEL", days, 0)
			history := records(test, c, append(transactions, amendment, cancellation)...)
			state := fold.Empty(key)
			for _, record := range history[:days] {
				transaction, reason := fold.Validate(c, record)
				if reason != "" {
					test.Fatal(reason)
				}
				var err error
				state, err = fold.Apply(state, transaction, record.Coordinates, c.Keys[0])
				if err != nil {
					test.Fatal(err)
				}
			}

			// Publish the completed baseline once; subsequent repairs use the same
			// production diff and memory materializer. No broker/SQL claim is made.
			materialized := view.New(c)
			publish := func(p ledger.Projection, record ledger.Record) []ledger.Envelope {
				transaction, err := ledger.Decode(record.Value)
				if err != nil {
					test.Fatal(err)
				}
				cause := ledger.Cause{Coordinates: record.Coordinates, TxnID: transaction.TxnID, Revision: transaction.Revision}
				messages, err := diff.Reconcile(c.Namespace, materialized.Rows, p, cause)
				if err != nil {
					test.Fatal(err)
				}
				for _, message := range messages {
					offset := materialized.Next[0]
					if err := materialized.Apply(0, offset, offset, message); err != nil {
						test.Fatal(err)
					}
				}
				return messages
			}
			publish(state.Projection, history[days-1])
			check := func(stage string, end int, corrected, cancelled bool) {
				want := localityExpectation(days, correctionDay, corrected, cancelled)
				result, err := oracle.Compute(c, history[:end])
				if err != nil || len(result.Keys) != 1 || len(result.Rejections) != 0 || len(result.Conflicts) != 0 {
					test.Fatalf("%s oracle: %+v, error %v", stage, result, err)
				}
				for name, got := range map[string]ledger.Projection{
					"fold": state.Projection, "oracle": result.Keys[0], "view": materialized.Projection(key),
				} {
					if !reflect.DeepEqual(got, want) {
						test.Fatalf("%s %s disagrees with independently stated balances/alerts", stage, name)
					}
				}
			}
			check("baseline", days, false, false)

			corrected, work, err := fold.ApplyMeasured(state, amendment, history[days].Coordinates, c.Keys[0])
			wantWork := fold.Work{Refold: true, FromDay: amendment.ValidTime.Format("2006-01-02"), RewindDays: int64(days - correctionDay), RefoldedRecords: int64(days - correctionDay + 1)}
			if err != nil || work != wantWork {
				test.Fatalf("correction work %+v, want %+v, error %v", work, wantWork, err)
			}
			state = corrected
			messages := publish(state.Projection, history[days])
			counts := map[string]int{}
			for _, message := range messages {
				counts[message.Type]++
			}
			if counts["BalanceStated"] != days-correctionDay+1 || counts["AlertRetracted"] != 1 || counts["AlertRaised"] != 1 || len(messages) != days-correctionDay+3 {
				test.Fatalf("correction output counts: %+v, total %d", counts, len(messages))
			}
			check("correction", days+1, true, false)
			test.Logf("correction_day=%d from_day=%s rewind_days=%d refolded_records=%d balance_updates=%d alert_withdrawals=1 alert_raises=1 business_outputs=%d independent_agreement=true", correctionDay, work.FromDay, work.RewindDays, work.RefoldedRecords, counts["BalanceStated"], len(messages))

			state, work, err = fold.ApplyMeasured(state, cancellation, history[days+1].Coordinates, c.Keys[0])
			if err != nil || work != (fold.Work{Refold: true, FromDay: cancellation.ValidTime.Format("2006-01-02")}) {
				test.Fatalf("cancellation work %+v, error %v", work, err)
			}
			messages = publish(state.Projection, history[days+1])
			if len(messages) != 1 || messages[0].Type != "BalanceRetracted" || messages[0].ID.Day != work.FromDay || messages[0].Version != 3 {
				test.Fatalf("cancellation outputs: %+v", messages)
			}
			check("cancellation", days+2, true, true)
			test.Logf("cancel_last_day from_day=%s rewind_days=%d refolded_records=%d balance_withdrawals=1 retained_deletion_version=3 independent_agreement=true", work.FromDay, work.RewindDays, work.RefoldedRecords)
		})
	}
}

// Expected business values follow the stated three-movement arithmetic. They
// are never obtained from fold, diff, view, or the oracle.
func localityExpectation(days, correctionDay int, corrected, cancelled bool) ledger.Projection {
	p := ledger.Projection{Key: key, Status: "READY", Balances: []ledger.Balance{}, Alerts: []ledger.Alert{}}
	for day := 1; day <= days; day++ {
		if cancelled && day == days {
			continue
		}
		close := int64(100)
		switch {
		case day < correctionDay:
		case day == correctionDay:
			close = 50
		case day == correctionDay+1:
			close = 30
		default:
			close = 80
		}
		if corrected && day >= correctionDay {
			close += 20
		}
		p.Balances = append(p.Balances, ledger.Balance{Day: origin.AddDate(0, 0, day-1).Format("2006-01-02"), Close: close})
	}
	alertDay, alertClose := correctionDay, int64(50)
	if corrected {
		alertDay++
	}
	p.Alerts = append(p.Alerts, ledger.Alert{Day: origin.AddDate(0, 0, alertDay-1).Format("2006-01-02"), Threshold: 60, Close: alertClose})
	return p
}
