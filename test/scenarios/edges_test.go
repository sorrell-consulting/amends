// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package scenarios_test

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"

	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/oracle"
	"github.com/sorrell-consulting/amends/sim"
)

func TestSC22_IndependentValidationReasons(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		edit         func(*ledger.Transaction)
	}{
		{"empty identity", "IDENTITY", func(tx *ledger.Transaction) { tx.TxnID = "" }},
		{"zero revision", "REVISION", func(tx *ledger.Transaction) { tx.Revision = 0 }},
		{"negative revision", "REVISION", func(tx *ledger.Transaction) { tx.Revision = -1 }},
		{"unknown kind", "KIND", func(tx *ledger.Transaction) { tx.Kind = "PATCH" }},
		{"cancel with quantity", "CANCEL_QUANTITY", func(tx *ledger.Transaction) { tx.Kind = "CANCEL" }},
		{"before origin", "VALID_TIME", func(tx *ledger.Transaction) { tx.ValidTime = origin.AddDate(0, 0, -1) }},
		{"unknown key", "UNKNOWN_KEY", func(tx *ledger.Transaction) { tx.StockKey.Item = "missing" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := config()
			tx := txn("a", 1, "NEW", 1, -50)
			tc.edit(&tx)
			raw, err := json.Marshal(tx)
			if err != nil {
				t.Fatal(err)
			}
			r := ledger.Record{Coordinates: ledger.Coordinates{Source: c.Source, Offset: 0}, Key: tx.StockKey, Value: raw}
			batch, err := oracle.Compute(c, []ledger.Record{r})
			if err != nil {
				t.Fatal(err)
			}
			if len(batch.Rejections) != 1 || batch.Rejections[0].Reason != tc.reason {
				t.Fatal("oracle rejection", batch.Rejections)
			}
			m := model(t, c, []ledger.Record{r})
			drain(t, m)
			got := m.Store.Snapshot()
			if len(got.Rejections) != 1 || got.Rejections[0].Reason != tc.reason || len(got.Outbox) != 0 {
				t.Fatal("worker rejection", got.Rejections)
			}
			if m.Check().Status != "PASS_WITH_EXCLUSIONS" {
				t.Fatal(m.Check())
			}
		})
	}
}

func TestSC08SC09_AllConflictsMustResolve(t *testing.T) {
	c := config()
	rs := records(t, c, txn("a", 1, "NEW", 1, -10), txn("a", 1, "AMEND", 1, -10), txn("b", 1, "NEW", 2, -10), txn("b", 1, "AMEND", 2, -10), txn("a", 2, "AMEND", 1, -10), txn("b", 2, "AMEND", 2, -10))
	m := model(t, c, rs)
	for i := 0; i < 4; i++ {
		step(t, m, "worker", sim.Commit)
	}
	if got := m.Store.Snapshot().Keys[key].Projection; got.Status != "BLOCKED" || len(got.Reasons) != 2 {
		t.Fatal(got)
	}
	step(t, m, "worker", sim.Commit)
	if got := m.Store.Snapshot().Keys[key].Projection; got.Status != "BLOCKED" || len(got.Reasons) != 1 {
		t.Fatal("one repair prematurely unblocked key", got)
	}
	drain(t, m)
	projection(t, m.View.Projection(key), []int{1, 2}, []int64{90, 80}, nil)
	if len(m.Check().Conflicts) != 2 {
		t.Fatal("historical evidence disappeared")
	}
}

func TestSC18_RetryTimeAndExhaustion(t *testing.T) {
	c := config()
	m := model(t, c, records(t, c, txn("a", 1, "NEW", 1, -10)))
	before := m.Store.Snapshot()
	for attempt := 0; attempt < 3; attempt++ {
		step(t, m, "worker", sim.UnknownAbort)
		step(t, m, "worker", sim.Commit)
		if !reflect.DeepEqual(before, m.Store.Snapshot()) {
			t.Fatal("early/parked retry changed durable state")
		}
		if attempt < 2 {
			for m.Now < m.RetryState("worker", 0).Next {
				step(t, m, "tick", sim.Commit)
			}
		}
	}
	if !m.RetryState("worker", 0).Exhausted || m.Events[len(m.Events)-1].Observation != "parked" || m.Check().Status != "INCOMPLETE" {
		t.Fatal("exhaustion not visible")
	}
	step(t, m, "resume-worker", sim.Commit)
	drain(t, m)
	if m.Check().Status != "PASS" {
		t.Fatal("explicit recovery did not converge")
	}
	if _, err := (sim.Retry{}).Failed(math.MaxInt64); err == nil {
		t.Fatal("retry time overflow accepted")
	}
}

func TestSC27_RevisionMaximumAndNegativeBalance(t *testing.T) {
	c := config()
	c.Keys[0].Opening = 0
	m := model(t, c, records(t, c, txn("a", math.MaxInt64, "AMEND", 1, math.MinInt64)))
	drain(t, m)
	projection(t, m.View.Projection(key), []int{1}, []int64{math.MinInt64}, nil)
	r := records(t, c, txn("b", 1, "NEW", 2, -1))[0]
	r.Offset = 1
	m.Source = append(m.Source, r)
	before := m.Store.Snapshot()
	if err := m.Step(sim.Step{Actor: "worker", Partition: 0, Outcome: sim.Commit}); err == nil {
		t.Fatal("negative overflow accepted")
	}
	if !reflect.DeepEqual(before, m.Store.Snapshot()) {
		t.Fatal("negative overflow changed state")
	}
}

func TestSC25_ViewStallAndEvidenceMismatch(t *testing.T) {
	c := config()
	m := model(t, c, records(t, c, txn("a", 1, "NEW", 1, -10)))
	step(t, m, "worker", sim.Commit)
	for {
		if _, ok := m.Store.Pending(0); !ok {
			break
		}
		step(t, m, "relay", sim.Commit)
	}
	if m.Check().Status != "INCOMPLETE" {
		t.Fatal("empty outbox certified unapplied outputs")
	}
	drain(t, m)
	for id, e := range m.View.Rows {
		if id.Family == "key-status" {
			e.Payload.Status = "BLOCKED"
			e.Payload.Reasons = []string{"invented"}
			m.View.Rows[id] = e
		}
	}
	if m.Check().Status != "DRIFT" {
		t.Fatal("trust mismatch hidden")
	}
}
