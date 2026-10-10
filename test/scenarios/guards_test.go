// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package scenarios_test

import (
	"testing"

	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/view"
	"github.com/sorrell-consulting/amends/sim"
)

func TestSC16_DelayedBalanceCannotRegress(t *testing.T) {
	c := config()
	v := view.New(c)
	old := ledger.Envelope{
		SchemaVersion: 1,
		ID:            ledger.OutputID{Namespace: c.Namespace, Family: "balance", Key: key, Day: "2026-01-01"},
		Version:       1, Type: "BalanceStated", Payload: ledger.Payload{Present: true, Close: 50},
		Cause: ledger.Cause{Coordinates: ledger.Coordinates{Source: c.Source, Partition: 0, Offset: 0}, TxnID: "issue", Revision: 1},
	}
	newer := old.Clone()
	newer.Version, newer.Payload.Close = 2, 70
	newer.Cause.Offset, newer.Cause.Revision = 1, 2
	for offset, e := range []ledger.Envelope{old, newer, old} {
		if err := v.Apply(0, int64(offset), int64(offset), e); err != nil {
			t.Fatal(err)
		}
	}
	if !v.Rows[old.ID].Equal(newer) || v.Next[0] != 3 {
		t.Fatal("[amends:view-monotonicity] delayed balance regressed current value/version or lost progress")
	}
}

// Equal business values cannot certify an unapplied output prefix. This
// cancellation has no business rows, but it still emits a READY status.
func TestSC25_EqualValuesStillNeedOutputBoundary(t *testing.T) {
	c := config()
	m := model(t, c, records(t, c, txn("inactive", 2, "CANCEL", 1, 0)))
	step(t, m, "worker", sim.Commit)
	step(t, m, "relay", sim.Commit)
	if len(m.Output[0]) != 1 || m.View.Next[0] != 0 {
		t.Fatal("fixture did not leave one unapplied status output")
	}
	if got := m.Check(); got.Status != "INCOMPLETE" {
		t.Fatalf("[amends:complete-boundary] equal values certified an incomplete output boundary: %+v", got)
	}
	drain(t, m)
	if got := m.Check(); got.Status != "PASS" {
		t.Fatalf("complete cancellation boundary did not pass: %+v", got)
	}
}
