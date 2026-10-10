// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"testing"

	"github.com/sorrell-consulting/amends/internal/fold"
	"github.com/sorrell-consulting/amends/internal/ledger"
)

// Hand-stated format-1 state: one cancelled issue, one repaired receipt, and
// retained duplicate/conflict evidence. Do not regenerate it with fold.Apply.
func TestKeyStateFormat1Compatibility(t *testing.T) {
	raw, err := os.ReadFile("testdata/key-state-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	state, err := decodeKeyState(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Identities) != 2 || state.Identities["cancelled"].Highest != 2 || state.Identities["repaired"].Highest != 2 {
		t.Fatal("format-1 authority was not restored")
	}
	if len(state.Identities["repaired"].Revisions[1]) != 2 || len(state.Identities["cancelled"].Revisions[2][0].Sources) != 2 {
		t.Fatal("format-1 conflict/duplicate evidence was not restored")
	}
	encoded, err := encodeKeyState(state)
	if err != nil {
		t.Fatal(err)
	}
	// Compare complete JSON values, including field names and null versus empty
	// collections. Formatting and object order are not part of this storage format.
	var originalJSON, encodedJSON any
	if err = json.Unmarshal(raw, &originalJSON); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(encoded, &encodedJSON); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(originalJSON, encodedJSON) {
		t.Fatalf("format-1 representation changed: %s", encoded)
	}
	key := ledger.KeyConfig{StockKey: ledger.StockKey{Item: "ITEM", Location: "DEPOT"}, Opening: 100, Threshold: 60}
	stale := state.Identities["cancelled"].Revisions[1][0].Transaction
	next, err := fold.Apply(state, stale, ledger.Coordinates{Source: "fixture-v1", Offset: 6}, key)
	if err != nil {
		t.Fatal(err)
	}
	if next.Projection.Status != ledger.Ready || !slices.Equal(next.Projection.Balances, []ledger.Balance{{Day: "2026-01-02", Close: 110}}) || len(next.Projection.Alerts) != 0 {
		t.Fatalf("restored cancellation lost authority: %+v", next.Projection)
	}
	stale.Revision, stale.Kind, stale.Qty = 3, ledger.Amend, -30
	next, err = fold.Apply(next, stale, ledger.Coordinates{Source: "fixture-v1", Offset: 7}, key)
	if err != nil {
		t.Fatal(err)
	}
	if next.Projection.Status != ledger.Ready || !slices.Equal(next.Projection.Balances, []ledger.Balance{{Day: "2026-01-01", Close: 70}, {Day: "2026-01-02", Close: 80}}) || len(next.Projection.Alerts) != 0 {
		t.Fatalf("restored state cannot continue with a higher revision: %+v", next.Projection)
	}
}
