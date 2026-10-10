// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package view_test

import (
	"fmt"

	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/view"
)

func ExampleState_Apply_staleDelivery() {
	config, err := fixtures.Config("stale-delivery-example")
	if err != nil {
		panic(err)
	}
	key := config.Keys[0].StockKey
	id := ledger.OutputID{Namespace: config.Namespace, Family: ledger.BalanceFamily, Key: key, Day: "2026-01-03"}
	withdrawal := ledger.Envelope{
		SchemaVersion: 1, ID: id, Version: 3, Type: ledger.BalanceRetracted,
		Payload: ledger.Payload{Present: false},
		Cause: ledger.Cause{
			Coordinates: ledger.Coordinates{Source: config.Source, Partition: 0, Offset: 4},
			TxnID:       "RECEIPT-003", Revision: 2,
		},
	}
	older := withdrawal.Clone()
	older.Version = 2
	older.Type = ledger.BalanceStated
	older.Payload = ledger.Payload{Present: true, Close: 100}
	older.Cause = ledger.Cause{
		Coordinates: ledger.Coordinates{Source: config.Source, Partition: 0, Offset: 3},
		TxnID:       "ISSUE-001", Revision: 2,
	}

	// Output delivery order differs from version order. The older statement
	// is consumed at the next broker offset without restoring the withdrawn row.
	state := view.New(config)
	if err := state.Apply(0, 0, 0, withdrawal); err != nil {
		panic(err)
	}
	if err := state.Apply(0, 1, 1, older); err != nil {
		panic(err)
	}
	fmt.Println("visible balances:", len(state.Projection(key).Balances))
	fmt.Println("retained version:", state.Rows[id].Version)
	fmt.Println("present:", state.Rows[id].Payload.Present)
	fmt.Println("next output offset:", state.Next[0])
	// Output:
	// visible balances: 0
	// retained version: 3
	// present: false
	// next output offset: 2
}
