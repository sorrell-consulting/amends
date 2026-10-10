// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package diff_test

import (
	"fmt"

	"github.com/sorrell-consulting/amends/internal/diff"
	"github.com/sorrell-consulting/amends/internal/ledger"
)

func ExampleReconcile_withdrawal() {
	key := ledger.StockKey{Item: "DEMO-PART", Location: "DEPOT-A"}
	id := ledger.OutputID{Namespace: "withdrawal-example", Family: ledger.BalanceFamily, Key: key, Day: "2026-01-03"}
	statusID := ledger.OutputID{Namespace: id.Namespace, Family: ledger.KeyStatusFamily, Key: key}
	previous := map[ledger.OutputID]ledger.Envelope{
		id: {
			SchemaVersion: 1, ID: id, Version: 2, Type: ledger.BalanceStated,
			Payload: ledger.Payload{Present: true, Close: 100},
			Cause: ledger.Cause{
				Coordinates: ledger.Coordinates{Source: "synthetic-example", Partition: 0, Offset: 3},
				TxnID:       "ISSUE-001", Revision: 2,
			},
		},
		statusID: {
			SchemaVersion: 1, ID: statusID, Version: 1, Type: ledger.KeyStatusStated,
			Payload: ledger.Payload{Present: true, Status: ledger.Ready},
			Cause: ledger.Cause{
				Coordinates: ledger.Coordinates{Source: "synthetic-example", Partition: 0, Offset: 0},
				TxnID:       "ISSUE-001", Revision: 1,
			},
		},
	}
	// Cancelling the day's only movement leaves no balance fact. The status
	// stays READY, so only the balance receives another output version.
	projection := ledger.Projection{Key: key, Status: ledger.Ready}
	cause := ledger.Cause{
		Coordinates: ledger.Coordinates{Source: "synthetic-example", Partition: 0, Offset: 4},
		TxnID:       "RECEIPT-003", Revision: 2,
	}
	messages, err := diff.Reconcile(id.Namespace, previous, projection, cause)
	if err != nil {
		panic(err)
	}
	for _, message := range messages {
		fmt.Printf("%s %s: version %d, present=%t\n", message.Type, message.ID.Day, message.Version, message.Payload.Present)
	}
	fmt.Println("previous version:", previous[id].Version)
	// Output:
	// BalanceRetracted 2026-01-03: version 3, present=false
	// previous version: 2
}
