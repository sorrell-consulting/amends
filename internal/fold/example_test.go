// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package fold_test

import (
	"fmt"

	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/fold"
)

func ExampleApply() {
	config, err := fixtures.Config("correction-example")
	if err != nil {
		panic(err)
	}
	// Opening stock is 100, with a threshold of 60. The three initial
	// movements are -50, -20, +50; the final record replaces -50 with -30.
	records, err := fixtures.Records(config, "before.jsonl", "correction.jsonl")
	if err != nil {
		panic(err)
	}
	key := config.Keys[0]
	state := fold.Empty(key.StockKey)
	for _, record := range records {
		transaction, reason := fold.Validate(config, record)
		if reason != "" {
			panic(reason)
		}
		state, err = fold.Apply(state, transaction, record.Coordinates, key)
		if err != nil {
			panic(err)
		}
		if record.Offset == 2 {
			fmt.Println("original balances:", state.Projection.Balances)
			fmt.Println("original crossing:", state.Projection.Alerts[0].Day)
		}
	}
	fmt.Println("corrected balances:", state.Projection.Balances)
	fmt.Println("corrected crossing:", state.Projection.Alerts[0].Day)
	// Output:
	// original balances: [{2026-01-01 50} {2026-01-02 30} {2026-01-03 80}]
	// original crossing: 2026-01-01
	// corrected balances: [{2026-01-01 70} {2026-01-02 50} {2026-01-03 100}]
	// corrected crossing: 2026-01-02
}
