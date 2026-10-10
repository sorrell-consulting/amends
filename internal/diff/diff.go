// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Package diff allocates semantic output versions, including retained withdrawals.
package diff

import (
	"fmt"
	"math"
	"slices"

	"github.com/sorrell-consulting/amends/internal/ledger"
)

// Reconcile returns the versioned changes needed to publish p in namespace.
// previous is committed output intent, including withdrawals, whether or not
// delivery has finished. Entries for other keys and namespaces are ignored.
// p must be a valid fold projection; a BLOCKED projection retains frozen facts.
//
// Unchanged payloads produce no message, even when cause changes. Withdrawals
// and reappearances continue the retained version sequence. Business repairs
// precede any status message, including resolution to READY. Version overflow
// returns no decision and an error. Neither previous nor p is mutated.
func Reconcile(namespace string, previous map[ledger.OutputID]ledger.Envelope, p ledger.Projection, cause ledger.Cause) ([]ledger.Envelope, error) {
	desired := map[ledger.OutputID]ledger.Payload{}
	for _, b := range p.Balances {
		desired[ledger.OutputID{Namespace: namespace, Family: ledger.BalanceFamily, Key: p.Key, Day: b.Day}] = ledger.Payload{Present: true, Close: b.Close}
	}
	for _, a := range p.Alerts {
		desired[ledger.OutputID{Namespace: namespace, Family: ledger.AlertFamily, Key: p.Key, Day: a.Day}] = ledger.Payload{Present: true, Close: a.Close, Threshold: a.Threshold}
	}
	status := ledger.OutputID{Namespace: namespace, Family: ledger.KeyStatusFamily, Key: p.Key}
	desired[status] = ledger.Payload{Present: true, Status: p.Status, Reasons: slices.Clone(p.Reasons)}
	for id, old := range previous {
		if id.Namespace != namespace || id.Key != p.Key || id.Family == ledger.KeyStatusFamily {
			continue
		}
		if _, ok := desired[id]; !ok && old.Payload.Present {
			desired[id] = ledger.Payload{}
		}
	}
	ids := make([]ledger.OutputID, 0, len(desired))
	for id := range desired {
		if id != status {
			ids = append(ids, id)
		}
	}
	slices.SortFunc(ids, func(a, b ledger.OutputID) int {
		if a.String() < b.String() {
			return -1
		}
		if a.String() > b.String() {
			return 1
		}
		return 0
	})
	ids = append(ids, status)
	var messages []ledger.Envelope
	for _, id := range ids {
		payload := desired[id]
		old, ok := previous[id]
		if ok && old.Payload.Equal(payload) {
			continue
		}
		if old.Version == math.MaxInt64 {
			return nil, fmt.Errorf("output version overflow: %s", id.String())
		}
		kind := ledger.KeyStatusStated
		switch id.Family {
		case ledger.BalanceFamily:
			kind = ledger.BalanceStated
			if !payload.Present {
				kind = ledger.BalanceRetracted
			}
		case ledger.AlertFamily:
			kind = ledger.AlertRaised
			if !payload.Present {
				kind = ledger.AlertRetracted
			}
		}
		messages = append(messages, ledger.Envelope{SchemaVersion: 1, ID: id, Version: old.Version + 1, Type: kind, Payload: payload, Cause: cause})
	}
	return messages, nil
}
