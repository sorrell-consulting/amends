// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package view_test

import (
	"strings"
	"testing"

	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/view"
)

func TestOutputValidationPreservesStateOnRejection(t *testing.T) {
	config, err := fixtures.Config("validation")
	if err != nil {
		t.Fatal(err)
	}
	key := config.Keys[0].StockKey
	balance := ledger.Envelope{
		SchemaVersion: 1, ID: ledger.OutputID{Namespace: config.Namespace, Family: ledger.BalanceFamily, Key: key, Day: "2026-01-01"},
		Version: 1, Type: ledger.BalanceStated, Payload: ledger.Payload{Present: true, Close: 50},
		Cause: ledger.Cause{Coordinates: ledger.Coordinates{Source: config.Source}, TxnID: "issue", Revision: 1},
	}
	alert := balance.Clone()
	alert.ID.Family, alert.Type, alert.Payload.Threshold = ledger.AlertFamily, ledger.AlertRaised, config.Keys[0].Threshold
	status := balance.Clone()
	status.ID.Family, status.ID.Day, status.Type = ledger.KeyStatusFamily, "", ledger.KeyStatusStated
	status.Payload = ledger.Payload{Present: true, Status: ledger.Ready}
	for _, tc := range []struct {
		name, diagnostic string
		base             ledger.Envelope
		change           func(*ledger.Envelope)
	}{
		{"schema", "schema_version", balance, func(e *ledger.Envelope) { e.SchemaVersion = 2 }},
		{"namespace", "namespace", balance, func(e *ledger.Envelope) { e.ID.Namespace = "other" }},
		{"key", "stock key", balance, func(e *ledger.Envelope) { e.ID.Key.Item = "unknown" }},
		{"key control character", `\x00`, balance, func(e *ledger.Envelope) { e.ID.Key.Item = "unknown\x00item" }},
		{"version", "version", balance, func(e *ledger.Envelope) { e.Version = 0 }},
		{"cause source", "cause source", balance, func(e *ledger.Envelope) { e.Cause.Source = "other" }},
		{"cause partition", "cause partition", balance, func(e *ledger.Envelope) { e.Cause.Partition = 1 }},
		{"cause offset", "cause offset", balance, func(e *ledger.Envelope) { e.Cause.Offset = -1 }},
		{"cause revision", "cause revision", balance, func(e *ledger.Envelope) { e.Cause.Revision = 0 }},
		{"cause identity", "transaction ID", balance, func(e *ledger.Envelope) { e.Cause.TxnID = "" }},
		{"family", "family", balance, func(e *ledger.Envelope) { e.ID.Family = "unknown" }},
		{"day spelling", "YYYY-MM-DD", balance, func(e *ledger.Envelope) { e.ID.Day = "2026-1-1" }},
		{"origin", "precedes ledger origin", balance, func(e *ledger.Envelope) { e.ID.Day = "2025-12-31" }},
		{"business status", "status and reasons", balance, func(e *ledger.Envelope) { e.Payload.Status = ledger.Ready }},
		{"business reasons", "status and reasons", balance, func(e *ledger.Envelope) { e.Payload.Reasons = []string{"a"} }},
		{"type", "type", balance, func(e *ledger.Envelope) { e.Type = ledger.AlertRaised }},
		{"balance threshold", "balance threshold", balance, func(e *ledger.Envelope) { e.Payload.Threshold = 60 }},
		{"withdrawal close", "withdrawal", balance, func(e *ledger.Envelope) { e.Payload.Present = false; e.Type = ledger.BalanceRetracted }},
		{"withdrawal threshold", "withdrawal", alert, func(e *ledger.Envelope) {
			e.Payload.Present = false
			e.Payload.Close = 0
			e.Type = ledger.AlertRetracted
		}},
		{"alert threshold", "alert threshold", alert, func(e *ledger.Envelope) { e.Payload.Threshold++ }},
		{"alert boundary", "must be below", alert, func(e *ledger.Envelope) { e.Payload.Close = e.Payload.Threshold }},
		{"status day", "day", status, func(e *ledger.Envelope) { e.ID.Day = "2026-01-01" }},
		{"status type", "type", status, func(e *ledger.Envelope) { e.Type = ledger.BalanceStated }},
		{"status withdrawal", "cannot be withdrawn", status, func(e *ledger.Envelope) { e.Payload.Present = false }},
		{"status business value", "must be zero", status, func(e *ledger.Envelope) { e.Payload.Close = 1 }},
		{"unknown status", "READY or BLOCKED", status, func(e *ledger.Envelope) { e.Payload.Status = "unknown" }},
		{"ready reasons", "READY must have no reasons", status, func(e *ledger.Envelope) { e.Payload.Reasons = []string{"a"} }},
		{"blocked without reasons", "BLOCKED requires reasons", status, func(e *ledger.Envelope) { e.Payload.Status = ledger.Blocked }},
		{"unordered reasons", "sorted", status, func(e *ledger.Envelope) { e.Payload.Status = ledger.Blocked; e.Payload.Reasons = []string{"b", "a"} }},
		{"duplicate reasons", "duplicate status reason", status, func(e *ledger.Envelope) { e.Payload.Status = ledger.Blocked; e.Payload.Reasons = []string{"a", "a"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := view.New(config)
			// An existing good row and frontier must survive malformed newer input.
			if err := state.Apply(0, 0, 0, balance); err != nil {
				t.Fatal(err)
			}
			invalid := tc.base.Clone()
			invalid.Version = 2
			tc.change(&invalid)
			err := state.Apply(0, 1, 1, invalid)
			if err == nil || !strings.Contains(err.Error(), tc.diagnostic) {
				t.Fatalf("got %v, want a diagnostic identifying %q", err, tc.diagnostic)
			}
			if state.Next[0] != 1 || len(state.Rows) != 1 || !state.Rows[balance.ID].Equal(balance) || state.Errors[0] == "" {
				t.Fatalf("invalid input changed rows/progress or lost its diagnostic: %+v", state)
			}
		})
	}
	for _, envelope := range []ledger.Envelope{balance, alert, status} {
		state := view.New(config)
		if err := state.Apply(0, 0, 0, envelope); err != nil {
			t.Fatal("valid output rejected:", err)
		}
		envelope.Version++
		if envelope.ID.Family == ledger.KeyStatusFamily {
			envelope.Payload.Status, envelope.Payload.Reasons = ledger.Blocked, []string{"a", "b"}
		} else {
			envelope.Payload = ledger.Payload{}
			envelope.Type = ledger.BalanceRetracted
			if envelope.ID.Family == ledger.AlertFamily {
				envelope.Type = ledger.AlertRetracted
			}
		}
		if err := state.Apply(0, 1, 1, envelope); err != nil || state.Next[0] != 2 || !state.Rows[envelope.ID].Equal(envelope) {
			t.Fatalf("valid withdrawal/status transition rejected: %+v, %v", state, err)
		}
	}
}
