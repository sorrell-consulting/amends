// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Package view implements version-aware memory materialization. Callers may
// atomically install or discard its cloned state.
package view

import (
	"fmt"
	"math"
	"slices"

	"github.com/sorrell-consulting/amends/internal/ledger"
)

// State retains the highest envelope per output ID, including withdrawn facts,
// along with exclusive output offsets and stopped-partition diagnostics.
// Initialize it with [New]; the zero value is not ready for Apply. State and its
// maps are not safe for concurrent use. Clone before a speculative application.
type State struct {
	Config ledger.Config
	Rows   map[ledger.OutputID]ledger.Envelope
	Next   map[int]int64
	Errors map[int]string
}

// New returns an empty materialization with copied configuration. The caller
// must validate c first. An untouched key has logical READY status and no facts.
func New(c ledger.Config) State {
	return State{Config: c.Clone(), Rows: map[ledger.OutputID]ledger.Envelope{}, Next: map[int]int64{}, Errors: map[int]string{}}
}

// Clone returns a deep copy whose application cannot change the original state.
func (s State) Clone() State {
	n := New(s.Config)
	for k, v := range s.Rows {
		n.Rows[k] = v.Clone()
	}
	for k, v := range s.Next {
		n.Next[k] = v
	}
	for k, v := range s.Errors {
		n.Errors[k] = v
	}
	return n
}

// Apply commits a decision within this memory value. On protocol error only
// diagnostics change; rows and frontier remain before the offending record.
// expected must equal the saved next output offset; offset may skip broker gaps.
// Valid duplicates and older versions advance Next to offset+1 without replacing
// the retained envelope. A frontier error leaves the state unchanged.
func (s *State) Apply(partition int, expected, offset int64, e ledger.Envelope) error {
	if message := s.Errors[partition]; message != "" {
		return fmt.Errorf("stopped output partition: %s", message)
	}
	if partition < 0 || partition >= s.Config.Partitions || s.Next[partition] != expected || offset < expected || offset == math.MaxInt64 {
		return fmt.Errorf("invalid view frontier")
	}
	if err := validate(s.Config, partition, e); err != nil {
		s.Errors[partition] = err.Error()
		return err
	}
	old, exists := s.Rows[e.ID]
	if exists && e.Version == old.Version && !e.Equal(old) {
		err := fmt.Errorf("conflicting output version: %s v%d", e.ID.String(), e.Version)
		s.Errors[partition] = err.Error()
		return err
	}
	if !exists || e.Version > old.Version {
		s.Rows[e.ID] = e.Clone()
	}
	s.Next[partition] = offset + 1
	return nil
}

// Projection returns a key's visible, day-sorted facts and latest trust status.
// Withdrawals stay in Rows as deletion guards but are absent from this result.
// READY alone does not establish that the view has reached a complete boundary.
func (s State) Projection(key ledger.StockKey) ledger.Projection {
	p := ledger.Projection{Key: key, Status: ledger.Ready, Balances: []ledger.Balance{}, Alerts: []ledger.Alert{}}
	for id, e := range s.Rows {
		if id.Key != key || !e.Payload.Present {
			continue
		}
		switch id.Family {
		case ledger.BalanceFamily:
			p.Balances = append(p.Balances, ledger.Balance{Day: id.Day, Close: e.Payload.Close})
		case ledger.AlertFamily:
			p.Alerts = append(p.Alerts, ledger.Alert{Day: id.Day, Threshold: e.Payload.Threshold, Close: e.Payload.Close})
		case ledger.KeyStatusFamily:
			p.Status = e.Payload.Status
			p.Reasons = slices.Clone(e.Payload.Reasons)
		}
	}
	slices.SortFunc(p.Balances, func(a, b ledger.Balance) int {
		if a.Day < b.Day {
			return -1
		}
		if a.Day > b.Day {
			return 1
		}
		return 0
	})
	slices.SortFunc(p.Alerts, func(a, b ledger.Alert) int {
		if a.Day < b.Day {
			return -1
		}
		if a.Day > b.Day {
			return 1
		}
		return 0
	})
	return p
}
