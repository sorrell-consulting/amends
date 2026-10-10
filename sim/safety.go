// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package sim

import (
	"fmt"
	"slices"

	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/oracle"
)

// Safety checks committed intent, unchanged relay content, retained view
// versions, and worker equivalence at its own processed source prefix. End-to-
// end agreement still requires Check's complete source AND output boundary.
func (m *Model) Safety() error {
	s := m.Store.Snapshot()
	latest := map[ledger.OutputID]ledger.Envelope{}
	sequences := map[int]int64{}
	pending := map[int]bool{}
	for _, row := range s.Outbox {
		if row.Sequence != sequences[row.Partition]+1 {
			return fmt.Errorf("outbox sequence gap")
		}
		sequences[row.Partition]++
		if row.Acknowledged && pending[row.Partition] {
			return fmt.Errorf("outbox acknowledged out of order")
		}
		if !row.Acknowledged {
			pending[row.Partition] = true
		}
		e := row.Envelope
		if e.Version != latest[e.ID].Version+1 {
			return fmt.Errorf("semantic version gap")
		}
		latest[e.ID] = e
	}
	if len(latest) != len(s.Outputs) {
		return fmt.Errorf("intent/output state size mismatch")
	}
	for id, e := range latest {
		if !e.Equal(s.Outputs[id]) {
			return fmt.Errorf("intent differs from output state")
		}
	}
	committed := func(e ledger.Envelope) bool {
		for _, row := range s.Outbox {
			if e.Equal(row.Envelope) {
				return true
			}
		}
		return false
	}
	for p, envelopes := range m.Output {
		for _, e := range envelopes {
			if e.Cause.Partition != p || !committed(e) {
				return fmt.Errorf("relay published uncommitted or altered envelope")
			}
		}
	}
	for _, e := range m.View.Rows {
		if !committed(e) {
			return fmt.Errorf("view contains uncommitted or altered envelope")
		}
	}
	for p, next := range m.View.Next {
		if next < 0 || next > int64(len(m.Output[p])) {
			return fmt.Errorf("view frontier exceeds output")
		}
	}
	var prefix []ledger.Record
	for _, r := range m.Source {
		if r.Offset < s.Partitions[r.Partition].Next {
			prefix = append(prefix, r)
		}
	}
	expected, err := oracle.Compute(m.Config, prefix)
	if err != nil {
		return err
	}
	for _, want := range expected.Keys {
		got := s.Keys[want.Key].Projection
		if got.Status != want.Status || !slices.Equal(got.Reasons, want.Reasons) {
			return fmt.Errorf("[amends:trust-agreement] worker trust differs from oracle")
		}
		if want.Status != "BLOCKED" && (!slices.Equal(got.Balances, want.Balances) || !slices.Equal(got.Alerts, want.Alerts)) {
			return fmt.Errorf("[amends:projection-agreement] worker projection differs from oracle: %+v", want.Key)
		}
	}
	if !sameRejections(s.Rejections, expected.Rejections) {
		return fmt.Errorf("quarantine/frontier mismatch")
	}
	return nil
}
