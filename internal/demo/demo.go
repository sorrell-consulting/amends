// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Package demo runs the embedded, hand-calculated correction example in memory.
package demo

import (
	"bytes"
	"fmt"
	"io"
	"slices"

	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/oracle"
	"github.com/sorrell-consulting/amends/sim"
)

func Run(w io.Writer) error {
	c, err := fixtures.Config("core-demo")
	if err != nil {
		return err
	}
	m, err := sim.New(c, nil)
	if err != nil {
		return err
	}
	var out bytes.Buffer
	fmt.Fprintln(&out, "amends: correction example (memory only)")
	fmt.Fprintln(&out, "Opening balance: 100; reorder threshold: 60; UTC daily closes.")
	fmt.Fprintf(&out, "Namespace: %s; source: embedded synthetic fixtures.\n", c.Namespace)
	names := []string{"before.jsonl", "correction.jsonl", "cancel-last-day.jsonl"}
	expectations := []string{"expected-before.json", "expected-after.json", "expected-after-cancel.json"}
	labels := []string{"Before correction", "After Day 1 issue changes from 50 to 30 units", "After cancelling the last activity on Day 3"}
	for stage := range names {
		source, err := fixtures.Records(c, names[:stage+1]...)
		if err != nil {
			return err
		}
		m.Source = source
		before := len(m.Store.Snapshot().Outbox)
		if err := m.Drain(); err != nil {
			return err
		}
		want, err := fixtures.Expected(expectations[stage])
		if err != nil {
			return err
		}
		batch, err := oracle.Compute(c, source)
		if err != nil {
			return err
		}
		got := m.View.Projection(c.Keys[0].StockKey)
		for _, p := range []ledger.Projection{batch.Keys[0], got} {
			if p.Status != want.Status || !slices.Equal(p.Balances, want.Balances) || !slices.Equal(p.Alerts, want.Alerts) {
				return fmt.Errorf("stage %d differs from hand-stated fixture", stage)
			}
		}
		if r := m.Check(); r.Status != "PASS" {
			return fmt.Errorf("core comparison %s: %s", r.Status, r.Difference)
		}
		fmt.Fprintf(&out, "\n%s\n", labels[stage])
		report := m.Check()
		fmt.Fprintf(&out, "  READY; input stopped; source frontier %d; view frontier %d.\n", report.SourceEnd[0], report.OutputEnd[0])
		for _, b := range got.Balances {
			fmt.Fprintf(&out, "  %s  closing balance %d\n", b.Day, b.Close)
		}
		for _, a := range got.Alerts {
			fmt.Fprintf(&out, "  crossing: %s (close %d < %d)\n", a.Day, a.Close, a.Threshold)
		}
		if stage > 0 {
			for _, row := range m.Store.Snapshot().Outbox[before:] {
				e := row.Envelope
				fmt.Fprintf(&out, "  %s %s version %d\n", e.Type, e.ID.Day, e.Version)
			}
		}
		fmt.Fprintln(&out, "  CORE_AGREEMENT: fixture, independent oracle, and memory view agree.")
	}
	fmt.Fprintln(&out, "\nDeletion versions remain in the memory view. CORE_AGREEMENT checks this in-memory example.")
	fmt.Fprintln(&out, "For PostgreSQL/Redpanda verification, run: make demo MODE=correction-only")
	fmt.Fprintln(&out, "For an owned worker crash and recovery, run: make drill DRILL=crash-mid-correction")
	_, err = w.Write(out.Bytes())
	return err
}
