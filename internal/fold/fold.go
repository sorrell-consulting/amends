// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Package fold implements incremental revision authority and suffix repair.
// All functions are pure: callers commit a returned decision, or discard it.
// Decode and check each input with [Validate] before passing it to [Apply].
package fold

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/sorrell-consulting/amends/internal/ledger"
)

// Candidate is one canonical payload and the source coordinates that asserted
// it. Repeated assertions share a candidate even if their SourceTime differs.
//
// The JSON tags on Candidate, Identity, and State are the persisted version-1
// format. Keep them stable; postgres owns the codec and compatibility test.
type Candidate struct {
	Transaction ledger.Transaction   `json:"Transaction"`
	Sources     []ledger.Coordinates `json:"Sources"`
}

// Identity retains every observed revision of one transaction. Highest is the
// greatest revision seen; multiple candidates there mean unresolved authority.
// Lower revisions remain available for historical conflict diagnostics.
type Identity struct {
	Highest   int64                 `json:"Highest"`
	Revisions map[int64][]Candidate `json:"Revisions"`
}

// State holds revision evidence and the last computed projection for one key.
// Identities is indexed by TxnID. Use [Empty] to initialize a state; its zero
// value does not identify a stock key. Apply and Clone return independent copies.
type State struct {
	Identities map[string]Identity `json:"Identities"`
	Projection ledger.Projection   `json:"Projection"`
}

// Empty returns a READY state with no transactions or business facts for key.
func Empty(key ledger.StockKey) State {
	return State{Identities: map[string]Identity{}, Projection: ledger.Projection{Key: key, Status: ledger.Ready, Balances: []ledger.Balance{}, Alerts: []ledger.Alert{}}}
}

// Clone returns a deep copy of the evidence maps, source lists, and projection.
func (s State) Clone() State {
	n := State{Identities: map[string]Identity{}, Projection: s.Projection.Clone()}
	for id, v := range s.Identities {
		w := Identity{Highest: v.Highest, Revisions: map[int64][]Candidate{}}
		for rev, cs := range v.Revisions {
			for _, c := range cs {
				c.Sources = slices.Clone(c.Sources)
				w.Revisions[rev] = append(w.Revisions[rev], c)
			}
		}
		n.Identities[id] = w
	}
	return n
}

// CanonicalEqual reports whether identity, revision, kind, effective instant,
// and quantity match. SourceTime and transport coordinates do not set authority.
func CanonicalEqual(a, b ledger.Transaction) bool {
	return a.TxnID == b.TxnID && a.StockKey == b.StockKey && a.Revision == b.Revision && a.Kind == b.Kind && a.ValidTime.Equal(b.ValidTime) && a.Qty == b.Qty
}

// Validate decodes r and checks its business fields, key, and configured routing.
// It returns a UTC-normalized transaction and an empty reason on success, or a
// rejection reason code on failure. Only a successful transaction may be applied.
// Callers validate c and guard source incarnation and offsets separately.
func Validate(c ledger.Config, r ledger.Record) (ledger.Transaction, string) {
	t, err := ledger.Decode(r.Value)
	if err != nil {
		return t, "MALFORMED"
	}
	if t.TxnID == "" || t.StockKey.Item == "" || t.StockKey.Location == "" {
		return t, "IDENTITY"
	}
	if t.Revision < 1 {
		return t, "REVISION"
	}
	if t.Kind != ledger.New && t.Kind != ledger.Amend && t.Kind != ledger.Cancel {
		return t, "KIND"
	}
	if t.Kind == ledger.Cancel && t.Qty != 0 {
		return t, "CANCEL_QUANTITY"
	}
	if t.ValidTime.Year() < 1 || t.ValidTime.Year() > 9999 || t.ValidTime.Before(c.Origin) {
		return t, "VALID_TIME"
	}
	k, ok := c.Key(t.StockKey)
	if !ok {
		return t, "UNKNOWN_KEY"
	}
	if r.Key != t.StockKey {
		return t, "KEY_MISMATCH"
	}
	if r.Partition != k.Partition {
		return t, "ROUTING"
	}
	return t, ""
}

// Apply returns the state after retaining t and repairing affected business
// facts, without mutating previous. t must have passed [Validate], and previous,
// t, and k must describe the same stock key. at identifies the source assertion.
//
// Stale candidates remain diagnosable. Highest-revision ambiguity returns a
// BLOCKED state with frozen business facts, not an error. Resolving all current
// conflicts recomputes the whole key. Arithmetic failure returns an error and
// no usable next state; callers must not commit any part of that decision.
func Apply(previous State, t ledger.Transaction, at ledger.Coordinates, k ledger.KeyConfig) (State, error) {
	next, _, err := ApplyMeasured(previous, t, at, k)
	return next, err
}

func apply(previous State, t ledger.Transaction, at ledger.Coordinates, k ledger.KeyConfig, work *Work) (State, error) {
	s := previous.Clone()
	id, exists := s.Identities[t.TxnID]
	if !exists {
		id = Identity{Revisions: map[int64][]Candidate{}}
	}
	work.Stale = t.Revision < id.Highest
	old, oldActive := active(id)
	cs := id.Revisions[t.Revision]
	duplicate := false
	for i := range cs {
		if CanonicalEqual(cs[i].Transaction, t) {
			duplicate = true
			if !slices.Contains(cs[i].Sources, at) {
				cs[i].Sources = append(cs[i].Sources, at)
			}
			break
		}
	}
	if !duplicate {
		cs = append(cs, Candidate{Transaction: t, Sources: []ledger.Coordinates{at}})
	}
	work.Duplicate = duplicate
	id.Revisions[t.Revision] = cs
	if t.Revision > id.Highest {
		id.Highest = t.Revision
	}
	s.Identities[t.TxnID] = id
	reasons := []string{}
	for txn, v := range s.Identities {
		if len(v.Revisions[v.Highest]) > 1 {
			reasons = append(reasons, (ledger.Conflict{StockKey: k.StockKey, TxnID: txn, Revision: v.Highest}).Ref())
		}
	}
	slices.Sort(reasons)
	s.Projection.Reasons = reasons
	if len(reasons) > 0 {
		// Keep the last business facts while recording arrivals during the block.
		s.Projection.Status = ledger.Blocked
		return s, nil
	}
	s.Projection.Status = ledger.Ready
	var earliest string
	if previous.Projection.Status == ledger.Blocked {
		// All arrivals retained during the block must participate in this repair.
		earliest = "0001-01-01"
		work.FullRefold = true
	} else if !duplicate && t.Revision >= id.Highest {
		if oldActive {
			earliest = old.ValidTime.UTC().Format("2006-01-02")
		}
		if next, ok := active(id); ok {
			day := next.ValidTime.UTC().Format("2006-01-02")
			if earliest == "" || day < earliest {
				earliest = day
			}
		}
	}
	if earliest == "" {
		return s, nil
	}
	p, err := refold(s, k, earliest, work)
	if err != nil {
		return State{}, err
	}
	s.Projection = p
	return s, nil
}

func active(id Identity) (ledger.Transaction, bool) {
	cs := id.Revisions[id.Highest]
	if len(cs) != 1 || cs[0].Transaction.Kind == ledger.Cancel {
		return ledger.Transaction{}, false
	}
	return cs[0].Transaction, true
}

func refold(s State, k ledger.KeyConfig, from string, work *Work) (ledger.Projection, error) {
	p := ledger.Projection{Key: k.StockKey, Status: ledger.Ready, Balances: []ledger.Balance{}, Alerts: []ledger.Alert{}}
	before := k.Opening
	for _, b := range s.Projection.Balances {
		if b.Day < from {
			p.Balances = append(p.Balances, b)
			before = b.Close
		}
	}
	for _, a := range s.Projection.Alerts {
		if a.Day < from {
			p.Alerts = append(p.Alerts, a)
		}
	}
	var txns []ledger.Transaction
	for _, id := range s.Identities {
		if t, ok := active(id); ok && t.ValidTime.Format("2006-01-02") >= from {
			txns = append(txns, t)
		}
	}
	sort.Slice(txns, func(i, j int) bool {
		if txns[i].ValidTime.Equal(txns[j].ValidTime) {
			return txns[i].TxnID < txns[j].TxnID
		}
		return txns[i].ValidTime.Before(txns[j].ValidTime)
	})
	work.Refold = true
	work.FromDay = from
	work.RefoldedRecords = int64(len(txns))
	if work.FullRefold {
		work.FromDay = ""
		if len(s.Projection.Balances) > 0 {
			work.FromDay = s.Projection.Balances[0].Day
		}
		if len(txns) > 0 {
			firstDay := txns[0].ValidTime.UTC().Format("2006-01-02")
			if work.FromDay == "" || firstDay < work.FromDay {
				work.FromDay = firstDay
			}
		}
	}
	if len(s.Projection.Balances) > 0 && work.FromDay != "" {
		previousLast := s.Projection.Balances[len(s.Projection.Balances)-1].Day
		if previousLast > work.FromDay {
			start, err := time.Parse("2006-01-02", work.FromDay)
			if err != nil {
				return p, err
			}
			end, err := time.Parse("2006-01-02", previousLast)
			if err != nil {
				return p, err
			}
			work.RewindDays = (end.Unix() - start.Unix()) / 86400
		}
	}
	for i := 0; i < len(txns); {
		day := txns[i].ValidTime.Format("2006-01-02")
		var quantities []int64
		for i < len(txns) && txns[i].ValidTime.Format("2006-01-02") == day {
			quantities = append(quantities, txns[i].Qty)
			i++
		}
		close, err := exactClose(before, quantities)
		if err != nil {
			return p, fmt.Errorf("%s: %w", day, err)
		}
		p.Balances = append(p.Balances, ledger.Balance{Day: day, Close: close})
		if before >= k.Threshold && close < k.Threshold {
			p.Alerts = append(p.Alerts, ledger.Alert{Day: day, Threshold: k.Threshold, Close: close})
		}
		before = close
	}
	return p, nil
}

// Cancel opposite signs before accumulating like signs. Every opposite-sign
// addition fits int64; once only one sign remains, overflow implies the exact
// final close is out of range. No transient intraday ordering determines validity.
// The oracle instead uses big.Int full-history aggregation.
func exactClose(value int64, quantities []int64) (int64, error) {
	var positive, negative []int64
	for _, q := range quantities {
		if q >= 0 {
			positive = append(positive, q)
		} else {
			negative = append(negative, q)
		}
	}
	for len(positive)+len(negative) > 0 {
		var q int64
		if len(positive) > 0 && (value < 0 || len(negative) == 0) {
			q = positive[len(positive)-1]
			positive = positive[:len(positive)-1]
		} else {
			q = negative[len(negative)-1]
			negative = negative[:len(negative)-1]
		}
		if q > 0 && value > math.MaxInt64-q || q < 0 && value < math.MinInt64-q {
			return 0, fmt.Errorf("balance overflow")
		}
		value += q
	}
	return value, nil
}

// Conflicts returns sorted conflict identities from all retained revisions,
// including superseded conflicts that no longer block computation.
func (s State) Conflicts() []ledger.Conflict {
	var result []ledger.Conflict
	for txn, id := range s.Identities {
		for revision, cs := range id.Revisions {
			if len(cs) > 1 {
				result = append(result, ledger.Conflict{StockKey: s.Projection.Key, TxnID: txn, Revision: revision})
			}
		}
	}
	slices.SortFunc(result, func(a, b ledger.Conflict) int {
		if a.Ref() < b.Ref() {
			return -1
		}
		if a.Ref() > b.Ref() {
			return 1
		}
		return 0
	})
	return result
}
