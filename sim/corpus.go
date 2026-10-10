// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package sim

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"runtime"
	"slices"
	"time"

	"github.com/sorrell-consulting/amends/internal/ledger"
)

const GeneratorVersion = 1
const ImplementationVersion = "core-schedule-v1"

// Trace includes raw records and explicit choices, so replay never depends on
// a seed retaining its meaning after a generator or scheduler change.
type Trace struct {
	Version        int             `json:"version"`
	Implementation string          `json:"implementation"`
	Toolchain      string          `json:"toolchain"`
	ConfigDigest   string          `json:"config_digest"`
	Seed           uint64          `json:"seed"`
	Config         ledger.Config   `json:"config"`
	Source         []ledger.Record `json:"source"`
	Steps          []Step          `json:"steps"`
}

func Generate(seed uint64, invalid bool) Trace {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	origin := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c := ledger.Config{SchemaVersion: 1, Namespace: fmt.Sprintf("seed-%d", seed), Source: "generated-v1", Partitions: 2, Origin: origin}
	for i := 0; i < 3; i++ {
		c.Keys = append(c.Keys, ledger.KeyConfig{StockKey: ledger.StockKey{Item: fmt.Sprintf("ITEM-%d", i), Location: "DEPOT"}, Opening: 100, Threshold: 60, Partition: i % 2})
	}
	t := Trace{Version: GeneratorVersion, Implementation: ImplementationVersion, Toolchain: runtime.Version(), ConfigDigest: c.Digest(), Seed: seed, Config: c}
	var logical []ledger.Transaction
	identities := 1 + rng.IntN(8)
	for identity := 0; identity < identities; identity++ {
		key := c.Keys[rng.IntN(len(c.Keys))]
		revisions := int64(1 + rng.IntN(4))
		for revision := int64(1); revision <= revisions; revision++ {
			kind := ledger.New
			if revision > 1 {
				kind = ledger.Amend
			}
			qty := int64(rng.IntN(121) - 60)
			if rng.IntN(5) == 0 {
				kind = ledger.Cancel
				qty = 0
			}
			transaction := ledger.Transaction{TxnID: fmt.Sprintf("TX-%d", identity), Revision: revision, Kind: kind, StockKey: key.StockKey, ValidTime: origin.Add(time.Duration(rng.IntN(10))*24*time.Hour + time.Duration(rng.IntN(24))*time.Hour), Qty: qty, SourceTime: origin.Add(20 * 24 * time.Hour)}
			logical = append(logical, transaction)
			if rng.IntN(3) == 0 {
				duplicate := transaction
				duplicate.SourceTime = duplicate.SourceTime.Add(time.Hour)
				logical = append(logical, duplicate)
			}
		}
	}
	if invalid && len(logical) > 0 {
		conflict := logical[0]
		if conflict.Kind == "CANCEL" {
			conflict.Kind = "AMEND"
		}
		conflict.Qty++
		logical = append(logical, conflict)
	}
	rng.Shuffle(len(logical), func(i, j int) { logical[i], logical[j] = logical[j], logical[i] })
	positions := map[int]int64{}
	for _, transaction := range logical {
		key, _ := c.Key(transaction.StockKey)
		raw, _ := json.Marshal(transaction)
		t.Source = append(t.Source, ledger.Record{Coordinates: ledger.Coordinates{Source: c.Source, Partition: key.Partition, Offset: positions[key.Partition]}, Key: transaction.StockKey, Value: raw})
		positions[key.Partition]++
	}
	if invalid {
		t.Source = append(t.Source, ledger.Record{Coordinates: ledger.Coordinates{Source: c.Source, Partition: 0, Offset: positions[0]}, Key: c.Keys[0].StockKey, Value: []byte(`{"qty":null}`)})
	}
	actors := []string{"worker", "relay", "view", "claim"}
	outcomes := []string{Commit, Commit, Commit, Abort, UnknownCommit, UnknownAbort}
	for i := 0; i < 80; i++ {
		t.Steps = append(t.Steps, Step{Actor: "tick", Partition: 0, Outcome: Commit})
		t.Steps = append(t.Steps, Step{Actor: actors[rng.IntN(len(actors))], Partition: rng.IntN(c.Partitions), Outcome: outcomes[rng.IntN(len(outcomes))]})
	}
	return t
}

func Run(trace Trace) (*Model, error) {
	if trace.Version != GeneratorVersion {
		return nil, fmt.Errorf("unsupported trace version %d", trace.Version)
	}
	if trace.Implementation != ImplementationVersion || trace.Toolchain == "" || trace.ConfigDigest != trace.Config.Digest() {
		return nil, fmt.Errorf("incompatible trace provenance or configuration digest")
	}
	m, err := New(trace.Config, trace.Source)
	if err != nil {
		return nil, err
	}
	for i, step := range trace.Steps {
		if err := m.Step(step); err != nil {
			return m, fmt.Errorf("step %d: %w", i, err)
		}
	}
	if err := m.Drain(); err != nil {
		return m, err
	}
	if err := m.Safety(); err != nil {
		return m, err
	}
	r := m.Check()
	if r.Status != "PASS" && r.Status != "PASS_WITH_EXCLUSIONS" && r.Status != "BLOCKED" {
		return m, fmt.Errorf("model %s: %s", r.Status, r.Difference)
	}
	if r.Difference != "" {
		return m, fmt.Errorf("model evidence mismatch: %s", r.Difference)
	}
	return m, nil
}

// Shrink removes schedule chunks, then source chunks, retaining original source
// coordinates and complete replacement assertions. It only accepts a candidate
// when the caller confirms the same property still fails. The bounded generator
// keeps every subset in the arithmetic domain. Reduction need not find the
// smallest counterexample.
func Shrink(original Trace, fails func(Trace) bool) Trace {
	t := original
	t.Source = slices.Clone(t.Source)
	t.Steps = slices.Clone(t.Steps)
	if !fails(t) {
		return t
	}
	for size := len(t.Steps)/2 + 1; size >= 1; size /= 2 {
		for i := 0; i+size <= len(t.Steps); {
			candidate := t
			candidate.Steps = append(slices.Clone(t.Steps[:i]), t.Steps[i+size:]...)
			if fails(candidate) {
				t = candidate
			} else {
				i++
			}
		}
	}
	for size := len(t.Source)/2 + 1; size >= 1; size /= 2 {
		for i := 0; i+size <= len(t.Source); {
			candidate := t
			candidate.Source = append(slices.Clone(t.Source[:i]), t.Source[i+size:]...)
			if fails(candidate) {
				t = candidate
			} else {
				i++
			}
		}
	}
	return t
}
