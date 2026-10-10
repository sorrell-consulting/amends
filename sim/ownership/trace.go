// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Package ownership schedules the production worker lifecycle with memory I/O.
// It models epoch ordering, not PostgreSQL locks or Kafka callback delivery.
package ownership

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"slices"

	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/sim"
)

const Version = 1
const Implementation = "worker-schedule-v1"
const Scope = "production-worker-lifecycle; memory I/O and logical time"

// Interleave resolves a named boundary before returning to the original call.
// Replacement starts a fresh lifecycle and assigns/seeks the same partition.
type Interleave struct {
	At          string `json:"at"` // before or after the atomic modeled I/O effect
	Stop        bool   `json:"stop,omitempty"`
	Replacement *int   `json:"replacement,omitempty"`
}
type Fault struct {
	Operation   string      `json:"operation"` // validate, claim, observe, process
	Call        int         `json:"call"`
	Outcome     string      `json:"outcome"`
	DelayMillis int64       `json:"delay_ms,omitempty"`
	Interleave  *Interleave `json:"interleave,omitempty"`
}
type Step struct {
	Actor     string  `json:"actor"`  // start, assign, seek, process, revoke, stop
	Worker    int     `json:"worker"` // lifetime identity, never reused after stop
	Partition int     `json:"partition"`
	Faults    []Fault `json:"faults,omitempty"`
}
type Trace struct {
	Version        int             `json:"version"`
	Implementation string          `json:"implementation"`
	Toolchain      string          `json:"toolchain"`
	Seed           uint64          `json:"seed"`
	ConfigDigest   string          `json:"config_digest"`
	Config         ledger.Config   `json:"config"`
	Source         []ledger.Record `json:"source"`
	Steps          []Step          `json:"steps"`
	Drain          bool            `json:"drain"`
}

func (t Trace) validate() error {
	if t.Version != Version || t.Implementation != Implementation || t.Toolchain == "" || t.ConfigDigest != t.Config.Digest() {
		return fmt.Errorf("incompatible worker trace provenance or configuration")
	}
	if err := t.Config.Validate(); err != nil {
		return err
	}
	started := map[int]bool{}
	for i, s := range t.Steps {
		if s.Worker < 0 || s.Worker > 10000 || s.Partition < 0 || s.Partition >= t.Config.Partitions {
			return fmt.Errorf("step %d: invalid worker/partition", i)
		}
		switch s.Actor {
		case "start":
			if started[s.Worker] {
				return fmt.Errorf("step %d: worker lifetime reused", i)
			}
			started[s.Worker] = true
		case "assign", "seek", "process", "revoke", "stop":
			if !started[s.Worker] {
				return fmt.Errorf("step %d: worker not started", i)
			}
		default:
			return fmt.Errorf("step %d: unknown actor", i)
		}
		seen := map[string]bool{}
		for _, f := range s.Faults {
			allowed := (s.Actor == "assign" && (f.Operation == "validate" || f.Operation == "claim" || f.Operation == "observe")) || (s.Actor == "seek" && (f.Operation == "validate" || f.Operation == "observe")) || (s.Actor == "process" && (f.Operation == "process" || f.Operation == "observe"))
			if !allowed || f.Call < 1 || f.Call > 10 || f.DelayMillis < 0 || f.DelayMillis > 60000 {
				return fmt.Errorf("step %d: invalid fault selector", i)
			}
			switch f.Outcome {
			case sim.Commit, sim.Abort:
			case sim.UnknownAbort, sim.UnknownCommit:
				if f.Operation != "claim" && f.Operation != "process" {
					return fmt.Errorf("step %d: unknown outcome requires a write", i)
				}
			default:
				return fmt.Errorf("step %d: invalid outcome", i)
			}
			key := fmt.Sprintf("%s/%d", f.Operation, f.Call)
			if seen[key] {
				return fmt.Errorf("step %d: repeated fault selector", i)
			}
			seen[key] = true
			if x := f.Interleave; x != nil {
				if (x.At != "before" && x.At != "after") || (!x.Stop && x.Replacement == nil) {
					return fmt.Errorf("step %d: invalid interleave", i)
				}
				if x.Replacement != nil {
					id := *x.Replacement
					if id < 0 || id > 10000 || started[id] {
						return fmt.Errorf("step %d: replacement must be a fresh lifetime", i)
					}
					// Reserve the ID even if its fault selector is not reached.
					started[id] = true
				}
			}
		}
	}
	return nil
}

// Generate stores complete input/choices. Recovery uses a fresh worker lifetime.
func Generate(seed uint64, invalid bool) Trace {
	base := sim.Generate(seed, invalid)
	t := Trace{Version: Version, Implementation: Implementation, Toolchain: runtime.Version(), Seed: seed, ConfigDigest: base.ConfigDigest, Config: base.Config, Source: base.Source, Drain: true}
	rng := rand.New(rand.NewPCG(seed, seed^0x0a11ce))
	for round := 0; round < 8; round++ {
		id, p := round*2, rng.IntN(t.Config.Partitions)
		steps := []Step{{Actor: "start", Worker: id, Partition: p}, {Actor: "assign", Worker: id, Partition: p}, {Actor: "seek", Worker: id, Partition: p}, {Actor: "process", Worker: id, Partition: p}, {Actor: "process", Worker: id, Partition: p}, {Actor: "stop", Worker: id, Partition: p}, {Actor: "assign", Worker: id, Partition: p}, {Actor: "seek", Worker: id, Partition: p}}
		index := 1 + rng.IntN(3)
		op := map[int]string{1: "claim", 2: "observe", 3: "process"}[index]
		outcomes := []string{sim.Commit, sim.Abort}
		if index != 2 {
			outcomes = append(outcomes, sim.UnknownAbort, sim.UnknownCommit)
		}
		f := Fault{Operation: op, Call: 1, Outcome: outcomes[rng.IntN(len(outcomes))], DelayMillis: int64(rng.IntN(5) * 50)}
		switch rng.IntN(5) {
		case 0:
			f.Interleave = &Interleave{At: "before", Stop: true}
		case 1:
			replacement := id + 1
			f.Interleave = &Interleave{At: "after", Replacement: &replacement}
		case 2:
			replacement := id + 1
			at := "before"
			f.Interleave = &Interleave{At: at, Stop: index == 1, Replacement: &replacement}
		}
		steps[index].Faults = []Fault{f}
		// A selected recovery-read outage must first reach an unknown write.
		if index != 2 && (f.Outcome == sim.UnknownAbort || f.Outcome == sim.UnknownCommit) && rng.IntN(2) == 0 {
			for call := 1; call <= 3; call++ {
				steps[index].Faults = append(steps[index].Faults, Fault{Operation: "observe", Call: call, Outcome: sim.Abort})
			}
		}
		t.Steps = append(t.Steps, steps...)
	}
	return t
}

// Shrink preserves source history and accepts only structurally valid schedules
// retaining the supplied failure. Lifetime dependencies can prevent deletions.
func Shrink(original Trace, fails func(Trace) bool) Trace {
	t := original
	t.Steps = slices.Clone(original.Steps)
	if !fails(t) {
		return t
	}
	accept := func(c Trace) bool { return c.validate() == nil && fails(c) }
	for size := len(t.Steps)/2 + 1; size >= 1; size /= 2 {
		for i := 0; i+size <= len(t.Steps); {
			c := t
			c.Steps = append(slices.Clone(t.Steps[:i]), t.Steps[i+size:]...)
			if accept(c) {
				t = c
			} else {
				i++
			}
		}
	}
	for i := range t.Steps {
		for j := 0; j < len(t.Steps[i].Faults); {
			c := t
			c.Steps = slices.Clone(t.Steps)
			c.Steps[i].Faults = append(slices.Clone(t.Steps[i].Faults[:j]), t.Steps[i].Faults[j+1:]...)
			if accept(c) {
				t = c
			} else {
				j++
			}
		}
	}
	return t
}
