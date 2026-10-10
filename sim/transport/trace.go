// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Package transport schedules the production delivery orchestration against
// controlled memory I/O. It supplies no database or broker durability evidence.
package transport

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"slices"

	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/sim"
)

const Version = 1
const Implementation = "delivery-schedule-v1"

// Fault targets a numbered call within one attempt, including recovery reads.
// DelayMillis delays the observation, after the chosen durable outcome. An
// unvisited fault remains visible in the report; it is not counted as coverage.
type Fault struct {
	Operation   string `json:"operation"`
	Call        int    `json:"call"`
	Outcome     string `json:"outcome"`
	DelayMillis int64  `json:"delay_ms,omitempty"`
}

type Step struct {
	Actor     string  `json:"actor"`
	Partition int     `json:"partition"`
	Offset    int64   `json:"offset,omitempty"` // redeliver an earlier published record
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
		return fmt.Errorf("incompatible delivery trace provenance or configuration")
	}
	if err := t.Config.Validate(); err != nil {
		return err
	}
	for i, s := range t.Steps {
		if s.Partition < 0 || s.Partition >= t.Config.Partitions || s.Offset < 0 {
			return fmt.Errorf("step %d: invalid partition/offset", i)
		}
		switch s.Actor {
		case "relay", "view", "redeliver", "resume-relay", "resume-view":
		default:
			return fmt.Errorf("step %d: unknown actor", i)
		}
		if s.Actor != "redeliver" && s.Offset != 0 {
			return fmt.Errorf("step %d: offset only applies to redelivery", i)
		}
		seen := map[string]bool{}
		for _, f := range s.Faults {
			reads := s.Actor == "relay" && (f.Operation == "pending" || f.Operation == "receipt-read") || s.Actor == "view" && f.Operation == "view-read"
			writes := s.Actor == "relay" && (f.Operation == "publish" || f.Operation == "receipt-write") || s.Actor == "view" && f.Operation == "view-write"
			if !reads && !writes || f.Call < 1 || f.Call > 9 || f.DelayMillis < 0 || f.DelayMillis > 60_000 {
				return fmt.Errorf("step %d: invalid I/O fault", i)
			}
			switch f.Outcome {
			case sim.Commit, sim.Abort:
			case sim.UnknownAbort, sim.UnknownCommit, "crash-before", "crash-after":
				if !writes {
					return fmt.Errorf("step %d: ambiguous/crash outcome requires a write", i)
				}
			default:
				return fmt.Errorf("step %d: invalid fault outcome", i)
			}
			key := fmt.Sprintf("%s/%d", f.Operation, f.Call)
			if seen[key] {
				return fmt.Errorf("step %d: repeated fault selector", i)
			}
			seen[key] = true
		}
	}
	return nil
}

// Generate preserves raw history in the trace, independently of future seed
// generator changes. Sources are prepared before this delivery-only schedule.
func Generate(seed uint64, invalid bool) Trace {
	base := sim.Generate(seed, invalid)
	t := Trace{Version: Version, Implementation: Implementation, Toolchain: runtime.Version(), Seed: seed, ConfigDigest: base.ConfigDigest, Config: base.Config, Source: base.Source, Drain: true}
	rng := rand.New(rand.NewPCG(seed, seed^0xd311e7))
	for i := 0; i < 48; i++ {
		actor := []string{"relay", "view", "redeliver", "resume-relay", "resume-view"}[rng.IntN(5)]
		s := Step{Actor: actor, Partition: rng.IntN(t.Config.Partitions)}
		if actor == "relay" || actor == "view" {
			ops := []string{"pending", "publish", "receipt-write", "receipt-read"}
			if actor == "view" {
				ops = []string{"view-write", "view-read"}
			}
			op := ops[rng.IntN(len(ops))]
			outcomes := []string{sim.Commit, sim.Abort}
			if op == "publish" || op == "receipt-write" || op == "view-write" {
				outcomes = append(outcomes, sim.UnknownAbort, sim.UnknownCommit, "crash-before", "crash-after")
			}
			s.Faults = []Fault{{Operation: op, Call: 1, Outcome: outcomes[rng.IntN(len(outcomes))], DelayMillis: int64(rng.IntN(5) * 50)}}
			if rng.IntN(3) == 0 {
				for call := 2; call <= 3; call++ {
					f := s.Faults[0]
					f.Call = call
					s.Faults = append(s.Faults, f)
				}
			}
			// Recovery reads occur only after a commit reply was lost.
			if op == "receipt-read" || op == "view-read" {
				write := "receipt-write"
				if actor == "view" {
					write = "view-write"
				}
				s.Faults = append(s.Faults, Fault{Operation: write, Call: 1, Outcome: []string{sim.UnknownAbort, sim.UnknownCommit}[rng.IntN(2)]})
			}
		}
		t.Steps = append(t.Steps, s)
	}
	return t
}

// Shrink removes whole attempts and fault choices while retaining the caller's
// exact failure predicate. It preserves raw history and valid complete records.
// Reduction need not find the smallest counterexample.
func Shrink(original Trace, fails func(Trace) bool) Trace {
	t := original
	t.Steps = slices.Clone(original.Steps)
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
	for i := range t.Steps {
		for j := 0; j < len(t.Steps[i].Faults); {
			candidate := t
			candidate.Steps = slices.Clone(t.Steps)
			candidate.Steps[i].Faults = append(slices.Clone(t.Steps[i].Faults[:j]), t.Steps[i].Faults[j+1:]...)
			if fails(candidate) {
				t = candidate
			} else {
				j++
			}
		}
	}
	return t
}
