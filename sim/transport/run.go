// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sorrell-consulting/amends/internal/delivery"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/sim"
)

const Scope = "production-delivery-orchestration; memory I/O and logical time"

type Event struct {
	Step        int    `json:"step"`
	Operation   string `json:"operation"`
	Call        int    `json:"call"`
	Outcome     string `json:"outcome,omitempty"`
	Applied     bool   `json:"effect_applied"`
	Observation string `json:"observation"`
	AtMillis    int64  `json:"at_ms"`
}
type Attempt struct {
	Step       Step   `json:"step"`
	Recovery   bool   `json:"fair_recovery"`
	Result     string `json:"result"`
	Error      string `json:"error,omitempty"`
	FaultsUsed int    `json:"faults_used"`
}
type Report struct {
	Status         string     `json:"status"`
	Scope          string     `json:"scope"`
	Implementation string     `json:"implementation"`
	LogicalMillis  int64      `json:"logical_ms"`
	BeforeDrain    sim.Report `json:"before_drain"`
	Boundary       sim.Report `json:"boundary"`
	Attempts       []Attempt  `json:"attempts"`
	Events         []Event    `json:"events"`
}
type Result struct {
	Report Report
	Model  *sim.Model
}
type record struct {
	envelope ledger.Envelope
	raw      []byte
	sequence int64
}
type harness struct {
	result         *Result
	wire           map[int][]record
	intent         map[int]map[int64]record
	receipts       map[int]map[int64]int64
	parked         map[string]bool
	step           Step
	calls          map[string]int
	used           int
	injectedErrors int
	index          int
	cancel         context.CancelFunc
	failure        error
}

func Run(ctx context.Context, trace Trace) (result *Result, err error) {
	if err = trace.validate(); err != nil {
		return nil, err
	}
	m, err := sim.New(trace.Config, trace.Source)
	if err != nil {
		return nil, err
	}
	result = &Result{Model: m, Report: Report{Scope: Scope, Implementation: Implementation}}
	defer func() {
		result.Report.Boundary = m.Check()
		result.Report.Boundary.Scope = Scope
		result.Report.Status = result.Report.Boundary.Status
		if err != nil {
			result.Report.Status = "ERROR"
		}
	}()
	// Worker effects use the existing core model and independent source oracle.
	// Source processing completes before the downstream recovery schedule starts.
	for _, r := range trace.Source {
		if err = ctx.Err(); err != nil {
			return result, err
		}
		if err = m.Step(sim.Step{Actor: "worker", Partition: r.Partition, Outcome: sim.Commit}); err != nil {
			return result, err
		}
	}
	h := &harness{result: result, wire: map[int][]record{}, intent: map[int]map[int64]record{}, receipts: map[int]map[int64]int64{}, parked: map[string]bool{}}
	for p := 0; p < trace.Config.Partitions; p++ {
		h.intent[p] = map[int64]record{}
		h.receipts[p] = map[int64]int64{}
	}
	for _, row := range m.Store.Snapshot().Outbox {
		raw, e := json.Marshal(row.Envelope)
		if e != nil {
			return result, e
		}
		h.intent[row.Partition][row.Sequence] = record{envelope: row.Envelope.Clone(), raw: raw, sequence: row.Sequence}
	}
	for _, step := range trace.Steps {
		if err = h.attempt(ctx, step, false); err != nil {
			return result, err
		}
	}
	result.Report.BeforeDrain = m.Check()
	result.Report.BeforeDrain.Scope = Scope
	if trace.Drain {
		for _, actor := range []string{"relay", "view"} {
			if h.parked[actor] {
				if err = h.attempt(ctx, Step{Actor: "resume-" + actor}, true); err != nil {
					return result, err
				}
			}
		}
		for p := 0; p < trace.Config.Partitions; p++ {
			for {
				pending, ok := m.Store.Pending(p)
				if !ok {
					break
				}
				if err = h.attempt(ctx, Step{Actor: "relay", Partition: p}, true); err != nil {
					return result, err
				}
				if next, ok := m.Store.Pending(p); ok && next.Sequence == pending.Sequence {
					return result, fmt.Errorf("fair relay made no progress")
				}
			}
			for m.View.Next[p] < int64(len(h.wire[p])) {
				next := m.View.Next[p]
				if err = h.attempt(ctx, Step{Actor: "view", Partition: p}, true); err != nil {
					return result, err
				}
				if m.View.Next[p] <= next {
					return result, fmt.Errorf("fair view made no progress")
				}
			}
		}
		if err = m.Safety(); err != nil {
			return result, err
		}
		if r := m.Check(); r.Status == "INCOMPLETE" || r.Status == "ERROR" || r.Status == "DRIFT" || r.Difference != "" {
			return result, fmt.Errorf("drained model %s: %s", r.Status, r.Difference)
		}
	}
	return result, h.safety()
}

func (h *harness) attempt(parent context.Context, step Step, recovery bool) error {
	if err := parent.Err(); err != nil {
		return err
	}
	h.step, h.calls, h.used, h.index = step, map[string]int{}, 0, len(h.result.Report.Attempts)
	h.injectedErrors = 0
	a := Attempt{Step: step, Recovery: recovery, Result: "idle"}
	m := h.result.Model
	ctx, cancel := context.WithCancel(parent)
	h.cancel = cancel
	defer cancel()
	stepper := delivery.Stepper{Wait: func(ctx context.Context, d time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		h.result.Report.LogicalMillis += d.Milliseconds()
		h.result.Report.Events = append(h.result.Report.Events, Event{Step: h.index, Operation: "retry-wait", Observation: "elapsed", AtMillis: h.result.Report.LogicalMillis})
		return h.safety()
	}}
	var err error
	switch {
	case step.Actor == "resume-relay" || step.Actor == "resume-view":
		h.parked[step.Actor[len("resume-"):]] = false
		a.Result = "resumed"
	case h.parked[step.Actor]:
		a.Result = "parked"
	case step.Actor == "relay":
		var worked bool
		worked, err = stepper.RelayStep(ctx, h, h, step.Partition)
		if worked {
			a.Result = "completed"
		}
	case step.Actor == "view":
		next := m.View.Next[step.Partition]
		if next < int64(len(h.wire[step.Partition])) {
			r := h.wire[step.Partition][next]
			err = stepper.ViewStep(ctx, h, next, store.ViewInput{Partition: step.Partition, Offset: next, Envelope: r.envelope.Clone(), Value: append([]byte(nil), r.raw...)})
			if err == nil {
				a.Result = "completed"
			}
		}
	case step.Actor == "redeliver":
		if step.Offset < int64(len(h.wire[step.Partition])) {
			h.appendOutput(step.Partition, h.wire[step.Partition][step.Offset])
			a.Result = "redelivered"
		}
	}
	a.FaultsUsed = h.used
	if err != nil {
		a.Result, a.Error = "retired", err.Error()
		h.parked[step.Actor] = true
	}
	h.result.Report.Attempts = append(h.result.Report.Attempts, a)
	if parent.Err() != nil {
		return parent.Err()
	}
	if h.failure != nil {
		return h.failure
	}
	if err != nil && h.injectedErrors == 0 {
		return fmt.Errorf("unexpected %s failure without injected error: %w", step.Actor, err)
	}
	return h.safety()
}

func (h *harness) appendOutput(p int, r record) {
	r.envelope = r.envelope.Clone()
	r.raw = append([]byte(nil), r.raw...)
	h.wire[p] = append(h.wire[p], r)
	h.result.Model.Output[p] = append(h.result.Model.Output[p], r.envelope.Clone())
}
