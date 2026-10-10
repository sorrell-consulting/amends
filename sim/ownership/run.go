// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package ownership

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"time"

	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/worker"
	"github.com/sorrell-consulting/amends/sim"
)

type Event struct {
	Step        int    `json:"step"`
	Worker      int    `json:"worker"`
	Operation   string `json:"operation"`
	Call        int    `json:"call,omitempty"`
	Outcome     string `json:"outcome,omitempty"`
	Applied     bool   `json:"effect_applied"`
	Observation string `json:"observation"`
	Selected    bool   `json:"selected_fault,omitempty"`
	AtMillis    int64  `json:"at_ms"`
	Partition   int    `json:"partition"`
	Epoch       int64  `json:"epoch"`
	Next        int64  `json:"next_offset"`
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
type member struct {
	life    *worker.Lifecycle
	adapter *adapter
	cursor  map[int]int64
}
type harness struct {
	ctx     context.Context
	result  *Result
	workers map[int]*member
	ends    []int64
	index   int
	failure error
}

func Run(ctx context.Context, trace Trace) (result *Result, err error) {
	if err = trace.validate(); err != nil {
		return nil, err
	}
	m, err := sim.NewUnclaimed(trace.Config, trace.Source)
	if err != nil {
		return nil, err
	}
	result = &Result{Model: m, Report: Report{Scope: Scope, Implementation: Implementation}}
	h := &harness{ctx: ctx, result: result, workers: map[int]*member{}, ends: make([]int64, trace.Config.Partitions)}
	defer func() {
		for _, w := range h.workers {
			w.life.Stop(context.Canceled)
			w.life.Revoke()
		}
		result.Report.Boundary = m.Check()
		result.Report.Boundary.Scope = Scope
		result.Report.Status = result.Report.Boundary.Status
		if err != nil {
			result.Report.Status = "ERROR"
		}
	}()
	for _, r := range trace.Source {
		h.ends[r.Partition] = r.Offset + 1
	}
	for _, step := range trace.Steps {
		if err = h.attempt(ctx, step, false); err != nil {
			return result, err
		}
	}
	result.Report.BeforeDrain = m.Check()
	result.Report.BeforeDrain.Scope = Scope
	if trace.Drain {
		ids := make([]int, 0, len(h.workers))
		next := 0
		for id := range h.workers {
			ids = append(ids, id)
			next = max(next, id+1)
		}
		slices.Sort(ids)
		for _, id := range ids {
			if err = h.attempt(ctx, Step{Actor: "stop", Worker: id}, true); err != nil {
				return result, err
			}
		}
		if err = h.attempt(ctx, Step{Actor: "start", Worker: next}, true); err != nil {
			return result, err
		}
		for p := range trace.Config.Partitions {
			for _, actor := range []string{"assign", "seek"} {
				if err = h.attempt(ctx, Step{Actor: actor, Worker: next, Partition: p}, true); err != nil {
					return result, err
				}
			}
			for h.nextRecord(next, p) != nil {
				before := m.Store.Snapshot().Partitions[p].Next
				if err = h.attempt(ctx, Step{Actor: "process", Worker: next, Partition: p}, true); err != nil {
					return result, err
				}
				if m.Store.Snapshot().Partitions[p].Next <= before {
					return result, fmt.Errorf("fair worker made no progress")
				}
			}
			// Downstream delivery is the established no-fault memory model here;
			// production delivery schedules are tested separately by sim/transport.
			for {
				if _, ok := m.Store.Pending(p); !ok {
					break
				}
				if err = m.Step(sim.Step{Actor: "relay", Partition: p, Outcome: sim.Commit}); err != nil {
					return result, err
				}
			}
			for m.View.Next[p] < int64(len(m.Output[p])) {
				if err = m.Step(sim.Step{Actor: "view", Partition: p, Outcome: sim.Commit}); err != nil {
					return result, err
				}
			}
		}
		r := m.Check()
		if r.Difference != "" || (r.Status != "PASS" && r.Status != "PASS_WITH_EXCLUSIONS" && r.Status != "BLOCKED") {
			return result, fmt.Errorf("drained worker model %s: %s", r.Status, r.Difference)
		}
	}
	return result, m.Safety()
}

func (h *harness) start(ctx context.Context, id int) error {
	if h.workers[id] != nil {
		return fmt.Errorf("worker lifetime reused")
	}
	a := &adapter{h: h, id: id, calls: map[string]int{}}
	life, err := worker.NewLifecycle(ctx, worker.LifecycleOptions{
		Config: h.result.Model.Config, State: a, Validate: a.validate, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Wait: func(ctx context.Context, d time.Duration) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			h.result.Report.LogicalMillis += d.Milliseconds()
			h.event(Event{Worker: id, Operation: "retry-wait", Observation: "elapsed", Partition: a.partition})
			return h.safety()
		},
	})
	if err != nil {
		return err
	}
	h.workers[id] = &member{life: life, adapter: a, cursor: map[int]int64{}}
	return nil
}

func (h *harness) attempt(ctx context.Context, s Step, recovery bool) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	h.index = len(h.result.Report.Attempts)
	a := Attempt{Step: s, Recovery: recovery, Result: "completed"}
	var err error
	w := h.workers[s.Worker]
	wasRetired := w != nil && w.life.Err() != nil
	if s.Actor == "start" {
		err = h.start(ctx, s.Worker)
	} else if w == nil {
		// A reserved replacement lifetime need not have been started: the
		// selector that would start it may never have been reached.
		a.Result = "unstarted"
	} else {
		w.adapter.faults, w.adapter.calls, w.adapter.used, w.adapter.partition = s.Faults, map[string]int{}, 0, s.Partition
		live := w.life.Context()
		switch s.Actor {
		case "stop":
			w.life.Stop(context.Canceled)
			w.life.Revoke()
			clear(w.cursor)
			a.Result = "stopped"
		case "revoke":
			w.life.Revoke()
			clear(w.cursor)
			a.Result = "revoked"
		case "assign":
			delete(w.cursor, s.Partition)
			err = w.life.Assign(live, []int{s.Partition})
		case "seek":
			var positions map[int]int64
			positions, err = w.life.Seek(live, []int{s.Partition})
			if err == nil {
				w.cursor[s.Partition] = positions[s.Partition]
			}
		case "process":
			if _, ok := w.cursor[s.Partition]; !ok {
				a.Result = "unseeked"
			} else if r := h.nextRecord(s.Worker, s.Partition); r != nil {
				w.cursor[s.Partition] = r.Offset + 1 // broker fetch progress, not a receipt
				err = w.life.Process(live, *r)
			} else {
				a.Result = "idle"
			}
		}
		a.FaultsUsed = w.adapter.used
	}
	if err != nil {
		a.Result, a.Error = "retired", err.Error()
		if w != nil && w.life.Err() == nil {
			return fmt.Errorf("failed operation left worker live: %w", err)
		}
	}
	h.result.Report.Attempts = append(h.result.Report.Attempts, a)
	if h.failure != nil {
		return h.failure
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if recovery && err != nil {
		return fmt.Errorf("fair recovery failed: %w", err)
	}
	if err != nil && !wasRetired && !errors.Is(err, store.ErrFence) && !errors.Is(err, context.Canceled) && (w == nil || w.adapter.used == 0) {
		return fmt.Errorf("unexpected worker error: %w", err)
	}
	return h.safety()
}

func (h *harness) nextRecord(id, p int) *ledger.Record {
	w := h.workers[id]
	next, ok := w.cursor[p]
	if !ok {
		return nil
	}
	for _, r := range h.result.Model.Source {
		if r.Partition == p && r.Offset >= next {
			copy := r.Clone()
			return &copy
		}
	}
	return nil
}
func (h *harness) event(e Event) {
	e.Step = h.index
	e.AtMillis = h.result.Report.LogicalMillis
	p := h.result.Model.Store.Snapshot().Partitions[e.Partition]
	e.Epoch, e.Next = p.Epoch, p.Next
	h.result.Report.Events = append(h.result.Report.Events, e)
}
func (h *harness) safety() error {
	if err := h.result.Model.Safety(); err != nil {
		h.failure = err
		return err
	}
	return nil
}
