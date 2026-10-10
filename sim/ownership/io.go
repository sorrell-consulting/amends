// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package ownership

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/sim"
)

var errUnavailable = errors.New("scheduled worker I/O unavailable")

type adapter struct {
	h                   *harness
	id, partition, used int
	faults              []Fault
	calls               map[string]int
}

// prepare returns an atomic effect. Interleaves occur outside that effect,
// matching the two possible epoch/processing serial orders at the store seam.
func (a *adapter) call(ctx context.Context, op string, p int, prepare func() (func() error, error)) error {
	h := a.h
	if ctx.Err() != nil {
		return ctx.Err()
	}
	a.calls[op]++
	f := Fault{Operation: op, Call: a.calls[op], Outcome: sim.Commit}
	selected := false
	for _, choice := range a.faults {
		if choice.Operation == op && choice.Call == f.Call {
			f = choice
			a.used++
			selected = true
			break
		}
	}
	interleave := func(at string) error {
		x := f.Interleave
		if x == nil || x.At != at {
			return nil
		}
		if x.Stop {
			h.workers[a.id].life.Stop(context.Canceled)
			h.event(Event{Worker: a.id, Partition: p, Operation: "stop-inflight", Observation: "canceled"})
		}
		if x.Replacement != nil {
			id := *x.Replacement
			if err := h.start(h.ctx, id); err != nil {
				return err
			}
			w := h.workers[id]
			w.adapter.partition = p
			h.event(Event{Worker: id, Partition: p, Operation: "replacement-start", Observation: "started"})
			if err := w.life.Assign(w.life.Context(), []int{p}); err != nil {
				return err
			}
			positions, err := w.life.Seek(w.life.Context(), []int{p})
			if err != nil {
				return err
			}
			w.cursor[p] = positions[p]
		}
		return h.safety()
	}
	var err error
	fatalInterleave := func(at string) {
		if e := interleave(at); e != nil {
			h.failure = e
			err = e
		}
	}
	fatalInterleave("before")
	if err == nil {
		err = ctx.Err()
	}
	var effect func() error
	if err == nil {
		effect, err = prepare()
	}
	if err == nil {
		err = ctx.Err()
	}
	applied := false
	if err == nil {
		if f.Outcome == sim.Commit || f.Outcome == sim.UnknownCommit {
			err = effect()
			applied = err == nil
		}
		if err == nil {
			switch f.Outcome {
			case sim.Abort:
				err = errUnavailable
			case sim.UnknownAbort, sim.UnknownCommit:
				err = store.ErrCommitUnknown
			}
		}
	}
	// A later replacement may follow either a committed or an aborted effect.
	fatalInterleave("after")
	h.result.Report.LogicalMillis += f.DelayMillis
	if ctx.Err() != nil && h.failure == nil {
		err = ctx.Err()
	}
	observation := "ok"
	switch {
	case err == nil:
	case errors.Is(err, store.ErrFence):
		observation = "fenced"
	case errors.Is(err, store.ErrCommitUnknown):
		observation = "unknown"
	case errors.Is(err, context.Canceled):
		observation = "canceled"
	case errors.Is(err, errUnavailable):
		observation = "unavailable"
	default:
		observation = "error"
		h.failure = err
	}
	h.event(Event{Worker: a.id, Partition: p, Operation: op, Call: f.Call, Outcome: f.Outcome, Applied: applied, Observation: observation, Selected: selected})
	if e := h.safety(); e != nil {
		return e
	}
	return err
}

func (a *adapter) validate(ctx context.Context) (ends []int64, err error) {
	err = a.call(ctx, "validate", a.partition, func() (func() error, error) { return func() error { ends = slices.Clone(a.h.ends); return nil }, nil })
	return
}
func (a *adapter) Claim(ctx context.Context, p int) (token store.Token, err error) {
	err = a.call(ctx, "claim", p, func() (func() error, error) {
		return func() error { var e error; token, e = a.h.result.Model.Store.Claim(p); return e }, nil
	})
	return
}
func (a *adapter) Observe(ctx context.Context, p int) (token store.Token, part store.Partition, err error) {
	err = a.call(ctx, "observe", p, func() (func() error, error) {
		return func() error {
			snapshot := a.h.result.Model.Store.Snapshot()
			part = snapshot.Partitions[p]
			// PostgreSQL permits observing an unclaimed epoch-zero partition.
			token = store.Token{Namespace: snapshot.Config.Namespace, Digest: snapshot.Config.Digest(), Partition: p, Epoch: part.Epoch}
			return nil
		}, nil
	})
	return
}

func (a *adapter) Process(ctx context.Context, token store.Token, next int64, r ledger.Record) error {
	m := a.h.result.Model
	fenced := func() bool {
		actual, e := m.Store.Observe(r.Partition)
		return e != nil || token != actual || next != m.Store.Snapshot().Partitions[r.Partition].Next
	}
	return a.call(ctx, "process", r.Partition, func() (func() error, error) {
		d, err := m.Store.Prepare(token, next, r)
		if err != nil {
			if fenced() {
				return nil, store.ErrFence
			}
			return nil, err
		}
		return func() error {
			stale := fenced()
			err := m.Store.Commit(d)
			if stale {
				if err == nil {
					return fmt.Errorf("obsolete epoch/frontier committed a processing decision")
				}
				return store.ErrFence
			}
			return err
		}, nil
	})
}
