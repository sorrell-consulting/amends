// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/sim"
)

var errUnavailable = errors.New("scheduled I/O unavailable")
var errPublishUnknown = errors.New("scheduled publication reply lost")

func (h *harness) call(ctx context.Context, op string, effect func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h.calls[op]++
	f := Fault{Operation: op, Call: h.calls[op], Outcome: sim.Commit}
	for _, choice := range h.step.Faults {
		if choice.Operation == op && choice.Call == f.Call {
			f = choice
			h.used++
			break
		}
	}
	install := f.Outcome == sim.Commit || f.Outcome == sim.UnknownCommit || f.Outcome == "crash-after"
	if install {
		if err := effect(); err != nil {
			h.failure = fmt.Errorf("%s violated memory adapter contract: %w", op, err)
			return h.failure
		}
	}
	h.result.Report.LogicalMillis += f.DelayMillis
	observation := "ok"
	var err error
	switch f.Outcome {
	case sim.Abort:
		observation = "unavailable"
		err = errUnavailable
	case sim.UnknownAbort, sim.UnknownCommit:
		observation = "unknown"
		err = store.ErrCommitUnknown
		if op == "publish" {
			err = errPublishUnknown
		}
	case "crash-before", "crash-after":
		observation = "crash"
		h.cancel()
		err = context.Canceled
	}
	if err != nil {
		h.injectedErrors++
	}
	h.result.Report.Events = append(h.result.Report.Events, Event{Step: h.index, Operation: op, Call: f.Call, Outcome: f.Outcome, Applied: install, Observation: observation, AtMillis: h.result.Report.LogicalMillis})
	if e := h.safety(); e != nil {
		h.failure = e
		return e
	}
	return err
}

func (h *harness) Pending(ctx context.Context, p int) (d store.Delivery, ok bool, err error) {
	err = h.call(ctx, "pending", func() error {
		row, pending := h.result.Model.Store.Pending(p)
		ok = pending
		if pending {
			r, exists := h.intent[p][row.Sequence]
			if !exists || !r.envelope.Equal(row.Envelope) {
				return fmt.Errorf("pending envelope changed")
			}
			d = store.Delivery{Partition: p, Sequence: row.Sequence, Envelope: r.envelope.Clone(), Bytes: bytes.Clone(r.raw)}
		}
		return nil
	})
	return
}
func (h *harness) Publish(ctx context.Context, d store.Delivery) (offset int64, err error) {
	err = h.call(ctx, "publish", func() error {
		r, ok := h.intent[d.Partition][d.Sequence]
		if !ok || !r.envelope.Equal(d.Envelope) || !bytes.Equal(r.raw, d.Bytes) {
			return fmt.Errorf("retry altered committed envelope bytes or identity")
		}
		pending, ok := h.result.Model.Store.Pending(d.Partition)
		if !ok || pending.Sequence != d.Sequence {
			return fmt.Errorf("publication was not earliest pending intent")
		}
		offset = int64(len(h.wire[d.Partition]))
		h.appendOutput(d.Partition, r)
		return nil
	})
	return
}
func (h *harness) Acknowledge(ctx context.Context, p int, sequence, offset int64) error {
	return h.call(ctx, "receipt-write", func() error {
		if offset < 0 || offset >= int64(len(h.wire[p])) || h.wire[p][offset].sequence != sequence {
			return fmt.Errorf("receipt has no matching publication")
		}
		if old, ok := h.receipts[p][sequence]; ok {
			if old != offset {
				return fmt.Errorf("receipt changed")
			}
			return nil
		}
		for _, old := range h.receipts[p] {
			if offset <= old {
				return fmt.Errorf("receipt does not advance")
			}
		}
		if err := h.result.Model.Store.Acknowledge(p, sequence); err != nil {
			return err
		}
		h.receipts[p][sequence] = offset
		return nil
	})
}
func (h *harness) Receipt(ctx context.Context, p int, sequence int64) (receipt *int64, err error) {
	err = h.call(ctx, "receipt-read", func() error {
		if offset, ok := h.receipts[p][sequence]; ok {
			receipt = &offset
		}
		return nil
	})
	return
}
func (h *harness) ApplyViewInput(ctx context.Context, expected int64, input store.ViewInput) error {
	return h.call(ctx, "view-write", func() error {
		if input.Offset < 0 || input.Offset >= int64(len(h.wire[input.Partition])) {
			return fmt.Errorf("view input was not published")
		}
		r := h.wire[input.Partition][input.Offset]
		if !r.envelope.Equal(input.Envelope) || !bytes.Equal(r.raw, input.Value) {
			return fmt.Errorf("view input changed")
		}
		candidate := h.result.Model.View.Clone()
		if err := candidate.Apply(input.Partition, expected, input.Offset, input.Envelope); err != nil {
			return err
		}
		h.result.Model.View = candidate
		return nil
	})
}
func (h *harness) ViewPosition(ctx context.Context, p int) (position store.ViewPosition, err error) {
	err = h.call(ctx, "view-read", func() error {
		position = store.ViewPosition{Next: h.result.Model.View.Next[p], Error: h.result.Model.View.Errors[p]}
		return nil
	})
	return
}

// These are transport invariants, independent of retry/recovery decisions.
// Worker business state was checked against the source oracle during setup.
// The maximum delivered version per ID independently checks view rows at every
// I/O boundary, including ignored duplicates and retained withdrawals.
func (h *harness) safety() error {
	m := h.result.Model
	want := map[ledger.OutputID]ledger.Envelope{}
	for p := 0; p < m.Config.Partitions; p++ {
		if m.View.Next[p] < 0 || m.View.Next[p] > int64(len(h.wire[p])) || len(m.Output[p]) != len(h.wire[p]) {
			return fmt.Errorf("view/output frontier mismatch")
		}
		for i, r := range h.wire[p] {
			committed, ok := h.intent[p][r.sequence]
			if !ok || !bytes.Equal(r.raw, committed.raw) || !r.envelope.Equal(committed.envelope) || !r.envelope.Equal(m.Output[p][i]) {
				return fmt.Errorf("publication differs from committed intent")
			}
			if int64(i) < m.View.Next[p] {
				old, ok := want[r.envelope.ID]
				if !ok || r.envelope.Version > old.Version {
					want[r.envelope.ID] = r.envelope
				}
			}
		}
		for sequence, offset := range h.receipts[p] {
			if offset < 0 || offset >= int64(len(h.wire[p])) || h.wire[p][offset].sequence != sequence {
				return fmt.Errorf("receipt/output mismatch")
			}
		}
	}
	if len(want) != len(m.View.Rows) {
		return fmt.Errorf("view rows/progress split")
	}
	for id, e := range want {
		if !e.Equal(m.View.Rows[id]) {
			return fmt.Errorf("view version, payload, or tombstone differs from delivered prefix")
		}
	}
	for _, row := range m.Store.Snapshot().Outbox {
		_, receipted := h.receipts[row.Partition][row.Sequence]
		if row.Acknowledged != receipted || !row.Envelope.Equal(h.intent[row.Partition][row.Sequence].envelope) {
			return fmt.Errorf("outbox intent/receipt changed")
		}
	}
	return nil
}
