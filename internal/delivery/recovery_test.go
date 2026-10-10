// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package delivery

import (
	"context"
	"errors"
	"testing"

	"github.com/sorrell-consulting/amends/internal/store"
)

// These doubles check retirement decisions when an observation cannot resolve
// an unknown commit. Real effect/transaction guarantees are integration tests.
type unresolvedState struct {
	receipt    int64
	position   store.ViewPosition
	observeErr error
	mutations  int
	cancel     context.CancelFunc
}

func (s *unresolvedState) Pending(context.Context, int) (store.Delivery, bool, error) {
	return store.Delivery{Partition: 0, Sequence: 1}, true, nil
}
func (s *unresolvedState) Acknowledge(context.Context, int, int64, int64) error {
	s.mutations++
	if s.cancel != nil {
		s.cancel()
	}
	return store.ErrCommitUnknown
}
func (s *unresolvedState) Receipt(context.Context, int, int64) (*int64, error) {
	return &s.receipt, s.observeErr
}
func (s *unresolvedState) ApplyViewInput(context.Context, int64, store.ViewInput) error {
	s.mutations++
	return store.ErrCommitUnknown
}
func (s *unresolvedState) ViewPosition(context.Context, int) (store.ViewPosition, error) {
	return s.position, s.observeErr
}

type countedPublisher struct{ calls int }

func (p *countedPublisher) Publish(context.Context, store.Delivery) (int64, error) {
	p.calls++
	return 3, nil
}

func TestUnresolvedReceiptStopsWithoutRepublishing(t *testing.T) {
	for _, name := range []string{"different receipt", "observation unavailable"} {
		t.Run(name, func(t *testing.T) {
			state := &unresolvedState{receipt: 4}
			if name == "observation unavailable" {
				state.observeErr = errors.New("database unavailable")
			}
			publisher := &countedPublisher{}
			worked, err := RelayStep(t.Context(), state, publisher, 0)
			if worked || !errors.Is(err, store.ErrFence) {
				t.Fatalf("unresolved receipt reported success: worked=%v err=%v", worked, err)
			}
			if publisher.calls != 1 || state.mutations != 1 {
				t.Fatalf("uncertain receipt repeated writes: sends=%d receipts=%d", publisher.calls, state.mutations)
			}
		})
	}
}

func TestUnresolvedReceiptPreservesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	state := &unresolvedState{cancel: cancel}
	publisher := &countedPublisher{}
	worked, err := RelayStep(ctx, state, publisher, 0)
	if worked || !errors.Is(err, store.ErrFence) || !errors.Is(err, context.Canceled) {
		t.Fatalf("unresolved receipt lost cancellation or retirement: worked=%v err=%v", worked, err)
	}
	if publisher.calls != 1 || state.mutations != 1 {
		t.Fatalf("cancelled receipt repeated writes: sends=%d receipts=%d", publisher.calls, state.mutations)
	}
}

func TestUnresolvedViewDecisionRetires(t *testing.T) {
	for _, tc := range []struct {
		name     string
		position store.ViewPosition
		readErr  error
		want     error
	}{
		{"unexpected progress", store.ViewPosition{Next: 2}, nil, store.ErrFence},
		{"observation unavailable", store.ViewPosition{}, errors.New("database unavailable"), store.ErrFence},
		{"durable protocol stop", store.ViewPosition{Error: "conflicting output"}, nil, store.ErrProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := &unresolvedState{position: tc.position, observeErr: tc.readErr}
			err := ViewStep(t.Context(), state, 0, store.ViewInput{Partition: 0, Offset: 0})
			if !errors.Is(err, tc.want) || state.mutations != 1 {
				t.Fatalf("unresolved view decision did not retire: err=%v writes=%d", err, state.mutations)
			}
		})
	}
}
