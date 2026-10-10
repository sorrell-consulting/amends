// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
)

type guardedClaim struct {
	entered, release chan struct{}
	claims           atomic.Int32
}

func (s *guardedClaim) Claim(ctx context.Context, _ int) (store.Token, error) {
	if s.claims.Add(1) != 1 {
		return store.Token{}, errors.New("obsolete claim reached I/O")
	}
	close(s.entered)
	select {
	case <-s.release:
	case <-ctx.Done():
	}
	return store.Token{}, ctx.Err()
}
func (*guardedClaim) Observe(context.Context, int) (store.Token, store.Partition, error) {
	return store.Token{}, store.Partition{}, errors.New("unexpected read after canceled claim")
}
func (*guardedClaim) Process(context.Context, store.Token, int64, ledger.Record) error {
	return errors.New("unexpected processing")
}

func TestLifecycleRetirementJoinsClaimAndDiscardsLateAssignment(t *testing.T) {
	c, err := fixtures.Config("guarded-worker")
	if err != nil {
		t.Fatal(err)
	}
	s := &guardedClaim{entered: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	life, err := NewLifecycle(ctx, LifecycleOptions{Config: c, State: s, Validate: func(context.Context) ([]int64, error) { return []int64{0}, nil }, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer life.Stop(context.Canceled)
	done := make(chan error, 1)
	go func() { done <- life.Assign(life.Context(), []int{0}) }()
	select {
	case <-s.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if life.mu.TryLock() {
		life.mu.Unlock()
		t.Fatal("claim did not hold lifecycle guard")
	}
	// Stop must cancel blocked I/O without first acquiring that guard.
	life.Stop(context.Canceled)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("canceled claim continued", err)
	}
	life.Revoke()
	if err := life.Assign(life.Context(), []int{0}); !errors.Is(err, context.Canceled) {
		t.Fatal("late assignment revived stopped worker", err)
	}
	if s.claims.Load() != 1 || len(life.owners) != 0 {
		t.Fatal("obsolete epoch installer survived retirement")
	}
}
