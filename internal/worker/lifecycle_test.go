//go:build integration

// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type delayedOffsets struct {
	*postgres.Store
	reads   atomic.Int32
	entered chan struct{}
}

func (s *delayedOffsets) Observe(ctx context.Context, p int) (store.Token, store.Partition, error) {
	// The first read follows Claim in OnPartitionsAssigned. The second is
	// the real AdjustFetchOffsetsFn callback; its client context stays live.
	if s.reads.Add(1) == 2 {
		close(s.entered)
		<-ctx.Done()
		return store.Token{}, store.Partition{}, ctx.Err()
	}
	return s.Store.Observe(ctx, p)
}
func TestWorkerShutdownCancelsDelayedOffsetAdjustment(t *testing.T) {
	e := setup(t, 1)
	e.publish(t, e.records(t, 0)...)
	s := &delayedOffsets{Store: e.db, entered: make(chan struct{})}
	w := e.start(t, s)
	select {
	case <-s.entered:
	case <-time.After(15 * time.Second):
		t.Fatal("offset adjustment did not start")
	}
	w.stop(t)
	_, before, err := e.db.Observe(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if before.Epoch != 1 || before.Next != 0 {
		t.Fatalf("delayed adjustment changed processing: %+v", before)
	}
	replacement := e.start(t, e.db)
	e.await(t, 5)
	replacement.stop(t)
	_, after, err := e.db.Observe(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if after.Epoch != 2 {
		t.Fatalf("obsolete callback reclaimed ownership: %+v", after)
	}
}

type delayedProcessing struct {
	*tracked
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *delayedProcessing) Process(ctx context.Context, token store.Token, next int64, r ledger.Record) error {
	s.once.Do(func() {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
		}
	})
	return s.tracked.Process(ctx, token, next, r)
}
func TestWorkerRebalanceWaitsForPolledDecision(t *testing.T) {
	e := setup(t, 2)
	e.publish(t, e.records(t, 0)[0])
	s := &delayedProcessing{tracked: track(e.db), entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(s.release) })
	w1 := e.start(t, s)
	select {
	case <-s.entered:
	case <-time.After(15 * time.Second):
		t.Fatal("worker did not poll input")
	}
	old := s.latest(0)
	w2 := e.start(t, e.db)
	eventually(t, func() bool {
		req := kmsg.NewDescribeGroupsRequest()
		req.Groups = []string{Group(e.c.Namespace)}
		response, err := req.RequestWith(t.Context(), e.cl)
		if err != nil {
			t.Fatal(err)
		}
		return len(response.Groups) == 1 && response.Groups[0].State == "PreparingRebalance"
	})
	token, state, err := e.db.Observe(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if token != old || state.Next != 0 {
		t.Fatalf("rebalance overtook polled decision: token=%+v state=%+v", token, state)
	}
	release.Do(func() { close(s.release) })
	eventually(t, func() bool {
		token, state, err := e.db.Observe(t.Context(), 0)
		if err != nil {
			t.Fatal(err)
		}
		return token.Epoch > old.Epoch && state.Next == 1
	})
	w1.stop(t)
	w2.stop(t)
	snapshot, err := e.db.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Outbox) != 3 {
		t.Fatalf("rebalance duplicated decision: outbox %d", len(snapshot.Outbox))
	}
}
