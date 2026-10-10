// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
)

// LifecycleOptions supplies configuration, storage, and source validation.
type LifecycleOptions struct {
	Config ledger.Config
	State  State
	// Validate returns retained, incarnation-checked exclusive source ends,
	// indexed by partition. It must reject incompatible or missing history.
	Validate func(context.Context) ([]int64, error)
	// Wait supplies cancellable retry timing. Nil uses the real timer.
	Wait func(context.Context, time.Duration) error
	// Logger defaults to slog.Default when nil.
	Logger *slog.Logger
}

type owner struct {
	token store.Token
	next  int64
}

// Lifecycle is the serial assignment/seek/processing guard shared by the broker
// runner and controlled schedules. Stop cancels independently of the guard;
// Revoke waits for guarded work. It never launches a detached epoch installer.
// Operation contexts must derive from Context; production callbacks bound them
// to 20 seconds, while controlled schedules supply logical I/O and retry waits.
// Construct it with [NewLifecycle]. Methods synchronize access, but callers
// still own assignment ordering. A stopped lifecycle cannot be restarted.
type Lifecycle struct {
	config   ledger.Config
	state    State
	validate func(context.Context) ([]int64, error)
	wait     func(context.Context, time.Duration) error
	logger   *slog.Logger
	ctx      context.Context
	cancel   context.CancelCauseFunc
	mu       sync.Mutex
	owners   map[int]owner
}

// NewLifecycle validates and copies configuration and creates an unassigned
// lifecycle rooted in ctx. State and Validate are required. The caller owns
// those adapters and must Stop and wait for outstanding calls before closing them.
func NewLifecycle(ctx context.Context, o LifecycleOptions) (*Lifecycle, error) {
	if err := o.Config.Validate(); err != nil {
		return nil, err
	}
	if o.State == nil || o.Validate == nil {
		return nil, fmt.Errorf("worker lifecycle requires state and source validation")
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Wait == nil {
		o.Wait = wait
	}
	r := &Lifecycle{config: o.Config.Clone(), state: o.State, validate: o.Validate, wait: o.Wait, logger: o.Logger, owners: map[int]owner{}}
	r.ctx, r.cancel = context.WithCancelCause(ctx)
	return r, nil
}

// Context returns the cancellation root for all lifecycle operation contexts.
func (r *Lifecycle) Context() context.Context { return r.ctx }

// Err returns the stopping cause, or nil while the lifecycle is active.
func (r *Lifecycle) Err() error { return context.Cause(r.ctx) }

// Stop cancels the lifecycle without taking its mutex or waiting for work to
// finish. A nil cause records context.Canceled. Callers must join outstanding
// operations; Revoke waits for guarded work before clearing the assignment.
func (r *Lifecycle) Stop(cause error) { r.cancel(cause) }

// Assign validates and claims synchronously under the lifecycle guard. An
// unknown claim is observed but never adopted as proof of this caller's claim.
// Any error stops the lifecycle; partitions claimed before that error may already
// have newer durable epochs. ctx must derive from [Lifecycle.Context].
func (r *Lifecycle) Assign(ctx context.Context, partitions []int) (err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer func() {
		if err != nil {
			r.Stop(err)
		}
	}()
	if r.ctx.Err() != nil {
		return r.Err()
	}
	ends, err := r.validate(ctx)
	if err != nil {
		return fmt.Errorf("assignment source: %w", err)
	}
	for _, p := range partitions {
		if p < 0 || p >= r.config.Partitions || p >= len(ends) {
			return store.ErrFence
		}
		var token store.Token
		err = r.retry(ctx, func() error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			var e error
			token, e = r.state.Claim(ctx, p)
			if errors.Is(e, store.ErrCommitUnknown) {
				if observeErr := r.retry(ctx, func() error { _, _, err := r.state.Observe(ctx, p); return err }); observeErr != nil {
					return fmt.Errorf("unresolved claim: %w: %v", store.ErrFence, observeErr)
				}
			}
			return e
		})
		if err != nil {
			return fmt.Errorf("claim partition %d: %w", p, err)
		}
		got, state, err := r.state.Observe(ctx, p)
		if err != nil {
			return err
		}
		if token.Namespace != r.config.Namespace || token.Digest != r.config.Digest() || token.Partition != p || token.Epoch < 1 || got != token || state.Next < 0 || state.Next > ends[p] {
			return fmt.Errorf("claim frontier/epoch incompatible: %w", store.ErrFence)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r.owners[p] = owner{token: token, next: state.Next}
		r.logger.Info("partition assigned", "partition", p, "epoch", token.Epoch, "next_offset", state.Next)
	}
	return nil
}

// Revoke invalidates the complete eager assignment after in-flight guarded work.
func (r *Lifecycle) Revoke() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for p, owner := range r.owners {
		r.logger.Info("partition released", "partition", p, "epoch", owner.token.Epoch)
	}
	clear(r.owners)
}

// Seek rereads the durable frontier under the saved ownership token. It cannot
// claim an epoch or adopt another owner's token after a delayed callback.
// It returns exclusive source positions by partition. Any error stops the
// lifecycle. ctx must derive from [Lifecycle.Context].
func (r *Lifecycle) Seek(ctx context.Context, partitions []int) (positions map[int]int64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer func() {
		if err != nil {
			r.Stop(fmt.Errorf("external seek: %w", err))
		}
	}()
	if r.ctx.Err() != nil {
		return nil, r.Err()
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	ends, err := r.validate(ctx)
	if err != nil {
		return nil, err
	}
	positions = map[int]int64{}
	for _, p := range partitions {
		o, ok := r.owners[p]
		if !ok || p < 0 || p >= len(ends) {
			return nil, store.ErrFence
		}
		token, state, err := r.state.Observe(ctx, p)
		if err != nil {
			return nil, err
		}
		if token != o.token || state.Next < 0 || state.Next > ends[p] {
			return nil, fmt.Errorf("external frontier/epoch incompatible: %w", store.ErrFence)
		}
		o.next = state.Next
		r.owners[p] = o
		positions[p] = state.Next
	}
	return positions, ctx.Err()
}

func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Process commits one record from an assigned partition, retaining its saved
// epoch and frontier through unknown-outcome recovery. Nil means the decision
// is durably resolved, including quarantine decisions. Any error stops the
// lifecycle and may leave a commit unresolved for the next owner to observe.
// ctx must derive from [Lifecycle.Context].
func (r *Lifecycle) Process(ctx context.Context, record ledger.Record) (err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer func() {
		if err != nil {
			r.Stop(err)
		}
	}()
	o, ok := r.owners[record.Partition]
	if !ok || r.ctx.Err() != nil {
		return store.ErrFence
	}
	if record.Offset < o.next {
		return fmt.Errorf("fetched record precedes durable frontier: %w", store.ErrFence)
	}
	err = r.retry(ctx, func() error {
		started := time.Now()
		err := r.state.Process(ctx, o.token, o.next, record)
		r.logger.Info("processing attempt", "event", ProcessingAttemptEvent, "namespace", r.config.Namespace, "partition", record.Partition,
			"offset", record.Offset, "epoch", o.token.Epoch, "outcome", processingOutcome(err), "storage_call_elapsed_ns", time.Since(started).Nanoseconds())
		if !errors.Is(err, store.ErrCommitUnknown) {
			return err
		}
		var token store.Token
		var state store.Partition
		if observeErr := r.retry(ctx, func() error { var e error; token, state, e = r.state.Observe(ctx, record.Partition); return e }); observeErr != nil {
			// Do not retry processing without resolving this unknown outcome.
			return fmt.Errorf("unresolved commit: %w: %v", store.ErrFence, observeErr)
		}
		if token != o.token {
			return store.ErrFence
		}
		if state.Next == record.Offset+1 {
			return nil
		}
		if state.Next != o.next {
			return store.ErrFence
		}
		return err // observed rollback; same record and expected frontier may retry
	})
	if err != nil {
		r.logger.Error("processing stopped", "event", ProcessingStoppedEvent, "namespace", r.config.Namespace,
			"partition", record.Partition, "offset", record.Offset, "epoch", o.token.Epoch,
			"outcome", processingOutcome(err), "error", err)
		return fmt.Errorf("partition %d offset %d stopped: %w", record.Partition, record.Offset, err)
	}
	o.next = record.Offset + 1
	r.owners[record.Partition] = o
	return nil
}

// Each operation gets at most three attempts and a shared deadline. A fence is
// terminal. Exhaustion exits the worker visibly; restart is explicit resumption.
func (r *Lifecycle) retry(ctx context.Context, operation func() error) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt > 0 {
			if err := r.wait(ctx, time.Duration(attempt)*100*time.Millisecond); err != nil {
				return err
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = operation()
		if err == nil || errors.Is(err, store.ErrFence) {
			return err
		}
	}
	return fmt.Errorf("three attempts exhausted: %w", err)
}
