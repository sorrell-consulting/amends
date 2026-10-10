// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Package worker connects broker assignment to externally fenced source progress.
package worker

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/twmb/franz-go/pkg/kgo"
)

// State supplies the durable ownership and processing operations used by
// [Lifecycle]. Implementations must commit each decision under its epoch and
// frontier guards and distinguish unknown outcomes with [store.ErrCommitUnknown].
type State interface {
	// Claim installs a higher epoch and returns its committed token. The caller
	// must hold a current assignment; an unknown result does not prove ownership.
	Claim(ctx context.Context, partition int) (store.Token, error)
	// Observe reads the installed token and progress together without claiming
	// ownership. It must reveal the durable outcome of earlier processing calls.
	Observe(ctx context.Context, partition int) (store.Token, store.Partition, error)
	// Process commits one record's state or quarantine, output intent, and source
	// progress atomically. expected is the saved exclusive source frontier.
	// Obsolete tokens/frontiers return store.ErrFence without committing a decision.
	Process(ctx context.Context, token store.Token, expected int64, record ledger.Record) error
}

// Options configures a source worker for one immutable topic/configuration pair.
// State must be initialized before Run. A nil Logger uses slog.Default.
type Options struct {
	Brokers []string
	Topic   string
	Config  ledger.Config
	State   State
	Logger  *slog.Logger
}

// Group is stable for a namespace, including when its config is accidentally
// changed. PostgreSQL rejects that incompatible configuration independently.
func Group(namespace string) string {
	return fmt.Sprintf("amends-source-%x", sha256.Sum256([]byte(namespace)))
}

type runner struct {
	options Options
	*Lifecycle
	admin *kgo.Client
}

// Run joins a classic eager group and returns a visible error when an operation
// exhausts its budget. Restart resumes from PostgreSQL; group offsets are never
// written. One record is in flight per runner (and thus per owned partition).
func Run(ctx context.Context, options Options) error {
	if err := options.Config.Validate(); err != nil {
		return err
	}
	if options.State == nil || options.Topic == "" {
		return fmt.Errorf("worker requires source topic and durable state")
	}
	options.Config = options.Config.Clone()
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	r := &runner{options: options}
	var err error
	r.admin, err = broker.Client(options.Brokers)
	if err != nil {
		return err
	}
	defer r.admin.Close()
	r.Lifecycle, err = NewLifecycle(ctx, LifecycleOptions{
		Config: options.Config, State: options.State, Logger: options.Logger,
		Validate: func(ctx context.Context) ([]int64, error) {
			return broker.Validate(ctx, r.admin, options.Topic, options.Config)
		},
	})
	if err != nil {
		return err
	}
	defer r.Stop(context.Canceled)
	check, cancel := context.WithTimeout(r.ctx, 20*time.Second)
	_, err = broker.Validate(check, r.admin, options.Topic, options.Config)
	cancel()
	if err != nil {
		return fmt.Errorf("source validation: %w", err)
	}
	cl, err := broker.Client(options.Brokers,
		kgo.ConsumerGroup(Group(options.Config.Namespace)), kgo.ConsumeTopics(options.Topic),
		kgo.Balancers(kgo.RangeBalancer()), kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.BlockRebalanceOnPoll(), kgo.RebalanceTimeout(time.Minute),
		kgo.SessionTimeout(10*time.Second), kgo.HeartbeatInterval(time.Second),
		kgo.OnPartitionsAssigned(r.assign), kgo.OnPartitionsRevoked(r.revoke), kgo.OnPartitionsLost(r.revoke),
		kgo.AdjustFetchOffsetsFn(r.adjust))
	if err != nil {
		return err
	}
	defer func() {
		// Never close from a group callback: franz-go waits for those callbacks.
		r.cancel(context.Canceled)
		r.Revoke()
		cl.CloseAllowingRebalance()
	}()
	for r.ctx.Err() == nil {
		if err = r.poll(cl); err != nil {
			r.cancel(err)
			break
		}
	}
	return context.Cause(r.ctx)
}

func (r *runner) assign(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	var partitions []int
	for topic, ps := range assigned {
		if topic != r.options.Topic {
			r.Stop(fmt.Errorf("unexpected assigned topic %s", topic))
			return
		}
		for _, p := range ps {
			partitions = append(partitions, int(p))
		}
	}
	ctx, cancel := context.WithTimeout(r.ctx, 20*time.Second)
	defer cancel()
	_ = r.Assign(ctx, partitions) // lifecycle failures retire the runner
}

func (r *runner) revoke(_ context.Context, _ *kgo.Client, _ map[string][]int32) {
	r.Revoke()
}

func (r *runner) adjust(_ context.Context, offsets map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	var partitions []int
	for topic, ps := range offsets {
		if topic != r.options.Topic {
			r.Stop(store.ErrFence)
			return offsets, store.ErrFence
		}
		for p := range ps {
			partitions = append(partitions, int(p))
		}
	}
	ctx, cancel := context.WithTimeout(r.ctx, 20*time.Second)
	defer cancel()
	positions, err := r.Seek(ctx, partitions)
	if err != nil {
		return offsets, err
	}
	for p, next := range positions {
		offsets[r.options.Topic][int32(p)] = kgo.NewOffset().At(next).WithEpoch(-1)
	}
	return offsets, nil
}

func (r *runner) poll(cl *kgo.Client) error {
	ctx, cancel := context.WithTimeout(r.ctx, time.Second)
	fetches := cl.PollRecords(ctx, 1)
	cancel()
	defer cl.AllowRebalance() // includes errors, empty polls, and cancelled work
	if r.ctx.Err() != nil {
		return context.Cause(r.ctx)
	}
	for _, e := range fetches.Errors() {
		if !errors.Is(e.Err, context.DeadlineExceeded) {
			return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
		}
	}
	check, done := context.WithTimeout(r.ctx, 20*time.Second)
	defer done()
	if _, err := broker.Validate(check, r.admin, r.options.Topic, r.options.Config); err != nil {
		return fmt.Errorf("source validation: %w", err)
	}
	for _, fetch := range fetches {
		for _, topic := range fetch.Topics {
			// The metadata check runs AFTER the poll, including for buffered
			// records. Fetch UUIDs, when supported by the negotiated protocol,
			// supply an additional binding to the checked incarnation.
			for _, partition := range topic.Partitions {
				for _, record := range partition.Records {
					if topic.Topic != r.options.Topic || (topic.TopicID != [16]byte{} && broker.Incarnation(topic.TopicID) != r.options.Config.Source) {
						return fmt.Errorf("fetched source incarnation differs from namespace")
					}
					if err := r.Process(check, broker.DecodeRecord(r.options.Config.Source, record)); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}
