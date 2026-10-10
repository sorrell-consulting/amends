// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package delivery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ViewState supplies atomic output application and its durable recovery position.
type ViewState interface {
	// ApplyViewInput checks expected, the saved next output offset, and commits
	// the version/tombstone decision with progress. Protocol errors persist a
	// diagnostic without advancing rows or progress and return store.ErrProtocol.
	// Unknown commits return store.ErrCommitUnknown; stale guards return store.ErrFence.
	ApplyViewInput(ctx context.Context, expected int64, input store.ViewInput) error
	// ViewPosition reads progress and protocol diagnostics together, including
	// the durable result of any earlier application whose reply was lost.
	ViewPosition(ctx context.Context, partition int) (store.ViewPosition, error)
}

// ViewStep applies one output with a 20-second operation budget, resolving
// unknown commits through ViewPosition. expected is the saved exclusive output
// frontier. Nil means committed application or a durably observed completion;
// a returned error may still have committed effects or protocol diagnostics.
func ViewStep(ctx context.Context, state ViewState, expected int64, input store.ViewInput) error {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	return (Stepper{}).ViewStep(ctx, state, expected, input)
}

// ViewStep has the outcome contract of the top-level [ViewStep] function, with ctx
// supplying the operation budget and Wait supplying retry timing. Serialize calls
// per partition and stop processing that partition if the outcome cannot be resolved.
func (s Stepper) ViewStep(ctx context.Context, state ViewState, expected int64, input store.ViewInput) error {
	return s.retry(ctx, func() error {
		err := state.ApplyViewInput(ctx, expected, input)
		if !errors.Is(err, store.ErrCommitUnknown) {
			return err
		}
		var position store.ViewPosition
		if observeErr := s.retry(ctx, func() error { var err error; position, err = state.ViewPosition(ctx, input.Partition); return err }); observeErr != nil {
			return fmt.Errorf("unresolved view commit: %w: %v", store.ErrFence, observeErr)
		}
		if position.Error != "" {
			return fmt.Errorf("%w: %s", store.ErrProtocol, position.Error)
		}
		if position.Next == input.Offset+1 {
			return nil
		}
		if position.Next != expected {
			return store.ErrFence
		}
		return err
	})
}

// RunView consumes from durable output positions under the view session guard.
// A protocol violation parks its partition while other partitions continue;
// all partitions parked, infrastructure failure, or cancellation ends the run.
// It closes its broker clients and guard before returning.
func RunView(ctx context.Context, o Options) error {
	if err := o.check(ctx); err != nil {
		return err
	}
	service, err := o.Store.AcquireService(ctx, "view")
	if err != nil {
		return err
	}
	defer service.Close()
	admin, err := broker.Client(o.Brokers)
	if err != nil {
		return err
	}
	defer admin.Close()
	check, cancel := context.WithTimeout(ctx, 20*time.Second)
	ends, err := broker.ValidateOutput(check, admin, *o.Manifest.Output, o.Manifest.Config)
	cancel()
	if err != nil {
		return err
	}
	positions := make(map[int]store.ViewPosition)
	offsets := map[int32]kgo.Offset{}
	for p := 0; p < o.Manifest.Config.Partitions; p++ {
		position, err := o.Store.ViewPosition(ctx, p)
		if err != nil {
			return err
		}
		if position.Next < 0 || position.Next > ends[p] {
			return fmt.Errorf("output frontier beyond broker boundary: %w", store.ErrFence)
		}
		positions[p] = position
		if position.Error != "" {
			o.Logger.Error("output partition stopped", "partition", p, "reason", position.Error)
			continue
		}
		offsets[int32(p)] = kgo.NewOffset().At(position.Next).WithEpoch(-1)
	}
	if len(offsets) == 0 {
		return store.ErrProtocol
	}
	active := len(offsets)
	topic := o.Manifest.Output.Topic
	cl, err := broker.Client(o.Brokers, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: offsets}), kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	if err != nil {
		return err
	}
	defer cl.Close()
	o.Logger.Info("view started", "namespace", o.Manifest.Config.Namespace, "topic", topic)
	for ctx.Err() == nil {
		poll, done := context.WithTimeout(ctx, time.Second)
		fetches := cl.PollRecords(poll, 1)
		done()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		for _, e := range fetches.Errors() {
			if !errors.Is(e.Err, context.DeadlineExceeded) {
				return e.Err
			}
		}
		check, done := context.WithTimeout(ctx, 20*time.Second)
		_, err := broker.ValidateOutput(check, admin, *o.Manifest.Output, o.Manifest.Config)
		if err == nil {
			err = service.Check(check)
		}
		done()
		if err != nil {
			return err
		}
		for _, fetch := range fetches {
			for _, ft := range fetch.Topics {
				for _, fp := range ft.Partitions {
					for _, record := range fp.Records {
						if ft.Topic != topic || (ft.TopicID != [16]byte{} && broker.Incarnation(ft.TopicID) != o.Manifest.Output.Incarnation) {
							return fmt.Errorf("fetched output incarnation changed")
						}
						p := int(record.Partition)
						position, ok := positions[p]
						if !ok {
							return store.ErrFence
						}
						if position.Error != "" {
							continue
						}
						envelope, decodeErr := broker.DecodeOutput(record.Key, record.Value)
						input := store.ViewInput{Partition: p, Offset: record.Offset, Envelope: envelope, Key: record.Key, Value: record.Value}
						if decodeErr != nil {
							input.WireError = decodeErr.Error()
						}
						err := ViewStep(ctx, o.Store, position.Next, input)
						if errors.Is(err, store.ErrProtocol) {
							position.Error = err.Error()
							positions[p] = position
							cl.PauseFetchPartitions(map[string][]int32{topic: {int32(p)}})
							active--
							o.Logger.Error("output partition stopped", "partition", p, "offset", record.Offset, "reason", err)
							if active == 0 {
								return err
							}
							continue
						}
						if err != nil {
							return err
						}
						position.Next = record.Offset + 1
						positions[p] = position
					}
				}
			}
		}
	}
	return ctx.Err()
}
