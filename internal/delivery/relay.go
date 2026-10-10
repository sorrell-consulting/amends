// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package delivery

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/twmb/franz-go/pkg/kgo"
)

// RelayState supplies committed output intent and durable publication receipts.
// Callers serialize relay attempts per partition; Pending does not reserve work.
type RelayState interface {
	// Pending returns the earliest pending sequence and its exact committed
	// bytes. False with no error means no work is pending.
	Pending(ctx context.Context, partition int) (store.Delivery, bool, error)
	// Acknowledge records the broker offset for the earliest pending sequence.
	// Repeating that receipt is idempotent. Unknown commits return store.ErrCommitUnknown.
	Acknowledge(ctx context.Context, partition int, sequence, outputOffset int64) error
	// Receipt reads the stored broker offset. Nil with no error means an
	// existing entry is pending; a missing entry is an error.
	Receipt(ctx context.Context, partition int, sequence int64) (*int64, error)
}

// Publisher delivers immutable committed bytes to the output partition.
type Publisher interface {
	// Publish returns the acknowledged broker offset. An error may follow broker
	// acceptance, so retries must reuse delivery.Bytes without changing content.
	Publish(ctx context.Context, delivery store.Delivery) (int64, error)
}

// RelayStep attempts one pending envelope with a 20-second operation budget.
// It returns true only after publication and its receipt are durably resolved;
// false with no error means no pending work. On error, publication may already
// have happened. Calls for partition must be serialized.
func RelayStep(ctx context.Context, state RelayState, publisher Publisher, partition int) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	return (Stepper{}).RelayStep(ctx, state, publisher, partition)
}

// RelayStep has the result and serialization contract of the top-level [RelayStep]
// function, using ctx as the operation budget and Wait for retries. Once publication
// is acknowledged, recovery retries only its receipt. Unresolved outcomes return
// an error.
func (s Stepper) RelayStep(ctx context.Context, state RelayState, publisher Publisher, partition int) (bool, error) {
	var d store.Delivery
	var pending bool
	if err := s.retry(ctx, func() error { var err error; d, pending, err = state.Pending(ctx, partition); return err }); err != nil {
		return false, err
	}
	if !pending {
		return false, nil
	}
	var offset int64
	if err := s.retry(ctx, func() error { var err error; offset, err = publisher.Publish(ctx, d); return err }); err != nil {
		return false, fmt.Errorf("publish partition %d sequence %d (pending; acceptance may be unknown): %w", partition, d.Sequence, err)
	}
	// Once publication is acknowledged, retry only this receipt. Republishing
	// after an unresolved database acknowledgement would lose its known offset.
	err := s.retry(ctx, func() error {
		err := state.Acknowledge(ctx, partition, d.Sequence, offset)
		if !errors.Is(err, store.ErrCommitUnknown) {
			return err
		}
		var receipt *int64
		if observeErr := s.retry(ctx, func() error { var err error; receipt, err = state.Receipt(ctx, partition, d.Sequence); return err }); observeErr != nil {
			return fmt.Errorf("unresolved publication receipt: %w: %w", store.ErrFence, observeErr)
		}
		if receipt == nil {
			return err
		}
		if *receipt != offset {
			return fmt.Errorf("different publication receipt: %w", store.ErrFence)
		}
		return nil
	})
	return err == nil, err
}

type brokerPublisher struct {
	options Options
	client  *kgo.Client
	service *postgres.Service
}

func (p *brokerPublisher) Publish(ctx context.Context, d store.Delivery) (int64, error) {
	if err := p.service.Check(ctx); err != nil {
		return 0, fmt.Errorf("relay session lost: %w: %v", store.ErrFence, err)
	}
	b := *p.options.Manifest.Output
	if _, err := broker.ValidateOutput(ctx, p.client, b, p.options.Manifest.Config); err != nil {
		return 0, err
	}
	key, ok := p.options.Manifest.Config.Key(d.Envelope.ID.Key)
	if !ok || key.Partition != d.Partition || d.Envelope.ID.Namespace != p.options.Manifest.Config.Namespace {
		return 0, store.ErrFence
	}
	r := &kgo.Record{Topic: b.Topic, Partition: int32(d.Partition), Key: broker.EncodeKey(d.Envelope.ID.Key), Value: slices.Clone(d.Bytes)}
	if err := p.client.ProduceSync(ctx, r).FirstErr(); err != nil {
		return 0, err
	}
	return r.Offset, nil
}

// RunRelay holds the relay session guard and publishes until cancellation or
// failure. It closes its broker client and guard before returning. Restart reads
// committed pending intent; cancellation alone does not prove non-publication.
func RunRelay(ctx context.Context, o Options) error {
	return runRelay(ctx, o, false)
}

// DrainRelay runs the relay until every partition has no pending output. The
// caller must first stop new processing decisions. It returns after closing its
// client and guard; the view may still need to catch up before verification.
func DrainRelay(ctx context.Context, o Options) error {
	return runRelay(ctx, o, true)
}

func runRelay(ctx context.Context, o Options, drain bool) error {
	if err := o.check(ctx); err != nil {
		return err
	}
	service, err := o.Store.AcquireService(ctx, "relay")
	if err != nil {
		return err
	}
	defer service.Close()
	cl, err := broker.Client(o.Brokers, kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.AllowIdempotentProduceCancellation(), kgo.RecordDeliveryTimeout(10*time.Second))
	if err != nil {
		return err
	}
	defer cl.Close()
	publisher := &brokerPublisher{options: o, client: cl, service: service}
	o.Logger.Info("relay started", "namespace", o.Manifest.Config.Namespace, "topic", o.Manifest.Output.Topic)
	for ctx.Err() == nil {
		check, cancel := context.WithTimeout(ctx, 20*time.Second)
		_, err := broker.ValidateOutput(check, cl, *o.Manifest.Output, o.Manifest.Config)
		if err == nil {
			err = service.Check(check)
		}
		cancel()
		if err != nil {
			return err
		}
		worked := false
		for p := 0; p < o.Manifest.Config.Partitions; p++ {
			didWork, err := RelayStep(ctx, o.Store, publisher, p)
			if err != nil {
				return err
			}
			worked = worked || didWork
		}
		if !worked {
			if drain {
				return nil
			}
			if err := wait(ctx, 200*time.Millisecond); err != nil {
				return err
			}
		}
	}
	return ctx.Err()
}
