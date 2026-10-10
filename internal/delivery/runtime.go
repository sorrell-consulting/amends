// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Package delivery runs the single relay and durable output materializer.
package delivery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
)

// Options binds a relay or materializer to an initialized processing store and
// output manifest. The caller owns Store's pool. A nil Logger uses slog.Default.
type Options struct {
	Brokers  []string
	Manifest broker.Manifest
	Store    *postgres.Store
	Logger   *slog.Logger
}

func (o *Options) check(ctx context.Context) error {
	if o.Store == nil || o.Manifest.Output == nil {
		return fmt.Errorf("output manifest and PostgreSQL store required")
	}
	o.Manifest.Config = o.Manifest.Config.Clone()
	binding := *o.Manifest.Output
	o.Manifest.Output = &binding
	if err := o.Manifest.Config.Validate(); err != nil {
		return err
	}
	b, err := o.Store.OutputBinding(ctx)
	if err != nil {
		return err
	}
	if b != *o.Manifest.Output {
		return store.ErrFence
	}
	token, _, err := o.Store.Observe(ctx, 0)
	if err != nil {
		return err
	}
	if token.Namespace != o.Manifest.Config.Namespace || token.Digest != o.Manifest.Config.Digest() {
		return store.ErrFence
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return nil
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

// Stepper runs relay and view recovery decisions with caller-supplied I/O and
// operation contexts. Its zero value uses a real cancellable retry timer.
// Each retry loop permits three calls and shares the caller's deadline; unknown
// outcomes require durable observation. Errors leave restart to the caller.
type Stepper struct {
	// Wait replaces cancellable retry timing for deterministic schedules.
	Wait func(context.Context, time.Duration) error
}

func (s Stepper) retry(ctx context.Context, op func() error) error {
	waitFor := s.Wait
	if waitFor == nil {
		waitFor = wait
	}
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt > 0 {
			if err := waitFor(ctx, time.Duration(attempt)*100*time.Millisecond); err != nil {
				return err
			}
		}
		err = op()
		if err == nil || errors.Is(err, store.ErrFence) || errors.Is(err, store.ErrProtocol) {
			return err
		}
	}
	return fmt.Errorf("three attempts exhausted: %w", err)
}
