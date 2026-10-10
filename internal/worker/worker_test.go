// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sorrell-consulting/amends/internal/store"
)

func TestRetryBudgetAndCancellation(t *testing.T) {
	r := &Lifecycle{wait: wait}
	attempts := 0
	err := r.retry(t.Context(), func() error { attempts++; return errors.New("unavailable") })
	if err == nil || attempts != 3 {
		t.Fatalf("attempts=%d error=%v", attempts, err)
	}
	attempts = 0
	err = r.retry(t.Context(), func() error { attempts++; return store.ErrFence })
	if !errors.Is(err, store.ErrFence) || attempts != 1 {
		t.Fatal("fenced operation retried")
	}
	ctx, cancel := context.WithCancel(t.Context())
	attempts = 0
	err = r.retry(ctx, func() error { attempts++; cancel(); return errors.New("unavailable") })
	if !errors.Is(err, context.Canceled) || attempts != 1 {
		t.Fatal("cancelled work retried")
	}
}

func TestRetryCancellationAtWakeup(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r := &Lifecycle{wait: func(context.Context, time.Duration) error { cancel(); return nil }}
	attempts := 0
	err := r.retry(ctx, func() error { attempts++; return errors.New("unavailable") })
	if !errors.Is(err, context.Canceled) || attempts != 1 {
		t.Fatal("cancellation at retry wakeup permitted another I/O call", attempts, err)
	}
}
