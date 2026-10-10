// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package verify

import (
	"context"
	"fmt"

	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/sorrell-consulting/amends/internal/view"
)

// Each wait returns its last observed pending boundary on error. Run uses it
// to distinguish expiration of the overall budget from other I/O failures.
// refresh retains only complete observations and rechecks the guarded source.
func awaitWorkerBoundary(ctx context.Context, r *Report, refresh func() error) (string, error) {
	var waitingFor string
	var err error
	for {
		if err = refresh(); err != nil {
			return waitingFor, err
		}
		ready := true
		for _, p := range r.Progress.Partitions {
			if p.SourceNext > r.SourceEnd[p.Partition] {
				return waitingFor, fmt.Errorf("%w: worker frontier exceeds source boundary", errIncomplete)
			}
			ready = ready && p.SourceNext == r.SourceEnd[p.Partition]
		}
		if ready {
			return "", nil
		}
		waitingFor = "workers have not reached source boundary"
		if err = pause(ctx); err != nil {
			return waitingFor, fmt.Errorf("%w: workers have not reached source boundary: %v", errIncomplete, err)
		}
	}
}

func awaitViewBoundary(ctx context.Context, r *Report, refresh func() error) (string, error) {
	var waitingFor string
	var err error
	for {
		if err = refresh(); err != nil {
			return waitingFor, err
		}
		ready := true
		for _, p := range r.Progress.Partitions {
			until := r.OutputEnd[p.Partition]
			if p.SourceNext != r.SourceEnd[p.Partition] || p.Pending != 0 || p.ViewNext > until || (p.LastReceipt != nil && *p.LastReceipt >= until) {
				return waitingFor, fmt.Errorf("%w: durable progress does not describe captured boundaries", errIncomplete)
			}
			ready = ready && p.ViewNext == until
		}
		if ready {
			return "", nil
		}
		waitingFor = "view has not reached output boundary"
		if err = pause(ctx); err != nil {
			return waitingFor, fmt.Errorf("%w: view has not reached output boundary: %v", errIncomplete, err)
		}
	}
}

// readBoundarySnapshots checks durable state against captured H/O. Updating
// lastView only after a complete read preserves the last observation on failure.
// Writer guards and the final broker rechecks remain owned by Run.
func readBoundarySnapshots(ctx context.Context, state *postgres.Store, r *Report, lastView *view.State) (store.Snapshot, error) {
	actual, err := state.View(ctx)
	if err != nil {
		return store.Snapshot{}, err
	}
	*lastView = actual
	for p, until := range r.OutputEnd {
		if actual.Next[p] != until || actual.Errors[p] != "" {
			return store.Snapshot{}, fmt.Errorf("%w: view changed at snapshot", errIncomplete)
		}
	}
	observed, err := state.Snapshot(ctx)
	if err != nil {
		return store.Snapshot{}, err
	}
	for p, until := range r.SourceEnd {
		if observed.Partitions[p].Next != until {
			return store.Snapshot{}, store.ErrFence
		}
	}
	for _, row := range observed.Outbox {
		if !row.Acknowledged {
			return store.Snapshot{}, fmt.Errorf("%w: publication changed at snapshot", errIncomplete)
		}
	}
	return observed, nil
}
