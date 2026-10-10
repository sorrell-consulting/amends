// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package verify

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"slices"
	"time"

	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/oracle"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/sorrell-consulting/amends/internal/view"
	"github.com/twmb/franz-go/pkg/kerr"
)

// Options identifies the initialized pipeline and bounds a verification run.
// Manifest must include the installed output binding; Timeout must be positive.
type Options struct {
	Brokers  []string
	Manifest broker.Manifest
	Store    *postgres.Store
	Timeout  time.Duration
	// Quiesce declares the stopped-writer procedure described by Run. It does
	// not stop or join an independently operated relay.
	Quiesce bool
}

var errIncomplete = errors.New("verification boundary incomplete")

// Run requires cooperative, owned source loaders and a relay stopped and joined
// by the operator. It awaits an active finite loader and holds both publication
// guards until the final recheck. It never steals a guard, kills a process, or
// assumes a lost session proves an orphan broker request has drained.
// The result carries failures in Status and Reason rather than a separate error.
// Inspect Status or use [Report.Accepts]; a returned report is not itself success.
func Run(parent context.Context, o Options) (r Report) {
	started := time.Now()
	defer func() { r.ElapsedMillis = time.Since(started).Milliseconds() }()
	c := o.Manifest.Config.Clone()
	r = Report{Status: "ERROR", Scope: "stopped-input-durable-pipeline", Namespace: c.Namespace, ConfigDigest: c.Digest(), SchemaVersion: c.SchemaVersion, Implementation: Implementation, GoVersion: runtime.Version(), LedgerOrigin: c.Origin, SourceTopic: o.Manifest.Topic, SourceIncarnation: c.Source, AuthorityBasis: "worker-observation"}
	ctx, cancel := context.WithTimeout(parent, o.Timeout)
	defer cancel()
	var waitingFor string
	fail := func(err error) Report {
		err = boundaryWaitError(ctx, waitingFor, err)
		r.Status = "ERROR"
		if errors.Is(err, errIncomplete) || errors.Is(err, broker.ErrBoundary) || errors.Is(err, store.ErrFence) || errors.Is(err, kerr.UnknownTopicOrPartition) || errors.Is(err, kerr.OffsetOutOfRange) {
			r.Status = "INCOMPLETE"
		}
		r.Reason = err.Error()
		return r
	}
	if !o.Quiesce || o.Store == nil || o.Manifest.Output == nil || o.Timeout <= 0 {
		return fail(fmt.Errorf("requires quiesce, pipeline manifest, store, and positive timeout"))
	}
	if err := c.Validate(); err != nil {
		return fail(err)
	}
	output := *o.Manifest.Output
	r.OutputTopic, r.OutputIncarnation = output.Topic, output.Incarnation
	observed, err := o.Store.Snapshot(ctx)
	if err != nil {
		return fail(err)
	}
	if observed.Config.Digest() != c.Digest() {
		return fail(store.ErrFence)
	}
	lastView, err := o.Store.View(ctx)
	if err != nil {
		return fail(err)
	}
	defer func() { fillViewStatuses(&r, lastView) }()
	r.KnownConflicts = observedConflicts(observed)
	for _, key := range c.Keys {
		p := observed.Keys[key.StockKey].Projection
		r.Keys = append(r.Keys, KeyResult{Key: key.StockKey, AuthorityStatus: p.Status, Reasons: p.Reasons})
	}
	progress, err := o.Store.Progress(ctx)
	if err != nil {
		return fail(err)
	}
	r.Progress = progress
	for _, p := range r.Progress.Partitions {
		if p.ProtocolError != "" {
			return fail(fmt.Errorf("output partition %d protocol failure: %s", p.Partition, p.ProtocolError))
		}
	}
	binding, err := o.Store.OutputBinding(ctx)
	if err != nil {
		return fail(err)
	}
	if binding != output {
		return fail(store.ErrFence)
	}
	var producer *postgres.Service
	for {
		producer, err = o.Store.AcquireService(ctx, "producer")
		if err == nil {
			break
		}
		if !errors.Is(err, postgres.ErrServiceBusy) {
			return fail(err)
		}
		waitingFor = "owned source loader has not stopped"
		if err = pause(ctx); err != nil {
			return fail(fmt.Errorf("%w: owned source loader has not stopped: %v", errIncomplete, err))
		}
	}
	waitingFor = ""
	defer producer.Close()
	relay, err := o.Store.AcquireService(ctx, "relay")
	if errors.Is(err, postgres.ErrServiceBusy) {
		return fail(fmt.Errorf("%w: stop and join the relay before verification", errIncomplete))
	}
	if err != nil {
		return fail(err)
	}
	defer relay.Close()
	r.Checks.WritersStopped = true
	cl, err := broker.Client(o.Brokers)
	if err != nil {
		return fail(err)
	}
	defer cl.Close()
	r.SourceEnd, err = broker.Validate(ctx, cl, o.Manifest.Topic, c)
	if err != nil {
		return fail(err)
	}
	r.SourceStart = make([]int64, c.Partitions) // Validate proved each retained start is zero.
	if _, err = broker.ValidateOutput(ctx, cl, output, c); err != nil {
		return fail(err)
	}
	history, err := broker.Scan(ctx, o.Brokers, o.Manifest, r.SourceEnd)
	if err != nil {
		return fail(err)
	}
	r.Checks.SourceScanned = true
	r.SourceRecords = len(history)
	expected, err := oracle.Compute(c, history)
	if err != nil {
		return fail(err)
	}
	r.AuthorityBasis = "independent-source-scan"
	r.KnownConflicts = expected.Conflicts
	r.Diagnostics = diagnostics(expected, history)
	r.AcceptedRecords = len(history) - len(expected.Rejections)
	r.Keys = nil
	for _, p := range expected.Keys {
		r.Keys = append(r.Keys, KeyResult{Key: p.Key, AuthorityStatus: p.Status, Reasons: p.Reasons})
	}

	stableSource := func() error {
		if err := producer.Check(ctx); err != nil {
			return err
		}
		if err := relay.Check(ctx); err != nil {
			return err
		}
		end, err := broker.Validate(ctx, cl, o.Manifest.Topic, c)
		if err != nil {
			return err
		}
		if !slices.Equal(end, r.SourceEnd) {
			return fmt.Errorf("%w: source advanced or changed during verification", errIncomplete)
		}
		return nil
	}
	refresh := func() error {
		progress, err := o.Store.Progress(ctx)
		if err != nil {
			return err
		}
		r.Progress = progress
		for _, p := range r.Progress.Partitions {
			if p.ProtocolError != "" {
				return fmt.Errorf("output partition %d protocol failure: %s", p.Partition, p.ProtocolError)
			}
		}
		observedView, err := o.Store.View(ctx)
		if err != nil {
			return err
		}
		lastView = observedView
		return stableSource()
	}
	waitingFor, err = awaitWorkerBoundary(ctx, &r, refresh)
	if err != nil {
		return fail(err)
	}
	r.Checks.WorkersReached = true
	for _, p := range r.Progress.Partitions {
		if p.Pending != 0 {
			return fail(fmt.Errorf("%w: pending outbox on partition %d; resume and drain the relay, then stop it and retry", errIncomplete, p.Partition))
		}
	}
	r.Checks.PublicationDrained = true
	r.OutputEnd, err = broker.ValidateOutput(ctx, cl, output, c)
	if err != nil {
		return fail(err)
	}
	r.OutputStart = make([]int64, c.Partitions)
	waitingFor, err = awaitViewBoundary(ctx, &r, refresh)
	if err != nil {
		return fail(err)
	}
	r.Checks.ViewReached = true
	observed, err = readBoundarySnapshots(ctx, o.Store, &r, &lastView)
	if err != nil {
		return fail(err)
	}
	if err = stableSource(); err != nil {
		return fail(err)
	}
	end, err := broker.ValidateOutput(ctx, cl, output, c)
	if err != nil {
		return fail(err)
	}
	if !slices.Equal(end, r.OutputEnd) {
		return fail(fmt.Errorf("%w: output advanced during verification", errIncomplete))
	}
	if err = producer.Check(ctx); err != nil {
		return fail(err)
	}
	if err = relay.Check(ctx); err != nil {
		return fail(err)
	}
	r.Checks.BoundariesStable = true
	compare(&r, expected, lastView, observed.Rejections, observedConflicts(observed), history)
	return r
}

// Once a boundary is observed as pending, expiry of the overall verification
// budget describes that incomplete wait whether it interrupts pause or I/O.
// A timeout while the overall budget is still live, a lost session, or another
// infrastructure error stays ERROR; do not relabel every failure just because
// a deadline also elapsed.
func boundaryWaitError(ctx context.Context, waitingFor string, err error) error {
	contextErr := ctx.Err()
	deadline, hasDeadline := ctx.Deadline()
	// Socket deadlines can expire before the context timer runs. pgx then
	// preserves the network timeout rather than wrapping DeadlineExceeded.
	budgetExpired := errors.Is(contextErr, context.DeadlineExceeded) ||
		(contextErr == nil && hasDeadline && !time.Now().Before(deadline))
	var networkError net.Error
	timeout := errors.Is(err, context.DeadlineExceeded) ||
		(errors.As(err, &networkError) && networkError.Timeout())
	if waitingFor != "" && budgetExpired && timeout && !errors.Is(err, errIncomplete) {
		return fmt.Errorf("%w: %s: %w", errIncomplete, waitingFor, err)
	}
	return err
}

func pause(ctx context.Context) error {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func observedConflicts(s store.Snapshot) []ledger.Conflict {
	var conflicts []ledger.Conflict
	for _, state := range s.Keys {
		conflicts = append(conflicts, state.Conflicts()...)
	}
	slices.SortFunc(conflicts, func(a, b ledger.Conflict) int {
		if a.Ref() < b.Ref() {
			return -1
		}
		if a.Ref() > b.Ref() {
			return 1
		}
		return 0
	})
	return conflicts
}

// fillViewStatuses preserves the view's current trust display even in reports
// that cannot certify a complete boundary. This is only an observation.
func fillViewStatuses(r *Report, state view.State) {
	for i := range r.Keys {
		r.Keys[i].ViewStatus = state.Projection(r.Keys[i].Key).Status
	}
}
