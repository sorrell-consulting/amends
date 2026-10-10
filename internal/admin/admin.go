// Package admin checks retained replay ranges and rebuilds isolated namespaces.
// Execution requires caller-managed quiescence; these operations do not stop
// independently launched workers or loaders. Dry runs still inspect the broker
// and database and take a producer guard, but do not change durable business state.
package admin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/delivery"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/sorrell-consulting/amends/internal/verify"
	"github.com/sorrell-consulting/amends/internal/worker"
	"github.com/twmb/franz-go/pkg/kerr"
)

// Options supplies the source manifest and caller-owned database pool for an
// administrative operation. The pool must remain open until the call returns.
type Options struct {
	Brokers  []string
	Manifest broker.Manifest
	Pool     *pgxpool.Pool
	Execute  bool // False checks prerequisites without executing replay or rebuild.
	// Quiesce declares that ordinary source workers and loaders have stopped
	// and joined. It is required for execution and does not stop them itself.
	Quiesce bool
}

// ReplayReport records administrative completion, not independent verification.
// From and Until bound a half-open source-offset range; Frontier is the unchanged
// durable next offset. Epoch is the observed or successfully installed generation.
// Status is DRY_RUN, REPLAYED, INCOMPLETE, or ERROR. Records counts visible inputs.
type ReplayReport struct {
	Status    string `json:"status"`
	Namespace string `json:"namespace"`
	Partition int    `json:"partition"`
	From      int64  `json:"from_offset"`
	Until     int64  `json:"until_offset"`
	Frontier  int64  `json:"authoritative_frontier"`
	Epoch     int64  `json:"epoch"`
	Records   int    `json:"records"`
	Reason    string `json:"reason,omitempty"`
}

// Replay checks [from, until) against intact, already-consumed retained history.
// A dry run preserves durable state. Execution requires Options.Quiesce and
// commits only a higher ownership epoch; facts, versions, and progress stay intact.
// Restart ordinary workers afterward so they claim fresh ownership.
//
// An error matching [store.ErrCommitUnknown] requires inspection of the durable
// epoch before retrying. Neither REPLAYED nor a nil error establishes verifier PASS.
func Replay(ctx context.Context, options Options, partition int, from, until int64) (report ReplayReport, err error) {
	report = ReplayReport{Status: "ERROR", Namespace: options.Manifest.Config.Namespace, Partition: partition, From: from, Until: until}
	defer func() {
		if err != nil {
			report.Reason = err.Error()
			if errors.Is(err, broker.ErrBoundary) || errors.Is(err, kerr.UnknownTopicOrPartition) || errors.Is(err, kerr.OffsetOutOfRange) {
				report.Status = "INCOMPLETE"
			}
		}
	}()
	if options.Execute && !options.Quiesce {
		return report, fmt.Errorf("execution requires stopped and joined source workers and loaders (--quiesce)")
	}
	state, err := postgres.New(options.Pool, options.Manifest.Config)
	if err != nil {
		return report, err
	}
	guard, err := state.AcquireService(ctx, "producer")
	if err != nil {
		return report, err
	}
	defer guard.Close()
	client, err := broker.Client(options.Brokers)
	if err != nil {
		return report, err
	}
	defer client.Close()
	ends, err := broker.Validate(ctx, client, options.Manifest.Topic, options.Manifest.Config)
	if err != nil {
		return report, err
	}
	if partition < 0 || partition >= len(ends) || from < 0 || until <= from {
		return report, fmt.Errorf("invalid replay partition or range")
	}
	token, progress, err := state.Observe(ctx, partition)
	if err != nil {
		return report, err
	}
	if until > progress.Next || progress.Next > ends[partition] {
		return report, fmt.Errorf("replay range exceeds consumed retained history")
	}
	history, err := broker.Scan(ctx, options.Brokers, options.Manifest, ends)
	if err != nil {
		return report, err
	}
	var records []ledger.Record
	for _, record := range history {
		if record.Partition == partition && record.Offset >= from && record.Offset < until {
			records = append(records, record)
		}
	}
	current, err := broker.Validate(ctx, client, options.Manifest.Topic, options.Manifest.Config)
	if err != nil {
		return report, err
	}
	if !slices.Equal(current, ends) {
		return report, broker.ErrBoundary
	}
	if err = guard.Check(ctx); err != nil {
		return report, err
	}
	report.Frontier, report.Records = progress.Next, len(records)
	report.Epoch, err = state.Replay(ctx, token, progress.Next, from, until, records, options.Execute)
	if err != nil {
		return report, err
	}
	report.Status = "DRY_RUN"
	if options.Execute {
		report.Status = "REPLAYED"
	}
	return report, nil
}

// RebuildReport describes a dry run, partial destination creation, or completed
// reconstruction. A completed execution retains the verifier's status and report,
// including negative and qualified outcomes. Manifest records known resources
// even on failure; they are not automatically removed or adopted on retry.
type RebuildReport struct {
	Status            string          `json:"status"`
	OriginalNamespace string          `json:"original_namespace"`
	Manifest          broker.Manifest `json:"manifest"`
	SourceEnd         []int64         `json:"source_end"`
	Records           int             `json:"records"`
	OutputTopic       string          `json:"output_topic"`
	Verification      *verify.Report  `json:"verification,omitempty"`
	Reason            string          `json:"reason,omitempty"`
}

// Rebuild reconstructs a fresh namespace and output topic from complete retained
// source history. It checks history and destination availability before creating
// anything. The original namespace is not changed; no live consumer cutover occurs.
//
// Execution requires Options.Quiesce and a non-nil saveManifest. The callback
// must persist each supplied manifest: first after namespace creation, then after
// topic creation adds its output identity. Callback or later failures can leave
// partial resources requiring inspection. A dry run never calls saveManifest.
//
// Completed execution drains delivery and independently verifies the new view.
// A nil error can accompany BLOCKED, INCOMPLETE, or qualified verification; callers
// must inspect report.Status and report.Verification. ctx bounds the whole run.
func Rebuild(ctx context.Context, options Options, namespace, outputTopic string, saveManifest func(broker.Manifest) error) (report RebuildReport, err error) {
	report = RebuildReport{Status: "ERROR", OriginalNamespace: options.Manifest.Config.Namespace, OutputTopic: outputTopic}
	defer func() {
		if err != nil {
			report.Reason = err.Error()
			if errors.Is(err, broker.ErrBoundary) || errors.Is(err, kerr.UnknownTopicOrPartition) || errors.Is(err, kerr.OffsetOutOfRange) {
				report.Status = "INCOMPLETE"
			}
		}
	}()
	if namespace == "" || namespace == report.OriginalNamespace || outputTopic == "" || outputTopic == options.Manifest.Topic || (options.Manifest.Output != nil && outputTopic == options.Manifest.Output.Topic) {
		return report, fmt.Errorf("rebuild requires a fresh namespace and distinct new output topic")
	}
	if options.Execute && (!options.Quiesce || saveManifest == nil) {
		return report, fmt.Errorf("execution requires --quiesce and a manifest destination")
	}
	original, err := postgres.New(options.Pool, options.Manifest.Config)
	if err != nil {
		return report, err
	}
	originalExists, err := original.Exists(ctx)
	if err != nil {
		return report, err
	}
	if originalExists {
		if err = original.CheckConfig(ctx); err != nil {
			return report, err
		}
	}
	guard, err := original.AcquireService(ctx, "producer")
	if err != nil {
		return report, err
	}
	defer guard.Close()
	client, err := broker.Client(options.Brokers)
	if err != nil {
		return report, err
	}
	defer client.Close()
	report.SourceEnd, err = broker.Validate(ctx, client, options.Manifest.Topic, options.Manifest.Config)
	if err != nil {
		return report, err
	}
	history, err := broker.Scan(ctx, options.Brokers, options.Manifest, report.SourceEnd)
	if err != nil {
		return report, err
	}
	report.Records = len(history)
	config := options.Manifest.Config.Clone()
	config.Namespace = namespace
	state, err := postgres.New(options.Pool, config)
	if err != nil {
		return report, err
	}
	exists, err := state.Exists(ctx)
	if err != nil {
		return report, err
	}
	if exists {
		return report, fmt.Errorf("rebuild requires an unused namespace: %w", store.ErrFence)
	}
	if _, err = broker.Inspect(ctx, client, outputTopic); !errors.Is(err, kerr.UnknownTopicOrPartition) {
		if err == nil {
			err = fmt.Errorf("rebuild output topic already exists")
		}
		return report, err
	}
	report.Manifest = broker.Manifest{Topic: options.Manifest.Topic, Config: config}
	stable := func() error {
		if err := guard.Check(ctx); err != nil {
			return err
		}
		ends, err := broker.Validate(ctx, client, options.Manifest.Topic, options.Manifest.Config)
		if err != nil {
			return err
		}
		if !slices.Equal(ends, report.SourceEnd) {
			return broker.ErrBoundary
		}
		return nil
	}
	if err = stable(); err != nil {
		return report, err
	}
	if !options.Execute {
		report.Status = "DRY_RUN"
		return report, nil
	}
	if err = postgres.Migrate(ctx, options.Pool); err != nil {
		return report, err
	}
	if err = state.CreateFresh(ctx); err != nil {
		return report, err
	}
	if err = saveManifest(report.Manifest); err != nil {
		return report, err
	}
	created, err := broker.CreateSource(ctx, client, outputTopic, config.Partitions)
	if err != nil {
		return report, fmt.Errorf("new output creation may require inspection: %w", err)
	}
	report.Manifest.Output = &store.OutputBinding{Topic: created.Topic, Incarnation: created.ID, Partitions: created.Partitions}
	if err = saveManifest(report.Manifest); err != nil {
		return report, err
	}
	ends, err := broker.ValidateOutput(ctx, client, *report.Manifest.Output, config)
	if err != nil {
		return report, err
	}
	for _, end := range ends {
		if end != 0 {
			return report, fmt.Errorf("new output is not empty")
		}
	}
	if err = state.BindOutput(ctx, *report.Manifest.Output); err != nil {
		return report, err
	}
	lifecycle, err := worker.NewLifecycle(ctx, worker.LifecycleOptions{
		Config: config, State: state,
		Validate: func(context.Context) ([]int64, error) {
			if err := stable(); err != nil {
				return nil, err
			}
			return slices.Clone(report.SourceEnd), nil
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		return report, err
	}
	defer lifecycle.Stop(context.Canceled)
	defer lifecycle.Revoke()
	partitions := make([]int, config.Partitions)
	for partition := range partitions {
		partitions[partition] = partition
	}
	if err = lifecycle.Assign(lifecycle.Context(), partitions); err != nil {
		return report, err
	}
	if _, err = lifecycle.Seek(lifecycle.Context(), partitions); err != nil {
		return report, err
	}
	for _, record := range history {
		if err = stable(); err != nil {
			return report, err
		}
		if err = lifecycle.Process(lifecycle.Context(), record); err != nil {
			return report, err
		}
	}
	deliveryOptions := delivery.Options{Brokers: options.Brokers, Manifest: report.Manifest, Store: state, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err = delivery.DrainRelay(ctx, deliveryOptions); err != nil {
		return report, err
	}
	if err = materialize(ctx, deliveryOptions); err != nil {
		return report, err
	}
	if err = stable(); err != nil {
		return report, err
	}
	guard.Close()
	budget := 30 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		budget = time.Until(deadline)
	}
	verification := verify.Run(ctx, verify.Options{Brokers: options.Brokers, Manifest: report.Manifest, Store: state, Timeout: budget, Quiesce: true})
	if !slices.Equal(verification.SourceEnd, report.SourceEnd) {
		verification.Status = "INCOMPLETE"
		verification.Reason = "source changed between rebuild and verification"
		verification.Checks.BoundariesStable = false
	}
	report.Verification = &verification
	report.Status = verification.Status
	return report, nil
}

func materialize(ctx context.Context, options delivery.Options) (err error) {
	viewContext, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- delivery.RunView(viewContext, options) }()
	joined := false
	defer func() {
		cancel()
		if !joined {
			viewErr := <-done
			if viewErr != nil && !errors.Is(viewErr, context.Canceled) {
				err = errors.Join(err, viewErr)
			}
		}
	}()
	client, err := broker.Client(options.Brokers)
	if err != nil {
		return err
	}
	defer client.Close()
	ends, err := broker.ValidateOutput(ctx, client, *options.Manifest.Output, options.Manifest.Config)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		progress, err := options.Store.Progress(ctx)
		if err != nil {
			return err
		}
		ready := true
		for _, partition := range progress.Partitions {
			if partition.ProtocolError != "" {
				return store.ErrProtocol
			}
			if partition.ViewNext > ends[partition.Partition] {
				return store.ErrFence
			}
			ready = ready && partition.ViewNext == ends[partition.Partition]
		}
		if ready {
			return nil
		}
		select {
		case err := <-done:
			joined = true
			if err == nil {
				return fmt.Errorf("view stopped before reaching output boundary")
			}
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
