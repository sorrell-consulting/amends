package showcase

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	"github.com/sorrell-consulting/amends/internal/admin"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/sorrell-consulting/amends/internal/verify"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type historyEvidence struct {
	Source               broker.Source           `json:"source"`
	RequiredStart        []int64                 `json:"required_start"`
	RetainedStart        []int64                 `json:"retained_start"`
	SourceEnd            []int64                 `json:"observed_source_end"`
	OutputEnd            []int64                 `json:"observed_output_end"`
	RemovedBefore        int64                   `json:"removed_before"`
	PartitionsBefore     map[int]store.Partition `json:"partitions_before"`
	PartitionsAfter      map[int]store.Partition `json:"partitions_after"`
	StateUnchanged       bool                    `json:"durable_state_unchanged"`
	DestinationNamespace string                  `json:"destination_namespace"`
	DestinationOutput    string                  `json:"destination_output"`
	DestinationAbsent    bool                    `json:"destination_absent"`
	Replay               *admin.ReplayReport     `json:"replay,omitempty"`
	Rebuild              *admin.RebuildReport    `json:"rebuild,omitempty"`
}

func (runner *runner) historyMissing(ctx context.Context) error {
	if err := runner.cleanup(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	before, err := runner.durableState(ctx)
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(runner.report.Directory, "history-state-before.json"), before, 0600); err != nil {
		return err
	}
	snapshot, err := runner.state.Snapshot(ctx)
	if err != nil {
		return err
	}
	evidence := historyEvidence{RequiredStart: []int64{0, 0}, RemovedBefore: 2, PartitionsBefore: snapshot.Partitions,
		DestinationNamespace: runner.report.Namespace + "-history-rebuild", DestinationOutput: runner.report.Namespace + "-history-rebuild-output"}
	if err = runner.historyDestinationAbsent(ctx, evidence); err != nil {
		return err
	}
	if _, err = fmt.Fprintln(runner.options.Output, "history-ready: all owned services stopped and joined; remove offsets [0,2) only from this run's fresh source topic."); err != nil {
		return err
	}
	if err = runner.checkpoint(ctx, HistoryReady); err != nil {
		return err
	}
	if err = runner.removeOwnedHistory(ctx, &evidence); err != nil {
		return err
	}
	if err = runner.save("history.json", evidence); err != nil {
		return err
	}
	report, err := runner.verifyIncomplete(ctx, "history-missing", "10s")
	if err != nil {
		return err
	}
	if report.Status != "INCOMPLETE" || report.Checks != (verify.Checks{WritersStopped: true}) || !strings.Contains(report.Reason, "required history missing: partition 0 starts at 2") || len(report.SourceEnd) != 0 || len(report.OutputEnd) != 0 || report.SourceRecords != 0 || report.BinarySHA256 == "" {
		return fmt.Errorf("missing history was not refused before source comparison: %+v", report)
	}
	for _, key := range report.Keys {
		if key.Compared || key.Agrees {
			return fmt.Errorf("missing-history verifier certified a key")
		}
	}
	if err = runner.inspectStage(ctx, "history-missing"); err != nil {
		return err
	}
	if _, err = fmt.Fprintln(runner.options.Output, "history-missing: verifier INCOMPLETE; required [0 0], retained [2 0]; intact view and zero pending output cannot certify lost history."); err != nil {
		return err
	}
	if err = runner.checkpoint(ctx, HistoryLost); err != nil {
		return err
	}
	options := admin.Options{Brokers: runner.options.Brokers, Manifest: runner.manifest, Pool: runner.pool, Execute: true, Quiesce: true}
	if err = runner.until(ctx, "replay history refusal", func() (bool, error) {
		replay, operationErr := admin.Replay(ctx, options, 0, 2, 4)
		if errors.Is(operationErr, postgres.ErrServiceBusy) {
			return false, nil
		}
		evidence.Replay = &replay
		if !errors.Is(operationErr, broker.ErrBoundary) || replay.Status != "INCOMPLETE" || !strings.Contains(replay.Reason, "required history missing") || replay.Epoch != 0 || replay.Records != 0 {
			return false, fmt.Errorf("replay did not refuse missing history before mutation: %+v, %w", replay, operationErr)
		}
		return true, nil
	}); err != nil {
		return err
	}
	if err = runner.save("history.json", evidence); err != nil {
		return err
	}
	if err = runner.until(ctx, "rebuild history refusal", func() (bool, error) {
		saved := false
		rebuild, operationErr := admin.Rebuild(ctx, options, evidence.DestinationNamespace, evidence.DestinationOutput, func(broker.Manifest) error {
			saved = true
			return fmt.Errorf("missing-history rebuild must not create a manifest")
		})
		if errors.Is(operationErr, postgres.ErrServiceBusy) {
			return false, nil
		}
		evidence.Rebuild = &rebuild
		if !errors.Is(operationErr, broker.ErrBoundary) || rebuild.Status != "INCOMPLETE" || !strings.Contains(rebuild.Reason, "required history missing") || saved || rebuild.Verification != nil || rebuild.Manifest.Config.Namespace != "" {
			return false, fmt.Errorf("rebuild did not refuse missing history before creation: %+v, %w", rebuild, operationErr)
		}
		return true, nil
	}); err != nil {
		return err
	}
	if err = runner.historyDestinationAbsent(ctx, evidence); err != nil {
		return err
	}
	evidence.DestinationAbsent = true
	after, err := runner.durableState(ctx)
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(runner.report.Directory, "history-state-after.json"), after, 0600); err != nil {
		return err
	}
	afterSnapshot, err := runner.state.Snapshot(ctx)
	if err != nil {
		return err
	}
	evidence.PartitionsAfter = afterSnapshot.Partitions
	evidence.StateUnchanged = bytes.Equal(before, after) && reflect.DeepEqual(snapshot, afterSnapshot)
	if evidence.OutputEnd, err = broker.ValidateOutput(ctx, runner.client, *runner.manifest.Output, runner.manifest.Config); err != nil {
		return err
	}
	starts, err := broker.Offsets(ctx, runner.client, runner.manifest.Topic, 2, -2)
	if err != nil {
		return err
	}
	ends, err := broker.Offsets(ctx, runner.client, runner.manifest.Topic, 2, -1)
	if err != nil {
		return err
	}
	if !slices.Equal(starts, evidence.RetainedStart) || !slices.Equal(ends, evidence.SourceEnd) {
		return fmt.Errorf("history refusal changed the retained source boundary")
	}
	if err = runner.save("history.json", evidence); err != nil {
		return err
	}
	if !evidence.StateUnchanged || !slices.Equal(evidence.OutputEnd, []int64{10, 0}) {
		return fmt.Errorf("history refusal changed durable state, epochs, or output boundary")
	}
	_, err = fmt.Fprintln(runner.options.Output, "history-refused: replay and rebuild INCOMPLETE; original state/epochs/versions unchanged; no destination created. No automatic recovery from a surviving suffix; retain evidence and obtain complete authoritative history. Re-verification must remain INCOMPLETE (exit 1).")
	return err
}

func (runner *runner) removeOwnedHistory(ctx context.Context, evidence *historyEvidence) error {
	if runner.manifest.Config.Namespace != runner.report.Namespace || runner.manifest.Topic != runner.report.Namespace+"-source" || runner.manifest.Config.Partitions != 2 {
		return fmt.Errorf("history removal requires this run's owned two-partition source")
	}
	var guard *postgres.Service
	if err := runner.until(ctx, "owned history producer guard", func() (bool, error) {
		var err error
		guard, err = runner.state.AcquireService(ctx, "producer")
		if errors.Is(err, postgres.ErrServiceBusy) {
			return false, nil
		}
		return err == nil, err
	}); err != nil {
		return err
	}
	defer guard.Close()
	var err error
	if evidence.SourceEnd, err = broker.Validate(ctx, runner.client, runner.manifest.Topic, runner.manifest.Config); err != nil {
		return err
	}
	if !slices.Equal(evidence.SourceEnd, []int64{4, 0}) {
		return fmt.Errorf("history removal requires the corrected fixture boundary")
	}
	if evidence.Source, err = broker.Inspect(ctx, runner.client, runner.manifest.Topic); err != nil {
		return err
	}
	if err = guard.Check(ctx); err != nil {
		return err
	}
	request := kmsg.NewDeleteRecordsRequest()
	topic := kmsg.NewDeleteRecordsRequestTopic()
	topic.Topic = runner.manifest.Topic
	partition := kmsg.NewDeleteRecordsRequestTopicPartition()
	partition.Partition, partition.Offset = 0, evidence.RemovedBefore
	topic.Partitions = append(topic.Partitions, partition)
	request.Topics = append(request.Topics, topic)
	if err = runner.save("history-request.json", request); err != nil {
		return err
	}
	response, err := request.RequestWith(ctx, runner.client)
	if err != nil {
		return fmt.Errorf("owned history removal outcome uncertain; retain the namespace for inspection: %w", err)
	}
	if len(response.Topics) != 1 || response.Topics[0].Topic != topic.Topic || len(response.Topics[0].Partitions) != 1 || response.Topics[0].Partitions[0].Partition != 0 {
		return fmt.Errorf("incomplete owned history removal response")
	}
	removed := response.Topics[0].Partitions[0]
	if err = kerr.ErrorForCode(removed.ErrorCode); err != nil {
		return err
	}
	if removed.LowWatermark != evidence.RemovedBefore {
		return fmt.Errorf("unexpected history removal low watermark: %d", removed.LowWatermark)
	}
	if err = runner.until(ctx, "owned source prefix removed", func() (bool, error) {
		evidence.RetainedStart, err = broker.Offsets(ctx, runner.client, topic.Topic, 2, -2)
		return slices.Equal(evidence.RetainedStart, []int64{2, 0}), err
	}); err != nil {
		return err
	}
	source, err := broker.Inspect(ctx, runner.client, topic.Topic)
	if err != nil {
		return err
	}
	ends, err := broker.Offsets(ctx, runner.client, topic.Topic, 2, -1)
	if err != nil {
		return err
	}
	if source != evidence.Source || !slices.Equal(ends, evidence.SourceEnd) {
		return fmt.Errorf("history removal changed source incarnation or end boundary")
	}
	return guard.Check(ctx)
}

func (runner *runner) historyDestinationAbsent(ctx context.Context, evidence historyEvidence) error {
	config := runner.manifest.Config.Clone()
	config.Namespace = evidence.DestinationNamespace
	destination, err := postgres.New(runner.pool, config)
	if err != nil {
		return err
	}
	exists, err := destination.Exists(ctx)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("missing-history rebuild destination exists")
	}
	if _, err = broker.Inspect(ctx, runner.client, evidence.DestinationOutput); !errors.Is(err, kerr.UnknownTopicOrPartition) {
		return fmt.Errorf("missing-history rebuild output is not confirmed absent: %v", err)
	}
	return nil
}
