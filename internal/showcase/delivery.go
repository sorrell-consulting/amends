package showcase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/sorrell-consulting/amends/internal/verify"
	"github.com/twmb/franz-go/pkg/kgo"
)

type stallEvidence struct {
	Before        postgres.Progress `json:"before_verification"`
	After         postgres.Progress `json:"after_verification"`
	OutputEnd     []int64           `json:"observed_output_end"`
	ViewUnchanged bool              `json:"view_unchanged"`
}

type redeliveryEvidence struct {
	Partition           int             `json:"partition"`
	Sequence            int64           `json:"committed_sequence"`
	Key                 []byte          `json:"raw_key"`
	Bytes               []byte          `json:"committed_bytes"`
	Offset              int64           `json:"redelivery_offset"`
	Tombstone           ledger.Envelope `json:"retained_tombstone"`
	ProcessingUnchanged bool            `json:"processing_unchanged"`
	ViewRowsUnchanged   bool            `json:"view_rows_unchanged"`
}

func (runner *runner) relayStall(ctx context.Context) error {
	baseline, err := runner.state.View(ctx)
	if err != nil {
		return err
	}
	if err = runner.load(ctx, "correction"); err != nil {
		return err
	}
	var evidence stallEvidence
	if err = runner.until(ctx, "correction committed with relay stopped", func() (bool, error) {
		progress, err := runner.state.Progress(ctx)
		if err != nil {
			return false, err
		}
		if len(progress.Partitions) != 2 {
			return false, fmt.Errorf("stall requires both partition observations")
		}
		if progress.Partitions[0].SourceNext < 4 {
			return false, nil
		}
		if err = checkStalledProgress(progress); err != nil {
			return false, err
		}
		evidence.Before = progress
		return true, nil
	}); err != nil {
		return err
	}
	report, err := runner.verifyIncomplete(ctx, "stalled", "30s")
	if err != nil {
		return err
	}
	checks := verify.Checks{WritersStopped: true, SourceScanned: true, WorkersReached: true}
	if report.Status != "INCOMPLETE" || report.Checks != checks || report.BinarySHA256 == "" || !strings.Contains(report.Reason, "pending outbox") || !slices.Equal(report.SourceEnd, []int64{4, 0}) || len(report.OutputEnd) != 0 {
		return fmt.Errorf("stalled verifier did not retain the incomplete publication boundary: %+v", report)
	}
	for _, key := range report.Keys {
		if key.Compared || key.Agrees {
			return fmt.Errorf("stalled verifier certified a key")
		}
	}
	if evidence.After, err = runner.state.Progress(ctx); err != nil {
		return err
	}
	if err = checkStalledProgress(evidence.After); err != nil {
		return err
	}
	if !evidence.Before.Partitions[0].OldestPending.Equal(*evidence.After.Partitions[0].OldestPending) || !evidence.After.ObservedAt.After(evidence.Before.ObservedAt) {
		return fmt.Errorf("stalled pending work did not retain its creation time across observations")
	}
	if evidence.OutputEnd, err = broker.ValidateOutput(ctx, runner.client, *runner.manifest.Output, runner.manifest.Config); err != nil {
		return err
	}
	view, err := runner.state.View(ctx)
	if err != nil {
		return err
	}
	evidence.ViewUnchanged = reflect.DeepEqual(baseline, view)
	if !evidence.ViewUnchanged || !slices.Equal(evidence.OutputEnd, []int64{5, 0}) {
		return fmt.Errorf("stopped relay changed output or durable view")
	}
	if err = runner.save("stall.json", evidence); err != nil {
		return err
	}
	if err = runner.inspectStage(ctx, "stalled"); err != nil {
		return err
	}
	_, err = fmt.Fprintln(runner.options.Output, "stalled: verifier INCOMPLETE; H=[4 0]; five pending envelopes age while output/view remain at [5 0]; no comparison certified.")
	if err != nil {
		return err
	}
	return runner.checkpoint(ctx, RelayStalled)
}

func (runner *runner) verifyIncomplete(ctx context.Context, name, timeout string) (verify.Report, error) {
	var report verify.Report
	if err := ctx.Err(); err != nil {
		return report, err
	}
	child, err := runner.start("verify-"+name, runner.options.ControlBinary, true, "verify", "--quiesce", "-brokers", strings.Join(runner.options.Brokers, ","), "-manifest", filepath.Join(runner.report.Directory, "manifest.json"), "-timeout", timeout)
	if err != nil {
		return report, err
	}
	var exitError *exec.ExitError
	if err = child.wait(ctx); !errors.As(err, &exitError) || exitError.ExitCode() != 1 {
		return report, fmt.Errorf("verify-%s must exit 1, got %v", name, err)
	}
	report, err = runner.readVerificationReport(name)
	if err != nil {
		return report, err
	}
	runner.report.Stages = append(runner.report.Stages, report)
	return report, nil
}

func checkStalledProgress(progress postgres.Progress) error {
	if len(progress.Partitions) != 2 {
		return fmt.Errorf("stall requires both partition observations")
	}
	for index, partition := range progress.Partitions {
		if partition.Partition != index {
			return fmt.Errorf("unexpected partition observation order")
		}
		sourceNext, sequence, pending, viewNext := int64(0), int64(0), int64(0), int64(0)
		if partition.Partition == 0 {
			sourceNext, sequence, pending, viewNext = 4, 10, 5, 5
			if partition.OldestPending == nil || partition.LastReceipt == nil || *partition.LastReceipt != 4 || partition.OldestPending.After(progress.ObservedAt) {
				return fmt.Errorf("stall missing pending age or baseline receipt: %+v", partition)
			}
		} else if partition.Partition != 1 || partition.OldestPending != nil || partition.LastReceipt != nil {
			return fmt.Errorf("unexpected idle partition: %+v", partition)
		}
		if partition.SourceNext != sourceNext || partition.EmissionSequence != sequence || partition.Pending != pending || partition.ViewNext != viewNext || partition.ProtocolError != "" || partition.Quarantined != 0 {
			return fmt.Errorf("unexpected stalled progress: %+v", partition)
		}
	}
	return nil
}

func (runner *runner) disappearingDay(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	id := ledger.OutputID{Namespace: runner.report.Namespace, Family: "balance", Key: runner.manifest.Config.Keys[0].StockKey, Day: "2026-01-03"}
	evidence := redeliveryEvidence{Partition: 0, Key: broker.EncodeKey(id.Key)}
	namespace, err := json.Marshal(runner.report.Namespace)
	if err != nil {
		return err
	}
	if err = runner.pool.QueryRow(ctx, `SELECT sequence,envelope FROM amends_outbox
	 WHERE namespace_id=$1 AND partition=0 AND output_id=$2 AND version=2 AND acknowledged_at IS NOT NULL`, string(namespace), id.String()).Scan(&evidence.Sequence, &evidence.Bytes); err != nil {
		return err
	}
	old, err := broker.DecodeOutput(evidence.Key, evidence.Bytes)
	if err != nil {
		return err
	}
	if old.ID != id || old.Version != 2 || old.Type != "BalanceStated" || !old.Payload.Present || old.Payload.Close != 100 {
		return fmt.Errorf("unexpected committed Day 3 balance for redelivery: %+v", old)
	}
	if runner.relay, err = runner.role("relay-withdrawn", "relay"); err != nil {
		return err
	}
	if runner.options.Scenario == MaterializerCrashDrill {
		err = runner.crashMaterializer(ctx, id)
	} else {
		err = runner.load(ctx, "cancel-last-day")
	}
	if err != nil {
		return err
	}
	if err = runner.awaitSourceOutbox(ctx, sourceExpectation{Next: 5}, sourceExpectation{}); err != nil {
		return err
	}
	if err = runner.relay.stop(); err != nil {
		return err
	}
	if err = runner.fixtureStage(ctx, "withdrawn", 5, "expected-after-cancel.json"); err != nil {
		return err
	}
	if err = runner.checkpoint(ctx, WithdrawalVerified); err != nil {
		return err
	}
	before, err := runner.state.Snapshot(ctx)
	if err != nil {
		return err
	}
	beforeView, err := runner.state.View(ctx)
	if err != nil {
		return err
	}
	evidence.Tombstone = beforeView.Rows[id]
	if evidence.Tombstone.Version != 3 || evidence.Tombstone.Type != "BalanceRetracted" || evidence.Tombstone.Payload.Present || !evidence.Tombstone.Equal(before.Outputs[id]) || len(before.Outbox) != 11 {
		return fmt.Errorf("withdrawal did not retain Day 3 deletion version 3")
	}
	if err = runner.command(ctx, "versions-withdrawn", "view-show", "-manifest", filepath.Join(runner.report.Directory, "manifest.json")); err != nil {
		return err
	}
	if runner.options.Scenario == MaterializerCrashDrill {
		if err = runner.crashCommittedMaterializer(ctx, id); err != nil {
			return err
		}
	}
	if evidence.Offset, err = runner.redeliver(ctx, evidence); err != nil {
		return err
	}
	if err = runner.save("redelivery.json", evidence); err != nil {
		return err
	}
	if evidence.Offset != 11 {
		return fmt.Errorf("unexpected redelivery coordinate: %d", evidence.Offset)
	}
	if runner.options.Scenario == MaterializerCrashDrill {
		if err = runner.restartMaterializer(ctx, "view-after-commit"); err != nil {
			return err
		}
		runner.report.MaterializerCrash.RestartPID = runner.view.cmd.Process.Pid
	}
	if err = runner.awaitSourceOutbox(ctx, sourceExpectation{Next: 5}, sourceExpectation{}); err != nil {
		return err
	}
	if err = runner.fixtureStage(ctx, "redelivered", 5, "expected-after-cancel.json"); err != nil {
		return err
	}
	after, err := runner.state.Snapshot(ctx)
	if err != nil {
		return err
	}
	afterView, err := runner.state.View(ctx)
	if err != nil {
		return err
	}
	evidence.ProcessingUnchanged = reflect.DeepEqual(before, after)
	evidence.ViewRowsUnchanged = reflect.DeepEqual(beforeView.Rows, afterView.Rows)
	if err = runner.save("redelivery.json", evidence); err != nil {
		return err
	}
	if !evidence.ProcessingUnchanged || !evidence.ViewRowsUnchanged || afterView.Next[0] != 12 || afterView.Next[1] != 0 {
		return fmt.Errorf("late old output changed semantic state or failed to advance only view progress")
	}
	if err = runner.command(ctx, "versions-redelivered", "view-show", "-manifest", filepath.Join(runner.report.Directory, "manifest.json")); err != nil {
		return err
	}
	_, err = fmt.Fprintln(runner.options.Output, "redelivery: exact committed Day 3 version 2 consumed; deletion version 3 retained; processing/output versions unchanged.")
	return err
}

func (runner *runner) redeliver(ctx context.Context, evidence redeliveryEvidence) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	guard, err := runner.state.AcquireService(ctx, "relay")
	if err != nil {
		return 0, err
	}
	defer guard.Close()
	client, err := broker.Client(runner.options.Brokers, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		return 0, err
	}
	defer client.Close()
	end, err := broker.ValidateOutput(ctx, client, *runner.manifest.Output, runner.manifest.Config)
	if err != nil {
		return 0, err
	}
	if !slices.Equal(end, []int64{11, 0}) {
		return 0, fmt.Errorf("redelivery requires the withdrawn output boundary, got %v", end)
	}
	if err = guard.Check(ctx); err != nil {
		return 0, err
	}
	record := &kgo.Record{Topic: runner.manifest.Output.Topic, Partition: int32(evidence.Partition), Key: slices.Clone(evidence.Key), Value: slices.Clone(evidence.Bytes)}
	if err = client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return 0, fmt.Errorf("redelivery outcome uncertain; retain namespace for inspection: %w", err)
	}
	if err = guard.Check(ctx); err != nil {
		return 0, err
	}
	return record.Offset, nil
}
