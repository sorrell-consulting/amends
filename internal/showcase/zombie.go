package showcase

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/verify"
	"github.com/sorrell-consulting/amends/internal/worker"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type ZombieEvidence struct {
	Boundary       string          `json:"boundary"`
	WorkerPID      int             `json:"worker_process_pid"`
	MemberID       string          `json:"broker_member_id"`
	Previous       store.Token     `json:"previous_token"`
	Installed      store.Token     `json:"installed_token"`
	Recovered      store.Token     `json:"recovered_token"`
	Before         store.Partition `json:"partition_before"`
	AfterFence     store.Partition `json:"partition_after_fence"`
	FencedAttempts int             `json:"fenced_attempts"`
	WorkerExited   bool            `json:"worker_exited_with_fence"`
	StateUnchanged bool            `json:"durable_state_unchanged_except_installed_epoch"`
	OutputEnd      []int64         `json:"output_end_after_fence"`
}

func (runner *runner) zombieWorker(ctx context.Context) error {
	for _, child := range runner.children {
		if child.name == "worker-1" || child.name == "worker-2" {
			if err := child.stop(); err != nil {
				return err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	stale, err := runner.role("worker-stale", "source")
	if err != nil {
		return err
	}
	evidence := &ZombieEvidence{Boundary: "normal source worker processing after a replacement PostgreSQL epoch commits", WorkerPID: stale.cmd.Process.Pid}
	runner.report.Zombie = evidence
	if err = runner.command(ctx, "load-zombie-duplicates", "source-load", "-manifest", filepath.Join(runner.report.Directory, "manifest.json"), "-file", filepath.Join(runner.report.Directory, "before.jsonl")); err != nil {
		return err
	}
	if err = runner.awaitSourceOutbox(ctx, sourceExpectation{Next: 6}, sourceExpectation{}); err != nil {
		return err
	}
	if err = runner.fixtureStage(ctx, "reowned", 6, "expected-before.json"); err != nil {
		return err
	}
	if err = runner.until(ctx, "sole worker owns both source partitions", func() (bool, error) {
		request := kmsg.NewDescribeGroupsRequest()
		request.Groups = []string{worker.Group(runner.report.Namespace)}
		response, err := request.RequestWith(ctx, runner.client)
		if err != nil {
			return false, err
		}
		if len(response.Groups) != 1 || response.Groups[0].ErrorCode != 0 || response.Groups[0].State != "Stable" || len(response.Groups[0].Members) != 1 {
			return false, nil
		}
		member := response.Groups[0].Members[0]
		var assignment kmsg.ConsumerMemberAssignment
		if err = assignment.ReadFrom(member.MemberAssignment); err != nil {
			return false, err
		}
		if len(assignment.Topics) != 1 || assignment.Topics[0].Topic != runner.manifest.Topic {
			return false, fmt.Errorf("unexpected sole-worker assignment")
		}
		partitions := slices.Clone(assignment.Topics[0].Partitions)
		slices.Sort(partitions)
		if !slices.Equal(partitions, []int32{0, 1}) {
			return false, fmt.Errorf("sole worker does not own both partitions")
		}
		evidence.MemberID = member.MemberID
		return true, nil
	}); err != nil {
		return err
	}
	evidence.Previous, evidence.Before, err = runner.state.Observe(ctx, 0)
	if err != nil {
		return err
	}
	if evidence.Before.Next != 6 || evidence.Before.Sequence != 5 {
		return fmt.Errorf("zombie worker requires the duplicated baseline frontier")
	}
	log, err := os.ReadFile(filepath.Join(runner.report.Directory, "worker-stale.log"))
	if err != nil {
		return err
	}
	events, err := readWorkerEvents(log)
	if err != nil {
		return err
	}
	acknowledged := slices.ContainsFunc(events, func(event workerEvent) bool {
		return event.Event == worker.ProcessingAttemptEvent && event.Namespace == runner.report.Namespace &&
			*event.Partition == 0 && *event.Offset == 5 && *event.Epoch == evidence.Previous.Epoch && event.Outcome == "acknowledged"
	})
	if !acknowledged {
		return fmt.Errorf("sole worker did not establish processing under the saved token")
	}
	before, err := runner.durableState(ctx)
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(runner.report.Directory, "zombie-before.json"), before, 0600); err != nil {
		return err
	}
	snapshot, err := runner.state.Snapshot(ctx)
	if err != nil {
		return err
	}
	if _, err = fmt.Fprintln(runner.options.Output, "zombie-ready: one normal worker has consumed duplicate input with its saved epoch; install a replacement database epoch before its next source record."); err != nil {
		return err
	}
	if err = runner.checkpoint(ctx, ZombieReady); err != nil {
		return err
	}
	evidence.Installed, err = runner.state.Claim(ctx, 0)
	if err != nil {
		return fmt.Errorf("replacement epoch not confirmed; retain namespace for inspection: %w", err)
	}
	installed, partition, err := runner.state.Observe(ctx, 0)
	if err != nil {
		return err
	}
	if installed != evidence.Installed || installed.Epoch != evidence.Previous.Epoch+1 || partition.Next != 6 || partition.Sequence != 5 {
		return fmt.Errorf("replacement epoch/frontier not independently observed")
	}
	if err = stale.expectExit(ctx, 1, func() error { return runner.load(ctx, "correction") }); err != nil {
		return err
	}

	log, err = os.ReadFile(filepath.Join(runner.report.Directory, "worker-stale.log"))
	if err != nil {
		return err
	}
	if evidence.FencedAttempts, err = zombieFenceAttempts(log, runner.report.Namespace, evidence.Previous.Epoch); err != nil {
		return err
	}
	evidence.WorkerExited = true
	_, evidence.AfterFence, err = runner.state.Observe(ctx, 0)
	if err != nil {
		return err
	}
	after, err := runner.durableState(ctx)
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(runner.report.Directory, "zombie-after-fence.json"), after, 0600); err != nil {
		return err
	}
	afterSnapshot, err := runner.state.Snapshot(ctx)
	if err != nil {
		return err
	}
	expectedPartition := snapshot.Partitions[0]
	expectedPartition.Epoch = evidence.Installed.Epoch
	snapshot.Partitions[0] = expectedPartition
	evidence.StateUnchanged = bytes.Equal(before, after) && reflect.DeepEqual(snapshot, afterSnapshot)
	evidence.OutputEnd, err = broker.ValidateOutput(ctx, runner.client, *runner.manifest.Output, runner.manifest.Config)
	if err != nil {
		return err
	}
	if !evidence.StateUnchanged || evidence.AfterFence != expectedPartition || !slices.Equal(evidence.OutputEnd, []int64{5, 0}) {
		return fmt.Errorf("obsolete worker changed state, output intent, progress, or view after replacement epoch")
	}
	report, err := runner.verifyIncomplete(ctx, "fenced", "5s")
	if err != nil {
		return err
	}
	if report.Status != "INCOMPLETE" || report.Checks != (verify.Checks{WritersStopped: true, SourceScanned: true}) || !slices.Equal(report.SourceEnd, []int64{7, 0}) || len(report.OutputEnd) != 0 || report.SourceRecords != 7 || !strings.Contains(report.Reason, "workers have not reached source boundary") {
		return fmt.Errorf("fenced pipeline did not retain the incomplete source boundary: %+v", report)
	}
	for _, key := range report.Keys {
		if key.Compared || key.Agrees {
			return fmt.Errorf("fenced pipeline certified an unprocessed correction")
		}
	}
	if err = runner.inspectStage(ctx, "fenced"); err != nil {
		return err
	}
	if _, err = fmt.Fprintf(runner.options.Output, "zombie-fenced: worker PID %d exited 1 after one real processing attempt with epoch %d; installed epoch %d preserved all other durable state. Verifier INCOMPLETE; source [7 0], processing [6 0], output/view [5 0].\n", evidence.WorkerPID, evidence.Previous.Epoch, evidence.Installed.Epoch); err != nil {
		return err
	}
	if err = runner.checkpoint(ctx, ZombieFenced); err != nil {
		return err
	}
	for _, name := range []string{"worker-recovery-1", "worker-recovery-2"} {
		if _, err = runner.role(name, "source"); err != nil {
			return err
		}
	}
	return runner.twoMembers(ctx)
}

func zombieFenceAttempts(log []byte, namespace string, epoch int64) (int, error) {
	events, err := readWorkerEvents(log)
	if err != nil {
		return 0, err
	}
	attempts, stopped := 0, 0
	for _, event := range events {
		if event.Namespace != namespace || *event.Partition != 0 || *event.Offset != 6 {
			continue
		}
		if *event.Epoch != epoch || event.Outcome != "fenced" {
			return attempts, fmt.Errorf("obsolete worker reported a non-fenced or different-epoch outcome")
		}
		switch event.Event {
		case worker.ProcessingAttemptEvent:
			attempts++
		case worker.ProcessingStoppedEvent:
			stopped++
		}
	}
	if attempts != 1 || stopped != 1 {
		return attempts, fmt.Errorf("worker must retain one fenced attempt and one terminal outcome; got %d and %d", attempts, stopped)
	}
	return attempts, nil
}

func (runner *runner) zombieRecovered(ctx context.Context) error {
	evidence := runner.report.Zombie
	var partition store.Partition
	var err error
	evidence.Recovered, partition, err = runner.state.Observe(ctx, 0)
	if err != nil {
		return err
	}
	final := runner.report.Stages[len(runner.report.Stages)-1]
	if evidence.Recovered.Epoch <= evidence.Installed.Epoch || partition.Next != 7 || partition.Sequence != 10 || final.Status != "PASS" || !slices.Equal(final.SourceEnd, []int64{7, 0}) || !slices.Equal(final.OutputEnd, []int64{10, 0}) {
		return fmt.Errorf("replacement workers did not resume the durable frontier exactly once")
	}
	_, err = fmt.Fprintln(runner.options.Output, "zombie-recovered: normal workers resumed from the unchanged frontier under a higher epoch; strict PASS at H=[7 0], O=[10 0]. No offset reset or extra semantic transition.")
	return err
}
