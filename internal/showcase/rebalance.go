package showcase

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/verify"
	"github.com/sorrell-consulting/amends/internal/worker"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type RebalanceGroup struct {
	State   string           `json:"state"`
	Members map[string][]int `json:"member_partitions"`
}

type RebalanceEvidence struct {
	Scope              string                     `json:"scope"`
	ExpectedSourceEnd  []int64                    `json:"expected_source_end"`
	Baseline           map[int]store.Partition    `json:"baseline_partitions"`
	Held               RebalanceAttempt           `json:"held_polled_decision"`
	Preparing          RebalanceGroup             `json:"preparing_group"`
	PreparingUnchanged bool                       `json:"preparing_state_and_epochs_unchanged"`
	Reassigned         RebalanceGroup             `json:"reassigned_group"`
	Successors         []RebalanceAttempt         `json:"successor_decisions_with_backlog"`
	Released           bool                       `json:"successors_released"`
	Completed          bool                       `json:"complete_coverage_and_versions_checked"`
	Workers            []*RebalanceWorkerEvidence `json:"workers"`
}

func (runner *runner) rebalanceGroup(ctx context.Context) (RebalanceGroup, error) {
	request := kmsg.NewDescribeGroupsRequest()
	request.Groups = []string{worker.Group(runner.report.Namespace)}
	response, err := request.RequestWith(ctx, runner.client)
	group := RebalanceGroup{Members: map[string][]int{}}
	if err != nil {
		return group, err
	}
	if len(response.Groups) != 1 || response.Groups[0].ErrorCode != 0 {
		return group, fmt.Errorf("cannot observe owned source group")
	}
	actual := response.Groups[0]
	group.State = actual.State
	for _, member := range actual.Members {
		group.Members[member.MemberID] = nil
		if len(member.MemberAssignment) == 0 {
			continue
		}
		var assignment kmsg.ConsumerMemberAssignment
		if err = assignment.ReadFrom(member.MemberAssignment); err != nil {
			return group, err
		}
		for _, topic := range assignment.Topics {
			if topic.Topic != runner.manifest.Topic {
				return group, fmt.Errorf("owned group was assigned a different source")
			}
			for _, partition := range topic.Partitions {
				group.Members[member.MemberID] = append(group.Members[member.MemberID], int(partition))
			}
		}
		slices.Sort(group.Members[member.MemberID])
	}
	return group, nil
}

func rebalanceAssigned(group RebalanceGroup, members int) bool {
	if group.State != "Stable" || len(group.Members) != members {
		return false
	}
	var partitions []int
	for _, assigned := range group.Members {
		if len(assigned) != 2/members {
			return false
		}
		partitions = append(partitions, assigned...)
	}
	slices.Sort(partitions)
	return slices.Equal(partitions, []int{0, 1})
}

func (runner *runner) rebalanceUnderLoad(ctx context.Context) (err error) {
	evidence := &RebalanceEvidence{Scope: "real production worker.Run group rebalance, two populated partitions, finite queued corrections; pre-storage gates, not a throughput benchmark", ExpectedSourceEnd: []int64{35, 35}}
	runner.report.Rebalance = evidence
	gates := newRebalanceGates()
	var actors []*rebalanceWorker
	defer func() { err = errors.Join(err, joinRebalanceWorkers(actors)) }()
	first, err := runner.startRebalanceWorker(ctx, "rebalance-worker-1", gates)
	if err != nil {
		return err
	}
	actors = append(actors, first)
	evidence.Workers = append(evidence.Workers, first.evidence)
	if runner.view, err = runner.role("view", "view"); err != nil {
		return err
	}
	if runner.relay, err = runner.role("relay-before", "relay"); err != nil {
		return err
	}
	if err = runner.until(ctx, "one worker assigned both partitions", func() (bool, error) {
		if err := rebalanceHealthy(actors); err != nil {
			return false, err
		}
		group, err := runner.rebalanceGroup(ctx)
		return rebalanceAssigned(group, 1), err
	}); err != nil {
		return err
	}
	if err = runner.load(ctx, "rebalance-before"); err != nil {
		return err
	}
	if err = runner.awaitSourceOutbox(ctx, sourceExpectation{Next: 3}, sourceExpectation{Next: 3}); err != nil {
		return err
	}
	if err = runner.relay.stop(); err != nil {
		return err
	}
	if err = runner.rebalanceStage(ctx, "before", 3, 5); err != nil {
		return err
	}
	snapshot, err := runner.state.Snapshot(ctx)
	if err != nil {
		return err
	}
	evidence.Baseline = snapshot.Partitions
	gates.arm(snapshot.Partitions)
	before, err := runner.durableState(ctx)
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(runner.report.Directory, "rebalance-before.json"), before, 0600); err != nil {
		return err
	}
	raw, err := fixtures.Files.ReadFile("rebalance-correction.jsonl")
	if err != nil {
		return err
	}
	burst := bytes.Repeat(raw, 32)
	digest := sha256.Sum256(burst)
	runner.report.SHA256["rebalance-burst.jsonl"] = hex.EncodeToString(digest[:])
	if err = os.WriteFile(filepath.Join(runner.report.Directory, "rebalance-burst.jsonl"), burst, 0600); err != nil {
		return err
	}
	if _, err = fmt.Fprintln(runner.options.Output, "rebalance-ready: one real group member has verified both populated partitions; queue 32 correction deliveries per partition."); err != nil {
		return err
	}
	if err = runner.checkpoint(ctx, RebalanceReady); err != nil {
		return err
	}
	if err = runner.load(ctx, "rebalance-burst"); err != nil {
		return err
	}
	select {
	case evidence.Held = <-gates.entered:
	case <-first.done:
		return fmt.Errorf("first worker stopped before the polled-decision gate: %v", first.err)
	case <-ctx.Done():
		return ctx.Err()
	}
	if evidence.Held.Expected != 3 || evidence.Held.Record.Offset != 3 || evidence.Held.Token.Epoch != snapshot.Partitions[evidence.Held.Record.Partition].Epoch {
		return fmt.Errorf("first polled correction did not retain baseline ownership/frontier")
	}
	second, err := runner.startRebalanceWorker(ctx, "rebalance-worker-2", gates)
	if err != nil {
		return err
	}
	actors = append(actors, second)
	evidence.Workers = append(evidence.Workers, second.evidence)
	if err = runner.until(ctx, "broker preparing rebalance while a polled decision is held", func() (bool, error) {
		if err := rebalanceHealthy(actors); err != nil {
			return false, err
		}
		group, err := runner.rebalanceGroup(ctx)
		evidence.Preparing = group
		return group.State == "PreparingRebalance", err
	}); err != nil {
		return err
	}
	preparing, err := runner.durableState(ctx)
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(runner.report.Directory, "rebalance-preparing.json"), preparing, 0600); err != nil {
		return err
	}
	preparedSnapshot, err := runner.state.Snapshot(ctx)
	if err != nil {
		return err
	}
	evidence.PreparingUnchanged = bytes.Equal(before, preparing) && reflect.DeepEqual(snapshot, preparedSnapshot)
	if !evidence.PreparingUnchanged {
		return fmt.Errorf("rebalance overtook the held polled decision")
	}
	if err = runner.rebalanceIncomplete(ctx, "preparing"); err != nil {
		return err
	}
	if _, err = fmt.Fprintln(runner.options.Output, "rebalance-preparing: broker PreparingRebalance; H=[35 35], processing [3 3], output/view [5 5]; state and epochs unchanged; verifier INCOMPLETE."); err != nil {
		return err
	}
	if err = runner.checkpoint(ctx, RebalancePreparing); err != nil {
		return err
	}
	close(gates.release)
	for len(evidence.Successors) < 2 {
		select {
		case attempt := <-gates.successorEntered:
			evidence.Successors = append(evidence.Successors, attempt)
		case <-first.done:
			return fmt.Errorf("first worker stopped during rebalance: %v", first.err)
		case <-second.done:
			return fmt.Errorf("second worker stopped during rebalance: %v", second.err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err = runner.until(ctx, "two members assigned one populated partition each", func() (bool, error) {
		if err := rebalanceHealthy(actors); err != nil {
			return false, err
		}
		group, err := runner.rebalanceGroup(ctx)
		evidence.Reassigned = group
		return rebalanceAssigned(group, 2), err
	}); err != nil {
		return err
	}
	runner.report.TwoMembersAssigned = true
	seen := map[int]bool{}
	workers := map[string]bool{}
	for _, attempt := range evidence.Successors {
		token, partition, err := runner.state.Observe(ctx, attempt.Record.Partition)
		if err != nil {
			return err
		}
		if token != attempt.Token || partition.Next != attempt.Expected || attempt.Expected != attempt.Record.Offset || attempt.Expected < 3 || attempt.Expected >= 35 || token.Epoch <= evidence.Baseline[token.Partition].Epoch {
			return fmt.Errorf("successor did not seek the durable frontier under a higher epoch with queued load")
		}
		seen[token.Partition], workers[attempt.Worker] = true, true
	}
	if !seen[0] || !seen[1] || len(workers) != 2 {
		return fmt.Errorf("both replacement assignments did not process populated partitions")
	}
	if err = runner.rebalanceIncomplete(ctx, "reassigned"); err != nil {
		return err
	}
	if _, err = fmt.Fprintln(runner.options.Output, "rebalance-reassigned: two real members each polled queued input under a higher epoch at its durable frontier; verifier remains INCOMPLETE until load drains."); err != nil {
		return err
	}
	if err = runner.checkpoint(ctx, RebalanceReassigned); err != nil {
		return err
	}
	evidence.Released = true
	close(gates.successorRelease)
	if runner.relay, err = runner.role("relay-after", "relay"); err != nil {
		return err
	}
	if err = runner.awaitSourceOutbox(ctx, sourceExpectation{Next: 35}, sourceExpectation{Next: 35}); err != nil {
		return err
	}
	if err = runner.relay.stop(); err != nil {
		return err
	}
	if err = runner.rebalanceStage(ctx, "after", 35, 10); err != nil {
		return err
	}
	if err = rebalanceHealthy(actors); err != nil {
		return err
	}
	if err = joinRebalanceWorkers(actors); err != nil {
		return err
	}
	if err = runner.checkRebalanceCoverage(ctx, evidence); err != nil {
		return err
	}
	evidence.Completed = true
	_, err = fmt.Fprintln(runner.options.Output, "rebalance-complete: 70 retained source records processed across both workers; strict PASS at H=[35 35], O=[10 10]; fixed versions and alert withdrawals retained. Finite backlog, not a load benchmark.")
	return err
}

func (runner *runner) rebalanceIncomplete(ctx context.Context, stage string) error {
	report, err := runner.verifyIncomplete(ctx, stage, "5s")
	if err != nil {
		return err
	}
	if report.Status != "INCOMPLETE" || report.Checks != (verify.Checks{WritersStopped: true, SourceScanned: true}) || !slices.Equal(report.SourceEnd, []int64{35, 35}) || len(report.OutputEnd) != 0 || report.SourceRecords != 70 || !strings.Contains(report.Reason, "workers have not reached source boundary") {
		return fmt.Errorf("rebalance certified a partial source/output boundary: %+v", report)
	}
	for _, key := range report.Keys {
		if key.Compared || key.Agrees {
			return fmt.Errorf("rebalance certified an incomplete key")
		}
	}
	return runner.inspectStage(ctx, stage)
}

func (runner *runner) rebalanceStage(ctx context.Context, name string, sourceEnd, outputEnd int64) error {
	if err := runner.command(ctx, "verify-"+name, "verify", "--quiesce", "-manifest", filepath.Join(runner.report.Directory, "manifest.json"), "-timeout", "30s"); err != nil {
		return err
	}
	report, err := runner.readVerificationReport(name)
	if err != nil {
		return err
	}
	runner.report.Stages = append(runner.report.Stages, report)
	if report.Status != "PASS" || !slices.Equal(report.SourceEnd, []int64{sourceEnd, sourceEnd}) || !slices.Equal(report.OutputEnd, []int64{outputEnd, outputEnd}) {
		return fmt.Errorf("rebalance %s did not verify both complete partition boundaries: got status=%s H=%v O=%v; want status=PASS H=%v O=%v; see %s",
			name, report.Status, report.SourceEnd, report.OutputEnd, []int64{sourceEnd, sourceEnd}, []int64{outputEnd, outputEnd},
			filepath.Join(runner.report.Directory, "verify-"+name+".log"))
	}
	view, err := runner.state.View(ctx)
	if err != nil {
		return err
	}
	var projections []ledger.Projection
	for _, file := range []string{"expected-" + name + ".json", "expected-rebalance-other-" + name + ".json"} {
		expected, err := fixtures.Expected(file)
		if err != nil {
			return err
		}
		actual := view.Projection(expected.Key)
		if !reflect.DeepEqual(actual, expected) {
			return fmt.Errorf("%s differs from the hand-derived two-key fixture %s: got %+v; want %+v",
				name, filepath.Join(runner.report.Directory, file), actual, expected)
		}
		projections = append(projections, actual)
	}
	if err = runner.save("view-"+name+".json", projections); err != nil {
		return err
	}
	if err = runner.inspectStage(ctx, name); err != nil {
		return err
	}
	_, err = fmt.Fprintf(runner.options.Output, "%s: both populated partitions match hand-derived fixtures; H=%v O=%v; verifier PASS.\n", name, report.SourceEnd, report.OutputEnd)
	return err
}

func (runner *runner) checkRebalanceCoverage(ctx context.Context, evidence *RebalanceEvidence) error {
	seen := map[ledger.Coordinates]bool{}
	for _, actor := range evidence.Workers {
		for _, attempt := range actor.Committed {
			if attempt.Record.Source != runner.manifest.Config.Source || attempt.Record.Partition < 0 || attempt.Record.Partition > 1 || attempt.Record.Offset < 0 || attempt.Record.Offset >= 35 || attempt.Expected != attempt.Record.Offset || seen[attempt.Record] {
				return fmt.Errorf("rebalance skipped or repeated an acknowledged source decision")
			}
			seen[attempt.Record] = true
		}
	}
	if len(seen) != 70 {
		return fmt.Errorf("rebalance did not acknowledge the complete retained source")
	}
	snapshot, err := runner.state.Snapshot(ctx)
	if err != nil {
		return err
	}
	view, err := runner.state.View(ctx)
	if err != nil {
		return err
	}
	if len(snapshot.Outbox) != 20 || len(snapshot.Outputs) != 12 || len(view.Rows) != 12 || len(snapshot.Rejections) != 0 {
		return fmt.Errorf("rebalance changed output count or diagnostics")
	}
	for _, key := range runner.manifest.Config.Keys {
		for _, expected := range []struct {
			family  ledger.OutputFamily
			day     string
			version int64
			present bool
		}{
			{"balance", "2026-01-01", 2, true}, {"balance", "2026-01-02", 2, true}, {"balance", "2026-01-03", 2, true},
			{"alert", "2026-01-01", 2, false}, {"alert", "2026-01-02", 1, true}, {"key-status", "", 1, true},
		} {
			row := view.Rows[ledger.OutputID{Namespace: runner.report.Namespace, Family: expected.family, Key: key.StockKey, Day: expected.day}]
			if row.Version != expected.version || row.Payload.Present != expected.present || !row.Equal(snapshot.Outputs[row.ID]) {
				return fmt.Errorf("rebalance lost a fixed output version or withdrawal: %+v", row)
			}
		}
	}
	return nil
}
