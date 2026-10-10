// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"encoding/json"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/sim"
)

func fixture(t *testing.T, names ...string) Trace {
	t.Helper()
	c, err := fixtures.Config("delivery-schedule-fixture")
	if err != nil {
		t.Fatal(err)
	}
	source, err := fixtures.Records(c, names...)
	if err != nil {
		t.Fatal(err)
	}
	return Trace{Version: Version, Implementation: Implementation, Toolchain: runtime.Version(), ConfigDigest: c.Digest(), Config: c, Source: source, Drain: true}
}
func run(t *testing.T, trace Trace) *Result {
	t.Helper()
	result, err := Run(t.Context(), trace)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func checkFixture(t *testing.T, result *Result, name string) {
	t.Helper()
	want, err := fixtures.Expected(name)
	if err != nil {
		t.Fatal(err)
	}
	got := result.Model.View.Projection(want.Key)
	if result.Report.Status != "PASS" || got.Key != want.Key || got.Status != want.Status || !slices.Equal(got.Balances, want.Balances) || !slices.Equal(got.Alerts, want.Alerts) {
		t.Fatalf("hand fixture mismatch: report=%s got=%+v want=%+v", result.Report.Status, got, want)
	}
}
func operations(r *Result, step int) []string {
	var result []string
	for _, e := range r.Report.Events {
		if e.Step == step {
			result = append(result, e.Operation)
		}
	}
	return result
}

func TestSC20_PublicationReplyLossAndDelay(t *testing.T) {
	trace := fixture(t, "before.jsonl")
	trace.Steps = []Step{{Actor: "relay", Faults: []Fault{{Operation: "publish", Call: 1, Outcome: sim.UnknownCommit, DelayMillis: 250}}}}
	r := run(t, trace)
	checkFixture(t, r, "expected-before.json")
	if !slices.Equal(operations(r, 0), []string{"pending", "publish", "retry-wait", "publish", "receipt-write"}) || r.Report.LogicalMillis != 350 {
		t.Fatalf("retry did not preserve real orchestration and logical wait: %+v", r.Report)
	}
	if len(r.Model.Output[0]) != 6 || !r.Model.Output[0][0].Equal(r.Model.Output[0][1]) {
		t.Fatal("lost reply did not retry the unchanged version/envelope")
	}
}

func TestSC20_ReceiptRecoveryDoesNotRepublish(t *testing.T) {
	for _, outcome := range []string{sim.UnknownCommit, sim.UnknownAbort} {
		t.Run(outcome, func(t *testing.T) {
			trace := fixture(t, "before.jsonl")
			trace.Steps = []Step{{Actor: "relay", Faults: []Fault{{Operation: "receipt-write", Call: 1, Outcome: outcome}}}}
			r := run(t, trace)
			checkFixture(t, r, "expected-before.json")
			want := []string{"pending", "publish", "receipt-write", "receipt-read"}
			if outcome == sim.UnknownAbort {
				want = append(want, "retry-wait", "receipt-write")
			}
			if !slices.Equal(operations(r, 0), want) || len(r.Model.Output[0]) != 5 {
				t.Fatalf("receipt recovery republished or skipped observation: %v", operations(r, 0))
			}
		})
	}
}

func TestSC20_CrashParksUntilExplicitRecovery(t *testing.T) {
	trace := fixture(t, "before.jsonl")
	trace.Drain = false
	trace.Steps = []Step{{Actor: "relay", Faults: []Fault{{Operation: "publish", Call: 1, Outcome: "crash-after"}}}, {Actor: "relay"}, {Actor: "view"}}
	r := run(t, trace)
	if r.Report.Status != "INCOMPLETE" || r.Report.Attempts[0].Result != "retired" || r.Report.Attempts[1].Result != "parked" || len(operations(r, 1)) != 0 || r.Model.View.Next[0] != 1 {
		t.Fatal("crash did not preserve explicit retirement and incomplete boundary", r.Report)
	}
	if row, ok := r.Model.Store.Pending(0); !ok || row.Sequence != 1 {
		t.Fatal("unacknowledged publication advanced intent")
	}
	trace.Drain = true
	r = run(t, trace)
	checkFixture(t, r, "expected-before.json")
	if !r.Report.Attempts[3].Recovery || r.Report.Attempts[3].Step.Actor != "resume-relay" || len(r.Model.Output[0]) != 6 {
		t.Fatal("fair suffix did not explicitly resume and redeliver")
	}
}

func TestSC21_ViewCommitOutcomes(t *testing.T) {
	for _, outcome := range []string{sim.UnknownCommit, sim.UnknownAbort, "crash-before", "crash-after"} {
		t.Run(outcome, func(t *testing.T) {
			trace := fixture(t, "before.jsonl")
			trace.Drain = false
			trace.Steps = []Step{{Actor: "relay"}, {Actor: "view", Faults: []Fault{{Operation: "view-write", Call: 1, Outcome: outcome}}}}
			r := run(t, trace)
			wantNext := int64(1)
			if outcome == "crash-before" {
				wantNext = 0
			}
			if r.Model.View.Next[0] != wantNext || r.Report.Status != "INCOMPLETE" {
				t.Fatal("view commit/progress outcome differs", r.Report)
			}
			if strings.HasPrefix(outcome, "crash-") {
				if r.Report.Attempts[1].Result != "retired" || len(operations(r, 1)) != 1 {
					t.Fatal("crashed view kept processing")
				}
			} else {
				want := []string{"view-write", "view-read"}
				if outcome == sim.UnknownAbort {
					want = append(want, "retry-wait", "view-write")
				}
				if !slices.Equal(operations(r, 1), want) {
					t.Fatal("wrong view recovery decisions", operations(r, 1))
				}
			}
			trace.Drain = true
			checkFixture(t, run(t, trace), "expected-before.json")
		})
	}
}

func TestSC20SC21_UnresolvedReadRetiresWithoutAnotherWrite(t *testing.T) {
	for _, actor := range []string{"relay", "view"} {
		t.Run(actor, func(t *testing.T) {
			trace := fixture(t, "before.jsonl")
			trace.Drain = false
			write, read := "receipt-write", "receipt-read"
			if actor == "view" {
				write, read = "view-write", "view-read"
				trace.Steps = []Step{{Actor: "relay"}}
			}
			faults := []Fault{{Operation: write, Call: 1, Outcome: sim.UnknownCommit}}
			for call := 1; call <= 3; call++ {
				faults = append(faults, Fault{Operation: read, Call: call, Outcome: sim.Abort})
			}
			index := len(trace.Steps)
			trace.Steps = append(trace.Steps, Step{Actor: actor, Faults: faults}, Step{Actor: actor})
			r := run(t, trace)
			ops := operations(r, index)
			if r.Report.Attempts[index].Result != "retired" || !strings.Contains(r.Report.Attempts[index].Error, "unresolved") || r.Report.Attempts[index+1].Result != "parked" || r.Report.LogicalMillis != 300 {
				t.Fatal("unresolved result did not retire", r.Report)
			}
			writes := 0
			for _, op := range ops {
				if op == write {
					writes++
				}
			}
			if writes != 1 {
				t.Fatal("unknown commit repeated a write before resolution")
			}
			trace.Drain = true
			checkFixture(t, run(t, trace), "expected-before.json")
		})
	}
}

func TestSC16SC21_RestartPreservesWithdrawalAgainstOldDelivery(t *testing.T) {
	trace := fixture(t, "before.jsonl", "correction.jsonl", "cancel-last-day.jsonl")
	for i := 0; i < 11; i++ {
		trace.Steps = append(trace.Steps, Step{Actor: "relay"})
	}
	for i := 0; i < 10; i++ {
		trace.Steps = append(trace.Steps, Step{Actor: "view"})
	}
	trace.Steps = append(trace.Steps, Step{Actor: "view", Faults: []Fault{{Operation: "view-write", Call: 1, Outcome: "crash-after"}}}, Step{Actor: "resume-view"}, Step{Actor: "redeliver", Offset: 4}, Step{Actor: "view"})
	r := run(t, trace)
	checkFixture(t, r, "expected-after-cancel.json")
	id := ledger.OutputID{Namespace: trace.Config.Namespace, Family: "balance", Key: trace.Config.Keys[0].StockKey, Day: "2026-01-03"}
	row := r.Model.View.Rows[id]
	if row.Version != 3 || row.Payload.Present || r.Model.View.Next[0] != 12 || r.Model.Output[0][11].Version != 1 {
		t.Fatal("old Day 3 balance escaped the retained version-3 withdrawal")
	}
}

func TestRetryBudgetAndUnusedFaultEvidence(t *testing.T) {
	trace := fixture(t, "before.jsonl")
	trace.Drain = false
	step := Step{Actor: "relay"}
	for call := 1; call <= 3; call++ {
		step.Faults = append(step.Faults, Fault{Operation: "publish", Call: call, Outcome: sim.Abort})
	}
	step.Faults = append(step.Faults, Fault{Operation: "receipt-write", Call: 1, Outcome: sim.UnknownCommit})
	trace.Steps = []Step{step, {Actor: "relay"}}
	r := run(t, trace)
	if r.Report.Status != "INCOMPLETE" || len(r.Model.Output[0]) != 0 || r.Report.LogicalMillis != 300 || r.Report.Attempts[0].FaultsUsed != 3 || r.Report.Attempts[1].Result != "parked" {
		t.Fatal("retry exhaustion or unused fault was misreported", r.Report)
	}
}

func TestTraceReplayValidationAndShrinking(t *testing.T) {
	trace := Generate(17, false)
	raw, err := json.Marshal(trace)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Trace
	if err = json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	a, b := run(t, trace), run(t, decoded)
	if !reflect.DeepEqual(a.Report, b.Report) || !reflect.DeepEqual(a.Model.View, b.Model.View) {
		t.Fatal("replay changed trace observations or final state")
	}
	for _, mutate := range []func(*Trace){func(t *Trace) { t.Version++ }, func(t *Trace) { t.ConfigDigest = "wrong" }, func(t *Trace) { t.Steps = []Step{{Actor: "unknown"}} }, func(t *Trace) {
		t.Steps = []Step{{Actor: "relay", Faults: []Fault{{Operation: "pending", Call: 1, Outcome: sim.UnknownCommit}}}}
	}} {
		candidate := decoded
		mutate(&candidate)
		if _, err := Run(t.Context(), candidate); err == nil {
			t.Fatal("malformed trace accepted")
		}
	}
	distinguished := Fault{Operation: "publish", Call: 1, Outcome: sim.Abort, DelayMillis: 1234}
	trace.Steps = append(trace.Steps, Step{Actor: "relay", Faults: []Fault{distinguished, {Operation: "publish", Call: 2, Outcome: sim.Abort}}})
	original, _ := json.Marshal(trace)
	predicate := func(t Trace) bool {
		for _, s := range t.Steps {
			if slices.Contains(s.Faults, distinguished) {
				return true
			}
		}
		return false
	}
	minimal := Shrink(trace, predicate)
	if len(minimal.Steps) != 1 || len(minimal.Steps[0].Faults) != 1 || !predicate(minimal) || !reflect.DeepEqual(minimal.Source, trace.Source) {
		t.Fatal("shrinker did not preserve the distinguished fault and history")
	}
	after, _ := json.Marshal(trace)
	if string(original) != string(after) {
		t.Fatal("shrinker mutated original")
	}
	run(t, minimal)
}
