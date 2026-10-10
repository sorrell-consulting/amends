// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package ownership

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
	c, err := fixtures.Config("worker-schedule-fixture")
	if err != nil {
		t.Fatal(err)
	}
	source, err := fixtures.Records(c, names...)
	if err != nil {
		t.Fatal(err)
	}
	return Trace{Version: Version, Implementation: Implementation, Toolchain: runtime.Version(), Config: c, ConfigDigest: c.Digest(), Source: source, Drain: true}
}
func run(t *testing.T, trace Trace) *Result {
	t.Helper()
	r, err := Run(t.Context(), trace)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func initialized() []Step {
	return []Step{{Actor: "start"}, Step{Actor: "assign"}, Step{Actor: "seek"}}
}
func operations(r *Result, step, id int) []string {
	var ops []string
	for _, e := range r.Report.Events {
		if e.Step == step && e.Worker == id {
			ops = append(ops, e.Operation)
		}
	}
	return ops
}
func checkFixture(t *testing.T, r *Result, name string) {
	t.Helper()
	want, err := fixtures.Expected(name)
	if err != nil {
		t.Fatal(err)
	}
	got := r.Model.View.Projection(want.Key)
	if r.Report.Status != "PASS" || got.Status != want.Status || !slices.Equal(got.Balances, want.Balances) || !slices.Equal(got.Alerts, want.Alerts) {
		t.Fatalf("fixture mismatch: %+v %+v", r.Report.Boundary, got)
	}
}

func TestSC18_UnknownWorkerCommit(t *testing.T) {
	for _, outcome := range []string{sim.UnknownAbort, sim.UnknownCommit} {
		t.Run(outcome, func(t *testing.T) {
			trace := fixture(t, "before.jsonl")
			trace.Steps = initialized()
			trace.Steps = append(trace.Steps, Step{Actor: "process", Faults: []Fault{{Operation: "process", Call: 1, Outcome: outcome, DelayMillis: 250}}})
			r := run(t, trace)
			checkFixture(t, r, "expected-before.json")
			want := []string{"process", "observe"}
			millis := int64(250)
			if outcome == sim.UnknownAbort {
				want = append(want, "retry-wait", "process")
				millis = 350
			}
			if !slices.Equal(operations(r, 3, 0), want) || r.Report.LogicalMillis != millis || len(r.Model.Store.Snapshot().Outbox) != 5 {
				t.Fatal("unknown commit did not resolve without duplicate output", r.Report)
			}
		})
	}
}

func TestSC19_UnknownClaimRequiresFreshClaim(t *testing.T) {
	for _, outcome := range []string{sim.UnknownAbort, sim.UnknownCommit} {
		t.Run(outcome, func(t *testing.T) {
			trace := fixture(t, "before.jsonl")
			trace.Drain = false
			trace.Steps = []Step{{Actor: "start"}, Step{Actor: "assign", Faults: []Fault{{Operation: "claim", Call: 1, Outcome: outcome}}}, Step{Actor: "seek"}, Step{Actor: "process"}}
			r := run(t, trace)
			wantEpoch := int64(1)
			if outcome == sim.UnknownCommit {
				wantEpoch = 2
			}
			p := r.Model.Store.Snapshot().Partitions[0]
			if p.Epoch != wantEpoch || p.Next != 1 || !slices.Equal(operations(r, 1, 0), []string{"validate", "claim", "observe", "retry-wait", "claim", "observe"}) {
				t.Fatal("unknown claim was adopted or processing repeated", p, r.Report)
			}
		})
	}
}

func TestSC19_BothEpochOrders(t *testing.T) {
	for _, at := range []string{"before", "after"} {
		t.Run(at, func(t *testing.T) {
			trace := fixture(t, "before.jsonl")
			trace.Drain = false
			trace.Steps = initialized()
			replacement := 1
			trace.Steps = append(trace.Steps, Step{Actor: "process", Faults: []Fault{{Operation: "process", Call: 1, Outcome: sim.Commit, Interleave: &Interleave{At: at, Replacement: &replacement}}}})
			r := run(t, trace)
			s := r.Model.Store.Snapshot()
			next := int64(0)
			outputs := 0
			if at == "after" {
				next = 1
				outputs = 3
			}
			if s.Partitions[0].Epoch != 2 || s.Partitions[0].Next != next || len(s.Outbox) != outputs {
				t.Fatal("epoch serial order violated", s.Partitions[0])
			}
			if at == "before" && (r.Report.Attempts[3].Result != "retired" || !strings.Contains(r.Report.Attempts[3].Error, "fence")) {
				t.Fatal("zombie was not fenced")
			}
			trace.Drain = true
			checkFixture(t, run(t, trace), "expected-before.json")
		})
	}
}

func TestSC18SC19_UnknownCommitCannotAdoptReplacement(t *testing.T) {
	trace := fixture(t, "before.jsonl")
	trace.Drain = false
	trace.Steps = initialized()
	replacement := 1
	trace.Steps = append(trace.Steps, Step{Actor: "process", Faults: []Fault{{Operation: "process", Call: 1, Outcome: sim.UnknownCommit, Interleave: &Interleave{At: "after", Replacement: &replacement}}}}, Step{Actor: "process"})
	r := run(t, trace)
	if r.Model.Store.Snapshot().Partitions[0].Next != 1 || r.Report.Attempts[3].Result != "retired" || len(operations(r, 4, 0)) != 0 || !slices.Equal(operations(r, 3, 0), []string{"process", "observe"}) {
		t.Fatal("obsolete owner adopted replacement or repeated work", r.Report)
	}
}

func TestSC19_CanceledCallbacksCannotInstallEpoch(t *testing.T) {
	for _, actor := range []string{"assign", "seek"} {
		for _, at := range []string{"before", "after"} {
			t.Run(actor+"/"+at, func(t *testing.T) {
				trace := fixture(t, "before.jsonl")
				trace.Drain = false
				trace.Steps = []Step{{Actor: "start"}}
				op := "claim"
				if actor == "seek" {
					trace.Steps = append(trace.Steps, Step{Actor: "assign"})
					op = "observe"
				}
				index := len(trace.Steps)
				replacement := 1
				trace.Steps = append(trace.Steps, Step{Actor: actor, Faults: []Fault{{Operation: op, Call: 1, Outcome: sim.Commit, Interleave: &Interleave{At: at, Stop: true, Replacement: &replacement}}}}, Step{Actor: "assign"}, Step{Actor: "seek"}, Step{Actor: "process", Worker: 1})
				r := run(t, trace)
				p := r.Model.Store.Snapshot().Partitions[0]
				wantEpoch := int64(2)
				if actor == "assign" && at == "before" {
					wantEpoch = 1
				}
				if p.Epoch != wantEpoch || p.Next != 1 || len(operations(r, index+1, 0)) != 0 || len(operations(r, index+2, 0)) != 0 || r.Report.Attempts[index].Result != "retired" {
					t.Fatal("canceled callback reclaimed epoch or prevented replacement", p, r.Report)
				}
			})
		}
	}
}

func TestSC19_LateSeekAfterRevokeDoesNotClaim(t *testing.T) {
	trace := fixture(t, "before.jsonl")
	trace.Drain = false
	trace.Steps = initialized()
	trace.Steps = append(trace.Steps, Step{Actor: "revoke"}, Step{Actor: "start", Worker: 1}, Step{Actor: "assign", Worker: 1}, Step{Actor: "seek"}, Step{Actor: "assign"})
	r := run(t, trace)
	if r.Model.Store.Snapshot().Partitions[0].Epoch != 2 || !slices.Equal(operations(r, 6, 0), []string{"validate"}) || len(operations(r, 7, 0)) != 0 || r.Report.Attempts[6].Result != "retired" {
		t.Fatal("late seek reclaimed or adopted ownership", r.Report)
	}
}

func TestSC18SC22_CanceledCommitAndQuarantine(t *testing.T) {
	for _, poison := range []bool{false, true} {
		for _, at := range []string{"before", "after"} {
			t.Run(at+map[bool]string{true: "/quarantine", false: "/business"}[poison], func(t *testing.T) {
				trace := fixture(t, "before.jsonl")
				trace.Source = trace.Source[:1]
				trace.Drain = false
				if poison {
					trace.Source[0].Value = []byte(`{}`)
				}
				trace.Steps = initialized()
				trace.Steps = append(trace.Steps, Step{Actor: "process", Faults: []Fault{{Operation: "process", Call: 1, Outcome: sim.Commit, Interleave: &Interleave{At: at, Stop: true}}}})
				r := run(t, trace)
				s := r.Model.Store.Snapshot()
				next := int64(0)
				count := 0
				if at == "after" {
					next = 1
					count = 1
				}
				status := "INCOMPLETE"
				// A fully committed rejection has no downstream output to drain.
				if poison && at == "after" {
					status = "PASS_WITH_EXCLUSIONS"
				}
				if s.Partitions[0].Next != next || (poison && len(s.Rejections) != count) || (!poison && len(s.Outbox) != count*3) || r.Report.Status != status || r.Report.Attempts[3].Result != "retired" {
					t.Fatal("partial or optimistic canceled commit", s, r.Report)
				}
			})
		}
	}
}

func TestUnresolvedReadsRetireWithoutAnotherWrite(t *testing.T) {
	for _, actor := range []string{"assign", "process"} {
		t.Run(actor, func(t *testing.T) {
			trace := fixture(t, "before.jsonl")
			trace.Steps = []Step{{Actor: "start"}}
			op := "claim"
			if actor == "process" {
				trace.Steps = initialized()
				op = "process"
			}
			index := len(trace.Steps)
			faults := []Fault{{Operation: op, Call: 1, Outcome: sim.UnknownCommit}}
			for call := 1; call <= 3; call++ {
				faults = append(faults, Fault{Operation: "observe", Call: call, Outcome: sim.Abort})
			}
			trace.Steps = append(trace.Steps, Step{Actor: actor, Faults: faults}, Step{Actor: "assign"})
			r := run(t, trace)
			checkFixture(t, r, "expected-before.json")
			writes := 0
			for _, operation := range operations(r, index, 0) {
				if operation == op {
					writes++
				}
			}
			if writes != 1 || r.Report.LogicalMillis != 300 || !strings.Contains(r.Report.Attempts[index].Error, "unresolved") || len(operations(r, index+1, 0)) != 0 {
				t.Fatal("unresolved operation kept writing", r.Report)
			}
		})
	}
}

func TestSC18_SeekUsesDurableCoordinatesAcrossGap(t *testing.T) {
	trace := fixture(t, "before.jsonl")
	trace.Source = trace.Source[:2]
	trace.Source[1].Offset = 4
	trace.Drain = false
	trace.Steps = initialized()
	trace.Steps = append(trace.Steps, Step{Actor: "process"}, Step{Actor: "stop"}, Step{Actor: "start", Worker: 1}, Step{Actor: "assign", Worker: 1}, Step{Actor: "seek", Worker: 1}, Step{Actor: "process", Worker: 1})
	r := run(t, trace)
	if r.Model.Store.Snapshot().Partitions[0].Next != 5 {
		t.Fatal("record count substituted for source coordinate")
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
	if !reflect.DeepEqual(a.Report, b.Report) {
		t.Fatal("replay changed observations")
	}
	for _, mutate := range []func(*Trace){func(t *Trace) { t.Version++ }, func(t *Trace) { t.ConfigDigest = "wrong" }, func(t *Trace) { t.Steps = []Step{{Actor: "assign"}} }, func(t *Trace) { t.Steps = []Step{{Actor: "start"}, Step{Actor: "start"}} }} {
		c := decoded
		mutate(&c)
		if _, err := Run(t.Context(), c); err == nil {
			t.Fatal("malformed trace accepted")
		}
	}
	trace = fixture(t, "before.jsonl")
	trace.Steps = initialized()
	marker := Fault{Operation: "process", Call: 1, Outcome: sim.Abort, DelayMillis: 1234}
	trace.Steps = append(trace.Steps, Step{Actor: "process", Faults: []Fault{marker}})
	before, _ := json.Marshal(trace)
	predicate := func(c Trace) bool {
		for _, s := range c.Steps {
			for _, f := range s.Faults {
				if f.DelayMillis == 1234 {
					return true
				}
			}
		}
		return false
	}
	reduced := Shrink(trace, predicate)
	after, _ := json.Marshal(trace)
	if len(reduced.Steps) != 2 || reduced.Steps[0].Actor != "start" || !predicate(reduced) || string(before) != string(after) || !reflect.DeepEqual(reduced.Source, trace.Source) {
		t.Fatal("reducer lost lifetime dependencies or mutated input")
	}
	run(t, reduced)
}

func TestSC18_RecoveryPreservesCorrectionAndWithdrawal(t *testing.T) {
	trace := fixture(t, "before.jsonl", "correction.jsonl", "cancel-last-day.jsonl")
	trace.Steps = initialized()
	for range 3 {
		trace.Steps = append(trace.Steps, Step{Actor: "process"})
	}
	trace.Steps = append(trace.Steps, Step{Actor: "process", Faults: []Fault{{Operation: "process", Call: 1, Outcome: sim.UnknownCommit, Interleave: &Interleave{At: "after", Stop: true}}}})
	r := run(t, trace)
	checkFixture(t, r, "expected-after-cancel.json")
	s := r.Model.Store.Snapshot()
	last := s.Outbox[len(s.Outbox)-1].Envelope
	if len(s.Outbox) != 11 || last.Version != 3 || last.Payload.Present || last.ID.Day != "2026-01-03" {
		t.Fatal("recovery changed fixed withdrawal history", last)
	}
}

func TestExhaustedWritePreservesFrontierAndReportsUnusedFault(t *testing.T) {
	trace := fixture(t, "before.jsonl")
	trace.Drain = false
	trace.Steps = initialized()
	faults := []Fault{}
	for call := 1; call <= 3; call++ {
		faults = append(faults, Fault{Operation: "process", Call: call, Outcome: sim.Abort})
	}
	faults = append(faults, Fault{Operation: "observe", Call: 1, Outcome: sim.Abort})
	trace.Steps = append(trace.Steps, Step{Actor: "process", Faults: faults}, Step{Actor: "process"})
	r := run(t, trace)
	s := r.Model.Store.Snapshot()
	if s.Partitions[0].Next != 0 || len(s.Outbox) != 0 || r.Report.Status != "INCOMPLETE" || r.Report.LogicalMillis != 300 || r.Report.Attempts[3].FaultsUsed != 3 || len(operations(r, 4, 0)) != 0 {
		t.Fatal("exhaustion advanced state or counted unused choice", r.Report)
	}
	trace.Drain = true
	checkFixture(t, run(t, trace), "expected-before.json")
}

func TestSC08SC25_IncompleteThenBlocked(t *testing.T) {
	trace := fixture(t, "before.jsonl")
	conflict := trace.Source[0].Clone()
	txn, decodeErr := ledger.Decode(conflict.Value)
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	txn.Qty = -49 // distinct revision-1 Day 1 assertion, with no higher repair
	raw, err := json.Marshal(txn)
	if err != nil {
		t.Fatal(err)
	}
	conflict.Value = raw
	conflict.Offset = int64(len(trace.Source))
	trace.Source = append(trace.Source, conflict)
	trace.Steps = initialized()
	trace.Drain = false
	a := run(t, trace)
	trace.Drain = true
	b := run(t, trace)
	if a.Report.Status != "INCOMPLETE" || b.Report.Status != "BLOCKED" || len(a.Report.Boundary.Conflicts) != 1 || len(b.Report.Boundary.Conflicts) != 1 {
		t.Fatal("incomplete/conflict precedence lost", a.Report.Boundary, b.Report.Boundary)
	}
}
