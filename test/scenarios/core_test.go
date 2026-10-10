// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package scenarios_test

import (
	"encoding/json"
	"math"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/diff"
	"github.com/sorrell-consulting/amends/internal/fold"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/oracle"
	"github.com/sorrell-consulting/amends/internal/store/memory"
	"github.com/sorrell-consulting/amends/internal/view"
	"github.com/sorrell-consulting/amends/sim"
)

var key = ledger.StockKey{Item: "PART", Location: "DEPOT"}
var other = ledger.StockKey{Item: "OTHER", Location: "DEPOT"}
var origin = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func config() ledger.Config {
	return ledger.Config{SchemaVersion: 1, Namespace: "scenario", Source: "source-v1", Partitions: 2, Origin: origin, Keys: []ledger.KeyConfig{{StockKey: key, Opening: 100, Threshold: 60, Partition: 0}, {StockKey: other, Opening: 100, Threshold: 60, Partition: 0}}}
}
func txn(id string, revision int64, kind ledger.TransactionKind, day int, qty int64) ledger.Transaction {
	return ledger.Transaction{TxnID: id, Revision: revision, Kind: kind, StockKey: key, ValidTime: origin.AddDate(0, 0, day-1).Add(12 * time.Hour), Qty: qty, SourceTime: origin.AddDate(0, 0, 20)}
}
func records(t *testing.T, c ledger.Config, txns ...ledger.Transaction) []ledger.Record {
	t.Helper()
	var rs []ledger.Record
	positions := map[int]int64{}
	for _, tx := range txns {
		raw, err := json.Marshal(tx)
		if err != nil {
			t.Fatal(err)
		}
		k, _ := c.Key(tx.StockKey)
		rs = append(rs, ledger.Record{Coordinates: ledger.Coordinates{Source: c.Source, Partition: k.Partition, Offset: positions[k.Partition]}, Key: tx.StockKey, Value: raw})
		positions[k.Partition]++
	}
	return rs
}
func model(t *testing.T, c ledger.Config, rs []ledger.Record) *sim.Model {
	t.Helper()
	m, err := sim.New(c, rs)
	if err != nil {
		t.Fatal(err)
	}
	return m
}
func drain(t *testing.T, m *sim.Model) {
	t.Helper()
	if err := m.Drain(); err != nil {
		t.Fatal(err)
	}
	if err := m.Safety(); err != nil {
		t.Fatal(err)
	}
}
func step(t *testing.T, m *sim.Model, actor, outcome string) {
	t.Helper()
	if err := m.Step(sim.Step{Actor: actor, Partition: 0, Outcome: outcome}); err != nil {
		t.Fatal(err)
	}
}
func projection(t *testing.T, got ledger.Projection, days []int, closes []int64, alertDays []int) {
	t.Helper()
	var balances []ledger.Balance
	var alerts []ledger.Alert
	for i, day := range days {
		date := origin.AddDate(0, 0, day-1).Format("2006-01-02")
		balances = append(balances, ledger.Balance{Day: date, Close: closes[i]})
		if slices.Contains(alertDays, day) {
			alerts = append(alerts, ledger.Alert{Day: date, Threshold: 60, Close: closes[i]})
		}
	}
	if got.Status != "READY" || !slices.Equal(got.Balances, balances) || !slices.Equal(got.Alerts, alerts) {
		t.Fatalf("[amends:fixture-projection] projection = %+v; want balances %+v alerts %+v READY", got, balances, alerts)
	}
}

// SC01–SC15, SC26: hand-derived S3–S7 business expectations, separately checked
// against the oracle and incremental/view path. No expected values come from
// either implementation.
func TestHandDerivedScenarios(t *testing.T) {
	cases := []struct {
		name    string
		input   []ledger.Transaction
		opening int64
		days    []int
		closes  []int64
		alerts  []int
	}{
		{
			name: "SC01_BackdatedNEW",
			input: []ledger.Transaction{
				txn("receipt", 1, "NEW", 3, 50),
				txn("issue", 1, "NEW", 1, -50),
				txn("later", 1, "NEW", 2, -20),
			},
			opening: 100,
			days:    []int{1, 2, 3},
			closes:  []int64{50, 30, 80},
			alerts:  []int{1},
		},
		{
			name: "SC03_MoveLater",
			input: []ledger.Transaction{
				txn("issue", 1, "NEW", 1, -50),
				txn("receipt", 1, "NEW", 3, 50),
				txn("issue", 2, "AMEND", 2, -50),
			},
			opening: 100,
			days:    []int{2, 3},
			closes:  []int64{50, 100},
			alerts:  []int{2},
		},
		{
			name: "SC03_MoveEarlier",
			input: []ledger.Transaction{
				txn("issue", 1, "NEW", 3, -50),
				txn("receipt", 1, "NEW", 2, 10),
				txn("issue", 2, "AMEND", 1, -50),
			},
			opening: 100,
			days:    []int{1, 2},
			closes:  []int64{50, 60},
			alerts:  []int{1},
		},
		{
			name: "SC04_CancelBeforeNew",
			input: []ledger.Transaction{
				txn("issue", 3, "CANCEL", 9, 0),
				txn("issue", 1, "NEW", 1, -50),
				txn("issue", 2, "AMEND", 2, -70),
			},
			opening: 100,
			days:    nil,
			closes:  nil,
			alerts:  nil,
		},
		{
			name: "SC04_CancelAfterNew",
			input: []ledger.Transaction{
				txn("issue", 1, "NEW", 1, -50),
				txn("later", 1, "NEW", 2, -20),
				txn("issue", 2, "CANCEL", 9, 0),
			},
			opening: 100,
			days:    []int{2},
			closes:  []int64{80},
			alerts:  nil,
		},
		{
			name: "SC05_Reactivation",
			input: []ledger.Transaction{
				txn("issue", 1, "NEW", 1, -50),
				txn("issue", 2, "CANCEL", 9, 0),
				txn("issue", 3, "AMEND", 1, -40),
			},
			opening: 100,
			days:    []int{1},
			closes:  []int64{60},
			alerts:  nil,
		},
		{
			name: "SC06_StaleLast",
			input: []ledger.Transaction{
				txn("issue", 3, "AMEND", 1, -30),
				txn("issue", 2, "AMEND", 1, -80),
				txn("issue", 1, "NEW", 1, -90),
			},
			opening: 100,
			days:    []int{1},
			closes:  []int64{70},
			alerts:  nil,
		},
		{
			name: "SC11_DailyNotIntraday",
			input: []ledger.Transaction{
				txn("issue", 1, "NEW", 1, -80),
				txn("receipt", 1, "NEW", 1, 100),
			},
			opening: 100,
			days:    []int{1},
			closes:  []int64{120},
			alerts:  nil,
		},
		{
			name: "SC11_EqualTimePermutation",
			input: []ledger.Transaction{
				txn("receipt", 1, "NEW", 1, 100),
				txn("issue", 1, "NEW", 1, -80),
			},
			opening: 100,
			days:    []int{1},
			closes:  []int64{120},
			alerts:  nil,
		},
		{
			name: "SC12_OpeningAlreadyLow",
			input: []ledger.Transaction{
				txn("issue", 1, "NEW", 1, -10),
			},
			opening: 40,
			days:    []int{1},
			closes:  []int64{30},
			alerts:  nil,
		},
		{
			name: "SC12_ThresholdEquality",
			input: []ledger.Transaction{
				txn("zero", 1, "NEW", 1, 0),
				txn("issue", 1, "NEW", 2, -1),
			},
			opening: 60,
			days:    []int{1, 2},
			closes:  []int64{60, 59},
			alerts:  []int{2},
		},
		{
			name: "SC13_SparseZeroActivity",
			input: []ledger.Transaction{
				txn("issue", 1, "NEW", 1, -40),
				txn("zero", 1, "NEW", 5, 0),
				txn("late", 1, "NEW", 9, -1),
			},
			opening: 100,
			days:    []int{1, 5, 9},
			closes:  []int64{60, 60, 59},
			alerts:  []int{9},
		},
		{
			name: "SC14_DisappearingMiddleDay",
			input: []ledger.Transaction{
				txn("issue", 1, "NEW", 1, -40),
				txn("zero", 1, "NEW", 5, 0),
				txn("late", 1, "NEW", 9, -1),
				txn("zero", 2, "CANCEL", 5, 0),
			},
			opening: 100,
			days:    []int{1, 9},
			closes:  []int64{60, 59},
			alerts:  []int{9},
		},
		{
			name: "SC15_RestatedHistoricalAlert",
			input: []ledger.Transaction{
				txn("issue", 1, "NEW", 1, -50),
				txn("issue", 2, "AMEND", 1, -60),
				txn("receipt", 1, "NEW", 2, 80),
			},
			opening: 100,
			days:    []int{1, 2},
			closes:  []int64{40, 120},
			alerts:  []int{1},
		},
		{
			name: "SC15_RetractedAlert",
			input: []ledger.Transaction{
				txn("issue", 1, "NEW", 1, -50),
				txn("issue", 2, "AMEND", 1, -30),
			},
			opening: 100,
			days:    []int{1},
			closes:  []int64{70},
			alerts:  nil,
		},
		{
			name: "SC26_NoOpSameDay",
			input: []ledger.Transaction{
				txn("issue", 1, "NEW", 1, -30),
				func() ledger.Transaction {
					x := txn("issue", 2, "AMEND", 1, -30)
					x.ValidTime = x.ValidTime.Add(time.Hour)
					return x
				}(),
			},
			opening: 100,
			days:    []int{1},
			closes:  []int64{70},
			alerts:  nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := config()
			c.Keys[0].Opening = tc.opening
			rs := records(t, c, tc.input...)
			expected, err := oracle.Compute(c, rs)
			if err != nil {
				t.Fatal(err)
			}
			projection(t, expected.Keys[0], tc.days, tc.closes, tc.alerts)
			m := model(t, c, rs)
			drain(t, m)
			projection(t, m.View.Projection(key), tc.days, tc.closes, tc.alerts)
			if got := m.Check().Status; got != "PASS" {
				t.Fatalf("model status %s", got)
			}
		})
	}
}

func TestSC02SC14_FixtureStages(t *testing.T) {
	c, err := fixtures.Config("fixture-test")
	if err != nil {
		t.Fatal(err)
	}
	names := []string{"before.jsonl", "correction.jsonl", "cancel-last-day.jsonl"}
	expectedNames := []string{"expected-before.json", "expected-after.json", "expected-after-cancel.json"}
	for stage := range names {
		rs, err := fixtures.Records(c, names[:stage+1]...)
		if err != nil {
			t.Fatal(err)
		}
		want, err := fixtures.Expected(expectedNames[stage])
		if err != nil {
			t.Fatal(err)
		}
		batch, err := oracle.Compute(c, rs)
		if err != nil {
			t.Fatal(err)
		}
		m := model(t, c, rs)
		drain(t, m)
		for name, got := range map[string]ledger.Projection{"oracle": batch.Keys[0], "incremental view": m.View.Projection(c.Keys[0].StockKey)} {
			if !slices.Equal(got.Balances, want.Balances) || !slices.Equal(got.Alerts, want.Alerts) || got.Status != want.Status {
				t.Fatalf("stage %d %s got %+v want %+v", stage, name, got, want)
			}
		}
	}
}

func TestSC07SC26_NoSemanticVersions(t *testing.T) {
	c := config()
	first := txn("issue", 1, "NEW", 1, -50)
	duplicate := first
	duplicate.SourceTime = duplicate.SourceTime.Add(time.Hour)
	duplicate.ValidTime = duplicate.ValidTime.In(time.FixedZone("offset", -6*60*60))
	noop := first
	noop.Revision = 2
	noop.Kind = "AMEND"
	noop.ValidTime = noop.ValidTime.Add(time.Hour)
	m := model(t, c, records(t, c, first, duplicate, noop))
	step(t, m, "worker", sim.Commit)
	before := m.Store.Snapshot()
	drain(t, m)
	after := m.Store.Snapshot()
	if !reflect.DeepEqual(before.Outputs, after.Outputs) {
		t.Fatal("duplicate/no-op restated output")
	}
	if after.Keys[key].Identities["issue"].Highest != 2 {
		t.Fatal("no-op failed to advance authority")
	}
	if !fold.CanonicalEqual(first, duplicate) {
		t.Fatal("offset or informational time changed equality")
	}
	changed := first
	changed.Kind = "AMEND"
	if fold.CanonicalEqual(first, changed) {
		t.Fatal("kind excluded from canonical payload")
	}
}

func TestSC08_HighestConflictBothOrders(t *testing.T) {
	for _, quantities := range [][]int64{{-50, -30}, {-30, -50}} {
		c := config()
		m := model(t, c, records(t, c, txn("issue", 1, "NEW", 1, quantities[0]), txn("issue", 1, "NEW", 1, quantities[1])))
		drain(t, m)
		got := m.View.Projection(key)
		if got.Status != "BLOCKED" || len(got.Reasons) != 1 || got.Balances[0].Close != 100+quantities[0] {
			t.Fatalf("conflict not visibly frozen: %+v", got)
		}
		r := m.Check()
		if r.Status != "BLOCKED" || r.Accepts(nil, nil) {
			t.Fatalf("conflict certified: %+v", r)
		}
	}
}

func TestSC09SC10_ResolutionAccumulatesAndOtherKeysProceed(t *testing.T) {
	c := config()
	otherTxn := txn("other", 1, "NEW", 1, -80)
	otherTxn.StockKey = other
	input := []ledger.Transaction{txn("issue", 1, "NEW", 1, -50), txn("issue", 1, "NEW", 1, -30), txn("later", 1, "NEW", 2, -20), otherTxn, txn("issue", 2, "AMEND", 1, -30)}
	m := model(t, c, records(t, c, input...))
	for i := 0; i < 4; i++ {
		step(t, m, "worker", sim.Commit)
	}
	s := m.Store.Snapshot()
	if s.Keys[key].Projection.Status != "BLOCKED" || s.Keys[key].Projection.Balances[0].Close != 50 || len(s.Keys[key].Projection.Balances) != 1 {
		t.Fatal("blocked business state changed")
	}
	projection(t, s.Keys[other].Projection, []int{1}, []int64{20}, []int{1})
	before := len(s.Outbox)
	step(t, m, "worker", sim.Commit)
	repair := m.Store.Snapshot().Outbox[before:]
	if len(repair) < 2 || repair[len(repair)-1].Envelope.Payload.Status != "READY" {
		t.Fatal("READY did not follow business repair")
	}
	drain(t, m)
	projection(t, m.View.Projection(key), []int{1, 2}, []int64{70, 50}, []int{2})
	r := m.Check()
	if r.Status != "PASS_WITH_EXCLUSIONS" || r.Accepts(nil, nil) || !r.Accepts(nil, r.Conflicts) {
		t.Fatalf("lost qualification: %+v", r)
	}
	// A conflict discovered only below a higher revision never blocks the key.
	m = model(t, c, records(t, c, txn("issue", 3, "AMEND", 1, -10), input[0], input[1]))
	drain(t, m)
	projection(t, m.View.Projection(key), []int{1}, []int64{90}, nil)
	if m.Check().Status != "PASS_WITH_EXCLUSIONS" {
		t.Fatal("historical conflict lost")
	}
}

func TestSC05SC16_WithdrawalAndReappearance(t *testing.T) {
	c := config()
	m := model(t, c, records(t, c, txn("issue", 1, "NEW", 1, -50), txn("issue", 2, "CANCEL", 9, 0), txn("issue", 3, "NEW", 1, -50)))
	drain(t, m)
	id := ledger.OutputID{Namespace: c.Namespace, Family: "balance", Key: key, Day: "2026-01-01"}
	var revisions []ledger.Envelope
	for _, row := range m.Store.Snapshot().Outbox {
		if row.Envelope.ID == id {
			revisions = append(revisions, row.Envelope)
		}
	}
	if len(revisions) != 3 || revisions[1].Type != "BalanceRetracted" || revisions[2].Version != 3 {
		t.Fatalf("lost withdrawal/version: %+v", revisions)
	}
	v := view.New(c)
	if err := v.Apply(0, 0, 0, revisions[1]); err != nil {
		t.Fatal(err)
	}
	v = v.Clone() // modeled restart retains the deletion guard
	if err := v.Apply(0, 1, 1, revisions[0]); err != nil {
		t.Fatal(err)
	}
	if len(v.Projection(key).Balances) != 0 || v.Rows[id].Version != 2 || v.Next[0] != 2 {
		t.Fatal("[amends:retained-deletion] old output resurrected deletion")
	}
	if err := v.Apply(0, 2, 2, revisions[2]); err != nil {
		t.Fatal(err)
	}
	if v.Rows[id].Version != 3 || len(v.Projection(key).Balances) != 1 {
		t.Fatal("legitimate reappearance lost")
	}
}

func TestSC17_EqualVersionProtocolConflict(t *testing.T) {
	c := config()
	m := model(t, c, records(t, c, txn("issue", 1, "NEW", 1, -50)))
	drain(t, m)
	original := m.Store.Snapshot().Outbox[0].Envelope
	for _, changeCause := range []bool{false, true} {
		v := view.New(c)
		if err := v.Apply(0, 0, 0, original); err != nil {
			t.Fatal(err)
		}
		if err := v.Apply(0, 1, 1, original); err != nil {
			t.Fatal(err)
		}
		conflict := original
		if changeCause {
			conflict.Cause.Offset++
		} else {
			conflict.Payload.Close--
		}
		if err := v.Apply(0, 2, 2, conflict); err == nil {
			t.Fatal("[amends:equal-version-conflict] accepted conflicting same version")
		}
		if v.Next[0] != 2 || !v.Rows[original.ID].Equal(original) || v.Errors[0] == "" {
			t.Fatal("protocol error changed view or lost diagnostics")
		}
		v = v.Clone()
		if err := v.Apply(0, 2, 2, original); err == nil {
			t.Fatal("stopped partition silently resumed")
		}
	}
}

func TestSC18SC20SC21_CommitAndAcknowledgementOutcomes(t *testing.T) {
	for _, actor := range []string{"worker", "relay", "view"} {
		for _, outcome := range []string{sim.Abort, sim.UnknownAbort, sim.UnknownCommit, sim.Commit} {
			t.Run(actor+"/"+outcome, func(t *testing.T) {
				c := config()
				m := model(t, c, records(t, c, txn("issue", 1, "NEW", 1, -50)))
				if actor != "worker" {
					step(t, m, "worker", sim.Commit)
				}
				if actor == "view" {
					step(t, m, "relay", sim.Commit)
				}
				before := m.Store.Snapshot()
				beforeView := m.View.Clone()
				step(t, m, actor, outcome)
				committed := outcome == sim.Commit || outcome == sim.UnknownCommit
				if actor == "worker" {
					if committed != (m.Store.Snapshot().Partitions[0].Next == 1) {
						t.Fatal("worker commit/progress split")
					}
					if !committed && !reflect.DeepEqual(before, m.Store.Snapshot()) {
						t.Fatal("aborted worker changed state")
					}
				}
				if actor == "relay" {
					if committed != (len(m.Output[0]) == 1) {
						t.Fatal("publication outcome not modeled")
					}
					if outcome == sim.UnknownCommit {
						pending, ok := m.Store.Pending(0)
						if !ok || !pending.Envelope.Equal(m.Output[0][0]) {
							t.Fatal("unknown publish lost unchanged retry")
						}
					}
				}
				if actor == "view" && !committed && !reflect.DeepEqual(beforeView, m.View) {
					t.Fatal("aborted view changed state/frontier")
				}
				if outcome == sim.UnknownAbort || outcome == sim.UnknownCommit {
					if m.Events[len(m.Events)-1].Observation != "unknown" {
						t.Fatal("unknown outcome reported as success/failure")
					}
				}
				drain(t, m)
				if m.Check().Status != "PASS" {
					t.Fatal(m.Check())
				}
				if m.Store.Snapshot().Partitions[0].Next != 1 {
					t.Fatal("recovery duplicated progress")
				}
				for _, e := range m.Store.Snapshot().Outputs {
					if e.Version != 1 {
						t.Fatal("recovery allocated new semantic version")
					}
				}
			})
		}
	}
}

func TestSC19_OwnershipReplacementOrders(t *testing.T) {
	for _, oldFirst := range []bool{false, true} {
		c := config()
		s, err := memory.New(c)
		if err != nil {
			t.Fatal(err)
		}
		old, err := s.Claim(0)
		if err != nil {
			t.Fatal(err)
		}
		r := records(t, c, txn("issue", 1, "NEW", 1, -50))[0]
		d, err := s.Prepare(old, 0, r)
		if err != nil {
			t.Fatal(err)
		}
		if oldFirst {
			if err := s.Commit(d); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.Claim(0); err != nil {
			t.Fatal(err)
		}
		before := s.Snapshot()
		if err := s.Commit(d); err == nil {
			t.Fatal("zombie committed after new epoch")
		}
		if !reflect.DeepEqual(before, s.Snapshot()) {
			t.Fatal("stale commit partially changed state")
		}
		if _, err := s.Prepare(old, before.Partitions[0].Next, r); err == nil {
			t.Fatal("obsolete assignment prepared work")
		}
	}
}

func TestSC22SC28_QuarantineAndRouting(t *testing.T) {
	c := config()
	valid := records(t, c, txn("issue", 1, "NEW", 1, -50))[0]
	cases := []struct {
		name, reason string
		edit         func(*ledger.Record)
	}{
		{"malformed", "MALFORMED", func(r *ledger.Record) { r.Value = []byte(`{"qty":`) }},
		{"missing", "MALFORMED", func(r *ledger.Record) { r.Value = []byte(`{}`) }},
		{"key mismatch", "KEY_MISMATCH", func(r *ledger.Record) { r.Key = other }},
		{"routing", "ROUTING", func(r *ledger.Record) { r.Partition = 1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := valid.Clone()
			tc.edit(&r)
			m := model(t, c, []ledger.Record{r})
			p := r.Partition
			if err := m.Step(sim.Step{Actor: "worker", Partition: p, Outcome: sim.UnknownAbort}); err != nil {
				t.Fatal(err)
			}
			if len(m.Store.Snapshot().Rejections) != 0 || m.Store.Snapshot().Partitions[p].Next != 0 {
				t.Fatal("aborted quarantine persisted")
			}
			drain(t, m)
			s := m.Store.Snapshot()
			if len(s.Rejections) != 1 || s.Rejections[0].Reason != tc.reason || s.Partitions[p].Next != 1 {
				t.Fatalf("bad quarantine: %+v", s.Rejections)
			}
			report := m.Check()
			if report.Status != "PASS_WITH_EXCLUSIONS" || report.Accepts(nil, nil) || !report.Accepts(s.Rejections, nil) {
				t.Fatal("qualification/manifest error")
			}
			wrong := s.Rejections
			wrong[0].Reason = "invented"
			if report.Accepts(wrong, nil) {
				t.Fatal("blanket quarantine suppression")
			}
		})
	}
}

func TestSC23_ReplayIntactAndRejectRanges(t *testing.T) {
	c := config()
	m := model(t, c, records(t, c, txn("issue", 1, "NEW", 1, -50), txn("issue", 2, "CANCEL", 9, 0)))
	drain(t, m)
	before := m.Store.Snapshot()
	if err := m.Replay(0, 0, 2); err != nil {
		t.Fatal(err)
	}
	after := m.Store.Snapshot()
	if !reflect.DeepEqual(before.Outputs, after.Outputs) || !reflect.DeepEqual(before.Keys, after.Keys) || after.Partitions[0].Next != 2 {
		t.Fatal("replay changed semantic state/frontier")
	}
	for _, span := range [][2]int64{{-1, 2}, {0, 3}, {2, 1}} {
		before = m.Store.Snapshot()
		if err := m.Replay(0, span[0], span[1]); err == nil {
			t.Fatal("unsupported replay accepted")
		}
		if !reflect.DeepEqual(before, m.Store.Snapshot()) {
			t.Fatal("invalid range mutated state")
		}
	}
	m.Retained[0] = 1
	if err := m.Replay(0, 0, 2); err == nil {
		t.Fatal("missing replay history accepted")
	}
}

func TestSC24_IsolatedRebuild(t *testing.T) {
	c := config()
	source := records(t, c, txn("issue", 1, "NEW", 1, -50), txn("issue", 2, "AMEND", 1, -30))
	old := model(t, c, source)
	drain(t, old)
	before := old.Store.Snapshot()
	c.Namespace = "rebuild"
	fresh := model(t, c, source)
	drain(t, fresh)
	if !reflect.DeepEqual(old.View.Projection(key), fresh.View.Projection(key)) {
		t.Fatal("rebuild business values differ")
	}
	if !reflect.DeepEqual(before, old.Store.Snapshot()) {
		t.Fatal("rebuild touched old state")
	}
	e := before.Outbox[0].Envelope
	v := fresh.View.Clone()
	next := v.Next[0]
	if err := v.Apply(0, next, next, e); err == nil {
		t.Fatal("old namespace envelope accepted")
	}
	if v.Next[0] != next {
		t.Fatal("foreign envelope advanced progress")
	}
}

func TestSC25_IncompleteBeforeBlockedAndDrift(t *testing.T) {
	c := config()
	m := model(t, c, records(t, c, txn("issue", 1, "NEW", 1, -50), txn("issue", 1, "NEW", 1, -30)))
	step(t, m, "worker", sim.Commit)
	step(t, m, "worker", sim.Commit)
	r := m.Check()
	if r.Status != "INCOMPLETE" || len(r.Conflicts) != 1 || m.Store.Snapshot().Keys[key].Projection.Status != "BLOCKED" {
		t.Fatalf("invalid precedence: %+v", r)
	}
	drain(t, m)
	if m.Check().Status != "BLOCKED" {
		t.Fatal("drain invented authority")
	}
	m = model(t, c, records(t, c, txn("issue", 1, "NEW", 1, -50)))
	drain(t, m)
	m.Retained[0] = 1
	if m.Check().Status != "INCOMPLETE" {
		t.Fatal("missing history certified")
	}
	m.Retained[0] = 0
	next := records(t, c, txn("later", 1, "NEW", 2, -10))[0]
	next.Offset = 1
	m.Source = append(m.Source, next)
	if m.Check().Status != "INCOMPLETE" {
		t.Fatal("advancing source certified")
	}
	drain(t, m)
	for id, e := range m.View.Rows {
		if id.Family == "balance" {
			e.Payload.Close++
			m.View.Rows[id] = e
			break
		}
	}
	if m.Check().Status != "DRIFT" {
		t.Fatal("business mismatch not detected")
	}
}

func TestSC27_ExactArithmeticAndAtomicOverflow(t *testing.T) {
	c := config()
	c.Keys[0].Opening = math.MaxInt64
	m := model(t, c, records(t, c, txn("overflow", 1, "NEW", 1, 1)))
	before := m.Store.Snapshot()
	if err := m.Step(sim.Step{Actor: "worker", Partition: 0, Outcome: sim.Commit}); err == nil {
		t.Fatal("overflow accepted")
	}
	if !reflect.DeepEqual(before, m.Store.Snapshot()) {
		t.Fatal("overflow partially committed")
	}
	if _, err := oracle.Compute(c, m.Source); err == nil {
		t.Fatal("oracle accepted out-of-domain close")
	}
	// Exact day sum is representable even when a naive intraday accumulator is not.
	c = config()
	c.Keys[0].Opening = 0
	input := []ledger.Transaction{txn("a", 1, "NEW", 1, math.MaxInt64), txn("c", 1, "NEW", 1, -math.MaxInt64), txn("b", 1, "NEW", 1, math.MaxInt64)}
	m = model(t, c, records(t, c, input...))
	drain(t, m)
	projection(t, m.View.Projection(key), []int{1}, []int64{math.MaxInt64}, nil)
	id := ledger.OutputID{Namespace: c.Namespace, Family: "balance", Key: key, Day: "2026-01-01"}
	previous := map[ledger.OutputID]ledger.Envelope{id: {ID: id, Version: math.MaxInt64, Payload: ledger.Payload{Present: true, Close: 0}}}
	want := ledger.Projection{Key: key, Status: "READY", Balances: []ledger.Balance{{Day: id.Day, Close: 1}}}
	if _, err := diff.Reconcile(c.Namespace, previous, want, ledger.Cause{}); err == nil {
		t.Fatal("version wrapped")
	}
	if previous[id].Version != math.MaxInt64 {
		t.Fatal("version input mutated")
	}
}

func TestSC28_IncompatibleResume(t *testing.T) {
	c := config()
	s, err := memory.New(c)
	if err != nil {
		t.Fatal(err)
	}
	token, err := s.Claim(0)
	if err != nil {
		t.Fatal(err)
	}
	r := records(t, c, txn("issue", 1, "NEW", 1, -50))[0]
	bad := token
	bad.Digest = "changed-config"
	if _, err := s.Prepare(bad, 0, r); err == nil {
		t.Fatal("changed configuration accepted")
	}
	r.Source = "recreated-topic"
	if _, err := s.Prepare(token, 0, r); err == nil {
		t.Fatal("topic recreation accepted")
	}
}
