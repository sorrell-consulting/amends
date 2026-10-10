// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Package sim supplies a bounded memory scheduler, not a Kafka/Postgres runtime.
package sim

import (
	"fmt"
	"math"
	"reflect"
	"slices"

	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/oracle"
	"github.com/sorrell-consulting/amends/internal/store/memory"
	"github.com/sorrell-consulting/amends/internal/view"
)

// Outcome separates durable effects from caller observation. UnknownCommit
// installs the decision; UnknownAbort does not. Neither is optimistic success.
const (
	Commit        = "commit"
	Abort         = "abort"
	UnknownCommit = "unknown-committed"
	UnknownAbort  = "unknown-aborted"
)

type Step struct {
	Actor     string `json:"actor"`
	Partition int    `json:"partition"`
	Outcome   string `json:"outcome"`
}
type Event struct {
	Step        Step   `json:"step"`
	Observation string `json:"observation"`
	Tick        int64  `json:"tick"`
}
type Model struct {
	Config   ledger.Config
	Source   []ledger.Record
	Store    *memory.Store
	View     view.State
	Output   map[int][]ledger.Envelope
	Tokens   map[int]memory.Token
	Events   []Event
	Retained map[int]int64
	Now      int64
	retries  map[string]Retry
}

func New(c ledger.Config, source []ledger.Record) (*Model, error) {
	m, err := NewUnclaimed(c, source)
	if err != nil {
		return nil, err
	}
	for p := 0; p < c.Partitions; p++ {
		token, err := m.Store.Claim(p)
		if err != nil {
			return nil, err
		}
		m.Tokens[p] = token
	}
	return m, nil
}

// NewUnclaimed leaves epochs at zero for schedules driving actual assignment
// orchestration. New claims each partition before returning the model.
func NewUnclaimed(c ledger.Config, source []ledger.Record) (*Model, error) {
	s, err := memory.New(c)
	if err != nil {
		return nil, err
	}
	m := &Model{Config: c.Clone(), Store: s, View: view.New(c), Output: map[int][]ledger.Envelope{}, Tokens: map[int]memory.Token{}, Retained: map[int]int64{}, retries: map[string]Retry{}}
	last := map[int]int64{}
	for _, r := range source {
		if r.Source != c.Source || r.Partition < 0 || r.Partition >= c.Partitions || r.Offset < last[r.Partition] || r.Offset < 0 || r.Offset == math.MaxInt64 {
			return nil, fmt.Errorf("source is not an ordered namespace history")
		}
		last[r.Partition] = r.Offset + 1
		m.Source = append(m.Source, r.Clone())
	}
	return m, nil
}

func (m *Model) Step(step Step) error {
	if step.Partition < 0 || step.Partition >= m.Config.Partitions {
		return fmt.Errorf("invalid scheduler partition")
	}
	if step.Outcome != Commit && step.Outcome != Abort && step.Outcome != UnknownCommit && step.Outcome != UnknownAbort {
		return fmt.Errorf("invalid scheduler outcome")
	}
	install := step.Outcome == Commit || step.Outcome == UnknownCommit
	observation := step.Outcome
	if step.Outcome == UnknownCommit || step.Outcome == UnknownAbort {
		observation = "unknown"
	}
	p := step.Partition
	retryKey := fmt.Sprintf("%s/%d", step.Actor, p)
	retrying := step.Actor == "worker" || step.Actor == "relay" || step.Actor == "view"
	if retrying {
		retry := m.retries[retryKey]
		if retry.Exhausted || m.Now < retry.Next {
			observation = "backoff"
			if retry.Exhausted {
				observation = "parked"
			}
			m.Events = append(m.Events, Event{Step: step, Observation: observation, Tick: m.Now})
			return m.Safety()
		}
	}
	switch step.Actor {
	case "tick":
		if step.Outcome != Commit || m.Now == math.MaxInt64 {
			return fmt.Errorf("invalid clock advance")
		}
		m.Now++
		observation = "clock"
	case "resume-worker", "resume-relay", "resume-view":
		if step.Outcome != Commit {
			return fmt.Errorf("invalid resume outcome")
		}
		m.retries[fmt.Sprintf("%s/%d", step.Actor[len("resume-"):], p)] = Retry{}
		observation = "resumed"
	case "worker":
		if _, known := m.Tokens[p]; !known {
			token, err := m.Store.Observe(p)
			if err != nil {
				return err
			}
			m.Tokens[p] = token
			m.Events = append(m.Events, Event{Step: step, Observation: "ownership-reobserved", Tick: m.Now})
		}
		frontier := m.Store.Snapshot().Partitions[p].Next
		var record *ledger.Record
		for i := range m.Source {
			r := &m.Source[i]
			if r.Partition == p && r.Offset >= frontier {
				record = r
				break
			}
		}
		if record == nil {
			observation = "idle"
			break
		}
		decision, err := m.Store.Prepare(m.Tokens[p], frontier, *record)
		if err != nil {
			return err
		}
		if install {
			if err := m.Store.Commit(decision); err != nil {
				return err
			}
		}
	case "relay":
		emission, ok := m.Store.Pending(p)
		if !ok {
			observation = "idle"
			break
		}
		if install {
			m.Output[p] = append(m.Output[p], emission.Envelope.Clone())
		}
		// Accepted publication with a lost reply stays pending, unchanged.
		if step.Outcome == Commit {
			if err := m.Store.Acknowledge(p, emission.Sequence); err != nil {
				return err
			}
		}
	case "view":
		offset := m.View.Next[p]
		if offset >= int64(len(m.Output[p])) {
			observation = "idle"
			break
		}
		candidate := m.View.Clone()
		if err := candidate.Apply(p, offset, offset, m.Output[p][offset]); err != nil {
			m.View = candidate
			return err
		}
		if install {
			m.View = candidate
		}
	case "claim":
		if install {
			token, err := m.Store.Claim(p)
			if err != nil {
				return err
			}
			m.Tokens[p] = token
		}
		if observation == "unknown" {
			delete(m.Tokens, p)
		}
	default:
		return fmt.Errorf("unknown scheduler actor %q", step.Actor)
	}
	if retrying && observation != "idle" {
		if step.Outcome == Commit {
			m.retries[retryKey] = Retry{}
		} else {
			retry, err := m.retries[retryKey].Failed(m.Now)
			if err != nil {
				return err
			}
			m.retries[retryKey] = retry
		}
	}
	m.Events = append(m.Events, Event{Step: step, Observation: observation, Tick: m.Now})
	return m.Safety()
}

// Drain is an explicit finite fair suffix after faults stop. Idle or stalled
// schedules are never assumed to have converged merely because time elapsed.
func (m *Model) Drain() error {
	for p := 0; p < m.Config.Partitions; p++ {
		// The fair recovery suffix explicitly resumes parked actors, then honors
		// outstanding backoff. Recovery is a schedule choice, never a deadline.
		for _, actor := range []string{"worker", "relay", "view"} {
			retry := m.RetryState(actor, p)
			if retry.Exhausted {
				if err := m.Step(Step{"resume-" + actor, p, Commit}); err != nil {
					return err
				}
			}
			for m.Now < retry.Next {
				if err := m.Step(Step{"tick", p, Commit}); err != nil {
					return err
				}
			}
		}
		for _, r := range m.Source {
			if r.Partition == p && r.Offset >= m.Store.Snapshot().Partitions[p].Next {
				if err := m.Step(Step{"worker", p, Commit}); err != nil {
					return err
				}
			}
		}
		for {
			if _, ok := m.Store.Pending(p); !ok {
				break
			}
			if err := m.Step(Step{"relay", p, Commit}); err != nil {
				return err
			}
		}
		for m.View.Next[p] < int64(len(m.Output[p])) {
			if err := m.Step(Step{"view", p, Commit}); err != nil {
				return err
			}
		}
	}
	return nil
}

type Report struct {
	Status       string             `json:"status"`
	Scope        string             `json:"scope"`
	Namespace    string             `json:"namespace"`
	ConfigDigest string             `json:"config_digest"`
	SourceEnd    map[int]int64      `json:"source_end"`
	OutputEnd    map[int]int64      `json:"output_end"`
	Conflicts    []ledger.Conflict  `json:"conflicts"`
	Rejections   []ledger.Rejection `json:"rejections"`
	Difference   string             `json:"difference,omitempty"`
}

func (m *Model) Check() Report {
	r := Report{Scope: "memory-model-only", Namespace: m.Config.Namespace, ConfigDigest: m.Config.Digest(), SourceEnd: map[int]int64{}, OutputEnd: map[int]int64{}}
	expected, err := oracle.Compute(m.Config, m.Source)
	r.Conflicts = expected.Conflicts
	r.Rejections = expected.Rejections
	if err != nil {
		r.Status = "ERROR"
		r.Difference = err.Error()
		return r
	}
	s := m.Store.Snapshot()
	for p := 0; p < m.Config.Partitions; p++ {
		r.SourceEnd[p] = 0
		r.OutputEnd[p] = int64(len(m.Output[p]))
	}
	for _, record := range m.Source {
		r.SourceEnd[record.Partition] = record.Offset + 1
	}
	if len(m.View.Errors) > 0 {
		r.Status = "ERROR"
		r.Difference = "output protocol violation"
		return r
	}
	if s.Config.Digest() != m.Config.Digest() || m.View.Config.Digest() != m.Config.Digest() {
		r.Status = "INCOMPLETE"
		r.Difference = "namespace configuration changed"
		return r
	}
	for p := 0; p < m.Config.Partitions; p++ {
		_, pending := m.Store.Pending(p)
		if m.Retained[p] > 0 || s.Partitions[p].Next != r.SourceEnd[p] || pending || m.View.Next[p] != r.OutputEnd[p] {
			r.Status = "INCOMPLETE"
			return r
		}
	}
	blocked := false
	for _, want := range expected.Keys {
		got := m.View.Projection(want.Key)
		if want.Status == "BLOCKED" {
			blocked = true
		}
		if got.Status != want.Status || !slices.Equal(got.Reasons, want.Reasons) {
			r.Difference = fmt.Sprintf("trust differs: %+v", want.Key)
		}
		if want.Status != "BLOCKED" && (!slices.Equal(got.Balances, want.Balances) || !slices.Equal(got.Alerts, want.Alerts)) {
			r.Difference = fmt.Sprintf("business projection differs: %+v", want.Key)
		}
	}
	// Compare exact rejected records/reasons, not a blanket quarantine count.
	if !sameRejections(s.Rejections, expected.Rejections) {
		r.Difference = "rejection evidence differs"
	}
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
	if !slices.Equal(conflicts, expected.Conflicts) {
		r.Difference = "conflict evidence differs"
	}
	if blocked {
		r.Status = "BLOCKED"
	} else if r.Difference != "" {
		r.Status = "DRIFT"
	} else if len(expected.Rejections)+len(expected.Conflicts) > 0 {
		r.Status = "PASS_WITH_EXCLUSIONS"
	} else {
		r.Status = "PASS"
	}
	return r
}

func sameRejections(a, b []ledger.Rejection) bool {
	a = slices.Clone(a)
	b = slices.Clone(b)
	less := func(x, y ledger.Rejection) int {
		if x.Record.Partition < y.Record.Partition {
			return -1
		}
		if x.Record.Partition > y.Record.Partition {
			return 1
		}
		if x.Record.Offset < y.Record.Offset {
			return -1
		}
		if x.Record.Offset > y.Record.Offset {
			return 1
		}
		return 0
	}
	slices.SortFunc(a, less)
	slices.SortFunc(b, less)
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !reflect.DeepEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

// Accepts permits a qualified result only for an exact explicit diagnostic
// manifest. Negative statuses never turn into successful verification.
func (r Report) Accepts(rejections []ledger.Rejection, conflicts []ledger.Conflict) bool {
	if r.Status == "PASS" {
		return len(rejections)+len(conflicts) == 0
	}
	return r.Status == "PASS_WITH_EXCLUSIONS" && sameRejections(r.Rejections, rejections) && slices.Equal(r.Conflicts, conflicts)
}

func (m *Model) Replay(partition int, from, until int64) error {
	s := m.Store.Snapshot()
	p, ok := s.Partitions[partition]
	if !ok || from < m.Retained[partition] || from < 0 || until < from || until > p.Next || s.Config.Digest() != m.Config.Digest() {
		return fmt.Errorf("unsupported replay range or configuration")
	}
	// Validate the entire range before installing the administrative fence.
	for _, r := range m.Source {
		if r.Partition == partition && r.Offset >= from && r.Offset < until {
			if err := m.Store.CheckReplay(m.Tokens[partition], r); err != nil {
				return err
			}
		}
	}
	token, err := m.Store.Claim(partition)
	if err != nil {
		return err
	}
	m.Tokens[partition] = token
	for _, r := range m.Source {
		if r.Partition == partition && r.Offset >= from && r.Offset < until {
			if err := m.Store.CheckReplay(token, r); err != nil {
				return err
			}
		}
	}
	return nil
}
