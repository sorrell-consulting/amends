// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Package memory is a serial, deterministic transaction model. It supplies no
// SQL locking or process durability guarantees and must not be used concurrently.
package memory

import (
	"bytes"
	"fmt"
	"math"
	"slices"

	"github.com/sorrell-consulting/amends/internal/diff"
	"github.com/sorrell-consulting/amends/internal/fold"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
)

// Partition is the shared ownership and processing progress value.
type Partition = store.Partition

// Token is the shared namespace/configuration/epoch guard.
type Token = store.Token

// Emission is the shared committed outbox entry.
type Emission = store.Emission

// Snapshot is the shared processing-state representation.
type Snapshot = store.Snapshot

// Store models serial atomic decisions in memory. Initialize it with [New];
// it supplies neither durable storage nor concurrent access protection.
type Store struct{ state Snapshot }

// Decision is a prepared record outcome tied to its original epoch and frontier.
// Only [Store.Commit] installs it; discarding it leaves the store unchanged.
type Decision struct {
	token     Token
	expected  int64
	record    ledger.Record
	next      fold.State
	messages  []ledger.Envelope
	rejection string
}

// New validates and copies c into empty state with unclaimed partitions.
func New(c ledger.Config) (*Store, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	s := &Store{state: Snapshot{Config: c.Clone(), Partitions: map[int]Partition{}, Keys: map[ledger.StockKey]fold.State{}, Outputs: map[ledger.OutputID]ledger.Envelope{}}}
	for i := 0; i < c.Partitions; i++ {
		s.state.Partitions[i] = Partition{}
	}
	for _, k := range c.Keys {
		s.state.Keys[k.StockKey] = fold.Empty(k.StockKey)
	}
	return s, nil
}

// Snapshot returns a deep copy of processing state, output intent, and evidence.
// The caller may change the returned maps and slices without changing the store.
func (s *Store) Snapshot() Snapshot {
	n := s.state
	n.Config = n.Config.Clone()
	n.Partitions = map[int]Partition{}
	n.Keys = map[ledger.StockKey]fold.State{}
	n.Outputs = map[ledger.OutputID]ledger.Envelope{}
	for k, v := range s.state.Partitions {
		n.Partitions[k] = v
	}
	for k, v := range s.state.Keys {
		n.Keys[k] = v.Clone()
	}
	for k, v := range s.state.Outputs {
		n.Outputs[k] = v.Clone()
	}
	n.Outbox = slices.Clone(s.state.Outbox)
	for i := range n.Outbox {
		n.Outbox[i].Envelope = n.Outbox[i].Envelope.Clone()
	}
	n.Rejections = slices.Clone(s.state.Rejections)
	for i := range n.Rejections {
		n.Rejections[i].Record = n.Rejections[i].Record.Clone()
	}
	return n
}

// Claim increments a partition's epoch and returns its token. Unknown partitions
// and epoch exhaustion fail without changing state.
func (s *Store) Claim(partition int) (Token, error) {
	p, ok := s.state.Partitions[partition]
	if !ok || p.Epoch == math.MaxInt64 {
		return Token{}, fmt.Errorf("invalid partition or epoch overflow")
	}
	p.Epoch++
	s.state.Partitions[partition] = p
	return Token{Namespace: s.state.Config.Namespace, Digest: s.state.Config.Digest(), Partition: partition, Epoch: p.Epoch}, nil
}

// Observe rereads the installed token after an unknown claim outcome. The
// scheduler models a still-current local assignment; real callback lifecycle
// validation belongs to the broker adapter.
func (s *Store) Observe(partition int) (Token, error) {
	p, ok := s.state.Partitions[partition]
	if !ok || p.Epoch < 1 {
		return Token{}, fmt.Errorf("no installed ownership")
	}
	return Token{Namespace: s.state.Config.Namespace, Digest: s.state.Config.Digest(), Partition: partition, Epoch: p.Epoch}, nil
}

func (s *Store) guard(token Token, expected int64) error {
	p, ok := s.state.Partitions[token.Partition]
	if !ok || token.Namespace != s.state.Config.Namespace || token.Digest != s.state.Config.Digest() || token.Epoch != p.Epoch || token.Epoch < 1 || expected != p.Next {
		return fmt.Errorf("epoch/frontier/configuration fence rejected")
	}
	return nil
}

// Prepare derives a decision without changing the store. expected is the saved
// exclusive source frontier; token must match current ownership. Invalid business
// input produces a quarantine decision with no error. The decision must pass its
// guards again at Commit, and becomes stale if ownership or progress changes.
func (s *Store) Prepare(token Token, expected int64, r ledger.Record) (*Decision, error) {
	if err := s.guard(token, expected); err != nil {
		return nil, err
	}
	if r.Source != s.state.Config.Source || r.Partition != token.Partition || r.Offset < expected || r.Offset == math.MaxInt64 {
		return nil, fmt.Errorf("incompatible source coordinates")
	}
	d := &Decision{token: token, expected: expected, record: r.Clone()}
	t, reason := fold.Validate(s.state.Config, r)
	d.rejection = reason
	if reason != "" {
		return d, nil
	}
	k, _ := s.state.Config.Key(t.StockKey)
	next, err := fold.Apply(s.state.Keys[t.StockKey], t, r.Coordinates, k)
	if err != nil {
		return nil, err
	}
	d.next = next
	d.messages, err = diff.Reconcile(s.state.Config.Namespace, s.state.Outputs, next.Projection, ledger.Cause{Coordinates: r.Coordinates, TxnID: t.TxnID, Revision: t.Revision})
	if err != nil {
		return nil, err
	}
	if int64(len(d.messages)) > math.MaxInt64-s.state.Partitions[r.Partition].Sequence {
		return nil, fmt.Errorf("emission sequence overflow")
	}
	return d, nil
}

// Commit models one atomic installation. A crash before this call discards the
// decision. A lost reply after this call leaves its effects available to reread.
// The guard also rejects a prepared decision after ownership replacement.
func (s *Store) Commit(d *Decision) error {
	if d == nil {
		return fmt.Errorf("nil decision")
	}
	if err := s.guard(d.token, d.expected); err != nil {
		return err
	}
	p := s.state.Partitions[d.token.Partition]
	if d.rejection != "" {
		s.state.Rejections = append(s.state.Rejections, ledger.Rejection{Record: d.record.Clone(), Reason: d.rejection})
	} else {
		s.state.Keys[d.next.Projection.Key] = d.next.Clone()
		for _, e := range d.messages {
			p.Sequence++
			s.state.Outputs[e.ID] = e.Clone()
			s.state.Outbox = append(s.state.Outbox, Emission{Partition: d.token.Partition, Sequence: p.Sequence, Envelope: e.Clone()})
		}
	}
	p.Next = d.record.Offset + 1
	s.state.Partitions[d.token.Partition] = p
	return nil
}

// Pending returns a copy of the earliest unacknowledged emission for partition,
// or false when none exists. The read does not acknowledge or reserve the entry.
func (s *Store) Pending(partition int) (Emission, bool) {
	for _, row := range s.state.Outbox {
		if row.Partition == partition && !row.Acknowledged {
			row.Envelope = row.Envelope.Clone()
			return row, true
		}
	}
	return Emission{}, false
}

// Acknowledge marks only the earliest pending sequence as published. An absent
// or out-of-order sequence is an error, including an already acknowledged entry.
func (s *Store) Acknowledge(partition int, sequence int64) error {
	row, ok := s.Pending(partition)
	if !ok || row.Sequence != sequence {
		return fmt.Errorf("outbox acknowledgement out of order")
	}
	for i := range s.state.Outbox {
		if s.state.Outbox[i].Partition == partition && s.state.Outbox[i].Sequence == sequence {
			s.state.Outbox[i].Acknowledged = true
			return nil
		}
	}
	return fmt.Errorf("outbox row missing")
}

// CheckReplay evaluates a previously consumed record without changing the
// authoritative frontier. The scheduler validates retention/range first. An
// intact replay must reproduce already-retained evidence and unchanged outputs.
func (s *Store) CheckReplay(token Token, r ledger.Record) error {
	p := s.state.Partitions[token.Partition]
	if err := s.guard(token, p.Next); err != nil {
		return err
	}
	if r.Source != s.state.Config.Source || r.Partition != token.Partition || r.Offset < 0 || r.Offset >= p.Next {
		return fmt.Errorf("unsupported replay coordinates")
	}
	t, reason := fold.Validate(s.state.Config, r)
	if reason != "" {
		for _, q := range s.state.Rejections {
			if q.Record.Coordinates == r.Coordinates && q.Record.Key == r.Key && bytes.Equal(q.Record.Value, r.Value) && q.Reason == reason {
				return nil
			}
		}
		return fmt.Errorf("missing replay rejection evidence")
	}
	state := s.state.Keys[t.StockKey]
	found := false
	for _, candidate := range state.Identities[t.TxnID].Revisions[t.Revision] {
		if fold.CanonicalEqual(candidate.Transaction, t) && slices.Contains(candidate.Sources, r.Coordinates) {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("missing replay authority evidence")
	}
	k, _ := s.state.Config.Key(t.StockKey)
	next, err := fold.Apply(state, t, r.Coordinates, k)
	if err != nil {
		return err
	}
	messages, err := diff.Reconcile(s.state.Config.Namespace, s.state.Outputs, next.Projection, ledger.Cause{Coordinates: r.Coordinates, TxnID: t.TxnID, Revision: t.Revision})
	if err != nil {
		return err
	}
	if len(messages) > 0 {
		return fmt.Errorf("replay requires state repair")
	}
	return nil
}
