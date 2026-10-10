// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"math"
	"testing"
	"time"

	"github.com/sorrell-consulting/amends/internal/ledger"
)

func TestSC27_CountersAndSnapshotIsolation(t *testing.T) {
	c := ledger.Config{SchemaVersion: 1, Namespace: "n", Source: "s", Partitions: 1, Origin: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Keys: []ledger.KeyConfig{{StockKey: ledger.StockKey{Item: "i", Location: "l"}}}}
	s, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	token, err := s.Claim(0)
	if err != nil {
		t.Fatal(err)
	}
	r := ledger.Record{Coordinates: ledger.Coordinates{Source: "s", Partition: 0, Offset: 0}, Key: c.Keys[0].StockKey, Value: []byte(`{"txn_id":"a","revision":1,"kind":"NEW","stock_key":{"item":"i","location":"l"},"valid_time":"2026-01-01T12:00:00Z","qty":0,"source_time":"2026-01-02T00:00:00Z"}`)}
	p := s.state.Partitions[0]
	p.Sequence = math.MaxInt64
	s.state.Partitions[0] = p
	if _, err := s.Prepare(token, 0, r); err == nil {
		t.Fatal("sequence overflow accepted")
	}
	if s.state.Partitions[0].Next != 0 {
		t.Fatal("overflow advanced frontier")
	}
	p.Epoch = math.MaxInt64
	s.state.Partitions[0] = p
	if _, err := s.Claim(0); err == nil {
		t.Fatal("epoch overflow accepted")
	}
	snap := s.Snapshot()
	snap.Config.Keys[0].Opening = 999
	snap.Partitions[0] = Partition{}
	if s.state.Config.Keys[0].Opening != 0 || s.state.Partitions[0].Epoch != math.MaxInt64 {
		t.Fatal("snapshot aliases store")
	}
}
