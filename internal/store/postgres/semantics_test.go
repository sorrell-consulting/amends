//go:build integration

// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"

	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/oracle"
)

func assertion(t *testing.T, c ledger.Config, id string, rev int64, kind ledger.TransactionKind, day int, qty int64, offset int64) ledger.Record {
	t.Helper()
	k := c.Keys[0]
	v := ledger.Transaction{TxnID: id, Revision: rev, Kind: kind, StockKey: k.StockKey, ValidTime: c.Origin.AddDate(0, 0, day-1), Qty: qty, SourceTime: c.Origin}
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return ledger.Record{Coordinates: ledger.Coordinates{Source: c.Source, Partition: k.Partition, Offset: offset}, Key: k.StockKey, Value: raw}
}

func TestPostgresSC08SC09SC10_ConflictEvidenceSurvivesReconnect(t *testing.T) {
	s, _ := fixtureStore(t)
	token := claim(t, s, 0)
	rs := []ledger.Record{
		assertion(t, s.config, "issue", 1, "NEW", 1, -50, 0),
		assertion(t, s.config, "issue", 1, "NEW", 1, -30, 1),
		assertion(t, s.config, "later", 1, "NEW", 2, -20, 2),
		assertion(t, s.config, "issue", 2, "AMEND", 1, -30, 3),
	}
	for _, r := range rs[:3] {
		process(t, s, token, r)
	}
	deliver(t, s, 0)
	before := snapshot(t, s)
	k := s.config.Keys[0].StockKey
	p := before.Keys[k].Projection
	if p.Status != "BLOCKED" || len(p.Reasons) != 1 || len(p.Balances) != 1 || p.Balances[0].Close != 50 {
		t.Fatalf("blocked projection did not freeze: %+v", p)
	}
	again, err := New(database(t), s.config)
	if err != nil {
		t.Fatal(err)
	}
	nextToken := claim(t, again, 0)
	process(t, again, nextToken, rs[3])
	deliver(t, again, 0)
	after := snapshot(t, again)
	p = after.Keys[k].Projection
	if p.Status != "READY" || len(p.Balances) != 2 || p.Balances[0].Close != 70 || p.Balances[1].Close != 50 || len(p.Alerts) != 1 || p.Alerts[0].Day != "2026-01-02" {
		t.Fatalf("resolution lost blocked input: %+v", p)
	}
	if len(after.Keys[k].Conflicts()) != 1 {
		t.Fatal("reconnect lost historical conflict evidence")
	}
	repairs := after.Outbox[len(before.Outbox):]
	if len(repairs) < 2 || repairs[len(repairs)-1].Envelope.Payload.Status != "READY" {
		t.Fatal("READY preceded repair envelopes")
	}
	batch, err := oracle.Compute(s.config, rs)
	if err != nil {
		t.Fatal(err)
	}
	v, err := again.View(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	compareProjection(t, v.Projection(k), batch.Keys[0])
}

func TestPostgresSC04SC05SC06_CancellationAndVersionReappearance(t *testing.T) {
	s, _ := fixtureStore(t)
	token := claim(t, s, 0)
	rs := []ledger.Record{
		assertion(t, s.config, "issue", 3, "CANCEL", 9, 0, 0),
		assertion(t, s.config, "issue", 1, "NEW", 1, -50, 1),
		assertion(t, s.config, "issue", 2, "AMEND", 1, -80, 2),
		assertion(t, s.config, "issue", 4, "NEW", 1, -50, 3),
		assertion(t, s.config, "issue", 5, "CANCEL", 9, 0, 4),
		assertion(t, s.config, "issue", 6, "AMEND", 1, -30, 5),
	}
	for _, r := range rs[:3] {
		process(t, s, token, r)
	}
	before := snapshot(t, s)
	if len(before.Outbox) != 1 || len(before.Keys[s.config.Keys[0].StockKey].Projection.Balances) != 0 {
		t.Fatal("stale activity defeated cancellation")
	}
	for _, r := range rs[3:] {
		process(t, s, token, r)
	}
	deliver(t, s, 0)
	v, err := s.View(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	id := ledger.OutputID{Namespace: s.config.Namespace, Family: "balance", Key: s.config.Keys[0].StockKey, Day: "2026-01-01"}
	if e := v.Rows[id]; e.Version != 3 || !e.Payload.Present || e.Payload.Close != 70 {
		t.Fatalf("reappearance reset version or lost authority: %+v", e)
	}
}

func TestPostgresSC27_ArithmeticAndVersionRollback(t *testing.T) {
	for _, version := range []bool{false, true} {
		t.Run(map[bool]string{false: "daily-close", true: "output-version"}[version], func(t *testing.T) {
			c, err := fixtures.Config("numeric")
			if err != nil {
				t.Fatal(err)
			}
			if !version {
				c.Keys[0].Opening = math.MaxInt64
			}
			s := newStore(t, database(t), c)
			token := claim(t, s, 0)
			r := assertion(t, s.config, "issue", 1, "NEW", 1, 1, 0)
			if version {
				r = assertion(t, s.config, "issue", 1, "NEW", 1, -30, 0)
				process(t, s, token, r)
				for id, e := range snapshot(t, s).Outputs {
					if id.Family != "balance" {
						continue
					}
					e.Version = math.MaxInt64
					raw, err := json.Marshal(e)
					if err != nil {
						t.Fatal(err)
					}
					if _, err = s.pool.Exec(t.Context(), "UPDATE amends_output_state SET version=$3,envelope=$4 WHERE namespace_id=$1 AND output_id=$2", s.ns, id.String(), e.Version, raw); err != nil {
						t.Fatal(err)
					}
				}
				r = assertion(t, s.config, "issue", 2, "AMEND", 1, -40, 1)
			}
			before := snapshot(t, s)
			if err = s.Process(t.Context(), token, before.Partitions[0].Next, r); err == nil {
				t.Fatal("overflow accepted")
			}
			if !reflect.DeepEqual(before, snapshot(t, s)) {
				t.Fatal("failed fold/diff changed durable state")
			}
		})
	}
}

func TestPostgresSC28_RoutingAndEncodedIdentifiers(t *testing.T) {
	c, err := fixtures.Config("synthetic\x00namespace")
	if err != nil {
		t.Fatal(err)
	}
	c.Partitions = 2
	c.Keys[0].Item = "synthetic\x00item/part"
	c.Keys[0].Location = "depot\nlocation"
	s := newStore(t, database(t), c)
	r := assertion(t, s.config, "txn\x00id", 1, "NEW", 1, -30, 0)
	wrong := r.Clone()
	wrong.Partition = 1
	process(t, s, claim(t, s, 1), wrong)
	if q := snapshot(t, s).Rejections; len(q) != 1 || q[0].Reason != "ROUTING" {
		t.Fatal("misrouting was not quarantined")
	}
	process(t, s, claim(t, s, 0), r)
	deliver(t, s, 0)
	v, err := s.View(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if p := v.Projection(c.Keys[0].StockKey); len(p.Balances) != 1 || p.Balances[0].Close != 70 {
		t.Fatalf("JSON identifiers did not round trip: %+v", p)
	}
}
