//go:build integration

// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/fold"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/migrations"
)

func TestOutputBindingAndServiceSessions(t *testing.T) {
	s, records := fixtureStore(t)
	token := claim(t, s, 0)
	if err := s.Process(t.Context(), token, 0, records[0]); err != nil {
		t.Fatal(err)
	}
	b := store.OutputBinding{Topic: "output", Incarnation: s.config.Namespace + "/output", Partitions: 1}
	if err := s.BindOutput(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	if err := s.BindOutput(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	changed := b
	changed.Incarnation += "-recreated"
	if err := s.BindOutput(t.Context(), changed); !errors.Is(err, store.ErrFence) {
		t.Fatalf("binding changed: %v", err)
	}
	relay, err := s.AcquireService(t.Context(), "relay")
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	if duplicate, err := s.AcquireService(t.Context(), "relay"); !errors.Is(err, ErrServiceBusy) {
		if duplicate != nil {
			duplicate.Close()
		}
		t.Fatalf("duplicate service accepted: %v", err)
	}
	view, err := s.AcquireService(t.Context(), "view")
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	if _, err := s.pool.Exec(t.Context(), "SELECT pg_terminate_backend($1)", relay.conn.PgConn().PID()); err != nil {
		t.Fatal(err)
	}
	if err := relay.Check(t.Context()); err == nil {
		t.Fatal("lost session treated as current")
	}
	replacement, err := s.AcquireService(t.Context(), "relay")
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if err := relay.Check(t.Context()); err == nil {
		t.Fatal("lost session reconnected")
	}
	if err := replacement.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestOutputBindingRefusesUnboundDeliveryHistory(t *testing.T) {
	s, rs := fixtureStore(t)
	token := claim(t, s, 0)
	if err := s.Process(t.Context(), token, 0, rs[0]); err != nil {
		t.Fatal(err)
	}
	d, ok, err := s.Pending(t.Context(), 0)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if err := s.Acknowledge(t.Context(), 0, d.Sequence, 0); err != nil {
		t.Fatal(err)
	}
	err = s.BindOutput(t.Context(), store.OutputBinding{Topic: "new", Incarnation: "new-incarnation", Partitions: 1})
	if !errors.Is(err, store.ErrFence) {
		t.Fatalf("unsafe history adopted: %v", err)
	}
}

func TestUpgradeFromInitialSchemaPreservesProcessing(t *testing.T) {
	for _, version := range []int{1, 2, 3} {
		t.Run(fmt.Sprint(version), func(test *testing.T) {
			testSchemaUpgrade(test, version)
		})
	}
}

func testSchemaUpgrade(t *testing.T, version int) {
	pool := database(t)
	schema := fmt.Sprintf("amends_upgrade_%d", time.Now().UnixNano())
	if _, err := pool.Exec(t.Context(), "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := pool.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	config := pool.Config()
	config.ConnConfig.RuntimeParams["search_path"] = schema
	isolated, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer isolated.Close()
	if _, err = isolated.Exec(t.Context(), "CREATE TABLE amends_schema_migrations(version INTEGER PRIMARY KEY,checksum TEXT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	for index, script := range migrations.Ordered()[:version] {
		if _, err = isolated.Exec(t.Context(), script); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256([]byte(script))
		if _, err = isolated.Exec(t.Context(), "INSERT INTO amends_schema_migrations VALUES($1,$2)", index+1, hex.EncodeToString(sum[:])); err != nil {
			t.Fatal(err)
		}
	}
	c, err := fixtures.Config("upgrade")
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(isolated, c)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Ensure(t.Context()); err != nil {
		t.Fatal(err)
	}
	rs, err := fixtures.Records(c, "before.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	token, err := s.Claim(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := s.Begin(t.Context(), token, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Rollback()
	transaction, err := ledger.Decode(rs[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	key := c.Keys[0].StockKey
	legacyState := fold.Empty(key)
	legacyState.Identities[transaction.TxnID] = fold.Identity{Highest: 1, Revisions: map[int64][]fold.Candidate{1: {{Transaction: transaction, Sources: []ledger.Coordinates{rs[0].Coordinates}}}}}
	legacyState.Projection.Balances = []ledger.Balance{{Day: "2026-01-01", Close: 50}}
	legacyState.Projection.Alerts = []ledger.Alert{{Day: "2026-01-01", Close: 50, Threshold: 60}}
	raw, err := json.Marshal(legacyState)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = legacy.tx.Exec(legacy.ctx, "UPDATE amends_key_state SET state=$3 WHERE namespace_id=$1 AND key_id=$2", s.ns, keyID(key), raw); err != nil {
		t.Fatal(err)
	}
	for index, output := range []ledger.Envelope{
		{ID: ledger.OutputID{Namespace: c.Namespace, Family: "alert", Key: key, Day: "2026-01-01"}, Type: "AlertRaised", Payload: ledger.Payload{Present: true, Close: 50, Threshold: 60}},
		{ID: ledger.OutputID{Namespace: c.Namespace, Family: "balance", Key: key, Day: "2026-01-01"}, Type: "BalanceStated", Payload: ledger.Payload{Present: true, Close: 50}},
		{ID: ledger.OutputID{Namespace: c.Namespace, Family: "key-status", Key: key}, Type: "KeyStatusStated", Payload: ledger.Payload{Present: true, Status: "READY", Reasons: []string{}}},
	} {
		output.SchemaVersion, output.Version = 1, 1
		output.Cause = ledger.Cause{Coordinates: rs[0].Coordinates, TxnID: transaction.TxnID, Revision: 1}
		raw, err = json.Marshal(output)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = legacy.tx.Exec(legacy.ctx, "INSERT INTO amends_output_state(namespace_id,output_id,partition,version,envelope) VALUES($1,$2,0,1,$3)", s.ns, output.ID.String(), raw); err != nil {
			t.Fatal(err)
		}
		if _, err = legacy.tx.Exec(legacy.ctx, "INSERT INTO amends_outbox(namespace_id,partition,sequence,output_id,version,envelope) VALUES($1,0,$2,$3,1,$4)", s.ns, index+1, output.ID.String(), raw); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = legacy.tx.Exec(legacy.ctx, "UPDATE amends_partitions SET next_offset=1,emission_sequence=3 WHERE namespace_id=$1 AND partition=0", s.ns); err != nil {
		t.Fatal(err)
	}
	if err = commit(legacy.ctx, legacy.tx); err != nil {
		t.Fatal(err)
	}
	before, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = Migrate(t.Context(), isolated); err != nil {
		t.Fatal(err)
	}
	if err = Migrate(t.Context(), isolated); err != nil {
		t.Fatal(err)
	}
	after, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("migration changed processing history")
	}
	if measured(t, s) != nil {
		t.Fatal("migration fabricated a historical measurement")
	}
	if statistics(t, s) != nil {
		t.Fatal("migration fabricated cumulative processing statistics")
	}
	process(t, s, token, rs[1])
	if measurement := measured(t, s); measurement == nil || measurement.Source.Offset != 1 || measurement.BusinessOutputs != 1 {
		t.Fatalf("upgraded writer did not retain measurement: %+v", measurement)
	}
	if counts := statistics(t, s); counts == nil || counts.Decisions != 1 || counts.FirstSource.Offset != 1 {
		t.Fatalf("statistics include unmeasured pre-upgrade history: %+v", counts)
	}
	if err = s.BindOutput(t.Context(), store.OutputBinding{Topic: "new-output", Incarnation: "upgrade-output", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err = isolated.Exec(t.Context(), "UPDATE amends_schema_migrations SET checksum='changed' WHERE version=2"); err != nil {
		t.Fatal(err)
	}
	if err = Migrate(t.Context(), isolated); err == nil {
		t.Fatal("modified migration accepted")
	}
}
