//go:build integration

// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/oracle"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/store/memory"
	"github.com/sorrell-consulting/amends/sim"
)

func database(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("AMENDS_TEST_DATABASE_URL")
	if url == "" {
		t.Fatal("integration tests require AMENDS_TEST_DATABASE_URL; use make test-postgres")
	}
	pool, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err = pool.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = Migrate(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	return pool
}

func TestPostgresEnvironment(t *testing.T) {
	pool := database(t)
	var version, fsync string
	if err := pool.QueryRow(t.Context(), "SELECT current_setting('server_version'),current_setting('fsync')").Scan(&version, &fsync); err != nil {
		t.Fatal(err)
	}
	t.Logf("PostgreSQL %s; fsync=%s; adapter sets synchronous_commit=on", version, fsync)
	if fsync != "on" {
		t.Fatal("durable-store integration requires fsync=on")
	}
}

func newStore(t *testing.T, pool *pgxpool.Pool, c ledger.Config) *Store {
	t.Helper()
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	c.Namespace += "/integration/" + t.Name() + "/" + hex.EncodeToString(suffix[:])
	s, err := New(pool, c)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Ensure(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Error(err)
			return
		}
		defer rollback(tx)
		// Only this test's newly created namespace is removed. Shared schema,
		// other namespaces, and Docker volumes are never reset.
		for _, table := range []string{"amends_view_rows", "amends_view_progress", "amends_quarantine", "amends_outbox", "amends_output_state", "amends_key_state", "amends_partitions", "amends_namespaces"} {
			if _, err = tx.Exec(ctx, "DELETE FROM "+table+" WHERE namespace_id=$1", s.ns); err != nil {
				t.Error(err)
				return
			}
		}
		if err = tx.Commit(ctx); err != nil {
			t.Error(err)
		}
	})
	return s
}
func fixtureStore(t *testing.T) (*Store, []ledger.Record) {
	t.Helper()
	c, err := fixtures.Config("temporary")
	if err != nil {
		t.Fatal(err)
	}
	s := newStore(t, database(t), c)
	rs, err := fixtures.Records(s.config, "before.jsonl", "correction.jsonl", "cancel-last-day.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	return s, rs
}
func claim(t *testing.T, s *Store, p int) store.Token {
	t.Helper()
	token, err := s.Claim(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	return token
}
func snapshot(t *testing.T, s *Store) store.Snapshot {
	t.Helper()
	v, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func process(t *testing.T, s *Store, token store.Token, r ledger.Record) {
	t.Helper()
	_, p, err := s.Observe(t.Context(), r.Partition)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Process(t.Context(), token, p.Next, r); err != nil {
		t.Fatal(err)
	}
}
func compareProjection(t *testing.T, got, want ledger.Projection) {
	t.Helper()
	if got.Key != want.Key || got.Status != want.Status || !slices.Equal(got.Reasons, want.Reasons) || !slices.Equal(got.Balances, want.Balances) || !slices.Equal(got.Alerts, want.Alerts) {
		t.Fatalf("projection\ngot: %+v\nwant: %+v", got, want)
	}
}
func deliver(t *testing.T, s *Store, p int) {
	t.Helper()
	for {
		d, ok, err := s.Pending(t.Context(), p)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return
		}
		v, err := s.View(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		offset := v.Next[p]
		if err = s.ApplyView(t.Context(), p, offset, offset, d.Envelope); err != nil {
			t.Fatal(err)
		}
		if err = s.Acknowledge(t.Context(), p, d.Sequence, offset); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPostgresSC02SC14SC16_FixturesAndReconnect(t *testing.T) {
	s, rs := fixtureStore(t)
	token := claim(t, s, 0)
	stages := []struct {
		end      int
		expected string
		outputs  int64
	}{{3, "expected-before.json", 5}, {4, "expected-after.json", 10}, {5, "expected-after-cancel.json", 11}}
	start := 0
	for _, stage := range stages {
		for _, r := range rs[start:stage.end] {
			process(t, s, token, r)
		}
		deliver(t, s, 0)
		want, err := fixtures.Expected(stage.expected)
		if err != nil {
			t.Fatal(err)
		}
		got, err := s.View(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		batch, err := oracle.Compute(s.config, rs[:stage.end])
		if err != nil {
			t.Fatal(err)
		}
		compareProjection(t, got.Projection(want.Key), want)
		compareProjection(t, batch.Keys[0], want)
		snap := snapshot(t, s)
		if snap.Partitions[0].Next != int64(stage.end) || snap.Partitions[0].Sequence != stage.outputs || got.Next[0] != stage.outputs {
			t.Fatalf("unexpected frontiers: %+v %+v", snap.Partitions, got.Next)
		}
		start = stage.end
	}
	// Recreate the pool and adapter: no in-process state can supply the answer.
	again, err := New(database(t), s.config)
	if err != nil {
		t.Fatal(err)
	}
	if err = again.Ensure(t.Context()); err != nil {
		t.Fatal(err)
	}
	rows, err := again.View(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	id := ledger.OutputID{Namespace: s.config.Namespace, Family: "balance", Key: s.config.Keys[0].StockKey, Day: "2026-01-03"}
	if e := rows.Rows[id]; e.Version != 3 || e.Payload.Present {
		t.Fatalf("lost withdrawal guard: %+v", e)
	}
	for _, row := range snapshot(t, s).Outbox {
		if row.Envelope.ID == id && row.Envelope.Version < 3 {
			n := rows.Next[0]
			if err = again.ApplyView(t.Context(), 0, n, n, row.Envelope); err != nil {
				t.Fatal(err)
			}
			rows, err = again.View(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if rows.Rows[id].Version != 3 || rows.Rows[id].Payload.Present || rows.Next[0] != n+1 {
				t.Fatal("older output resurrected the withdrawn row or lost progress")
			}
		}
	}
}

func TestPostgresSC18SC22_AbortAndQuarantine(t *testing.T) {
	s, rs := fixtureStore(t)
	token := claim(t, s, 0)
	for _, poison := range []bool{false, true} {
		t.Run(fmt.Sprintf("poison=%v", poison), func(t *testing.T) {
			_, p, err := s.Observe(t.Context(), 0)
			if err != nil {
				t.Fatal(err)
			}
			r := rs[0].Clone()
			r.Offset = p.Next
			if poison {
				r.Value = []byte("{broken\x00json")
			}
			before := snapshot(t, s)
			tx, err := s.Begin(t.Context(), token, p.Next)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if err = tx.Apply(r); err != nil {
				t.Fatal(err)
			}
			// A separate connection sees neither the decision nor its frontier.
			if !reflect.DeepEqual(before, snapshot(t, s)) {
				t.Fatal("uncommitted decision escaped")
			}
			tx.Rollback()
			if !reflect.DeepEqual(before, snapshot(t, s)) {
				t.Fatal("rollback left partial state")
			}
			process(t, s, token, r)
			after := snapshot(t, s)
			if after.Partitions[0].Next != p.Next+1 {
				t.Fatal("progress not installed")
			}
			if poison {
				if len(after.Rejections) != 1 || after.Rejections[0].Reason != "MALFORMED" || !bytes.Equal(after.Rejections[0].Record.Value, r.Value) {
					t.Fatal("quarantine did not retain exact raw bytes and reason")
				}
				if !reflect.DeepEqual(before.Outputs, after.Outputs) {
					t.Fatal("rejection changed business outputs")
				}
			}
		})
	}
}

func waitBlocked(t *testing.T, pool *pgxpool.Pool, blocker uint32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1::int=ANY(pg_blocking_pids(pid)))", blocker).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("second connection never waited for the partition row")
		case <-ticker.C:
		}
	}
}

func TestPostgresSC19_BothDatabaseLockOrders(t *testing.T) {
	for _, oldFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("old-first=%v", oldFirst), func(t *testing.T) {
			s, rs := fixtureStore(t)
			old := claim(t, s, 0)
			other, err := New(database(t), s.config)
			if err != nil {
				t.Fatal(err)
			}
			if oldFirst {
				tx, err := s.Begin(t.Context(), old, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if err = tx.Apply(rs[0]); err != nil {
					t.Fatal(err)
				}
				result := make(chan error, 1)
				go func() { _, err := other.Claim(t.Context(), 0); result <- err }()
				waitBlocked(t, s.pool, tx.tx.Conn().PgConn().PID())
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
				if err = <-result; err != nil {
					t.Fatal(err)
				}
				after := snapshot(t, s)
				if after.Partitions[0].Next != 1 || after.Partitions[0].Epoch != 2 || len(after.Outbox) != 3 {
					t.Fatal("old commit did not precede ownership installation atomically")
				}
			} else {
				// Hold the actual replacement UPDATE uncommitted, then show the
				// old processing transaction waits and rereads the newer epoch.
				replacement, ctx, cancel, err := begin(t.Context(), other.pool, pgx.TxOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defer cancel()
				defer rollback(replacement)
				if _, err = other.lock(ctx, replacement, 0); err != nil {
					t.Fatal(err)
				}
				if _, err = replacement.Exec(ctx, "UPDATE amends_partitions SET epoch=epoch+1 WHERE namespace_id=$1 AND partition=0", s.ns); err != nil {
					t.Fatal(err)
				}
				result := make(chan error, 1)
				go func() { result <- s.Process(t.Context(), old, 0, rs[0]) }()
				waitBlocked(t, other.pool, replacement.Conn().PgConn().PID())
				if err = commit(ctx, replacement); err != nil {
					t.Fatal(err)
				}
				if err = <-result; !errors.Is(err, store.ErrFence) {
					t.Fatalf("[amends:epoch-fence] stale owner: %v", err)
				}
				after := snapshot(t, s)
				if after.Partitions[0].Next != 0 || after.Partitions[0].Epoch != 2 || len(after.Outbox) != 0 || len(after.Rejections) != 0 {
					t.Fatal("fenced owner partially committed")
				}
			}
			before := snapshot(t, s)
			if err = s.Process(t.Context(), old, before.Partitions[0].Next, rs[0]); !errors.Is(err, store.ErrFence) {
				t.Fatalf("obsolete token accepted: %v", err)
			}
			if !reflect.DeepEqual(before, snapshot(t, s)) {
				t.Fatal("obsolete owner changed durable state")
			}
		})
	}
}

func TestPostgresSC17_ProtocolErrorIsDurable(t *testing.T) {
	s, rs := fixtureStore(t)
	process(t, s, claim(t, s, 0), rs[0])
	d, _, err := s.Pending(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ApplyView(t.Context(), 0, 0, 0, d.Envelope); err != nil {
		t.Fatal(err)
	}
	duplicate := d.Envelope.Clone()
	if err = s.ApplyView(t.Context(), 0, 1, 1, duplicate); err != nil {
		t.Fatal(err)
	}
	bad := duplicate.Clone()
	bad.Cause.Offset++
	if err = s.ApplyView(t.Context(), 0, 2, 2, bad); !errors.Is(err, store.ErrProtocol) {
		t.Fatalf("same version with a different cause accepted: %v", err)
	}
	v, err := s.View(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if v.Next[0] != 2 || v.Errors[0] == "" || !v.Rows[duplicate.ID].Equal(duplicate) {
		t.Fatal("protocol error changed view/progress or lost diagnostics")
	}
	again, err := New(database(t), s.config)
	if err != nil {
		t.Fatal(err)
	}
	if err = again.ApplyView(t.Context(), 0, 2, 2, duplicate); !errors.Is(err, store.ErrProtocol) {
		t.Fatalf("reconnect forgot stopped partition: %v", err)
	}
}

func TestPostgresSC28_ProtocolDiagnosticEscapesIdentifiers(t *testing.T) {
	s, records := fixtureStore(t)
	process(t, s, claim(t, s, 0), records[0])
	delivery, _, err := s.Pending(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	bad := delivery.Envelope.Clone()
	bad.ID.Key.Item = "unknown\x00item"
	raw, err := json.Marshal(bad)
	if err != nil {
		t.Fatal(err)
	}
	input := store.ViewInput{Partition: 0, Offset: 0, Envelope: bad, Value: raw}
	if err = s.ApplyViewInput(t.Context(), 0, input); !errors.Is(err, store.ErrProtocol) {
		t.Fatalf("invalid key must commit a protocol diagnostic: %v", err)
	}
	v, err := s.View(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if v.Next[0] != 0 || len(v.Rows) != 0 || !strings.Contains(v.Errors[0], `unknown\x00item`) {
		t.Fatalf("lost diagnostic or advanced invalid output: %+v", v)
	}
	var retained []byte
	if err = s.pool.QueryRow(t.Context(), "SELECT error_value FROM amends_view_progress WHERE namespace_id=$1 AND partition=0", s.ns).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(retained, raw) {
		t.Fatal("diagnostic escaping changed the retained raw envelope")
	}
}

func TestPostgresSC20_OutboxOrderAndImmutableRetry(t *testing.T) {
	s, rs := fixtureStore(t)
	process(t, s, claim(t, s, 0), rs[0])
	first, ok, err := s.Pending(t.Context(), 0)
	if err != nil || !ok {
		t.Fatal(err)
	}
	original := bytes.Clone(first.Bytes)
	first.Bytes[0] = '!'
	again, _, err := s.Pending(t.Context(), 0)
	if err != nil || !bytes.Equal(again.Bytes, original) {
		t.Fatal("pending envelope mutated between retries", err)
	}
	if err = s.Acknowledge(t.Context(), 0, 2, 1); err == nil {
		t.Fatal("out-of-order acknowledgement accepted")
	}
	if err = s.Acknowledge(t.Context(), 0, 1, math.MaxInt64); err == nil {
		t.Fatal("publication receipt has no representable exclusive frontier")
	}
	if err = s.Acknowledge(t.Context(), 0, 1, 3); err != nil {
		t.Fatal(err)
	}
	if err = s.Acknowledge(t.Context(), 0, 1, 3); err != nil {
		t.Fatal("same receipt was not idempotent", err)
	}
	if err = s.Acknowledge(t.Context(), 0, 1, 4); err == nil {
		t.Fatal("conflicting receipt accepted")
	}
	if err = s.Acknowledge(t.Context(), 0, 2, 2); err == nil {
		t.Fatal("backward publication position accepted")
	}
	next, ok, err := s.Pending(t.Context(), 0)
	if err != nil || !ok || next.Sequence != 2 {
		t.Fatal("lost next pending row", err)
	}
}

func TestPostgresSC27SC28_ConfigurationAndOverflow(t *testing.T) {
	s, rs := fixtureStore(t)
	if err := Migrate(t.Context(), s.pool); err != nil {
		t.Fatal("idempotent migration", err)
	}
	if err := s.Ensure(t.Context()); err != nil {
		t.Fatal("idempotent namespace", err)
	}
	bad := s.config.Clone()
	bad.Keys[0].Threshold++
	other, err := New(s.pool, bad)
	if err != nil {
		t.Fatal(err)
	}
	if err = other.Ensure(t.Context()); !errors.Is(err, store.ErrFence) {
		t.Fatalf("configuration overwrite accepted: %v", err)
	}
	token := claim(t, s, 0)
	if _, err = s.pool.Exec(t.Context(), "UPDATE amends_partitions SET emission_sequence=$2 WHERE namespace_id=$1 AND partition=0", s.ns, int64(math.MaxInt64)); err != nil {
		t.Fatal(err)
	}
	before := snapshot(t, s)
	tx, err := s.Begin(t.Context(), token, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = tx.Apply(rs[0]); err == nil {
		t.Fatal("emission overflow accepted")
	}
	if err = tx.Commit(); err == nil {
		t.Fatal("failed decision could commit")
	}
	if !reflect.DeepEqual(before, snapshot(t, s)) {
		t.Fatal("overflow advanced state/frontier")
	}
	if _, err = s.pool.Exec(t.Context(), "UPDATE amends_partitions SET epoch=$2 WHERE namespace_id=$1 AND partition=0", s.ns, int64(math.MaxInt64)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Claim(t.Context(), 0); err == nil {
		t.Fatal("ownership epoch wrapped")
	}
}

func equalSnapshot(t *testing.T, got, want store.Snapshot) {
	t.Helper()
	if !reflect.DeepEqual(got.Partitions, want.Partitions) || len(got.Keys) != len(want.Keys) || len(got.Outputs) != len(want.Outputs) || len(got.Outbox) != len(want.Outbox) || len(got.Rejections) != len(want.Rejections) {
		t.Fatal("store conformance: sizes/frontiers differ")
	}
	for key, state := range want.Keys {
		a, err := json.Marshal(got.Keys[key])
		if err != nil {
			t.Fatal(err)
		}
		b, err := json.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(a, b) {
			t.Fatalf("store conformance: evidence differs for %+v\n%s\n%s", key, a, b)
		}
	}
	for id, e := range want.Outputs {
		if !e.Equal(got.Outputs[id]) {
			t.Fatal("store conformance: output differs")
		}
	}
	for _, expected := range want.Outbox {
		found := false
		for _, actual := range got.Outbox {
			if actual.Partition == expected.Partition && actual.Sequence == expected.Sequence {
				found = actual.Envelope.Equal(expected.Envelope) && actual.Acknowledged == expected.Acknowledged
			}
		}
		if !found {
			t.Fatal("store conformance: outbox differs")
		}
	}
	for _, expected := range want.Rejections {
		found := false
		for _, actual := range got.Rejections {
			if reflect.DeepEqual(actual, expected) {
				found = true
			}
		}
		if !found {
			t.Fatal("store conformance: quarantine differs")
		}
	}
}

func TestPostgresMemoryConformance(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(fmt.Sprintf("invalid=%v", invalid), func(t *testing.T) {
			trace := sim.Generate(7, invalid)
			s := newStore(t, database(t), trace.Config)
			model, err := memory.New(s.config)
			if err != nil {
				t.Fatal(err)
			}
			tokens := map[int]store.Token{}
			for p := 0; p < s.config.Partitions; p++ {
				tokens[p] = claim(t, s, p)
				if _, err = model.Claim(p); err != nil {
					t.Fatal(err)
				}
			}
			for _, r := range trace.Source {
				expected := model.Snapshot().Partitions[r.Partition].Next
				d, err := model.Prepare(tokens[r.Partition], expected, r)
				if err != nil {
					t.Fatal(err)
				}
				if err = model.Commit(d); err != nil {
					t.Fatal(err)
				}
				process(t, s, tokens[r.Partition], r)
				equalSnapshot(t, snapshot(t, s), model.Snapshot())
			}
			batch, err := oracle.Compute(s.config, trace.Source)
			if err != nil {
				t.Fatal(err)
			}
			got := snapshot(t, s)
			for _, want := range batch.Keys {
				if want.Status != "BLOCKED" {
					compareProjection(t, got.Keys[want.Key].Projection, want)
				}
			}
		})
	}
}
