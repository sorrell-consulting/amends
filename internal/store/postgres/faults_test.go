//go:build integration

// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
)

// commitLoss is a test-only socket boundary, not a fake database. It drops
// either the COMMIT request before transmission or the server's successful
// COMMIT reply before pgx receives it. All writes and locks are real PostgreSQL.
type commitLoss struct {
	mode           atomic.Int32 // 0=off, 1=drop successful reply, 2=drop request
	observedCommit atomic.Bool
}
type lossyConn struct {
	net.Conn
	loss      *commitLoss
	dropReply atomic.Bool
}

func (c *lossyConn) Write(p []byte) (int, error) {
	// pgx v5.11.0 sends argument-free COMMIT with a simple-query frame.
	if len(p) >= 5 && p[0] == 'Q' && int(binary.BigEndian.Uint32(p[1:5]))+1 == len(p) && string(p[5:]) == "commit\x00" {
		switch c.loss.mode.Swap(0) {
		case 1:
			c.dropReply.Store(true)
		case 2:
			_ = c.Conn.Close()
			return 0, io.ErrClosedPipe
		}
	}
	return c.Conn.Write(p)
}
func (c *lossyConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if !c.dropReply.Swap(false) {
		return n, err
	}
	// Read complete frames so a split TCP packet cannot be mistaken for proof
	// of a successful commit. Never deliver these bytes to pgx.
	buffer := bytes.Clone(p[:n])
	for {
		for len(buffer) >= 5 {
			size := int(binary.BigEndian.Uint32(buffer[1:5])) + 1
			if size < 5 {
				_ = c.Conn.Close()
				return 0, fmt.Errorf("invalid PostgreSQL response frame")
			}
			if len(buffer) < size {
				break
			}
			if buffer[0] == 'C' && string(buffer[5:size]) == "COMMIT\x00" {
				c.loss.observedCommit.Store(true)
			}
			if buffer[0] == 'Z' {
				_ = c.Conn.Close()
				return 0, io.EOF
			}
			buffer = buffer[size:]
		}
		if err != nil {
			_ = c.Conn.Close()
			return 0, err
		}
		var more [4096]byte
		n, err = c.Conn.Read(more[:])
		buffer = append(buffer, more[:n]...)
	}
}
func faultStore(t *testing.T, s *Store) (*Store, *commitLoss) {
	t.Helper()
	config, err := pgxpool.ParseConfig(os.Getenv("AMENDS_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	if config.ConnConfig.TLSConfig != nil {
		t.Fatal("wire-fault tests require the local sslmode=disable endpoint")
	}
	loss := &commitLoss{}
	dial := config.ConnConfig.DialFunc
	config.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &lossyConn{Conn: conn, loss: loss}, nil
	}
	config.MaxConns = 1
	config.MinConns = 0
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err = pool.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}
	result, err := New(pool, s.config)
	if err != nil {
		t.Fatal(err)
	}
	return result, loss
}

func TestPostgresSC18SC20SC21SC22_LostCommitReplies(t *testing.T) {
	for _, actor := range []string{"worker", "quarantine", "claim", "acknowledge", "view"} {
		for _, committed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/committed=%v", actor, committed), func(t *testing.T) {
				s, rs := fixtureStore(t)
				fault, loss := faultStore(t, s)
				arm := func() {
					if committed {
						loss.mode.Store(1)
					} else {
						loss.mode.Store(2)
					}
				}
				checkUnknown := func(err error) {
					t.Helper()
					if !errors.Is(err, store.ErrCommitUnknown) {
						t.Fatalf("lost reply reported as known: %v", err)
					}
					if loss.mode.Load() != 0 || loss.observedCommit.Load() != committed {
						t.Fatal("fault did not exercise its declared server outcome")
					}
				}
				switch actor {
				case "worker", "quarantine":
					token := claim(t, s, 0)
					r := rs[0].Clone()
					if actor == "quarantine" {
						r.Value = []byte("{")
					}
					arm()
					checkUnknown(fault.Process(t.Context(), token, 0, r))
					measurement := measured(t, s)
					counts := statistics(t, s)
					if (counts != nil) != committed || (committed && (counts.Decisions != 1 || len(counts.Recent) != 1 || counts.Recent[0].Source != r.Coordinates)) {
						t.Fatalf("statistics did not share actual unknown commit outcome: %+v", counts)
					}
					if (measurement != nil) != committed {
						t.Fatal("measurement did not share the unknown decision's actual commit outcome")
					}
					if committed && (measurement.Source != r.Coordinates || (measurement.QuarantineReason != "") != (actor == "quarantine")) {
						t.Fatalf("unknown committed measurement lost source/decision: %+v", measurement)
					}
					before := snapshot(t, s)
					wantNext := int64(0)
					if committed {
						wantNext = 1
					}
					if before.Partitions[0].Next != wantNext {
						t.Fatal("unknown result has wrong durable frontier")
					}
					if !committed && (len(before.Outbox) > 0 || len(before.Rejections) > 0 || len(before.Outputs) > 0) {
						t.Fatal("aborted commit left partial state")
					}
					_, progress, err := s.Observe(t.Context(), 0)
					if err != nil {
						t.Fatal(err)
					}
					if progress.Next == 0 {
						process(t, s, token, r)
					}
					after := snapshot(t, s)
					if counts := statistics(t, s); counts == nil || counts.Decisions != 1 || len(counts.Recent) != 1 {
						t.Fatalf("unknown recovery duplicated processing statistics: %+v", counts)
					}
					if after.Partitions[0].Next != 1 {
						t.Fatal("recovery lost progress")
					}
					if actor == "quarantine" {
						if len(after.Rejections) != 1 || len(after.Outbox) != 0 {
							t.Fatal("quarantine recovery was not atomic")
						}
					} else {
						if len(after.Outbox) != 3 {
							t.Fatal("[amends:committed-intent] recovery duplicated or lost intent")
						}
						for _, e := range after.Outputs {
							if e.Version != 1 {
								t.Fatal("recovery allocated a second semantic version")
							}
						}
					}
				case "claim":
					arm()
					token, err := fault.Claim(t.Context(), 0)
					checkUnknown(err)
					if token.Epoch != 0 {
						t.Fatal("unknown claim exposed a success token")
					}
					observed, progress, err := s.Observe(t.Context(), 0)
					if err != nil {
						t.Fatal(err)
					}
					wantEpoch := int64(0)
					if committed {
						wantEpoch = 1
					}
					if observed.Epoch != wantEpoch || progress.Next != 0 {
						t.Fatal("claim observation differs from durable outcome")
					}
				case "acknowledge":
					process(t, s, claim(t, s, 0), rs[0])
					d, ok, err := s.Pending(t.Context(), 0)
					if err != nil || !ok {
						t.Fatal(err)
					}
					arm()
					checkUnknown(fault.Acknowledge(t.Context(), 0, d.Sequence, 17))
					next, ok, err := s.Pending(t.Context(), 0)
					if err != nil || !ok {
						t.Fatal(err)
					}
					want := int64(1)
					if committed {
						want = 2
					}
					if next.Sequence != want {
						t.Fatal("acknowledgement did not match durable outcome")
					}
					if !committed && !bytes.Equal(next.Bytes, d.Bytes) {
						t.Fatal("retry changed committed bytes")
					}
					if err = s.Acknowledge(t.Context(), 0, d.Sequence, 17); err != nil {
						t.Fatal("receipt recovery", err)
					}
				case "view":
					token := claim(t, s, 0)
					for _, r := range rs {
						process(t, s, token, r)
					}
					id := ledger.OutputID{Namespace: s.config.Namespace, Family: "balance", Key: s.config.Keys[0].StockKey, Day: "2026-01-03"}
					withdrawal := snapshot(t, s).Outputs[id]
					arm()
					checkUnknown(fault.ApplyView(t.Context(), 0, 0, 0, withdrawal))
					v, err := s.View(t.Context())
					if err != nil {
						t.Fatal(err)
					}
					wantNext := int64(0)
					if committed {
						wantNext = 1
					}
					if v.Next[0] != wantNext || (len(v.Rows) == 1) != committed {
						t.Fatal("[amends:view-atomicity] view and frontier did not commit together")
					}
					if v.Next[0] == 0 {
						if err = s.ApplyView(t.Context(), 0, 0, 0, withdrawal); err != nil {
							t.Fatal(err)
						}
					}
					if err = s.ApplyView(t.Context(), 0, 1, 1, withdrawal); err != nil {
						t.Fatal("duplicate view application", err)
					}
					v, err = s.View(t.Context())
					if err != nil {
						t.Fatal(err)
					}
					if v.Next[0] != 2 || v.Rows[id].Version != 3 || v.Rows[id].Payload.Present {
						t.Fatal("recovery lost deletion guard or progress")
					}
				}
			})
		}
	}
}

func TestPostgresSC18_ConnectionDiesDuringCorrection(t *testing.T) {
	s, rs := fixtureStore(t)
	token := claim(t, s, 0)
	for _, r := range rs[:3] {
		process(t, s, token, r)
	}
	before := snapshot(t, s)
	tx, err := s.Begin(t.Context(), token, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = tx.Apply(rs[3]); err != nil {
		t.Fatal(err)
	}
	var killed bool
	if err = s.pool.QueryRow(t.Context(), "SELECT pg_terminate_backend($1)", tx.tx.Conn().PgConn().PID()).Scan(&killed); err != nil || !killed {
		t.Fatal("failed to terminate this test's transaction", err)
	}
	if err = tx.Commit(); !errors.Is(err, store.ErrCommitUnknown) {
		t.Fatalf("terminated connection result: %v", err)
	}
	if !reflect.DeepEqual(before, snapshot(t, s)) {
		t.Fatal("[amends:processing-atomicity] terminated correction left partial outputs or progress")
	}
	replacement := claim(t, s, 0)
	process(t, s, replacement, rs[3])
	after := snapshot(t, s)
	if after.Partitions[0].Next != 4 || len(after.Outbox) != 10 {
		t.Fatal("correction recovery lost/duplicated intent")
	}
	got := after.Keys[s.config.Keys[0].StockKey].Projection
	if len(got.Balances) != 3 || got.Balances[0].Close != 70 || got.Balances[1].Close != 50 || got.Balances[2].Close != 100 || len(got.Alerts) != 1 || got.Alerts[0].Day != "2026-01-02" {
		t.Fatalf("wrong recovered correction: %+v", got)
	}
}
