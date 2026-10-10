// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sorrell-consulting/amends/internal/store"
)

// BindOutput installs a fresh output lineage, or accepts exactly the existing
// binding after an unknown commit. Callers must prove the new topic is empty.
// Existing processing/outbox intent is allowed; prior publication or view work
// without this binding is not safe to adopt.
func (s *Store) BindOutput(ctx context.Context, b store.OutputBinding) error {
	if b.Topic == "" || b.Incarnation == "" || b.Incarnation == s.config.Source || b.Partitions != s.config.Partitions {
		return store.ErrFence
	}
	tx, ctx, cancel, err := begin(ctx, s.pool, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer cancel()
	defer rollback(tx)
	if err = s.compatible(ctx, tx); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "SELECT namespace_id FROM amends_namespaces WHERE namespace_id=$1 FOR UPDATE", s.ns); err != nil {
		return err
	}
	var got store.OutputBinding
	err = tx.QueryRow(ctx, "SELECT topic_name,incarnation,partitions FROM amends_output_binding WHERE namespace_id=$1", s.ns).Scan(&got.Topic, &got.Incarnation, &got.Partitions)
	if err == nil {
		if got != b {
			return store.ErrFence
		}
		return commit(ctx, tx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	for p := 0; p < s.config.Partitions; p++ {
		if _, err = s.lock(ctx, tx, p); err != nil {
			return err
		}
		var next int64
		var stopped *string
		if err = tx.QueryRow(ctx, "SELECT next_offset,protocol_error FROM amends_view_progress WHERE namespace_id=$1 AND partition=$2 FOR UPDATE", s.ns, p).Scan(&next, &stopped); err != nil {
			return err
		}
		if next != 0 || stopped != nil {
			return fmt.Errorf("cannot bind previously used view: %w", store.ErrFence)
		}
	}
	var used bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM amends_outbox WHERE namespace_id=$1 AND acknowledged_at IS NOT NULL) OR EXISTS(SELECT 1 FROM amends_view_rows WHERE namespace_id=$1)`, s.ns).Scan(&used); err != nil {
		return err
	}
	if used {
		return fmt.Errorf("cannot bind previously published/materialized state: %w", store.ErrFence)
	}
	if _, err = tx.Exec(ctx, "INSERT INTO amends_output_binding(namespace_id,topic_name,incarnation,partitions) VALUES($1,$2,$3,$4)", s.ns, b.Topic, b.Incarnation, b.Partitions); err != nil {
		return err
	}
	return commit(ctx, tx)
}

// OutputBinding returns the namespace's installed output identity. An unbound
// namespace returns [pgx.ErrNoRows]; it does not acquire a binding implicitly.
func (s *Store) OutputBinding(ctx context.Context) (store.OutputBinding, error) {
	var b store.OutputBinding
	tx, ctx, cancel, err := begin(ctx, s.pool, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return b, err
	}
	defer cancel()
	defer rollback(tx)
	if err = s.compatible(ctx, tx); err != nil {
		return b, err
	}
	err = tx.QueryRow(ctx, "SELECT topic_name,incarnation,partitions FROM amends_output_binding WHERE namespace_id=$1", s.ns).Scan(&b.Topic, &b.Incarnation, &b.Partitions)
	return b, err
}

// Receipt returns the acknowledged broker offset for a committed outbox entry.
// A nil offset with no error means the entry is still pending; a missing entry
// returns [pgx.ErrNoRows]. Use it to resolve an unknown Acknowledge outcome.
func (s *Store) Receipt(ctx context.Context, p int, sequence int64) (*int64, error) {
	tx, ctx, cancel, err := begin(ctx, s.pool, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer cancel()
	defer rollback(tx)
	if err = s.compatible(ctx, tx); err != nil {
		return nil, err
	}
	var offset *int64
	err = tx.QueryRow(ctx, "SELECT output_offset FROM amends_outbox WHERE namespace_id=$1 AND partition=$2 AND sequence=$3", s.ns, p, sequence).Scan(&offset)
	return offset, err
}

// ErrServiceBusy means another session holds the requested service guard.
// Producer guards are shared by source incarnation across namespaces.
var ErrServiceBusy = errors.New("namespace service already running")

// Service owns a dedicated PostgreSQL session, not a renewable distributed
// lease. It prevents accidental duplicate local service starts. Losing this
// connection is fatal; it is never silently reconnected. It cannot fence a
// broker request that was already in flight when the connection disappeared.
// Service is not safe for concurrent use. Close it after guarded work has joined.
type Service struct{ conn *pgx.Conn }

// AcquireService takes a session guard for role: "producer", "relay", or "view".
// Producer guards use source incarnation; the others use namespace identity.
// A held guard returns [ErrServiceBusy] without waiting for that owner to exit.
// Callers must close the returned Service and stop on any failed Check.
func (s *Store) AcquireService(ctx context.Context, role string) (*Service, error) {
	if role != "relay" && role != "view" && role != "producer" {
		return nil, fmt.Errorf("unknown service role")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	c, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	conn := c.Hijack() // Session locks must never return to the connection pool.
	service := &Service{conn: conn}
	identity := s.ns
	if role == "producer" {
		identity = s.config.Source
	}
	sum := sha256.Sum256([]byte(identity + "/" + role))
	var acquired bool
	err = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", int64(binary.BigEndian.Uint64(sum[:8]))).Scan(&acquired)
	if err != nil || !acquired {
		service.Close()
		if err != nil {
			return nil, err
		}
		return nil, ErrServiceBusy
	}
	return service, nil
}

// Check verifies that the guarded session is still reachable. Failure requires
// retirement; it cannot establish that an outstanding broker request has stopped.
func (s *Service) Check(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err := s.conn.Exec(ctx, "SELECT 1")
	return err
}

// Close closes the dedicated connection, releasing its session guard. It does
// not wait for broker operations or application goroutines to finish.
func (s *Service) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.conn.Close(ctx) // Connection close releases this session's lock.
}
