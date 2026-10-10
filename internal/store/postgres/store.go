// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Package postgres persists processing and view state under actual PostgreSQL
// transactions. Broker assignment and transport remain the caller's concerns.
package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/internal/fold"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/migrations"
)

const formatVersion = 1

// Store operates on one namespace using a caller-owned connection pool.
// Construct it with [New], then explicitly migrate and initialize the namespace.
// A Store may be used concurrently; processing transactions serialize on their
// partition rows. The caller must keep the pool open for all operations.
type Store struct {
	pool   *pgxpool.Pool
	config ledger.Config
	digest string
	ns     string
}

// New validates and copies c without querying or changing the database. It does
// not take ownership of pool. Use [Migrate] and [Store.Ensure] before processing,
// or [Store.CreateFresh] when adopting an existing namespace must be forbidden.
func New(pool *pgxpool.Pool, c ledger.Config) (*Store, error) {
	if pool == nil {
		return nil, fmt.Errorf("nil PostgreSQL pool")
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if _, err := json.Marshal(c); err != nil {
		return nil, fmt.Errorf("configuration: %w", err)
	}
	return &Store{pool: pool, config: c.Clone(), digest: c.Digest(), ns: namespaceID(c.Namespace)}, nil
}

func namespaceID(namespace string) string {
	raw, _ := json.Marshal(namespace)
	return string(raw)
}
func keyID(key ledger.StockKey) string {
	raw, _ := json.Marshal([]string{key.Item, key.Location})
	return string(raw)
}

// begin bounds waits and transaction lifetime. pgx does not automatically roll
// back when a context is cancelled, so every caller must defer rollback.
func begin(ctx context.Context, pool *pgxpool.Pool, options pgx.TxOptions) (pgx.Tx, context.Context, context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	tx, err := pool.BeginTx(ctx, options)
	if err != nil {
		cancel()
		return nil, ctx, func() {}, err
	}
	_, err = tx.Exec(ctx, "SET LOCAL lock_timeout = '3s'; SET LOCAL statement_timeout = '10s'; SET LOCAL idle_in_transaction_session_timeout = '15s'; SET LOCAL synchronous_commit = 'on'")
	if err != nil {
		rollback(tx)
		cancel()
		return nil, ctx, func() {}, err
	}
	return tx, ctx, cancel, nil
}
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// ErrTxClosed is expected after commit. pgx discards a connection whose
	// rollback fails; cleanup must not overwrite the original operation error.
	_ = tx.Rollback(ctx)
}
func commit(ctx context.Context, tx pgx.Tx) error {
	err := tx.Commit(ctx)
	if err == nil || errors.Is(err, pgx.ErrTxCommitRollback) || errors.Is(err, pgx.ErrTxClosed) {
		return err
	}
	// Conservatively require durable re-observation on any unclassified failure.
	// In particular, cancellation or a socket error does not establish rollback.
	return fmt.Errorf("%w: %w", store.ErrCommitUnknown, err)
}

// Migrate serializes schema installation and refuses changed migration bytes.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	tx, ctx, cancel, err := begin(ctx, pool, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer cancel()
	defer rollback(tx)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(70890124001)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS amends_schema_migrations (version INTEGER PRIMARY KEY, checksum TEXT NOT NULL, installed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())"); err != nil {
		return err
	}
	scripts := migrations.Ordered()
	checksums := make([]string, len(scripts))
	for i, script := range scripts {
		sum := sha256.Sum256([]byte(script))
		checksums[i] = hex.EncodeToString(sum[:])
	}
	rows, err := tx.Query(ctx, "SELECT version, checksum FROM amends_schema_migrations ORDER BY version")
	if err != nil {
		return err
	}
	applied := 0
	for rows.Next() {
		var version int
		var got string
		if err = rows.Scan(&version, &got); err != nil {
			rows.Close()
			return err
		}
		if version != applied+1 || version > len(scripts) || got != checksums[version-1] {
			rows.Close()
			return fmt.Errorf("unsupported or modified migration %d", version)
		}
		applied++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for i := applied; i < len(scripts); i++ {
		if _, err = tx.Exec(ctx, scripts[i]); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "INSERT INTO amends_schema_migrations(version,checksum) VALUES($1,$2)", i+1, checksums[i]); err != nil {
			return err
		}
	}
	return commit(ctx, tx)
}

// Ensure creates a fresh namespace or validates the existing immutable manifest.
// It never fills in missing state for an existing namespace.
func (s *Store) Ensure(ctx context.Context) error {
	return s.ensure(ctx, false)
}

// CreateFresh initializes an unused namespace and refuses existing state.
// An unknown commit requires inspection; retrying must not adopt the destination.
func (s *Store) CreateFresh(ctx context.Context) error {
	return s.ensure(ctx, true)
}

// Exists reports whether the namespace is present. Missing schema returns false
// without creating it; an existing namespace still needs [Store.CheckConfig].
func (s *Store) Exists(ctx context.Context) (bool, error) {
	var installed bool
	if err := s.pool.QueryRow(ctx, "SELECT to_regclass('amends_namespaces') IS NOT NULL").Scan(&installed); err != nil || !installed {
		return false, err
	}
	var exists bool
	err := s.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM amends_namespaces WHERE namespace_id=$1)", s.ns).Scan(&exists)
	return exists, err
}

// CheckConfig checks the persisted namespace configuration and storage format
// against this Store without initializing or repairing state.
func (s *Store) CheckConfig(ctx context.Context) error {
	transaction, ctx, cancel, err := begin(ctx, s.pool, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer cancel()
	defer rollback(transaction)
	return s.compatible(ctx, transaction)
}

func (s *Store) ensure(ctx context.Context, fresh bool) error {
	tx, ctx, cancel, err := begin(ctx, s.pool, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer cancel()
	defer rollback(tx)
	raw, err := json.Marshal(s.config)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, "INSERT INTO amends_namespaces(namespace_id,config_digest,format_version,config) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING", s.ns, s.digest, formatVersion, raw)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		if fresh {
			return fmt.Errorf("rebuild requires an unused namespace: %w", store.ErrFence)
		}
		if err := s.compatible(ctx, tx); err != nil {
			return err
		}
		return commit(ctx, tx)
	}
	for p := 0; p < s.config.Partitions; p++ {
		if _, err = tx.Exec(ctx, "INSERT INTO amends_partitions(namespace_id,partition) VALUES($1,$2)", s.ns, p); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "INSERT INTO amends_view_progress(namespace_id,partition) VALUES($1,$2)", s.ns, p); err != nil {
			return err
		}
	}
	for _, k := range s.config.Keys {
		raw, err = encodeKeyState(fold.Empty(k.StockKey))
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "INSERT INTO amends_key_state(namespace_id,key_id,partition,state) VALUES($1,$2,$3,$4)", s.ns, keyID(k.StockKey), k.Partition, raw); err != nil {
			return err
		}
	}
	return commit(ctx, tx)
}

func (s *Store) compatible(ctx context.Context, tx pgx.Tx) error {
	var digest string
	var version int
	var raw []byte
	if err := tx.QueryRow(ctx, "SELECT config_digest,format_version,config FROM amends_namespaces WHERE namespace_id=$1", s.ns).Scan(&digest, &version, &raw); err != nil {
		return err
	}
	var c ledger.Config
	if err := json.Unmarshal(raw, &c); err != nil {
		return err
	}
	if version != formatVersion || digest != s.digest || c.Digest() != s.digest {
		return store.ErrFence
	}
	return nil
}

func (s *Store) lock(ctx context.Context, tx pgx.Tx, partition int) (store.Partition, error) {
	var p store.Partition
	if partition < 0 || partition >= s.config.Partitions {
		return p, store.ErrFence
	}
	err := tx.QueryRow(ctx, "SELECT epoch,next_offset,emission_sequence FROM amends_partitions WHERE namespace_id=$1 AND partition=$2 FOR UPDATE", s.ns, partition).Scan(&p.Epoch, &p.Next, &p.Sequence)
	if err != nil {
		return p, err
	}
	return p, s.compatible(ctx, tx)
}

// Claim increments the ownership epoch under the processing partition lock and
// returns the committed token. The caller must hold a current broker assignment.
// An error matching [store.ErrCommitUnknown] requires durable observation and a
// fresh guarded claim; observing an epoch alone cannot prove this caller owns it.
func (s *Store) Claim(ctx context.Context, partition int) (store.Token, error) {
	tx, ctx, cancel, err := begin(ctx, s.pool, pgx.TxOptions{})
	if err != nil {
		return store.Token{}, err
	}
	defer cancel()
	defer rollback(tx)
	p, err := s.lock(ctx, tx, partition)
	if err != nil {
		return store.Token{}, err
	}
	if p.Epoch == math.MaxInt64 {
		return store.Token{}, fmt.Errorf("ownership epoch overflow")
	}
	p.Epoch++
	if _, err = tx.Exec(ctx, "UPDATE amends_partitions SET epoch=$3 WHERE namespace_id=$1 AND partition=$2", s.ns, partition, p.Epoch); err != nil {
		return store.Token{}, err
	}
	if err = commit(ctx, tx); err != nil {
		return store.Token{}, err
	}
	return store.Token{Namespace: s.config.Namespace, Digest: s.digest, Partition: partition, Epoch: p.Epoch}, nil
}

// Observe returns the installed token and source progress in one SQL snapshot.
// It does not claim assignment authority. The broker adapter must independently
// prove its local assignment is still current before resuming.
func (s *Store) Observe(ctx context.Context, partition int) (store.Token, store.Partition, error) {
	tx, ctx, cancel, err := begin(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return store.Token{}, store.Partition{}, err
	}
	defer cancel()
	defer rollback(tx)
	var p store.Partition
	if err = s.compatible(ctx, tx); err != nil {
		return store.Token{}, p, err
	}
	if err = tx.QueryRow(ctx, "SELECT epoch,next_offset,emission_sequence FROM amends_partitions WHERE namespace_id=$1 AND partition=$2", s.ns, partition).Scan(&p.Epoch, &p.Next, &p.Sequence); err != nil {
		return store.Token{}, p, err
	}
	return store.Token{Namespace: s.config.Namespace, Digest: s.digest, Partition: partition, Epoch: p.Epoch}, p, nil
}
