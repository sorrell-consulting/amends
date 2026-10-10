package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/sorrell-consulting/amends/internal/diff"
	"github.com/sorrell-consulting/amends/internal/fold"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
)

// Replay checks records from the already-consumed range [from, until) against
// intact evidence under observed and frontier. The caller supplies the complete
// ordered retained range and parks normal processing before execution.
//
// With execute=false it returns the current epoch without mutation. Execution
// commits only a higher epoch; business state, versions, and source progress stay
// unchanged. [store.ErrCommitUnknown] requires re-observation before resumption.
func (s *Store) Replay(ctx context.Context, observed store.Token, frontier, from, until int64, records []ledger.Record, execute bool) (int64, error) {
	if from < 0 || until <= from || until > frontier || observed.Epoch < 1 {
		return 0, fmt.Errorf("replay requires a nonempty already-consumed range")
	}
	tx, ctx, cancel, err := begin(ctx, s.pool, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer cancel()
	defer rollback(tx)
	partition, err := s.lock(ctx, tx, observed.Partition)
	if err != nil {
		return 0, err
	}
	if observed.Namespace != s.config.Namespace || observed.Digest != s.digest || observed.Epoch != partition.Epoch || frontier != partition.Next {
		return 0, store.ErrFence
	}
	outputs, err := loadEnvelopes(ctx, tx, s.ns, observed.Partition, "amends_output_state")
	if err != nil {
		return 0, err
	}
	cursor := from
	for _, record := range records {
		if record.Source != s.config.Source || record.Partition != observed.Partition || record.Offset < cursor || record.Offset >= until {
			return 0, fmt.Errorf("unsupported replay coordinates")
		}
		if err := s.checkReplay(ctx, tx, record, outputs); err != nil {
			return 0, err
		}
		cursor = record.Offset + 1
	}
	if cursor != until {
		return 0, fmt.Errorf("replay scan did not reach the requested boundary")
	}
	if !execute {
		return partition.Epoch, nil
	}
	if partition.Epoch == math.MaxInt64 {
		return 0, fmt.Errorf("ownership epoch overflow")
	}
	epoch := partition.Epoch + 1
	if _, err = tx.Exec(ctx, "UPDATE amends_partitions SET epoch=$3 WHERE namespace_id=$1 AND partition=$2", s.ns, observed.Partition, epoch); err != nil {
		return 0, err
	}
	if err = commit(ctx, tx); err != nil {
		return 0, err
	}
	return epoch, nil
}

func (s *Store) checkReplay(ctx context.Context, tx pgx.Tx, record ledger.Record, outputs map[ledger.OutputID]ledger.Envelope) error {
	transaction, reason := fold.Validate(s.config, record)
	var raw []byte
	if reason != "" {
		var retainedReason string
		if err := tx.QueryRow(ctx, "SELECT record,reason FROM amends_quarantine WHERE namespace_id=$1 AND partition=$2 AND source_offset=$3", s.ns, record.Partition, record.Offset).Scan(&raw, &retainedReason); err != nil {
			return fmt.Errorf("missing replay rejection evidence: %w", err)
		}
		var retained ledger.Record
		if err := json.Unmarshal(raw, &retained); err != nil {
			return err
		}
		if retainedReason != reason || retained.Coordinates != record.Coordinates || retained.Key != record.Key || !bytes.Equal(retained.RawKey, record.RawKey) || !bytes.Equal(retained.Value, record.Value) {
			return fmt.Errorf("replay rejection evidence differs")
		}
		return nil
	}
	if err := tx.QueryRow(ctx, "SELECT state FROM amends_key_state WHERE namespace_id=$1 AND key_id=$2 AND partition=$3", s.ns, keyID(transaction.StockKey), record.Partition).Scan(&raw); err != nil {
		return err
	}
	state, err := decodeKeyState(raw)
	if err != nil {
		return err
	}
	found := false
	for _, candidate := range state.Identities[transaction.TxnID].Revisions[transaction.Revision] {
		if fold.CanonicalEqual(candidate.Transaction, transaction) && slices.Contains(candidate.Sources, record.Coordinates) {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("missing replay authority evidence")
	}
	key, _ := s.config.Key(transaction.StockKey)
	next, err := fold.Apply(state, transaction, record.Coordinates, key)
	if err != nil {
		return err
	}
	messages, err := diff.Reconcile(s.config.Namespace, outputs, next.Projection, ledger.Cause{Coordinates: record.Coordinates, TxnID: transaction.TxnID, Revision: transaction.Revision})
	if err != nil {
		return err
	}
	if len(messages) != 0 {
		return fmt.Errorf("replay requires state repair; use an isolated rebuild")
	}
	return nil
}
