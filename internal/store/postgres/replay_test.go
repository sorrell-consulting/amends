//go:build integration

package postgres

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
)

func TestPostgresSC23_ReplayPreservesIntactState(t *testing.T) {
	state, records := fixtureStore(t)
	token := claim(t, state, 0)
	poison := records[0].Clone()
	poison.Offset = int64(len(records))
	poison.RawKey, poison.Value = []byte("invalid key"), []byte("{")
	records = append(records, poison)
	for _, record := range records {
		process(t, state, token, record)
	}
	before := snapshot(t, state)
	frontier := before.Partitions[0].Next
	if epoch, err := state.Replay(t.Context(), token, frontier, 0, frontier, records, false); err != nil || epoch != token.Epoch {
		t.Fatalf("dry run: epoch=%d err=%v", epoch, err)
	}
	if !reflect.DeepEqual(before, snapshot(t, state)) {
		t.Fatal("dry run changed durable state")
	}
	epoch, err := state.Replay(t.Context(), token, frontier, 0, frontier, records, true)
	if err != nil || epoch != token.Epoch+1 {
		t.Fatalf("replay: epoch=%d err=%v", epoch, err)
	}
	partition := before.Partitions[0]
	partition.Epoch = epoch
	before.Partitions[0] = partition
	if !reflect.DeepEqual(before, snapshot(t, state)) {
		t.Fatal("replay changed authority, tombstones, versions, quarantine, outbox, or frontier")
	}
	if err := state.Process(t.Context(), token, frontier, records[0]); !errors.Is(err, store.ErrFence) {
		t.Fatalf("replay did not fence previous owner: %v", err)
	}
	resume := claim(t, state, 0)
	if resume.Epoch <= epoch {
		t.Fatal("normal owner did not acquire newer epoch")
	}
}

func TestPostgresSC23_ReplayRejectsBeforeMutation(t *testing.T) {
	state, records := fixtureStore(t)
	token := claim(t, state, 0)
	for _, record := range records {
		process(t, state, token, record)
	}
	before := snapshot(t, state)
	frontier := before.Partitions[0].Next
	for _, test := range []struct {
		name        string
		from, until int64
		records     []ledger.Record
	}{
		{"unprocessed", 0, frontier + 1, records},
		{"negative", -1, frontier, records},
		{"empty", 0, 0, nil},
		{"missing suffix", 0, frontier, records[:len(records)-1]},
		{"reordered", 0, frontier, []ledger.Record{records[1], records[0]}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := state.Replay(t.Context(), token, frontier, test.from, test.until, test.records, true); err == nil {
				t.Fatal("unsupported replay accepted")
			}
			if !reflect.DeepEqual(before, snapshot(t, state)) {
				t.Fatal("rejected replay changed durable state")
			}
		})
	}
	for _, value := range []string{`{`, `{"txn_id":"missing","revision":1,"kind":"NEW","stock_key":{"item":"DEMO-PART","location":"DEPOT-A"},"valid_time":"2026-01-01T00:00:00Z","qty":1,"source_time":"2026-01-01T00:00:00Z"}`} {
		forged := records[0].Clone()
		forged.Value = []byte(value)
		if _, err := state.Replay(t.Context(), token, frontier, 0, 1, []ledger.Record{forged}, true); err == nil {
			t.Fatal("missing replay evidence accepted")
		}
		if !reflect.DeepEqual(before, snapshot(t, state)) {
			t.Fatal("missing evidence changed durable state")
		}
	}
}

func TestPostgresSC19SC23_ReplayWaitsAndRejectsChangedFrontier(t *testing.T) {
	state, records := fixtureStore(t)
	token := claim(t, state, 0)
	process(t, state, token, records[0])
	transaction, err := state.Begin(t.Context(), token, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer transaction.Rollback()
	if err = transaction.Apply(records[1]); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, replayErr := state.Replay(t.Context(), token, 1, 0, 1, records[:1], true)
		done <- replayErr
	}()
	waitBlocked(t, state.pool, transaction.tx.Conn().PgConn().PID())
	if err = transaction.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = <-done; !errors.Is(err, store.ErrFence) {
		t.Fatalf("changed frontier accepted: %v", err)
	}
	after := snapshot(t, state)
	if after.Partitions[0].Epoch != token.Epoch || after.Partitions[0].Next != 2 {
		t.Fatal("rejected replay partially installed ownership")
	}
}

func TestPostgresSC23_ReplayUnknownCommit(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprint(committed), func(t *testing.T) {
			state, records := fixtureStore(t)
			token := claim(t, state, 0)
			process(t, state, token, records[0])
			before := snapshot(t, state)
			fault, loss := faultStore(t, state)
			mode := int32(2)
			if committed {
				mode = 1
			}
			loss.mode.Store(mode)
			if epoch, err := fault.Replay(t.Context(), token, 1, 0, 1, records[:1], true); epoch != 0 || !errors.Is(err, store.ErrCommitUnknown) {
				t.Fatalf("unknown replay reported success: epoch=%d err=%v", epoch, err)
			}
			if loss.observedCommit.Load() != committed || loss.mode.Load() != 0 {
				t.Fatal("declared commit outcome not exercised")
			}
			if committed {
				partition := before.Partitions[0]
				partition.Epoch++
				before.Partitions[0] = partition
			}
			if !reflect.DeepEqual(before, snapshot(t, state)) {
				t.Fatal("unknown replay changed business state or frontier")
			}
		})
	}
}

func TestPostgresSC23_ReplayRefusesStateRepair(t *testing.T) {
	state, records := fixtureStore(t)
	token := claim(t, state, 0)
	process(t, state, token, records[0])
	before := snapshot(t, state)
	for identity, envelope := range before.Outputs {
		if envelope.Type == "BalanceStated" {
			if _, err := state.pool.Exec(t.Context(), "UPDATE amends_output_state SET envelope=$3 WHERE namespace_id=$1 AND output_id=$2", state.ns, identity.String(), []byte("{}")); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	corrupt := snapshot(t, state)
	if reflect.DeepEqual(before, corrupt) {
		t.Fatal("test did not corrupt retained output state")
	}
	if _, err := state.Replay(t.Context(), token, 1, 0, 1, records[:1], true); err == nil {
		t.Fatal("replay silently repaired corrupt state")
	}
	if !reflect.DeepEqual(corrupt, snapshot(t, state)) {
		t.Fatal("refused repair changed durable state")
	}
}

func TestPostgresSC24_FreshNamespaceAndSharedSourceGuard(t *testing.T) {
	state, records := fixtureStore(t)
	token := claim(t, state, 0)
	process(t, state, token, records[0])
	before := snapshot(t, state)
	if err := state.CreateFresh(t.Context()); !errors.Is(err, store.ErrFence) {
		t.Fatalf("existing namespace adopted: %v", err)
	}
	if !reflect.DeepEqual(before, snapshot(t, state)) {
		t.Fatal("fresh creation changed existing state")
	}
	other := newStore(t, state.pool, state.config)
	guard, err := state.AcquireService(t.Context(), "producer")
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()
	if duplicate, err := other.AcquireService(t.Context(), "producer"); !errors.Is(err, ErrServiceBusy) {
		if duplicate != nil {
			duplicate.Close()
		}
		t.Fatalf("same source in different namespace bypassed writer guard: %v", err)
	}
	if err := guard.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
}
