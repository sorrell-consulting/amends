package showcase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/sorrell-consulting/amends/internal/worker"
)

type RebalanceAttempt struct {
	Worker   string             `json:"worker"`
	Token    store.Token        `json:"token"`
	Expected int64              `json:"expected_frontier"`
	Record   ledger.Coordinates `json:"record"`
}

type RebalanceWorkerEvidence struct {
	Name      string             `json:"name"`
	Joined    bool               `json:"joined"`
	Claims    []store.Token      `json:"acknowledged_claims"`
	Committed []RebalanceAttempt `json:"acknowledged_decisions"`
}

type rebalanceGates struct {
	mu               sync.Mutex
	baseline         map[int]store.Partition
	held             bool
	successors       map[int]bool
	entered          chan RebalanceAttempt
	release          chan struct{}
	successorEntered chan RebalanceAttempt
	successorRelease chan struct{}
}

func newRebalanceGates() *rebalanceGates {
	return &rebalanceGates{successors: map[int]bool{}, entered: make(chan RebalanceAttempt, 1), release: make(chan struct{}), successorEntered: make(chan RebalanceAttempt, 2), successorRelease: make(chan struct{})}
}

func (gates *rebalanceGates) arm(baseline map[int]store.Partition) {
	gates.mu.Lock()
	defer gates.mu.Unlock()
	gates.baseline = baseline
}

func (gates *rebalanceGates) before(ctx context.Context, attempt RebalanceAttempt) error {
	if attempt.Record.Offset < 3 {
		return ctx.Err()
	}
	gates.mu.Lock()
	baseline, ok := gates.baseline[attempt.Record.Partition]
	if !ok {
		gates.mu.Unlock()
		return fmt.Errorf("rebalance load reached an unarmed partition")
	}
	var entered chan RebalanceAttempt
	var release chan struct{}
	if !gates.held {
		gates.held = true
		entered, release = gates.entered, gates.release
	} else if attempt.Token.Epoch > baseline.Epoch && !gates.successors[attempt.Record.Partition] {
		gates.successors[attempt.Record.Partition] = true
		entered, release = gates.successorEntered, gates.successorRelease
	}
	gates.mu.Unlock()
	if entered != nil {
		entered <- attempt
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return ctx.Err()
}

type rebalanceState struct {
	*postgres.Store
	gates    *rebalanceGates
	evidence *RebalanceWorkerEvidence
}

func (state *rebalanceState) Claim(ctx context.Context, partition int) (store.Token, error) {
	token, err := state.Store.Claim(ctx, partition)
	if err == nil {
		state.evidence.Claims = append(state.evidence.Claims, token)
	}
	return token, err
}

func (state *rebalanceState) Process(ctx context.Context, token store.Token, expected int64, record ledger.Record) error {
	attempt := RebalanceAttempt{Worker: state.evidence.Name, Token: token, Expected: expected, Record: record.Coordinates}
	if err := state.gates.before(ctx, attempt); err != nil {
		return err
	}
	if err := state.Store.Process(ctx, token, expected, record); err != nil {
		return err
	}
	state.evidence.Committed = append(state.evidence.Committed, attempt)
	return nil
}

type rebalanceWorker struct {
	cancel   context.CancelFunc
	done     chan struct{}
	err      error
	log      *os.File
	evidence *RebalanceWorkerEvidence
}

func (runner *runner) startRebalanceWorker(ctx context.Context, name string, gates *rebalanceGates) (*rebalanceWorker, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	log, err := os.OpenFile(filepath.Join(runner.report.Directory, name+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	evidence := &RebalanceWorkerEvidence{Name: name}
	actor := &rebalanceWorker{cancel: cancel, done: make(chan struct{}), log: log, evidence: evidence}
	state := &rebalanceState{Store: runner.state, gates: gates, evidence: evidence}
	go func() {
		actor.err = worker.Run(ctx, worker.Options{Brokers: runner.options.Brokers, Topic: runner.manifest.Topic, Config: runner.manifest.Config, State: state, Logger: slog.New(slog.NewJSONHandler(log, nil))})
		close(actor.done)
	}()
	return actor, nil
}

func rebalanceHealthy(actors []*rebalanceWorker) error {
	for _, actor := range actors {
		select {
		case <-actor.done:
			return fmt.Errorf("%s stopped unexpectedly: %v", actor.evidence.Name, actor.err)
		default:
		}
	}
	return nil
}

func joinRebalanceWorkers(actors []*rebalanceWorker) error {
	for _, actor := range actors {
		actor.cancel()
	}
	var result error
	for _, actor := range actors {
		if actor.evidence.Joined {
			continue
		}
		<-actor.done
		actor.evidence.Joined = true
		if !errors.Is(actor.err, context.Canceled) {
			result = errors.Join(result, fmt.Errorf("%s did not stop cleanly: %v", actor.evidence.Name, actor.err))
		}
		result = errors.Join(result, actor.log.Close())
	}
	return result
}
