package showcase

import "context"

// Checkpoint names a completed observation before the next scenario action.
// Notifications run synchronously in the showcase controller, never in a worker,
// store transaction, or fault gate. A callback may cancel the caller's context.
type Checkpoint string

const (
	BaselineVerified         Checkpoint = "baseline-verified"
	DiagnosticVerified       Checkpoint = "diagnostic-verified"
	RelayStalled             Checkpoint = "relay-stalled"
	WithdrawalVerified       Checkpoint = "withdrawal-verified"
	MaterializerKilled       Checkpoint = "materializer-killed"
	MaterializerCommitKilled Checkpoint = "materializer-commit-killed"
	InputReady               Checkpoint = "input-ready"
	StaleFirstVerified       Checkpoint = "stale-first-verified"
	HistoryReady             Checkpoint = "history-ready"
	HistoryLost              Checkpoint = "history-lost"
	ZombieReady              Checkpoint = "zombie-ready"
	ZombieFenced             Checkpoint = "zombie-fenced"
	CommitReady              Checkpoint = "commit-ready"
	CommitUnknown            Checkpoint = "commit-unknown"
	RebalanceReady           Checkpoint = "rebalance-ready"
	RebalancePreparing       Checkpoint = "rebalance-preparing"
	RebalanceReassigned      Checkpoint = "rebalance-reassigned"
)

func (r *runner) checkpoint(ctx context.Context, point Checkpoint) error {
	if r.options.OnCheckpoint != nil {
		r.options.OnCheckpoint(point)
	}
	return ctx.Err()
}
