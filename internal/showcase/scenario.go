// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package showcase

// Scenario selects exactly one owned-pipeline schedule. Empty means CorrectionOnly.
type Scenario string

const (
	CorrectionOnly         Scenario = "correction-only"
	CrashMidCorrection     Scenario = "crash-mid-correction"
	PoisonEvent            Scenario = "poison-event"
	SourceConflict         Scenario = "source-conflict"
	RelayStall             Scenario = "relay-stall"
	DisappearingDay        Scenario = "disappearing-day"
	MaterializerCrashDrill Scenario = "materializer-crash"
	DuplicateFlood         Scenario = "duplicate-flood"
	StaleAmendmentStorm    Scenario = "stale-amendment-storm"
	HistoryMissing         Scenario = "history-missing"
	ZombieWorker           Scenario = "zombie-worker"
	AmbiguousRequest       Scenario = "ambiguous-commit-request"
	AmbiguousReply         Scenario = "ambiguous-commit-reply"
	RebalanceUnderLoad     Scenario = "rebalance-under-load"
)

func (s Scenario) valid() bool {
	switch s {
	case CorrectionOnly, CrashMidCorrection, PoisonEvent, SourceConflict, RelayStall, DisappearingDay, MaterializerCrashDrill, DuplicateFlood, StaleAmendmentStorm, HistoryMissing, ZombieWorker, AmbiguousRequest, AmbiguousReply, RebalanceUnderLoad:
		return true
	default:
		return false
	}
}

func scenarioFixtures(s Scenario) []string {
	names := []string{"config.json", "before.jsonl", "correction.jsonl", "expected-before.json", "expected-after.json"}
	switch s {
	case RebalanceUnderLoad:
		names = append(names, "rebalance-other-key.json", "rebalance-before.jsonl", "rebalance-correction.jsonl", "expected-rebalance-other-before.json", "expected-rebalance-other-after.json")
	case DisappearingDay, MaterializerCrashDrill:
		names = append(names, "cancel-last-day.jsonl", "expected-after-cancel.json")
	case DuplicateFlood:
		names = append(names, "duplicate-deliveries.jsonl")
	case StaleAmendmentStorm:
		names = append(names, "stale-protection.jsonl", "stale-deliveries.jsonl", "expected-after-cancel.json")
	case PoisonEvent:
		names = append(names, "poison-event.jsonl", "poison-repair.jsonl", "expected-poison-repaired.json")
	case SourceConflict:
		names = append(names, "source-conflict.jsonl", "conflict-repair.jsonl", "conflict-other-key.json", "expected-conflict-repaired.json", "expected-conflict-other.json")
	}
	return names
}
