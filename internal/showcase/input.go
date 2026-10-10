package showcase

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"

	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/fold"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
)

type inputCheckpoint struct {
	state  store.Snapshot
	tables map[string]json.RawMessage
}

type inputEvidence struct {
	SourceFrom         int64                           `json:"source_from"`
	SourceNext         int64                           `json:"source_next"`
	AppendedRecords    int64                           `json:"appended_records"`
	RetainedReferences int64                           `json:"retained_source_references"`
	OutputEnd          []int64                         `json:"output_end"`
	AuthorityUnchanged bool                            `json:"authority_unchanged"`
	OutputUnchanged    bool                            `json:"output_intent_and_receipts_unchanged"`
	ViewUnchanged      bool                            `json:"view_rows_and_progress_unchanged"`
	LastProcessing     *postgres.ProcessingMeasurement `json:"last_processing"`
}

func (runner *runner) inputDrill(ctx context.Context) error {
	stale := runner.options.Scenario == StaleAmendmentStorm
	inputFile, expectedFile, stage := "duplicate-deliveries.jsonl", "expected-after.json", "duplicates"
	sourceEnd, outputEnd := int64(4), int64(10)
	if stale {
		inputFile, expectedFile, stage = "stale-deliveries.jsonl", "expected-after-cancel.json", "stale-storm"
		sourceEnd, outputEnd = 6, 11
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		if runner.relay, err = runner.role("relay-protected", "relay"); err != nil {
			return err
		}
		if err = runner.load(ctx, "stale-protection"); err != nil {
			return err
		}
		if err = runner.awaitSourceOutbox(ctx, sourceExpectation{Next: sourceEnd}, sourceExpectation{}); err != nil {
			return err
		}
		if err = runner.relay.stop(); err != nil {
			return err
		}
		if err = runner.fixtureStage(ctx, "protected", sourceEnd, expectedFile); err != nil {
			return err
		}
	}
	before, err := runner.inputSnapshot(ctx, "baseline")
	if err != nil {
		return err
	}
	raw, err := fixtures.Files.ReadFile(inputFile)
	if err != nil {
		return err
	}
	burst := bytes.Repeat(raw, 64)
	sum := sha256.Sum256(burst)
	runner.report.SHA256["input-burst.jsonl"] = hex.EncodeToString(sum[:])
	if err = os.WriteFile(filepath.Join(runner.report.Directory, "input-burst.jsonl"), burst, 0600); err != nil {
		return err
	}
	if _, err = fmt.Fprintln(runner.options.Output, "input-ready: verified baseline retained; finite source deliveries follow with the relay stopped."); err != nil {
		return err
	}
	if err = runner.checkpoint(ctx, InputReady); err != nil {
		return err
	}
	if stale {
		if err = runner.load(ctx, "stale-deliveries"); err != nil {
			return err
		}
		sourceEnd += 2
		if err = runner.awaitSourceOutbox(ctx, sourceExpectation{Next: sourceEnd}, sourceExpectation{}); err != nil {
			return err
		}
		if err = runner.fixtureStage(ctx, "stale-first", sourceEnd, expectedFile); err != nil {
			return err
		}
		if err = runner.checkpoint(ctx, StaleFirstVerified); err != nil {
			return err
		}
		if err = runner.checkInput(ctx, "stale-first", before, sourceEnd, outputEnd, false, true); err != nil {
			return err
		}
	}
	if err = runner.load(ctx, "input-burst"); err != nil {
		return err
	}
	sourceEnd += 128
	if err = runner.awaitSourceOutbox(ctx, sourceExpectation{Next: sourceEnd}, sourceExpectation{}); err != nil {
		return err
	}
	if err = runner.fixtureStage(ctx, stage, sourceEnd, expectedFile); err != nil {
		return err
	}
	if err = runner.checkInput(ctx, stage, before, sourceEnd, outputEnd, true, stale); err != nil {
		return err
	}
	_, err = fmt.Fprintf(runner.options.Output, "input: %d new source references retained; authority, output bytes/versions/receipts, and view unchanged; latest decision duplicate=%t stale=%t, zero refold/output work. Not cumulative metrics or a throughput benchmark.\n", sourceEnd-before.state.Partitions[0].Next, true, stale)
	return err
}

func (runner *runner) inputSnapshot(ctx context.Context, stage string) (inputCheckpoint, error) {
	var checkpoint inputCheckpoint
	raw, err := runner.durableState(ctx)
	if err != nil {
		return checkpoint, err
	}
	if err = os.WriteFile(filepath.Join(runner.report.Directory, "input-state-"+stage+".json"), raw, 0600); err != nil {
		return checkpoint, err
	}
	if err = json.Unmarshal(raw, &checkpoint.tables); err != nil {
		return checkpoint, err
	}
	checkpoint.state, err = runner.state.Snapshot(ctx)
	return checkpoint, err
}

func (runner *runner) checkInput(ctx context.Context, stage string, before inputCheckpoint, sourceEnd, outputEnd int64, duplicate, stale bool) error {
	after, err := runner.inputSnapshot(ctx, stage)
	if err != nil {
		return err
	}
	progress, err := runner.state.Progress(ctx)
	if err != nil {
		return err
	}
	if len(progress.Partitions) != 2 || len(after.state.Partitions) != 2 {
		return fmt.Errorf("input drill requires both partition observations")
	}
	evidence := inputEvidence{SourceFrom: before.state.Partitions[0].Next, SourceNext: sourceEnd, OutputEnd: runner.report.Stages[len(runner.report.Stages)-1].OutputEnd, LastProcessing: progress.Partitions[0].LastProcessing}
	evidence.AppendedRecords = sourceEnd - evidence.SourceFrom
	if !slices.Equal(evidence.OutputEnd, []int64{outputEnd, 0}) {
		return fmt.Errorf("no-op input changed output boundary: %v", evidence.OutputEnd)
	}
	for partition, previous := range before.state.Partitions {
		if partition == 0 {
			previous.Next = sourceEnd
		}
		if after.state.Partitions[partition] != previous {
			return fmt.Errorf("no-op input changed more than source progress: %+v", after.state.Partitions)
		}
	}
	if len(before.state.Keys) != 1 || len(after.state.Keys) != 1 {
		return fmt.Errorf("input drill changed the configured key set")
	}
	key := runner.manifest.Config.Keys[0].StockKey
	previous, current := before.state.Keys[key], after.state.Keys[key]
	if !reflect.DeepEqual(previous.Projection, current.Projection) || len(previous.Identities) != len(current.Identities) {
		return fmt.Errorf("no-op input changed the projection or identity set")
	}
	for identity, old := range previous.Identities {
		next := current.Identities[identity]
		oldCandidates, nextCandidates := old.Revisions[old.Highest], next.Revisions[next.Highest]
		if old.Highest != next.Highest || len(oldCandidates) != 1 || len(nextCandidates) != 1 || !reflect.DeepEqual(oldCandidates[0].Transaction, nextCandidates[0].Transaction) {
			return fmt.Errorf("no-op input changed authoritative revision/payload of %s", identity)
		}
	}
	evidence.AuthorityUnchanged = true
	references := map[ledger.Coordinates]bool{}
	for _, identity := range current.Identities {
		for _, candidates := range identity.Revisions {
			if len(candidates) != 1 {
				return fmt.Errorf("input drill created conflicting revision evidence")
			}
			for _, source := range candidates[0].Sources {
				if source.Source != runner.manifest.Config.Source || source.Partition != 0 || source.Offset < 0 || source.Offset >= sourceEnd || references[source] {
					return fmt.Errorf("invalid or repeated retained source reference: %+v", source)
				}
				references[source] = true
			}
		}
	}
	evidence.RetainedReferences = int64(len(references))
	if evidence.RetainedReferences != sourceEnd {
		return fmt.Errorf("source progress lacks complete retained input evidence")
	}
	for _, table := range []string{"outputs", "outbox", "quarantine", "view_rows", "view_progress"} {
		if len(before.tables[table]) == 0 || !bytes.Equal(before.tables[table], after.tables[table]) {
			return fmt.Errorf("no-op input changed durable %s", table)
		}
	}
	evidence.OutputUnchanged, evidence.ViewUnchanged = true, true
	measurement := evidence.LastProcessing
	wantSource := ledger.Coordinates{Source: runner.manifest.Config.Source, Partition: 0, Offset: sourceEnd - 1}
	if measurement == nil || measurement.Source != wantSource || measurement.Epoch != after.state.Partitions[0].Epoch || measurement.Work != (fold.Work{Duplicate: duplicate, Stale: stale}) || measurement.BusinessOutputs != 0 || measurement.StatusOutputs != 0 || measurement.QuarantineReason != "" || progress.Partitions[1].LastProcessing != nil {
		return fmt.Errorf("unexpected latest no-op processing measurement: %+v", measurement)
	}
	return runner.save("input-checks-"+stage+".json", evidence)
}
