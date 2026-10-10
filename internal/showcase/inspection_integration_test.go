//go:build integration

package showcase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/fold"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
)

type inspectedReview struct {
	Scope      string             `json:"scope"`
	Count      int                `json:"count"`
	Rejections []ledger.Rejection `json:"quarantine"`
	Conflicts  []struct {
		Conflict          ledger.Conflict `json:"conflict"`
		AtHighestRevision bool            `json:"at_highest_revision"`
		HighestRevision   int64           `json:"highest_observed_revision"`
		KeyStatus         string          `json:"key_status"`
		Candidates        []struct {
			Payload ledger.Transaction   `json:"representative_payload"`
			Sources []ledger.Coordinates `json:"source_references"`
		} `json:"candidates"`
	} `json:"conflicts"`
}

func checkInspectionArtifacts(test *testing.T, directory, stage, drill string) {
	test.Helper()
	raw, err := os.ReadFile(filepath.Join(directory, "dashboard-"+stage+".html"))
	if err != nil {
		test.Fatal(err)
	}
	requiredSections := []string{"<!doctype html>", "NOT VERIFIED", "separately timed reads", "Partition progress", "Sparse daily closes", "Source conflicts", "Quarantine", "Latest processing work"}
	if drill != "rebalance-under-load" {
		requiredSections = append(requiredSections, "Not measured")
	}
	for _, required := range requiredSections {
		if !bytes.Contains(raw, []byte(required)) {
			test.Fatalf("dashboard omitted %q", required)
		}
	}
	if drill == "" {
		raw, err := os.ReadFile(filepath.Join(directory, "inspect-"+stage+".log"))
		if err != nil {
			test.Fatal(err)
		}
		var observed struct {
			Partitions []postgres.PartitionProgress `json:"partitions"`
		}
		if err = json.Unmarshal(raw, &observed); err != nil || len(observed.Partitions) != 2 {
			test.Fatalf("invalid measured inspection: %s, error=%v", raw, err)
		}
		measurement := observed.Partitions[0].LastProcessing
		if measurement == nil || observed.Partitions[1].LastProcessing != nil || measurement.ElapsedBeforeCommitNS < measurement.LockAcquireNS || measurement.LockAcquireNS <= 0 {
			test.Fatalf("missing/fabricated processing observations: %+v", observed)
		}
		want := fold.Work{Refold: true, FromDay: "2026-01-03", RefoldedRecords: 1}
		offset, outputs := int64(2), int64(1)
		if stage == "after" {
			want = fold.Work{Refold: true, FromDay: "2026-01-01", RewindDays: 2, RefoldedRecords: 3}
			offset, outputs = 3, 5
		}
		if measurement.Source.Offset != offset || measurement.Work != want || measurement.BusinessOutputs != outputs || measurement.StatusOutputs != 0 {
			test.Fatalf("%s measurement differs from hand-derived work: %+v", stage, measurement)
		}
	}
	if drill == "source-conflict" {
		expected := "superseded conflict"
		if stage == "diagnostic" {
			expected = "unresolved highest revision"
			if !bytes.Contains(raw, []byte("Uncertified frozen business values")) {
				test.Fatal("blocked dashboard certified frozen values")
			}
		}
		if !bytes.Contains(raw, []byte(expected)) || !bytes.Contains(raw, []byte("DEMO-OTHER")) {
			test.Fatal("dashboard omitted conflict classification or unaffected key")
		}
	}
	for _, kind := range []string{"quarantine", "conflicts"} {
		raw, err = os.ReadFile(filepath.Join(directory, kind+"-"+stage+".log"))
		if err != nil {
			test.Fatal(err)
		}
		var review inspectedReview
		if err = json.Unmarshal(raw, &review); err != nil || !strings.Contains(review.Scope, "not a verification result") {
			test.Fatalf("invalid review scope: %s error=%v", raw, err)
		}
		count := 0
		if (drill == "poison-event" && kind == "quarantine") || (drill == "source-conflict" && kind == "conflicts") {
			count = 1
		}
		if review.Count != count {
			test.Fatalf("%s %s count=%d, want %d", stage, kind, review.Count, count)
		}
		if count == 1 && kind == "quarantine" {
			if len(review.Rejections) != 1 || review.Rejections[0].Reason != "REVISION" || review.Rejections[0].Record.Offset != 4 || len(review.Rejections[0].Record.RawKey) == 0 {
				test.Fatalf("quarantine review lost exact evidence: %s", raw)
			}
		}
		if count == 1 && kind == "conflicts" {
			if len(review.Conflicts) != 1 {
				test.Fatalf("wrong conflict evidence: %s", raw)
			}
			conflict := review.Conflicts[0]
			highest, status := int64(3), "READY"
			if stage == "diagnostic" {
				highest, status = 2, "BLOCKED"
			}
			if conflict.AtHighestRevision != (stage == "diagnostic") || conflict.HighestRevision != highest || conflict.KeyStatus != status || conflict.Conflict.Revision != 2 || len(conflict.Candidates) != 2 {
				test.Fatalf("conflict authority/evidence hidden: %s", raw)
			}
			if conflict.Candidates[0].Payload.Qty != -30 || conflict.Candidates[1].Payload.Qty != -40 || len(conflict.Candidates[0].Sources) != 1 || len(conflict.Candidates[1].Sources) != 1 || conflict.Candidates[0].Sources[0].Offset != 3 || conflict.Candidates[1].Sources[0].Offset != 4 {
				test.Fatalf("conflict candidate payloads/references lost: %s", raw)
			}
		}
	}
}

func checkReviewCommands(test *testing.T, ctx context.Context, binary, databaseURL, manifestPath string, state *postgres.Store, drill string) {
	test.Helper()
	before, err := state.Snapshot(ctx)
	if err != nil {
		test.Fatal(err)
	}
	beforeView, err := state.View(ctx)
	if err != nil {
		test.Fatal(err)
	}
	command := func(exitCode int, args ...string) []byte {
		test.Helper()
		child := exec.CommandContext(ctx, binary, args...)
		child.Env = append(os.Environ(), "AMENDS_DATABASE_URL="+databaseURL)
		raw, err := child.CombinedOutput()
		if exitCode == 0 {
			if err != nil {
				test.Fatalf("read command failed: %v %s error=%v", args, raw, err)
			}
		} else {
			var exitError *exec.ExitError
			if !errors.As(err, &exitError) || exitError.ExitCode() != exitCode {
				test.Fatalf("read command did not refuse invalid selection/config: %v %s error=%v", args, raw, err)
			}
		}
		return raw
	}
	var args []string
	if drill == "poison-event" {
		args = []string{"quarantine", "show", "-partition", "0", "-offset", "4"}
	} else {
		args = []string{"conflicts", "show", "-item", "DEMO-PART", "-location", "DEPOT-A", "-txn-id", "ISSUE-001", "-revision", "2"}
	}
	args = append(args, "-manifest", manifestPath)
	raw := command(0, args...)
	var review inspectedReview
	if err = json.Unmarshal(raw, &review); err != nil || review.Count != 1 {
		test.Fatalf("exact show did not select one record: %s error=%v", raw, err)
	}
	if drill == "poison-event" && !reflect.DeepEqual(review.Rejections, before.Rejections) {
		test.Fatal("show lost rejected raw bytes or coordinates")
	}
	command(1, "quarantine", "show", "-manifest", manifestPath, "-partition", "0", "-offset", "900")
	command(1, "conflicts", "show", "-manifest", manifestPath, "-item", "DEMO-PART", "-location", "DEPOT-A", "-txn-id", "missing", "-revision", "2")
	raw, err = os.ReadFile(manifestPath)
	if err != nil {
		test.Fatal(err)
	}
	manifest, err := broker.ReadManifest(bytes.NewReader(raw))
	if err != nil {
		test.Fatal(err)
	}
	manifest.Config.Keys[0].Opening++
	raw, err = json.Marshal(manifest)
	if err != nil {
		test.Fatal(err)
	}
	wrongPath := filepath.Join(test.TempDir(), "wrong-config.json")
	if err = os.WriteFile(wrongPath, raw, 0600); err != nil {
		test.Fatal(err)
	}
	command(1, "quarantine", "list", "-manifest", wrongPath)
	command(1, "conflicts", "list", "-manifest", wrongPath)
	raw = command(0, "inspect", "-manifest", manifestPath, "-brokers", "127.0.0.1:1", "-timeout", "2s")
	var unavailable struct {
		Errors     []string `json:"observation_errors"`
		Partitions []struct {
			SourceEnd *int64 `json:"observed_source_end"`
			OutputEnd *int64 `json:"observed_output_end"`
			InputLag  *int64 `json:"input_offset_distance"`
			ViewLag   *int64 `json:"view_offset_distance"`
		} `json:"partitions"`
	}
	if err = json.Unmarshal(raw, &unavailable); err != nil || len(unavailable.Errors) == 0 || len(unavailable.Partitions) != 2 {
		test.Fatalf("unavailable broker observations concealed: %s error=%v", raw, err)
	}
	for _, partition := range unavailable.Partitions {
		if partition.SourceEnd != nil || partition.OutputEnd != nil || partition.InputLag != nil || partition.ViewLag != nil {
			test.Fatal("unavailable broker observations fabricated as known positions or lag")
		}
	}
	raw = command(0, "inspect", "-manifest", manifestPath, "-format", "html", "-brokers", "127.0.0.1:1", "-timeout", "2s")
	if !bytes.Contains(raw, []byte("NOT VERIFIED")) || !bytes.Contains(raw, []byte("Observation unavailable")) || !bytes.Contains(raw, []byte("unknown")) {
		test.Fatal("HTML concealed unavailable broker observations")
	}
	after, err := state.Snapshot(ctx)
	if err != nil {
		test.Fatal(err)
	}
	afterView, err := state.View(ctx)
	if err != nil {
		test.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(beforeView, afterView) {
		test.Fatal("review changed durable state, progress, outputs, or view")
	}
}
