// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestDeliveryCorpus(t *testing.T) {
	raw, err := os.ReadFile("testdata/corpus.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Version        int      `json:"trace_version"`
		Implementation string   `json:"implementation"`
		First          uint64   `json:"first_seed"`
		Last           uint64   `json:"last_seed"`
		Classes        []string `json:"history_classes"`
		Attempts       int      `json:"scheduled_attempts"`
		Runs           int      `json:"runs"`
		Scope          string   `json:"scope"`
	}
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	if corpus.Version != Version || corpus.Implementation != Implementation || corpus.First != 1 || corpus.Last != 16 || corpus.Attempts != 48 || corpus.Runs != 32 || corpus.Scope != Scope || !slices.Equal(corpus.Classes, []string{"valid", "conflict-and-rejection"}) {
		t.Fatal("unsupported delivery corpus manifest")
	}
	observed := map[string]int{}
	statuses := map[string]int{}
	runs := 0
	for seed := corpus.First; seed <= corpus.Last; seed++ {
		for _, invalid := range []bool{false, true} {
			t.Run(fmt.Sprintf("seed=%d/invalid=%v", seed, invalid), func(t *testing.T) {
				trace := Generate(seed, invalid)
				if len(trace.Steps) != corpus.Attempts {
					t.Fatal("schedule differs from manifest")
				}
				check := func(candidate Trace) (*Result, string) {
					r, err := Run(t.Context(), candidate)
					if err != nil {
						return r, err.Error()
					}
					if !invalid && r.Report.Status != "PASS" {
						return r, "valid history: " + r.Report.Status
					}
					if invalid && (r.Report.Status != "BLOCKED" && r.Report.Status != "PASS_WITH_EXCLUSIONS" || len(r.Report.Boundary.Rejections) == 0) {
						return r, "missing negative/qualified evidence: " + r.Report.Status
					}
					return r, ""
				}
				r, failure := check(trace)
				if failure != "" {
					preserveFailure(t, trace, func(candidate Trace) bool { _, got := check(candidate); return got == failure })
					t.Fatal(failure)
				}
				runs++
				statuses[r.Report.Status]++
				// Only calls actually reached by production orchestration count.
				for _, e := range r.Report.Events {
					for _, f := range r.Report.Attempts[e.Step].Step.Faults {
						if f.Operation == e.Operation && f.Call == e.Call {
							observed[e.Operation+"/"+e.Outcome]++
						}
					}
				}
			})
		}
	}
	if runs != corpus.Runs {
		t.Fatalf("completed %d of %d declared runs", runs, corpus.Runs)
	}
	coverage, err := json.Marshal(struct {
		Runs       int            `json:"runs"`
		Statuses   map[string]int `json:"statuses"`
		FaultCalls map[string]int `json:"observed_fault_calls"`
	}{runs, statuses, observed})
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(coverage))
}

func preserveFailure(t *testing.T, trace Trace, sameFailure func(Trace) bool) {
	t.Helper()
	directory := os.Getenv("AMENDS_ARTIFACT_DIR")
	if directory == "" {
		directory = "../../artifacts/failures"
	}
	directory = filepath.Join(directory, strings.NewReplacer("/", "-", "=", "-").Replace(t.Name()))
	if err := os.MkdirAll(directory, 0755); err != nil {
		t.Fatal(err)
	}
	// Preserve the original before attempting reduction, including on a reducer failure.
	for _, name := range []string{"original", "reduced"} {
		candidate := trace
		if name == "reduced" {
			candidate = Shrink(trace, sameFailure)
		}
		raw, err := json.MarshalIndent(candidate, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(directory, name+".json")
		if err := os.WriteFile(path, append(raw, '\n'), 0644); err != nil {
			t.Fatal(err)
		}
		t.Logf("Replay: amendsctl sim-delivery -replay %s", path)
	}
}
