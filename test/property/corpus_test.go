// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package property_test

import (
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/sim"
)

// Fixed, measured corpus: 64 valid + 64 conflict/rejection histories, each with
// 80 scheduled choices followed by a fair drain. Every seed also gets a second
// delivery permutation and duplicate injection. Magnitudes are <=61 with <=33
// logical revision candidates, well inside the S3 absolute-quantity bound.
func TestSeededCorpus(t *testing.T) {
	raw, err := os.ReadFile("corpus.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Version      int    `json:"generator_version"`
		First        uint64 `json:"first_seed"`
		Last         uint64 `json:"last_seed"`
		Attempts     int    `json:"scheduled_attempts"`
		Ticks        int    `json:"clock_ticks"`
		Permutations int    `json:"delivery_permutations_per_history"`
	}
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	if corpus.Version != sim.GeneratorVersion || corpus.First != 1 || corpus.Last != 64 || corpus.Permutations != 2 {
		t.Fatal("unsupported corpus manifest")
	}
	for seed := corpus.First; seed <= corpus.Last; seed++ {
		for _, invalid := range []bool{false, true} {
			t.Run(fmt.Sprintf("seed=%d/invalid=%v", seed, invalid), func(t *testing.T) {
				trace := sim.Generate(seed, invalid)
				if len(trace.Steps) != corpus.Attempts+corpus.Ticks {
					t.Fatal("schedule differs from corpus manifest")
				}
				m, err := sim.Run(trace)
				if err != nil {
					preserveFailure(t, trace, err)
					t.Fatal(err)
				}
				if !invalid && m.Check().Status != "PASS" {
					preserveFailure(t, trace, fmt.Errorf("valid corpus qualified"))
					t.Fatalf("valid corpus qualified: %+v", m.Check())
				}
				permuted := trace
				permuted.Source = slices.Clone(trace.Source)
				rng := rand.New(rand.NewPCG(seed, 71))
				rng.Shuffle(len(permuted.Source), func(i, j int) { permuted.Source[i], permuted.Source[j] = permuted.Source[j], permuted.Source[i] })
				if len(permuted.Source) > 0 {
					permuted.Source = append(permuted.Source, permuted.Source[0].Clone())
				}
				positions := map[int]int64{}
				for i := range permuted.Source {
					r := &permuted.Source[i]
					r.Offset = positions[r.Partition]
					positions[r.Partition]++
				}
				other, err := sim.Run(permuted)
				if err != nil {
					preserveFailure(t, permuted, err)
					t.Fatal(err)
				}
				for _, key := range trace.Config.Keys {
					a := m.View.Projection(key.StockKey)
					b := other.View.Projection(key.StockKey)
					if a.Status != b.Status || !slices.Equal(a.Reasons, b.Reasons) {
						preserveComparison(t, trace, permuted)
						t.Fatal("arrival order changed authority")
					}
					if a.Status != "BLOCKED" && (!slices.Equal(a.Balances, b.Balances) || !slices.Equal(a.Alerts, b.Alerts)) {
						preserveComparison(t, trace, permuted)
						t.Fatal("arrival order changed business state")
					}
				}
			})
		}
	}
}

func preserveComparison(t *testing.T, a, b sim.Trace) {
	t.Helper()
	preserveFailure(t, a, fmt.Errorf("arrival order property"))
	directory := os.Getenv("AMENDS_ARTIFACT_DIR")
	if directory == "" {
		directory = "../../artifacts/failures"
	}
	directory = filepath.Join(directory, strings.NewReplacer("/", "-", "=", "-").Replace(t.Name()))
	raw, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "permuted.json"), raw, 0644); err != nil {
		t.Fatal(err)
	}
}

func preserveFailure(t *testing.T, trace sim.Trace, original error) {
	t.Helper()
	directory := os.Getenv("AMENDS_ARTIFACT_DIR")
	if directory == "" {
		directory = "../../artifacts/failures"
	}
	directory = filepath.Join(directory, strings.NewReplacer("/", "-", "=", "-").Replace(t.Name()))
	if err := os.MkdirAll(directory, 0755); err != nil {
		t.Fatalf("preserve failure: %v; original: %v", err, original)
	}
	// Keep the reproduction even if reduction fails or is interrupted.
	for _, name := range []string{"original", "minimized"} {
		value := trace
		if name == "minimized" {
			value = sim.Shrink(trace, func(candidate sim.Trace) bool {
				_, err := sim.Run(candidate)
				return err != nil && failureClass(err) == failureClass(original)
			})
		}
		path := filepath.Join(directory, fmt.Sprintf("seed-%d-%s.json", trace.Seed, name))
		raw, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			t.Fatalf("encode failure trace %s: %v; original: %v", path, err, original)
		}
		if err := os.WriteFile(path, raw, 0644); err != nil {
			t.Fatalf("write failure trace %s: %v; original: %v", path, err, original)
		}
		t.Logf("failure trace: %s", path)
	}
}
func failureClass(err error) string {
	s := err.Error()
	if strings.HasPrefix(s, "step ") {
		_, s, _ = strings.Cut(s, ": ")
	}
	return strings.Split(s, ":")[0]
}

func TestTraceReplayAndShrinking(t *testing.T) {
	trace := sim.Generate(17, false)
	raw, err := json.Marshal(trace)
	if err != nil {
		t.Fatal(err)
	}
	var decoded sim.Trace
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	a, err := sim.Run(trace)
	if err != nil {
		t.Fatal(err)
	}
	b, err := sim.Run(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a.Events, b.Events) || !reflect.DeepEqual(a.Check(), b.Check()) {
		t.Fatal("serialized trace changed replay")
	}
	// Exercise shrinker with a transparent synthetic predicate, not deliberately
	// broken production code. It must retain the distinguished complete revision.
	target := trace.Source[0]
	predicate := func(candidate sim.Trace) bool {
		for _, r := range candidate.Source {
			if r.Coordinates == target.Coordinates && string(r.Value) == string(target.Value) {
				return true
			}
		}
		return false
	}
	minimal := sim.Shrink(trace, predicate)
	if len(minimal.Source) != 1 || len(minimal.Steps) != 0 || !predicate(minimal) {
		t.Fatalf("shrink did not preserve predicate: %d records %d steps", len(minimal.Source), len(minimal.Steps))
	}
	if !reflect.DeepEqual(trace, sim.Generate(17, false)) {
		t.Fatal("shrinker mutated original")
	}
	if _, err := sim.Run(minimal); err != nil {
		t.Fatalf("shrunk complete-record trace is invalid: %v", err)
	}
}

// S1/S3 independent check: refuse new implementation dependencies in oracle.
func TestOracleImportBoundary(t *testing.T) {
	files, err := filepath.Glob("../../internal/oracle/*.go")
	if err != nil || len(files) == 0 {
		t.Fatal("oracle sources missing", err)
	}
	for _, name := range files {
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range file.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(path, ".") && path != "github.com/sorrell-consulting/amends/internal/ledger" {
				t.Fatalf("oracle dependency violates independence: %s", path)
			}
		}
	}
}

func TestOutputIDUnambiguous(t *testing.T) {
	a := ledger.OutputID{Namespace: "a/b", Family: "balance", Key: ledger.StockKey{Item: "c", Location: "d"}, Day: "2026-01-01"}
	b := a
	b.Namespace = "a"
	b.Key.Item = "b/c"
	if a.String() == b.String() {
		t.Fatal("ambiguous namespaced output identity")
	}
}
