package showcase

import (
	"bytes"
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/oracle"
)

func TestDiagnosticFixtures(t *testing.T) {
	for _, drill := range []string{"poison-event", "source-conflict"} {
		t.Run(drill, func(t *testing.T) {
			config, err := fixtures.Config("diagnostic-fixtures")
			if err != nil {
				t.Fatal(err)
			}
			config.Partitions = 2
			repair, projection, sourceEnd := "poison-repair.jsonl", "expected-poison-repaired.json", int64(6)
			if drill == "source-conflict" {
				raw, err := fixtures.Files.ReadFile("conflict-other-key.json")
				if err != nil {
					t.Fatal(err)
				}
				var other ledger.KeyConfig
				if err = json.Unmarshal(raw, &other); err != nil {
					t.Fatal(err)
				}
				if other.Partition != 0 || other.Opening != 100 || other.Threshold != 60 {
					t.Fatal("other-key fixture must exercise progress in the blocked key's partition")
				}
				config.Keys = append(config.Keys, other)
				repair, projection, sourceEnd = "conflict-repair.jsonl", "expected-conflict-repaired.json", 7
			}
			expected, err := diagnosticExpectation(config, drill, sourceEnd)
			if err != nil {
				t.Fatal(err)
			}
			if expected.Name != drill || expected.Namespace != config.Namespace || expected.ConfigDigest != config.Digest() || !slices.Equal(expected.SourceEnd, []int64{sourceEnd, 0}) {
				t.Fatal("diagnostics not bound to declared namespace and source boundary")
			}
			history, err := fixtures.Records(config, "before.jsonl", "correction.jsonl", drill+".jsonl", repair)
			if err != nil {
				t.Fatal(err)
			}
			for index := range history {
				history[index].RawKey = broker.EncodeKey(history[index].Key)
			}
			if drill == "poison-event" {
				if len(expected.Diagnostics.Rejections) != 1 || len(expected.Diagnostics.Conflicts) != 0 || expected.Diagnostics.Rejections[0].Reason != "REVISION" || !reflect.DeepEqual(expected.Diagnostics.Rejections[0].Record, history[4]) {
					t.Fatalf("wrong exact rejection expectation: %+v", expected)
				}
			} else {
				if len(expected.Diagnostics.Rejections) != 0 || len(expected.Diagnostics.Conflicts) != 1 {
					t.Fatalf("wrong conflict count: %+v", expected)
				}
				conflict := expected.Diagnostics.Conflicts[0]
				if conflict.Conflict.TxnID != "ISSUE-001" || conflict.Conflict.Revision != 2 || !reflect.DeepEqual(conflict.Records, history[3:5]) {
					t.Fatalf("wrong exact conflict expectation: %+v", conflict)
				}
				if bytes.Equal(conflict.Records[0].Value, conflict.Records[1].Value) {
					t.Fatal("conflict fixture no longer contains differing payloads")
				}
			}
			result, err := oracle.Compute(config, history)
			if err != nil {
				t.Fatal(err)
			}
			expectedFiles := []string{projection}
			if drill == "source-conflict" {
				expectedFiles = append(expectedFiles, "expected-conflict-other.json")
			}
			for _, name := range expectedFiles {
				want, err := fixtures.Expected(name)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, actual := range result.Keys {
					if actual.Key == want.Key {
						found = true
						if actual.Status != want.Status || !slices.Equal(actual.Balances, want.Balances) || !slices.Equal(actual.Alerts, want.Alerts) {
							t.Fatalf("oracle differs from hand-derived %s: %+v", name, actual)
						}
					}
				}
				if !found {
					t.Fatalf("oracle omitted %s", name)
				}
			}
		})
	}
}

func TestScenarioRejectsInvalidOptionsBeforeSetup(t *testing.T) {
	for _, options := range []Options{
		{Scenario: "unknown"},
		{Scenario: "ambiguous-commit"}, // request/reply must be explicit in the API
		{Scenario: "correction-only,poison-event"},
		{Scenario: "replay-intact"}, // administrative schedules have their own entry point
	} {
		options.DatabaseURL = "postgres://unused"
		options.Brokers = []string{"unused"}
		options.ControlBinary, options.WorkerBinary = "unused", "unused"
		options.ArtifactParent = t.TempDir()
		report, err := Run(t.Context(), options)
		if err == nil || report.Directory != "" {
			t.Fatalf("invalid drill created resources: %+v error=%v", report, err)
		}
	}
}
