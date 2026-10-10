package showcase

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/oracle"
)

func inputFixtureNames(stale bool, repetitions int) []string {
	names := []string{"before.jsonl", "correction.jsonl"}
	input := "duplicate-deliveries.jsonl"
	if stale {
		names = append(names, "stale-protection.jsonl")
		input = "stale-deliveries.jsonl"
	}
	for repeat := 0; repeat < repetitions; repeat++ {
		names = append(names, input)
	}
	return names
}

func TestInputDrillFixtures(test *testing.T) {
	for _, scenario := range []struct {
		name        string
		stale       bool
		repetitions int
		records     int
	}{
		{"duplicates", false, 64, 132},
		{"protected", true, 0, 6},
		{"stale-first", true, 1, 8},
		{"stale-storm", true, 65, 136},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			config, err := fixtures.Config("input-drill-fixtures")
			if err != nil {
				test.Fatal(err)
			}
			history, err := fixtures.Records(config, inputFixtureNames(scenario.stale, scenario.repetitions)...)
			if err != nil || len(history) != scenario.records {
				test.Fatalf("wrong fixture history: %d records, error=%v", len(history), err)
			}
			if !scenario.stale {
				original, err := ledger.Decode(history[3].Value)
				if err != nil || !bytes.Equal(history[3].Value, history[4].Value) {
					test.Fatal("duplicate fixture lost its byte-identical correction")
				}
				newer, err := ledger.Decode(history[5].Value)
				if err != nil || newer.SourceTime.Year() != 2050 || !newer.SourceTime.After(original.SourceTime) {
					test.Fatal("duplicate fixture lost its newer informational timestamp")
				}
				newer.SourceTime = original.SourceTime
				if !reflect.DeepEqual(newer, original) {
					test.Fatal("informational duplicate changed a canonical field")
				}
			} else if scenario.repetitions > 0 {
				for index, quantity := range []int64{-40, 50} {
					transaction, err := ledger.Decode(history[6+index].Value)
					if err != nil || transaction.Kind != "AMEND" || transaction.Revision != int64(3-index) || transaction.Qty != quantity || transaction.SourceTime.Year() != 2050 {
						test.Fatalf("stale fixture lost its counterfactual correction/reactivation: %+v error=%v", transaction, err)
					}
				}
			}
			result, err := oracle.Compute(config, history)
			if err != nil || len(result.Keys) != 1 || len(result.Conflicts) != 0 || len(result.Rejections) != 0 {
				test.Fatalf("input fixture is not unqualified: %+v error=%v", result, err)
			}
			file := "expected-after.json"
			if scenario.stale {
				file = "expected-after-cancel.json"
			}
			expected, err := fixtures.Expected(file)
			if err != nil || !reflect.DeepEqual(result.Keys[0], expected) {
				test.Fatalf("oracle differs from hand-derived %s: %+v error=%v", file, result, err)
			}
		})
	}
	for _, name := range []string{"duplicate-deliveries.jsonl", "stale-protection.jsonl", "stale-deliveries.jsonl"} {
		raw, err := fixtures.Files.ReadFile(name)
		if err != nil || bytes.Count(raw, []byte{'\n'}) != 2 || !bytes.HasSuffix(raw, []byte{'\n'}) {
			test.Fatalf("%s must be two newline-terminated source assertions: %v", name, err)
		}
	}
}
