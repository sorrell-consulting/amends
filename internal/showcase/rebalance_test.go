package showcase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/oracle"
	"github.com/sorrell-consulting/amends/internal/store"
)

func rebalanceFixtureRecords(test *testing.T, config ledger.Config, corrected bool) []ledger.Record {
	test.Helper()
	raw, err := fixtures.Files.ReadFile("rebalance-before.jsonl")
	if err != nil {
		test.Fatal(err)
	}
	if corrected {
		correction, err := fixtures.Files.ReadFile("rebalance-correction.jsonl")
		if err != nil {
			test.Fatal(err)
		}
		raw = append(raw, bytes.Repeat(correction, 32)...)
	}
	partitions := map[int][]ledger.Record{}
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		transaction, err := ledger.Decode(line)
		if err != nil {
			test.Fatal(err)
		}
		key, ok := config.Key(transaction.StockKey)
		if !ok {
			test.Fatal("fixture key absent from manifest")
		}
		partition := key.Partition
		partitions[partition] = append(partitions[partition], ledger.Record{Coordinates: ledger.Coordinates{Source: config.Source, Partition: partition, Offset: int64(len(partitions[partition]))}, Key: key.StockKey, RawKey: broker.EncodeKey(key.StockKey), Value: bytes.Clone(line)})
	}
	return append(partitions[0], partitions[1]...)
}

func TestRebalanceFixtures(test *testing.T) {
	config, err := fixtures.Config("rebalance-fixtures")
	if err != nil {
		test.Fatal(err)
	}
	config.Partitions = 2
	raw, err := fixtures.Files.ReadFile("rebalance-other-key.json")
	if err != nil {
		test.Fatal(err)
	}
	var other ledger.KeyConfig
	if err = json.Unmarshal(raw, &other); err != nil || other.Partition != 1 || other.Opening != 100 || other.Threshold != 60 {
		test.Fatal("invalid second populated partition fixture", err)
	}
	config.Keys = append(config.Keys, other)
	for _, stage := range []string{"before", "after"} {
		test.Run(stage, func(test *testing.T) {
			history := rebalanceFixtureRecords(test, config, stage == "after")
			result, err := oracle.Compute(config, history)
			if err != nil || len(result.Keys) != 2 || len(result.Rejections) != 0 || len(result.Conflicts) != 0 {
				test.Fatal("invalid rebalance fixture", err)
			}
			for _, file := range []string{"expected-" + stage + ".json", "expected-rebalance-other-" + stage + ".json"} {
				expected, err := fixtures.Expected(file)
				if err != nil {
					test.Fatal(err)
				}
				found := false
				for _, actual := range result.Keys {
					if actual.Key == expected.Key {
						found = true
						if !reflect.DeepEqual(actual, expected) {
							test.Fatalf("independent oracle differs from hand-derived fixture %s: %+v", file, actual)
						}
					}
				}
				if !found {
					test.Fatal("oracle omitted fixture key")
				}
			}
		})
	}
}

func TestRebalanceAssignmentEvidence(test *testing.T) {
	for _, scenario := range []struct {
		name    string
		group   RebalanceGroup
		members int
		valid   bool
	}{
		{"sole", RebalanceGroup{"Stable", map[string][]int{"one": {0, 1}}}, 1, true},
		{"split", RebalanceGroup{"Stable", map[string][]int{"one": {0}, "two": {1}}}, 2, true},
		{"preparing", RebalanceGroup{"PreparingRebalance", map[string][]int{"one": {0, 1}}}, 1, false},
		{"idle member", RebalanceGroup{"Stable", map[string][]int{"one": {0, 1}, "two": nil}}, 2, false},
		{"duplicate", RebalanceGroup{"Stable", map[string][]int{"one": {0}, "two": {0}}}, 2, false},
		{"wrong partition", RebalanceGroup{"Stable", map[string][]int{"one": {0}, "two": {2}}}, 2, false},
		{"missing", RebalanceGroup{"Stable", map[string][]int{"one": {0}}}, 2, false},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			if rebalanceAssigned(scenario.group, scenario.members) != scenario.valid {
				test.Fatal("incorrect group assignment evidence acceptance")
			}
		})
	}
}

func TestRebalanceGatesRespectCancellationAndEpochs(test *testing.T) {
	gates := newRebalanceGates()
	gates.arm(map[int]store.Partition{0: {Epoch: 1}, 1: {Epoch: 1}})
	ctx, cancel := context.WithCancel(test.Context())
	defer cancel()
	attempt := RebalanceAttempt{Token: store.Token{Partition: 0, Epoch: 1}, Expected: 3, Record: ledger.Coordinates{Partition: 0, Offset: 3}}
	done := make(chan error, 1)
	go func() { done <- gates.before(ctx, attempt) }()
	if entered := <-gates.entered; entered != attempt {
		test.Fatal("gate lost the polled decision")
	}
	close(gates.release)
	if err := <-done; err != nil {
		test.Fatal(err)
	}
	if err := gates.before(ctx, attempt); err != nil {
		test.Fatal(err)
	}
	select {
	case <-gates.successorEntered:
		test.Fatal("old epoch was counted as a successor")
	default:
	}
	for _, partition := range []int{0, 1} {
		next := attempt
		next.Token.Partition, next.Token.Epoch, next.Record.Partition = partition, 2, partition
		go func() { done <- gates.before(ctx, next) }()
	}
	var partitions []int
	for range 2 {
		partitions = append(partitions, (<-gates.successorEntered).Record.Partition)
	}
	slices.Sort(partitions)
	if !slices.Equal(partitions, []int{0, 1}) {
		test.Fatal("successor gates lost a partition")
	}
	cancel()
	for range 2 {
		if err := <-done; !errors.Is(err, context.Canceled) {
			test.Fatal("canceled gate allowed storage to proceed", err)
		}
	}
}
