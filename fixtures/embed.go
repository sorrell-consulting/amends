// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Package fixtures embeds the hand-stated synthetic specification files.
package fixtures

import (
	"bufio"
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sorrell-consulting/amends/internal/ledger"
)

// Files contains the synthetic inputs and independently stated expected results.
//
//go:embed *.json *.jsonl
var Files embed.FS

// Config returns validated single-partition fixture configuration in namespace.
// Its synthetic source identity is for in-memory examples, not a broker topic.
func Config(namespace string) (ledger.Config, error) {
	raw, err := Files.ReadFile("config.json")
	if err != nil {
		return ledger.Config{}, err
	}
	var fixture struct {
		Schema int                `json:"schema_version"`
		Origin time.Time          `json:"ledger_origin"`
		Keys   []ledger.KeyConfig `json:"stock_keys"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		return ledger.Config{}, err
	}
	c := ledger.Config{SchemaVersion: fixture.Schema, Namespace: namespace, Source: "synthetic-fixture-v1", Partitions: 1, Origin: fixture.Origin, Keys: fixture.Keys}
	return c, c.Validate()
}

// Records reads named JSONL fixtures in order, preserving each line's bytes.
// It assigns c.Source, partition zero, and consecutive offsets starting at zero
// across all names in this call. It decodes wire shape but does not reject invalid
// business fields, allowing fixtures to exercise rejection paths.
func Records(c ledger.Config, names ...string) ([]ledger.Record, error) {
	var records []ledger.Record
	for _, name := range names {
		raw, err := Files.ReadFile(name)
		if err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(bytes.NewReader(raw))
		for scanner.Scan() {
			t, err := ledger.Decode(scanner.Bytes())
			if err != nil {
				return nil, fmt.Errorf("%s: %w", name, err)
			}
			records = append(records, ledger.Record{Coordinates: ledger.Coordinates{Source: c.Source, Partition: 0, Offset: int64(len(records))}, Key: t.StockKey, Value: bytes.Clone(scanner.Bytes())})
		}
		if err := scanner.Err(); err != nil {
			return nil, err
		}
	}
	return records, nil
}

// Expected reads a hand-stated projection without invoking fold or oracle code.
func Expected(name string) (ledger.Projection, error) {
	raw, err := Files.ReadFile(name)
	if err != nil {
		return ledger.Projection{}, err
	}
	var p ledger.Projection
	err = json.Unmarshal(raw, &p)
	return p, err
}
