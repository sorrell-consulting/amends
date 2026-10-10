// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bytes"
	"encoding/json"
	"io"
	"slices"
	"unicode/utf8"

	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/twmb/franz-go/pkg/kgo"
)

func EncodeKey(key ledger.StockKey) []byte { raw, _ := json.Marshal(key); return raw }

// DecodeRecord retains raw bytes even if the key is not a complete, unambiguous
// JSON stock key. An invalid key becomes the empty key, which cannot match an
// accepted envelope and is quarantined atomically by the normal validator.
func DecodeRecord(source string, r *kgo.Record) ledger.Record {
	return ledger.Record{Coordinates: ledger.Coordinates{Source: source, Partition: int(r.Partition), Offset: r.Offset},
		Key: decodeKey(r.Key), RawKey: slices.Clone(r.Key), Value: slices.Clone(r.Value)}
}

func decodeKey(raw []byte) ledger.StockKey {
	if !utf8.Valid(raw) {
		return ledger.StockKey{}
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return ledger.StockKey{}
	}
	fields := map[string]string{}
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return ledger.StockKey{}
		}
		name, ok := token.(string)
		if !ok || (name != "item" && name != "location") {
			return ledger.StockKey{}
		}
		if _, exists := fields[name]; exists {
			return ledger.StockKey{}
		}
		var value *string
		if err := d.Decode(&value); err != nil || value == nil {
			return ledger.StockKey{}
		}
		fields[name] = *value
	}
	if _, err := d.Token(); err != nil {
		return ledger.StockKey{}
	}
	if _, err := d.Token(); err != io.EOF || len(fields) != 2 {
		return ledger.StockKey{}
	}
	return ledger.StockKey{Item: fields["item"], Location: fields["location"]}
}
