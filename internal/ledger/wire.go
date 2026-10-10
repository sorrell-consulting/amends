// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package ledger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// Decode checks the wire shape only. Worker and oracle validate business rules
// independently. Duplicate names and null/missing fields are not replacements.
// ValidTime and SourceTime are normalized to UTC. A nil error does not imply
// that the transaction's kind, quantity, revision, or routing is valid.
func Decode(raw []byte) (Transaction, error) {
	//TODO: Does this silently replace invalid bytes?
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := object(d); err != nil {
		return Transaction{}, err
	}
	if _, err := d.Token(); err != io.EOF {
		return Transaction{}, fmt.Errorf("trailing JSON")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return Transaction{}, err
	}
	allowed := map[string]bool{"txn_id": true, "revision": true, "kind": true, "stock_key": true, "valid_time": true, "qty": true, "source_time": true}
	for name := range fields {
		if !allowed[name] {
			return Transaction{}, fmt.Errorf("unknown field %s", name)
		}
	}
	for _, name := range []string{"txn_id", "revision", "kind", "stock_key", "valid_time", "qty", "source_time"} {
		v, ok := fields[name]
		if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return Transaction{}, fmt.Errorf("missing %s", name)
		}
	}
	var key map[string]json.RawMessage
	if err := json.Unmarshal(fields["stock_key"], &key); err != nil {
		return Transaction{}, err
	}
	for name := range key {
		if name != "item" && name != "location" {
			return Transaction{}, fmt.Errorf("unknown stock key field %s", name)
		}
	}
	for _, name := range []string{"item", "location"} {
		v, ok := key[name]
		if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return Transaction{}, fmt.Errorf("missing stock key %s", name)
		}
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	var t Transaction
	if err := d.Decode(&t); err != nil {
		return Transaction{}, err
	}
	t.ValidTime = t.ValidTime.UTC()
	t.SourceTime = t.SourceTime.UTC() //TODO: Will extreme source_time block indefinitely?
	return t, nil
}

func object(d *json.Decoder) error {
	t, err := d.Token()
	if err != nil {
		return err
	}
	if t != json.Delim('{') {
		return fmt.Errorf("expected object")
	}
	names := map[string]bool{}
	for d.More() {
		t, err = d.Token()
		if err != nil {
			return err
		}
		name, ok := t.(string)
		if !ok || names[name] {
			return fmt.Errorf("duplicate or invalid member")
		}
		names[name] = true
		var raw json.RawMessage
		if err := d.Decode(&raw); err != nil {
			return err
		}
		if name == "stock_key" {
			child := json.NewDecoder(bytes.NewReader(raw))
			if err := object(child); err != nil {
				return err
			}
		}
	}
	_, err = d.Token()
	return err
}
