// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"unicode/utf8"

	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/twmb/franz-go/pkg/kgo"
)

func ValidateOutput(ctx context.Context, cl *kgo.Client, b store.OutputBinding, c ledger.Config) ([]int64, error) {
	if b.Topic == "" || b.Incarnation == "" || b.Incarnation == c.Source || b.Partitions != c.Partitions {
		return nil, fmt.Errorf("incompatible output binding")
	}
	// Validate's metadata/retention checks use only Source and Partitions.
	boundary := c.Clone()
	boundary.Source = b.Incarnation
	ends, err := Validate(ctx, cl, b.Topic, boundary)
	if err != nil {
		return nil, fmt.Errorf("output topic: %w", err)
	}
	return ends, nil
}

// DecodeOutput checks the wire representation, independently of the view's
// namespace, version, payload, and source-cause validation. Unknown, duplicate,
// missing, null, or case-aliased members must not silently alter interpretation.
func DecodeOutput(key, raw []byte) (ledger.Envelope, error) {
	var envelope ledger.Envelope
	root, err := outputObject(raw, []string{"schema_version", "output_id", "version", "type", "payload", "cause"}, nil)
	if err != nil {
		return envelope, err
	}
	id, err := outputObject(root["output_id"], []string{"namespace", "family", "stock_key"}, []string{"day"})
	if err != nil {
		return envelope, err
	}
	if _, err = outputObject(id["stock_key"], []string{"item", "location"}, nil); err != nil {
		return envelope, err
	}
	payload, err := outputObject(root["payload"], []string{"present"}, []string{"closing_balance", "reorder_threshold", "status", "reasons"})
	if err != nil {
		return envelope, err
	}
	if reasons, ok := payload["reasons"]; ok {
		var items []*string
		if err = json.Unmarshal(reasons, &items); err != nil {
			return envelope, err
		}
		for _, item := range items {
			if item == nil {
				return envelope, fmt.Errorf("null output reason")
			}
		}
	}
	if _, err = outputObject(root["cause"], []string{"source", "partition", "offset", "txn_id", "revision"}, nil); err != nil {
		return envelope, err
	}
	if err = json.Unmarshal(raw, &envelope); err != nil {
		return envelope, err
	}
	decodedKey := decodeKey(key)
	if decodedKey.Item == "" || decodedKey.Location == "" || decodedKey != envelope.ID.Key {
		return envelope, fmt.Errorf("output broker key does not match envelope")
	}
	return envelope, nil
}

func outputObject(raw []byte, required, optional []string) (map[string]json.RawMessage, error) {
	if !utf8.Valid(raw) {
		return nil, fmt.Errorf("invalid output UTF-8")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	first, err := d.Token()
	if err != nil || first != json.Delim('{') {
		return nil, fmt.Errorf("expected output object")
	}
	fields := map[string]json.RawMessage{}
	for d.More() {
		t, err := d.Token()
		if err != nil {
			return nil, err
		}
		name, ok := t.(string)
		if !ok || (!slices.Contains(required, name) && !slices.Contains(optional, name)) {
			// Quote untrusted names so even a NUL can be retained in a SQL
			// TEXT diagnostic; the original bytes are stored separately.
			return nil, fmt.Errorf("unknown output member %q", t)
		}
		if _, ok := fields[name]; ok {
			return nil, fmt.Errorf("duplicate output member %s", name)
		}
		var value json.RawMessage
		if err = d.Decode(&value); err != nil {
			return nil, err
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, fmt.Errorf("null output member %s", name)
		}
		fields[name] = value
	}
	if _, err = d.Token(); err != nil {
		return nil, err
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, fmt.Errorf("trailing output JSON")
	}
	for _, name := range required {
		if _, ok := fields[name]; !ok {
			return nil, fmt.Errorf("missing output member %s", name)
		}
	}
	return fields, nil
}
