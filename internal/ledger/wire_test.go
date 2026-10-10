// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package ledger_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/sorrell-consulting/amends/internal/ledger"
)

const valid = `{"txn_id":"a","revision":1,"kind":"NEW","stock_key":{"item":"i","location":"l"},"valid_time":"2026-01-02T01:00:00+02:00","qty":0,"source_time":"2026-01-03T00:00:00Z"}`

func TestStrictReplacementShape(t *testing.T) {
	transaction, err := ledger.Decode([]byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	if got := transaction.ValidTime.Format("2006-01-02T15:04:05Z07:00"); got != "2026-01-01T23:00:00Z" {
		t.Fatalf("normalization %s", got)
	}
	for _, field := range []string{"txn_id", "revision", "kind", "stock_key", "valid_time", "qty", "source_time"} {
		for _, null := range []bool{false, true} {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal([]byte(valid), &fields); err != nil {
				t.Fatal(err)
			}
			if null {
				fields[field] = json.RawMessage("null")
			} else {
				delete(fields, field)
			}
			raw, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ledger.Decode(raw); err == nil {
				t.Fatalf("accepted missing/null %s", field)
			}
		}
	}
	for _, raw := range []string{valid + valid, strings.Replace(valid, `"qty":0`, `"qty":0,"qty":1`, 1), strings.Replace(valid, `"item":"i"`, `"item":"i","item":"j"`, 1), strings.Replace(valid, `"qty":0`, `"qty":0,"unknown":1`, 1), strings.Replace(valid, `"revision":1`, `"revision":9223372036854775808`, 1), strings.Replace(valid, `"qty":0`, `"qty":0.5`, 1), strings.Replace(valid, `"location":"l"`, `"location":null`, 1)} {
		if _, err := ledger.Decode([]byte(raw)); err == nil {
			t.Fatalf("accepted malformed record: %s", raw)
		}
	}
	if _, err := ledger.Decode([]byte(strings.Replace(valid, `"qty":0`, `"qty":0,"Qty":1`, 1))); err == nil {
		t.Fatal("case-insensitive JSON alias accepted")
	}
}
