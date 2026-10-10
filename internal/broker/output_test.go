// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/sorrell-consulting/amends/internal/ledger"
)

func TestOutputWireRejectsAmbiguousRepresentations(t *testing.T) {
	e := ledger.Envelope{SchemaVersion: 1, ID: ledger.OutputID{Namespace: "ns", Family: "balance", Key: ledger.StockKey{Item: "part", Location: "depot"}, Day: "2026-01-01"}, Version: 1, Type: "BalanceStated", Payload: ledger.Payload{Present: true, Close: 50}, Cause: ledger.Cause{Coordinates: ledger.Coordinates{Source: "source", Partition: 0, Offset: 0}, TxnID: "txn", Revision: 1}}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	key := EncodeKey(e.ID.Key)
	decoded, err := DecodeOutput(key, raw)
	if err != nil || !decoded.Equal(e) {
		t.Fatalf("envelope changed: %v", err)
	}
	for _, bad := range []string{
		strings.Replace(string(raw), `"version":1`, `"version":1,"version":2`, 1),
		strings.Replace(string(raw), `"version":1`, `"Version":1`, 1),
		strings.Replace(string(raw), `"present":true`, `"present":null`, 1),
		strings.Replace(string(raw), `"present":true,`, ``, 1),
		strings.Replace(string(raw), `"offset":0`, `"offset":null`, 1),
		strings.Replace(string(raw), `"item":"part"`, `"item":"part","item":"other"`, 1),
		strings.Replace(string(raw), `"closing_balance":50`, `"closing_balance":50,"unknown":1`, 1),
		string(raw) + `{}`, string([]byte{255, 0}),
	} {
		if _, err := DecodeOutput(key, []byte(bad)); err == nil {
			t.Fatalf("ambiguous envelope accepted: %s", bad)
		}
	}
	if _, err := DecodeOutput([]byte(`{"item":"wrong","location":"depot"}`), raw); err == nil {
		t.Fatal("wrong broker key accepted")
	}
}
