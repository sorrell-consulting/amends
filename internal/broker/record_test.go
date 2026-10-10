// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBrokerKeyEncodingAndEvidence(t *testing.T) {
	key := ledger.StockKey{Item: "part\x00a", Location: "depot"}
	for _, raw := range [][]byte{EncodeKey(key), []byte(` { "location": "depot", "item": "part\u0000a" } `)} {
		got := DecodeRecord("incarnation", &kgo.Record{Partition: 1, Offset: 42, Key: raw, Value: []byte{0, 255}})
		if got.Key != key || !bytes.Equal(got.RawKey, raw) || got.Partition != 1 || got.Offset != 42 || got.Source != "incarnation" {
			t.Fatalf("record changed: %+v", got)
		}
		clone := got.Clone()
		clone.RawKey[0] ^= 1
		clone.Value[0] ^= 1
		if bytes.Equal(got.RawKey, clone.RawKey) || bytes.Equal(got.Value, clone.Value) {
			t.Fatal("record clone aliases raw bytes")
		}
	}
	for _, raw := range []string{`null`, `{}`, `{"item":"a"}`, `{"item":"a","location":null}`, `{"item":"a","item":"b","location":"c"}`, `{"Item":"a","location":"b"}`, `{"item":"a","location":"b","extra":0}`, `{"item":1,"location":"b"}`, `{"item":"a","location":"b"} {}`, "\xff\x00"} {
		got := DecodeRecord("source", &kgo.Record{Key: []byte(raw)})
		if got.Key != (ledger.StockKey{}) || !bytes.Equal(got.RawKey, []byte(raw)) {
			t.Fatalf("invalid key accepted or lost: %q", raw)
		}
	}
}

func TestManifestRequiresBrokerIncarnationAndCompleteConfig(t *testing.T) {
	c, err := fixtures.Config("test")
	if err != nil {
		t.Fatal(err)
	}
	c.Source = Incarnation([16]byte{1})
	raw, err := json.Marshal(Manifest{Topic: "source", Config: c})
	if err != nil {
		t.Fatal(err)
	}
	m, err := ReadManifest(bytes.NewReader(raw))
	if err != nil || m.Config.Digest() != c.Digest() {
		t.Fatalf("manifest changed: %v", err)
	}
	for _, invalid := range []string{string(raw) + `{}`, strings.Replace(string(raw), c.Source, "synthetic-fixture-v1", 1), strings.Replace(string(raw), c.Source, Incarnation([16]byte{}), 1), strings.Replace(string(raw), `"source_topic":"source"`, `"source_topic":""`, 1), `{"config":null}`, `{"unknown":1}`} {
		if _, err := ReadManifest(strings.NewReader(invalid)); err == nil {
			t.Fatalf("invalid manifest accepted: %s", invalid)
		}
	}
}
