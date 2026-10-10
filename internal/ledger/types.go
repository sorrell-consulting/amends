// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Package ledger defines the shared schema, not authority or folding decisions.
package ledger

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"time"
)

// StockKey identifies the item/location whose transactions share a balance.
type StockKey struct {
	Item     string `json:"item"`
	Location string `json:"location"`
}

// KeyConfig fixes a stock key's opening inventory, reorder threshold, and source
// partition for the lifetime of its namespace. Opening and Threshold use units
// of stock; Opening is the balance immediately before [Config.Origin].
type KeyConfig struct {
	StockKey
	Opening   int64 `json:"opening_balance"`
	Threshold int64 `json:"reorder_threshold"`
	Partition int   `json:"partition"`
}

// Config identifies an immutable processing namespace and its source history.
// Ordinary resume requires compatibility with the persisted configuration.
type Config struct {
	SchemaVersion int         `json:"schema_version"`
	Namespace     string      `json:"namespace"`
	Source        string      `json:"source_incarnation"` // Topic incarnation, not a reusable topic name.
	Partitions    int         `json:"partitions"`
	Origin        time.Time   `json:"ledger_origin"` // Earliest accepted ValidTime.
	Keys          []KeyConfig `json:"stock_keys"`
}

// Validate checks the supported schema, required identities, origin, and unique
// stock-key partition mappings. It does not inspect retained source history.
func (c Config) Validate() error {
	if c.SchemaVersion != 1 || c.Namespace == "" || c.Source == "" || c.Partitions < 1 || c.Origin.Year() < 1 || c.Origin.Year() > 9999 {
		return fmt.Errorf("incompatible namespace configuration")
	}
	seen := map[StockKey]bool{}
	for _, k := range c.Keys {
		if k.Item == "" || k.Location == "" || seen[k.StockKey] || k.Partition < 0 || k.Partition >= c.Partitions {
			return fmt.Errorf("invalid or duplicate stock key: %+v", k.StockKey)
		}
		seen[k.StockKey] = true
	}
	if len(seen) == 0 {
		return fmt.Errorf("no configured stock keys")
	}
	return nil
}

// Key returns the configuration for key and reports whether it is configured.
func (c Config) Key(key StockKey) (KeyConfig, bool) {
	for _, k := range c.Keys {
		if k.StockKey == key {
			return k, true
		}
	}
	return KeyConfig{}, false
}

// Clone returns a copy with an independent Keys slice.
func (c Config) Clone() Config { c.Keys = slices.Clone(c.Keys); return c }

// Digest returns the SHA-256 hex digest of the configuration's JSON encoding,
// with a UTC origin and sorted keys. Callers must supply valid, JSON-encodable
// configuration; key order and the origin's time zone do not affect the digest.
func (c Config) Digest() string {
	c = c.Clone()
	c.Origin = c.Origin.UTC()
	slices.SortFunc(c.Keys, func(a, b KeyConfig) int { return compareKey(a.StockKey, b.StockKey) })
	b, _ := json.Marshal(c) // Config contains only JSON-representable schema fields.
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// Transaction is a complete source assertion about one (StockKey, TxnID).
// NEW and AMEND replace the prior value; CANCEL makes the identity inactive.
// The highest revision controls authority regardless of arrival order. Distinct
// business payloads at that revision block computation for the whole stock key.
// Active zero-quantity transactions still create an activity day; cancellations do not.
type Transaction struct {
	TxnID      string          `json:"txn_id"`
	Revision   int64           `json:"revision"` // Positive source authority, independent of output versions.
	Kind       TransactionKind `json:"kind"`
	StockKey   StockKey        `json:"stock_key"`
	ValidTime  time.Time       `json:"valid_time"`  // Effective time; Decode normalizes it to UTC.
	Qty        int64           `json:"qty"`         // Positive receipt or negative issue; CANCEL requires zero.
	SourceTime time.Time       `json:"source_time"` // Informational only; excluded from authority equality.
}

// Coordinates locates a record in a source incarnation. Offset is a broker
// position, not a count of business records; consecutive records may have gaps.
type Coordinates struct {
	Source    string `json:"source"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

// Record retains source coordinates, a decoded routing key, and the original
// input bytes. Value uses base64 in trace JSON, preserving even malformed input.
type Record struct {
	Coordinates
	Key   StockKey `json:"key"`
	Value []byte   `json:"value"`
	// RawKey preserves broker key bytes, including invalid encodings, in quarantine.
	// Memory fixtures may omit it; authority never depends on these bytes.
	RawKey []byte `json:"raw_key,omitempty"`
}

// Clone returns a record with independent Value and RawKey byte slices.
func (r Record) Clone() Record {
	r.Value = slices.Clone(r.Value)
	r.RawKey = slices.Clone(r.RawKey)
	return r
}

// Rejection retains an invalid input and its validation reason code. A rejected
// record supplies no revision authority; storage commits it with input progress.
type Rejection struct {
	Record Record `json:"record"`
	Reason string `json:"reason"`
}

// Conflict identifies a revision with distinct canonical business payloads.
// It remains diagnostic after a higher unique revision resolves its authority.
type Conflict struct {
	StockKey StockKey `json:"stock_key"`
	TxnID    string   `json:"txn_id"`
	Revision int64    `json:"revision"`
}

// Ref returns the stable JSON identity used in blocked-key reason lists.
func (c Conflict) Ref() string {
	b, _ := json.Marshal(c)
	return string(b)
}

// Balance is the cumulative inventory at the close of an active UTC day.
// Day is YYYY-MM-DD. Inactive days have no row and carry the prior close, or
// opening balance, implicitly.
type Balance struct {
	Day   string `json:"day"`
	Close int64  `json:"closing_balance"`
}

// Alert records a historical downward crossing on Day (YYYY-MM-DD, UTC): the
// preceding close is at or above Threshold and Close is below it. A later receipt
// does not retract the crossing unless a correction changes the earlier history.
type Alert struct {
	Day       string `json:"crossing_day"`
	Threshold int64  `json:"reorder_threshold"`
	Close     int64  `json:"closing_balance"`
}

// Projection contains a key's trust status and day-sorted business facts.
// Reasons are sorted conflict references for BLOCKED authority. A blocked fold
// retains its last computed balances and alerts; the oracle leaves them empty.
// Neither representation certifies business values while Status is BLOCKED.
type Projection struct {
	Key      StockKey  `json:"stock_key"`
	Status   KeyStatus `json:"key_status"`
	Reasons  []string  `json:"reasons,omitempty"`
	Balances []Balance `json:"balances"`
	Alerts   []Alert   `json:"active_crossing_alerts"`
}

// Clone returns a projection with independent reason, balance, and alert slices.
func (p Projection) Clone() Projection {
	p.Reasons = slices.Clone(p.Reasons)
	p.Balances = slices.Clone(p.Balances)
	p.Alerts = slices.Clone(p.Alerts)
	return p
}

// OutputID identifies one versioned fact within a namespace. Day is a UTC
// YYYY-MM-DD for balances and alerts, and empty for a key-status fact.
type OutputID struct {
	Namespace string       `json:"namespace"`
	Family    OutputFamily `json:"family"`
	Key       StockKey     `json:"stock_key"`
	Day       string       `json:"day,omitempty"`
}

// String returns a JSON tuple, avoiding separator collisions in arbitrary identifiers.
func (id OutputID) String() string {
	b, _ := json.Marshal([]string{id.Namespace, string(id.Family), id.Key.Item, id.Key.Location, id.Day})
	return string(b)
}

// Payload is a business value or key status, independent of delivery metadata.
// Present=false withdraws a balance or alert and requires zero-valued fields.
// Close is used by balances and alerts; Threshold only by alerts; Status and
// sorted Reasons only by key-status messages, which are always present.
type Payload struct {
	Present   bool      `json:"present"`
	Close     int64     `json:"closing_balance,omitempty"`
	Threshold int64     `json:"reorder_threshold,omitempty"`
	Status    KeyStatus `json:"status,omitempty"`
	Reasons   []string  `json:"reasons,omitempty"`
}

// Equal reports whether all business fields match. Nil and empty Reasons are
// equal; reason order matters. Output identity, version, and cause are separate.
func (p Payload) Equal(q Payload) bool {
	return p.Present == q.Present && p.Close == q.Close && p.Threshold == q.Threshold && p.Status == q.Status && slices.Equal(p.Reasons, q.Reasons)
}

// Cause attributes an output revision to the source record that triggered it.
// It is not a complete list of dependencies of a cumulative balance or alert.
type Cause struct {
	Coordinates
	TxnID    string `json:"txn_id,omitempty"`
	Revision int64  `json:"revision,omitempty"`
}

// Envelope is a committed output change. Version starts at one and increases
// per OutputID, including withdrawals and reappearances. Consumers retain the
// highest version even when Payload.Present is false. Retries preserve the
// complete envelope, including Cause, instead of allocating another version.
type Envelope struct {
	SchemaVersion int        `json:"schema_version"`
	ID            OutputID   `json:"output_id"`
	Version       int64      `json:"version"`
	Type          OutputType `json:"type"`
	Payload       Payload    `json:"payload"`
	Cause         Cause      `json:"cause"`
}

// Clone returns an envelope with an independent payload reason slice.
func (e Envelope) Clone() Envelope { e.Payload.Reasons = slices.Clone(e.Payload.Reasons); return e }

// Equal reports whether identity, version, type, payload, schema, and cause
// match. Equal-version delivery requires this comparison, not just [Payload.Equal].
func (e Envelope) Equal(f Envelope) bool {
	return e.SchemaVersion == f.SchemaVersion && e.ID == f.ID && e.Version == f.Version && e.Type == f.Type && e.Payload.Equal(f.Payload) && e.Cause == f.Cause
}

func compareKey(a, b StockKey) int {
	if a.Item < b.Item {
		return -1
	}
	if a.Item > b.Item {
		return 1
	}
	if a.Location < b.Location {
		return -1
	}
	if a.Location > b.Location {
		return 1
	}
	return 0
}
