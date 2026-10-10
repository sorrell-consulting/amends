// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Package verify compares an independently scanned source prefix with a
// complete durable view boundary. It does not supply oracle business decisions.
package verify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/oracle"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/sorrell-consulting/amends/internal/view"
)

// Implementation identifies the verification protocol in saved reports.
const Implementation = "bounded-verifier-v1"

// Checks records the completed prerequisites for a comparable source/view cut.
// An unobserved check remains false; it must not be inferred from zero lag.
type Checks struct {
	WritersStopped     bool `json:"writers_stopped"`
	SourceScanned      bool `json:"source_scanned"`
	WorkersReached     bool `json:"workers_reached"`
	PublicationDrained bool `json:"publication_drained"`
	ViewReached        bool `json:"view_reached"`
	BoundariesStable   bool `json:"boundaries_stable"`
}

// ConflictEvidence retains the raw assertions at one conflicting revision,
// including superseded conflicts that still qualify verification.
type ConflictEvidence struct {
	Conflict ledger.Conflict `json:"conflict"`
	Records  []ledger.Record `json:"records"`
}

// Diagnostics contains exact rejected inputs and conflicting source assertions.
type Diagnostics struct {
	Rejections []ledger.Rejection `json:"rejections"`
	Conflicts  []ConflictEvidence `json:"conflicts"`
}

// KeyResult separates source authority, observed view status, and comparison.
// Compared and Agrees are false for unresolved authority or an incomplete check.
type KeyResult struct {
	Key             ledger.StockKey  `json:"stock_key"`
	AuthorityStatus ledger.KeyStatus `json:"authority_status"`
	Reasons         []string         `json:"reasons,omitempty"`
	ViewStatus      ledger.KeyStatus `json:"view_status"`
	Compared        bool             `json:"compared"`
	Agrees          bool             `json:"agrees"`
}

// Difference describes a business, trust-status, or retained-evidence mismatch.
// Key and Day may be absent for comparisons spanning the whole namespace.
type Difference struct {
	Key              *ledger.StockKey     `json:"stock_key,omitempty"`
	Field            string               `json:"field"`
	Day              string               `json:"day,omitempty"`
	Expected         any                  `json:"expected"`
	Actual           any                  `json:"actual"`
	SourceReferences []ledger.Coordinates `json:"source_references,omitempty"`
}

// Report records a verification result and the source/output boundary behind it.
// Status is PASS, PASS_WITH_EXCLUSIONS, BLOCKED, DRIFT, INCOMPLETE, or ERROR.
// Only PASS is unqualified agreement. SourceEnd and OutputEnd are exclusive broker
// offsets by partition, not record counts; missing boundaries remain unknown.
// Early failure can leave partial observations and known conflict diagnostics.
type Report struct {
	Status                   string            `json:"status"`
	Scope                    string            `json:"scope"`
	Namespace                string            `json:"namespace"`
	ConfigDigest             string            `json:"config_digest"`
	SchemaVersion            int               `json:"schema_version"`
	Implementation           string            `json:"implementation"`
	GoVersion                string            `json:"go_version"`
	BinarySHA256             string            `json:"binary_sha256,omitempty"`
	LedgerOrigin             time.Time         `json:"ledger_origin"`
	SourceTopic              string            `json:"source_topic"`
	SourceIncarnation        string            `json:"source_incarnation"`
	SourceStart              []int64           `json:"source_start"`
	SourceEnd                []int64           `json:"source_end"`
	OutputTopic              string            `json:"output_topic"`
	OutputIncarnation        string            `json:"output_incarnation"`
	OutputStart              []int64           `json:"output_start"`
	OutputEnd                []int64           `json:"output_end"`
	Checks                   Checks            `json:"checks"`
	Progress                 postgres.Progress `json:"progress"`
	AuthorityBasis           string            `json:"authority_basis"`
	Keys                     []KeyResult       `json:"keys"`
	KnownConflicts           []ledger.Conflict `json:"known_conflicts"`
	Diagnostics              Diagnostics       `json:"diagnostics"`
	SourceRecords            int               `json:"source_records"`
	AcceptedRecords          int               `json:"accepted_records"`
	Differences              []Difference      `json:"differences"`
	Reason                   string            `json:"reason,omitempty"`
	ExpectedDiagnostics      string            `json:"expected_diagnostics,omitempty"`
	ExpectedDiagnosticsMatch bool              `json:"expected_diagnostics_match"`
	ElapsedMillis            int64             `json:"elapsed_ms"`
}

// ExpectedDiagnostics is an exact, named fixture, tied to one source boundary.
// Raw bytes and coordinates distinguish different rejected/conflicting inputs.
// No setting suppresses unknown quarantine or source violations.
type ExpectedDiagnostics struct {
	Name         string      `json:"name"`
	Namespace    string      `json:"namespace"`
	ConfigDigest string      `json:"config_digest"`
	SourceEnd    []int64     `json:"source_end"`
	Diagnostics  Diagnostics `json:"diagnostics"`
}

// ReadExpected decodes one diagnostics fixture, rejecting unknown fields,
// trailing JSON, and missing identity or source-boundary fields. Matching the
// fixture against a completed report is a separate [Report.Accepts] operation.
func ReadExpected(r io.Reader) (*ExpectedDiagnostics, error) {
	var e ExpectedDiagnostics
	d := json.NewDecoder(r)
	d.DisallowUnknownFields()
	if err := d.Decode(&e); err != nil {
		return nil, err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("trailing diagnostics manifest content")
	}
	if e.Name == "" || e.Namespace == "" || e.ConfigDigest == "" || len(e.SourceEnd) == 0 {
		return nil, fmt.Errorf("diagnostics fixture needs name, namespace, digest, and source end")
	}
	return &e, nil
}

// Accepts reports whether the completed result meets the requested diagnostics
// policy. Without e it accepts only PASS. With e it requires an exact fixture
// match and accepts PASS or PASS_WITH_EXCLUSIONS, leaving Status unchanged.
//
// A non-nil e updates ExpectedDiagnostics and ExpectedDiagnosticsMatch in r;
// nil leaves those fields untouched. This method checks an existing report and
// does not establish a verification boundary or perform another comparison.
func (r *Report) Accepts(e *ExpectedDiagnostics) bool {
	if e != nil {
		r.ExpectedDiagnostics = e.Name
		r.ExpectedDiagnosticsMatch = e.Name != "" && e.Namespace == r.Namespace && e.ConfigDigest == r.ConfigDigest && slices.Equal(e.SourceEnd, r.SourceEnd) && sameDiagnostics(e.Diagnostics, r.Diagnostics)
	}
	if r.Status == "PASS" {
		return e == nil || r.ExpectedDiagnosticsMatch
	}
	return r.Status == "PASS_WITH_EXCLUSIONS" && e != nil && r.ExpectedDiagnosticsMatch
}

func recordLess(a, b ledger.Record) int {
	if a.Partition < b.Partition {
		return -1
	}
	if a.Partition > b.Partition {
		return 1
	}
	if a.Offset < b.Offset {
		return -1
	}
	if a.Offset > b.Offset {
		return 1
	}
	return 0
}
func sameRecord(a, b ledger.Record) bool {
	return a.Coordinates == b.Coordinates && a.Key == b.Key && bytes.Equal(a.RawKey, b.RawKey) && bytes.Equal(a.Value, b.Value)
}
func sameRejections(a, b []ledger.Rejection) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	less := func(x, y ledger.Rejection) int { return recordLess(x.Record, y.Record) }
	slices.SortFunc(a, less)
	slices.SortFunc(b, less)
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Reason != b[i].Reason || !sameRecord(a[i].Record, b[i].Record) {
			return false
		}
	}
	return true
}
func sameDiagnostics(a, b Diagnostics) bool {
	if !sameRejections(a.Rejections, b.Rejections) || len(a.Conflicts) != len(b.Conflicts) {
		return false
	}
	a.Conflicts, b.Conflicts = slices.Clone(a.Conflicts), slices.Clone(b.Conflicts)
	less := func(x, y ConflictEvidence) int {
		if x.Conflict.Ref() < y.Conflict.Ref() {
			return -1
		}
		if x.Conflict.Ref() > y.Conflict.Ref() {
			return 1
		}
		return 0
	}
	slices.SortFunc(a.Conflicts, less)
	slices.SortFunc(b.Conflicts, less)
	for i, c := range a.Conflicts {
		d := b.Conflicts[i]
		if c.Conflict != d.Conflict || len(c.Records) != len(d.Records) {
			return false
		}
		x, y := slices.Clone(c.Records), slices.Clone(d.Records)
		slices.SortFunc(x, recordLess)
		slices.SortFunc(y, recordLess)
		for j := range x {
			if !sameRecord(x[j], y[j]) {
				return false
			}
		}
	}
	return true
}

func diagnostics(expected oracle.Result, records []ledger.Record) Diagnostics {
	d := Diagnostics{Rejections: expected.Rejections}
	rejected := map[ledger.Coordinates]bool{}
	for _, r := range expected.Rejections {
		rejected[r.Record.Coordinates] = true
	}
	for _, conflict := range expected.Conflicts {
		e := ConflictEvidence{Conflict: conflict}
		for _, r := range records {
			if rejected[r.Coordinates] {
				continue
			}
			t, err := ledger.Decode(r.Value)
			if err == nil && t.StockKey == conflict.StockKey && t.TxnID == conflict.TxnID && t.Revision == conflict.Revision {
				e.Records = append(e.Records, r.Clone())
			}
		}
		slices.SortFunc(e.Records, recordLess)
		d.Conflicts = append(d.Conflicts, e)
	}
	return d
}

// compare is only called after all completion checks hold. BLOCKED keeps its
// precedence even if another key has a difference; those differences stay visible.
func compare(r *Report, expected oracle.Result, actual view.State, rejected []ledger.Rejection, conflicts []ledger.Conflict, history []ledger.Record) {
	r.Keys = nil
	blocked := false
	for _, want := range expected.Keys {
		got := actual.Projection(want.Key)
		key := KeyResult{Key: want.Key, AuthorityStatus: want.Status, Reasons: want.Reasons, ViewStatus: got.Status, Compared: true, Agrees: true}
		add := func(field, day string, w, g any) {
			key.Agrees = false
			d := Difference{Key: &want.Key, Field: field, Day: day, Expected: w, Actual: g}
			for _, record := range history {
				if record.Key == want.Key {
					d.SourceReferences = append(d.SourceReferences, record.Coordinates)
				}
			}
			r.Differences = append(r.Differences, d)
		}
		if want.Status != got.Status || !slices.Equal(want.Reasons, got.Reasons) {
			add("trust", "", []any{want.Status, want.Reasons}, []any{got.Status, got.Reasons})
		}
		if want.Status == "BLOCKED" {
			blocked = true
			key.Compared = false
			key.Agrees = false
		} else {
			if !slices.Equal(want.Balances, got.Balances) {
				add("balances", firstBalanceDifference(want.Balances, got.Balances), want.Balances, got.Balances)
			}
			if !slices.Equal(want.Alerts, got.Alerts) {
				add("alerts", firstAlertDifference(want.Alerts, got.Alerts), want.Alerts, got.Alerts)
			}
		}
		r.Keys = append(r.Keys, key)
	}
	if !sameRejections(expected.Rejections, rejected) {
		r.Differences = append(r.Differences, Difference{Field: "quarantine", Expected: expected.Rejections, Actual: rejected})
	}
	if !slices.Equal(expected.Conflicts, conflicts) {
		r.Differences = append(r.Differences, Difference{Field: "conflict_evidence", Expected: expected.Conflicts, Actual: conflicts})
	}
	switch {
	case blocked:
		r.Status = "BLOCKED"
	case len(r.Differences) > 0:
		r.Status = "DRIFT"
	case len(expected.Rejections)+len(expected.Conflicts) > 0:
		r.Status = "PASS_WITH_EXCLUSIONS"
	default:
		r.Status = "PASS"
	}
}
func firstBalanceDifference(a, b []ledger.Balance) string {
	for i := 0; i < len(a) || i < len(b); i++ {
		if i == len(a) {
			return b[i].Day
		}
		if i == len(b) {
			return a[i].Day
		}
		if a[i] != b[i] {
			if a[i].Day < b[i].Day {
				return a[i].Day
			}
			return b[i].Day
		}
	}
	return ""
}
func firstAlertDifference(a, b []ledger.Alert) string {
	for i := 0; i < len(a) || i < len(b); i++ {
		if i == len(a) {
			return b[i].Day
		}
		if i == len(b) {
			return a[i].Day
		}
		if a[i] != b[i] {
			if a[i].Day < b[i].Day {
				return a[i].Day
			}
			return b[i].Day
		}
	}
	return ""
}
