package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/sorrell-consulting/amends/internal/fold"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
)

func TestDashboardPreservesUnknownAndUncertifiedState(test *testing.T) {
	zero := int64(0)
	observed := inspection{Namespace: "dashboard-test", Scope: "live-observations; not a verification result", ObservedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Partitions: []inspectedPartition{{PartitionProgress: postgres.PartitionProgress{Partition: 0, Epoch: 2}, SourceEnd: &zero, InputLag: &zero}}}
	var output bytes.Buffer
	if err := renderDashboard(&output, observed); err != nil {
		test.Fatal(err)
	}
	for _, required := range []string{"NOT VERIFIED", "not a verification result", "unknown", "none pending", "not configured", "no diagnostics, and READY status do not establish agreement", "Content-Security-Policy"} {
		if !strings.Contains(output.String(), required) {
			test.Fatalf("dashboard omitted %q", required)
		}
	}
	observed.Partitions[0].Pending = 2
	observed.Partitions[0].ProtocolError = "equal version has differing payload"
	observed.ObservationalErrors = []string{"broker unavailable"}
	observed.WorkerKeys = []ledger.Projection{{Key: ledger.StockKey{Item: "DEMO-PART", Location: "DEPOT-A"}, Status: "BLOCKED", Balances: []ledger.Balance{{Day: "2026-01-01", Close: 70}}}}
	observed.ViewKeys = []ledger.Projection{{Key: observed.WorkerKeys[0].Key, Status: "READY"}}
	observed.ConflictDetails = []reviewedConflict{{AtHighestRevision: true, HighestRevision: 2, KeyStatus: "BLOCKED"}, {AtHighestRevision: false, HighestRevision: 3, KeyStatus: "READY"}}
	output.Reset()
	if err := renderDashboard(&output, observed); err != nil {
		test.Fatal(err)
	}
	for _, required := range []string{"NOT VERIFIED", "Uncertified frozen business values", "BLOCKED", "READY", "2026-01-01", "70", "broker unavailable", "equal version has differing payload", "unresolved highest revision", "superseded conflict", "No activity-day rows.", "No present crossing facts."} {
		if !strings.Contains(output.String(), required) {
			test.Fatalf("dashboard hid diagnostic %q", required)
		}
	}
	if strings.Contains(output.String(), "none pending") {
		test.Fatal("missing pending age fabricated as no pending work")
	}
	if err := renderDashboard(failingWriter{}, observed); err == nil {
		test.Fatal("dashboard ignored output failure")
	}
}

func TestDashboardProcessingMeasurements(test *testing.T) {
	observed := inspection{Partitions: []inspectedPartition{
		{PartitionProgress: postgres.PartitionProgress{Partition: 0, LastProcessing: &postgres.ProcessingMeasurement{
			Source: ledger.Coordinates{Offset: 3}, Epoch: 2,
			Work:            fold.Work{Refold: true, FullRefold: true, FromDay: "2026-01-01", RewindDays: 2, RefoldedRecords: 3},
			BusinessOutputs: 5, StatusOutputs: 1, LockAcquireNS: 1500000, ElapsedBeforeCommitNS: 4500000,
		}}},
		{PartitionProgress: postgres.PartitionProgress{Partition: 1}},
	}}
	var output bytes.Buffer
	if err := renderDashboard(&output, observed); err != nil {
		test.Fatal(err)
	}
	for _, required := range []string{"Latest processing work", "NOT VERIFIED", "Not measured", "Source offset 3", "decision epoch 2", "full key", "2026-01-01", "Changed business outputs", "1.500 ms", "4.500 ms", "not just server lock wait", "before the progress/measurement write and COMMIT", "Not cumulative totals"} {
		if !strings.Contains(output.String(), required) {
			test.Fatalf("dashboard omitted measurement or limit %q", required)
		}
	}
	observed.Partitions[0].LastProcessing = &postgres.ProcessingMeasurement{QuarantineReason: "MALFORMED"}
	output.Reset()
	if err := renderDashboard(&output, observed); err != nil {
		test.Fatal(err)
	}
	if !strings.Contains(output.String(), "quarantined: MALFORMED") || !strings.Contains(output.String(), "0.000 ms") {
		test.Fatal("quarantine/zero measurement was rendered as missing")
	}
}

func TestDashboardEscapesUntrustedText(test *testing.T) {
	attack := `<script>alert("unsafe")</script><img src="https://invalid.example/x" onerror="alert(1)">`
	observed := inspection{Namespace: attack, SourceTopic: attack, ConfigDigest: attack, Scope: attack, ObservationalErrors: []string{attack}, WorkerKeys: []ledger.Projection{{Key: ledger.StockKey{Item: attack, Location: attack}, Status: ledger.KeyStatus(attack), Reasons: []string{attack}}}, Rejections: []ledger.Rejection{{Reason: attack, Record: ledger.Record{Value: []byte(attack)}}}, ConflictDetails: []reviewedConflict{{Conflict: ledger.Conflict{TxnID: attack, StockKey: ledger.StockKey{Item: attack}}}}}
	var output bytes.Buffer
	if err := renderDashboard(&output, observed); err != nil {
		test.Fatal(err)
	}
	for _, forbidden := range []string{"<script", "<img", "<iframe", "<link", "<form", "ZgotmplZ"} {
		if strings.Contains(output.String(), forbidden) {
			test.Fatalf("untrusted markup reached HTML: %s", forbidden)
		}
	}
	if !strings.Contains(output.String(), "&lt;script&gt;") {
		test.Fatal("escaped evidence was omitted")
	}
}

func TestDashboardProcessingStatistics(test *testing.T) {
	observed := inspection{Partitions: []inspectedPartition{
		{PartitionProgress: postgres.PartitionProgress{Partition: 0, Statistics: &postgres.ProcessingStatistics{
			FirstSource: ledger.Coordinates{Offset: 17}, Decisions: 34, CountersSaturated: true,
			Recent: []postgres.ProcessingMeasurement{{Source: ledger.Coordinates{Offset: 50}, QuarantineReason: "<MALFORMED>"}},
		}}},
		{PartitionProgress: postgres.PartitionProgress{Partition: 1}},
	}}
	var output bytes.Buffer
	if err := renderDashboard(&output, observed); err != nil {
		test.Fatal(err)
	}
	for _, required := range []string{"Committed aggregates", "source offset 17", "Latest 1 committed decisions", "at most 32", "lower bounds", "Not collected", "Earlier history is unknown", "&lt;MALFORMED&gt;", "NOT VERIFIED"} {
		if !strings.Contains(output.String(), required) {
			test.Fatalf("dashboard omitted aggregate qualification %q", required)
		}
	}
}
