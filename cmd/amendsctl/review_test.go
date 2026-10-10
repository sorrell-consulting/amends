package main

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/fold"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
)

func TestReviewEvidenceAndSelection(test *testing.T) {
	config, err := fixtures.Config("review-test")
	if err != nil {
		test.Fatal(err)
	}
	key := config.Keys[0].StockKey
	conflict := ledger.Conflict{StockKey: key, TxnID: "ISSUE-001", Revision: 2}
	first := ledger.Coordinates{Source: config.Source, Partition: 0, Offset: 3}
	second := ledger.Coordinates{Source: config.Source, Partition: 0, Offset: 4}
	state := fold.State{Projection: ledger.Projection{Key: key, Status: "BLOCKED", Reasons: []string{conflict.Ref()}}, Identities: map[string]fold.Identity{
		"ISSUE-001": {Highest: 2, Revisions: map[int64][]fold.Candidate{2: {
			{Transaction: ledger.Transaction{TxnID: "ISSUE-001", Revision: 2, StockKey: key, Kind: "AMEND", Qty: -30}, Sources: []ledger.Coordinates{first}},
			{Transaction: ledger.Transaction{TxnID: "ISSUE-001", Revision: 2, StockKey: key, Kind: "AMEND", Qty: -40}, Sources: []ledger.Coordinates{second}},
		}}},
	}}
	rejection := ledger.Rejection{Reason: "MALFORMED", Record: ledger.Record{Coordinates: second, RawKey: []byte{0xff, 0x00}, Value: []byte{0xfe, '<', '>'}}}
	snapshot := store.Snapshot{Config: config, Partitions: map[int]store.Partition{0: {Epoch: 7, Next: 6}}, Keys: map[ledger.StockKey]fold.State{key: state}, Rejections: []ledger.Rejection{rejection}}
	for _, kind := range []string{"quarantine", "conflicts"} {
		report, err := selectReview(snapshot, kind, reviewSelector{})
		if err != nil || report.Count != 1 || !strings.Contains(report.Scope, "not a verification result") || report.Namespace != config.Namespace || report.ConfigDigest != config.Digest() || report.SourceIncarnation != config.Source || len(report.Partitions) != 1 || report.Partitions[0].SourceNext != 6 {
			test.Fatalf("incorrect snapshot context: %+v error=%v", report, err)
		}
	}
	report, err := selectReview(snapshot, "quarantine", reviewSelector{Show: true, Partition: 0, Offset: 4})
	if err != nil || len(report.Rejections) != 1 || !reflect.DeepEqual(report.Rejections[0], rejection) {
		test.Fatalf("rejected raw bytes/reason changed: %+v error=%v", report, err)
	}
	report.Rejections[0].Record.RawKey[0] = 1
	if rejection.Record.RawKey[0] != 0xff {
		test.Fatal("review aliases stored raw evidence")
	}
	selected := reviewSelector{Show: true, Key: key, TxnID: "ISSUE-001", Revision: 2}
	report, err = selectReview(snapshot, "conflicts", selected)
	if err != nil || report.Count != 1 {
		test.Fatalf("conflict selection failed: %+v error=%v", report, err)
	}
	actual := report.Conflicts[0]
	if actual.Conflict != conflict || actual.HighestRevision != 2 || !actual.AtHighestRevision || actual.KeyStatus != "BLOCKED" || len(actual.Candidates) != 2 || actual.Candidates[0].Payload.Qty != -30 || actual.Candidates[1].Payload.Qty != -40 || !reflect.DeepEqual(actual.Candidates[0].Sources, []ledger.Coordinates{first}) || !reflect.DeepEqual(actual.Candidates[1].Sources, []ledger.Coordinates{second}) {
		test.Fatalf("ambiguous authority lost: %+v", actual)
	}
	actual.Candidates[0].Sources[0].Offset++
	actual.KeyReasons[0] = "changed"
	if state.Identities["ISSUE-001"].Revisions[2][0].Sources[0] != first || state.Projection.Reasons[0] != conflict.Ref() {
		test.Fatal("review aliases stored conflict evidence")
	}
	identity := state.Identities["ISSUE-001"]
	identity.Highest = 3
	identity.Revisions[3] = []fold.Candidate{{Transaction: ledger.Transaction{TxnID: "ISSUE-001", Revision: 3, Kind: "CANCEL"}}}
	state.Identities["ISSUE-001"] = identity
	state.Projection.Status, state.Projection.Reasons = "READY", nil
	snapshot.Keys[key] = state
	report, err = selectReview(snapshot, "conflicts", selected)
	if err != nil || len(report.Conflicts) != 1 || report.Conflicts[0].AtHighestRevision || report.Conflicts[0].HighestRevision != 3 || report.Conflicts[0].KeyStatus != "READY" {
		test.Fatalf("superseded conflict hidden or still called current: %+v error=%v", report, err)
	}
	state.Projection.Status = "BLOCKED"
	state.Projection.Reasons = []string{"another identity remains ambiguous"}
	snapshot.Keys[key] = state
	report, err = selectReview(snapshot, "conflicts", selected)
	if err != nil || report.Conflicts[0].AtHighestRevision || report.Conflicts[0].KeyStatus != "BLOCKED" {
		test.Fatalf("superseded identity incorrectly certified the whole key: %+v error=%v", report, err)
	}
	for _, missing := range []reviewSelector{
		{Show: true, Key: key, TxnID: "missing", Revision: 2},
		{Show: true, Key: key, TxnID: "ISSUE-001", Revision: 3},
		{Show: true, Key: ledger.StockKey{Item: "different", Location: key.Location}, TxnID: "ISSUE-001", Revision: 2},
	} {
		if _, err = selectReview(snapshot, "conflicts", missing); err == nil {
			test.Fatal("missing evidence silently selected something else")
		}
	}
	if _, err = selectReview(snapshot, "quarantine", reviewSelector{Show: true, Partition: 1, Offset: 4}); err == nil {
		test.Fatal("quarantine selection ignored partition")
	}
	if _, err = selectReview(snapshot, "release", reviewSelector{}); err == nil {
		test.Fatal("unsupported review operation accepted")
	}
	snapshot.Rejections = nil
	report, err = selectReview(snapshot, "quarantine", reviewSelector{})
	if err != nil || report.Count != 0 {
		test.Fatal("empty list is not a successful empty observation")
	}
}

func TestReviewCLIRejectsInvalidUsage(test *testing.T) {
	test.Setenv("AMENDS_DATABASE_URL", "postgres://unused")
	for _, args := range [][]string{
		{"quarantine"}, {"quarantine", "release"}, {"conflicts", "resolve"},
		{"quarantine", "list"}, {"conflicts", "show", "-manifest", "unused"},
		{"quarantine", "show", "-manifest", "unused", "-partition", "0"},
		{"quarantine", "show", "-manifest", "unused", "-partition", "-1", "-offset", "0"},
		{"quarantine", "list", "-manifest", "unused", "-offset", "0"},
		{"conflicts", "list", "-manifest", "unused", "-timeout", "0s"},
		{"conflicts", "show", "-manifest", "unused", "-item", "DEMO-PART", "-location", "DEPOT-A", "-txn-id", "ISSUE-001", "-revision", "0"},
		{"inspect", "-manifest", "unused", "-format", "unsupported"},
		{"verify", "-manifest", "unused", "--quiesce", "-format", "html"},
	} {
		var output, diagnostic bytes.Buffer
		if code := run(args, &output, &diagnostic); code != 2 || output.Len() != 0 || diagnostic.Len() == 0 {
			test.Fatalf("invalid usage reached I/O: args=%v code=%d output=%s diagnostic=%s", args, code, output.String(), diagnostic.String())
		}
	}
	for _, args := range [][]string{{"quarantine", "-h"}, {"conflicts", "--help"}, {"quarantine", "show", "-h"}, {"conflicts", "list", "-h"}} {
		var output, diagnostic bytes.Buffer
		if code := run(args, &output, &diagnostic); code != 0 || diagnostic.Len() == 0 {
			test.Fatalf("review help failed: %v code=%d %s", args, code, diagnostic.String())
		}
	}
}
