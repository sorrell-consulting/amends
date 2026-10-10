// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package verify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/oracle"
	"github.com/sorrell-consulting/amends/internal/view"
)

func TestBoundaryWaitDeadlineClassification(t *testing.T) {
	expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	ioDeadline := fmt.Errorf("database observation: %w", context.DeadlineExceeded)
	connectionLost := errors.New("writer session lost")
	socketDeadline := fmt.Errorf("write failed: %w", &net.OpError{Op: "write", Net: "tcp", Err: os.ErrDeadlineExceeded})
	pendingDeadline := deadlineBeforeCancellation{Context: t.Context(), deadline: time.Now().Add(-time.Second)}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	canceledDeadline := deadlineBeforeCancellation{Context: canceled, deadline: pendingDeadline.deadline}
	for _, tc := range []struct {
		name       string
		ctx        context.Context
		waiting    string
		err        error
		incomplete bool
	}{
		{"producer budget", expired, "owned source loader has not stopped", ioDeadline, true},
		{"worker budget", expired, "workers have not reached source boundary", ioDeadline, true},
		{"view budget", expired, "view has not reached output boundary", ioDeadline, true},
		{"socket timeout at worker budget", expired, "workers have not reached source boundary", socketDeadline, true},
		{"socket deadline precedes context timer", pendingDeadline, "workers have not reached source boundary", socketDeadline, true},
		{"context deadline precedes context timer", pendingDeadline, "view has not reached output boundary", ioDeadline, true},
		{"socket timeout before budget", t.Context(), "workers have not reached source boundary", socketDeadline, false},
		{"socket timeout during initial observation", expired, "", socketDeadline, false},
		{"socket timeout after cancellation", canceledDeadline, "workers have not reached source boundary", socketDeadline, false},
		{"initial observation", expired, "", ioDeadline, false},
		{"shorter adapter deadline", t.Context(), "workers have not reached source boundary", ioDeadline, false},
		{"lost session even at deadline", expired, "view has not reached output boundary", connectionLost, false},
		{"cancellation is not deadline expiry", expired, "owned source loader has not stopped", context.Canceled, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := boundaryWaitError(tc.ctx, tc.waiting, tc.err)
			if errors.Is(err, errIncomplete) != tc.incomplete || !errors.Is(err, tc.err) {
				t.Fatalf("wrong classification or lost cause: %v", err)
			}
			if tc.incomplete && !strings.Contains(err.Error(), tc.waiting) {
				t.Fatalf("missing pending boundary: %v", err)
			}
		})
	}
}

// A socket deadline can fire before the context's timer callback runs.
type deadlineBeforeCancellation struct {
	context.Context
	deadline time.Time
}

func (c deadlineBeforeCancellation) Deadline() (time.Time, bool) { return c.deadline, true }

func TestBlockedReportKeepsOtherKeyDifference(t *testing.T) {
	c, err := fixtures.Config("blocked-scope")
	if err != nil {
		t.Fatal(err)
	}
	blocked := c.Keys[0].StockKey
	other := ledger.StockKey{Item: "OTHER", Location: blocked.Location}
	c.Keys = append(c.Keys, ledger.KeyConfig{StockKey: other, Opening: 100, Threshold: 60, Partition: 0})
	conflict := ledger.Conflict{StockKey: blocked, TxnID: "ambiguous", Revision: 1}
	expected := oracle.Result{Keys: []ledger.Projection{
		{Key: blocked, Status: "BLOCKED", Reasons: []string{conflict.Ref()}},
		{Key: other, Status: "READY", Balances: []ledger.Balance{{Day: "2026-01-01", Close: 80}}},
	}, Conflicts: []ledger.Conflict{conflict}}
	actual := view.New(c)
	id := ledger.OutputID{Namespace: c.Namespace, Family: "key-status", Key: blocked}
	actual.Rows[id] = ledger.Envelope{ID: id, Payload: ledger.Payload{Present: true, Status: "BLOCKED", Reasons: []string{conflict.Ref()}}}
	r := Report{}
	compare(&r, expected, actual, nil, expected.Conflicts, nil)
	if r.Status != "BLOCKED" || len(r.Differences) != 1 || r.Differences[0].Day != "2026-01-01" || r.Keys[0].Compared || !r.Keys[1].Compared || r.Keys[1].Agrees {
		t.Fatalf("scoped mismatch hidden: %+v", r)
	}
	if r.Accepts(&ExpectedDiagnostics{Name: "expected blocked"}) {
		t.Fatal("negative result certified")
	}
}

func TestExpectedDiagnosticsCannotHideNewOrChangedEvidence(t *testing.T) {
	record := ledger.Record{Coordinates: ledger.Coordinates{Source: "source", Partition: 0, Offset: 4}, RawKey: []byte{0, 255}, Value: []byte("bad")}
	d := Diagnostics{Rejections: []ledger.Rejection{{Record: record, Reason: "MALFORMED"}}}
	r := Report{Status: "PASS_WITH_EXCLUSIONS", Namespace: "ns", ConfigDigest: "digest", SourceEnd: []int64{5}, Diagnostics: d}
	fixture := ExpectedDiagnostics{Name: "poison", Namespace: "ns", ConfigDigest: "digest", SourceEnd: []int64{5}, Diagnostics: Diagnostics{Rejections: []ledger.Rejection{{Record: record.Clone(), Reason: "MALFORMED"}}}}
	if r.Accepts(nil) || !r.Accepts(&fixture) || r.Status != "PASS_WITH_EXCLUSIONS" {
		t.Fatal("qualification lost")
	}
	raw, _ := json.Marshal(fixture)
	parsed, err := ReadExpected(bytes.NewReader(raw))
	if err != nil || !r.Accepts(parsed) {
		t.Fatalf("fixture round trip: %v", err)
	}
	for _, mutate := range []func(*ExpectedDiagnostics){
		func(e *ExpectedDiagnostics) { e.SourceEnd[0]++ },
		func(e *ExpectedDiagnostics) { e.Namespace = "other" },
		func(e *ExpectedDiagnostics) { e.Diagnostics.Rejections[0].Record.RawKey[0] = 1 },
		func(e *ExpectedDiagnostics) { e.Diagnostics.Rejections[0].Record.Offset++ },
		func(e *ExpectedDiagnostics) { e.Diagnostics.Rejections[0].Reason = "KIND" },
		func(e *ExpectedDiagnostics) {
			e.Diagnostics.Rejections = append(e.Diagnostics.Rejections, e.Diagnostics.Rejections[0])
		},
	} {
		e, err := ReadExpected(bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		mutate(e)
		if r.Accepts(e) {
			t.Fatal("different expected evidence accepted")
		}
	}
	for _, status := range []string{"BLOCKED", "DRIFT", "ERROR", "INCOMPLETE"} {
		r.Status = status
		if r.Accepts(&fixture) {
			t.Fatalf("%s accepted", status)
		}
	}
	for _, invalid := range []string{"null", "{}", string(raw) + "{}", strings.Replace(string(raw), `"name":"poison"`, `"unknown":"poison"`, 1)} {
		if _, err := ReadExpected(strings.NewReader(invalid)); err == nil {
			t.Fatal("invalid diagnostics manifest accepted")
		}
	}
}
