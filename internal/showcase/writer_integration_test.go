//go:build integration

package showcase

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

// Writer failures are exercised separately from checkpoint cancellation. This
// writer rejects the baseline summary after real verification has completed.
type failingSummaryWriter struct {
	transcript *bytes.Buffer
	err        error
}

func (w failingSummaryWriter) Write(data []byte) (int, error) {
	if bytes.Contains(data, []byte("before: READY")) {
		return 0, w.err
	}
	return w.transcript.Write(data)
}

func TestShowcaseWriterFailure(t *testing.T) {
	options := showcaseTestOptions(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	var transcript bytes.Buffer
	wantErr := errors.New("summary output unavailable")
	options.Output = failingSummaryWriter{transcript: &transcript, err: wantErr}
	checkpointReached := false
	options.OnCheckpoint = func(Checkpoint) { checkpointReached = true }
	report, err := Run(ctx, options)
	retainTestRun(t, options, report, &transcript)
	if !errors.Is(err, wantErr) || report.Status != "ERROR" || report.FinalVerification != "PASS" || len(report.Stages) != 1 || checkpointReached {
		t.Fatalf("writer failure lost or schedule continued: %+v error=%v checkpoint=%t", report, err, checkpointReached)
	}
	for _, child := range report.Processes {
		checkChildExit(t, child, 0)
		if child.Name == "relay-after" || child.Name == "load-correction" {
			t.Fatal("writer failure continued the correction")
		}
	}
}
