// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package showcase

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/sorrell-consulting/amends/internal/worker"
)

type workerEvent struct {
	Event     string `json:"event"`
	Namespace string `json:"namespace"`
	Partition *int   `json:"partition"`
	Offset    *int64 `json:"offset"`
	Epoch     *int64 `json:"epoch"`
	Outcome   string `json:"outcome"`
	Elapsed   *int64 `json:"storage_call_elapsed_ns"`
}

// Ignore human messages and unrelated events, but fail on incomplete evidence.
// Pointers distinguish a missing coordinate from a legitimate zero value.
func readWorkerEvents(raw []byte) ([]workerEvent, error) {
	var events []workerEvent
	decoder := json.NewDecoder(bytes.NewReader(raw))
	for {
		var event workerEvent
		if err := decoder.Decode(&event); errors.Is(err, io.EOF) {
			return events, nil
		} else if err != nil {
			return nil, fmt.Errorf("read worker event: %w", err)
		}
		if event.Event != worker.ProcessingAttemptEvent && event.Event != worker.ProcessingStoppedEvent {
			continue
		}
		if event.Namespace == "" || event.Partition == nil || *event.Partition < 0 || event.Offset == nil || *event.Offset < 0 || event.Epoch == nil || *event.Epoch < 1 {
			return nil, fmt.Errorf("worker event %q has incomplete attribution", event.Event)
		}
		switch event.Outcome {
		case "acknowledged", "unknown", "fenced", "error":
		default:
			return nil, fmt.Errorf("worker event %q has unknown outcome %q", event.Event, event.Outcome)
		}
		if event.Event == worker.ProcessingAttemptEvent && (event.Elapsed == nil || *event.Elapsed < 0) {
			return nil, fmt.Errorf("processing attempt has no valid elapsed time")
		}
		events = append(events, event)
	}
}
