// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package worker

import (
	"errors"

	"github.com/sorrell-consulting/amends/internal/store"
)

// Event codes and their namespace/partition/offset/epoch/outcome attributes are
// consumed by the owned drills. Message wording and attribute order are free
// to change. Attempts additionally report storage_call_elapsed_ns.
const (
	ProcessingAttemptEvent = "processing_attempt"
	ProcessingStoppedEvent = "processing_stopped"
)

func processingOutcome(err error) string {
	switch {
	case errors.Is(err, store.ErrCommitUnknown):
		return "unknown"
	case errors.Is(err, store.ErrFence):
		return "fenced"
	case err != nil:
		return "error"
	default:
		return "acknowledged"
	}
}
