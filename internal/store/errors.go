// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package store

import "errors"

// These identities describe storage outcomes consumed by worker and delivery
// protocols. Adapters wrap them with context; callers use errors.Is.
var (
	// ErrFence rejects obsolete ownership, progress, or configuration.
	ErrFence = errors.New("epoch/frontier/configuration fence rejected")
	// ErrCommitUnknown requires durable observation before deciding to retry.
	ErrCommitUnknown = errors.New("commit outcome unknown; reread durable state")
	// ErrProtocol marks a terminal output violation, not a transient I/O failure.
	ErrProtocol = errors.New("output protocol violation")
)
