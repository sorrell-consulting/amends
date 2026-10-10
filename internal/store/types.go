// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Package store defines values and error identities shared by storage adapters,
// worker/delivery protocols, and deterministic schedules.
package store

import (
	"time"

	"github.com/sorrell-consulting/amends/internal/fold"
	"github.com/sorrell-consulting/amends/internal/ledger"
)

// Partition is a source partition's committed ownership and processing progress.
type Partition struct {
	Epoch    int64 // Ownership generation; zero means no owner has claimed it.
	Next     int64 // Exclusive source offset to resume from, not a record count.
	Sequence int64 // Last allocated outbox sequence; zero means no emissions yet.
}

// OutputBinding is installed once, separately from the existing source config.
type OutputBinding struct {
	Topic       string `json:"topic"`
	Incarnation string `json:"incarnation"`
	Partitions  int    `json:"partitions"`
}

// Token binds a processing claim to immutable configuration and an ownership
// generation. Observing a token does not prove that the caller owns it.
type Token struct {
	Namespace, Digest string
	Partition         int
	Epoch             int64
}

// Emission is a memory-store outbox entry. Sequence orders committed intent
// within its partition; Acknowledged records publication, not view application.
type Emission struct {
	Partition    int
	Sequence     int64
	Envelope     ledger.Envelope
	Acknowledged bool
}

// Snapshot is a consistent copy of a namespace's processing state. Outputs
// includes withdrawals, and Outbox includes both pending and acknowledged intent.
// It is not a snapshot of downstream application or a verification result.
type Snapshot struct {
	Config     ledger.Config
	Partitions map[int]Partition
	Keys       map[ledger.StockKey]fold.State
	Outputs    map[ledger.OutputID]ledger.Envelope
	Outbox     []Emission
	Rejections []ledger.Rejection
}

// Delivery is committed publication intent. Retries publish Bytes unchanged;
// Envelope provides the decoded identity and version, not replacement wire data.
type Delivery struct {
	Partition int
	Sequence  int64
	Envelope  ledger.Envelope
	Bytes     []byte
	CreatedAt time.Time
}

// ViewInput carries the broker evidence and the independently decoded envelope.
// WireError is a transport/encoding rejection; business validation stays in view.
type ViewInput struct {
	Partition  int
	Offset     int64
	Envelope   ledger.Envelope
	Key, Value []byte
	WireError  string
}

// ViewPosition reports durable next output offset and any retained protocol stop.
type ViewPosition struct {
	Next  int64
	Error string
}
