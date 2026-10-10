// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package ledger

// TransactionKind names a complete active assertion or cancellation. Decoding
// accepts unknown strings so business validation can retain rejection evidence.
type TransactionKind string

// Source assertion kinds. New and Amend both supply complete active values.
const (
	New    TransactionKind = "NEW"
	Amend  TransactionKind = "AMEND"
	Cancel TransactionKind = "CANCEL"
)

// KeyStatus reports whether the highest revisions for a stock key are resolved.
// Ready does not imply that processing or downstream delivery has caught up.
type KeyStatus string

// Authority states for a stock key.
const (
	Ready   KeyStatus = "READY"
	Blocked KeyStatus = "BLOCKED"
)

// OutputFamily selects the identity and payload rules for a derived fact.
type OutputFamily string

// Output families, each with its own versioned identities.
const (
	BalanceFamily   OutputFamily = "balance"
	AlertFamily     OutputFamily = "alert"
	KeyStatusFamily OutputFamily = "key-status"
)

// OutputType identifies a statement or withdrawal within an [OutputFamily].
// Unknown wire values remain available to the output validator for diagnostics.
type OutputType string

// Output operations. A withdrawal uses the same identity as its statement.
const (
	BalanceStated    OutputType = "BalanceStated"
	BalanceRetracted OutputType = "BalanceRetracted"
	AlertRaised      OutputType = "AlertRaised"
	AlertRetracted   OutputType = "AlertRetracted"
	KeyStatusStated  OutputType = "KeyStatusStated"
)
