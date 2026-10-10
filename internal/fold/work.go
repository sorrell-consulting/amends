package fold

import "github.com/sorrell-consulting/amends/internal/ledger"

// Work describes derivation performed by one successful [ApplyMeasured] call.
// It excludes evidence scans, state copying, output diffing, and storage costs.
type Work struct {
	Duplicate  bool `json:"duplicate"`   // The canonical payload was already retained.
	Stale      bool `json:"stale"`       // Its revision was below the prior highest revision.
	Refold     bool `json:"refold"`      // Derivation ran, even if the suffix was empty.
	FullRefold bool `json:"full_refold"` // Resolving BLOCKED authority required the whole key.
	// FromDay is the earliest repair day (YYYY-MM-DD, UTC). A full refold uses
	// the earliest old or remaining active day; no affected day leaves it empty.
	FromDay string `json:"from_day,omitempty"`
	// RewindDays is the nonnegative calendar distance from FromDay to the old
	// last balance day. It excludes the starting day and is not an activity count.
	RewindDays int64 `json:"rewind_days"`
	// RefoldedRecords counts authoritative active transactions in the suffix.
	RefoldedRecords int64 `json:"refolded_records"`
}

// ApplyMeasured is [Apply] with derivation counts. It has the same validation
// preconditions and leaves previous unchanged. On failure it returns zero State
// and Work values; neither partial state nor partial measurements may be committed.
func ApplyMeasured(previous State, transaction ledger.Transaction, at ledger.Coordinates, key ledger.KeyConfig) (State, Work, error) {
	work := Work{}
	next, err := apply(previous, transaction, at, key, &work)
	if err != nil {
		return State{}, Work{}, err
	}
	return next, work, nil
}
