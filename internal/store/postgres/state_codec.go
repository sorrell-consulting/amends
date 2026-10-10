// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"encoding/json"
	"fmt"

	"github.com/sorrell-consulting/amends/internal/fold"
)

// Key state uses storage format 1, checked by Store.compatible. Explicit JSON
// names in fold and ledger preserve existing rows. Incompatible changes require
// a format/migration decision, separate from the SQL migration version.
func encodeKeyState(state fold.State) ([]byte, error) {
	raw, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("encode key state (format %d): %w", formatVersion, err)
	}
	return raw, nil
}

func decodeKeyState(raw []byte) (fold.State, error) {
	var state fold.State
	if err := json.Unmarshal(raw, &state); err != nil {
		return state, fmt.Errorf("decode key state (format %d): %w", formatVersion, err)
	}
	return state, nil
}
