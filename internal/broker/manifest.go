// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
)

// Manifest binds a source topic name to the immutable configuration persisted
// by PostgreSQL. Broker endpoints and database connection details are supplied
// at runtime.
type Manifest struct {
	Topic  string               `json:"source_topic"`
	Config ledger.Config        `json:"config"`
	Output *store.OutputBinding `json:"output,omitempty"`
}

func ReadManifest(input io.Reader) (Manifest, error) {
	var m Manifest
	d := json.NewDecoder(input)
	d.DisallowUnknownFields()
	if err := d.Decode(&m); err != nil {
		return m, err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return m, fmt.Errorf("trailing manifest content")
	}
	if err := m.Config.Validate(); err != nil {
		return m, err
	}
	id, err := hex.DecodeString(strings.TrimPrefix(m.Config.Source, "kafka:"))
	if m.Topic == "" || !strings.HasPrefix(m.Config.Source, "kafka:") || err != nil || len(id) != 16 || m.Config.Source == Incarnation([16]byte{}) {
		return m, fmt.Errorf("manifest requires source topic and broker UUID")
	}
	if m.Output != nil {
		id, err := hex.DecodeString(strings.TrimPrefix(m.Output.Incarnation, "kafka:"))
		if m.Output.Topic == "" || m.Output.Topic == m.Topic || m.Output.Partitions != m.Config.Partitions || m.Output.Incarnation == m.Config.Source || !strings.HasPrefix(m.Output.Incarnation, "kafka:") || err != nil || len(id) != 16 || m.Output.Incarnation == Incarnation([16]byte{}) {
			return m, fmt.Errorf("invalid output binding")
		}
	}
	return m, nil
}
