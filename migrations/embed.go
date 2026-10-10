// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Package migrations embeds the versioned PostgreSQL schema in the application.
package migrations

import _ "embed"

//go:embed 001_initial.sql
var Initial string

//go:embed 002_output_transport.sql
var OutputTransport string

//go:embed 003_processing_measurements.sql
var ProcessingMeasurements string

//go:embed 004_processing_statistics.sql
var ProcessingStatistics string

// Ordered is append-only. Installed checksums must never change.
func Ordered() []string {
	return []string{Initial, OutputTransport, ProcessingMeasurements, ProcessingStatistics}
}
