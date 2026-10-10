-- Copyright 2026 Sorrell Consulting, LLC
-- SPDX-License-Identifier: Apache-2.0
-- IDs are JSON-encoded strings/tuples in TEXT. Payloads are exact JSON bytes
-- in BYTEA, preserving envelopes and identifiers that JSONB cannot represent.
CREATE TABLE amends_namespaces (
    namespace_id TEXT PRIMARY KEY,
    config_digest TEXT NOT NULL,
    format_version INTEGER NOT NULL CHECK (format_version = 1),
    config BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE amends_partitions (
    namespace_id TEXT NOT NULL REFERENCES amends_namespaces,
    partition INTEGER NOT NULL CHECK (partition >= 0),
    epoch BIGINT NOT NULL DEFAULT 0 CHECK (epoch >= 0),
    next_offset BIGINT NOT NULL DEFAULT 0 CHECK (next_offset >= 0),
    emission_sequence BIGINT NOT NULL DEFAULT 0 CHECK (emission_sequence >= 0),
    PRIMARY KEY (namespace_id, partition)
);
CREATE TABLE amends_key_state (
    namespace_id TEXT NOT NULL,
    key_id TEXT NOT NULL,
    partition INTEGER NOT NULL,
    state BYTEA NOT NULL,
    PRIMARY KEY (namespace_id, key_id),
    FOREIGN KEY (namespace_id, partition) REFERENCES amends_partitions
);
CREATE TABLE amends_output_state (
    namespace_id TEXT NOT NULL,
    output_id TEXT NOT NULL,
    partition INTEGER NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0),
    envelope BYTEA NOT NULL,
    PRIMARY KEY (namespace_id, output_id),
    FOREIGN KEY (namespace_id, partition) REFERENCES amends_partitions
);
CREATE TABLE amends_outbox (
    namespace_id TEXT NOT NULL,
    partition INTEGER NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    output_id TEXT NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0),
    envelope BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    acknowledged_at TIMESTAMPTZ,
    output_offset BIGINT CHECK (output_offset >= 0),
    PRIMARY KEY (namespace_id, partition, sequence),
    UNIQUE (namespace_id, output_id, version),
    FOREIGN KEY (namespace_id, partition) REFERENCES amends_partitions,
    FOREIGN KEY (namespace_id, output_id) REFERENCES amends_output_state,
    CHECK ((acknowledged_at IS NULL) = (output_offset IS NULL))
);
CREATE INDEX amends_pending_outbox ON amends_outbox (namespace_id, partition, sequence)
    WHERE acknowledged_at IS NULL;
CREATE TABLE amends_quarantine (
    namespace_id TEXT NOT NULL,
    partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL CHECK (source_offset >= 0),
    record BYTEA NOT NULL,
    reason TEXT NOT NULL CHECK (reason IN (
        'MALFORMED', 'IDENTITY', 'REVISION', 'KIND', 'CANCEL_QUANTITY',
        'VALID_TIME', 'UNKNOWN_KEY', 'KEY_MISMATCH', 'ROUTING')),
    PRIMARY KEY (namespace_id, partition, source_offset),
    FOREIGN KEY (namespace_id, partition) REFERENCES amends_partitions
);
CREATE TABLE amends_view_progress (
    namespace_id TEXT NOT NULL,
    partition INTEGER NOT NULL,
    next_offset BIGINT NOT NULL DEFAULT 0 CHECK (next_offset >= 0),
    protocol_error TEXT,
    PRIMARY KEY (namespace_id, partition),
    FOREIGN KEY (namespace_id, partition) REFERENCES amends_partitions
);
CREATE TABLE amends_view_rows (
    namespace_id TEXT NOT NULL,
    output_id TEXT NOT NULL,
    partition INTEGER NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0),
    present BOOLEAN NOT NULL,
    envelope BYTEA NOT NULL,
    PRIMARY KEY (namespace_id, output_id),
    FOREIGN KEY (namespace_id, partition) REFERENCES amends_view_progress
);
