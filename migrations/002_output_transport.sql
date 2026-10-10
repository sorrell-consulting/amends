-- Copyright 2026 Sorrell Consulting, LLC
-- SPDX-License-Identifier: Apache-2.0
CREATE TABLE amends_output_binding (
    namespace_id TEXT PRIMARY KEY REFERENCES amends_namespaces ON DELETE CASCADE,
    topic_name TEXT NOT NULL,
    incarnation TEXT NOT NULL UNIQUE,
    partitions INTEGER NOT NULL CHECK (partitions > 0)
);
ALTER TABLE amends_view_progress
    ADD COLUMN error_offset BIGINT CHECK (error_offset >= 0),
    ADD COLUMN error_key BYTEA,
    ADD COLUMN error_value BYTEA;
