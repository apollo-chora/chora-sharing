-- ============================================================================
-- chora-sharing : 0031_outbox_dead_letters.up.sql
--
-- Creates the sharing_outbox_dead_letters table referenced by the
-- PostgresStore.Deadletter() method in
-- internal/adapter/outbox/store.go. The table was referenced in Go code
-- but never created by any migration — this fixes the gap.
--
-- Operational table (same class as sharing_outbox_events): NO RLS.
-- The Relay dispatcher needs to see all tenants' dead-letter rows.
-- ============================================================================

CREATE TABLE sharing_outbox_dead_letters (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    outbox_event_id TEXT        NOT NULL UNIQUE,
    failure_reason  TEXT        NOT NULL DEFAULT '',
    attempt_count   INT         NOT NULL DEFAULT 0,
    worker_id       TEXT        NOT NULL DEFAULT '',
    deadlettered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX sharing_outbox_dead_letters_deadlettered_at_idx
    ON sharing_outbox_dead_letters (deadlettered_at DESC);

COMMENT ON TABLE sharing_outbox_dead_letters IS
'Dead-letter store for outbox rows that exhausted retry budget. Inserted by
the PostgresStore.Deadletter() method when retry_count exceeds the configured
threshold. Rows stay for audit / manual replay. Operational — no RLS.';
