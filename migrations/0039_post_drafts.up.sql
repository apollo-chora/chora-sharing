-- =============================================================================
-- chora-sharing : 0039_post_drafts.up.sql
--
-- Domain   : Content Sharing (5 core)
-- Database : chora_sharing
-- Story    : CHO-2203 (durable Familiar-milestone lane; parent CHO-1889)
--
-- The durable backing table for the FamiliarMilestoneSubscriber's Draft port.
-- Milestone events from chora-consumption resolve to a per-user share
-- preference; the DEFAULT policy is `draft`, which lands one pending row here for
-- the owner to publish or discard from C+.
--
-- ⚠ RECONCILING AN OUT-OF-BAND TABLE (report this loudly): like user_preferences
-- (0040) and subscriber_idempotency (0041), `post_drafts` ALREADY EXISTED in prod
-- chora_sharing, created OUT-OF-BAND — NO migration file in the repo creates it
-- (grep is empty), so it has no tracker lineage. All THREE milestone tables were
-- hand-provisioned once (matching the FamiliarMilestoneSubscriber package doc's
-- "cmd/server wires Postgres implementations" intent) but the migrations + pg
-- adapters were never written. Its live shape is well-formed: draft_id UUID PK,
-- composed_from_event_id UUID, status enum post_draft_status
-- (pending|published|discarded), UNIQUE(composed_from_topic,
-- composed_from_event_id), RLS + tenant_isolation policy + a set_updated_at
-- trigger, 0 rows. This migration adopts that shape verbatim so the pg adapter
-- and a fresh rebuild agree with deployed reality. Idempotent for BOTH the
-- drifted live DB (everything exists → no-ops) AND a fresh rebuild (CREATE TYPE
-- via DO-guard + CREATE TABLE IF NOT EXISTS). The original 0039 (TEXT status /
-- TEXT event_id / a tenant-scoped UNIQUE) was recorded applied but its CREATE
-- TABLE was SKIPPED on the pre-existing OOB table, so nothing wrong landed; this
-- content correction is fresh-rebuild fidelity only and re-running on live is a
-- safe no-op.
--
-- CONTENT AGGREGATE → SOFT-DELETE (ddd-enforcement.md #6): a draft is user-owned
-- content — deleted_at, NEVER hard-deleted. Discard = soft-delete
-- (status='discarded' + deleted_at=now()); default reads filter deleted_at IS
-- NULL.
--
-- Idempotency: one draft per (composed_from_topic, composed_from_event_id) —
-- event_id is a globally-unique UUIDv7, a sound global dedup key. The subscriber
-- Insert is ON CONFLICT (...) DO NOTHING RETURNING draft_id.
--
-- RLS: ENABLE + FORCE + strict tenant_isolation. GRANTs inline (42501 lesson).
--
-- Date : 2026-07-16
-- =============================================================================

BEGIN;

-- Idempotent enum creation (CREATE TYPE has no IF NOT EXISTS).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'post_draft_status') THEN
        CREATE TYPE post_draft_status AS ENUM ('pending', 'published', 'discarded');
    END IF;
END $$;

-- Fresh rebuild: lay the canonical shape (verbatim to the live OOB table).
-- No-op on the drifted live DB.
CREATE TABLE IF NOT EXISTS post_drafts (
    draft_id               UUID              PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id              UUID              NOT NULL,
    author_gcid            UUID              NOT NULL,
    composed_from_topic    TEXT              NOT NULL,
    composed_from_event_id UUID              NOT NULL,
    body                   TEXT              NOT NULL,
    metadata               JSONB             NOT NULL DEFAULT '{}'::jsonb,
    status                 post_draft_status NOT NULL DEFAULT 'pending',
    created_at             TIMESTAMPTZ       NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ       NOT NULL DEFAULT now(),
    deleted_at             TIMESTAMPTZ,
    -- Idempotency arbiter for the subscriber's ON CONFLICT DO NOTHING insert.
    UNIQUE (composed_from_topic, composed_from_event_id)
);

-- ListPending path: owner's pending drafts, soft-delete aware.
CREATE INDEX IF NOT EXISTS idx_post_drafts_owner_pending
    ON post_drafts (tenant_id, author_gcid)
    WHERE deleted_at IS NULL AND status = 'pending';

ALTER TABLE post_drafts ENABLE ROW LEVEL SECURITY;
ALTER TABLE post_drafts FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON post_drafts;
CREATE POLICY tenant_isolation ON post_drafts
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

GRANT SELECT, INSERT, UPDATE, DELETE ON post_drafts TO chora_sharing_app_rw;
GRANT SELECT ON post_drafts TO chora_sharing_app_ro;

COMMIT;
