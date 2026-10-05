-- =============================================================================
-- chora-sharing : 0040_user_preferences.up.sql
--
-- Domain   : Content Sharing (5 core)
-- Database : chora_sharing
-- Story    : CHO-2203 (durable Familiar-milestone lane; parent CHO-1889)
--
-- The durable backing table for the FamiliarMilestoneSubscriber's Preference
-- port. Resolves the per-user share policy for Familiar milestones:
--
--   auto      → compose Post directly + outbox chora.sharing.post.created.v1
--   draft     → insert a post_drafts row (DEFAULT — pending user review)
--   suppress  → no-op (still logged for IMDA D1 audit)
--
-- ⚠ RECONCILING AN OUT-OF-BAND TABLE (report this loudly): like
-- subscriber_idempotency (0041), `user_preferences` ALREADY EXISTED in prod
-- chora_sharing, created OUT-OF-BAND — NO migration file in the repo creates it
-- (grep is empty), so it has no tracker lineage. Its live shape is
-- (tenant_id UUID, gcid UUID, familiar_milestone_share
-- familiar_milestone_share_pref NOT NULL DEFAULT 'draft', updated_at TIMESTAMPTZ)
-- PK(tenant_id, gcid), RLS ENABLED + a tenant_isolation policy + a
-- set_updated_at trigger, 0 rows. It is WELL-FORMED and matches the intended
-- design (the enum values are exactly the subscribers.Policy set), so this
-- migration adopts that shape verbatim rather than inventing a second one — the
-- pg adapter reads/writes the existing `familiar_milestone_share` column
-- (enum familiar_milestone_share_pref). This file is idempotent for BOTH the
-- drifted live DB (everything already exists → no-ops) AND a fresh rebuild
-- (CREATE TYPE via DO-guard + CREATE TABLE IF NOT EXISTS lay the canonical
-- shape). NOTE: the original 0040 (a familiar_milestone_share_pref TEXT column)
-- was recorded applied but its CREATE TABLE was SKIPPED on the pre-existing OOB
-- table — so nothing wrong ever landed; this content correction only matters for
-- a fresh rebuild's fidelity, and re-running it on live is a safe no-op.
--
-- NATURAL TOGGLE SEMANTICS → NO soft-delete: a preference is a setting, upserted
-- in place (ON CONFLICT (tenant_id, gcid) DO UPDATE). Same soft-delete exemption
-- class as an operational/config row per ddd-enforcement.md.
--
-- RLS: ENABLE + FORCE + strict tenant_isolation (0037/0038 precedent). GRANTs
-- inline (42501 lesson).
--
-- Date : 2026-07-16
-- =============================================================================

BEGIN;

-- Idempotent enum creation (CREATE TYPE has no IF NOT EXISTS). Values match
-- subscribers.ParsePolicy exactly.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'familiar_milestone_share_pref') THEN
        CREATE TYPE familiar_milestone_share_pref AS ENUM ('auto', 'draft', 'suppress');
    END IF;
END $$;

-- Fresh rebuild: lay the canonical shape (verbatim to the live OOB table).
-- No-op on the drifted live DB.
CREATE TABLE IF NOT EXISTS user_preferences (
    tenant_id                UUID                          NOT NULL,
    gcid                     UUID                          NOT NULL,
    familiar_milestone_share familiar_milestone_share_pref NOT NULL DEFAULT 'draft',
    updated_at               TIMESTAMPTZ                   NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, gcid)
);

CREATE INDEX IF NOT EXISTS idx_user_preferences_tenant ON user_preferences (tenant_id);

ALTER TABLE user_preferences ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_preferences FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON user_preferences;
CREATE POLICY tenant_isolation ON user_preferences
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

GRANT SELECT, INSERT, UPDATE, DELETE ON user_preferences TO chora_sharing_app_rw;
GRANT SELECT ON user_preferences TO chora_sharing_app_ro;

COMMIT;
