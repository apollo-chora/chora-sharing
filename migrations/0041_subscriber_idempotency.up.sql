-- =============================================================================
-- chora-sharing : 0041_subscriber_idempotency.up.sql
--
-- Domain   : Content Sharing (5 core)
-- Database : chora_sharing
-- Story    : CHO-2203 (durable Familiar-milestone lane; parent CHO-1889)
--
-- The durable backing table for the FamiliarMilestoneSubscriber's Idempotency
-- port — the at-least-once dedup guard for inbound Pub/Sub milestone events.
-- Claim is an ON CONFLICT (subscriber_handler, source_event_id) DO NOTHING
-- RETURNING upsert: a returned row means THIS delivery is fresh (proceed with the
-- side effect); no row means a redelivery (skip, emit duplicate_event_skipped).
-- The uniqueness is enforced by Postgres, not a Go check-then-set two concurrent
-- pods could both pass. Before CHO-2203 this port was an in-memory map, so
-- at-least-once redelivery across a pod restart re-ran every milestone side
-- effect (double drafts / double auto-posts).
--
-- ⚠ RECONCILING AN OUT-OF-BAND TABLE (report this loudly): `subscriber_idempotency`
-- ALREADY EXISTED in prod chora_sharing, created OUT-OF-BAND — NO migration file
-- anywhere in the repo creates it (grep is empty), so it has no tracker lineage.
-- Its live shape was (subscriber_handler TEXT, source_event_id UUID, processed_at
-- TIMESTAMPTZ) PK(subscriber_handler, source_event_id), RLS DISABLED, 0 rows
-- (verified as the BYPASSRLS migrate role). The FamiliarMilestoneSubscriber +
-- LiveQuizScoreSubscriber referenced it in comments but nothing ever wrote to it
-- (both used the in-memory double). This migration RECONCILES that drift IN PLACE
-- rather than DROP+CREATE: the assert-not-destructive scanner refuses DROP TABLE
-- (CHO-2193), and rightly — a committed DROP TABLE is a lane landmine. Every
-- statement below is NON-DESTRUCTIVE and idempotent for BOTH the drifted live DB
-- (table exists, legacy shape) AND a fresh rebuild (table absent): CREATE IF NOT
-- EXISTS lays the canonical shape on a fresh DB; the ADD COLUMN / SET NOT NULL /
-- ENABLE RLS statements are no-ops there and the reconcile path on the drifted DB
-- (safe because it is empty). The legacy composite PK (subscriber_handler,
-- source_event_id) IS the dedup key and is kept — event_id is a globally-unique
-- UUIDv7, so one event belongs to exactly one tenant and (handler, event) is
-- already a sound global dedup key; tenant_id is added for RLS isolation.
--
-- OPERATIONAL / APPEND-ONLY → NO soft-delete: existence of a row IS the "already
-- processed" flag (same soft-delete exemption class as sharing_outbox_events /
-- closure_pseudonymisation_state per ddd-enforcement.md Soft-Delete Exceptions).
--
-- TENANT-SCOPED + RLS: the pg adapter (MilestoneIdempotencyStore) reads the
-- tenant from ctx (tracing.TenantIDFromContext) — the shared
-- subscribers.IdempotencyStore.Claim port has no tenant parameter and is used by
-- 15 call sites across 8 subscribers, so widening it was out of scope. The
-- milestone Cloud PULL handler stamps tracing.WithTenantID(ctx, env.TenantID)
-- before dispatch, and rls.ApplySession fails LOUD (ErrNoTenantContext → NACK +
-- retry) if the tenant is ever absent — never a silent cross-tenant write.
--
-- Self-contained GRANTs (42501 lesson — 9999's ALTER DEFAULT PRIVILEGES does not
-- retroactively cover a later targeted migration's table).
--
-- Date : 2026-07-16
-- =============================================================================

BEGIN;

-- Fresh rebuild: lay the canonical, tenant-scoped shape. (No-op on the drifted
-- live DB, where the table already exists.)
CREATE TABLE IF NOT EXISTS subscriber_idempotency (
    tenant_id          UUID        NOT NULL,
    subscriber_handler TEXT        NOT NULL,
    source_event_id    UUID        NOT NULL,
    processed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (subscriber_handler, source_event_id)
);

-- Reconcile the out-of-band legacy shape (empty; RLS-off; no tenant_id) → add the
-- RLS scoping column. No-op on a fresh table (column already present); on the
-- drifted table it is data-safe because the table is empty.
ALTER TABLE subscriber_idempotency ADD COLUMN IF NOT EXISTS tenant_id UUID;
ALTER TABLE subscriber_idempotency ALTER COLUMN tenant_id SET NOT NULL;

-- Keep the debug index the OOB table shipped with (idempotent).
CREATE INDEX IF NOT EXISTS idx_subscriber_idempotency_processed_at
    ON subscriber_idempotency (processed_at);

ALTER TABLE subscriber_idempotency ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscriber_idempotency FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON subscriber_idempotency;
CREATE POLICY tenant_isolation ON subscriber_idempotency
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

GRANT SELECT, INSERT, UPDATE, DELETE ON subscriber_idempotency TO chora_sharing_app_rw;
GRANT SELECT ON subscriber_idempotency TO chora_sharing_app_ro;

COMMIT;
