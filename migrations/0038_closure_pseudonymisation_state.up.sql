-- =============================================================================
-- chora-sharing : 0038_closure_pseudonymisation_state.up.sql
--
-- Domain        : Content Sharing (5 core)
-- Database      : chora_sharing
-- Story         : CHO-2198 (W0-F1 durability + W0-F5 error-honesty)
--
-- Durable ack/dedup state for the federated account-closure saga (Tier 3
-- D11 / ADR-184 / ADR-186), replacing the process-local
-- events.InMemoryClosureRepo (W0-F1 UNGATED-DEFECT — see
-- docs/references/w0-f1-inmemory-inventory.md §6 item 4). Today the repo is
-- gated on `pubsubClient != nil`, never on pool health, so this ack/dedup
-- state is lost on every pod restart, which can duplicate-process or
-- permanently stall the closure saga for this domain.
--
-- NOTE on the pre-existing `closure_log` table (0007_closure_idempotency_
-- outbox.up.sql): that table is unwired (no Go code references it) and,
-- per docs/TODO-DEVELOPMENT.md, one of four repo-defined tables ABSENT
-- from prod chora_sharing (defined in a migration file that was never
-- actually applied). This migration does not touch or reuse it — it adds
-- the SAME closure_pseudonymisation_state shape the chora-notifications
-- reference fix + this CHO-2198 pass use across every closure-saga
-- service, so the durable ack contract is uniform. Reconciling/retiring
-- closure_log is a separate decision, out of scope here.
--
-- What this table is NOT: it does not itself redact any PII column. The
-- closure subscriber (internal/adapter/events/closure_subscriber.go) still
-- decides WHAT to tokenise from config/PII_Closure_Map.yaml at read time;
-- neither the prior in-memory repo NOR this pg-backed ClosureRepository
-- executes a real per-table UPDATE against posts / reactions / comments /
-- social_profiles / pvp_duels / leaderboard_entries / refer_a_friend_log /
-- territory_conquest_log — both only durably record THAT a (tenant, gcid)
-- pair has been processed and HOW MANY columns the map declared
-- (rows_touched is a declared-intent count from the PII map, not an
-- actual per-row UPDATE-affected count). Real per-table redaction is a
-- separate, deeper gap confirmed to apply identically across ALL 9
-- closure-saga services (see CHO-2198 durability report). This migration
-- only fixes DURABILITY of the ack/dedup signal.
--
-- Row semantics: INSERT-once via ON CONFLICT (tenant_id, gcid) DO NOTHING,
-- never UPDATEd by application code — existence of a (tenant_id, gcid) row
-- IS the "pseudonymised" flag (mirrors the in-memory repo's
-- `pseudonymed[gcid] = true`, now tenant-scoped: the in-memory version
-- collapsed the key to gcid-only, a latent cross-tenant dedup collision
-- fixed alongside this migration — see closure_subscriber.go). No
-- deleted_at: this is append-only operational/ack state, not domain
-- content (same soft-delete exemption class as sharing_outbox_events /
-- idempotency_keys per ddd-enforcement.md §Soft Deletes Exceptions), and
-- MUST NOT be reversed by anything short of the saga's own compensation
-- path — which, per the account-closure-saga skill, is refused once
-- pseudonymisation has been ack'd (point of no return).
--
-- RLS: ENABLE + FORCE with a STRICT tenant_isolation policy (matches this
-- service's own most recent RLS precedent,
-- 0037_reactions_duelrounds_tenant_isolation.up.sql). Every repo method in
-- internal/adapter/pg unconditionally calls rls.ApplySession BEFORE the
-- user query (see post.go / social_repo.go / closure_repository.go's
-- `run` helper), so a strict policy is safe and correct here — unlike
-- chora-a2a-gateway, this service's PgxPoolQuerier equivalent (the
-- cmd/server TxRunner bridge) always opens a SET LOCAL chora.tenant_id
-- transaction first.
--
-- Date : 2026-07-15
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS closure_pseudonymisation_state (
    id               UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        UUID         NOT NULL,
    gcid             UUID         NOT NULL,
    rows_touched     INTEGER      NOT NULL DEFAULT 0,
    pseudonymised_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- One durable ack per (tenant, gcid) — the natural idempotency key the
    -- ClosureRepository.Pseudonymise port operates on.
    UNIQUE (tenant_id, gcid)
);

-- Cross-tenant admin/debug lookup path ("has this GCID been closed in ANY
-- tenant this domain has rows for?"). The (tenant_id, gcid) UNIQUE
-- constraint above already covers the tenant-scoped lookup RLS/app queries
-- use; this is the gcid-only shape for O+ closure-pipeline visibility
-- (account-closure-saga skill "Admin O+ closure-pipeline visibility").
CREATE INDEX IF NOT EXISTS idx_closure_pseudonymisation_state_gcid
    ON closure_pseudonymisation_state (gcid);

ALTER TABLE closure_pseudonymisation_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE closure_pseudonymisation_state FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON closure_pseudonymisation_state;
CREATE POLICY tenant_isolation ON closure_pseudonymisation_state
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- Self-contained grants (9999_grant_app_roles.sql already ran against the
-- live DB; new tables created afterwards need explicit grants — same
-- defence-in-depth convention as the chora-notifications reference (0013);
-- ALTER DEFAULT PRIVILEGES alone has previously caused 42501 on
-- targeted/backdated migrations, see
-- reusable_gotcha_targeted_migration_skips_9999_grants_and_tracker_lineage_drift).
GRANT SELECT, INSERT, UPDATE, DELETE ON closure_pseudonymisation_state TO chora_sharing_app_rw;
GRANT SELECT ON closure_pseudonymisation_state TO chora_sharing_app_ro;

COMMIT;
