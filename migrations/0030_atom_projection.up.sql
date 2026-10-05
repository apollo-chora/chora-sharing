-- =============================================================================
-- chora-sharing : 0030_atom_projection.up.sql
--
-- Atom Sharing Redesign (specs/001-atom-sharing-redesign) — wires the
-- previously-dead AtomProjectionSubscriber (T011) to a durable read-model so
-- ShareAtom can enforce R1 (author-of-record) WITHOUT a cross-DB read of
-- chora_creation (cross-DB queries forbidden — events only). CHO-1972.
--
-- NEW table (additive; touches no existing table):
--   atom_projections — cached, event-fed read-model of a published LearningAtom.
--     Fed ONLY by chora.creation.atom.published.v1 (upsert) +
--     chora.creation.atom.archived.v1 (archive flag). chora-sharing NEVER edits
--     it. It is the R1 attribution source: ShareAtom validates
--     owner_gcid == caller author_gcid before allowing a share.
--
-- Domain  : Content Sharing (5 core)   Database: chora_sharing
--
-- HARD INVARIANTS (CLAUDE.md / ddd-enforcement.md):
--   * Cross-domain refs (atom_id / revision_id -> chora_creation;
--     owner_gcid -> chora_identity) are opaque UUID with NO FK (cross-DB
--     queries forbidden; validated via Pub/Sub events).
--   * Soft-delete only — `archived` is the withdrawn flag (NEVER hard-delete);
--     `deleted_at` is reserved for the federated closure-saga crypto-shred
--     (ADR-186), never a plain delete.
--   * ENABLE + FORCE ROW LEVEL SECURITY (a non-superuser table owner cannot
--     silently drop RLS).
--   * RLS GUC = `chora.tenant_id` — matches the rls.ApplySession Go helper +
--     the CORE 0001 tables (posts / reactions / social_follows). This DIFFERS
--     deliberately from the 0028 circle/community tables (app.current_tenant_id)
--     because those are NOT wired through a Go pg adapter; this table IS (the
--     pg AtomProjectionStore reads/writes via rls.ApplySession, which emits
--     SET LOCAL chora.tenant_id). NULLIF-safe: an unset/'' GUC yields NULL
--     (row hidden) rather than raising 22P02 on ''::uuid.
--   * app_rw / app_ro grants inherit via 9999_grant_app_roles.sql
--     (GRANT ... ON ALL TABLES, re-run last) — no manual GRANT here.
--   * NO new RLS-bypass surface (exactly 2 ADR-scoped exist: ADR-165/184).
-- =============================================================================

-- Drop the 0006-era atom_projections table if it still exists (surrogate PK
-- design). The 0030 redesign uses atom_id as the natural PK + adds deleted_at
-- + NULLIF-safe RLS. This makes the migration idempotent on databases where
-- 0006 already ran.
DROP TABLE IF EXISTS atom_projections;

CREATE TABLE atom_projections (
    -- atom_id is the globally-unique UUIDv7 of the LearningAtom in
    -- chora_creation (opaque cross-domain ref, NO FK). Natural PK: one cached
    -- projection per atom — ON CONFLICT (atom_id) makes the upsert idempotent.
    atom_id             UUID         PRIMARY KEY,
    tenant_id           UUID         NOT NULL,  -- RLS; cross-context ref, no FK
    revision_id         UUID         NOT NULL,  -- chora_creation AtomRevision (pinned), no FK
    owner_gcid          UUID         NOT NULL,  -- the AUTHOR of record (R1) -> chora_identity, no FK
    author_display_name TEXT         NOT NULL DEFAULT '',
    stem                TEXT         NOT NULL DEFAULT '',  -- feed-card preview source
    -- question_type stores the chora_creation AtomType LABEL (e.g. 'mcq' / 'oe')
    -- as TEXT (not a PG enum) so new atom types need no migration; the
    -- subscriber maps the proto enum to this string.
    question_type       TEXT         NOT NULL DEFAULT '',
    published_at        TIMESTAMPTZ,            -- nullable; informational
    -- archived = true marks the atom withdrawn in chora_creation
    -- (atom.archived.v1). The read-side (ShareAtom R1 Get) excludes archived
    -- rows; the row stays for audit (soft-delete invariant).
    archived            BOOLEAN      NOT NULL DEFAULT false,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    deleted_at          TIMESTAMPTZ            -- closure crypto-shred ONLY (ADR-186)
);

-- Author's projections (R1 lookups + "atoms I authored" fan-out).
CREATE INDEX idx_atom_projections_owner
    ON atom_projections (owner_gcid, tenant_id)
    WHERE deleted_at IS NULL;

-- Active (shareable) projections per tenant — the feed/read-side hot path.
CREATE INDEX idx_atom_projections_active
    ON atom_projections (tenant_id)
    WHERE archived = false AND deleted_at IS NULL;

ALTER TABLE atom_projections ENABLE ROW LEVEL SECURITY;
ALTER TABLE atom_projections FORCE ROW LEVEL SECURITY;

-- Tenant isolation. NULLIF-safe: when chora.tenant_id is unset/'' the predicate
-- is NULL (row hidden) rather than raising 22P02 on ''::uuid.
CREATE POLICY tenant_isolation ON atom_projections
    FOR ALL
    USING (tenant_id = NULLIF(current_setting('chora.tenant_id', true), '')::uuid);
