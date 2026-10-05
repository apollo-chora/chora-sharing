-- ============================================================================
-- chora-sharing : 0008_atom_bookmarks.up.sql
--
-- Spec ref : bookmark aggregate — personal atom collection (one bookmark
--           per (gcid, atom_id), idempotent).
--
-- RLS discipline (§4.1):
--   - tenant_id UUID NOT NULL on every tenant-scoped table
--   - ENABLE + FORCE ROW LEVEL SECURITY
--   - policy FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid)
--   - GUC is chora.tenant_id (NOT app.current_tenant_id — legacy, not set here)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- atom_bookmarks — a user's saved atoms (personal collection)
--   atom_revision_id is nullable — a user may bookmark without pinning a revision.
--   UNIQUE(gcid, atom_id) — idempotent bookmark (one per user+atom).
-- ----------------------------------------------------------------------------
CREATE TABLE atom_bookmarks (
    id               UUID         PRIMARY KEY,
    tenant_id        UUID         NOT NULL,
    gcid             UUID         NOT NULL,
    atom_id          UUID         NOT NULL,
    atom_revision_id UUID,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (gcid, atom_id)
);

-- Keyset pagination: newest-first by UUIDv7 id (lexicographic desc = creation-order desc).
CREATE INDEX atom_bookmarks_gcid_created_idx
    ON atom_bookmarks (gcid, created_at DESC);

ALTER TABLE atom_bookmarks ENABLE ROW LEVEL SECURITY;
ALTER TABLE atom_bookmarks FORCE  ROW LEVEL SECURITY;
CREATE POLICY atom_bookmarks_tenant_isolation ON atom_bookmarks
    FOR ALL
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);
