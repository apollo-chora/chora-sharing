-- ============================================================================
-- chora-sharing : 0006_projections.up.sql
--
-- Spec ref : docs/chora-sharing.md §4.2 (atom_projections) + §3.2 R1
--
-- Cached LearningAtom read-model (R1 attribution source). NOT an aggregate —
-- event-fed by creation.atom.published.v1 / creation.atom.archived.v1.
-- owner_gcid + author_display_name denormalised so the feed never does a
-- cross-DB read of chora_identity.
-- ============================================================================

CREATE TABLE atom_projections (
    id                   UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id            UUID           NOT NULL,
    atom_id              UUID           NOT NULL,   -- cross-DB ref, no FK
    revision_id          UUID           NOT NULL,   -- pinned at share/grant time
    owner_gcid           UUID           NOT NULL,   -- the AUTHOR of record (R1)
    author_display_name  TEXT           NOT NULL DEFAULT '',
    stem                 TEXT           NOT NULL DEFAULT '',
    question_type        TEXT           NOT NULL DEFAULT '',
    published_at         TIMESTAMPTZ    NOT NULL,
    archived             BOOLEAN        NOT NULL DEFAULT FALSE,
    created_at           TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX atom_projections_atom_unique_idx
    ON atom_projections (atom_id);   -- upsert key (published.v1 subscriber upserts)
CREATE INDEX atom_projections_owner_tenant_idx
    ON atom_projections (owner_gcid, tenant_id) WHERE archived = FALSE;

CREATE TRIGGER atom_projections_set_updated_at
    BEFORE UPDATE ON atom_projections
    FOR EACH ROW EXECUTE FUNCTION circle_set_updated_at();

ALTER TABLE atom_projections ENABLE ROW LEVEL SECURITY;
ALTER TABLE atom_projections FORCE  ROW LEVEL SECURITY;
CREATE POLICY atom_projections_tenant_isolation ON atom_projections
    FOR ALL
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);
