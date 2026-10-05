-- =============================================================================
-- chora-sharing : 0031_social_blocks_and_projection_reconcile.up.sql
--
-- ADR-229 WS-0 (CHO-2102). Three duties:
--
--  (1) social_blocks — durable block edges. Blocks were an in-memory-only
--      list on the social Graph (lost on pod restart); they gate the
--      mutual-follow friend set behind GetReuseContext (ADR-229 D3), so they
--      must persist.
--
--  (2) atom_projections shape reconcile — 0006 (surrogate `id` PK) and 0030
--      (`atom_id` PK, authoritative) BOTH `CREATE TABLE atom_projections`;
--      a DB that ran 0006 cannot apply 0030 and vice versa. The projection is
--      a rebuildable, event-fed cache (0030.down.sql records this), so a
--      legacy-shape table is DROPPED and recreated in the 0030 shape; the
--      AtomProjectionSubscriber + creation-side backfill repopulate it.
--      Fresh installs: apply in order and SKIP whichever of 0006/0030 errors —
--      0031 converges the shape either way. Idempotent on re-run.
--
--  (3) reuse_visibility on atom_projections (ADR-229 D2) — the event-fed copy
--      of chora_creation.learning_atoms.reuse_visibility. Default 'private'
--      (consent-closed) until the WS-1 creation event carries the field.
--
-- RLS: ENABLE + FORCE, GUC chora.tenant_id, NULLIF-safe (0030 idiom).
-- NOTE: re-run 9999_grant_app_roles.sql after this migration so app_rw /
-- app_ro receive grants on social_blocks (GRANT ... ON ALL TABLES).
-- =============================================================================

-- (1) social_blocks -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS social_blocks (
    block_id     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id    UUID        NOT NULL,
    blocker_gcid UUID        NOT NULL,
    blocked_gcid UUID        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (blocker_gcid, blocked_gcid),
    CHECK (blocker_gcid <> blocked_gcid)   -- self-block rejected
);

CREATE INDEX IF NOT EXISTS social_blocks_blocker_idx ON social_blocks (blocker_gcid);
CREATE INDEX IF NOT EXISTS social_blocks_blocked_idx ON social_blocks (blocked_gcid);
CREATE INDEX IF NOT EXISTS social_blocks_tenant_idx  ON social_blocks (tenant_id);

ALTER TABLE social_blocks ENABLE ROW LEVEL SECURITY;
ALTER TABLE social_blocks FORCE  ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE tablename = 'social_blocks'
          AND policyname = 'social_blocks_tenant_isolation'
    ) THEN
        CREATE POLICY social_blocks_tenant_isolation ON social_blocks
            FOR ALL
            USING (tenant_id = NULLIF(current_setting('chora.tenant_id', true), '')::uuid);
    END IF;
END $$;

-- (2) atom_projections shape reconcile ----------------------------------------

DO $$
BEGIN
    -- The legacy 0006 shape is identified by its surrogate `id` column.
    -- Rebuildable cache → safe to drop (subscriber + backfill repopulate).
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name   = 'atom_projections'
          AND column_name  = 'id'
    ) THEN
        DROP TABLE atom_projections;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS atom_projections (
    atom_id             UUID         PRIMARY KEY,
    tenant_id           UUID         NOT NULL,
    revision_id         UUID         NOT NULL,
    owner_gcid          UUID         NOT NULL,
    author_display_name TEXT         NOT NULL DEFAULT '',
    stem                TEXT         NOT NULL DEFAULT '',
    question_type       TEXT         NOT NULL DEFAULT '',
    published_at        TIMESTAMPTZ,
    archived            BOOLEAN      NOT NULL DEFAULT false,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    deleted_at          TIMESTAMPTZ            -- closure crypto-shred ONLY (ADR-186)
);

CREATE INDEX IF NOT EXISTS idx_atom_projections_owner
    ON atom_projections (owner_gcid, tenant_id)
    WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_atom_projections_active
    ON atom_projections (tenant_id)
    WHERE archived = false AND deleted_at IS NULL;

ALTER TABLE atom_projections ENABLE ROW LEVEL SECURITY;
ALTER TABLE atom_projections FORCE  ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE tablename = 'atom_projections'
          AND policyname = 'tenant_isolation'
    ) THEN
        CREATE POLICY tenant_isolation ON atom_projections
            FOR ALL
            USING (tenant_id = NULLIF(current_setting('chora.tenant_id', true), '')::uuid);
    END IF;
END $$;

-- (3) reuse_visibility (ADR-229 D2) --------------------------------------------

ALTER TABLE atom_projections
    ADD COLUMN IF NOT EXISTS reuse_visibility TEXT NOT NULL DEFAULT 'private';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'atom_projections_reuse_visibility_check'
    ) THEN
        ALTER TABLE atom_projections
            ADD CONSTRAINT atom_projections_reuse_visibility_check
            CHECK (reuse_visibility IN ('private', 'friends', 'tenant'));
    END IF;
END $$;
