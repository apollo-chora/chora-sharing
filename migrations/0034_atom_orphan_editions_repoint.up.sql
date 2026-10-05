-- ============================================================================
-- chora-sharing : 0034_atom_orphan_editions_repoint.up.sql
--
-- ADR-229 Amendment A1 (CHO-2132) — the sharing half of the orphan-edition
-- saga. Grants are NEVER revoked by a reuse-visibility withdrawal: stranded
-- AtomUsageGrants REPOINT onto the singleton orphan edition chora-creation
-- mints (chora.creation.atom.orphan_created.v1).
--
--  (1) atom_orphan_editions — the event-fed (atom, last-published-revision)
--      → orphan mapping. Lets a REPEAT withdrawal at the same revision
--      repoint locally without round-tripping creation (whose idempotent
--      mint deliberately emits nothing on a singleton conflict). Cross-domain
--      refs are opaque UUIDs — NO FK (ddd-enforcement #3).
--  (2) grant_events.event_type CHECK gains 'repointed' — the append-only
--      trail entry every repoint (and dedupe-merge) writes.
-- ============================================================================
BEGIN;

-- ----------------------------------------------------------------------------
-- (1) atom_orphan_editions
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS atom_orphan_editions (
    id                  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID         NOT NULL,
    atom_id             UUID         NOT NULL,   -- the withdrawn original (chora_creation ref, no FK)
    source_revision_id  UUID         NOT NULL,   -- the pinned last-published revision
    orphan_atom_id      UUID         NOT NULL,   -- the frozen orphan edition
    orphaned_at         TIMESTAMPTZ  NOT NULL,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now()
);

COMMENT ON TABLE atom_orphan_editions IS
'ADR-229 A1 (CHO-2132) — event-fed singleton map (atom, last-published-revision) -> orphan edition. Fed by chora.creation.atom.orphan_created.v1; consulted by the stranding detector for local repoints on repeat withdrawals.';

-- Singleton mirror of creation''s partial unique — one orphan per
-- (atom, source revision); redeliveries upsert idempotently against it.
CREATE UNIQUE INDEX IF NOT EXISTS atom_orphan_editions_singleton_idx
    ON atom_orphan_editions (atom_id, source_revision_id);

-- Detector lookup: latest edition for an atom.
CREATE INDEX IF NOT EXISTS atom_orphan_editions_atom_orphaned_idx
    ON atom_orphan_editions (atom_id, orphaned_at DESC);

ALTER TABLE atom_orphan_editions ENABLE ROW LEVEL SECURITY;
ALTER TABLE atom_orphan_editions FORCE  ROW LEVEL SECURITY;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE tablename = 'atom_orphan_editions'
          AND policyname = 'atom_orphan_editions_tenant_isolation'
    ) THEN
        CREATE POLICY atom_orphan_editions_tenant_isolation ON atom_orphan_editions
            FOR ALL
            USING (tenant_id = current_setting('chora.tenant_id', true)::uuid)
            WITH CHECK (tenant_id = current_setting('chora.tenant_id', true)::uuid);
    END IF;
END $$;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE atom_orphan_editions TO chora_sharing_app_rw;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_sharing_app_ro') THEN
        GRANT SELECT ON TABLE atom_orphan_editions TO chora_sharing_app_ro;
    END IF;
END $$;

-- ----------------------------------------------------------------------------
-- (2) grant_events.event_type += 'repointed'
--     The 0004 CHECK was declared inline (auto-named, conventionally
--     grant_events_event_type_check) — discover + drop whatever CHECK
--     mentions event_type so a name drift can't leave the old constraint
--     silently rejecting 'repointed' rows.
-- ----------------------------------------------------------------------------
DO $$
DECLARE
    conname_found TEXT;
BEGIN
    FOR conname_found IN
        SELECT conname FROM pg_constraint
        WHERE conrelid = 'grant_events'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%event_type%'
    LOOP
        EXECUTE format('ALTER TABLE grant_events DROP CONSTRAINT %I', conname_found);
    END LOOP;
END $$;

ALTER TABLE grant_events
    ADD CONSTRAINT grant_events_event_type_check
    CHECK (event_type IN ('granted', 'revoked', 'expired', 'repointed'));

COMMIT;
