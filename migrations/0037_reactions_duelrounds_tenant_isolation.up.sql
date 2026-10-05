-- =============================================================================
-- chora-sharing : 0037_reactions_duelrounds_tenant_isolation.up.sql
--
-- Give `social_feed_reactions` and `duel_rounds` the tenant isolation their own
-- migrations already claim they have.
--
-- Domain   : Content Sharing (5 core)
-- Database : chora_sharing
--
-- THE GAP
-- -------
-- 0002_social.up.sql and 0003_duels.up.sql both define these tables with
-- `tenant_id UUID NOT NULL`, ENABLE + FORCE ROW LEVEL SECURITY, and a
-- `*_tenant_isolation` policy. **Production has none of the three.** Verified
-- against the live DB (not the files) on 2026-07-15:
--
--   social_feed_reactions : no tenant_id · relrowsecurity=f · 0 policies
--   duel_rounds           : no tenant_id · relrowsecurity=f · 0 policies
--
-- The repo asserts a tenant-isolation control that the database does not have.
-- That is the CHO-2140 "legacy-GUC dark class" trap again, and it is why the
-- standing rule is: audit live `pg_policies`, never count files.
--
-- HOW IT HAPPENED: chora-sharing's migrations were renumbered/squashed. The
-- tracker still holds the OLD set (0001_initial.sql, 0002_outbox.sql,
-- 0003_community_*…) while origin/main holds a rewritten set that was authored
-- but NEVER APPLIED. Production runs the old shape; the repo describes the new
-- one (54 live tables vs 24 the migrations define).
--
-- NOT AN ACTIVE BREACH — and saying so plainly matters. Both tables are EMPTY
-- (0 rows), `social_feed_reactions` is referenced by no Go code at all (the live
-- code still reads the OLD `reactions` table), and `duel_rounds` is referenced
-- but unused. So nothing is leaking today. This is a LOADED GUN, not a wound:
-- the first row written to either table lands with no tenant filter on it.
--
-- IT ALSO UNWEDGES THE LANE. The migration runner currently dies on chora-sharing
-- at 0002_social with `42703 column "tenant_id" does not exist` — it is trying to
-- CREATE POLICY on a column production lacks. A real error ⇒ the runner `break`s
-- and every migration lex-after it never applies, which is why atom_bookmarks was
-- never created and C+ bookmarks have 500'd for fifteen days (CHO-2177). Closing
-- this gap makes 0002/0003 replay clean, and the rest of the lane can follow.
--
-- SCOPE: tenant isolation ONLY. The live and canonical shapes diverge in other
-- ways too (canonical adds an FK + UNIQUE to social_feed_reactions; live
-- duel_rounds carries extra scoring columns canonical does not). Reconciling
-- those is a separate, deliberate piece of work — this migration does not guess.
--
-- SAFETY: both tables are empty, so `ADD COLUMN ... NOT NULL` needs no backfill
-- and no DEFAULT. That is asserted below and the migration ABORTS if it is ever
-- untrue — a NOT NULL tenant_id over real rows requires a real backfill from the
-- parent (social_feed_entries.tenant_id via entry_id; duel_sessions.tenant_id via
-- duel_session_id), and this file will not silently invent one.
--
-- Policy names are deliberately IDENTICAL to the ones in 0002/0003 so that a
-- later replay of those files hits `already exists` (absorbed) rather than a
-- conflicting second policy.
--
-- Replay-safe: every step is guarded. (Both runners re-apply every *.up.sql on
-- every pass; a migration that errors on a second apply wedges its whole lane.)
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- Refuse to run over data. A NOT NULL tenant column on populated rows is a
-- backfill, not an ALTER, and guessing the tenant is exactly the kind of silent
-- coercion that must never happen.
-- -----------------------------------------------------------------------------
DO $guard$
DECLARE
    n_reactions BIGINT;
    n_rounds    BIGINT;
BEGIN
    SELECT count(*) INTO n_reactions FROM social_feed_reactions;
    SELECT count(*) INTO n_rounds    FROM duel_rounds;

    IF n_reactions > 0 AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'social_feed_reactions' AND column_name = 'tenant_id'
    ) THEN
        RAISE EXCEPTION
            'social_feed_reactions holds % row(s) but has no tenant_id. A NOT NULL tenant column here needs a real backfill from social_feed_entries.tenant_id via entry_id — refusing to invent one.', n_reactions;
    END IF;

    IF n_rounds > 0 AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'duel_rounds' AND column_name = 'tenant_id'
    ) THEN
        RAISE EXCEPTION
            'duel_rounds holds % row(s) but has no tenant_id. A NOT NULL tenant column here needs a real backfill from duel_sessions.tenant_id via duel_session_id — refusing to invent one.', n_rounds;
    END IF;

    RAISE NOTICE 'ADR-229/230 tenant isolation repair: social_feed_reactions=% row(s), duel_rounds=% row(s) — safe to add NOT NULL tenant_id.', n_reactions, n_rounds;
END
$guard$;

-- -----------------------------------------------------------------------------
-- social_feed_reactions
-- -----------------------------------------------------------------------------
ALTER TABLE social_feed_reactions
    ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL;

CREATE INDEX IF NOT EXISTS social_feed_reactions_tenant_idx
    ON social_feed_reactions (tenant_id);

ALTER TABLE social_feed_reactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE social_feed_reactions FORCE  ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE schemaname = current_schema()
          AND tablename  = 'social_feed_reactions'
          AND policyname = 'social_feed_reactions_tenant_isolation'
    ) THEN
        CREATE POLICY social_feed_reactions_tenant_isolation ON social_feed_reactions
            FOR ALL
            USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);
    END IF;
END
$$;

-- -----------------------------------------------------------------------------
-- duel_rounds
-- -----------------------------------------------------------------------------
ALTER TABLE duel_rounds
    ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL;

CREATE INDEX IF NOT EXISTS duel_rounds_tenant_idx
    ON duel_rounds (tenant_id);

ALTER TABLE duel_rounds ENABLE ROW LEVEL SECURITY;
ALTER TABLE duel_rounds FORCE  ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE schemaname = current_schema()
          AND tablename  = 'duel_rounds'
          AND policyname = 'duel_rounds_tenant_isolation'
    ) THEN
        CREATE POLICY duel_rounds_tenant_isolation ON duel_rounds
            FOR ALL
            USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);
    END IF;
END
$$;

COMMENT ON COLUMN social_feed_reactions.tenant_id IS
    'Tenant scope. Added 2026-07-15 — the table shipped to production with NO tenant column and NO RLS while 0002_social.up.sql claimed both. Empty at repair time, so no backfill was needed.';
COMMENT ON COLUMN duel_rounds.tenant_id IS
    'Tenant scope. Added 2026-07-15 — the table shipped to production with NO tenant column and NO RLS while 0003_duels.up.sql claimed both. Empty at repair time, so no backfill was needed.';

COMMIT;
