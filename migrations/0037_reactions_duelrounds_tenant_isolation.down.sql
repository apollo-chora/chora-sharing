-- =============================================================================
-- chora-sharing : 0037_reactions_duelrounds_tenant_isolation.down.sql
--
-- Reverse the tenant-isolation repair on social_feed_reactions + duel_rounds.
--
-- ⚠ THINK BEFORE YOU RUN THIS. Rolling back does not restore a neutral state —
-- it RE-OPENS the gap: it drops the tenant column and the RLS policy from two
-- tables whose own forward migrations (0002_social / 0003_duels) declare both.
-- After this runs, anything written to either table lands with no tenant filter,
-- and the migration runner goes back to wedging chora-sharing at 0002_social
-- with `42703 column "tenant_id" does not exist`.
--
-- It exists for completeness and for a same-session mistake, not as a routine
-- lever.
--
-- Dropping tenant_id DISCARDS the tenant scope of any row written since the
-- repair. That is data loss, not a no-op, so this refuses to run if either table
-- has rows.
-- =============================================================================

BEGIN;

DO $guard$
DECLARE
    n_reactions BIGINT;
    n_rounds    BIGINT;
BEGIN
    SELECT count(*) INTO n_reactions FROM social_feed_reactions;
    SELECT count(*) INTO n_rounds    FROM duel_rounds;

    IF n_reactions > 0 OR n_rounds > 0 THEN
        RAISE EXCEPTION
            'refusing to roll back: social_feed_reactions has % row(s) and duel_rounds has % row(s). Dropping tenant_id would silently discard their tenant scope and leave the rows unisolated. Reconcile deliberately instead.',
            n_reactions, n_rounds;
    END IF;
END
$guard$;

DROP POLICY IF EXISTS social_feed_reactions_tenant_isolation ON social_feed_reactions;
ALTER TABLE social_feed_reactions NO FORCE ROW LEVEL SECURITY;
ALTER TABLE social_feed_reactions DISABLE ROW LEVEL SECURITY;
DROP INDEX IF EXISTS social_feed_reactions_tenant_idx;
ALTER TABLE social_feed_reactions DROP COLUMN IF EXISTS tenant_id;

DROP POLICY IF EXISTS duel_rounds_tenant_isolation ON duel_rounds;
ALTER TABLE duel_rounds NO FORCE ROW LEVEL SECURITY;
ALTER TABLE duel_rounds DISABLE ROW LEVEL SECURITY;
DROP INDEX IF EXISTS duel_rounds_tenant_idx;
ALTER TABLE duel_rounds DROP COLUMN IF EXISTS tenant_id;

COMMIT;
