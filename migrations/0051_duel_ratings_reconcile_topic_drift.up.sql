-- 0051: Reconcile duel_ratings schema drift (CHO-336).
--
-- The live DB diverged out-of-band: a topic_id column + a 3-column unique
-- index (uq_duel_ratings_tenant_gcid_topic) were added by an unknown source
-- (no migration in this repo creates them; no Go code references topic_id
-- for duels). The 3-column index shadowed the original 2-column index
-- (duel_ratings_active_unique_idx from 0004), so the code's
-- ON CONFLICT (tenant_id, gcid) WHERE deleted_at IS NULL could no longer
-- find a matching arbiter → ELO upserts silently failed on every completed
-- ranked duel.
--
-- This migration restores the schema the application code expects:
--   1. Drop the drifted 3-column unique index.
--   2. Recreate the original 2-column unique index.
--   3. Drop the unused topic_id column + its leaderboard index.
--   4. Recreate the original tenant_rating index (without topic_id).

-- 1. Drop the drifted indexes.
DROP INDEX IF EXISTS uq_duel_ratings_tenant_gcid_topic;
DROP INDEX IF EXISTS idx_duel_ratings_leaderboard;

-- 2. Drop the topic_id column (no code references it; nullable with no data).
ALTER TABLE duel_ratings DROP COLUMN IF EXISTS topic_id;

-- 3. Recreate the original 2-column unique index (from 0004).
CREATE UNIQUE INDEX IF NOT EXISTS duel_ratings_active_unique_idx
    ON duel_ratings (tenant_id, gcid)
    WHERE deleted_at IS NULL;

-- 4. Recreate the original tenant_rating index (from 0004).
CREATE INDEX IF NOT EXISTS duel_ratings_tenant_rating_idx
    ON duel_ratings (tenant_id, rating DESC)
    WHERE deleted_at IS NULL;
