-- ============================================================================
-- chora-sharing : 0055_per_category_ratings.up.sql
--
-- WS1: per-category leaderboards + matchmaking. Adds category to:
--   1. duel_ratings — composite key (tenant_id × gcid × category), reversing
--      0051's deliberate topic_id drop with a structured category column.
--      Existing rows backfilled to "overall" (the default bucket).
--   2. duel_sessions — so the duel carries its category for ELO writes.
--   3. matchmaking_queue — so the matchmaker pairs same-category entries.
--
-- The 6-category taxonomy mirrors the profiler domain
-- (programming / mathematics / science / humanities / arts / languages).
-- "overall" is the default (un-categorized) bucket for backward compat.
-- ============================================================================

-- 1. duel_ratings: add category + backfill existing rows to "overall".
ALTER TABLE duel_ratings
    ADD COLUMN IF NOT EXISTS category TEXT NOT NULL DEFAULT 'overall';

-- Drop the old 2-column unique index (tenant_id, gcid) — it conflicts with
-- the new 3-column composite. Recreate as 3-column.
DROP INDEX IF EXISTS duel_ratings_active_unique_idx;
CREATE UNIQUE INDEX duel_ratings_active_unique_idx
    ON duel_ratings (tenant_id, gcid, category)
    WHERE deleted_at IS NULL;

-- Index for per-category leaderboard queries (top-N by rating per category).
DROP INDEX IF EXISTS duel_ratings_tenant_rating_idx;
CREATE INDEX duel_ratings_tenant_rating_idx
    ON duel_ratings (tenant_id, category, rating DESC)
    WHERE deleted_at IS NULL;

-- 2. duel_sessions: add category so the duel carries it for ELO writes.
ALTER TABLE duel_sessions
    ADD COLUMN IF NOT EXISTS category TEXT NOT NULL DEFAULT 'overall';

-- 3. matchmaking_queue: add category so the matchmaker pairs same-category.
ALTER TABLE matchmaking_queue
    ADD COLUMN IF NOT EXISTS category TEXT NOT NULL DEFAULT 'overall';

-- Index for same-category + same-question-count pairing.
DROP INDEX IF EXISTS idx_matchmaking_queue_finding_count;
CREATE INDEX IF NOT EXISTS idx_matchmaking_queue_finding_count
    ON matchmaking_queue (tenant_id, category, question_count)
    WHERE status = 'finding';
