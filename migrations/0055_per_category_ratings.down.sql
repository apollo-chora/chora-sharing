-- ============================================================================
-- chora-sharing : 0055_per_category_ratings.down.sql
--
-- Reverses WS1 per-category ratings.
-- ============================================================================

-- Restore the 2-column unique index (pre-WS1).
DROP INDEX IF EXISTS duel_ratings_active_unique_idx;
CREATE UNIQUE INDEX IF NOT EXISTS duel_ratings_active_unique_idx
    ON duel_ratings (tenant_id, gcid)
    WHERE deleted_at IS NULL;

DROP INDEX IF EXISTS duel_ratings_tenant_rating_idx;
CREATE INDEX IF NOT EXISTS duel_ratings_tenant_rating_idx
    ON duel_ratings (tenant_id, rating DESC)
    WHERE deleted_at IS NULL;

ALTER TABLE duel_ratings DROP COLUMN IF EXISTS category;
ALTER TABLE duel_sessions DROP COLUMN IF EXISTS category;
ALTER TABLE matchmaking_queue DROP COLUMN IF EXISTS category;

-- Restore the WS4-only finding count index (without category).
DROP INDEX IF EXISTS idx_matchmaking_queue_finding_count;
CREATE INDEX IF NOT EXISTS idx_matchmaking_queue_finding_count
    ON matchmaking_queue (tenant_id, question_count)
    WHERE status = 'finding';
