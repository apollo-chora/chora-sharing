-- 0051 down: reverse the reconciliation (re-introduce the drift).
-- This is provided for completeness; do NOT run in production.

DROP INDEX IF EXISTS duel_ratings_active_unique_idx;
DROP INDEX IF EXISTS duel_ratings_tenant_rating_idx;

ALTER TABLE duel_ratings ADD COLUMN IF NOT EXISTS topic_id UUID;

CREATE UNIQUE INDEX IF NOT EXISTS uq_duel_ratings_tenant_gcid_topic
    ON duel_ratings (tenant_id, gcid, COALESCE(topic_id, '00000000-0000-0000-0000-000000000000'::uuid))
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_duel_ratings_leaderboard
    ON duel_ratings (tenant_id, topic_id, rating DESC)
    WHERE deleted_at IS NULL;
