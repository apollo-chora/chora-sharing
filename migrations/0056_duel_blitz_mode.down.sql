-- ============================================================================
-- chora-sharing : 0056_duel_blitz_mode.down.sql
--
-- Reverses 0056_duel_blitz_mode.up.sql — drops the blitz columns from
-- duel_sessions + matchmaking_queue.
-- ============================================================================

DROP INDEX IF EXISTS idx_matchmaking_queue_finding_mode;

ALTER TABLE matchmaking_queue
    DROP COLUMN IF EXISTS blitz_variant;
ALTER TABLE matchmaking_queue
    DROP COLUMN IF EXISTS mode;

ALTER TABLE duel_sessions
    DROP COLUMN IF EXISTS blitz_started_at;
ALTER TABLE duel_sessions
    DROP COLUMN IF EXISTS blitz_race_target;
ALTER TABLE duel_sessions
    DROP COLUMN IF EXISTS blitz_time_limit_sec;
ALTER TABLE duel_sessions
    DROP COLUMN IF EXISTS blitz_variant;
ALTER TABLE duel_sessions
    DROP COLUMN IF EXISTS mode;

-- Recreate the pre-blitz finding-count index (dropped in the up migration).
CREATE INDEX IF NOT EXISTS idx_matchmaking_queue_finding_count
    ON matchmaking_queue (tenant_id, category, question_count)
    WHERE status = 'finding';
