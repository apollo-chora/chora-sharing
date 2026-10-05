-- ============================================================================
-- chora-sharing : 0056_duel_blitz_mode.up.sql
--
-- Blitz mode for duels. Adds mode + blitz columns to duel_sessions so a
-- duel can be classic (sequential FCFS rounds) or blitz (all questions
-- open at once — timed or race-to-N). Also adds mode + blitz_variant to
-- matchmaking_queue so the matchmaker pairs only same-mode + same-variant
-- entries.
--
-- Defaults preserve backward compatibility: existing rows are 'classic'
-- with no blitz config (NULL blitz_started_at, 0 blitz_time_limit_sec,
-- 0 blitz_race_target).
-- ============================================================================

-- 1. duel_sessions: mode + blitz config.
ALTER TABLE duel_sessions
    ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'classic';
ALTER TABLE duel_sessions
    ADD COLUMN IF NOT EXISTS blitz_variant TEXT;
ALTER TABLE duel_sessions
    ADD COLUMN IF NOT EXISTS blitz_time_limit_sec INTEGER NOT NULL DEFAULT 0;
ALTER TABLE duel_sessions
    ADD COLUMN IF NOT EXISTS blitz_race_target INTEGER NOT NULL DEFAULT 0;
ALTER TABLE duel_sessions
    ADD COLUMN IF NOT EXISTS blitz_started_at TIMESTAMPTZ;

-- 2. matchmaking_queue: mode + blitz_variant so the matchmaker pairs
--    same-mode + same-variant entries.
ALTER TABLE matchmaking_queue
    ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'classic';
ALTER TABLE matchmaking_queue
    ADD COLUMN IF NOT EXISTS blitz_variant TEXT;

-- Index for same-mode + same-variant + same-count pairing.
DROP INDEX IF EXISTS idx_matchmaking_queue_finding_count;
CREATE INDEX IF NOT EXISTS idx_matchmaking_queue_finding_mode
    ON matchmaking_queue (tenant_id, category, question_count, mode, blitz_variant)
    WHERE status = 'finding';
