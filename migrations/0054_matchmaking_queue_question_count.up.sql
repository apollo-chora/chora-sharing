-- ============================================================================
-- chora-sharing : 0054_matchmaking_queue_question_count.up.sql
--
-- WS4: configurable question count. Adds question_count to matchmaking_queue
-- so the matchmaker can pair only searchers who agree on the duel length
-- preset (Quick 5 / Standard 10 / Marathon 15). Default 5 preserves
-- backward compatibility with existing rows.
-- ============================================================================

ALTER TABLE matchmaking_queue
    ADD COLUMN IF NOT EXISTS question_count INTEGER NOT NULL DEFAULT 5;

-- Index for the matchmaker's same-count pairing filter: a finding searcher
-- is only paired with another finding searcher with the same question_count.
-- Partial index keeps it small (only finding rows).
CREATE INDEX IF NOT EXISTS idx_matchmaking_queue_finding_count
    ON matchmaking_queue (tenant_id, question_count)
    WHERE status = 'finding';
