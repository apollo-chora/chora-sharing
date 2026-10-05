-- ============================================================================
-- chora-sharing : 0054_matchmaking_queue_question_count.down.sql
--
-- Reverses WS4 question_count column addition.
-- ============================================================================

DROP INDEX IF EXISTS idx_matchmaking_queue_finding_count;
ALTER TABLE matchmaking_queue DROP COLUMN IF EXISTS question_count;
