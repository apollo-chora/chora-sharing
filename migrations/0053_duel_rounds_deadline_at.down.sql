-- ============================================================================
-- chora-sharing : 0053_duel_rounds_deadline_at.down.sql
--
-- Reverses WS3 deadline_at column addition.
-- ============================================================================

DROP INDEX IF EXISTS idx_duel_rounds_expired_deadline;
ALTER TABLE duel_rounds DROP COLUMN IF EXISTS deadline_at;
