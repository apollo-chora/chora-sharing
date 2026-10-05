-- ============================================================================
-- chora-sharing : 0046_duel_sessions_interest_tags.up.sql
--
-- Adds the interest_tags column to duel_sessions. The column was defined in
-- 0004_duel.up.sql, but 0004 was superseded (marked applied without running)
-- because the live duel_sessions table was created by a diverged schema.
-- The Go duel_repo.go SELECTs/INSERTs/UPDATEs interest_tags on every duel
-- query, so without this column every matchmaker SaveDuel fails with
-- SQLSTATE 42703 and both searchers are restored to the pool → infinite
-- "Finding an opponent..." loop.
--
-- TEXT[] NOT NULL DEFAULT '{}' matches the original 0004 definition.
-- ============================================================================

ALTER TABLE duel_sessions
    ADD COLUMN IF NOT EXISTS interest_tags TEXT[] NOT NULL DEFAULT '{}';
