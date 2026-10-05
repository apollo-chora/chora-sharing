-- ============================================================================
-- chora-sharing : 0049_duel_rounds_columns.up.sql
--
-- Adds 5 columns to duel_rounds that the Go duel_repo.go INSERTs but were
-- missing from the live table (same superseded-migration divergence as
-- 0046/0047/0048). Without these, SaveRound fails with SQLSTATE 42703
-- when the matchmaker creates duel rounds after SaveDuel succeeds.
--
-- Column types match the 0004_duel.up.sql definitions:
--   question            TEXT
--   options             TEXT[]
--   correct_answer      TEXT
--   challenger_answered BOOLEAN NOT NULL DEFAULT false
--   opponent_answered   BOOLEAN NOT NULL DEFAULT false
-- ============================================================================

ALTER TABLE duel_rounds
    ADD COLUMN IF NOT EXISTS question TEXT,
    ADD COLUMN IF NOT EXISTS options TEXT[],
    ADD COLUMN IF NOT EXISTS correct_answer TEXT,
    ADD COLUMN IF NOT EXISTS challenger_answered BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS opponent_answered BOOLEAN NOT NULL DEFAULT false;
