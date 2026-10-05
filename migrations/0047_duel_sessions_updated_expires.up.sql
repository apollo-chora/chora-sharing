-- ============================================================================
-- chora-sharing : 0047_duel_sessions_updated_expires.up.sql
--
-- Adds updated_at + expires_at to duel_sessions. Same root cause as 0046:
-- 0004_duel.up.sql was superseded (marked applied without running), and
-- the live table created by a diverged schema lacks these columns.
-- duel_repo.go INSERTs updated_at on every SaveDuel and SELECTs expires_at
-- on every GetDuel — missing either column causes SQLSTATE 42703 → both
-- searchers restored to pool → infinite "Finding an opponent..." loop.
--
-- Column types match the original 0004_duel.up.sql definitions:
--   updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
--   expires_at TIMESTAMPTZ (nullable — duels may not have an expiry)
-- ============================================================================

ALTER TABLE duel_sessions
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;

-- Backfill updated_at for pre-existing rows.
UPDATE duel_sessions SET updated_at = created_at;
