-- ============================================================================
-- chora-sharing : 0036_duel_peak_elo.down.sql
-- ============================================================================

ALTER TABLE duel_ratings
    DROP COLUMN IF EXISTS peak_elo;
