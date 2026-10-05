-- ============================================================================
-- chora-sharing : 0036_duel_peak_elo.up.sql
--
-- Add peak_elo column to duel_ratings (CHO-335 acceptance criterion).
-- Tracks the highest rating a player has ever achieved. Updated on every
-- ranked ELO application when the new rating exceeds the stored peak.
-- ============================================================================

ALTER TABLE duel_ratings
    ADD COLUMN IF NOT EXISTS peak_elo INTEGER NOT NULL DEFAULT 1200;

-- Backfill peak_elo from current rating for existing rows.
UPDATE duel_ratings
SET peak_elo = GREATEST(peak_elo, rating)
WHERE peak_elo < rating;
