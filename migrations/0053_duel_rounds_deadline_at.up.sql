-- ============================================================================
-- chora-sharing : 0053_duel_rounds_deadline_at.up.sql
--
-- WS3: enforced round timer. Adds deadline_at to duel_rounds so the
-- server-side round-timer sweep can auto-resolve expired rounds. The
-- column is nullable (existing rows pre-WS3 have no deadline) and the
-- domain treats a NULL deadline as "no timer" (round never auto-times out).
-- New rounds get deadline_at = now + round_timer_sec stamped at StartBattle.
-- ============================================================================

ALTER TABLE duel_rounds
    ADD COLUMN IF NOT EXISTS deadline_at TIMESTAMPTZ;

-- Index for the round-sweeper's expired-round scan: in-progress duels with
-- at least one unresolved round whose deadline_at < now. A partial index
-- keeps it small (only unresolved rows).
CREATE INDEX IF NOT EXISTS idx_duel_rounds_expired_deadline
    ON duel_rounds (deadline_at)
    WHERE resolved_at IS NULL AND deadline_at IS NOT NULL;
