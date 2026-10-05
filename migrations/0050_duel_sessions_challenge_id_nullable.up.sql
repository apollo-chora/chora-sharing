-- ============================================================================
-- chora-sharing : 0050_duel_sessions_challenge_id_nullable.up.sql
--
-- Drops NOT NULL on duel_sessions.challenge_id. Pool-based matchmaking
-- (duel.go NewPoolDuel) doesn't use challenges — the Go INSERT doesn't
-- supply challenge_id. The column was NOT NULL from the old schema, causing
-- SaveDuel to fail with SQLSTATE 23502 (null value violates not-null
-- constraint) → both searchers restored to pool → infinite loop.
--
-- The column may not exist (the base 0001 schema doesn't include it), so
-- the ALTER is guarded by a column-existence check.
-- ============================================================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'duel_sessions' AND column_name = 'challenge_id'
    ) THEN
        ALTER TABLE duel_sessions ALTER COLUMN challenge_id DROP NOT NULL;
    END IF;
END $$;
