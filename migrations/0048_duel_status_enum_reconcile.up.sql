-- ============================================================================
-- chora-sharing : 0048_duel_status_enum_reconcile.up.sql
--
-- Adds 'accepted' + 'in_progress' to the duel_status enum. The Go
-- duel domain (duel.go) uses 5 states (pending, accepted, in_progress,
-- completed, cancelled) but the live enum only had 4 (pending, active,
-- completed, cancelled) — 'active' was the old name for in_progress.
--
-- Pool matchmaking creates duels as 'accepted' (pool matching IS the
-- acceptance per spec §4.8) then transitions to 'in_progress' via
-- StartBattle. Without these enum values SaveDuel fails with
-- SQLSTATE 22P02 → both searchers restored to pool → infinite loop.
-- ============================================================================

ALTER TYPE duel_status ADD VALUE IF NOT EXISTS 'accepted';
ALTER TYPE duel_status ADD VALUE IF NOT EXISTS 'in_progress';
