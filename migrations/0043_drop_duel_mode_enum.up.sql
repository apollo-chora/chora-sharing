-- ============================================================================
-- chora-sharing : 0043_drop_duel_mode_enum.up.sql
--
-- Drops the dead duel_mode enum ('topic_ai', 'pool'). This enum was defined
-- in 0001_sharing_schema.up.sql:61 but never referenced by any table or Go
-- code. The pool-based matchmaking rewrite uses a single duel mode (no AI).
-- ============================================================================

DROP TYPE IF EXISTS duel_mode;
