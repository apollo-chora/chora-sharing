-- ============================================================================
-- chora-sharing : 0045_profiler_proficiency.down.sql
-- Reverses 0045_profiler_proficiency.up.sql.
-- ============================================================================

ALTER TABLE profiler_profiles
    DROP COLUMN IF EXISTS proficiency,
    DROP COLUMN IF EXISTS display_name;
