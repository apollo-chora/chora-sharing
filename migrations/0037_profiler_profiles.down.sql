-- ============================================================================
-- chora-sharing : 0037_profiler_profiles.down.sql
-- ============================================================================

DROP POLICY IF EXISTS profiler_profiles_tenant_isolation ON profiler_profiles;
DROP TABLE IF EXISTS profiler_profiles;
