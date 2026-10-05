-- ============================================================================
-- chora-sharing : 0037_profiler_profiles.up.sql
--
-- Interest profiler profiles (C+ / duel matchmaking). Bio + course titles
-- + structured interest tags. Tags are currently produced by the static
-- extractor; the LLM extractor will replace generation later — storage is
-- durable either way.
--
-- RLS discipline:
--   - tenant_id UUID NOT NULL
--   - ENABLE + FORCE ROW LEVEL SECURITY
--   - policy FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid)
--   - GUC is chora.tenant_id
-- ============================================================================

CREATE TABLE IF NOT EXISTS profiler_profiles (
    gcid          UUID         NOT NULL,
    tenant_id     UUID         NOT NULL,
    bio           TEXT         NOT NULL DEFAULT '',
    tags          JSONB        NOT NULL DEFAULT '[]'::jsonb,
    course_titles TEXT[]       NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, gcid)
);

CREATE INDEX IF NOT EXISTS profiler_profiles_gcid_idx
    ON profiler_profiles (gcid);

ALTER TABLE profiler_profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE profiler_profiles FORCE  ROW LEVEL SECURITY;

DROP POLICY IF EXISTS profiler_profiles_tenant_isolation ON profiler_profiles;
CREATE POLICY profiler_profiles_tenant_isolation ON profiler_profiles
    FOR ALL
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- Ensure app roles can touch the new table even if 9999 defaults were
-- applied before this migration existed.
GRANT SELECT, INSERT, UPDATE, DELETE ON profiler_profiles TO chora_sharing_app_rw;
GRANT SELECT ON profiler_profiles TO chora_sharing_app_ro;
