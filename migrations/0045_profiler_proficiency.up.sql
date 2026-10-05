-- ============================================================================
-- chora-sharing : 0045_profiler_proficiency.up.sql
--
-- Adds the agent-inferred proficiency band to profiler_profiles, plus the
-- display_name column used by the duel leaderboard JOIN
-- (sqlDuelRatingTopN already COALESCEs pp.display_name). display_name was
-- applied by hand to the local DB in an earlier session; this folds it
-- into versioned migrations so fresh environments match.
--
-- proficiency is JSONB (not an enum) so the profiler agent's per-category
-- map can evolve without a migration. Default '{}' keeps NOT NULL honest
-- for pre-existing rows.
-- ============================================================================

ALTER TABLE profiler_profiles
    ADD COLUMN IF NOT EXISTS proficiency JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS display_name TEXT NOT NULL DEFAULT '';
