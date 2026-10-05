-- ============================================================================
-- chora-sharing : 0001_sharing_schema.down.sql
--
-- Reverses 0001_sharing_schema.up.sql: drops the trigger function, all enums,
-- and the extensions installed by the up migration.
--
-- CHO-2193: the up migration no longer drops any table, so there is nothing
-- destructive left to reverse. It used to carry 36 `DROP TABLE ... CASCADE`
-- and this header used to explain, calmly, that they were "intentionally NOT
-- reversed (greenfield cutover — the legacy tables are deleted, not
-- restored)". Those tables were production. The drop block is withdrawn.
-- ============================================================================

DROP FUNCTION IF EXISTS circle_set_updated_at() CASCADE;

DROP TYPE IF EXISTS leaderboard_scope_kind;
DROP TYPE IF EXISTS leaderboard_period;
DROP TYPE IF EXISTS currency_code;
DROP TYPE IF EXISTS royalty_usage_context;
DROP TYPE IF EXISTS royalty_currency;
DROP TYPE IF EXISTS atom_share_event_type;
DROP TYPE IF EXISTS atom_grant_status;
DROP TYPE IF EXISTS atom_grant_scope;
DROP TYPE IF EXISTS atom_license_terms;
DROP TYPE IF EXISTS duel_scope;
DROP TYPE IF EXISTS duel_mode;
DROP TYPE IF EXISTS duel_status;
DROP TYPE IF EXISTS feed_entry_type;
DROP TYPE IF EXISTS post_visibility;

-- Extensions are shared across the DB; only drop if nothing else depends on
-- them. IF NOT EXISTS guards are not valid for DROP EXTENSION, so we drop
-- quietly and ignore dependency errors via CASCADE.
DROP EXTENSION IF EXISTS "pgcrypto";
DROP EXTENSION IF EXISTS "uuid-ossp";
