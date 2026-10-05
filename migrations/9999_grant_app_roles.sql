-- ============================================================================
-- chora-sharing : 9999_grant_app_roles.sql
--
-- Domain        : Content Sharing (core)
-- Database      : chora_sharing
-- Spec ref      : docs/chora-sharing.md §4.1 (app_rw / app_ro grants inherit
--                automatically via ALTER DEFAULT PRIVILEGES in the final
--                9999_grant_app_roles.sql)
--
-- Purpose:
--   Every tenant-scoped table created by migrations 0001..0007 needs the
--   application roles (app_rw / app_ro) to have privileges. The `migrate` role
--   bypasses RLS and owns the tables; app_rw / app_ro do NOT have BYPASSRLS,
--   so their grants remain subject to every per-table RLS policy.
--
-- Idempotency:
--   - GRANT is idempotent by design — re-applying is a no-op.
--   - ALTER DEFAULT PRIVILEGES is idempotent on the same (role, schema) pair.
--   - This file is the LAST in lex order (9999 prefix) — every prior migration's
--     tables/sequences/functions are picked up by the ALL ... IN SCHEMA public
--     clauses, AND every future migration's newly-created objects inherit
--     automatically.
--
-- Role naming (per chora-infra m10-data-plane convention):
--   chora_sharing_app_rw   SELECT, INSERT, UPDATE, DELETE on all tables;
--                          USAGE+SELECT+UPDATE on all sequences;
--                          EXECUTE on all functions
--   chora_sharing_app_ro   SELECT on all tables;
--                          USAGE+SELECT on all sequences;
--                          EXECUTE on all functions (for SECURITY DEFINER paths)
--   chora_sharing_migrate  owns the tables (no GRANT needed)
--
-- HARD RULE: cross-database queries forbidden. This migration only touches
-- chora_sharing-local roles + objects.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1) Schema USAGE — both app roles need to "see" the public schema before
--    they can touch any object inside it.
-- ----------------------------------------------------------------------------

GRANT USAGE ON SCHEMA public TO chora_sharing_app_rw, chora_sharing_app_ro;

-- ----------------------------------------------------------------------------
-- 2) Existing-object grants — covers every table/sequence/function created
--    by migrations 0001..0007 already applied.
-- ----------------------------------------------------------------------------

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public
  TO chora_sharing_app_rw;

GRANT SELECT ON ALL TABLES IN SCHEMA public
  TO chora_sharing_app_ro;

GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public
  TO chora_sharing_app_rw;

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public
  TO chora_sharing_app_ro;

GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public
  TO chora_sharing_app_rw, chora_sharing_app_ro;

-- ----------------------------------------------------------------------------
-- 3) Default privileges for FUTURE objects — so subsequent migrations
--    creating new tables/sequences/functions Just Work without app_rw / app_ro
--    losing access. Tied to the migrate role (the role that runs DDL).
-- ----------------------------------------------------------------------------

ALTER DEFAULT PRIVILEGES FOR ROLE chora_sharing_migrate IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES
  TO chora_sharing_app_rw;

ALTER DEFAULT PRIVILEGES FOR ROLE chora_sharing_migrate IN SCHEMA public
  GRANT SELECT ON TABLES
  TO chora_sharing_app_ro;

ALTER DEFAULT PRIVILEGES FOR ROLE chora_sharing_migrate IN SCHEMA public
  GRANT USAGE, SELECT, UPDATE ON SEQUENCES
  TO chora_sharing_app_rw;

ALTER DEFAULT PRIVILEGES FOR ROLE chora_sharing_migrate IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES
  TO chora_sharing_app_ro;

ALTER DEFAULT PRIVILEGES FOR ROLE chora_sharing_migrate IN SCHEMA public
  GRANT EXECUTE ON FUNCTIONS
  TO chora_sharing_app_rw, chora_sharing_app_ro;
