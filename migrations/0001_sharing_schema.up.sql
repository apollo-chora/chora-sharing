-- ============================================================================
-- chora-sharing : 0001_sharing_schema.up.sql
--
-- Domain       : Content Sharing (core) — C+ Connect+
-- Database     : chora_sharing
-- Spec ref     : docs/chora-sharing.md §4.2 (Extensions + enums + trigger)
--
-- Purpose:
--   Extensions, the circle_set_updated_at() trigger function, and the §4.2
--   enums. ADDITIVE ONLY. Tenant-scoped tables + RLS are created in
--   subsequent migrations.
--
-- ── CHO-2193 — this file used to be a demolition charge ─────────────────────
--
--   Until 2026-07-15 it opened with a "greenfield instruction": 36
--   unconditional `DROP TABLE ... CASCADE`, on the grounds that the legacy
--   tables were "NOT part of the target design". The greenfield reset it was
--   written for never happened. Production has been LIVE on those tables for
--   months — posts (16 rows), reactions (3 rows — the table the C+ code
--   actually reads), and 28 others.
--
--   The file had no `chora_runner_schema_migrations` row, and it sorts FIRST
--   in lex order. So the next run of the standard migration pipeline —
--   cloudbuild-migrations-apply.yaml rsyncs services/*/migrations/ to GCS and
--   then executes the apply Job over everything staged there — would have
--   staged this file and immediately run it, dropping the entire C+ social
--   dataset. Arming and firing in one pipeline.
--
--   The drop block is WITHDRAWN, not allowlisted. A migration does not get to
--   destroy live data because an old design document says a table should not
--   exist. If the greenfield redesign is ever genuinely adopted it needs a
--   real data migration and its own ADR — not a CASCADE hidden in the boring
--   file that everyone applies for its enum declarations.
--
--   Guarded going forward by migrations-runner/destructive-guard_test.sh (no
--   unreviewed DROP TABLE may sit in any lane) and by the runner's own
--   assert_not_destructive(), which now refuses to apply one.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1) Extensions
-- ----------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ----------------------------------------------------------------------------
-- 2) Auto-update updated_at trigger function
--    (bumps updated_at on every UPDATE; applied per-table in later migrations)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION circle_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ----------------------------------------------------------------------------
-- 3) Enums (lowercase snake_case labels in DB; proto uses UPPER_CASE — adapter maps)
--    Per §4.2 first block. duel_status uses the full v2 value set (§15).
--
--    Each is wrapped in an EXCEPTION-guarded DO block so the file is genuinely
--    REPLAY-SAFE — the standard runner.sh declares for every migration
--    ("IF NOT EXISTS / CREATE OR REPLACE / EXCEPTION WHEN duplicate_object").
--    A bare CREATE TYPE raises 42710 on replay and survives only because the
--    runner's classifier happens to forgive "already exists" — a lenient
--    runner covering for a non-idempotent file. That cover disappears the
--    moment the file meets anything stricter (migrate CLI, psql -1, a
--    rehearsal harness), which is exactly when a lane wedges.
-- ----------------------------------------------------------------------------

DO $$ BEGIN
    CREATE TYPE post_visibility AS ENUM ('public', 'tenant', 'private');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE feed_entry_type AS ENUM (
        'share',
        'completion',
        'achievement',
        'challenge_accepted',
        'challenge_completed',
        'duel_completed',
        'streak_milestone'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- Full v2 value set (§15): pending/accepted/in_progress/completed/forfeited/expired.
-- Legacy 'active'/'cancelled' are NOT present.
DO $$ BEGIN
    CREATE TYPE duel_status AS ENUM (
        'pending',
        'accepted',
        'in_progress',
        'completed',
        'forfeited',
        'expired'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE duel_mode AS ENUM ('topic_ai', 'pool');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE duel_scope AS ENUM ('friendly', 'ranked');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE atom_license_terms AS ENUM (
        'free',
        'royalty_pct',
        'royalty_fixed',
        'cc_by_sa',
        'cc_nd'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE atom_grant_scope AS ENUM (
        'test_set',
        'duel',
        'live_quiz',
        'collection',
        'unlimited'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE atom_grant_status AS ENUM ('active', 'revoked', 'expired');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE atom_share_event_type AS ENUM (
        'hidden',
        'price_changed',
        'revoked',
        'moderation_hidden'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE royalty_currency AS ENUM ('mana', 'coins', 'reputation', 'usd');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE royalty_usage_context AS ENUM ('question_set', 'duel', 'live_quiz');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- 0005_currency_leaderboard.up.sql depends on this type: currency_balances.currency
-- is declared `currency_code NOT NULL`. Because 0001 never applied, the enum never
-- existed, so 0005 failed loudly and correctly — and currency_balances +
-- atom_xp_credits have been absent from production ever since. Disarming this file
-- is what unblocks them.
DO $$ BEGIN
    CREATE TYPE currency_code AS ENUM ('xp', 'coins', 'reputation');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE leaderboard_period AS ENUM ('weekly', 'monthly', 'all-time');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE leaderboard_scope_kind AS ENUM ('global', 'tenant', 'cohort');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
