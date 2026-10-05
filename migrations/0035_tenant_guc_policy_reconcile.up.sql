-- ============================================================================
-- chora-sharing : 0035_tenant_guc_policy_reconcile.up.sql
--
-- Live-walk deploy catch #4 (2026-07-11, CHO-2133/CHO-2132): chora_sharing is
-- split-brain across its own migration eras. The 0002/0004/0008/0030-era
-- tables carry `tenant_isolation` policies keyed on the LEGACY GUC
-- `app.current_tenant_id`, while every live code path sets `chora.tenant_id`
-- (libs/chora-go-common/rls.ApplySession) and the 0031+ tables key on it.
-- Under FORCE RLS that means: reads against the legacy-era tables silently
-- return zero rows and writes fail 42501 for EVERY ApplySession caller — the
-- first live AtomUsageGrant insert (ADR-229 D2 audit grant at snapshot time)
-- surfaced it: `new row violates row-level security policy for table
-- "atom_usage_grants"`. NO chora-sharing code sets the legacy GUC (verified
-- by grep — the only app.current_tenant_id users are chora-observability
-- adapters against chora_observability, a different database), so this swap
-- is strictly healing: it can only un-break paths, never widen access.
--
-- Mechanics: for every policy in THIS database whose USING expression
-- references app.current_tenant_id, drop it and recreate the SAME-named
-- policy keyed on chora.tenant_id (same FOR ALL / USING-only shape the
-- 0004-era policies had). Idempotent: after the swap the predicate no longer
-- matches, so a re-run is a no-op.
-- ============================================================================
BEGIN;

DO $$
DECLARE
    pol RECORD;
BEGIN
    FOR pol IN
        SELECT polrelid::regclass AS rel, polname
        FROM pg_policy
        WHERE pg_get_expr(polqual, polrelid) LIKE '%app.current_tenant_id%'
    LOOP
        EXECUTE format('DROP POLICY %I ON %s', pol.polname, pol.rel);
        EXECUTE format(
            'CREATE POLICY %I ON %s FOR ALL USING (tenant_id = current_setting(''chora.tenant_id'', true)::uuid)',
            pol.polname, pol.rel);
        RAISE NOTICE '0035: re-keyed policy % on % to chora.tenant_id', pol.polname, pol.rel;
    END LOOP;
END $$;

COMMIT;
