-- ============================================================================
-- chora-sharing : 0035_tenant_guc_policy_reconcile.down.sql
--
-- Best-effort inverse: re-key every chora.tenant_id tenant_isolation policy
-- back to the legacy app.current_tenant_id GUC. NOTE this deliberately
-- matches ONLY policies named exactly 'tenant_isolation' (the 0002/0004/
-- 0008/0030-era name) so the 0031+ era policies (distinct names, canonical
-- GUC) are untouched.
-- ============================================================================
BEGIN;

DO $$
DECLARE
    pol RECORD;
BEGIN
    FOR pol IN
        SELECT polrelid::regclass AS rel, polname
        FROM pg_policy
        WHERE polname = 'tenant_isolation'
          AND pg_get_expr(polqual, polrelid) LIKE '%chora.tenant_id%'
    LOOP
        EXECUTE format('DROP POLICY %I ON %s', pol.polname, pol.rel);
        EXECUTE format(
            'CREATE POLICY %I ON %s FOR ALL USING (tenant_id = current_setting(''app.current_tenant_id'', true)::uuid)',
            pol.polname, pol.rel);
    END LOOP;
END $$;

COMMIT;
