-- =============================================================================
-- chora-sharing : 0041_subscriber_idempotency.down.sql  (CHO-2203)
--
-- Reverses the 0041 up RECONCILIATION only — it does NOT drop the table, which
-- pre-existed out-of-band before this migration (see the up header). Rolling
-- back removes the RLS scoping this migration added and re-opens the table.
-- =============================================================================
BEGIN;

DROP POLICY IF EXISTS tenant_isolation ON subscriber_idempotency;
ALTER TABLE subscriber_idempotency NO FORCE ROW LEVEL SECURITY;
ALTER TABLE subscriber_idempotency DISABLE ROW LEVEL SECURITY;
-- Leave the tenant_id column + table in place (the table pre-existed OOB; a
-- DROP COLUMN would be destructive and is refused by the lane scanner).

COMMIT;
