-- =============================================================================
-- chora-sharing : 0040_user_preferences.down.sql  (CHO-2203)
--
-- Reverses the 0040 up RECONCILIATION only — it does NOT drop the table or the
-- enum type, which pre-existed out-of-band before this migration (see the up
-- header). Rolling back removes only the RLS scoping this migration re-asserted.
-- =============================================================================
BEGIN;

DROP POLICY IF EXISTS tenant_isolation ON user_preferences;
ALTER TABLE user_preferences NO FORCE ROW LEVEL SECURITY;
ALTER TABLE user_preferences DISABLE ROW LEVEL SECURITY;

COMMIT;
