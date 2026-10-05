-- =============================================================================
-- chora-sharing : 0039_post_drafts.down.sql  (CHO-2203)
--
-- Reverses the 0039 up RECONCILIATION only — it does NOT drop the table or the
-- post_draft_status enum, which pre-existed out-of-band before this migration
-- (see the up header). Rolling back removes only the RLS scoping re-asserted here.
-- =============================================================================
BEGIN;

DROP POLICY IF EXISTS tenant_isolation ON post_drafts;
ALTER TABLE post_drafts NO FORCE ROW LEVEL SECURITY;
ALTER TABLE post_drafts DISABLE ROW LEVEL SECURITY;

COMMIT;
