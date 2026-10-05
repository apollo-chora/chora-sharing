-- ============================================================================
-- chora-sharing : 0007_closure_idempotency_outbox.down.sql
-- Reverses 0007_closure_idempotency_outbox.up.sql.
-- Drop order: function last (tables may not depend on it, but drop function
-- after tables for cleanliness).
-- ============================================================================

DROP TABLE IF EXISTS sharing_outbox_events CASCADE;
DROP TABLE IF EXISTS idempotency_keys CASCADE;
DROP TABLE IF EXISTS closure_log CASCADE;
DROP FUNCTION IF EXISTS cleanup_idempotency_keys(TEXT) CASCADE;
