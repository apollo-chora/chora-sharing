-- ============================================================================
-- chora-sharing : 0004_atom_sharing.down.sql
-- Reverses 0004_atom_sharing.up.sql. Drop order respects FK:
--   royalty_settlements + grant_events reference atom_usage_grants;
--   atom_share_events references social_feed_entries (created in 0002).
-- ============================================================================

DROP TABLE IF EXISTS royalty_settlements CASCADE;
DROP TABLE IF EXISTS grant_events CASCADE;
DROP TABLE IF EXISTS atom_usage_grants CASCADE;
DROP TABLE IF EXISTS atom_share_events CASCADE;
