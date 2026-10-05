-- ============================================================================
-- chora-sharing : 0002_social.down.sql
-- Reverses 0002_social.up.sql. CASCADE drops the policies + indexes.
-- ============================================================================

DROP TABLE IF EXISTS post_comments CASCADE;
DROP TABLE IF EXISTS social_feed_reactions CASCADE;
DROP TABLE IF EXISTS social_feed_entries CASCADE;
DROP TABLE IF EXISTS social_follows CASCADE;
