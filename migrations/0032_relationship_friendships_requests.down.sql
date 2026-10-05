-- =============================================================================
-- chora-sharing : 0032_relationship_friendships_requests.down.sql
--
-- Reverts ADR-230 B-lite.1 relationship tables. Relationship lifecycle
-- HISTORY lives in sharing_outbox_events / Pub/Sub (append-only) and is not
-- touched; only the current-state tables drop.
-- =============================================================================

DROP TABLE IF EXISTS social_friendships;
DROP TABLE IF EXISTS social_friend_requests;
