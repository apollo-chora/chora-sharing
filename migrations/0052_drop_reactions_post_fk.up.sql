-- 0052_drop_reactions_post_fk.up.sql
--
-- The reactions.post_id column accepts BOTH social posts (posts.post_id)
-- AND shared-atom feed entries (social_feed_entries.id). The original
-- FK constraint reactions_post_id_fkey only references posts.post_id,
-- causing 23503 violations when a user reacts to a feed card (which
-- uses the social_feed_entries.id as the post_id).
--
-- The reactToPost handler (social_handlers.go:55) already does a dual
-- existence check — Posts.Get() first, then Shares.GetShare() fallback
-- so removing the FK doesn't weaken referential integrity at the
-- application layer. The RLS policy on reactions still enforces
-- tenant isolation.

ALTER TABLE social_feed_reactions DROP CONSTRAINT IF EXISTS social_feed_reactions_post_id_fkey;
