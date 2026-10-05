-- 0052_drop_reactions_post_fk.down.sql
-- Restore the FK constraint (re-adds the 23503 risk for feed-card reactions).
ALTER TABLE reactions
    ADD CONSTRAINT reactions_post_id_fkey
    FOREIGN KEY (post_id) REFERENCES posts(post_id) ON DELETE CASCADE;
