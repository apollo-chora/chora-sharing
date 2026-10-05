-- ============================================================================
-- chora-sharing : 0002_social.up.sql
--
-- Spec ref : docs/chora-sharing.md §4.2 (social_follows, social_feed_entries,
--           social_feed_reactions, post_comments)
--
-- RLS discipline (§4.1):
--   - tenant_id UUID NOT NULL on every tenant-scoped table
--   - ENABLE + FORCE ROW LEVEL SECURITY
--   - policy FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid)
--   - GUC is chora.tenant_id (NOT app.current_tenant_id — legacy, not set here)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- social_follows — append-only directional follow edges (follower → followee)
-- ----------------------------------------------------------------------------
CREATE TABLE social_follows (
    edge_id        UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id      UUID         NOT NULL,
    follower_gcid  UUID         NOT NULL,
    followee_gcid  UUID         NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (follower_gcid, followee_gcid),
    CHECK (follower_gcid <> followee_gcid)   -- self-follow rejected
);

CREATE INDEX social_follows_follower_idx ON social_follows (follower_gcid);
CREATE INDEX social_follows_followee_idx ON social_follows (followee_gcid);
CREATE INDEX social_follows_tenant_idx   ON social_follows (tenant_id);

ALTER TABLE social_follows ENABLE ROW LEVEL SECURITY;
ALTER TABLE social_follows FORCE  ROW LEVEL SECURITY;
CREATE POLICY social_follows_tenant_isolation ON social_follows
    FOR ALL
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- ----------------------------------------------------------------------------
-- social_feed_entries — IMMUTABLE feed (Posts + shared-atom entries)
--   entry_type='share' carries license_terms + royalty_rate + atom refs + caption
--   in content JSONB. NO updated_at, NO deleted_at (hide/revoke = atom_share_events).
-- ----------------------------------------------------------------------------
CREATE TABLE social_feed_entries (
    id               UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        UUID            NOT NULL,
    actor_gcid       UUID            NOT NULL,
    entry_type       feed_entry_type NOT NULL,
    content          JSONB           NOT NULL DEFAULT '{}',
    reaction_counts  JSONB           NOT NULL DEFAULT '{}',
    created_at       TIMESTAMPTZ     NOT NULL DEFAULT now()
);

CREATE INDEX social_feed_entries_tenant_created_idx
    ON social_feed_entries (tenant_id, created_at DESC);
CREATE INDEX social_feed_entries_actor_tenant_idx
    ON social_feed_entries (actor_gcid, tenant_id);

ALTER TABLE social_feed_entries ENABLE ROW LEVEL SECURITY;
ALTER TABLE social_feed_entries FORCE  ROW LEVEL SECURITY;
CREATE POLICY social_feed_entries_tenant_isolation ON social_feed_entries
    FOR ALL
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- ----------------------------------------------------------------------------
-- social_feed_reactions — reactions on feed entries
--   tenant_id is denormalised onto the reaction row so RLS can scope it
--   directly (avoids a join through the entry FK at the policy layer).
-- ----------------------------------------------------------------------------
CREATE TABLE social_feed_reactions (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id    UUID        NOT NULL,
    entry_id     UUID        NOT NULL REFERENCES social_feed_entries(id) ON DELETE CASCADE,
    reactor_gcid UUID        NOT NULL,
    emoji        VARCHAR(10) NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (entry_id, reactor_gcid, emoji)
);

CREATE INDEX social_feed_reactions_entry_idx ON social_feed_reactions (entry_id);

ALTER TABLE social_feed_reactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE social_feed_reactions FORCE  ROW LEVEL SECURITY;
CREATE POLICY social_feed_reactions_tenant_isolation ON social_feed_reactions
    FOR ALL
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- ----------------------------------------------------------------------------
-- post_comments — 1-level reply via parent_comment_id self-FK
-- ----------------------------------------------------------------------------
CREATE TABLE post_comments (
    id                UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id         UUID         NOT NULL,
    post_id           UUID         NOT NULL,   -- references social_feed_entries.id (no FK: cross-aggregate, validated at write)
    author_gcid       UUID         NOT NULL,
    body              TEXT         NOT NULL,
    parent_comment_id UUID,                              -- 1-level reply (NULL = top-level)
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT fk_parent FOREIGN KEY (parent_comment_id) REFERENCES post_comments(id)
);

CREATE INDEX post_comments_tenant_post_idx ON post_comments (tenant_id, post_id);
CREATE INDEX post_comments_post_created_idx ON post_comments (post_id, created_at DESC);

ALTER TABLE post_comments ENABLE ROW LEVEL SECURITY;
ALTER TABLE post_comments FORCE  ROW LEVEL SECURITY;
CREATE POLICY post_comments_tenant_isolation ON post_comments
    FOR ALL
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);
