-- =============================================================================
-- chora-sharing : 0032_relationship_friendships_requests.up.sql
--
-- ADR-230 B-lite.1 (CHO-2119). The Relationship aggregate's two new planes:
--
--  (1) social_friend_requests — pending friendship intents (requester →
--      addressee). Deliberately rows, never graph edges: a pending request
--      has no graph-query value and would leak intent to the future
--      projector. Decline/cancel DELETE the row (no event).
--
--  (2) social_friendships — accepted friendships as ONE canonical undirected
--      pair per tenant: member_lo_gcid < member_hi_gcid (Postgres uuid
--      ordering, enforced by CHECK; writers canonicalise via LEAST/GREATEST
--      in SQL so the DB ordering is authoritative).
--
-- Explicit friendship replaces the WS-0 derived mutual-follow friend set
-- (ADR-230 D2); GetReuseContext's contract is unchanged. NO backfill of
-- mutual follows — friendship exists only by request/accept consent.
--
-- Uniqueness is TENANT-SCOPED (unlike the legacy pair-only uniques on
-- social_follows/social_blocks): a dual-tenant member pair holds
-- independent relationship state per tenant, and a cross-tenant ON CONFLICT
-- can never silently no-op against a row RLS hides.
--
-- RLS: ENABLE + FORCE, GUC chora.tenant_id, NULLIF-safe (0031 idiom).
-- NOTE: 9999_grant_app_roles.sql grants cover these tables automatically
-- (ALTER DEFAULT PRIVILEGES) — re-run it anyway when applying batches, per
-- the 0031 deploy recipe.
-- =============================================================================

-- (1) social_friend_requests ---------------------------------------------------

CREATE TABLE IF NOT EXISTS social_friend_requests (
    request_id     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id      UUID        NOT NULL,
    requester_gcid UUID        NOT NULL,
    addressee_gcid UUID        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, requester_gcid, addressee_gcid),
    CHECK (requester_gcid <> addressee_gcid)   -- self-request rejected
);

CREATE INDEX IF NOT EXISTS social_friend_requests_addressee_idx
    ON social_friend_requests (tenant_id, addressee_gcid);
CREATE INDEX IF NOT EXISTS social_friend_requests_requester_idx
    ON social_friend_requests (tenant_id, requester_gcid);

ALTER TABLE social_friend_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE social_friend_requests FORCE  ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE tablename = 'social_friend_requests'
          AND policyname = 'social_friend_requests_tenant_isolation'
    ) THEN
        CREATE POLICY social_friend_requests_tenant_isolation ON social_friend_requests
            FOR ALL
            USING (tenant_id = NULLIF(current_setting('chora.tenant_id', true), '')::uuid);
    END IF;
END $$;

-- (2) social_friendships --------------------------------------------------------

CREATE TABLE IF NOT EXISTS social_friendships (
    friendship_id  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id      UUID        NOT NULL,
    member_lo_gcid UUID        NOT NULL,
    member_hi_gcid UUID        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, member_lo_gcid, member_hi_gcid),
    CHECK (member_lo_gcid < member_hi_gcid)    -- canonical pair; also bans self
);

CREATE INDEX IF NOT EXISTS social_friendships_lo_idx
    ON social_friendships (tenant_id, member_lo_gcid);
CREATE INDEX IF NOT EXISTS social_friendships_hi_idx
    ON social_friendships (tenant_id, member_hi_gcid);

ALTER TABLE social_friendships ENABLE ROW LEVEL SECURITY;
ALTER TABLE social_friendships FORCE  ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE tablename = 'social_friendships'
          AND policyname = 'social_friendships_tenant_isolation'
    ) THEN
        CREATE POLICY social_friendships_tenant_isolation ON social_friendships
            FOR ALL
            USING (tenant_id = NULLIF(current_setting('chora.tenant_id', true), '')::uuid);
    END IF;
END $$;
