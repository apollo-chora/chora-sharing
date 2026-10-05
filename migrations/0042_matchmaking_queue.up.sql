-- ============================================================================
-- chora-sharing : 0042_matchmaking_queue.up.sql
--
-- Pool-based duel matchmaking queue table. Tracks users actively searching
-- for duel opponents. The in-memory Matchmaker uses this for crash recovery
-- + audit; the in-memory pool does the actual matching.
--
-- Per the duel-matchmaking-rewrite-plan spec §4.3.
-- ============================================================================

CREATE TYPE matchmaking_queue_status AS ENUM (
    'finding',
    'matched',
    'cancelled',
    'expired',
    'abandoned'
);

CREATE TABLE matchmaking_queue (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID         NOT NULL,
    gcid            UUID         NOT NULL,
    status          matchmaking_queue_status NOT NULL DEFAULT 'finding',
    proficiency     INTEGER      NOT NULL DEFAULT 1200,
    interest_tags   TEXT[]       NOT NULL DEFAULT '{}',
    entered_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ  NOT NULL,
    last_heartbeat  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    matched_duel_id UUID,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- One active search per (tenant, gcid) — enforced by partial unique index.
CREATE UNIQUE INDEX matchmaking_queue_finding_unique_idx
    ON matchmaking_queue (tenant_id, gcid)
    WHERE status = 'finding';

CREATE INDEX matchmaking_queue_status_idx
    ON matchmaking_queue (tenant_id, status, expires_at);

-- GIN index for interest_tags array-overlap queries (&& operator).
-- Without it, every match tick triggers a full table scan on TEXT[].
CREATE INDEX matchmaking_queue_tags_idx
    ON matchmaking_queue USING GIN (interest_tags);

ALTER TABLE matchmaking_queue ENABLE ROW LEVEL SECURITY;
ALTER TABLE matchmaking_queue FORCE ROW LEVEL SECURITY;
-- Explicit WITH CHECK for defense-in-depth clarity (FOR ALL implicitly
-- applies USING to new rows, but making it explicit documents intent).
CREATE POLICY matchmaking_queue_tenant_isolation ON matchmaking_queue
    FOR ALL
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid)
    WITH CHECK (tenant_id = current_setting('chora.tenant_id', true)::uuid);

CREATE TRIGGER matchmaking_queue_set_updated_at
    BEFORE UPDATE ON matchmaking_queue
    FOR EACH ROW EXECUTE FUNCTION circle_set_updated_at();
