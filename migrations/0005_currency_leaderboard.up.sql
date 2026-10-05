-- ============================================================================
-- chora-sharing : 0005_currency_leaderboard.up.sql
--
-- Spec ref : docs/chora-sharing.md §4.2 (currency_balances, atom_xp_credits)
--           + §3.6 leaderboard model (CQRS read-model)
--
-- XP is NOT the canonical source here: chora-consumption owns canonical XP.
-- atom_xp_credits is an append-only projection of
-- chora.consumption.atom_session.completed.v1 for the leaderboard read-model.
-- deleted_at on atom_xp_credits is for closure crypto-shred ONLY (not soft-delete
-- of normal rows — those are append-only and never deleted).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- currency_balances — author non-cash running balance (Reputation/Coins)
--   UNIQUE (tenant_id, holder_gcid, currency) — upsert key.
--   balance >= 0 CHECK (no negative balances).
-- ----------------------------------------------------------------------------
CREATE TABLE currency_balances (
    id            UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     UUID           NOT NULL,
    holder_gcid   UUID           NOT NULL,   -- the author
    currency      currency_code  NOT NULL,
    balance       NUMERIC(18,6)  NOT NULL DEFAULT 0 CHECK (balance >= 0),
    created_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX currency_balances_tenant_holder_currency_unique_idx
    ON currency_balances (tenant_id, holder_gcid, currency);   -- upsert key
CREATE INDEX currency_balances_holder_currency_idx
    ON currency_balances (holder_gcid, currency);

CREATE TRIGGER currency_balances_set_updated_at
    BEFORE UPDATE ON currency_balances
    FOR EACH ROW EXECUTE FUNCTION circle_set_updated_at();

ALTER TABLE currency_balances ENABLE ROW LEVEL SECURITY;
ALTER TABLE currency_balances FORCE  ROW LEVEL SECURITY;
CREATE POLICY currency_balances_tenant_isolation ON currency_balances
    FOR ALL
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- ----------------------------------------------------------------------------
-- atom_xp_credits — append-only leaderboard XP projection
--   UNIQUE (event_id) WHERE deleted_at IS NULL — idempotency: replaying the
--   consumption completed event never double-credits.
--   occurred_at drives the season-window filter (FR-028: query filter, not reset).
-- ----------------------------------------------------------------------------
CREATE TABLE atom_xp_credits (
    id            UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     UUID         NOT NULL,
    gcid          UUID         NOT NULL,
    atom_id       UUID         NOT NULL,
    session_id    UUID         NOT NULL,
    xp            INTEGER      NOT NULL CHECK (xp >= 0),
    event_id      UUID         NOT NULL,   -- chora.consumption.atom_session.completed.v1 event_id
    occurred_at   TIMESTAMPTZ  NOT NULL,   -- season-window filter
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    deleted_at    TIMESTAMPTZ              -- closure crypto-shred only
);

CREATE UNIQUE INDEX atom_xp_credits_event_unique_idx
    ON atom_xp_credits (event_id)
    WHERE deleted_at IS NULL;   -- idempotency: replay never double-credits
CREATE INDEX atom_xp_credits_tenant_occurred_gcid_idx
    ON atom_xp_credits (tenant_id, occurred_at, gcid) WHERE deleted_at IS NULL;
CREATE INDEX atom_xp_credits_gcid_occurred_idx
    ON atom_xp_credits (gcid, occurred_at DESC) WHERE deleted_at IS NULL;

ALTER TABLE atom_xp_credits ENABLE ROW LEVEL SECURITY;
ALTER TABLE atom_xp_credits FORCE  ROW LEVEL SECURITY;
CREATE POLICY atom_xp_credits_tenant_isolation ON atom_xp_credits
    FOR ALL
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);
