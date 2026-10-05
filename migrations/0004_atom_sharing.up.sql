-- ============================================================================
-- chora-sharing : 0004_atom_sharing.up.sql
--
-- Spec ref : docs/chora-sharing.md §4.2 (atom_share_events, atom_usage_grants,
--           grant_events, royalty_settlements) + §3.2 (R1+R2) + §3.3 (license)
--
-- R1 (author stays the original) enforced at the contract boundary, but the
-- schema records owner_gcid on grants for provenance.
-- R2 (license frozen at grant time) — license_terms_snapshot +
-- royalty_rate_snapshot are FROZEN columns on atom_usage_grants.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- atom_share_events — append-only audit child of the immutable share Post
--   feed_entry_id → social_feed_entries.id (same-DB FK allowed).
-- ----------------------------------------------------------------------------
CREATE TABLE atom_share_events (
    id              UUID                   PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID                   NOT NULL,
    feed_entry_id   UUID                   NOT NULL REFERENCES social_feed_entries(id) ON DELETE CASCADE,
    actor_gcid      UUID                   NOT NULL,
    event_type      atom_share_event_type  NOT NULL,
    payload         JSONB                  NOT NULL DEFAULT '{}',  -- for price_changed: new terms snapshot
    source_event_id UUID,                                            -- idempotent upstream event_id
    created_at      TIMESTAMPTZ           NOT NULL DEFAULT now()
);

CREATE INDEX atom_share_events_entry_created_idx
    ON atom_share_events (feed_entry_id, created_at DESC);
CREATE INDEX atom_share_events_actor_tenant_idx
    ON atom_share_events (actor_gcid, tenant_id);
-- idempotency: replaying an upstream event never double-appends
CREATE UNIQUE INDEX atom_share_events_source_event_unique_idx
    ON atom_share_events (source_event_id)
    WHERE source_event_id IS NOT NULL;

ALTER TABLE atom_share_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE atom_share_events FORCE  ROW LEVEL SECURITY;
CREATE POLICY atom_share_events_tenant_isolation ON atom_share_events
    FOR ALL
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- ----------------------------------------------------------------------------
-- atom_usage_grants — the licensing record (the new aggregate root)
--   tenant_id = the REUSER's tenant (pays royalty).
--   license_terms_snapshot + royalty_rate_snapshot FROZEN at grant time (R2).
--   UNIQUE active per (grantee, atom, scope) — Authorize idempotency.
-- ----------------------------------------------------------------------------
CREATE TABLE atom_usage_grants (
    id                      UUID                PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id               UUID                NOT NULL,   -- the REUSER's tenant (pays royalty)
    source_share_entry      UUID                REFERENCES social_feed_entries(id),  -- provenance (nullable for own-atom)
    owner_gcid              UUID                NOT NULL,   -- the AUTHOR (R1)
    atom_id                 UUID                NOT NULL,   -- cross-DB ref, no FK
    atom_revision_id        UUID                NOT NULL,   -- pinned at grant time
    grantee_gcid            UUID                NOT NULL,   -- the reuser
    scope                   atom_grant_scope     NOT NULL,
    license_terms_snapshot  atom_license_terms  NOT NULL,   -- FROZEN
    royalty_rate_snapshot   JSONB               NOT NULL DEFAULT '{}',  -- FROZEN
    status                  atom_grant_status   NOT NULL DEFAULT 'active',
    granted_at              TIMESTAMPTZ        NOT NULL DEFAULT now(),
    revoked_at              TIMESTAMPTZ,
    expires_at              TIMESTAMPTZ,                    -- NULL = no expiry
    created_at              TIMESTAMPTZ        NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ        NOT NULL DEFAULT now(),
    deleted_at              TIMESTAMPTZ                    -- soft delete
);

-- idempotency: one active grant per (grantee, atom, scope)
CREATE UNIQUE INDEX atom_usage_grants_active_unique_idx
    ON atom_usage_grants (grantee_gcid, atom_id, scope)
    WHERE status = 'active' AND deleted_at IS NULL;

CREATE INDEX atom_usage_grants_owner_tenant_idx
    ON atom_usage_grants (owner_gcid, tenant_id) WHERE deleted_at IS NULL;
CREATE INDEX atom_usage_grants_grantee_atom_status_idx
    ON atom_usage_grants (grantee_gcid, atom_id, status) WHERE deleted_at IS NULL;
CREATE INDEX atom_usage_grants_atom_status_idx
    ON atom_usage_grants (atom_id, status) WHERE deleted_at IS NULL;
-- expiry sweep: background job finds active grants past expires_at
CREATE INDEX atom_usage_grants_expiry_sweep_idx
    ON atom_usage_grants (expires_at)
    WHERE status = 'active' AND expires_at IS NOT NULL AND deleted_at IS NULL;

CREATE TRIGGER atom_usage_grants_set_updated_at
    BEFORE UPDATE ON atom_usage_grants
    FOR EACH ROW EXECUTE FUNCTION circle_set_updated_at();

ALTER TABLE atom_usage_grants ENABLE ROW LEVEL SECURITY;
ALTER TABLE atom_usage_grants FORCE  ROW LEVEL SECURITY;
CREATE POLICY atom_usage_grants_tenant_isolation ON atom_usage_grants
    FOR ALL
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- ----------------------------------------------------------------------------
-- grant_events — append-only lifecycle log
--   grant_id → atom_usage_grants.id (same-DB FK allowed).
-- ----------------------------------------------------------------------------
CREATE TABLE grant_events (
    id              UUID                PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID                NOT NULL,
    grant_id        UUID                NOT NULL REFERENCES atom_usage_grants(id) ON DELETE CASCADE,
    actor_gcid      UUID                NOT NULL,
    event_type      TEXT                NOT NULL CHECK (event_type IN ('granted', 'revoked', 'expired')),
    reason          TEXT                NOT NULL DEFAULT '',
    source_event_id UUID,
    created_at      TIMESTAMPTZ        NOT NULL DEFAULT now()
);

CREATE INDEX grant_events_grant_created_idx ON grant_events (grant_id, created_at DESC);
CREATE INDEX grant_events_actor_tenant_idx  ON grant_events (actor_gcid, tenant_id);
CREATE UNIQUE INDEX grant_events_source_event_unique_idx
    ON grant_events (source_event_id)
    WHERE source_event_id IS NOT NULL;

ALTER TABLE grant_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE grant_events FORCE  ROW LEVEL SECURITY;
CREATE POLICY grant_events_tenant_isolation ON grant_events
    FOR ALL
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- ----------------------------------------------------------------------------
-- royalty_settlements — double-entry accrual
--   grant_id → atom_usage_grants.id (same-DB FK allowed).
--   UNIQUE (source_event_id) — replay never double-credits.
-- ----------------------------------------------------------------------------
CREATE TABLE royalty_settlements (
    id                UUID                  PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id         UUID                  NOT NULL,   -- debit side (reuser tenant)
    grant_id          UUID                  NOT NULL REFERENCES atom_usage_grants(id) ON DELETE CASCADE,
    owner_gcid        UUID                  NOT NULL,   -- credit (the author)
    grantee_tenant_id UUID                  NOT NULL,   -- debit
    atom_id           UUID                  NOT NULL,
    amount            NUMERIC(18,6)         NOT NULL CHECK (amount >= 0),
    currency          royalty_currency      NOT NULL DEFAULT 'mana',
    usage_context     royalty_usage_context NOT NULL,
    source_event_id   UUID                  NOT NULL,   -- idempotency (triggering event)
    created_at        TIMESTAMPTZ          NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX royalty_settlements_source_event_unique_idx
    ON royalty_settlements (source_event_id);   -- idempotency
CREATE INDEX royalty_settlements_grant_created_idx
    ON royalty_settlements (grant_id, created_at DESC);
CREATE INDEX royalty_settlements_owner_tenant_created_idx
    ON royalty_settlements (owner_gcid, tenant_id, created_at DESC);
CREATE INDEX royalty_settlements_atom_created_idx
    ON royalty_settlements (atom_id, created_at DESC);

ALTER TABLE royalty_settlements ENABLE ROW LEVEL SECURITY;
ALTER TABLE royalty_settlements FORCE  ROW LEVEL SECURITY;
CREATE POLICY royalty_settlements_tenant_isolation ON royalty_settlements
    FOR ALL
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);
