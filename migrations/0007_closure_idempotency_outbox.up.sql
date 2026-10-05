-- ============================================================================
-- chora-sharing : 0007_closure_idempotency_outbox.up.sql
--
-- Spec ref : docs/chora-sharing.md §4.2 (closure_log, idempotency_keys,
--           sharing_outbox_events) + §8.3 atomic outbox
--
-- Three operational tables, NONE of which follow the domain soft-delete
-- invariant (they are operational/platform, not domain content):
--   - closure_log        : RLS-DISABLED (closure subscriber runs with
--                          service-level credentials, §12).
--   - idempotency_keys   : TTL-bounded hard-delete on expiry (operational dedup).
--   - sharing_outbox_events : atomic outbox per libs/chora-go-common/outbox
--                          (Relay drains pending → Pub/Sub; platform worker,
--                          RLS at write-site not on the table).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- closure_log — RLS-DISABLED operational table
--   The closure subscriber runs with service-level credentials, not per-tenant
--   RLS (§12). It tokenises GCIDs across ALL tenants. RLS would block it.
--   GRANT to chora_sharing_app_rw so the subscriber role can write.
-- ----------------------------------------------------------------------------
CREATE TABLE closure_log (
    gcid             UUID         PRIMARY KEY,
    tenant_id        UUID         NOT NULL,
    pseudonymised_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- NO RLS: deliberately disabled for this operational table (§4.2 + §12).
GRANT SELECT, INSERT, UPDATE ON closure_log TO chora_sharing_app_rw;

-- ----------------------------------------------------------------------------
-- idempotency_keys — operational dedup (TTL-bounded)
--   NOT subject to soft-delete invariant: operational table, hard-delete on
--   TTL expiry. Companion cleanup_idempotency_keys() is SECURITY DEFINER so the
--   app role only needs EXECUTE, not direct DELETE.
-- ----------------------------------------------------------------------------
CREATE TABLE idempotency_keys (
    key          TEXT        PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ttl_at       TIMESTAMPTZ NOT NULL,
    result_hash  TEXT
);

CREATE INDEX idempotency_keys_ttl_at_idx ON idempotency_keys (ttl_at);

CREATE OR REPLACE FUNCTION cleanup_idempotency_keys(table_name TEXT DEFAULT 'idempotency_keys')
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    removed_count BIGINT;
BEGIN
    -- Validate table_name is a safe identifier to prevent SQL injection.
    IF table_name !~ '^[a-z_][a-z0-9_]*$' THEN
        RAISE EXCEPTION 'invalid table_name: %', table_name;
    END IF;

    EXECUTE format('DELETE FROM %I WHERE ttl_at <= now()', table_name);
    GET DIAGNOSTICS removed_count = ROW_COUNT;
    RETURN removed_count;
END;
$$;

COMMENT ON FUNCTION cleanup_idempotency_keys(TEXT) IS
'Operational TTL purge for idempotency_keys table. Called from
chora-go-common/idempotent.Store.CleanupExpired() via pg_cron or a
Cloud Run Job (typical: hourly). SECURITY DEFINER so the app role needs
only EXECUTE, not direct DELETE.';

-- Per-domain GRANT (chora_sharing_app_rw). Idempotent via DO block so the
-- migration applies cleanly even before the roles exist on a fresh DB.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_sharing_app_rw') THEN
        GRANT SELECT, INSERT, UPDATE, DELETE ON idempotency_keys TO chora_sharing_app_rw;
        GRANT EXECUTE ON FUNCTION cleanup_idempotency_keys(TEXT) TO chora_sharing_app_rw;
    END IF;
END $$;

-- ----------------------------------------------------------------------------
-- sharing_outbox_events — atomic outbox (state-write + event-publish)
--   Canonical shape per libs/chora-go-common/outbox/sql_fixtures/outbox_events.up.sql,
--   renamed to the sharing service's table. The outbox dispatcher polls this
--   table and publishes to Pub/Sub; ack-after-processing. State-write +
--   event-publish enroll in the same DB transaction (§8.3, §14 checklist).
--
--   RLS is intentionally NOT enabled: the Relay process needs to see ALL
--   tenants' outbox rows (platform-internal worker). Tenant scoping is
--   enforced at the WRITE site via the surrounding domain repository
--   (per the library template's documented rationale).
-- ----------------------------------------------------------------------------
CREATE TABLE sharing_outbox_events (
    -- UUIDv7 — sortable by time, globally unique.
    id              TEXT        PRIMARY KEY,

    -- tenant_id drives multi-tenant isolation indexing (D6.3); NULL for
    -- system/platform events that pre-date a tenant binding.
    tenant_id       UUID,
    gcid            UUID,
    idempotency_key TEXT,

    -- Domain aggregate emitting the event.
    aggregate_type  TEXT        NOT NULL,
    aggregate_id    TEXT        NOT NULL,

    -- Logical event_type matching the topic suffix (e.g. "atom.shared.v1").
    event_type      TEXT        NOT NULL,

    -- Canonical Pub/Sub topic per pub-sub-topology skill
    -- (chora.{domain}.{aggregate}.{event_type}.v{N}).
    topic           TEXT        NOT NULL,

    -- Marshalled Protobuf event body validated by Pub/Sub Schema Registry.
    payload         BYTEA       NOT NULL,

    -- Mandatory chora.common.v1.EventEnvelope, JSONB-serialised (observability).
    envelope        JSONB       NOT NULL,

    -- Domain event time (drives reorder window in the Relay).
    occurred_at     TIMESTAMPTZ NOT NULL,

    -- Lifecycle.
    status          TEXT        NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','published','failed','deadlettered')),

    -- Retry telemetry.
    retry_count     INT         NOT NULL DEFAULT 0,
    last_error      TEXT        NOT NULL DEFAULT '',
    last_attempt_at TIMESTAMPTZ,

    -- Set by the Relay on successful publish.
    published_at    TIMESTAMPTZ,

    -- Provenance for in-domain audit only (NEVER cross-DB JOIN).
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Hot path: Relay scans for pending rows ordered by occurred_at.
CREATE INDEX sharing_outbox_events_pending_idx
    ON sharing_outbox_events (occurred_at ASC)
    WHERE status = 'pending';

-- Aggregate-driven replay queries.
CREATE INDEX sharing_outbox_events_aggregate_idx
    ON sharing_outbox_events (aggregate_type, aggregate_id, occurred_at DESC);

-- Topic monitoring.
CREATE INDEX sharing_outbox_events_topic_idx
    ON sharing_outbox_events (topic, status);

-- D6.3 multi-tenant isolation lookup — dispatcher MAY filter per-tenant.
CREATE INDEX sharing_outbox_events_tenant_idx
    ON sharing_outbox_events (tenant_id, status, occurred_at);

-- Idempotency dedupe — partial unique to allow legacy rows with NULL key.
CREATE UNIQUE INDEX sharing_outbox_events_idempotency_idx
    ON sharing_outbox_events (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

COMMENT ON TABLE sharing_outbox_events IS
'Transactional outbox per data-consistency skill + libs/chora-go-common/outbox.
Domain writes state-change + outbox row in one txn; Relay drains pending rows
to Cloud Pub/Sub. Single writer per service (leader-elected via FOR UPDATE
SKIP LOCKED).';
