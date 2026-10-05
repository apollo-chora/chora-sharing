-- 0033_outbox_aggregate_columns_reconcile.up.sql
--
-- Reconcile the LIVE sharing_outbox_events shape with migration 0007's
-- canonical definition (ADR-230 B-lite deploy, CHO-2121 PREPARE-smoke catch).
--
-- Deployed reality: the live table was created by an earlier variant of the
-- outbox migration WITHOUT aggregate_type / aggregate_id (and without the
-- aggregate replay index), while 0007 in the repo — and BOTH writer lanes
-- (outbox.PostgresStore.Insert + pg.relationshipTx.Enqueue) — expect them.
-- Neither writer has ever fired in production (the direct-publisher lane is
-- deliberately discarded; the relationship lane ships with this deploy), so
-- the columns can be added forward with a safe default. Never edit an
-- applied migration — this forward migration closes the drift.

ALTER TABLE sharing_outbox_events
    ADD COLUMN IF NOT EXISTS aggregate_type TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS aggregate_id   TEXT NOT NULL DEFAULT '';

-- Aggregate-driven replay queries (0007's intended index).
CREATE INDEX IF NOT EXISTS sharing_outbox_events_aggregate_idx
    ON sharing_outbox_events (aggregate_type, aggregate_id, occurred_at DESC);
