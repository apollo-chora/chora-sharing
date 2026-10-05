-- 0033_outbox_aggregate_columns_reconcile.down.sql
--
-- Reverts the outbox aggregate-column reconcile. Safe: both columns are
-- write-only provenance (no reader SELECTs them) and default-filled.

DROP INDEX IF EXISTS sharing_outbox_events_aggregate_idx;

ALTER TABLE sharing_outbox_events
    DROP COLUMN IF EXISTS aggregate_type,
    DROP COLUMN IF EXISTS aggregate_id;
