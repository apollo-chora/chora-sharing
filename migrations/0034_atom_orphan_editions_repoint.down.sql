-- ============================================================================
-- chora-sharing : 0034_atom_orphan_editions_repoint.down.sql
--
-- Reverts the ADR-229 A1 sharing-side orphan machinery. FAIL-LOUD rollback:
-- restoring the narrower grant_events CHECK will REFUSE if 'repointed' rows
-- exist (Postgres validates existing rows) — that is deliberate. A rollback
-- with live repoint history is lossy and must be a conscious operator
-- decision (soft-delete-only invariant forbids purging the trail here).
-- ============================================================================
BEGIN;

ALTER TABLE grant_events
    DROP CONSTRAINT IF EXISTS grant_events_event_type_check;
ALTER TABLE grant_events
    ADD CONSTRAINT grant_events_event_type_check
    CHECK (event_type IN ('granted', 'revoked', 'expired'));

DROP TABLE IF EXISTS atom_orphan_editions;

COMMIT;
