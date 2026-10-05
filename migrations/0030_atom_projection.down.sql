-- =============================================================================
-- chora-sharing : 0030_atom_projection.down.sql
--
-- Reverses 0030_atom_projection.up.sql. DROP TABLE cascades the table's two
-- indexes + the tenant_isolation RLS policy.
--
-- atom_projections holds ONLY a cached, event-fed read-model (no source-of-
-- truth data) — dropping it loses the cache, which the AtomProjectionSubscriber
-- rebuilds from chora.creation.atom.published.v1 events going forward (a
-- creation-side re-publish / backfill repopulates historical atoms). Safe to
-- reverse, but NEVER run in prod without confirming the subscriber + backfill
-- plan first.
-- =============================================================================

DROP TABLE IF EXISTS atom_projections;
