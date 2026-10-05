-- =============================================================================
-- chora-sharing : 0044_atom_projection_options.up.sql
--
-- Extends atom_projections with MCQ options + correct_answer columns so duels
-- can select atoms from the projection WITHOUT AI (spec §5.1 Option A).
--
-- The atom.published.v1 event payload now carries these fields; the
-- AtomProjectionSubscriber populates them on upsert.
--
-- Domain  : Content Sharing (5 core)   Database: chora_sharing
-- =============================================================================

ALTER TABLE atom_projections
    ADD COLUMN IF NOT EXISTS options         TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS correct_answer  TEXT    NOT NULL DEFAULT '';
