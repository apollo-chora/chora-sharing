-- =============================================================================
-- chora-sharing : 0031_social_blocks_and_projection_reconcile.down.sql
--
-- Reverses 0031. The atom_projections shape reconcile is intentionally NOT
-- reversed — the 0030 shape is authoritative and the table is a rebuildable
-- event-fed cache (see 0030.down.sql). Only the additive pieces roll back.
-- =============================================================================

ALTER TABLE atom_projections DROP CONSTRAINT IF EXISTS atom_projections_reuse_visibility_check;
ALTER TABLE atom_projections DROP COLUMN IF EXISTS reuse_visibility;

DROP TABLE IF EXISTS social_blocks;
