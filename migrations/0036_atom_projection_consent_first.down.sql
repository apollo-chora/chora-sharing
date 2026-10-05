-- =============================================================================
-- chora-sharing : 0036_atom_projection_consent_first.down.sql
--
-- Reverses 0036 — restores NOT NULL on revision_id / stem / question_type.
--
-- ⚠ LOSSY BY CONSTRUCTION. 0036 exists precisely so that CONSENT-only rows (a
-- published atom with no question revision) can be projected. Restoring
-- `revision_id NOT NULL` is therefore only possible by REMOVING those rows —
-- there is no value to backfill, and inventing one would fabricate a revision
-- pin that does not exist in chora_creation (forbidden: no fabricated state).
--
-- atom_projections is a rebuildable, event-fed CACHE (0030.down.sql records
-- this): the AtomProjectionSubscriber + the creation-side backfill
-- (POST /api/internal/atoms/backfill-published?reemit_salt=…) repopulate it. The
-- rows deleted here are exactly the ones the pre-0036 code could never have held
-- in the first place, so this returns the table to its pre-0036 population.
--
-- Reversing this migration RE-BREAKS the ADR-229 consent gate for question-less
-- atoms (AuthorizeAtomUse 412s on them again — CHO-2174b). Do NOT run it in prod
-- without accepting that.
--
-- Soft-delete note: this DELETE targets cache rows only — never source-of-truth
-- data, and never a closure tombstone (deleted_at rows are already excluded from
-- every default query and are re-derivable the same way).
-- =============================================================================

BEGIN;

-- stem / question_type: '' is the pre-0036 sentinel for ABSENT (they were
-- NOT NULL DEFAULT ''), so NULLs collapse back to '' losslessly.
UPDATE atom_projections SET stem = '' WHERE stem IS NULL;
UPDATE atom_projections SET question_type = '' WHERE question_type IS NULL;

-- revision_id has NO pre-0036 sentinel (uuid NOT NULL, no default): a
-- consent-only row simply cannot exist under the old shape. Drop those cache
-- rows; the event spine rebuilds whatever the old shape can hold.
DELETE FROM atom_projections WHERE revision_id IS NULL;

ALTER TABLE atom_projections ALTER COLUMN question_type SET NOT NULL;
ALTER TABLE atom_projections ALTER COLUMN stem SET NOT NULL;
ALTER TABLE atom_projections ALTER COLUMN revision_id SET NOT NULL;

COMMIT;
