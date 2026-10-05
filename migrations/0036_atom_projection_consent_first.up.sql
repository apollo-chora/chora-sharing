-- =============================================================================
-- chora-sharing : 0036_atom_projection_consent_first.up.sql
--
-- CHO-2174b — atom_projections becomes a CONSENT read-model FIRST.
--
-- THE DEFECT
-- ----------
-- atom_projections welded two unrelated concerns into one row shape:
--
--   CONSENT facts          owner_gcid, reuse_visibility, published-ness.
--                          EVERY published atom has these.
--   QUESTION-DISPLAY facts revision_id, stem, question_type.
--                          ONLY a question-bearing atom has these — and all
--                          three were NOT NULL (revision_id with no default).
--
-- atom_projections is the read-model the ADR-229 consent gate reads
-- (AuthorizeAtomUse / UsableChecker / the WS-2 picker). Because a *question*
-- revision was structurally mandatory, an atom whose CONSENT was perfectly
-- well-defined could not be projected AT ALL when it carried no question
-- revision — so the gate never saw it and refused it forever:
--
--   AuthorizeAtomUse -> AtomProjectionReader.Get -> ErrNotFound
--                    -> FailedPrecondition "atom not published or withdrawn (412)"
--
-- Live (tenant 11111111-1111-7111-8111-111111111111, 2026-07-14): 118 published
-- atoms in chora_creation, 97 projected here, 21 unprojected. Of the 21, 19 are
-- ordinary published atoms carrying question_type (mcq/essay/outline) on the
-- learning_atoms row itself with an atom_revisions row and NO question revision;
-- the remaining 2 are ADR-229 A1 ORPHAN EDITIONS, which are deliberately never
-- projected (they must stay reachable only via a repointed grant — see
-- backfill_published.go and the UsableChecker doc). So the reachable target is
-- 116/118 projected, NOT 118/118.
--
-- This is a MODELLING defect, not a data-quality one. It blocks a live user
-- path: a non-owner cannot convert those atoms into a study list (ADR-233 WS-4
-- made that the primary way atoms enter the daily dose).
--
-- THE CHANGE
-- ----------
-- revision_id / stem / question_type become NULLABLE — question data is OPTIONAL
-- ENRICHMENT. The CONSENT columns stay exactly as they were:
--
--   owner_gcid        UUID NOT NULL                       (unchanged — R1 / `own` leg)
--   reuse_visibility  TEXT NOT NULL DEFAULT 'private'     (unchanged — consent-closed default)
--                     CHECK (reuse_visibility IN ('private','friends','tenant'))
--
-- 🔴 CONSENT NON-WIDENING (the whole risk of this change)
-- ------------------------------------------------------
-- Projecting MORE atoms must NOT widen consent. It does not:
--
--   * reuse_visibility rides chora.creation.atom.published.v1 and is UNCHANGED
--     by this migration: still NOT NULL, still DEFAULT 'private', still CHECK-
--     constrained. The pg layer hardens an EMPTY label to 'private' on INSERT
--     and PRESERVES the existing value on conflict-update, so a newly-projected
--     atom lands CONSENT-CLOSED.
--   * The ADR-229 disjunct — own ∪ (tenant-visible ∧ published) ∪ granted —
--     still gates every read. A newly-projected PRIVATE atom is still REFUSED to
--     a non-owner; it merely becomes VISIBLE TO THE GATE (which can now evaluate
--     it and say no) rather than INVISIBLE (the gate 412'd on a missing row).
--     The refusal is INVARIANT across the change — only WHICH refusal changes.
--   * All 118 live published atoms are reuse_visibility='private', so the
--     widening surface is empty in practice as well as in principle.
--
-- Pinned by tests: usable_checker_consent_nonwidening_test.go (pg) +
-- sharing_server_questionless_test.go (grpc).
--
-- CONSUMERS THAT GENUINELY NEED A QUESTION keep their own precondition and now
-- refuse EXPLICITLY (never silently run on a question-less projection):
--   * ShareAtom              — needs a stem to render a feed card  -> 412, named.
--   * AuthorizeAtomUse       — atom_usage_grants.atom_revision_id is NOT NULL, so
--                              a grant cannot pin a revision that does not exist
--                              -> 412, named (AFTER the consent gate, so a
--                              non-owner is always refused on CONSENT first).
--   * AuthorizeLiveQuizAtoms — needs a question to ask -> unusable, named reason.
-- The gate legs that need only owner + reuse_visibility (UsableChecker, the
-- picker's `own` leg) now resolve for every projected atom.
--
-- Domain  : Content Sharing (5 core)   Database: chora_sharing
--
-- HARD INVARIANTS (CLAUDE.md / ddd-enforcement.md):
--   * Additive/relaxing only — no data is destroyed, no row is deleted.
--   * NO new RLS-bypass surface (exactly 2 ADR-scoped exist: ADR-165 / ADR-184).
--   * RLS (ENABLE + FORCE, GUC chora.tenant_id) is untouched by a column-
--     nullability change; the tenant_isolation policy stays as 0030/0031/0035
--     left it.
--   * Soft-delete preserved — deleted_at remains the closure crypto-shred lane.
--   * Cross-domain refs stay opaque UUID with NO FK (cross-DB forbidden).
-- =============================================================================

BEGIN;

-- revision_id: the pinned chora_creation revision. NULL = the atom carries no
-- question revision (it is revisioned on the atom itself). It is a uuid column,
-- so the Go adapter binds SQL NULL — never '' (which is a live 22P02).
ALTER TABLE atom_projections ALTER COLUMN revision_id DROP NOT NULL;

-- stem / question_type: feed-card + display enrichment. NULL and '' both mean
-- ABSENT; every read COALESCEs them so a NULL can never break a scan.
ALTER TABLE atom_projections ALTER COLUMN stem DROP NOT NULL;
ALTER TABLE atom_projections ALTER COLUMN question_type DROP NOT NULL;

COMMENT ON COLUMN atom_projections.revision_id IS
    'Pinned chora_creation AtomRevision (opaque UUID, no FK). NULLABLE since 0036 (CHO-2174b): '
    'question data is optional enrichment on a consent-first read-model. NULL => the atom carries '
    'no question revision; consumers that must pin a revision (AuthorizeAtomUse) refuse explicitly.';

COMMENT ON COLUMN atom_projections.stem IS
    'Feed-card preview source. NULLABLE since 0036 (CHO-2174b). NULL/'''' => no question stem; '
    'ShareAtom refuses explicitly rather than render an empty card.';

COMMENT ON COLUMN atom_projections.question_type IS
    'chora_creation AtomType label (mcq/oe/essay/outline/...). NULLABLE since 0036 (CHO-2174b).';

COMMENT ON COLUMN atom_projections.reuse_visibility IS
    'ADR-229 D2 author-consent audience (private|friends|tenant). NOT NULL, DEFAULT ''private'' — '
    'DELIBERATELY unchanged by 0036: the consent facts are mandatory; only the QUESTION facts became '
    'optional. Projecting more atoms must never widen consent.';

COMMIT;
