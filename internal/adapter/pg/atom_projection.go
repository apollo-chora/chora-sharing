// atom_projection.go — Postgres adapter for the cached AtomProjection
// read-model of the Content Sharing domain (Atom Sharing Redesign).
//
// SCHEMA: see migrations/0030_atom_projection.up.sql (`atom_projections`).
//
// AtomProjection is NOT an aggregate — it is a cached, event-fed read-model of
// a LearningAtom owned by chora_creation. Sharing NEVER edits it; the
// AtomProjectionSubscriber upserts/invalidates it from two Content Creation
// events (atom.published.v1 / atom.archived.v1) so the ShareAtom endpoint can
// validate R1 (author-of-record) WITHOUT a cross-DB read of chora_creation
// (cross-DB queries forbidden — events only).
//
// This single store satisfies BOTH domain ports:
//   - atom_projection.AtomProjectionWriter  (Upsert / Invalidate) — subscriber
//   - atom_projection.AtomProjectionReader  (Get)                 — ShareAtom
//
// Resilience-priority (`feedback_resilience_priority`) + multi-tenant-rls:
//   - Idempotency: Upsert is INSERT … ON CONFLICT (atom_id) DO UPDATE — retry-
//     safe under Pub/Sub at-least-once redelivery + concurrent writers.
//   - Soft-delete: Invalidate flips the `archived` flag — NEVER hard-deletes
//     (ddd-enforcement #4). Closure crypto-shred uses `deleted_at` (set by the
//     closure saga, not here). Get filters `archived = false AND
//     deleted_at IS NULL`, honouring the AtomProjectionReader.Get contract
//     ("ErrNotFound when never published OR archived").
//   - RLS: every method calls rls.ApplySession (SET LOCAL chora.tenant_id)
//     before the user query so the row-level policy enforces tenant isolation
//     under PgBouncer transaction-pooling. The Projection value object carries
//     no tenant_id, so the tenant is sourced from ctx (set by the caller via
//     tracing.WithTenantID) — both as the SET LOCAL GUC and the bound column.
//
// Cross-DB queries forbidden — chora-sharing reads only chora_sharing.
package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
)

// -----------------------------------------------------------------------------
// SQL templates (exported so CI / Cloud Build lint can grep them)
// -----------------------------------------------------------------------------

// SQLUpsertAtomProjection caches/refreshes a projection from a published.v1
// event. Idempotent on atom_id. A re-publish of a previously-archived atom
// un-archives it (archived = false) since the event means it is published.
// tenant_id ($2) is bound from ctx — the Projection carries none.
//
// reuse_visibility ($9, ADR-229 WS-1): an EMPTY label means the producing
// event pre-dates ADR-229 — INSERT hardens '' to 'private' (consent-first
// default) while the conflict-UPDATE PRESERVES the existing column value, so
// a re-publish from an old producer never resets an audience applied via
// reuse_visibility_changed.v1. A non-empty label always wins (the publish
// snapshot is authoritative at publish time).
//
// NB: persisted columns are pinned to today's STABLE/core Projection fields.
// Additive contract evolution upstream (e.g. CHO-1967 "answerability" on
// atom.published.v1) is safely ignored — the hand-written subscriber decode
// never surfaces unknown fields, so they never reach this store.
// CONSENT-FIRST (CHO-2174b, migration 0036): revision_id / stem / question_type
// are NULLABLE. The question fields are optional ENRICHMENT, so:
//
//   - revision_id ($3) binds SQL NULL — never "" — when the atom carries no
//     question revision (it is a uuid column; "" is a live 22P02).
//   - the conflict-UPDATE PRESERVES a known question field when the incoming
//     event carries none (COALESCE / NULLIF against the existing row), exactly
//     as reuse_visibility already does. Without this, a re-emit from a producer
//     that cannot resolve the question revision would silently ERASE the pinned
//     revision of an atom that has one, breaking every grant snapshot on it.
//     A NON-EMPTY incoming value always wins (the publish snapshot is
//     authoritative at publish time).
const SQLUpsertAtomProjection = `
INSERT INTO atom_projections (
    atom_id, tenant_id, revision_id, owner_gcid,
    author_display_name, stem, question_type, published_at,
    reuse_visibility, options, correct_answer,
    archived, created_at, updated_at
) VALUES (
    $1, $2, $3, $4,
    $5, $6, $7, $8,
    COALESCE(NULLIF($9, ''), 'private'),
    $10, $11,
    false, now(), now()
)
ON CONFLICT (atom_id) DO UPDATE SET
    revision_id         = COALESCE(EXCLUDED.revision_id, atom_projections.revision_id),
    owner_gcid          = EXCLUDED.owner_gcid,
    author_display_name = COALESCE(NULLIF(EXCLUDED.author_display_name, ''), atom_projections.author_display_name),
    stem                = COALESCE(NULLIF(EXCLUDED.stem, ''), atom_projections.stem),
    question_type       = COALESCE(NULLIF(EXCLUDED.question_type, ''), atom_projections.question_type),
    published_at        = EXCLUDED.published_at,
    reuse_visibility    = CASE WHEN $9 <> '' THEN $9 ELSE atom_projections.reuse_visibility END,
    options             = EXCLUDED.options,
    correct_answer      = EXCLUDED.correct_answer,
    archived            = false,
    updated_at          = now()
`

// SQLSelectAtomProjectionByID resolves the cached projection for R1
// author-validation. RLS-scoped; excludes archived + soft-deleted rows per the
// AtomProjectionReader.Get contract.
//
// COALESCE on the now-nullable question columns (CHO-2174b): a NULL scanned into
// a plain Go string is a hard driver error, which the consent gate would surface
// as a phantom 500 instead of a consent decision. NULL and '' both mean ABSENT.
const SQLSelectAtomProjectionByID = `
SELECT atom_id, COALESCE(revision_id::text, '') AS revision_id, owner_gcid,
       author_display_name, COALESCE(stem, '') AS stem,
       COALESCE(question_type, '') AS question_type, published_at,
       reuse_visibility, COALESCE(options, '{}') AS options, correct_answer
FROM atom_projections
WHERE atom_id = $1
  AND archived = false
  AND deleted_at IS NULL
`

// SQLSetAtomProjectionReuseVisibility applies an author audience change from
// chora.creation.atom.reuse_visibility_changed.v1 (ADR-229 WS-1). RLS-scoped;
// soft-delete filtered. 0 matched rows = the atom was never published (no
// projection row) — NOT an error; the eventual publish event carries the
// current flag. Deliberately does NOT filter on archived: an archived
// projection keeps tracking the author's audience for audit fidelity.
const SQLSetAtomProjectionReuseVisibility = `
UPDATE atom_projections
SET reuse_visibility = $2,
    updated_at = now()
WHERE atom_id = $1
  AND deleted_at IS NULL
`

// SQLInvalidateAtomProjection marks the projection archived (the chora_creation
// "withdrawn" equivalent) on atom.archived.v1. Soft-delete only — NEVER a
// hard DELETE (ddd-enforcement #4). Idempotent.
const SQLInvalidateAtomProjection = `
UPDATE atom_projections
SET archived = true,
    updated_at = now()
WHERE atom_id = $1
  AND deleted_at IS NULL
`

// -----------------------------------------------------------------------------
// AtomProjectionStore
// -----------------------------------------------------------------------------

// AtomProjectionStore is the Postgres-backed read-model store. It satisfies
// both the AtomProjectionWriter (subscriber) and AtomProjectionReader
// (ShareAtom) ports.
type AtomProjectionStore struct {
	tx TxRunner
}

// NewAtomProjectionStore constructs the store around a TxRunner. A nil runner
// (dev-fallback / misuse) makes every method return ErrNotImplemented.
func NewAtomProjectionStore(tx TxRunner) *AtomProjectionStore {
	return &AtomProjectionStore{tx: tx}
}

// Upsert caches/refreshes a projection (AtomProjectionWriter). The caller MUST
// have set tenant_id on ctx via tracing.WithTenantID; rls.ApplySession returns
// ErrNoTenantContext otherwise (fail-loud — never a silent zero-row write).
func (s *AtomProjectionStore) Upsert(ctx context.Context, p atom_projection.Projection) error {
	if s == nil || s.tx == nil {
		return ErrNotImplemented
	}
	if err := p.Validate(); err != nil {
		return fmt.Errorf("pg: atom projection invalid: %w", err)
	}
	return s.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		tenantID := tracing.TenantIDFromContext(ctx)
		_, err := q.Exec(ctx, SQLUpsertAtomProjection,
			p.AtomID,
			tenantID,
			nullStr(p.RevisionID),
			p.OwnerGCID,
			p.AuthorDisplayName,
			p.Stem,
			string(p.QuestionType),
			nullTimeFromUnix(p.PublishedAt),
		p.ReuseVisibility,
		nonNilTags(p.Options),
		p.CorrectAnswer,
		)
		if err != nil {
			return fmt.Errorf("pg: upsert atom projection: %w", err)
		}
		return nil
	})
}

// Invalidate marks the projection archived (AtomProjectionWriter). Soft-delete
// only; idempotent on atom_id. RLS-scoped to the ctx tenant.
func (s *AtomProjectionStore) Invalidate(ctx context.Context, atomID string) error {
	if s == nil || s.tx == nil {
		return ErrNotImplemented
	}
	return s.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		if _, err := q.Exec(ctx, SQLInvalidateAtomProjection, atomID); err != nil {
			return fmt.Errorf("pg: invalidate atom projection: %w", err)
		}
		return nil
	})
}

// SetReuseVisibility applies an author audience change (AtomProjectionWriter,
// ADR-229 WS-1). The label is allowlist-validated BEFORE any SQL so garbage
// fails loud here rather than as an opaque CHECK violation. 0 matched rows is
// fine — the atom was never published (see the SQL const doc). RLS-scoped.
func (s *AtomProjectionStore) SetReuseVisibility(ctx context.Context, atomID, visibility string) error {
	if s == nil || s.tx == nil {
		return ErrNotImplemented
	}
	if !atom_projection.ValidReuseVisibility(visibility) {
		return fmt.Errorf("pg: %w: reuse_visibility %q", atom_projection.ErrInvalidArgument, visibility)
	}
	return s.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		if _, err := q.Exec(ctx, SQLSetAtomProjectionReuseVisibility, atomID, visibility); err != nil {
			return fmt.Errorf("pg: set atom projection reuse_visibility: %w", err)
		}
		return nil
	})
}

// Get resolves the cached projection for atom_id (AtomProjectionReader).
// Returns atom_projection.ErrNotFound when no active row matches under the
// tenant context (never published, or archived/withdrawn). RLS-scoped.
func (s *AtomProjectionStore) Get(ctx context.Context, atomID string) (atom_projection.Projection, error) {
	if s == nil || s.tx == nil {
		return atom_projection.Projection{}, ErrNotImplemented
	}
	var (
		found bool
		out   atom_projection.Projection
	)
	err := s.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectAtomProjectionByID, atomID)
		p, scanErr := scanProjection(row.Scan)
		if scanErr != nil {
			// Only the driver's no-rows signal is the not-found path; any
			// other scan error propagates loud (2026-07-11 regression — a
			// blanket not-found here masks RLS/driver failures as 404s).
			if isNoRows(scanErr) {
				return nil // found stays false → ErrNotFound
			}
			return fmt.Errorf("pg: get atom projection: %w", scanErr)
		}
		out = p
		found = true
		return nil
	})
	if err != nil {
		return atom_projection.Projection{}, err
	}
	if !found {
		return atom_projection.Projection{}, atom_projection.ErrNotFound
	}
	return out, nil
}

// ListRandom returns up to `limit` random published atoms NOT owned by any
// of the excludeGCIDs. Delegates to ProjectionRepo.ListRandom.
func (s *AtomProjectionStore) ListRandom(ctx context.Context, tenantID string, excludeGCIDs []string, limit int) ([]atom_projection.Projection, error) {
	return (*ProjectionRepo)(s).ListRandom(ctx, tenantID, excludeGCIDs, limit)
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

// scanProjection consumes a row scanner into a Projection. Column order matches
// SQLSelectAtomProjectionByID (10 columns). published_at is nullable.
func scanProjection(scan func(...any) error) (atom_projection.Projection, error) {
	var (
		atomID, revisionID, ownerGCID  string
		authorDisplayName, stem, qType string
		publishedAt                    *time.Time
		reuseVisibility                string
		options                        []string
		correctAnswer                  string
	)
	if err := scan(
		&atomID, &revisionID, &ownerGCID,
		&authorDisplayName, &stem, &qType, &publishedAt,
		&reuseVisibility, &options, &correctAnswer,
	); err != nil {
		return atom_projection.Projection{}, err
	}
	p := atom_projection.Projection{
		AtomID:            atomID,
		RevisionID:        revisionID,
		OwnerGCID:         ownerGCID,
		AuthorDisplayName: authorDisplayName,
		Stem:              stem,
		QuestionType:      atom_projection.QuestionType(qType),
		ReuseVisibility:   reuseVisibility,
		Options:           options,
		CorrectAnswer:     correctAnswer,
	}
	if publishedAt != nil {
		p.PublishedAt = publishedAt.UTC()
	}
	return p, nil
}

// Compile-time conformance to BOTH domain ports.
var (
	_ atom_projection.AtomProjectionWriter = (*AtomProjectionStore)(nil)
	_ atom_projection.AtomProjectionReader = (*AtomProjectionStore)(nil)
)
