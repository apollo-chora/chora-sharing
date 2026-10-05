// projection_repo.go — pgx-backed atom_projection.AtomProjectionReader +
// AtomProjectionWriter.
//
// The cached LearningAtom read-model (NOT an aggregate) — event-fed by
// chora.creation.atom.published.v1 (upsert) / archived.v1 (invalidate). RLS
// applied on every method via qToExecer(q) before user queries.
//
// Schema: migrations/0006_projections.up.sql (atom_projections).
package pg

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
)

const (
	// sqlProjectionGet — ADR-229 WS-2 (CHO-2133): also reads
	// reuse_visibility (mig 0031; NOT NULL DEFAULT 'private') so
	// AuthorizeAtomUse can resolve the audience-based D2 audit grant.
	// COALESCE on the now-nullable question columns (CHO-2174b, mig 0036): this
	// is the READ path of the ADR-229 consent gate (AuthorizeAtomUse /
	// UsableChecker / picker). A NULL revision_id scanned into a plain string is
	// a hard driver error — the gate would surface it as a phantom 500 instead
	// of a consent decision. NULL and '' both mean ABSENT.
	// mig 0040: options + correct_answer for AI-free duel MCQ (spec §5.1 A).
	sqlProjectionGet = `
SELECT atom_id, tenant_id, COALESCE(revision_id::text, '') AS revision_id,
       owner_gcid, author_display_name, COALESCE(stem, '') AS stem,
       COALESCE(question_type, '') AS question_type, published_at, archived,
       reuse_visibility, options, correct_answer
FROM atom_projections
WHERE atom_id = $1 AND archived = FALSE`

	// sqlProjectionUpsert caches (or refreshes) a projection from a
	// published.v1 event. Idempotent on atom_id (UNIQUE index).
	//
	// CHO-2174b: an absent revision binds SQL NULL (never "" — uuid column), and
	// the conflict-UPDATE PRESERVES known question fields when the incoming event
	// carries none, so a re-emit can never erase a pinned revision. This repo
	// deliberately does NOT write reuse_visibility: the column DEFAULTs to
	// 'private' on INSERT and is left untouched on UPDATE — consent-closed either
	// way, so this path can never widen an audience.
	sqlProjectionUpsert = `
INSERT INTO atom_projections
    (atom_id, tenant_id, revision_id, owner_gcid, author_display_name, stem,
     question_type, published_at, archived, options, correct_answer)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, FALSE, $9, $10)
ON CONFLICT (atom_id) DO UPDATE SET
    tenant_id            = EXCLUDED.tenant_id,
    revision_id          = COALESCE(EXCLUDED.revision_id, atom_projections.revision_id),
    owner_gcid           = EXCLUDED.owner_gcid,
    author_display_name  = EXCLUDED.author_display_name,
    stem                 = COALESCE(NULLIF(EXCLUDED.stem, ''), atom_projections.stem),
    question_type        = COALESCE(NULLIF(EXCLUDED.question_type, ''), atom_projections.question_type),
    published_at         = EXCLUDED.published_at,
    archived             = FALSE,
    options              = EXCLUDED.options,
    correct_answer       = EXCLUDED.correct_answer`

	// sqlProjectionInvalidate marks the projection archived. Idempotent.
	sqlProjectionInvalidate = `
UPDATE atom_projections SET archived = TRUE WHERE atom_id = $1`

	// sqlProjectionSetReuseVisibility applies an ADR-229 WS-1 audience change
	// (reuse_visibility_changed.v1). 0 rows = never published; not an error.
	sqlProjectionSetReuseVisibility = `
UPDATE atom_projections SET reuse_visibility = $2 WHERE atom_id = $1`
)

// ProjectionRepo implements both atom_projection.AtomProjectionReader and
// atom_projection.AtomProjectionWriter against Postgres.
type ProjectionRepo struct {
	tx TxRunner
}

// NewProjectionRepo constructs a ProjectionRepo bound to a TxRunner.
func NewProjectionRepo(tx TxRunner) *ProjectionRepo { return &ProjectionRepo{tx: tx} }

// Compile-time port assertions.
var (
	_ atom_projection.AtomProjectionReader = (*ProjectionRepo)(nil)
	_ atom_projection.AtomProjectionWriter = (*ProjectionRepo)(nil)
)

// Get returns the cached projection for atom_id, or ErrNotFound when the atom
// has never been published (or has been archived).
//
// Live-walk regression (2026-07-11, CHO-2133): published_at is NULLABLE in
// practice — the atom-projection subscriber never sets it — so it scans via
// *time.Time; and ONLY the no-rows case maps to ErrNotFound. Any other scan/
// driver error propagates loud (the old blanket ErrNotFound turned a scan
// failure into a phantom 412 at AuthorizeAtomUse).
func (r *ProjectionRepo) Get(ctx context.Context, atomID string) (atom_projection.Projection, error) {
	if r == nil || r.tx == nil {
		return atom_projection.Projection{}, ErrNotImplemented
	}
	var p atom_projection.Projection
	var qType string
	var publishedAt *time.Time
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		if err := q.QueryRow(ctx, sqlProjectionGet, atomID).Scan(
			&p.AtomID, &p.TenantID, &p.RevisionID, &p.OwnerGCID, &p.AuthorDisplayName,
			&p.Stem, &qType, &publishedAt, &p.Archived, &p.ReuseVisibility,
			&p.Options, &p.CorrectAnswer,
		); err != nil {
			if isNoRows(err) {
				return atom_projection.ErrNotFound
			}
			return fmt.Errorf("pg: get projection: %w", err)
		}
		return nil
	})
	if err != nil {
		return atom_projection.Projection{}, err
	}
	if publishedAt != nil {
		p.PublishedAt = publishedAt.UTC()
	}
	p.QuestionType = atom_projection.QuestionType(qType)
	return p, nil
}

// isNoRows reports whether err is the driver's empty-result signal. The pg
// package's Querier abstraction hides the concrete driver, so this matches
// the pgx/database-sql message shape ("no rows in result set" / "sql: no
// rows") rather than a typed sentinel.
func isNoRows(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "no rows")
}

// Upsert caches (or refreshes) a projection from a published.v1 event.
// Idempotent on atom_id — re-publishing overwrites with the latest revision.
func (r *ProjectionRepo) Upsert(ctx context.Context, p atom_projection.Projection) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		if _, err := q.Exec(ctx, sqlProjectionUpsert,
			p.AtomID, p.TenantID, nullStr(p.RevisionID), p.OwnerGCID, p.AuthorDisplayName,
			p.Stem, string(p.QuestionType), nullTimeFromUnix(p.PublishedAt),
			p.Options, p.CorrectAnswer,
		); err != nil {
			return fmt.Errorf("pg: upsert projection: %w", err)
		}
		return nil
	})
}

// Invalidate marks the projection archived on atom.archived.v1. Idempotent.
func (r *ProjectionRepo) Invalidate(ctx context.Context, atomID string) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		if _, err := q.Exec(ctx, sqlProjectionInvalidate, atomID); err != nil {
			return fmt.Errorf("pg: invalidate projection: %w", err)
		}
		return nil
	})
}

// SetReuseVisibility applies an ADR-229 WS-1 audience change onto the cached
// projection (reuse_visibility_changed.v1). Allowlist-validated before SQL;
// 0 matched rows is fine (the atom was never published — the eventual publish
// event carries the current flag). RLS-scoped.
func (r *ProjectionRepo) SetReuseVisibility(ctx context.Context, atomID, visibility string) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if !atom_projection.ValidReuseVisibility(visibility) {
		return fmt.Errorf("pg: %w: reuse_visibility %q", atom_projection.ErrInvalidArgument, visibility)
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		if _, err := q.Exec(ctx, sqlProjectionSetReuseVisibility, atomID, visibility); err != nil {
			return fmt.Errorf("pg: set projection reuse_visibility: %w", err)
		}
		return nil
	})
}

var sqlProjectionListRandom = `
SELECT atom_id, revision_id, owner_gcid,
       author_display_name, stem, question_type, published_at,
       archived, reuse_visibility, options, correct_answer
FROM atom_projections
WHERE tenant_id = $1
  AND published_at IS NOT NULL
  AND archived = false
  AND deleted_at IS NULL
  AND owner_gcid <> ALL($2::uuid[])
ORDER BY RANDOM()
LIMIT $3`

func (r *ProjectionRepo) ListRandom(ctx context.Context, tenantID string, excludeGCIDs []string, limit int) ([]atom_projection.Projection, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if limit <= 0 {
		limit = 5
	}
	if len(excludeGCIDs) == 0 {
		excludeGCIDs = []string{""}
	}
	var results []atom_projection.Projection
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		rows, err := q.Query(ctx, sqlProjectionListRandom, tenantID, excludeGCIDs, limit)
		if err != nil {
			return fmt.Errorf("pg: list random projections: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var p atom_projection.Projection
			var qType string
			var publishedAt *time.Time
			if err := rows.Scan(&p.AtomID, &p.RevisionID, &p.OwnerGCID, &p.AuthorDisplayName, &p.Stem, &qType, &publishedAt, &p.Archived, &p.ReuseVisibility, &p.Options, &p.CorrectAnswer); err != nil {
				return fmt.Errorf("pg: scan random projection: %w", err)
			}
			if publishedAt != nil {
				p.PublishedAt = publishedAt.UTC()
			}
			p.QuestionType = atom_projection.QuestionType(qType)
			results = append(results, p)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}
