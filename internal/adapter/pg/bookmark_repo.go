// bookmark_repo.go — pgx-backed bookmark.BookmarkRepo.
//
// Maps the Bookmark aggregate to the atom_bookmarks table. RLS is applied on
// every method via qToExecer(q) before any user query, per multi-tenant-rls
// + the runtime.go contract.
//
// Schema: migrations/0008_atom_bookmarks.up.sql.
package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-sharing/internal/domain/bookmark"
	"github.com/apollo-chora/chora-sharing/internal/keyset"
)

// SQL constants — reviewable, parametrised.
const (
	// sqlBookmarkInsert is idempotent on (gcid, atom_id) via ON CONFLICT
	// DO NOTHING — a replay is a no-op that preserves the original row.
	sqlBookmarkInsert = `
INSERT INTO atom_bookmarks
    (id, tenant_id, gcid, atom_id, atom_revision_id, created_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (gcid, atom_id) DO NOTHING`

	// sqlBookmarkDelete removes the bookmark for (gcid, atom_id). Tenant
	// isolation is enforced by RLS (the policy scopes on tenant_id via the
	// chora.tenant_id session GIC).
	sqlBookmarkDelete = `
DELETE FROM atom_bookmarks
WHERE gcid = $1 AND atom_id = $2`

	// sqlBookmarkListByOwner selects the owner's bookmarks newest-first by
	// created_at, with id as the stable tie-break, keyset-paginated on the
	// (created_at, id) tuple. Tenant isolation is enforced by RLS.
	//
	// It orders on created_at, NOT on id. Bookmark ids are UUIDv7
	// (bookmark.NewUUIDv7), which only orders to the millisecond: two
	// bookmarks minted inside the same millisecond order by their random
	// bits. created_at is microsecond-precision and is the field the API
	// returns, so it is the one the page order is built on. It is also what
	// atom_bookmarks_gcid_created_idx is built on, which the old id ordering
	// could not use.
	//
	// $2 (timestamp) and $3 (id) are the two halves of the decoded cursor.
	// Both are empty on the first page. The NULLIFs are load-bearing: an OR
	// is not guaranteed to short-circuit, so ''::timestamptz and ''::uuid
	// would raise on the first page without them.
	sqlBookmarkListByOwner = `
SELECT id, tenant_id, gcid, atom_id, atom_revision_id, created_at
FROM atom_bookmarks
WHERE gcid = $1::uuid
  AND ($2::text = ''
       OR (created_at, id) < (NULLIF($2::text, '')::timestamptz, NULLIF($3::text, '')::uuid))
ORDER BY created_at DESC, id DESC
LIMIT $4`
)

// BookmarkRepo implements bookmark.BookmarkRepo against Postgres.
type BookmarkRepo struct {
	tx TxRunner
}

// NewBookmarkRepo constructs a BookmarkRepo bound to a TxRunner.
func NewBookmarkRepo(tx TxRunner) *BookmarkRepo { return &BookmarkRepo{tx: tx} }

// Compile-time port assertion.
var _ bookmark.BookmarkRepo = (*BookmarkRepo)(nil)

// Save persists a bookmark. Idempotent on (gcid, atom_id) via ON CONFLICT
// DO NOTHING — a replay is a no-op.
func (r *BookmarkRepo) Save(ctx context.Context, b *bookmark.Bookmark) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if b == nil {
		return bookmark.ErrInvalidArgument
	}
	created := b.CreatedAt
	if created.IsZero() {
		created = time.Now().UTC()
	}
	// atom_revision_id is nullable — pass nil when empty so the UUID column
	// stores NULL rather than failing on an empty-string cast.
	var revID any
	if b.AtomRevisionID != "" {
		revID = b.AtomRevisionID
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		if _, err := q.Exec(ctx, sqlBookmarkInsert,
			b.ID, b.TenantID, b.GCID, b.AtomID, revID, created,
		); err != nil {
			return fmt.Errorf("pg: insert bookmark: %w", err)
		}
		return nil
	})
}

// Delete removes the bookmark for (gcid, atom_id). Tenant isolation is
// enforced by RLS (the tenant_id session GUC is set via ApplySession). The
// tenantID parameter is carried by the context — not referenced in the WHERE
// clause — to match the multi-tenant-rls belt-and-suspenders contract.
func (r *BookmarkRepo) Delete(ctx context.Context, tenantID, gcid, atomID string) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		if _, err := q.Exec(ctx, sqlBookmarkDelete, gcid, atomID); err != nil {
			return fmt.Errorf("pg: delete bookmark: %w", err)
		}
		return nil
	})
}

// ListByOwner returns the owner's bookmarks newest-first by created_at with
// id as the tie-break. The cursor is the opaque internal/keyset token
// carrying the (created_at, id) tuple of the last row on the previous page;
// an empty or unparseable cursor starts at the first page. limit is clamped
// to [1, 100] with default 20.
func (r *BookmarkRepo) ListByOwner(ctx context.Context, tenantID, gcid, cursor string, limit int) ([]bookmark.Bookmark, string, error) {
	if r == nil || r.tx == nil {
		return nil, "", ErrNotImplemented
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	curTS, curID := keyset.Decode(cursor)
	curAt := ""
	if curID != "" {
		curAt = curTS.UTC().Format(time.RFC3339Nano)
	}

	var out []bookmark.Bookmark
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		rows, err := q.Query(ctx, sqlBookmarkListByOwner, gcid, curAt, curID, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var (
				b       bookmark.Bookmark
				revID   *string
				created time.Time
			)
			if err := rows.Scan(&b.ID, &b.TenantID, &b.GCID, &b.AtomID, &revID, &created); err != nil {
				return err
			}
			if revID != nil {
				b.AtomRevisionID = *revID
			}
			b.CreatedAt = created
			out = append(out, b)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) == limit {
		last := out[limit-1]
		next = keyset.Encode(last.CreatedAt, last.ID)
	}
	if out == nil {
		out = []bookmark.Bookmark{}
	}
	return out, next, nil
}
