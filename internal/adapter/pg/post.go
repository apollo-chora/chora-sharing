// post.go — Postgres adapter for the Post aggregate of the Content
// Sharing domain.
//
// SCHEMA: see migrations/0001_initial.sql (`posts` table).
//
// Resilience-priority directive (`feedback_resilience_priority`):
//
//   - Idempotency: UPSERT on conflict(post_id) DO UPDATE — Save is
//     retry-safe under multi-pod replays and concurrent writers
//   - Multi-user concurrent: every write runs inside a transaction
//     with `SET LOCAL chora.tenant_id` applied first, so under PgBouncer
//     transaction-pooling the tenant context never leaks across sibling
//     requests
//   - Soft-delete-aware: Get / List queries always filter
//     `WHERE deleted_at IS NULL` per ddd-enforcement #6
//   - RLS: every read/write applies rls.ApplySession before the user
//     query so the row-level policy on `posts` enforces tenant isolation
//
// Cross-DB queries forbidden — chora-sharing reads only chora_sharing.
// Inter-domain side effects flow through Pub/Sub via the outbox-backed
// Bus (see ../events/outbox_bus.go).
package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-sharing/internal/domain/post"
)

// ErrInvalidPost is the sentinel for a nil-input write.
var ErrInvalidPost = errors.New("pg: post is nil")

// -----------------------------------------------------------------------------
// SQL templates (exported so CI / Cloud Build lint can grep them)
// -----------------------------------------------------------------------------

// SQLUpsertPost — INSERT … ON CONFLICT DO UPDATE.
//
// Idempotent on post_id. atom_id is bound as nullable (NULLIF empty
// string) because Post.AtomID is optional. tags is bound as TEXT[].
const SQLUpsertPost = `
INSERT INTO posts (
    post_id, tenant_id, author_gcid, body, atom_id,
    tags, visibility, posted_at, updated_at, deleted_at
) VALUES (
    $1, $2, $3, $4, NULLIF($5, '')::uuid,
    $6, $7::post_visibility, $8, $9, $10
)
ON CONFLICT (post_id) DO UPDATE SET
    body         = EXCLUDED.body,
    atom_id      = EXCLUDED.atom_id,
    tags         = EXCLUDED.tags,
    visibility   = EXCLUDED.visibility,
    updated_at   = EXCLUDED.updated_at,
    deleted_at   = EXCLUDED.deleted_at
`

// SQLSelectPostByID returns a single post by id, RLS-scoped + soft-delete-aware.
const SQLSelectPostByID = `
SELECT post_id, tenant_id, author_gcid, body,
       COALESCE(atom_id::text, ''),
       tags, visibility::text,
       posted_at, updated_at, deleted_at
FROM posts
WHERE post_id = $1
  AND deleted_at IS NULL
`

// SQLListPostsByTenant returns active posts for a tenant, paged in
// creation order (UUIDv7-friendly).
const SQLListPostsByTenant = `
SELECT post_id, tenant_id, author_gcid, body,
       COALESCE(atom_id::text, ''),
       tags, visibility::text,
       posted_at, updated_at, deleted_at
FROM posts
WHERE tenant_id = $1
  AND deleted_at IS NULL
ORDER BY posted_at DESC
LIMIT $2 OFFSET $3
`

// -----------------------------------------------------------------------------
// PostRepository
// -----------------------------------------------------------------------------

// PostRepository is the Postgres-backed Post repo.
type PostRepository struct {
	tx TxRunner
}

// NewPostRepository constructs a pg PostRepository around a TxRunner.
func NewPostRepository(tx TxRunner) *PostRepository {
	return &PostRepository{tx: tx}
}

// Save persists a Post aggregate (UPSERT on post_id). Idempotent.
//
// The caller MUST have set tenant_id on ctx via tracing.WithTenantID;
// rls.ApplySession returns ErrNoTenantContext otherwise.
func (r *PostRepository) Save(ctx context.Context, p *post.Post) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if p == nil {
		return ErrInvalidPost
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		visibility := string(p.Visibility)
		if visibility == "" {
			visibility = string(post.VisibilityPublic)
		}
		_, err := q.Exec(ctx, SQLUpsertPost,
			p.ID,
			p.TenantID,
			p.AuthorGCID,
			p.Body,
			p.AtomID,
			tagsToArray(p.Tags),
			visibility,
			p.PostedAt,
			p.UpdatedAt,
			nullTime(p.DeletedAt),
		)
		if err != nil {
			return fmt.Errorf("pg: upsert post: %w", err)
		}
		return nil
	})
}

// Get returns a Post by id, RLS-scoped. Returns (nil, false, nil) when
// no row matches under the tenant context.
func (r *PostRepository) Get(ctx context.Context, postID string) (*post.Post, bool, error) {
	if r == nil || r.tx == nil {
		return nil, false, ErrNotImplemented
	}
	var found *post.Post
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectPostByID, postID)
		p, err := scanPost(row.Scan)
		if err != nil {
			return nil // not-found path → ok=false at outer
		}
		found = p
		return nil
	})
	if err != nil || found == nil {
		return nil, false, err
	}
	return found, true, nil
}

// ListByTenant returns active posts for a tenant, paged. Total reflects
// the page slice length (cheap; an exact total is available via a
// COUNT query that the in-memory adapter never offered either).
func (r *PostRepository) ListByTenant(ctx context.Context, tenantID string, offset, limit int) ([]*post.Post, int, error) {
	if r == nil || r.tx == nil {
		return nil, 0, ErrNotImplemented
	}
	if limit <= 0 {
		limit = 50
	}
	var out []*post.Post
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		rows, err := q.Query(ctx, SQLListPostsByTenant, tenantID, limit, offset)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			p, err := scanPost(rows.Scan)
			if err != nil {
				return err
			}
			out = append(out, p)
		}
		return rows.Err()
	})
	return out, len(out), err
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

// scanPost consumes a row scanner into a fresh Post aggregate.
//
// Column order matches SELECT lists in SQLSelectPostByID + SQLListPostsByTenant
// (10 columns).
func scanPost(scan func(...any) error) (*post.Post, error) {
	var (
		id, tenantID, authorGCID, body, atomID, visibility string
		tags                                                []string
		postedAt, updatedAt                                 time.Time
		deletedAt                                           *time.Time
	)
	if err := scan(
		&id, &tenantID, &authorGCID, &body, &atomID,
		&tags, &visibility,
		&postedAt, &updatedAt, &deletedAt,
	); err != nil {
		return nil, err
	}
	p := &post.Post{
		ID:         id,
		TenantID:   tenantID,
		AuthorGCID: authorGCID,
		Body:       body,
		AtomID:     atomID,
		Tags:       tags,
		Visibility: post.Visibility(visibility),
		PostedAt:   postedAt.UTC(),
		UpdatedAt:  updatedAt.UTC(),
	}
	if deletedAt != nil {
		t := deletedAt.UTC()
		p.DeletedAt = &t
	}
	return p, nil
}

// tagsToArray returns the slice as-is. pgx maps []string to TEXT[]
// natively. nil-safe because pgx admits a nil slice as an empty array.
func tagsToArray(tags []string) any {
	if tags == nil {
		return []string{}
	}
	return tags
}

// nullTime + deref used to live here. They are now in helpers.go, which the
// rest of the pg package grew while this adapter was deleted (470ec0ef9).
// helpers.go's nullTime takes *time.Time directly, so the deref hop is gone.
