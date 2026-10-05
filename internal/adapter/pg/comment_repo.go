// comment_repo.go — pgx-backed comment.CommentRepo.
//
// Maps the Comment aggregate to the post_comments table. RLS applied on every
// method via qToExecer(q) before user queries. ListByPost is keyset-paginated
// on the (created_at, id) tuple, newest-first, with the tuple carried in an
// opaque internal/keyset cursor.
//
// Schema: migrations/0002_social.up.sql (post_comments).
package pg

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-sharing/internal/domain/comment"
	"github.com/apollo-chora/chora-sharing/internal/keyset"
)

const (
	sqlCommentInsert = `
INSERT INTO post_comments
    (id, tenant_id, post_id, author_gcid, body, parent_comment_id, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)`

	// sqlCommentParentCheck verifies the parent exists + is itself top-level
	// (1-level reply constraint). Returns the parent's tenant_id (also used
	// to validate the reply's tenant matches).
	sqlCommentParentCheck = `
SELECT tenant_id, parent_comment_id FROM post_comments WHERE id = $1`

	// sqlCommentListByPost is newest-first by created_at, with id as the
	// stable tie-break, keyset-paginated on the (created_at, id) tuple.
	//
	// It orders on created_at, NOT on id. Comment ids are UUIDv7
	// (comment.NewUUIDv7), which only orders to the millisecond: two comments
	// minted inside the same millisecond order by their random bits. The id
	// column also still carries a DEFAULT gen_random_uuid(), which is a v4
	// and carries no time at all. created_at is microsecond-precision and is
	// the field the API returns, so it is the one the page order is built on.
	//
	// $2 (timestamp) and $3 (id) are the two halves of the decoded cursor.
	// Both are empty on the first page. The NULLIFs are load-bearing: an OR
	// is not guaranteed to short-circuit, so ''::timestamptz and ''::uuid
	// would raise on the first page without them.
	sqlCommentListByPost = `
SELECT id, tenant_id, post_id, author_gcid, body, parent_comment_id, created_at
FROM post_comments
WHERE post_id = $1
  AND ($2::text = ''
       OR (created_at, id) < (NULLIF($2::text, '')::timestamptz, NULLIF($3::text, '')::uuid))
ORDER BY created_at DESC, id DESC
LIMIT $4`

	// sqlCommentUpdate updates the body of a comment owned by authorGCID.
	// Only the author can update (WHERE author_gcid = $2). Returns no rows
	// affected when the caller is not the author or the comment doesn't exist.
	sqlCommentUpdate = `
UPDATE post_comments
SET body = $3
WHERE id = $1 AND author_gcid = $2`

	// sqlCommentDelete deletes a comment owned by authorGCID.
	sqlCommentDelete = `
DELETE FROM post_comments
WHERE id = $1 AND author_gcid = $2`
)

// CommentRepo implements comment.CommentRepo against Postgres.
type CommentRepo struct {
	tx TxRunner
}

// NewCommentRepo constructs a CommentRepo bound to a TxRunner.
func NewCommentRepo(tx TxRunner) *CommentRepo { return &CommentRepo{tx: tx} }

// Compile-time port assertion.
var _ comment.CommentRepo = (*CommentRepo)(nil)

// Create inserts a comment. The adapter enforces the 1-level-reply
// constraint: a reply's parent must exist + be itself a top-level comment +
// belong to the same post (mirrors the inmem behavior).
func (r *CommentRepo) Create(ctx context.Context, c *comment.Comment) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if c == nil {
		return comment.ErrInvalidArgument
	}
	created := c.CreatedAt
	if created.IsZero() {
		created = time.Now().UTC()
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		if c.ParentCommentID != "" {
			var (
				parentTenant string
				parentParent *string
			)
			if err := q.QueryRow(ctx, sqlCommentParentCheck, c.ParentCommentID).Scan(
				&parentTenant, &parentParent,
			); err != nil {
				return fmt.Errorf("pg: parent comment %s: %w", c.ParentCommentID, comment.ErrNotFound)
			}
			if parentParent != nil {
				return fmt.Errorf("%w: parent %s is itself a reply (1-level only)", comment.ErrInvalidArgument, c.ParentCommentID)
			}
		}
		if _, err := q.Exec(ctx, sqlCommentInsert,
			c.ID, c.TenantID, c.PostID, c.AuthorGCID, c.Body,
			nullStr(c.ParentCommentID), created,
		); err != nil {
			return fmt.Errorf("pg: insert comment: %w", err)
		}
		return nil
	})
}

// ListByPost is a keyset-paginated read, newest-first by created_at with id
// as the tie-break. The cursor is the opaque internal/keyset token carrying
// the (created_at, id) tuple of the last row on the previous page; an empty
// or unparseable cursor starts at the first page. Returns the comments + the
// next cursor (empty when there are no more pages).
func (r *CommentRepo) ListByPost(ctx context.Context, postID string, limit int, cursor string) ([]comment.Comment, string, error) {
	if r == nil || r.tx == nil {
		return nil, "", ErrNotImplemented
	}
	if limit <= 0 {
		limit = 50
	}
	curTS, curID := keyset.Decode(cursor)
	curAt := ""
	if curID != "" {
		curAt = curTS.UTC().Format(time.RFC3339Nano)
	}
	var out []comment.Comment
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		rows, err := q.Query(ctx, sqlCommentListByPost, postID, curAt, curID, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var (
				c       comment.Comment
				parent  *string
				created time.Time
			)
			if err := rows.Scan(
				&c.ID, &c.TenantID, &c.PostID, &c.AuthorGCID, &c.Body,
				&parent, &created,
			); err != nil {
				return err
			}
			if parent != nil {
				c.ParentCommentID = *parent
			}
			c.CreatedAt = created
			out = append(out, c)
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
		out = []comment.Comment{}
	}
	return out, next, nil
}

// Update updates the body of a comment. Only the author (authorGCID) can
// update — the SQL WHERE clause enforces ownership. Returns comment.ErrNotFound
// when the comment doesn't exist or the caller is not the author.
func (r *CommentRepo) Update(ctx context.Context, commentID, authorGCID, body string) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	trimmed := strings.TrimSpace(body)
	if trimmed == "" {
		return fmt.Errorf("%w: body must be non-empty", comment.ErrInvalidArgument)
	}
	if len([]rune(trimmed)) > comment.MaxBodyLen {
		return fmt.Errorf("%w: body length exceeds max %d", comment.ErrInvalidArgument, comment.MaxBodyLen)
	}
	var rowsAffected int64
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		tag, err := q.Exec(ctx, sqlCommentUpdate, commentID, authorGCID, trimmed)
		if err != nil {
			return fmt.Errorf("pg: update comment: %w", err)
		}
		rowsAffected = tag.RowsAffected
		return nil
	})
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return comment.ErrNotFound
	}
	return nil
}

// Delete deletes a comment. Only the author (authorGCID) can delete — the SQL
// WHERE clause enforces ownership. Returns comment.ErrNotFound when the comment
// doesn't exist or the caller is not the author.
func (r *CommentRepo) Delete(ctx context.Context, commentID, authorGCID string) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	var rowsAffected int64
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		tag, err := q.Exec(ctx, sqlCommentDelete, commentID, authorGCID)
		if err != nil {
			return fmt.Errorf("pg: delete comment: %w", err)
		}
		rowsAffected = tag.RowsAffected
		return nil
	})
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return comment.ErrNotFound
	}
	return nil
}
