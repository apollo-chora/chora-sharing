// Package comment is the pure-domain core for the Comment aggregate of the
// Content Sharing domain.
//
// A Comment is a 1-level reply on a Post: a top-level comment has an empty
// ParentCommentID; a reply has a ParentCommentID pointing at a top-level
// comment. Replies to replies are NOT permitted (1-level only) — the domain
// enforces this via NewComment's parent-format check (the parent itself must
// not have its own parent, but since the domain holds no repo, NewComment
// validates only that the parent is a UUID or empty; the adapter enforces the
// 1-level constraint via the parent's own parent_comment_id at insert time).
//
// Aggregate root : Comment.
// Owning domain : Content Sharing (chora_sharing DB).
// Owning team   : Team 1 (Content).
//
// Hard rules (per .claude/rules/ddd-enforcement.md):
//   - This file holds the domain only — NO HTTP, NO persistence.
//   - UUIDv7 IDs.
//   - Cross-domain references (post_id, author_gcid) carry as opaque UUIDs.
package comment

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"regexp"
	"time"
)

// MaxBodyLen caps the comment body. 4 KiB is a generous reply ceiling that
// keeps the post_comments table + indexes bounded.
const MaxBodyLen = 4 * 1024

// ErrInvalidArgument is returned for guard-clause failures inside NewComment.
var ErrInvalidArgument = errors.New("invalid argument")

// uuidRe matches a canonical 8-4-4-4-12 lowercase hex UUID (the format our
// NewUUIDv7 produces). Used to validate that ParentCommentID, when non-empty,
// is a well-formed UUID reference (not arbitrary string). Case-insensitive.
var uuidRe = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// Comment is the aggregate root for a 1-level reply on a Post.
//
// Cross-domain references (all opaque UUID, no FK):
//   - PostID     → chora_sharing.social_feed_entries.id (same DB, FK allowed
//     but the domain treats it as opaque)
//   - AuthorGCID → chora_identity.Account
//
// ParentCommentID is empty for a top-level comment; when set, it points at
// another Comment that is itself a top-level comment (1-level reply only).
type Comment struct {
	ID              string    `json:"id"`
	TenantID        string    `json:"tenant_id"`
	PostID          string    `json:"post_id"`
	AuthorGCID      string    `json:"author_gcid"`
	Body            string    `json:"body"`
	ParentCommentID string    `json:"parent_comment_id,omitempty"` // 1-level reply
	CreatedAt       time.Time `json:"created_at"`
}

// NewComment constructs a Comment, validating the 1-level-reply constraint at
// the domain level (parent format check). The adapter enforces the deeper
// constraint — that the referenced parent is itself a top-level comment (has
// no own parent) — at insert time via the parent's parent_comment_id.
//
// Validation guards rejected with ErrInvalidArgument:
//   - tenantID, postID, authorGCID required
//   - body must be non-empty (after trim) and ≤ MaxBodyLen
//   - parentCommentID is OPTIONAL: empty = top-level comment; when set, must
//     be a well-formed UUID (the 1-level constraint — only a UUID-shaped
//     parent is accepted; the adapter verifies the parent exists + is itself
//     top-level).
func NewComment(tenantID, postID, authorGCID, body, parentCommentID string) (*Comment, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if postID == "" {
		return nil, fmt.Errorf("%w: post_id required", ErrInvalidArgument)
	}
	if authorGCID == "" {
		return nil, fmt.Errorf("%w: author_gcid required", ErrInvalidArgument)
	}
	trimmed := trimSpace(body)
	if trimmed == "" {
		return nil, fmt.Errorf("%w: body must be non-empty", ErrInvalidArgument)
	}
	if len([]rune(trimmed)) > MaxBodyLen {
		return nil, fmt.Errorf("%w: body length %d exceeds max %d", ErrInvalidArgument, len([]rune(trimmed)), MaxBodyLen)
	}
	if parentCommentID != "" {
		if !uuidRe.MatchString(parentCommentID) {
			return nil, fmt.Errorf("%w: parent_comment_id %q is not a valid UUID", ErrInvalidArgument, parentCommentID)
		}
	}
	return &Comment{
		ID:              NewUUIDv7(),
		TenantID:        tenantID,
		PostID:          postID,
		AuthorGCID:      authorGCID,
		Body:            trimmed,
		ParentCommentID: parentCommentID,
		CreatedAt:       time.Now().UTC(),
	}, nil
}

// IsReply reports whether this comment is a reply (has a parent) vs a
// top-level comment.
func (c *Comment) IsReply() bool {
	return c.ParentCommentID != ""
}

// trimSpace trims leading + trailing ASCII whitespace (space, tab, newline,
// carriage return) without importing the strings package — keeps the domain
// minimal. The body is normalised once at construction.
func trimSpace(s string) string {
	start := 0
	for start < len(s) {
		c := s[start]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			start++
			continue
		}
		break
	}
	end := len(s)
	for end > start {
		c := s[end-1]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			end--
			continue
		}
		break
	}
	return s[start:end]
}

// CommentRepo is the hexagonal port the comment handlers use. The pg adapter
// implements it with RLS-aware pgx; the inmem double for tests. ListByPost is
// a keyset-paginated read (cursor = last comment id, newest-first).
type CommentRepo interface {
	Create(ctx context.Context, c *Comment) error
	ListByPost(ctx context.Context, postID string, limit int, cursor string) ([]Comment, string, error)
	Update(ctx context.Context, commentID, authorGCID, body string) error
	Delete(ctx context.Context, commentID, authorGCID string) error
}

// ErrNotFound is returned by CommentRepo lookups when the comment is missing.
var ErrNotFound = errors.New("comment not found")

// NewUUIDv7 — RFC 9562 §5.7 UUIDv7. See post.NewUUIDv7 for rationale.
func NewUUIDv7() string {
	const buflen = 16
	var b [buflen]byte
	now := uint64(time.Now().UnixMilli())
	b[0] = byte(now >> 40)
	b[1] = byte(now >> 32)
	b[2] = byte(now >> 24)
	b[3] = byte(now >> 16)
	b[4] = byte(now >> 8)
	b[5] = byte(now)
	if _, err := rand.Read(b[6:]); err != nil {
		for i := 6; i < buflen; i++ {
			b[i] = byte(now >> uint(8*(i-6)))
		}
	}
	b[6] = (b[6] & 0x0F) | 0x70
	b[8] = (b[8] & 0x3F) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
