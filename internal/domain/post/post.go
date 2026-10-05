// Package post is the pure-domain core for the Post aggregate of the
// Content Sharing domain.
//
// Aggregate root: Post.
// Owning domain : Content Sharing (chora_sharing DB).
// Owning team   : Team 1 (Content).
// Surface       : C+ Connect+ ("curiosity meets connection").
//
// Hard rules (per .claude/rules/ddd-enforcement.md):
//
//   - LearningAtom is a cross-domain aggregate; here we only hold an
//     opaque atom_id (UUID, no FK constraint).
//   - Post uses UUIDv7 (creation-order sortable).
//   - Soft delete only — Post.DeletedAt records moderator/author removal;
//     hard-delete is reserved for crypto-shred at account closure.
//   - This file holds the domain only — NO HTTP, NO persistence.
package post

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"time"
)

// MaxBodyLen caps Post body length. The brief leaves the limit unspecified;
// 8 KiB is a generous social-post ceiling that keeps our in-memory repo
// bounded and matches the Content Sharing AsyncAPI envelope size budget.
const MaxBodyLen = 8 * 1024

// ErrInvalidArgument is returned for guard-clause failures inside NewPost.
var ErrInvalidArgument = errors.New("invalid argument")

// Visibility scopes a Post's audience reach.
//
//   - public  → visible across tenants (atomic-post propagation; Comic Ch5 P11)
//   - tenant  → visible only within the author's tenant
//   - private → visible only to the author
type Visibility string

const (
	VisibilityPublic  Visibility = "public"
	VisibilityTenant  Visibility = "tenant"
	VisibilityPrivate Visibility = "private"
)

// Post is the aggregate root for atomic social posts.
//
// Cross-domain references:
//   - AtomID: opaque UUID of a LearningAtom in chora_creation. Optional
//     — a Post may be free-form (no atom anchor).
//   - AuthorGCID: cross-tenant Global Chora ID (UUIDv7). Source-of-truth
//     in chora_identity; here we only hold the opaque reference.
type Post struct {
	ID         string
	TenantID   string
	AuthorGCID string
	Body       string
	AtomID     string
	Tags       []string
	Visibility Visibility
	PostedAt   time.Time
	UpdatedAt  time.Time
	DeletedAt  *time.Time
}

// NewPost constructs a Post aggregate (defaults Visibility to public).
// Convenience wrapper around NewPostWithVisibility for the common
// default-public case.
func NewPost(tenantID, authorGCID, body, atomID string, tags []string) (*Post, error) {
	return NewPostWithVisibility(tenantID, authorGCID, body, atomID, tags, VisibilityPublic)
}

// NewPostWithVisibility constructs a Post aggregate with an explicit visibility scope.
//
// Validation guards rejected with ErrInvalidArgument:
//   - tenantID required
//   - authorGCID required
//   - body trim-non-empty + within MaxBodyLen
//   - atomID is OPTIONAL (empty string accepted = free-form post)
//   - visibility must be one of public|tenant|private (empty defaults to public)
func NewPostWithVisibility(tenantID, authorGCID, body, atomID string, tags []string, vis Visibility) (*Post, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(authorGCID) == "" {
		return nil, fmt.Errorf("%w: author_gcid required", ErrInvalidArgument)
	}
	if strings.TrimSpace(body) == "" {
		return nil, fmt.Errorf("%w: body required", ErrInvalidArgument)
	}
	if len(body) > MaxBodyLen {
		return nil, fmt.Errorf("%w: body exceeds max length %d", ErrInvalidArgument, MaxBodyLen)
	}
	if vis == "" {
		vis = VisibilityPublic
	}
	if !isValidVisibility(vis) {
		return nil, fmt.Errorf("%w: unknown visibility %q (allowed: public|tenant|private)", ErrInvalidArgument, vis)
	}
	now := time.Now().UTC()
	return &Post{
		ID:         NewUUIDv7(),
		TenantID:   tenantID,
		AuthorGCID: authorGCID,
		Body:       body,
		AtomID:     atomID,
		Tags:       append([]string(nil), tags...), // copy to defeat aliasing
		Visibility: vis,
		PostedAt:   now,
		UpdatedAt:  now,
	}, nil
}

func isValidVisibility(v Visibility) bool {
	switch v {
	case VisibilityPublic, VisibilityTenant, VisibilityPrivate:
		return true
	default:
		return false
	}
}

// VisibleTo reports whether a viewer in (viewerTenant, viewerGCID) is
// permitted to see this Post under its Visibility setting.
//
//   - public  → always visible
//   - tenant  → visible only when viewerTenant == p.TenantID
//   - private → visible only when viewerGCID == p.AuthorGCID
func (p *Post) VisibleTo(viewerTenant, viewerGCID string) bool {
	switch p.Visibility {
	case VisibilityPublic, "":
		return true
	case VisibilityTenant:
		return viewerTenant == p.TenantID
	case VisibilityPrivate:
		return viewerGCID == p.AuthorGCID
	default:
		return false
	}
}

// SoftDelete marks the post deleted via DeletedAt; idempotent.
//
// Per .claude/rules/ddd-enforcement.md §5, soft delete preserves the row
// for audit + reaction history. The aggregate exposes no Hard-delete API.
func (p *Post) SoftDelete() {
	if p.DeletedAt != nil {
		return
	}
	now := time.Now().UTC()
	p.DeletedAt = &now
}

// NewUUIDv7 returns a freshly generated UUIDv7 string.
//
// We generate UUIDv7 ourselves to keep the skeleton dependency-free
// (the brief says the M11.4 chora-contracts package supplies typed
// wrappers later; this in-memory skeleton avoids importing it).
// Format follows RFC 9562 §5.7: unix_ts_ms (48 bits) || ver (4)
// || rand_a (12) || var (2) || rand_b (62).
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
		// rand.Read failure is exceedingly rare; fall back to time bits.
		for i := 6; i < buflen; i++ {
			b[i] = byte(now >> uint(8*(i-6)))
		}
	}
	b[6] = (b[6] & 0x0F) | 0x70
	b[8] = (b[8] & 0x3F) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// -----------------------------------------------------------------------------
// PostRepo — the hexagonal persistence port for the Post aggregate.
//
// This port did not exist until CHO-2193/W0-F1. Post was the only aggregate in
// chora_sharing without one: instead, two consumer-side `PostStore` interfaces
// were declared in the http and grpc adapters, and both were satisfied by a
// concrete in-memory struct whose methods took no ctx and returned no error.
//
// That shape made durability STRUCTURALLY IMPOSSIBLE, not merely absent:
//   - no ctx  ⇒ rls.ApplySession cannot be called ⇒ no RLS-aware pg adapter
//     could ever satisfy it, so the tenant policy on `posts` had nothing to act on
//   - no error ⇒ every adapter is FORCED to swallow (the W0-F5 (T, bool) class)
//   - no port ⇒ nothing to substitute a durable adapter into
//
// So every post the C+ surface accepted was written to a map and died with the
// pod, while a correct, RLS-ready `posts` table sat in the database unused.
//
// Shape follows the house convention (comment.CommentRepo, bookmark.BookmarkRepo,
// atom_share.ShareRepo): ctx first, error last, implemented by the pg adapter in
// production and the inmem double in tests.
type PostRepo interface {
	// Save persists a Post. Idempotent — UPSERT on post_id, so a replay is a
	// no-op rather than a duplicate.
	Save(ctx context.Context, p *Post) error
	// Get returns the post by id. ok=false means ABSENT; a non-nil error means
	// the lookup FAILED and the caller must not treat it as absent.
	Get(ctx context.Context, id string) (*Post, bool, error)
	// ListByTenant returns the tenant's posts (soft-deleted rows excluded) plus
	// the total count.
	ListByTenant(ctx context.Context, tenantID string, offset, limit int) ([]*Post, int, error)
}
