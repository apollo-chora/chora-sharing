// Package bookmark is the pure-domain core for the Bookmark aggregate.
//
// Aggregate root: Bookmark (gcid, atom_id) — a user's saved atom in their
// personal collection.
// Owning domain : Content Sharing (chora_sharing DB).
// Owning team   : Team 1 (Content).
//
// Per the brief:
//   - A bookmark is a presence record: one per (gcid, atom_id).
//   - Re-bookmarking an already-saved atom is an idempotent no-op.
//   - Unbookmark removes the record.
//   - atom_revision_id is optional (nullable) — a user may bookmark without
//     pinning a specific revision.
//
// Hard rules (per .claude/rules/ddd-enforcement.md):
//   - This file holds the domain only — NO HTTP, NO persistence.
//   - UUIDv7 IDs.
//   - GCIDs travel as opaque cross-domain UUIDs.
package bookmark
import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Errors.
var (
	// ErrInvalidArgument is returned for guard-clause failures.
	ErrInvalidArgument = errors.New("invalid argument")

	// ErrNotFound is returned when a bookmark is not found.
	ErrNotFound = errors.New("bookmark not found")
)

// Bookmark is a single user → atom bookmark edge.
//
// CreatedAt is set on insert and is never mutated. The bookmark is removed
// via Delete rather than soft-deleted because it is a presence record, not
// a content record (mirrors social.Edge semantics).
type Bookmark struct {
	ID             string
	TenantID       string
	GCID           string // owner
	AtomID         string
	AtomRevisionID string // optional — nullable in DB
	CreatedAt      time.Time
}

// NewBookmark constructs a Bookmark with guard-clauses.
//
// tenantID, gcid, and atomID are required (non-empty). atomRevisionID is
// optional — a user may bookmark an atom without pinning a revision.
func NewBookmark(tenantID, gcid, atomID, atomRevisionID string) (*Bookmark, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(gcid) == "" {
		return nil, fmt.Errorf("%w: gcid required", ErrInvalidArgument)
	}
	if strings.TrimSpace(atomID) == "" {
		return nil, fmt.Errorf("%w: atom_id required", ErrInvalidArgument)
	}
	return &Bookmark{
		ID:             NewUUIDv7(),
		TenantID:       tenantID,
		GCID:           gcid,
		AtomID:         atomID,
		AtomRevisionID: strings.TrimSpace(atomRevisionID),
		CreatedAt:      time.Now().UTC(),
	}, nil
}

// BookmarkRepo is the persistence port for the Bookmark aggregate.
type BookmarkRepo interface {
	// Save persists a bookmark. Idempotent on (gcid, atom_id) — a replay is
	// a no-op.
	Save(ctx context.Context, b *Bookmark) error
	// Delete removes the bookmark for (gcid, atom_id). Idempotent — returns
	// nil when the bookmark is absent.
	Delete(ctx context.Context, tenantID, gcid, atomID string) error
	// ListByOwner returns the owner's bookmarks newest-first, keyset-paginated
	// by cursor (last bookmark id). Returns the page + next cursor ("" when
	// no more rows).
	ListByOwner(ctx context.Context, tenantID, gcid, cursor string, limit int) ([]Bookmark, string, error)
}

// NewUUIDv7 — RFC 9562 §5.7 UUIDv7.
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
