package inmem

import (
	"context"
	"sort"
	"sync"

	"github.com/apollo-chora/chora-sharing/internal/domain/bookmark"
	"github.com/apollo-chora/chora-sharing/internal/keyset"
)

// BookmarkRepo is an in-memory adapter implementing bookmark.BookmarkRepo.
// Bookmarks are keyed by gcid+"|"+atomID; Save is idempotent on that key.
type BookmarkRepo struct {
	mu    sync.Mutex
	items map[string]*bookmark.Bookmark
}

// NewBookmarkRepo returns an empty BookmarkRepo.
func NewBookmarkRepo() *BookmarkRepo {
	return &BookmarkRepo{items: make(map[string]*bookmark.Bookmark)}
}

// Compile-time port assertion.
var _ bookmark.BookmarkRepo = (*BookmarkRepo)(nil)

func bookmarkKey(gcid, atomID string) string { return gcid + "|" + atomID }

// Save persists a bookmark. Idempotent on (gcid, atom_id) — a replay leaves
// the original bookmark unchanged (preserves original CreatedAt + ID).
func (r *BookmarkRepo) Save(_ context.Context, b *bookmark.Bookmark) error {
	if b == nil {
		return bookmark.ErrInvalidArgument
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := bookmarkKey(b.GCID, b.AtomID)
	if _, exists := r.items[key]; exists {
		return nil // idempotent — keep original
	}
	r.items[key] = b
	return nil
}

// Delete removes the bookmark for (gcid, atom_id). Idempotent — returns nil
// when the bookmark is absent.
func (r *BookmarkRepo) Delete(_ context.Context, _, gcid, atomID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.items, bookmarkKey(gcid, atomID))
	return nil
}

// ListByOwner returns the owner's bookmarks newest-first by created_at with
// id as the tie-break, mirroring the pg adapter. The cursor is the same
// opaque internal/keyset token; an empty or unparseable cursor starts at the
// first page. limit is clamped to [1, 100] with default 20.
func (r *BookmarkRepo) ListByOwner(_ context.Context, tenantID, gcid, cursor string, limit int) ([]bookmark.Bookmark, string, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	r.mu.Lock()
	out := make([]bookmark.Bookmark, 0, len(r.items))
	for _, b := range r.items {
		if b.TenantID != tenantID || b.GCID != gcid {
			continue
		}
		out = append(out, *b)
	}
	r.mu.Unlock()

	// Newest-first by created_at, id descending as the stable tie-break.
	sort.Slice(out, func(i, j int) bool {
		return keyset.Less(out[j].CreatedAt, out[j].ID, out[i].CreatedAt, out[i].ID)
	})

	// Keyset: skip the rows at or newer than the cursor tuple.
	start := 0
	if curTS, curID := keyset.Decode(cursor); curID != "" {
		for start < len(out) && !keyset.Less(out[start].CreatedAt, out[start].ID, curTS, curID) {
			start++
		}
	}
	if start >= len(out) {
		return []bookmark.Bookmark{}, "", nil
	}
	end := start + limit
	if end > len(out) {
		end = len(out)
	}
	page := out[start:end]
	next := ""
	if end < len(out) {
		last := page[len(page)-1]
		next = keyset.Encode(last.CreatedAt, last.ID)
	}
	return page, next, nil
}
