package inmem

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
)

// ShareRepo is an in-memory adapter implementing atom_share.ShareRepo.
// Shares are keyed by FeedEntryID; append-only events are keyed by
// FeedEntryID and deduped on SourceEventID.
type ShareRepo struct {
	mu     sync.RWMutex
	shares map[string]*atom_share.Share       // key = feed_entry_id
	events map[string][]atom_share.ShareEvent // key = feed_entry_id
	bySrc  map[string]*atom_share.ShareEvent   // key = source_event_id (idempotency)
}

// NewShareRepo returns an empty ShareRepo.
func NewShareRepo() *ShareRepo {
	return &ShareRepo{
		shares: make(map[string]*atom_share.Share),
		events: make(map[string][]atom_share.ShareEvent),
		bySrc:  make(map[string]*atom_share.ShareEvent),
	}
}

// SaveShare persists the immutable feed entry. Idempotent on FeedEntryID.
func (r *ShareRepo) SaveShare(_ context.Context, s *atom_share.Share) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shares[s.FeedEntryID] = s
	return nil
}

// GetShare loads a share by feed entry id. Returns ErrNotFound when the
// entry is missing or revoked (the read-side status excludes revoked).
func (r *ShareRepo) GetShare(_ context.Context, feedEntryID string) (*atom_share.Share, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.shares[feedEntryID]
	if !ok {
		return nil, atom_share.ErrNotFound
	}
	if atom_share.DeriveStatus(r.events[feedEntryID]) == atom_share.StatusRevoked {
		return nil, atom_share.ErrNotFound
	}
	return s, nil
}

// GetShareByAtom loads the newest visible share for a given atom + owner.
// Returns ErrNotFound when no visible share exists (never shared or already
// revoked). Visible = DeriveStatus != StatusRevoked. The newest share wins
// when multiple exist (e.g. re-shared after a previous hide).
func (r *ShareRepo) GetShareByAtom(_ context.Context, atomID, ownerGCID string) (*atom_share.Share, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var best *atom_share.Share
	for _, s := range r.shares {
		if s.AtomID != atomID || s.OwnerGCID != ownerGCID {
			continue
		}
		if atom_share.DeriveStatus(r.events[s.FeedEntryID]) == atom_share.StatusRevoked {
			continue
		}
		if best == nil || s.CreatedAt.After(best.CreatedAt) {
			best = s
		}
	}
	if best == nil {
		return nil, atom_share.ErrNotFound
	}
	return best, nil
}

// ListSharedAtoms is a keyset-paginated read (cursor = opaque composite of
// created_at + feed_entry_id, newest-first). Revoked + hidden shares are
// excluded by derived status.
//
// Ordering is by CreatedAt DESC (NOT FeedEntryID) because feed entry IDs are
// UUIDv5 deterministic hashes — lexicographic id ordering does NOT reflect
// creation order. The cursor encodes (CreatedAt, FeedEntryID) of the last
// row on the page.
func (r *ShareRepo) ListSharedAtoms(_ context.Context, tenantID, cursor string, limit int, topicFilter, questionTypeFilter string, scope string, followingGCIDs []string, blockedGCIDs []string) ([]atom_share.Share, string, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	if scope == "" {
		scope = "tenant"
	}
	followingSet := make(map[string]struct{}, len(followingGCIDs))
	for _, g := range followingGCIDs {
		followingSet[g] = struct{}{}
	}
	blockedSet := make(map[string]struct{}, len(blockedGCIDs))
	for _, g := range blockedGCIDs {
		blockedSet[g] = struct{}{}
	}

	r.mu.RLock()
	out := make([]atom_share.Share, 0, len(r.shares))
	for _, s := range r.shares {
		if scope == "tenant" && s.TenantID != tenantID {
			continue
		}
		if scope == "following" {
			if _, ok := followingSet[s.OwnerGCID]; !ok {
				continue
			}
		}
		if _, ok := blockedSet[s.OwnerGCID]; ok {
			continue
		}
		if atom_share.DeriveStatus(r.events[s.FeedEntryID]) != atom_share.StatusVisible {
			continue
		}
		if topicFilter != "" && !strings.EqualFold(s.QuestionType, topicFilter) {
			continue
		}
		if questionTypeFilter != "" && !strings.EqualFold(s.QuestionType, questionTypeFilter) {
			continue
		}
		out = append(out, *s)
	}
	r.mu.RUnlock()

	// Newest-first by CreatedAt DESC. FeedEntryID is a UUIDv5 deterministic
	// hash (NOT UUIDv7), so it cannot be used for time ordering. Tie-break on
	// FeedEntryID descending for stable determinism.
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}
		return out[i].FeedEntryID > out[j].FeedEntryID
	})

	// Keyset: decode the opaque composite cursor (created_at, feed_entry_id).
	// Skip until we find the cursor row, then start on the NEXT row.
	start := 0
	if cursor != "" {
		cursorTS, cursorID := decodeInmemCursor(cursor)
		if !cursorTS.IsZero() {
			found := false
			for i, s := range out {
				if s.CreatedAt.Equal(cursorTS) && s.FeedEntryID == cursorID {
					start = i + 1
					found = true
					break
				}
			}
			if !found {
				// Stale/unrecognised cursor — treat as first page.
				start = 0
			}
		}
	}
	if start >= len(out) {
		return []atom_share.Share{}, "", nil
	}
	end := start + limit
	if end > len(out) {
		end = len(out)
	}
	page := out[start:end]
	next := ""
	if end < len(out) {
		last := page[len(page)-1]
		next = encodeInmemCursor(last.CreatedAt, last.FeedEntryID)
	}
	return page, next, nil
}

// AppendEvent appends an append-only ShareEvent child row. Idempotent on
// SourceEventID when non-empty: a replay with the same payload is a no-op;
// a replay with a different payload returns ErrConflict.
func (r *ShareRepo) AppendEvent(_ context.Context, e *atom_share.ShareEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if e.SourceEventID != "" {
		if existing, ok := r.bySrc[e.SourceEventID]; ok {
			if eventsEqual(existing, e) {
				return nil // idempotent replay
			}
			return atom_share.ErrConflict
		}
		r.bySrc[e.SourceEventID] = e
	}

	r.events[e.FeedEntryID] = append(r.events[e.FeedEntryID], *e)
	return nil
}

// ListEvents returns the events for a feed entry, newest-first.
func (r *ShareRepo) ListEvents(_ context.Context, feedEntryID string) ([]atom_share.ShareEvent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	evs := r.events[feedEntryID]
	out := make([]atom_share.ShareEvent, len(evs))
	copy(out, evs)
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

// allVisibleShares returns a snapshot of all visible shares (unexported;
// used by GrantRepo.ListEntitled to build the "free" portion of the union).
func (r *ShareRepo) allVisibleShares() []atom_share.Share {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]atom_share.Share, 0, len(r.shares))
	for _, s := range r.shares {
		if atom_share.DeriveStatus(r.events[s.FeedEntryID]) == atom_share.StatusVisible {
			out = append(out, *s)
		}
	}
	return out
}

func eventsEqual(a, b *atom_share.ShareEvent) bool {
	return a.Type == b.Type &&
		a.ActorGCID == b.ActorGCID &&
		reflect.DeepEqual(a.Payload, b.Payload)
}
