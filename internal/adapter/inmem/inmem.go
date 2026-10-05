// Package inmem holds in-memory repository adapters for the chora-sharing
// skeleton. Production implementations (M12+) will swap in a Postgres
// adapter against chora_sharing via PgBouncer; the domain layer is
// unchanged.
//
// Hexagonal layout: this package depends on the domain packages, but
// domain packages NEVER import this package.
package inmem

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/apollo-chora/chora-sharing/internal/domain/post"
)

// PostRepo is an in-memory store for Post aggregates, scoped by tenant.
type PostRepo struct {
	mu sync.RWMutex
	by map[string]*post.Post // key = post_id
}

// NewPostRepo returns an empty repo.
func NewPostRepo() *PostRepo { return &PostRepo{by: make(map[string]*post.Post)} }

// Save inserts or upserts a post. Satisfies post.PostRepo (CHO-2193/W0-F1): ctx
// is accepted and ignored — the in-memory double has no RLS session to apply —
// and error is always nil, because a map write cannot fail. The SIGNATURE is
// what matters: it is what lets the RLS-aware pg adapter be substituted here.
func (r *PostRepo) Save(_ context.Context, p *post.Post) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.by[p.ID] = p
	return nil
}

// Get returns a post by ID. ok=false means ABSENT. The error is always nil here
// (a map lookup cannot fail); it exists so the pg adapter can distinguish
// "absent" from "the lookup broke" instead of being forced to swallow.
func (r *PostRepo) Get(_ context.Context, id string) (*post.Post, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.by[id]
	if !ok {
		return nil, false, nil
	}
	return p, true, nil
}

// ListByTenant returns the non-soft-deleted posts for a tenant, sorted
// by ID (UUIDv7 ⇒ creation order).
func (r *PostRepo) ListByTenant(_ context.Context, tenantID string, offset, limit int) ([]*post.Post, int, error) {
	r.mu.RLock()
	out := make([]*post.Post, 0, len(r.by))
	for _, p := range r.by {
		if p.TenantID != tenantID || p.DeletedAt != nil {
			continue
		}
		out = append(out, p)
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return strings.Compare(out[i].ID, out[j].ID) < 0 })
	total := len(out)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return out[offset:end], total, nil
}
