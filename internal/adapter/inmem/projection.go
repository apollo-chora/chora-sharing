package inmem

import (
	"context"
	"sync"

	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
)

// ProjectionRepo is an in-memory adapter implementing both
// atom_projection.AtomProjectionReader and atom_projection.AtomProjectionWriter.
// Projections are value objects stored by atom_id.
type ProjectionRepo struct {
	mu   sync.RWMutex
	byID map[string]atom_projection.Projection // key = atom_id
}

// NewProjectionRepo returns an empty ProjectionRepo.
func NewProjectionRepo() *ProjectionRepo {
	return &ProjectionRepo{byID: make(map[string]atom_projection.Projection)}
}

// Get returns the cached projection for atom_id, or ErrNotFound when the
// atom has never been published or has been archived.
func (r *ProjectionRepo) Get(_ context.Context, atomID string) (atom_projection.Projection, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.byID[atomID]
	if !ok || p.Archived {
		return atom_projection.Projection{}, atom_projection.ErrNotFound
	}
	return p, nil
}

// Upsert caches (or refreshes) a projection from a published.v1 event.
// Idempotent on atom_id — re-publishing overwrites with the latest revision.
func (r *ProjectionRepo) Upsert(_ context.Context, p atom_projection.Projection) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byID[p.AtomID] = p
	return nil
}

// Invalidate marks the projection archived on atom.archived.v1. Idempotent.
func (r *ProjectionRepo) Invalidate(_ context.Context, atomID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.byID[atomID]
	if !ok {
		return atom_projection.ErrNotFound
	}
	p.Archived = true
	r.byID[atomID] = p
	return nil
}

// SetReuseVisibility applies an author audience change from
// reuse_visibility_changed.v1 (ADR-229 WS-1). A missing projection row is
// NOT an error — the atom was never published; the eventual publish event
// carries the current flag (mirrors the pg store's 0-row semantics).
func (r *ProjectionRepo) SetReuseVisibility(_ context.Context, atomID, visibility string) error {
	if !atom_projection.ValidReuseVisibility(visibility) {
		return atom_projection.ErrInvalidArgument
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.byID[atomID]
	if !ok {
		return nil
	}
	p.ReuseVisibility = visibility
	r.byID[atomID] = p
	return nil
}

// ListRandom returns up to `limit` random published atoms NOT owned by any
// of the excludeGCIDs. Used by the matchmake handler as a fallback.
func (r *ProjectionRepo) ListRandom(_ context.Context, _ string, excludeGCIDs []string, limit int) ([]atom_projection.Projection, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	exclude := make(map[string]bool, len(excludeGCIDs))
	for _, g := range excludeGCIDs {
		exclude[g] = true
	}
	out := make([]atom_projection.Projection, 0, limit)
	for _, p := range r.byID {
		if p.Archived || exclude[p.OwnerGCID] {
			continue
		}
		out = append(out, p)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// allProjections returns a snapshot of all non-archived projections (unexported;
// used by GrantRepo.ListEntitled to build the "own" portion of the union).
func (r *ProjectionRepo) allProjections() []atom_projection.Projection {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]atom_projection.Projection, 0, len(r.byID))
	for _, p := range r.byID {
		if !p.Archived {
			out = append(out, p)
		}
	}
	return out
}
