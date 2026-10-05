package inmem

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
	"github.com/apollo-chora/chora-sharing/internal/domain/grant"
)

// activeKey builds the idempotency key for an active grant:
// (grantee_gcid, atom_id, scope) WHERE active.
func activeKey(grantee, atomID string, scope grant.Scope) string {
	return grantee + "|" + atomID + "|" + string(scope)
}

// GrantRepo is an in-memory adapter implementing grant.GrantRepo.
// Authorize is idempotent on (grantee, atom, scope) WHERE active — a
// re-call returns the existing frozen snapshot.
type GrantRepo struct {
	mu          sync.RWMutex
	byID        map[string]*grant.AtomUsageGrant // key = grant_id
	active      map[string]*grant.AtomUsageGrant // key = grantee|atom|scope (active only)
	projections *ProjectionRepo                    // for ListEntitled "own" + metadata
	shares      *ShareRepo                         // for ListEntitled "free" + metadata
}

// NewGrantRepo returns a GrantRepo wired to the projection + share repos
// (needed by ListEntitled to build the own ∪ free ∪ active-grant union).
func NewGrantRepo(projections *ProjectionRepo, shares *ShareRepo) *GrantRepo {
	return &GrantRepo{
		byID:        make(map[string]*grant.AtomUsageGrant),
		active:      make(map[string]*grant.AtomUsageGrant),
		projections: projections,
		shares:      shares,
	}
}

// Authorize persists a grant. Idempotent on (grantee, atom, scope) WHERE
// active: if an active grant already exists for that triple, it is returned
// unchanged (the frozen snapshot from the first call wins — R2).
func (r *GrantRepo) Authorize(_ context.Context, g *grant.AtomUsageGrant) (*grant.AtomUsageGrant, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := activeKey(g.GranteeGCID, g.AtomID, g.Scope)
	if existing, ok := r.active[key]; ok {
		return existing, nil
	}
	r.byID[g.ID] = g
	r.active[key] = g
	return g, nil
}

// Revoke transitions a grant to revoked. Idempotent on grant_id — revoking
// an already-revoked grant is a no-op.
func (r *GrantRepo) Revoke(_ context.Context, grantID, _ string, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	g, ok := r.byID[grantID]
	if !ok {
		return grant.ErrGrantNotFound
	}
	if g.Status.IsTerminal() {
		return nil // idempotent: already revoked/expired
	}
	if err := g.Revoke(time.Now().UTC()); err != nil {
		return err
	}
	delete(r.active, activeKey(g.GranteeGCID, g.AtomID, g.Scope))
	return nil
}

// GetActive returns the active grant for (grantee, atom, scope), checking
// Covers semantics: an active UNLIMITED grant satisfies any requested scope.
// Returns ErrGrantNotFound when no active grant covers the triple.
func (r *GrantRepo) GetActive(_ context.Context, grantee, atomID string, scope grant.Scope) (*grant.AtomUsageGrant, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Exact scope match first.
	if g, ok := r.active[activeKey(grantee, atomID, scope)]; ok {
		return g, nil
	}
	// Fall back to UNLIMITED (covers any scope).
	if scope != grant.ScopeUnlimited {
		if g, ok := r.active[activeKey(grantee, atomID, grant.ScopeUnlimited)]; ok {
			return g, nil
		}
	}
	return nil, grant.ErrGrantNotFound
}

// ListEntitled builds the "atoms usable by me" union per §7.4:
// own ∪ free ∪ active-grant. An atom may appear once with both IsOwn and
// HasGrant set if the grantee both owns the atom and has a grant for it.
func (r *GrantRepo) ListEntitled(_ context.Context, gcid string, scope grant.Scope, topicTags []string, limit int) ([]grant.EntitledAtom, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}

	tagSet := make(map[string]bool, len(topicTags))
	for _, t := range topicTags {
		tagSet[strings.ToLower(t)] = true
	}

	r.mu.RLock()
	type activeGrant struct {
		atomID string
		g      *grant.AtomUsageGrant
	}
	var grantsForGcid []activeGrant
	for _, g := range r.active {
		if g.GranteeGCID == gcid && g.Scope.Covers(scope) {
			grantsForGcid = append(grantsForGcid, activeGrant{atomID: g.AtomID, g: g})
		}
	}
	r.mu.RUnlock()

	// Build the union keyed by atom_id.
	union := make(map[string]grant.EntitledAtom)

	// 1. Own atoms: projection.owner_gcid == gcid.
	for _, p := range r.projections.allProjections() {
		if p.OwnerGCID != gcid {
			continue
		}
		if len(tagSet) > 0 && !tagSet[strings.ToLower(string(p.QuestionType))] {
			continue
		}
		e := union[p.AtomID]
		e.AtomID = p.AtomID
		e.RevisionID = p.RevisionID
		e.AuthorGCID = p.OwnerGCID
		e.AuthorDisplayName = p.AuthorDisplayName
		e.StemPreview = p.StemPreview(140)
		e.License = atom_share.LicenseFree
		e.IsOwn = true
		union[p.AtomID] = e
	}

	// 2. Free shares: a visible share with a free/cc license (usable by anyone).
	for _, s := range r.shares.allVisibleShares() {
		if s.License.IsRoyalty() {
			continue // free licenses only: free / cc_by_sa / cc_nd
		}
		if len(tagSet) > 0 && !tagSet[strings.ToLower(s.QuestionType)] {
			continue
		}
		e := union[s.AtomID]
		e.AtomID = s.AtomID
		e.RevisionID = s.RevisionID
		e.AuthorGCID = s.OwnerGCID
		e.AuthorDisplayName = s.AuthorDisplayName
		e.StemPreview = s.StemPreview
		e.License = s.License
		union[s.AtomID] = e
	}

	// 3. Active grants: an active grant covers (gcid, atom) for the scope.
	for _, ag := range grantsForGcid {
		e := union[ag.atomID]
		e.AtomID = ag.atomID
		e.RevisionID = ag.g.RevisionID
		e.AuthorGCID = ag.g.OwnerGCID
		e.License = ag.g.LicenseTermsSnapshot
		e.HasGrant = true
		union[ag.atomID] = e
	}

	out := make([]grant.EntitledAtom, 0, len(union))
	for _, e := range union {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AtomID < out[j].AtomID })
	if limit < len(out) {
		out = out[:limit]
	}
	return out, nil
}

// RoyaltyRepo is an in-memory adapter implementing grant.RoyaltyRepo.
// Record is idempotent on SourceEventID — a replay never double-credits.
type RoyaltyRepo struct {
	mu       sync.RWMutex
	bySource map[string]*grant.RoyaltySettlement // key = source_event_id
}

// NewRoyaltyRepo returns an empty RoyaltyRepo.
func NewRoyaltyRepo() *RoyaltyRepo {
	return &RoyaltyRepo{bySource: make(map[string]*grant.RoyaltySettlement)}
}

// Record persists a royalty settlement. Idempotent on SourceEventID: if a
// settlement with the same SourceEventID already exists, returns
// ErrRoyaltyAlreadySettled (the caller treats this as success — idempotent).
func (r *RoyaltyRepo) Record(_ context.Context, s *grant.RoyaltySettlement) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.bySource[s.SourceEventID]; ok {
		return grant.ErrRoyaltyAlreadySettled
	}
	r.bySource[s.SourceEventID] = s
	return nil
}

// Exists reports whether a settlement has been recorded for the SourceEventID.
func (r *RoyaltyRepo) Exists(_ context.Context, sourceEventID string) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.bySource[sourceEventID]
	return ok, nil
}
