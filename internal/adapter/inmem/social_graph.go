// social_graph.go — tenant-aware in-memory SocialGraph (dev-mode fallback for
// the pg-backed SocialGraphRepo; ADR-229 WS-0, CHO-2102).
//
// Semantics mirror the pg adapter exactly: idempotent append-only follows,
// tenant-scoped reads (the old domain Graph mixed a dual-membership user's
// edges across tenants), and the mutual-follow friend set with blocks
// excluded in either direction (social.FriendReader).
//
// NOT durable — dev/tests only. Production wires pg.NewSocialGraphRepo.
package inmem

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

// SocialGraph is the tenant-aware in-memory graph.
type SocialGraph struct {
	mu      sync.Mutex
	follows map[string]*social.Edge  // key = tenant|follower|followee
	blocks  map[string]*social.Block // key = tenant|blocker|blocked
}

// NewSocialGraph returns an empty tenant-aware graph.
func NewSocialGraph() *SocialGraph {
	return &SocialGraph{
		follows: make(map[string]*social.Edge),
		blocks:  make(map[string]*social.Block),
	}
}

func key3(tenant, a, b string) string { return tenant + "|" + a + "|" + b }

// Follow inserts the edge if absent; a duplicate returns the ORIGINAL edge
// unchanged with created=false (append-only).
func (g *SocialGraph) Follow(_ context.Context, tenantID, follower, followee string) (*social.Edge, bool, error) {
	edge, err := social.NewEdge(tenantID, follower, followee)
	if err != nil {
		return nil, false, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	k := key3(tenantID, follower, followee)
	if existing, ok := g.follows[k]; ok {
		return existing, false, nil
	}
	g.follows[k] = edge
	return edge, true, nil
}

// Unfollow removes the edge; returns true when it existed.
func (g *SocialGraph) Unfollow(_ context.Context, tenantID, follower, followee string) (bool, error) {
	if err := requireInmemGCIDs(follower, followee); err != nil {
		return false, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	k := key3(tenantID, follower, followee)
	if _, ok := g.follows[k]; !ok {
		return false, nil
	}
	delete(g.follows, k)
	return true, nil
}

// Block records a block edge idempotently.
func (g *SocialGraph) Block(_ context.Context, tenantID, blocker, blocked string) error {
	b, err := social.NewBlock(tenantID, blocker, blocked)
	if err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	k := key3(tenantID, blocker, blocked)
	if _, ok := g.blocks[k]; ok {
		return nil
	}
	g.blocks[k] = b
	return nil
}

// Unblock removes a block edge; returns true when it existed.
func (g *SocialGraph) Unblock(_ context.Context, tenantID, blocker, blocked string) (bool, error) {
	if err := requireInmemGCIDs(blocker, blocked); err != nil {
		return false, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	k := key3(tenantID, blocker, blocked)
	if _, ok := g.blocks[k]; !ok {
		return false, nil
	}
	delete(g.blocks, k)
	return true, nil
}

// FollowingGCIDs lists followees of gcid within the tenant (sorted).
func (g *SocialGraph) FollowingGCIDs(_ context.Context, tenantID, gcid string) ([]string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]string, 0, 8)
	for _, e := range g.follows {
		if e.TenantID == tenantID && e.FollowerGCID == gcid {
			out = append(out, e.FolloweeGCID)
		}
	}
	sort.Strings(out)
	return out, nil
}

// FollowersGCIDs lists followers of gcid within the tenant (sorted).
func (g *SocialGraph) FollowersGCIDs(_ context.Context, tenantID, gcid string) ([]string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]string, 0, 8)
	for _, e := range g.follows {
		if e.TenantID == tenantID && e.FolloweeGCID == gcid {
			out = append(out, e.FollowerGCID)
		}
	}
	sort.Strings(out)
	return out, nil
}

// BlockedBy lists the GCIDs gcid has blocked within the tenant (sorted).
func (g *SocialGraph) BlockedBy(_ context.Context, tenantID, gcid string) ([]string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]string, 0, 4)
	for _, b := range g.blocks {
		if b.TenantID == tenantID && b.BlockerGCID == gcid {
			out = append(out, b.BlockedGCID)
		}
	}
	sort.Strings(out)
	return out, nil
}

// FriendSet resolves the friend set (social.GraphQueries). The friendship
// write path is gone, so the friend set is always EMPTY. Kept for the
// atom_reuse audience tier + GetReuseContext gRPC contract.
func (g *SocialGraph) FriendSet(_ context.Context, tenantID, gcid string) ([]string, error) {
	return []string{}, nil
}

// FriendSuggestions returns bounded FoF candidates (social.GraphQueries).
// Always EMPTY now that friendships are gone — kept for interface parity.
func (g *SocialGraph) FriendSuggestions(_ context.Context, _, _ string, _ int) ([]social.FriendSuggestion, error) {
	return []social.FriendSuggestion{}, nil
}

// FollowSuggestions (social.SuggestionQueries) — dev mode implements the
// follow-graph signal for real (2-hop through in-memory follow edges +
// mutual-follow path count) but returns empty SharedTags (the in-memory
// graph has no profiler_profiles; the interest signal lives in the pg
// adapter). Excludes self + already-followed + blocked-either-direction.
func (g *SocialGraph) FollowSuggestions(_ context.Context, _, gcid string, limit int) ([]social.FollowSuggestion, error) {
	if limit <= 0 {
		return nil, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	// Build followee set for the caller.
	followees := make(map[string]bool)
	for _, e := range g.follows {
		if e.FollowerGCID == gcid {
			followees[e.FolloweeGCID] = true
		}
	}
	// Build blocked set (either direction).
	blocked := make(map[string]bool)
	for _, b := range g.blocks {
		if b.BlockerGCID == gcid {
			blocked[b.BlockedGCID] = true
		}
		if b.BlockedGCID == gcid {
			blocked[b.BlockerGCID] = true
		}
	}
	// 2-hop candidates: followees-of-my-followees, excluding self +
	// already-followed + blocked. Count path frequency (how many of my
	// followees follow each candidate) as mutual_follows.
	candidatePaths := make(map[string]int)
	for _, e1 := range g.follows {
		if e1.FollowerGCID != gcid {
			continue
		}
		// e1.followee is someone I follow — find who they follow.
		for _, e2 := range g.follows {
			if e2.FollowerGCID != e1.FolloweeGCID {
				continue
			}
			cand := e2.FolloweeGCID
			if cand == gcid || followees[cand] || blocked[cand] {
				continue
			}
			candidatePaths[cand]++
		}
	}
	out := make([]social.FollowSuggestion, 0, len(candidatePaths))
	for cand, paths := range candidatePaths {
		out = append(out, social.FollowSuggestion{
			GCID:          cand,
			SharedTags:    []string{}, // no interest signal in dev
			MutualFollows: paths,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if len(out[i].SharedTags) != len(out[j].SharedTags) {
			return len(out[i].SharedTags) > len(out[j].SharedTags)
		}
		if out[i].MutualFollows != out[j].MutualFollows {
			return out[i].MutualFollows > out[j].MutualFollows
		}
		return out[i].GCID < out[j].GCID
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func requireInmemGCIDs(a, b string) error {
	if strings.TrimSpace(a) == "" || strings.TrimSpace(b) == "" {
		return social.ErrInvalidArgument
	}
	return nil
}
