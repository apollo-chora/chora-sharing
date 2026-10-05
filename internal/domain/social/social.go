// Package social is the pure-domain core for the SocialGraph aggregate.
//
// Aggregate root: SocialGraph edge (follower_gcid, followee_gcid).
// Owning domain : Content Sharing (chora_sharing DB).
// Owning team   : Team 1 (Content).
//
// Per the brief:
//   - Edges are append-only (no UPDATE) — a re-Follow returns the existing
//     edge unchanged. This lets us preserve original CreatedAt for audit.
//   - Self-follow is rejected.
//   - Bidirectional follow = TWO records (A→B and B→A).
//   - Unfollow removes the edge.
//
// Hard rules (per .claude/rules/ddd-enforcement.md):
//   - This file holds the domain only — NO HTTP, NO persistence.
//   - UUIDv7 IDs.
//   - GCIDs travel as opaque cross-domain UUIDs.
package social

import (
	"crypto/rand"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Errors.
var (
	// ErrInvalidArgument is returned for guard-clause failures.
	ErrInvalidArgument = errors.New("invalid argument")

	// ErrSelfFollow is returned when follower_gcid == followee_gcid.
	ErrSelfFollow = errors.New("cannot follow self")
)

// Edge is a single follower → followee edge.
//
// CreatedAt is set on insert and is never mutated (append-only). The edge
// is removed via Unfollow rather than soft-deleted because the brief
// frames Edge as a presence record, not a content record.
type Edge struct {
	ID           string
	TenantID     string
	FollowerGCID string
	FolloweeGCID string
	CreatedAt    time.Time
}

// NewEdge constructs an Edge with guard-clauses.
//
// Used by Graph.Follow and exposed for tests that do not need the registry.
func NewEdge(tenantID, follower, followee string) (*Edge, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(follower) == "" {
		return nil, fmt.Errorf("%w: follower_gcid required", ErrInvalidArgument)
	}
	if strings.TrimSpace(followee) == "" {
		return nil, fmt.Errorf("%w: followee_gcid required", ErrInvalidArgument)
	}
	if follower == followee {
		return nil, ErrSelfFollow
	}
	return &Edge{
		ID:           NewUUIDv7(),
		TenantID:     tenantID,
		FollowerGCID: follower,
		FolloweeGCID: followee,
		CreatedAt:    time.Now().UTC(),
	}, nil
}

// Graph is an in-memory append-only social graph.
//
// Production wiring (M12+) will swap in a Postgres adapter against
// chora_sharing.social_edges with a UNIQUE(follower_gcid, followee_gcid)
// constraint enforcing the append-only invariant.
type Graph struct {
	mu        sync.Mutex
	byPair    map[string]*Edge    // key = follower|followee
	followers map[string][]*Edge  // followee → edges (people who follow X)
	following map[string][]*Edge  // follower → edges (people X follows)
	blocked   map[string][]string // blocker → blocked GCIDs
}

// NewGraph returns an empty social graph.
func NewGraph() *Graph {
	return &Graph{
		byPair:    make(map[string]*Edge),
		followers: make(map[string][]*Edge),
		following: make(map[string][]*Edge),
		blocked:   make(map[string][]string),
	}
}

func pairKey(follower, followee string) string {
	return follower + "|" + followee
}

// Follow inserts the (follower → followee) edge if absent.
//
// Returns (edge, created, err):
//   - created=true  → freshly inserted edge.
//   - created=false → edge already existed; returned UNCHANGED (append-only).
//
// Self-follow is rejected with ErrSelfFollow.
func (g *Graph) Follow(tenantID, follower, followee string) (*Edge, bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	key := pairKey(follower, followee)
	if existing, ok := g.byPair[key]; ok {
		return existing, false, nil
	}
	e, err := NewEdge(tenantID, follower, followee)
	if err != nil {
		return nil, false, err
	}
	g.byPair[key] = e
	g.followers[followee] = append(g.followers[followee], e)
	g.following[follower] = append(g.following[follower], e)
	return e, true, nil
}

// Unfollow removes the edge; returns true if removed, false if absent.
func (g *Graph) Unfollow(follower, followee string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	key := pairKey(follower, followee)
	e, ok := g.byPair[key]
	if !ok {
		return false
	}
	delete(g.byPair, key)
	g.followers[followee] = removeEdge(g.followers[followee], e.ID)
	g.following[follower] = removeEdge(g.following[follower], e.ID)
	if len(g.followers[followee]) == 0 {
		delete(g.followers, followee)
	}
	if len(g.following[follower]) == 0 {
		delete(g.following, follower)
	}
	return true
}

// removeEdge filters an edge by ID — O(n), fine for in-memory skeleton.
func removeEdge(in []*Edge, id string) []*Edge {
	for i, e := range in {
		if e.ID == id {
			return append(in[:i], in[i+1:]...)
		}
	}
	return in
}

// Followers returns the people who follow gcid.
func (g *Graph) Followers(gcid string) []*Edge {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]*Edge, 0, len(g.followers[gcid]))
	out = append(out, g.followers[gcid]...)
	return out
}

// Following returns the people gcid is following.
func (g *Graph) Following(gcid string) []*Edge {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]*Edge, 0, len(g.following[gcid]))
	out = append(out, g.following[gcid]...)
	return out
}

// FollowerCount returns the number of edges where gcid is the followee.
func (g *Graph) FollowerCount(gcid string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.followers[gcid])
}

// FollowingCount returns the number of edges where gcid is the follower.
func (g *Graph) FollowingCount(gcid string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.following[gcid])
}

// Mutuals returns the GCIDs that BOTH a and b follow (intersection of
// their respective Following sets). Result is sorted ascending for stable
// API output. Empty when there is no overlap.
func (g *Graph) Mutuals(a, b string) []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	aSet := make(map[string]struct{}, len(g.following[a]))
	for _, e := range g.following[a] {
		aSet[e.FolloweeGCID] = struct{}{}
	}
	out := make([]string, 0)
	for _, e := range g.following[b] {
		if _, ok := aSet[e.FolloweeGCID]; ok {
			out = append(out, e.FolloweeGCID)
		}
	}
	sort.Strings(out)
	return out
}

// FollowingGCIDs returns the followee GCIDs for a given follower.
func (g *Graph) FollowingGCIDs(gcid string) []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	edges := g.following[gcid]
	out := make([]string, 0, len(edges))
	for _, e := range edges {
		out = append(out, e.FolloweeGCID)
	}
	return out
}

// FollowersGCIDs returns the follower GCIDs for a given followee.
func (g *Graph) FollowersGCIDs(gcid string) []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	edges := g.followers[gcid]
	out := make([]string, 0, len(edges))
	for _, e := range edges {
		out = append(out, e.FollowerGCID)
	}
	return out
}

// Block records a block edge (blocker → blocked). Idempotent.
func (g *Graph) Block(tenantID, blocker, blocked string) error {
	if blocker == blocked {
		return ErrSelfFollow
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, b := range g.blocked[blocker] {
		if b == blocked {
			return nil
		}
	}
	g.blocked[blocker] = append(g.blocked[blocker], blocked)
	return nil
}

// Unblock removes a block edge; returns true if removed.
func (g *Graph) Unblock(blocker, blocked string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	list := g.blocked[blocker]
	for i, b := range list {
		if b == blocked {
			g.blocked[blocker] = append(list[:i], list[i+1:]...)
			if len(g.blocked[blocker]) == 0 {
				delete(g.blocked, blocker)
			}
			return true
		}
	}
	return false
}

// BlockedBy returns the GCIDs that gcid has blocked.
func (g *Graph) BlockedBy(gcid string) []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]string, 0, len(g.blocked[gcid]))
	out = append(out, g.blocked[gcid]...)
	return out
}

// BlockersOf returns the GCIDs that have blocked gcid (reverse direction).
func (g *Graph) BlockersOf(gcid string) []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out []string
	for blocker, blocked := range g.blocked {
		for _, b := range blocked {
			if b == gcid {
				out = append(out, blocker)
			}
		}
	}
	return out
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
