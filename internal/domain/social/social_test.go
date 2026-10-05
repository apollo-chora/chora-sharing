// Package social_test holds RED-phase TDD specs for the SocialGraph aggregate.
package social_test

import (
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

const (
	tenantA = "01970000-0000-7000-8000-000000000001"
	gcidA   = "01970000-0000-7000-9000-000000000001"
	gcidB   = "01970000-0000-7000-9000-000000000002"
	gcidC   = "01970000-0000-7000-9000-000000000003"
)

func TestEdge_New_StoresFollowerFollowee(t *testing.T) {
	t.Parallel()
	e, err := social.NewEdge(tenantA, gcidA, gcidB)
	if err != nil {
		t.Fatalf("NewEdge: %v", err)
	}
	if e.ID == "" {
		t.Fatalf("expected non-empty edge ID")
	}
	if e.FollowerGCID != gcidA {
		t.Fatalf("follower mismatch")
	}
	if e.FolloweeGCID != gcidB {
		t.Fatalf("followee mismatch")
	}
	if e.CreatedAt.IsZero() {
		t.Fatalf("expected CreatedAt")
	}
}

func TestEdge_New_RejectsSelfFollow(t *testing.T) {
	t.Parallel()
	_, err := social.NewEdge(tenantA, gcidA, gcidA)
	if err == nil {
		t.Fatalf("expected ErrSelfFollow")
	}
	if err != social.ErrSelfFollow {
		t.Fatalf("expected ErrSelfFollow, got %v", err)
	}
}

func TestEdge_New_RejectsEmptyTenant(t *testing.T) {
	t.Parallel()
	if _, err := social.NewEdge("", gcidA, gcidB); err == nil {
		t.Fatalf("expected error for empty tenant")
	}
}

func TestEdge_New_RejectsEmptyGCID(t *testing.T) {
	t.Parallel()
	if _, err := social.NewEdge(tenantA, "", gcidB); err == nil {
		t.Fatalf("expected error for empty follower")
	}
	if _, err := social.NewEdge(tenantA, gcidA, ""); err == nil {
		t.Fatalf("expected error for empty followee")
	}
}

func TestGraph_Follow_PersistsEdge(t *testing.T) {
	t.Parallel()
	g := social.NewGraph()
	e, created, err := g.Follow(tenantA, gcidA, gcidB)
	if err != nil {
		t.Fatalf("Follow: %v", err)
	}
	if e == nil || e.ID == "" {
		t.Fatalf("expected edge")
	}
	if !created {
		t.Fatalf("expected created=true on first follow")
	}
}

// Append-only: re-following an existing edge does NOT mutate; returns the
// existing edge and created=false.
func TestGraph_Follow_AppendOnly_NoUpdateOnRepeat(t *testing.T) {
	t.Parallel()
	g := social.NewGraph()
	e1, _, _ := g.Follow(tenantA, gcidA, gcidB)
	e2, created, err := g.Follow(tenantA, gcidA, gcidB)
	if err != nil {
		t.Fatalf("Follow #2: %v", err)
	}
	if e1.ID != e2.ID {
		t.Fatalf("expected same edge ID on repeat follow (append-only); got %q vs %q", e1.ID, e2.ID)
	}
	if created {
		t.Fatalf("expected created=false on repeat")
	}
	if !e1.CreatedAt.Equal(e2.CreatedAt) {
		t.Fatalf("CreatedAt must NOT change on append-only repeat")
	}
}

func TestGraph_Follow_RejectsSelf(t *testing.T) {
	t.Parallel()
	g := social.NewGraph()
	_, _, err := g.Follow(tenantA, gcidA, gcidA)
	if err == nil {
		t.Fatalf("expected ErrSelfFollow")
	}
}

func TestGraph_Unfollow_RemovesEdge(t *testing.T) {
	t.Parallel()
	g := social.NewGraph()
	_, _, _ = g.Follow(tenantA, gcidA, gcidB)
	removed := g.Unfollow(gcidA, gcidB)
	if !removed {
		t.Fatalf("expected Unfollow to return true on existing edge")
	}
	// re-Unfollow should be a no-op.
	if g.Unfollow(gcidA, gcidB) {
		t.Fatalf("expected Unfollow to return false the second time")
	}
}

func TestGraph_Followers_ReturnsAllForFollowee(t *testing.T) {
	t.Parallel()
	g := social.NewGraph()
	_, _, _ = g.Follow(tenantA, gcidA, gcidB)
	_, _, _ = g.Follow(tenantA, gcidC, gcidB)
	out := g.Followers(gcidB)
	if len(out) != 2 {
		t.Fatalf("expected 2 followers, got %d", len(out))
	}
}

func TestGraph_Following_ReturnsAllForFollower(t *testing.T) {
	t.Parallel()
	g := social.NewGraph()
	_, _, _ = g.Follow(tenantA, gcidA, gcidB)
	_, _, _ = g.Follow(tenantA, gcidA, gcidC)
	out := g.Following(gcidA)
	if len(out) != 2 {
		t.Fatalf("expected 2 followings, got %d", len(out))
	}
}

// Bidirectional follow = two separate records (A→B and B→A). Test that
// adding the reverse edge produces a distinct record.
func TestGraph_Bidirectional_TwoEdges(t *testing.T) {
	t.Parallel()
	g := social.NewGraph()
	e1, _, _ := g.Follow(tenantA, gcidA, gcidB)
	e2, _, _ := g.Follow(tenantA, gcidB, gcidA)
	if e1.ID == e2.ID {
		t.Fatalf("expected distinct edges for A→B and B→A")
	}
	if len(g.Followers(gcidA)) != 1 {
		t.Fatalf("expected gcidA to have 1 follower")
	}
	if len(g.Followers(gcidB)) != 1 {
		t.Fatalf("expected gcidB to have 1 follower")
	}
}
