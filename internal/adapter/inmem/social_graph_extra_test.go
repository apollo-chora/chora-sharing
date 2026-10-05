// social_graph_extra_test.go — residual branches of the tenant-aware
// in-memory SocialGraph not exercised by social_graph_test.go: argument
// validation on Unfollow/Unblock, the followers index, the always-empty
// FriendSuggestions stub, and the full FollowSuggestions 2-hop algorithm.
package inmem_test

import (
	"context"
	"errors"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

// Additional GCIDs for the 2-hop suggestion topology (userA..userC live in
// social_graph_test.go). Lexicographic order matters: suggestions tie-break
// on gcid ascending, so D < E < F < G must hold as strings.
const (
	userD = "01970000-0000-7000-c000-00000000000d"
	userE = "01970000-0000-7000-c000-00000000000e"
	userF = "01970000-0000-7000-c000-00000000000f"
	userG = "01970000-0000-7000-c000-00000000001a"
	userH = "01970000-0000-7000-c000-00000000001b"
	userI = "01970000-0000-7000-c000-00000000001c"
)

func TestInmemSocialGraph_UnfollowUnblock_InvalidGCIDs(t *testing.T) {
	t.Parallel()
	g := inmem.NewSocialGraph()
	ctx := context.Background()

	if _, err := g.Unfollow(ctx, tenant1, "", userB); !errors.Is(err, social.ErrInvalidArgument) {
		t.Fatalf("unfollow empty follower: expected ErrInvalidArgument, got %v", err)
	}
	if _, err := g.Unfollow(ctx, tenant1, userA, "   "); !errors.Is(err, social.ErrInvalidArgument) {
		t.Fatalf("unfollow blank followee: expected ErrInvalidArgument, got %v", err)
	}
	if _, err := g.Unblock(ctx, tenant1, "", userB); !errors.Is(err, social.ErrInvalidArgument) {
		t.Fatalf("unblock empty blocker: expected ErrInvalidArgument, got %v", err)
	}
	if _, err := g.Unblock(ctx, tenant1, userA, "   "); !errors.Is(err, social.ErrInvalidArgument) {
		t.Fatalf("unblock blank blocked: expected ErrInvalidArgument, got %v", err)
	}

	// Unblocking an edge that was never blocked is a no-op (false, nil).
	removed, err := g.Unblock(ctx, tenant1, userA, userB)
	if err != nil || removed {
		t.Fatalf("unblock absent: removed=%v err=%v", removed, err)
	}
}

func TestInmemSocialGraph_FollowersGCIDs_Index(t *testing.T) {
	t.Parallel()
	g := inmem.NewSocialGraph()
	ctx := context.Background()

	mustFollow(t, g, tenant1, userA, userC)
	mustFollow(t, g, tenant1, userB, userC)
	mustFollow(t, g, tenant2, userA, userC) // tenant isolation

	got, err := g.FollowersGCIDs(ctx, tenant1, userC)
	if err != nil {
		t.Fatalf("FollowersGCIDs: %v", err)
	}
	if len(got) != 2 || got[0] != userA || got[1] != userB {
		t.Fatalf("followers of C in t1: want [%s %s], got %v", userA, userB, got)
	}
}

func TestInmemSocialGraph_FriendSuggestions_AlwaysEmpty(t *testing.T) {
	t.Parallel()
	g := inmem.NewSocialGraph()
	ctx := context.Background()

	got, err := g.FriendSuggestions(ctx, tenant1, userA, 10)
	if err != nil {
		t.Fatalf("FriendSuggestions: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("dev FriendSuggestions must be empty; got %v", got)
	}
}

func TestInmemSocialGraph_FollowSuggestions(t *testing.T) {
	t.Parallel()
	g := inmem.NewSocialGraph()
	ctx := context.Background()

	// limit <= 0 → nil slice, no error.
	if got, _ := g.FollowSuggestions(ctx, tenant1, userA, 0); got != nil {
		t.Fatalf("limit<=0: want nil, got %v", got)
	}

	// Network:
	//   A→B, A→C, B→D, C→D (D: 2 paths), B→E (E: 1 path), B→F (F: 1 path),
	//   B→A (self skip), B→C (already followed skip), B→G (G: 1 path).
	mustFollow(t, g, tenant1, userA, userB)
	mustFollow(t, g, tenant1, userA, userC)
	mustFollow(t, g, tenant1, userB, userD)
	mustFollow(t, g, tenant1, userC, userD)
	mustFollow(t, g, tenant1, userB, userE)
	mustFollow(t, g, tenant1, userB, userF)
	mustFollow(t, g, tenant1, userB, userA)
	mustFollow(t, g, tenant1, userB, userC)
	mustFollow(t, g, tenant1, userB, userG)

	// Blocks in both directions: A→H blocked; I→A blocked.
	if err := g.Block(ctx, tenant1, userA, userH); err != nil {
		t.Fatalf("block: %v", err)
	}
	if err := g.Block(ctx, tenant1, userI, userA); err != nil {
		t.Fatalf("block: %v", err)
	}

	// Candidates: D(2), then E/F/G(1) — but blocked H/I never become
	// candidates since no edges reach them. limit 2 → [D, E].
	sug, err := g.FollowSuggestions(ctx, tenant1, userA, 2)
	if err != nil {
		t.Fatalf("FollowSuggestions: %v", err)
	}
	if len(sug) != 2 || sug[0].GCID != userD || sug[1].GCID != userE {
		t.Fatalf("limit 2: want [%s %s], got %+v", userD, userE, sug)
	}
	if sug[0].MutualFollows != 2 || sug[1].MutualFollows != 1 {
		t.Fatalf("path counts: want D=2 E=1, got D=%d E=%d", sug[0].MutualFollows, sug[1].MutualFollows)
	}

	// No limit pressure → all four candidates, sorted by mutuals desc then gcid.
	sug, err = g.FollowSuggestions(ctx, tenant1, userA, 10)
	if err != nil {
		t.Fatalf("FollowSuggestions(10): %v", err)
	}
	if len(sug) != 4 {
		t.Fatalf("want 4 candidates, got %d", len(sug))
	}

	// Fresh graph → no candidates.
	fresh := inmem.NewSocialGraph()
	freshGot, err := fresh.FollowSuggestions(ctx, tenant1, userA, 5)
	if err != nil {
		t.Fatalf("fresh FollowSuggestions: %v", err)
	}
	if len(freshGot) != 0 {
		t.Fatalf("fresh graph: want 0, got %d", len(freshGot))
	}
}