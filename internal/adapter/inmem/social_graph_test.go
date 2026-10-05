// social_graph_test.go — tenant-aware in-memory SocialGraph port (dev-mode
// fallback for the pg-backed repo; ADR-229 WS-0, CHO-2102). Semantics must
// match the pg adapter: idempotent follows (append-only), tenant scoping,
// mutual-follow friend set with blocks excluded in either direction.
package inmem_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

const (
	tenant1 = "01970000-0000-7000-a000-000000000001"
	tenant2 = "01970000-0000-7000-a000-000000000002"
	userA   = "01970000-0000-7000-c000-00000000000a"
	userB   = "01970000-0000-7000-c000-00000000000b"
	userC   = "01970000-0000-7000-c000-00000000000c"
)

func TestInmemSocialGraph_Follow_IdempotentAppendOnly(t *testing.T) {
	t.Parallel()
	g := inmem.NewSocialGraph()
	ctx := context.Background()

	e1, created, err := g.Follow(ctx, tenant1, userA, userB)
	if err != nil || !created {
		t.Fatalf("first follow: created=%v err=%v", created, err)
	}
	e2, created2, err := g.Follow(ctx, tenant1, userA, userB)
	if err != nil {
		t.Fatalf("re-follow: %v", err)
	}
	if created2 {
		t.Fatalf("re-follow must be created=false (append-only)")
	}
	if e2.ID != e1.ID || !e2.CreatedAt.Equal(e1.CreatedAt) {
		t.Fatalf("re-follow must return the ORIGINAL edge unchanged")
	}
}

func TestInmemSocialGraph_SelfFollow_Rejected(t *testing.T) {
	t.Parallel()
	g := inmem.NewSocialGraph()
	if _, _, err := g.Follow(context.Background(), tenant1, userA, userA); !errors.Is(err, social.ErrSelfFollow) {
		t.Fatalf("expected ErrSelfFollow; got %v", err)
	}
}

func TestInmemSocialGraph_TenantScoping(t *testing.T) {
	t.Parallel()
	g := inmem.NewSocialGraph()
	ctx := context.Background()
	if _, _, err := g.Follow(ctx, tenant1, userA, userB); err != nil {
		t.Fatalf("follow: %v", err)
	}

	got, err := g.FollowingGCIDs(ctx, tenant2, userA)
	if err != nil {
		t.Fatalf("FollowingGCIDs(t2): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("tenant2 must not see tenant1 edges; got %v", got)
	}
	got, err = g.FollowingGCIDs(ctx, tenant1, userA)
	if err != nil {
		t.Fatalf("FollowingGCIDs(t1): %v", err)
	}
	if !reflect.DeepEqual(got, []string{userB}) {
		t.Fatalf("expected [%s]; got %v", userB, got)
	}
}

func TestInmemSocialGraph_Unfollow(t *testing.T) {
	t.Parallel()
	g := inmem.NewSocialGraph()
	ctx := context.Background()
	if _, _, err := g.Follow(ctx, tenant1, userA, userB); err != nil {
		t.Fatalf("follow: %v", err)
	}
	removed, err := g.Unfollow(ctx, tenant1, userA, userB)
	if err != nil || !removed {
		t.Fatalf("unfollow: removed=%v err=%v", removed, err)
	}
	removed, err = g.Unfollow(ctx, tenant1, userA, userB)
	if err != nil {
		t.Fatalf("re-unfollow: %v", err)
	}
	if removed {
		t.Fatalf("re-unfollow must report removed=false")
	}
	followers, err := g.FollowersGCIDs(ctx, tenant1, userB)
	if err != nil {
		t.Fatalf("FollowersGCIDs: %v", err)
	}
	if len(followers) != 0 {
		t.Fatalf("edge must be gone from the followers index; got %v", followers)
	}
}

// ADR-230 D2: friendship exists only by explicit request/accept consent —
// the inmem dev graph has no friendship write path, so its FriendSet is
// truthfully EMPTY, even for mutual follows, before or after blocks. This
// pins that deriving friends from mutuals never sneaks back into dev mode.
func TestInmemSocialGraph_FriendSet_AlwaysEmptyInDev(t *testing.T) {
	t.Parallel()
	g := inmem.NewSocialGraph()
	ctx := context.Background()
	// A↔B mutual; A→C one-way — under the retired ADR-229 D3 semantics
	// A/B would have been friends. Under ADR-230 they are NOT.
	mustFollow(t, g, tenant1, userA, userB)
	mustFollow(t, g, tenant1, userB, userA)
	mustFollow(t, g, tenant1, userA, userC)

	for _, u := range []string{userA, userB, userC} {
		friends, err := g.FriendSet(ctx, tenant1, u)
		if err != nil {
			t.Fatalf("FriendSet(%s): %v", u, err)
		}
		if len(friends) != 0 {
			t.Fatalf("dev FriendSet must be empty (no consent path); %s got %v", u, friends)
		}
	}

	// Block/unblock don't change the (empty) friend set either.
	if err := g.Block(ctx, tenant1, userA, userB); err != nil {
		t.Fatalf("block: %v", err)
	}
	removed, err := g.Unblock(ctx, tenant1, userA, userB)
	if err != nil || !removed {
		t.Fatalf("unblock: removed=%v err=%v", removed, err)
	}
	friends, err := g.FriendSet(ctx, tenant1, userA)
	if err != nil {
		t.Fatalf("FriendSet after unblock: %v", err)
	}
	if len(friends) != 0 {
		t.Fatalf("dev FriendSet must stay empty; got %v", friends)
	}
}

func TestInmemSocialGraph_BlockedBy_And_SelfBlock(t *testing.T) {
	t.Parallel()
	g := inmem.NewSocialGraph()
	ctx := context.Background()
	if err := g.Block(ctx, tenant1, userA, userA); !errors.Is(err, social.ErrSelfBlock) {
		t.Fatalf("expected ErrSelfBlock; got %v", err)
	}
	if err := g.Block(ctx, tenant1, userA, userB); err != nil {
		t.Fatalf("block: %v", err)
	}
	if err := g.Block(ctx, tenant1, userA, userB); err != nil {
		t.Fatalf("re-block must be idempotent: %v", err)
	}
	blocked, err := g.BlockedBy(ctx, tenant1, userA)
	if err != nil {
		t.Fatalf("BlockedBy: %v", err)
	}
	if !reflect.DeepEqual(blocked, []string{userB}) {
		t.Fatalf("expected [%s]; got %v", userB, blocked)
	}
	// Tenant-scoped: tenant2 sees no blocks.
	blocked, err = g.BlockedBy(ctx, tenant2, userA)
	if err != nil {
		t.Fatalf("BlockedBy(t2): %v", err)
	}
	if len(blocked) != 0 {
		t.Fatalf("tenant2 must not see tenant1 blocks; got %v", blocked)
	}
}

func mustFollow(t *testing.T, g *inmem.SocialGraph, tenant, follower, followee string) {
	t.Helper()
	if _, _, err := g.Follow(context.Background(), tenant, follower, followee); err != nil {
		t.Fatalf("follow %s->%s: %v", follower, followee, err)
	}
}

// Compile-time proof: the inmem graph satisfies the FriendReader port too
// (dev-mode GetReuseContext).
var _ social.GraphQueries = (*inmem.SocialGraph)(nil)
