// Package social_test holds RED-phase TDD specs for mutual-connections + counts.
package social_test

import (
	"sort"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

// Mutuals are the people each side follows in common (intersection of
// "following" sets).
func TestGraph_Mutuals_EmptyWhenNoOverlap(t *testing.T) {
	t.Parallel()
	g := social.NewGraph()
	gcidD := "01970000-0000-7000-9000-000000000004"
	_, _, _ = g.Follow(tenantA, gcidA, gcidB)
	_, _, _ = g.Follow(tenantA, gcidC, gcidD)
	out := g.Mutuals(gcidA, gcidC)
	if len(out) != 0 {
		t.Fatalf("expected empty mutuals, got %d", len(out))
	}
}

func TestGraph_Mutuals_ReturnsCommonFollowees(t *testing.T) {
	t.Parallel()
	g := social.NewGraph()
	gcidD := "01970000-0000-7000-9000-000000000004"
	gcidE := "01970000-0000-7000-9000-000000000005"
	// gcidA follows D + E
	_, _, _ = g.Follow(tenantA, gcidA, gcidD)
	_, _, _ = g.Follow(tenantA, gcidA, gcidE)
	// gcidC follows D + B
	_, _, _ = g.Follow(tenantA, gcidC, gcidD)
	_, _, _ = g.Follow(tenantA, gcidC, gcidB)
	out := g.Mutuals(gcidA, gcidC)
	if len(out) != 1 {
		t.Fatalf("expected 1 mutual, got %d", len(out))
	}
	if out[0] != gcidD {
		t.Fatalf("expected mutual=%q, got %q", gcidD, out[0])
	}
}

// Mutuals must be deterministic (sorted ascending) for stable API output.
func TestGraph_Mutuals_DeterministicOrder(t *testing.T) {
	t.Parallel()
	g := social.NewGraph()
	gcidD := "01970000-0000-7000-9000-000000000004"
	gcidE := "01970000-0000-7000-9000-000000000005"
	gcidF := "01970000-0000-7000-9000-000000000006"
	for _, f := range []string{gcidF, gcidD, gcidE} {
		_, _, _ = g.Follow(tenantA, gcidA, f)
		_, _, _ = g.Follow(tenantA, gcidB, f)
	}
	got := g.Mutuals(gcidA, gcidB)
	sorted := append([]string(nil), got...)
	sort.Strings(sorted)
	for i := range got {
		if got[i] != sorted[i] {
			t.Fatalf("Mutuals must be sorted; got %v", got)
		}
	}
}

func TestGraph_Counts_FollowersAndFollowing(t *testing.T) {
	t.Parallel()
	g := social.NewGraph()
	_, _, _ = g.Follow(tenantA, gcidA, gcidB)
	_, _, _ = g.Follow(tenantA, gcidC, gcidB)
	if c := g.FollowerCount(gcidB); c != 2 {
		t.Fatalf("expected FollowerCount(B)=2, got %d", c)
	}
	if c := g.FollowingCount(gcidA); c != 1 {
		t.Fatalf("expected FollowingCount(A)=1, got %d", c)
	}
}
