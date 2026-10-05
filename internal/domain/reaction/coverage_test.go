// Package reaction_test holds the remaining coverage specs for the Reaction
// aggregate: guard branches, the React error path, ownership-checked
// UnreactByID, and the UUIDv7 rand-failure fallback.
package reaction_test

import (
	"context"
	"errors"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/reaction"
)

func TestReaction_New_RejectsEmptyTenant(t *testing.T) {
	t.Parallel()
	if _, err := reaction.NewReaction("  ", gcidA, postA, reaction.TypeLike); err == nil {
		t.Fatalf("expected error for blank tenant_id")
	}
}

// Registry.React propagates NewReaction guard failures instead of storing a
// partial record.
func TestRegistry_React_PropagatesInvalidType(t *testing.T) {
	t.Parallel()
	reg := reaction.NewRegistry()
	if _, _, err := reg.React(context.Background(), tenantA, gcidA, postA, reaction.Type("bogus")); err == nil {
		t.Fatalf("expected error for unknown reaction type")
	}
	if out, _ := reg.ListByPost(context.Background(), postA); len(out) != 0 {
		t.Fatalf("failed React must not store a reaction; got %d", len(out))
	}
}

// UnreactByID removes the reaction when the caller owns it, and clears the
// per-post bucket when it was the post's last reaction.
func TestRegistry_UnreactByID_RemovesOwnedReaction(t *testing.T) {
	t.Parallel()
	reg := reaction.NewRegistry()
	rx, _, err := reg.React(context.Background(), tenantA, gcidA, postA, reaction.TypeCurious)
	if err != nil {
		t.Fatalf("React: %v", err)
	}
	removed, err := reg.UnreactByID(context.Background(), rx.ID, gcidA)
	if err != nil {
		t.Fatalf("UnreactByID: unexpected error: %v", err)
	}
	if !removed {
		t.Fatalf("expected removed=true for an owned reaction")
	}
	if out, _ := reg.ListByPost(context.Background(), postA); len(out) != 0 {
		t.Fatalf("last reaction removed by ID must clear the byPost bucket; got %d", len(out))
	}
}

// UnreactByID refuses to remove another user's reaction (§10.6
// ownership-checked) — the record stays live.
func TestRegistry_UnreactByID_RejectsNonOwner(t *testing.T) {
	t.Parallel()
	reg := reaction.NewRegistry()
	rx, _, err := reg.React(context.Background(), tenantA, gcidA, postA, reaction.TypeCurious)
	if err != nil {
		t.Fatalf("React: %v", err)
	}
	removed, err := reg.UnreactByID(context.Background(), rx.ID, gcidB)
	if removed {
		t.Fatalf("a non-owner must not remove a reaction")
	}
	if !errors.Is(err, reaction.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
	if out, _ := reg.ListByPost(context.Background(), postA); len(out) != 1 {
		t.Fatalf("failed ownership check must leave the reaction live; got %d", len(out))
	}
}

func TestRegistry_UnreactByID_UnknownIDIsSilentNoOp(t *testing.T) {
	t.Parallel()
	reg := reaction.NewRegistry()
	removed, err := reg.UnreactByID(context.Background(), "no-such-id", gcidA)
	if err != nil {
		t.Fatalf("UnreactByID: unexpected error: %v", err)
	}
	if removed {
		t.Fatalf("expected removed=false for unknown reaction id")
	}
}

// UnreactByID removes a reaction while siblings stay in the bucket (loop
// break path of the per-post cleanup).
func TestRegistry_UnreactByID_RemovesOneOfManyForPost(t *testing.T) {
	t.Parallel()
	reg := reaction.NewRegistry()
	rx, _, err := reg.React(context.Background(), tenantA, gcidA, postA, reaction.TypeCurious)
	if err != nil {
		t.Fatalf("React: %v", err)
	}
	if _, _, err := reg.React(context.Background(), tenantA, gcidB, postA, reaction.TypeLike); err != nil {
		t.Fatalf("React: %v", err)
	}
	removed, err := reg.UnreactByID(context.Background(), rx.ID, gcidA)
	if err != nil || !removed {
		t.Fatalf("UnreactByID: removed=%v err=%v", removed, err)
	}
	out, _ := reg.ListByPost(context.Background(), postA)
	if len(out) != 1 {
		t.Fatalf("expected 1 reaction to remain, got %d", len(out))
	}
}

// NOTE: NewUUIDv7's rand-read-failure fallback is dead code on Go ≥1.24 —
// crypto/rand.Read never returns an error (it crashes the program), so the
// fallback branch cannot be exercised from tests without touching production.