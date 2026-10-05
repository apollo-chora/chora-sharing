// Package reaction_test holds RED-phase TDD specs for the Reaction aggregate
// + idempotency registry.
package reaction_test

import (
	"context"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/reaction"
)

const (
	tenantA = "01970000-0000-7000-8000-000000000001"
	gcidA   = "01970000-0000-7000-9000-000000000001"
	gcidB   = "01970000-0000-7000-9000-000000000002"
	postA   = "01970000-0000-7000-b000-000000000001"
)

func TestReaction_New_AssignsIDAndCreatedAt(t *testing.T) {
	t.Parallel()
	r, err := reaction.NewReaction(tenantA, gcidA, postA, reaction.TypeCurious)
	if err != nil {
		t.Fatalf("NewReaction: unexpected error: %v", err)
	}
	if r.ID == "" {
		t.Fatalf("expected non-empty ID")
	}
	if r.Type != reaction.TypeCurious {
		t.Fatalf("type mismatch")
	}
	if r.CreatedAt.IsZero() {
		t.Fatalf("expected CreatedAt set")
	}
}

func TestReaction_New_RejectsUnknownType(t *testing.T) {
	t.Parallel()
	if _, err := reaction.NewReaction(tenantA, gcidA, postA, reaction.Type("bogus")); err == nil {
		t.Fatalf("expected error for unknown reaction type")
	}
}

func TestReaction_New_RejectsEmptyGCID(t *testing.T) {
	t.Parallel()
	if _, err := reaction.NewReaction(tenantA, "", postA, reaction.TypeLike); err == nil {
		t.Fatalf("expected error for empty gcid")
	}
}

func TestReaction_New_RejectsEmptyPost(t *testing.T) {
	t.Parallel()
	if _, err := reaction.NewReaction(tenantA, gcidA, "", reaction.TypeLike); err == nil {
		t.Fatalf("expected error for empty post_id")
	}
}

// Idempotency: same (gcid, post_id, type) returns the SAME reaction record.
// Per the brief: NOT a 409 — must return existing.
func TestRegistry_React_IdempotentOnSameTriple(t *testing.T) {
	t.Parallel()
	reg := reaction.NewRegistry()
	r1, _, err := reg.React(context.Background(), tenantA, gcidA, postA, reaction.TypeCurious)
	if err != nil {
		t.Fatalf("React #1: unexpected error: %v", err)
	}
	r2, created, err := reg.React(context.Background(), tenantA, gcidA, postA, reaction.TypeCurious)
	if err != nil {
		t.Fatalf("React #2: unexpected error: %v", err)
	}
	if r1.ID != r2.ID {
		t.Fatalf("expected same reaction ID on repeat (idempotent), got %q vs %q", r1.ID, r2.ID)
	}
	if created {
		t.Fatalf("expected created=false on the idempotent repeat")
	}
}

func TestRegistry_React_DifferentTypeIsDistinct(t *testing.T) {
	t.Parallel()
	reg := reaction.NewRegistry()
	r1, _, _ := reg.React(context.Background(), tenantA, gcidA, postA, reaction.TypeCurious)
	r2, created, err := reg.React(context.Background(), tenantA, gcidA, postA, reaction.TypeLike)
	if err != nil {
		t.Fatalf("React with different type: %v", err)
	}
	if r1.ID == r2.ID {
		t.Fatalf("different reaction types must produce distinct records")
	}
	if !created {
		t.Fatalf("expected created=true for a new (gcid, post, type) triple")
	}
}

func TestRegistry_React_DifferentGCIDsAreIndependent(t *testing.T) {
	t.Parallel()
	reg := reaction.NewRegistry()
	r1, _, _ := reg.React(context.Background(), tenantA, gcidA, postA, reaction.TypeCurious)
	r2, created, err := reg.React(context.Background(), tenantA, gcidB, postA, reaction.TypeCurious)
	if err != nil {
		t.Fatalf("React: %v", err)
	}
	if r1.ID == r2.ID {
		t.Fatalf("different gcids must produce distinct reactions")
	}
	if !created {
		t.Fatalf("expected created=true for different gcid")
	}
}

func TestRegistry_Unreact_RemovesAndReturnsTrue(t *testing.T) {
	t.Parallel()
	reg := reaction.NewRegistry()
	if _, _, err := reg.React(context.Background(), tenantA, gcidA, postA, reaction.TypeCurious); err != nil {
		t.Fatalf("React: %v", err)
	}
	removed, _ := reg.Unreact(context.Background(), gcidA, postA, reaction.TypeCurious)
	if !removed {
		t.Fatalf("expected Unreact to return true on existing reaction")
	}
	// Calling Unreact again returns false (already gone).
	if ok, _ := reg.Unreact(context.Background(), gcidA, postA, reaction.TypeCurious); ok {
		t.Fatalf("expected Unreact to return false the second time")
	}
}

func TestRegistry_ListByPost_ReturnsAllForPost(t *testing.T) {
	t.Parallel()
	reg := reaction.NewRegistry()
	_, _, _ = reg.React(context.Background(), tenantA, gcidA, postA, reaction.TypeCurious)
	_, _, _ = reg.React(context.Background(), tenantA, gcidB, postA, reaction.TypeLike)
	out, _ := reg.ListByPost(context.Background(), postA)
	if len(out) != 2 {
		t.Fatalf("ListByPost: expected 2 entries, got %d", len(out))
	}
}

func TestType_AllValid(t *testing.T) {
	t.Parallel()
	cases := []reaction.Type{
		reaction.TypeCurious,
		reaction.TypeInsightful,
		reaction.TypeLike,
		reaction.TypeInspired,
	}
	for _, c := range cases {
		c := c
		t.Run(string(c), func(t *testing.T) {
			t.Parallel()
			if _, err := reaction.NewReaction(tenantA, gcidA, postA, c); err != nil {
				t.Fatalf("expected %q to be valid: %v", c, err)
			}
		})
	}
}
