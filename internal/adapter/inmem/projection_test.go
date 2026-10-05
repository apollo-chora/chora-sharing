// projection_test.go — exercises the in-memory AtomProjectionReader/Writer:
// get with absent/archived handling, idempotent upsert, invalidate, the
// ADR-229 reuse-visibility setter, and the ListRandom matchmake fallback.
package inmem_test

import (
	"context"
	"errors"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
)

func mustProjection(atomID, owner string) atom_projection.Projection {
	return atom_projection.Projection{
		AtomID:            atomID,
		TenantID:          tenantA,
		RevisionID:        "rev-" + atomID,
		OwnerGCID:         owner,
		AuthorDisplayName: "display-" + owner,
		QuestionType:      atom_projection.QuestionTypeMCQ,
	}
}

func TestProjectionRepo_GetUpsertInvalidate(t *testing.T) {
	t.Parallel()
	repo := inmem.NewProjectionRepo()
	ctx := context.Background()

	// Get before any upsert → ErrNotFound.
	if _, err := repo.Get(ctx, "atom-1"); !errors.Is(err, atom_projection.ErrNotFound) {
		t.Fatalf("Get missing: expected ErrNotFound, got %v", err)
	}

	// Upsert then Get.
	if err := repo.Upsert(ctx, mustProjection("atom-1", gcidA)); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	got, err := repo.Get(ctx, "atom-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.AtomID != "atom-1" || got.OwnerGCID != gcidA {
		t.Fatalf("Get: mismatched projection %+v", got)
	}

	// Re-publish overwrites with the latest revision (idempotent on atom_id).
	refreshed := mustProjection("atom-1", gcidA)
	refreshed.RevisionID = "rev-2"
	if err := repo.Upsert(ctx, refreshed); err != nil {
		t.Fatalf("re-Upsert: %v", err)
	}
	got, _ = repo.Get(ctx, "atom-1")
	if got.RevisionID != "rev-2" {
		t.Fatalf("re-publish must refresh the revision; got %q", got.RevisionID)
	}

	// Invalidate → archived → Get returns ErrNotFound.
	if err := repo.Invalidate(ctx, "atom-1"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	if _, err := repo.Get(ctx, "atom-1"); !errors.Is(err, atom_projection.ErrNotFound) {
		t.Fatalf("Get after invalidate: expected ErrNotFound, got %v", err)
	}
	// Invalidate on an unknown atom → ErrNotFound.
	if err := repo.Invalidate(ctx, "atom-ghost"); !errors.Is(err, atom_projection.ErrNotFound) {
		t.Fatalf("Invalidate missing: expected ErrNotFound, got %v", err)
	}
}

func TestProjectionRepo_SetReuseVisibility(t *testing.T) {
	t.Parallel()
	repo := inmem.NewProjectionRepo()
	ctx := context.Background()

	// Unrecognised label → ErrInvalidArgument.
	if err := repo.SetReuseVisibility(ctx, "atom-1", "public"); !errors.Is(err, atom_projection.ErrInvalidArgument) {
		t.Fatalf("invalid visibility: expected ErrInvalidArgument, got %v", err)
	}
	// Missing projection row is NOT an error (0-row semantics).
	if err := repo.SetReuseVisibility(ctx, "atom-1", "private"); err != nil {
		t.Fatalf("missing row must be a silent no-op; got %v", err)
	}

	// Existing row gets the new audience.
	if err := repo.Upsert(ctx, mustProjection("atom-1", gcidA)); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := repo.SetReuseVisibility(ctx, "atom-1", "friends"); err != nil {
		t.Fatalf("SetReuseVisibility: %v", err)
	}
	got, _ := repo.Get(ctx, "atom-1")
	if got.ReuseVisibility != "friends" {
		t.Fatalf("visibility not persisted; got %q", got.ReuseVisibility)
	}
}

func TestProjectionRepo_ListRandom(t *testing.T) {
	t.Parallel()
	repo := inmem.NewProjectionRepo()
	ctx := context.Background()

	_ = repo.Upsert(ctx, mustProjection("a1", gcidA))       // owned by excluded gcidA
	_ = repo.Upsert(ctx, mustProjection("a2", "other"))     // eligible
	archived := mustProjection("a3", "other")
	archived.Archived = true
	_ = repo.Upsert(ctx, archived)                          // archived → skipped
	_ = repo.Upsert(ctx, mustProjection("a4", gcidA))       // owned by excluded gcidA

	// Exclude gcidA → only a2 remains; limit 1 breaks after filling.
	out, err := repo.ListRandom(ctx, tenantA, []string{gcidA}, 1)
	if err != nil {
		t.Fatalf("ListRandom: %v", err)
	}
	if len(out) != 1 || out[0].AtomID != "a2" {
		t.Fatalf("ListRandom exclude: want [a2], got %+v", out)
	}

	// No excludes → a1 + a2 + a4 (a3 archived); limit 10.
	out, err = repo.ListRandom(ctx, tenantA, nil, 10)
	if err != nil {
		t.Fatalf("ListRandom(nil): %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("ListRandom nil exclude: want 3, got %d", len(out))
	}
}