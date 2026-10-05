// profiler_test.go — exercises the in-memory ProfilerRepo: save with
// deep-copy semantics, get with missing-profile errors, and the
// always-empty display-name resolver.
package inmem_test

import (
	"context"
	"errors"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/profiler"
)

func TestProfilerRepo_SaveGet(t *testing.T) {
	t.Parallel()
	repo := inmem.NewProfilerRepo()
	ctx := context.Background()

	// Get before save → ErrProfileNotFound.
	if _, err := repo.GetProfile(ctx, "g-1"); !errors.Is(err, inmem.ErrProfileNotFound) {
		t.Fatalf("GetProfile missing: expected ErrProfileNotFound, got %v", err)
	}
	// Saving nil → error.
	if err := repo.SaveProfile(ctx, nil); !errors.Is(err, inmem.ErrProfileNotFound) {
		t.Fatalf("SaveProfile(nil): expected ErrProfileNotFound, got %v", err)
	}

	p, err := profiler.NewProfile("g-1", "t-1", "bio", []string{"course-a", "course-b"})
	if err != nil {
		t.Fatalf("NewProfile: %v", err)
	}
	p.AddTag(profiler.CategoryProgramming, "golang")
	p.SetTags([]profiler.InterestTag{
		{Category: profiler.CategoryMathematics, Tag: "calculus"},
	})
	if err := repo.SaveProfile(ctx, p); err != nil {
		t.Fatalf("SaveProfile: %v", err)
	}

	got, err := repo.GetProfile(ctx, "g-1")
	if err != nil {
		t.Fatalf("GetProfile: %v", err)
	}
	if got.GCID != "g-1" || got.Bio != "bio" {
		t.Fatalf("GetProfile: mismatched profile %+v", got)
	}
	if len(got.Tags) != 1 || got.Tags[0].Tag != "calculus" {
		t.Fatalf("GetProfile tags: %+v", got.Tags)
	}
	if len(got.CourseTitles) != 2 {
		t.Fatalf("GetProfile course titles: want 2, got %d", len(got.CourseTitles))
	}

	// The stored profile is a deep copy: mutating the fetch must not leak.
	got.Tags[0].Tag = "mutated"
	got.CourseTitles[0] = "mutated-course"
	again, err := repo.GetProfile(ctx, "g-1")
	if err != nil {
		t.Fatalf("GetProfile again: %v", err)
	}
	if again.Tags[0].Tag != "calculus" || again.CourseTitles[0] != "course-a" {
		t.Fatalf("clone independence violated: %+v", again)
	}

	// Display names are not stored on the in-memory profile → empty map.
	names, err := repo.ResolveDisplayNames(ctx, []string{"g-1", "g-2"})
	if err != nil {
		t.Fatalf("ResolveDisplayNames: %v", err)
	}
	if len(names) != 0 {
		t.Fatalf("ResolveDisplayNames: want empty map, got %v", names)
	}
}