package inmem

import (
	"context"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/bookmark"
)

func TestBookmarkRepo_SaveIdempotent(t *testing.T) {
	t.Parallel()
	r := NewBookmarkRepo()
	ctx := context.Background()

	bm1, _ := bookmark.NewBookmark("t1", "g1", "a1", "r1")
	if err := r.Save(ctx, bm1); err != nil {
		t.Fatalf("first save: %v", err)
	}
	// Second save with different bookmark for same (gcid, atom_id) is a no-op.
	bm2, _ := bookmark.NewBookmark("t1", "g1", "a1", "r2")
	if err := r.Save(ctx, bm2); err != nil {
		t.Fatalf("second save: %v", err)
	}

	got, _, err := r.ListByOwner(ctx, "t1", "g1", "", 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 bookmark (idempotent), got %d", len(got))
	}
	if got[0].AtomRevisionID != "r1" {
		t.Errorf("expected original revision r1 preserved, got %q", got[0].AtomRevisionID)
	}
}

func TestBookmarkRepo_Delete(t *testing.T) {
	t.Parallel()
	r := NewBookmarkRepo()
	ctx := context.Background()

	bm, _ := bookmark.NewBookmark("t1", "g1", "a1", "r1")
	_ = r.Save(ctx, bm)

	if err := r.Delete(ctx, "t1", "g1", "a1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got, _, _ := r.ListByOwner(ctx, "t1", "g1", "", 10)
	if len(got) != 0 {
		t.Fatalf("expected 0 after delete, got %d", len(got))
	}

	// Idempotent delete — no error on absent bookmark.
	if err := r.Delete(ctx, "t1", "g1", "a1"); err != nil {
		t.Fatalf("idempotent delete: %v", err)
	}
}

func TestBookmarkRepo_ListByOwner_TenantIsolation(t *testing.T) {
	t.Parallel()
	r := NewBookmarkRepo()
	ctx := context.Background()

	// g1 in t1 has 2 bookmarks; g2 in t2 has 1.
	b1, _ := bookmark.NewBookmark("t1", "g1", "a1", "")
	b2, _ := bookmark.NewBookmark("t1", "g1", "a2", "")
	b3, _ := bookmark.NewBookmark("t2", "g2", "a3", "")
	_ = r.Save(ctx, b1)
	_ = r.Save(ctx, b2)
	_ = r.Save(ctx, b3)

	got, _, err := r.ListByOwner(ctx, "t1", "g1", "", 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 for t1/g1, got %d", len(got))
	}
}

func TestBookmarkRepo_ListByOwner_KeysetPagination(t *testing.T) {
	t.Parallel()
	r := NewBookmarkRepo()
	ctx := context.Background()

	// Create 5 bookmarks for g1 in t1.
	var ids []string
	for i := 0; i < 5; i++ {
		bm, _ := bookmark.NewBookmark("t1", "g1", "a"+string(rune('1'+i)), "")
		_ = r.Save(ctx, bm)
		ids = append(ids, bm.ID)
	}

	// Page 1: limit 2.
	page1, next1, err := r.ListByOwner(ctx, "t1", "g1", "", 2)
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("page1 expected 2, got %d", len(page1))
	}
	if next1 == "" {
		t.Fatal("expected next cursor for page1")
	}
	// Newest-first is the last one saved, because the list orders on
	// created_at. It is NOT the lexicographically largest id: all five are
	// minted inside the same millisecond, so their UUIDv7 timestamps tie
	// and only the random bits break it.
	if want := ids[len(ids)-1]; page1[0].ID != want {
		t.Errorf("page1[0] = %s, want %s (newest)", page1[0].ID, want)
	}

	// Page 2: cursor = next1.
	page2, next2, err := r.ListByOwner(ctx, "t1", "g1", next1, 2)
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if len(page2) != 2 {
		t.Fatalf("page2 expected 2, got %d", len(page2))
	}
	if next2 == "" {
		t.Fatal("expected next cursor for page2")
	}

	// Page 3: cursor = next2 — last item.
	page3, next3, err := r.ListByOwner(ctx, "t1", "g1", next2, 2)
	if err != nil {
		t.Fatalf("page3: %v", err)
	}
	if len(page3) != 1 {
		t.Fatalf("page3 expected 1, got %d", len(page3))
	}
	if next3 != "" {
		t.Errorf("page3 expected empty next, got %q", next3)
	}
}

func TestBookmarkRepo_LimitClamping(t *testing.T) {
	t.Parallel()
	r := NewBookmarkRepo()
	ctx := context.Background()
	bm, _ := bookmark.NewBookmark("t1", "g1", "a1", "")
	_ = r.Save(ctx, bm)

	// limit <= 0 → default 20.
	got, _, err := r.ListByOwner(ctx, "t1", "g1", "", 0)
	if err != nil {
		t.Fatalf("limit 0: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("limit 0 expected 1, got %d", len(got))
	}

	// limit > 100 → clamped to 100.
	got, _, err = r.ListByOwner(ctx, "t1", "g1", "", 500)
	if err != nil {
		t.Fatalf("limit 500: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("limit 500 expected 1, got %d", len(got))
	}
}

func TestBookmarkRepo_SaveNilBookmark(t *testing.T) {
	t.Parallel()
	r := NewBookmarkRepo()
	if err := r.Save(context.Background(), nil); err == nil {
		t.Error("expected error saving nil bookmark")
	}
}
