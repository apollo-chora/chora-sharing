// comment_test.go — exercises the in-memory CommentRepo: the 1-level
// reply constraint enforced at insert time, keyset pagination (newest-
// first), and author-only update/delete.
package inmem_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/comment"
	"github.com/apollo-chora/chora-sharing/internal/keyset"
)

func TestCommentRepo_Create_Constraints(t *testing.T) {
	t.Parallel()
	repo := inmem.NewCommentRepo()
	ctx := context.Background()

	top, err := comment.NewComment("t1", "post-1", "g1", "top-level", "")
	if err != nil {
		t.Fatalf("NewComment: %v", err)
	}
	if err := repo.Create(ctx, top); err != nil {
		t.Fatalf("Create top-level: %v", err)
	}

	// A valid 1-level reply.
	reply, err := comment.NewComment("t1", "post-1", "g2", "reply", top.ID)
	if err != nil {
		t.Fatalf("NewComment reply: %v", err)
	}
	if err := repo.Create(ctx, reply); err != nil {
		t.Fatalf("Create reply: %v", err)
	}

	// Reply to a missing parent → ErrNotFound.
	orphan, err := comment.NewComment("t1", "post-1", "g3", "orphan", comment.NewUUIDv7())
	if err != nil {
		t.Fatalf("NewComment orphan: %v", err)
	}
	if err := repo.Create(ctx, orphan); !errors.Is(err, comment.ErrNotFound) {
		t.Fatalf("orphan reply: expected ErrNotFound, got %v", err)
	}

	// Reply to a reply (2 levels) → ErrInvalidArgument.
	grandchild, err := comment.NewComment("t1", "post-1", "g4", "too deep", reply.ID)
	if err != nil {
		t.Fatalf("NewComment grandchild: %v", err)
	}
	if err := repo.Create(ctx, grandchild); !errors.Is(err, comment.ErrInvalidArgument) {
		t.Fatalf("2-level reply: expected ErrInvalidArgument, got %v", err)
	}

	// Reply whose parent lives on a different post → ErrInvalidArgument.
	crossPost, err := comment.NewComment("t1", "post-2", "g5", "cross-post", top.ID)
	if err != nil {
		t.Fatalf("NewComment cross-post: %v", err)
	}
	if err := repo.Create(ctx, crossPost); !errors.Is(err, comment.ErrInvalidArgument) {
		t.Fatalf("cross-post reply: expected ErrInvalidArgument, got %v", err)
	}
}

func TestCommentRepo_ListByPost_Pagination(t *testing.T) {
	t.Parallel()
	repo := inmem.NewCommentRepo()
	ctx := context.Background()

	var ids []string
	for i := 0; i < 5; i++ {
		c, err := comment.NewComment("t1", "post-1", "g1", fmt.Sprintf("body-%d", i), "")
		if err != nil {
			t.Fatalf("NewComment: %v", err)
		}
		ids = append(ids, c.ID)
		if err := repo.Create(ctx, c); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}
	// Another post's comments are not visible under post-1.
	other, err := comment.NewComment("t1", "post-2", "g1", "other post", "")
	if err != nil {
		t.Fatalf("NewComment: %v", err)
	}
	if err := repo.Create(ctx, other); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Newest-first is creation order reversed, because the feed orders on
	// created_at. It is NOT the ids sorted lexicographically: all five are
	// minted inside the same millisecond, so their UUIDv7 timestamps tie
	// and only the random bits break it.
	sorted := make([]string, 0, len(ids))
	for i := len(ids) - 1; i >= 0; i-- {
		sorted = append(sorted, ids[i])
	}

	// Page 1: limit 2, newest first.
	page1, next1, err := repo.ListByPost(ctx, "post-1", 2, "")
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if len(page1) != 2 || page1[0].ID != sorted[0] || page1[1].ID != sorted[1] {
		t.Fatalf("page1: got %+v", page1)
	}
	// The cursor is the opaque (created_at, id) keyset token, not a bare id.
	if _, curID := keyset.Decode(next1); curID != sorted[1] {
		t.Fatalf("page1 next: want a keyset token for %s, got %q", sorted[1], next1)
	}

	// Page 2 from cursor.
	page2, next2, err := repo.ListByPost(ctx, "post-1", 2, next1)
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if len(page2) != 2 || page2[0].ID != sorted[2] || page2[1].ID != sorted[3] {
		t.Fatalf("page2: got %+v", page2)
	}
	if _, curID := keyset.Decode(next2); curID != sorted[3] {
		t.Fatalf("page2 next: want a keyset token for %s, got %q", sorted[3], next2)
	}

	// Page 3: last row, no next cursor.
	page3, next3, err := repo.ListByPost(ctx, "post-1", 2, next2)
	if err != nil {
		t.Fatalf("page3: %v", err)
	}
	if len(page3) != 1 || page3[0].ID != sorted[4] || next3 != "" {
		t.Fatalf("page3: got %d rows next=%q", len(page3), next3)
	}

	// Cursor at the last row gives an empty page. The cursor has to be that
	// row keyset token, not its bare id.
	lastCursor := keyset.Encode(page3[0].CreatedAt, page3[0].ID)
	page4, next4, err := repo.ListByPost(ctx, "post-1", 10, lastCursor)
	if err != nil {
		t.Fatalf("page4: %v", err)
	}
	if len(page4) != 0 || next4 != "" {
		t.Fatalf("page4: got %d rows next=%q", len(page4), next4)
	}

	// Unknown cursor → treated as first page.
	full, _, err := repo.ListByPost(ctx, "post-1", 10, "unknown-cursor")
	if err != nil {
		t.Fatalf("unknown cursor: %v", err)
	}
	if len(full) != 5 {
		t.Fatalf("unknown cursor: want 5, got %d", len(full))
	}

	// limit <= 0 → default of 50.
	all, _, err := repo.ListByPost(ctx, "post-1", 0, "")
	if err != nil {
		t.Fatalf("limit<=0: %v", err)
	}
	if len(all) != 5 {
		t.Fatalf("limit<=0: want 5, got %d", len(all))
	}

	// Post without comments → empty.
	empty, _, err := repo.ListByPost(ctx, "post-none", 10, "")
	if err != nil {
		t.Fatalf("empty post: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("empty post: want 0, got %d", len(empty))
	}
}

func TestCommentRepo_UpdateAndDelete(t *testing.T) {
	t.Parallel()
	repo := inmem.NewCommentRepo()
	ctx := context.Background()

	author, err := comment.NewComment("t1", "post-1", "g1", "original", "")
	if err != nil {
		t.Fatalf("NewComment: %v", err)
	}
	other, err := comment.NewComment("t1", "post-1", "g2", "other", "")
	if err != nil {
		t.Fatalf("NewComment: %v", err)
	}
	_ = repo.Create(ctx, author)
	_ = repo.Create(ctx, other)

	// Author-only update.
	if err := repo.Update(ctx, author.ID, "g1", "edited"); err != nil {
		t.Fatalf("Update: %v", err)
	}
	list, _, _ := repo.ListByPost(ctx, "post-1", 10, "")
	for _, c := range list {
		if c.ID == author.ID && c.Body != "edited" {
			t.Fatalf("Update: body not persisted")
		}
	}

	// Wrong author → ErrNotFound.
	if err := repo.Update(ctx, author.ID, "g2", "hijack"); !errors.Is(err, comment.ErrNotFound) {
		t.Fatalf("update wrong author: expected ErrNotFound, got %v", err)
	}
	// Missing comment → ErrNotFound.
	if err := repo.Update(ctx, comment.NewUUIDv7(), "g1", "x"); !errors.Is(err, comment.ErrNotFound) {
		t.Fatalf("update missing: expected ErrNotFound, got %v", err)
	}

	// Author-only delete.
	if err := repo.Delete(ctx, author.ID, "g1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	list, _, _ = repo.ListByPost(ctx, "post-1", 10, "")
	if len(list) != 1 || list[0].ID != other.ID {
		t.Fatalf("after delete: want only the other comment, got %d", len(list))
	}

	// Missing comment → ErrNotFound.
	if err := repo.Delete(ctx, comment.NewUUIDv7(), "g1"); !errors.Is(err, comment.ErrNotFound) {
		t.Fatalf("delete missing: expected ErrNotFound, got %v", err)
	}
	// Wrong author → ErrNotFound.
	if err := repo.Delete(ctx, other.ID, "g1"); !errors.Is(err, comment.ErrNotFound) {
		t.Fatalf("delete wrong author: expected ErrNotFound, got %v", err)
	}
	// Deleted id is not re-queryable → ErrNotFound.
	if err := repo.Delete(ctx, author.ID, "g1"); !errors.Is(err, comment.ErrNotFound) {
		t.Fatalf("re-delete: expected ErrNotFound, got %v", err)
	}
}