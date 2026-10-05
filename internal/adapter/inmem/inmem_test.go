// Package inmem_test exercises the in-memory adapter for the Post repo
// with focus on tenant filtering, soft-delete invisibility, and pagination
// math.
package inmem_test

import (
	"context"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/post"
)

const (
	tenantA = "01970000-0000-7000-8000-000000000001"
	tenantB = "01970000-0000-7000-8000-000000000002"
	gcidA   = "01970000-0000-7000-9000-000000000001"
	atomA   = "01970000-0000-7000-a000-000000000001"
)

func TestPostRepo_SaveAndGet(t *testing.T) {
	t.Parallel()
	repo := inmem.NewPostRepo()
	p, _ := post.NewPost(tenantA, gcidA, "x", atomA, nil)
	_ = repo.Save(context.Background(), p)
	got, ok, _ := repo.Get(context.Background(), p.ID)
	if !ok {
		t.Fatalf("Get: expected hit")
	}
	if got.ID != p.ID {
		t.Fatalf("Get: ID mismatch")
	}
	if _, ok, _ := repo.Get(context.Background(), "nope"); ok {
		t.Fatalf("Get: expected miss")
	}
}

func TestPostRepo_ListByTenant_FiltersByTenantAndSoftDelete(t *testing.T) {
	t.Parallel()
	repo := inmem.NewPostRepo()
	a1, _ := post.NewPost(tenantA, gcidA, "a1", atomA, nil)
	a2, _ := post.NewPost(tenantA, gcidA, "a2", atomA, nil)
	b1, _ := post.NewPost(tenantB, gcidA, "b1", atomA, nil)
	_ = repo.Save(context.Background(), a1)
	_ = repo.Save(context.Background(), a2)
	_ = repo.Save(context.Background(), b1)
	a2.SoftDelete()
	_ = repo.Save(context.Background(), a2)

	out, total, _ := repo.ListByTenant(context.Background(), tenantA, 0, 50)
	if total != 1 || len(out) != 1 {
		t.Fatalf("ListByTenant: expected total=1,len=1; got total=%d,len=%d", total, len(out))
	}
	if out[0].ID != a1.ID {
		t.Fatalf("ListByTenant: expected a1, got %q", out[0].ID)
	}
}

func TestPostRepo_ListByTenant_PagingEdgeCases(t *testing.T) {
	t.Parallel()
	repo := inmem.NewPostRepo()
	for i := 0; i < 3; i++ {
		p, _ := post.NewPost(tenantA, gcidA, "x", atomA, nil)
		_ = repo.Save(context.Background(), p)
		// UUIDv7 carries unix_ts_ms; force monotonic spread so sort is stable.
		time.Sleep(2 * time.Millisecond)
	}
	// offset > total: empty slice, total=3
	out, total, _ := repo.ListByTenant(context.Background(), tenantA, 100, 10)
	if total != 3 || len(out) != 0 {
		t.Fatalf("offset>total: expected len=0 total=3, got len=%d total=%d", len(out), total)
	}
	// limit smaller than total
	out, _, _ = repo.ListByTenant(context.Background(), tenantA, 0, 2)
	if len(out) != 2 {
		t.Fatalf("limit=2: expected 2 items, got %d", len(out))
	}
	// offset + limit overflow
	out, _, _ = repo.ListByTenant(context.Background(), tenantA, 2, 50)
	if len(out) != 1 {
		t.Fatalf("offset=2,limit=50: expected 1 item, got %d", len(out))
	}
}
