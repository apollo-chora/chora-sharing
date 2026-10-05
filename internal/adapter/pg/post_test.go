// post_test.go — unit tests for the pgx-backed PostRepository.
//
// Mirrors chora-delivery's pg unit-test pattern: stub the Querier so the
// SQL surface is exercised without a live DB. Live RLS isolation is
// verified separately in integration_test.go.
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-common/tracing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/domain/post"
)

// tenantID + authorGCID come from the shared harness_test.go.
const postID = "01970000-0000-7000-a000-000000000001"

// Stub Querier / Row / Rows / TxRunner now live in the SHARED harness_test.go,
// which the pg package grew while this adapter was deleted (470ec0ef9). One
// harness, not two — a second copy drifts.

func TestPostRepository_NilTxRunner_ReturnsErrNotImplemented(t *testing.T) {
	t.Parallel()
	r := pg.NewPostRepository(nil)
	if err := r.Save(context.Background(), &post.Post{}); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("Save: expected ErrNotImplemented; got %v", err)
	}
	if _, _, err := r.Get(context.Background(), postID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("Get: expected ErrNotImplemented; got %v", err)
	}
	if _, _, err := r.ListByTenant(context.Background(), tenantID, 0, 10); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("ListByTenant: expected ErrNotImplemented; got %v", err)
	}
}

func TestPostRepository_Save_RejectsNil(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	tx := &stubTxRunner{q: q}
	r := pg.NewPostRepository(tx)

	ctx := tracing.WithTenantID(context.Background(), tenantID)
	if err := r.Save(ctx, nil); !errors.Is(err, pg.ErrInvalidPost) {
		t.Fatalf("expected ErrInvalidPost; got %v", err)
	}
}

func TestPostRepository_Save_AppliesRLSThenUpserts(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	tx := &stubTxRunner{q: q}
	r := pg.NewPostRepository(tx)

	p, err := post.NewPost(tenantID, authorGCID, "RED test post", "", []string{"tag1"})
	if err != nil {
		t.Fatalf("NewPost: %v", err)
	}
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	if err := r.Save(ctx, p); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if len(q.sqls) < 2 {
		t.Fatalf("expected SET LOCAL + UPSERT; got %d SQLs", len(q.sqls))
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL chora.tenant_id") {
		t.Fatalf("first SQL must be SET LOCAL chora.tenant_id; got %q", q.sqls[0])
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "INSERT INTO posts") {
		t.Fatalf("expected INSERT INTO posts; got %q", last)
	}
	if !strings.Contains(last, "ON CONFLICT (post_id) DO UPDATE") {
		t.Fatalf("expected UPSERT clause; got %q", last)
	}
}

func TestPostRepository_Save_DefaultsVisibilityToPublic(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	tx := &stubTxRunner{q: q}
	r := pg.NewPostRepository(tx)

	p := &post.Post{
		ID:         postID,
		TenantID:   tenantID,
		AuthorGCID: authorGCID,
		Body:       "no visibility set",
		// Visibility intentionally empty — Save must default to public so
		// the post_visibility ENUM cast does not blow up.
	}
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	if err := r.Save(ctx, p); err != nil {
		t.Fatalf("Save: %v", err)
	}
	// Last SQL is the INSERT; arg index 6 (0-based) is visibility.
	lastArgs := q.args[len(q.args)-1]
	if got, ok := lastArgs[6].(string); !ok || got != "public" {
		t.Fatalf("expected visibility 'public' default; got %v", lastArgs[6])
	}
}

func TestPostRepository_Get_NoRow_ReturnsOkFalse(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error {
				return errors.New("no rows in result set")
			}}
		},
	}
	tx := &stubTxRunner{q: q}
	r := pg.NewPostRepository(tx)
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	p, ok, err := r.Get(ctx, "nope")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok || p != nil {
		t.Fatalf("expected ok=false / nil for unknown id")
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL chora.tenant_id") {
		t.Fatalf("first SQL must be SET LOCAL chora.tenant_id; got %q", q.sqls[0])
	}
}

func TestPostRepository_Get_HappyPath(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error {
				// 10 columns per scanPost.
				if v, ok := dest[0].(*string); ok {
					*v = postID
				}
				if v, ok := dest[1].(*string); ok {
					*v = tenantID
				}
				if v, ok := dest[2].(*string); ok {
					*v = authorGCID
				}
				if v, ok := dest[3].(*string); ok {
					*v = "body"
				}
				if v, ok := dest[4].(*string); ok {
					*v = ""
				}
				if v, ok := dest[5].(*[]string); ok {
					*v = []string{"a", "b"}
				}
				if v, ok := dest[6].(*string); ok {
					*v = "public"
				}
				return nil
			}}
		},
	}
	tx := &stubTxRunner{q: q}
	r := pg.NewPostRepository(tx)
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	p, ok, err := r.Get(ctx, postID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || p == nil {
		t.Fatalf("expected ok=true / non-nil")
	}
	if p.ID != postID || p.TenantID != tenantID || p.Visibility != post.VisibilityPublic {
		t.Fatalf("scanned post mismatched: %+v", p)
	}
}

func TestPostRepository_ListByTenant_ReturnsRows(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{
				scans: []func(dest ...any) error{
					func(dest ...any) error {
						if v, ok := dest[0].(*string); ok {
							*v = "p-1"
						}
						if v, ok := dest[1].(*string); ok {
							*v = tenantID
						}
						if v, ok := dest[6].(*string); ok {
							*v = "tenant"
						}
						return nil
					},
				},
			}, nil
		},
	}
	tx := &stubTxRunner{q: q}
	r := pg.NewPostRepository(tx)
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	items, total, err := r.ListByTenant(ctx, tenantID, 0, 10)
	if err != nil {
		t.Fatalf("ListByTenant: %v", err)
	}
	if len(items) != 1 || total != 1 {
		t.Fatalf("expected 1 item; got %d / %d", len(items), total)
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL chora.tenant_id") {
		t.Fatalf("first SQL must be SET LOCAL chora.tenant_id; got %q", q.sqls[0])
	}
	if !strings.Contains(q.sqls[1], "FROM posts") || !strings.Contains(q.sqls[1], "deleted_at IS NULL") {
		t.Fatalf("expected SELECT … FROM posts with soft-delete filter; got %q", q.sqls[1])
	}
}

func TestSQLPostTemplates_AreExported(t *testing.T) {
	t.Parallel()
	if !strings.Contains(pg.SQLUpsertPost, "INSERT INTO posts") {
		t.Fatalf("SQLUpsertPost malformed")
	}
	if !strings.Contains(pg.SQLUpsertPost, "ON CONFLICT (post_id) DO UPDATE") {
		t.Fatalf("SQLUpsertPost missing UPSERT clause")
	}
	if !strings.Contains(pg.SQLSelectPostByID, "FROM posts") {
		t.Fatalf("SQLSelectPostByID malformed")
	}
	if !strings.Contains(pg.SQLListPostsByTenant, "FROM posts") {
		t.Fatalf("SQLListPostsByTenant malformed")
	}
}
