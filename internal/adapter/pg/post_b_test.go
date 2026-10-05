// post_b_test.go — extra coverage for PostRepository (second coverage agent):
// error branches not exercised by post_test.go (Exec failure, query/scan/
// rows.Err on List, scanPost deleted_at populate).
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/domain/post"
)

func TestPostRepository_Save_ExecError(t *testing.T) {
	t.Parallel()
	q := &stubQuerierB{
		execErrFn: func(sql string) error {
			if strings.Contains(sql, "SET LOCAL") {
				return nil
			}
			return errors.New("boom")
		},
	}
	r := pg.NewPostRepository(&stubTxRunnerB{q: q})
	p := &post.Post{ID: postID, TenantID: tenantID, AuthorGCID: authorGCID, Body: "b"}
	err := r.Save(tracing.WithTenantID(context.Background(), tenantID), p)
	if err == nil || !strings.Contains(err.Error(), "upsert post") {
		t.Fatalf("expected wrapped upsert error; got %v", err)
	}
}

func TestPostRepository_Save_NilTagsBindEmptyArray(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewPostRepository(&stubTxRunner{q: q})
	p := &post.Post{ID: postID, TenantID: tenantID, AuthorGCID: authorGCID, Body: "b"}
	if err := r.Save(tracing.WithTenantID(context.Background(), tenantID), p); err != nil {
		t.Fatalf("Save: %v", err)
	}
	tags := q.args[len(q.args)-1][5]
	arr, ok := tags.([]string)
	if !ok || len(arr) != 0 {
		t.Fatalf("nil tags must bind empty []string; got %#v", tags)
	}
}

func TestPostRepository_Get_ScanErrorIsOkFalse(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error { return errors.New("no rows in result set") }}
		},
	}
	r := pg.NewPostRepository(&stubTxRunner{q: q})
	p, ok, err := r.Get(tracing.WithTenantID(context.Background(), tenantID), postID)
	if err != nil || ok || p != nil {
		t.Fatalf("expected ok=false, nil, nil-err; got %+v %v %v", p, ok, err)
	}
}

func TestPostRepository_Get_HappyPathWithDeletedAt(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error {
				bSetInto(dest, 0, postID)
				bSetInto(dest, 1, tenantID)
				bSetInto(dest, 2, authorGCID)
				bSetInto(dest, 3, "body")
				bSetInto(dest, 4, "atom-1")
				bSetInto(dest, 5, []string{"t1"})
				bSetInto(dest, 6, "tenant")
				bSetInto(dest, 7, time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC))
				bSetInto(dest, 8, time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC))
				bSetInto(dest, 9, bPtr(time.Date(2026, 7, 10, 13, 0, 0, 0, time.UTC)))
				return nil
			}}
		},
	}
	r := pg.NewPostRepository(&stubTxRunner{q: q})
	p, ok, err := r.Get(tracing.WithTenantID(context.Background(), tenantID), postID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || p == nil || p.DeletedAt == nil || p.AtomID != "atom-1" || p.Tags[0] != "t1" {
		t.Fatalf("scan wrong: ok=%v p=%+v", ok, p)
	}
}

func TestPostRepository_ListByTenant_Errors(t *testing.T) {
	t.Parallel()

	t.Run("query error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("q") }}
		r := pg.NewPostRepository(&stubTxRunner{q: q})
		_, _, err := r.ListByTenant(tracing.WithTenantID(context.Background(), tenantID), tenantID, 0, 10)
		if err == nil {
			t.Fatalf("query error must propagate")
		}
	})

	t.Run("scan error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{func(dest ...any) error { return errors.New("s") }}}, nil
		}}
		r := pg.NewPostRepository(&stubTxRunner{q: q})
		if _, _, err := r.ListByTenant(tracing.WithTenantID(context.Background(), tenantID), tenantID, 0, 10); err == nil {
			t.Fatalf("scan error must propagate")
		}
	})

	t.Run("rows.Err", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{}, err: errors.New("r")}, nil
		}}
		r := pg.NewPostRepository(&stubTxRunner{q: q})
		if _, _, err := r.ListByTenant(tracing.WithTenantID(context.Background(), tenantID), tenantID, 0, 10); err == nil {
			t.Fatalf("rows.Err must propagate")
		}
	})
}

func TestScannerHelpersB_TagsToArrayNilViaSave(t *testing.T) {
	// tagsToArray(nil) must yield an empty array, not nil (exercised via
	// Save with nil tags above); direct assertion not needed — keep coverage
	// signal by exercising post-tag bind path once more with explicit tags.
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewPostRepository(&stubTxRunner{q: q})
	p := &post.Post{ID: postID, TenantID: tenantID, AuthorGCID: authorGCID, Body: "b", Tags: []string{"x", "y"}}
	if err := r.Save(tracing.WithTenantID(context.Background(), tenantID), p); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if tags, _ := q.args[len(q.args)-1][5].([]string); len(tags) != 2 {
		t.Fatalf("tags must pass through; got %v", q.args[len(q.args)-1][5])
	}
}

func TestPostRepository_ListByTenant_DefaultLimitAndEmpty(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewPostRepository(&stubTxRunner{q: q})
	items, total, err := r.ListByTenant(tracing.WithTenantID(context.Background(), tenantID), tenantID, 5, 0)
	if err != nil {
		t.Fatalf("ListByTenant: %v", err)
	}
	if len(items) != 0 || total != 0 {
		t.Fatalf("expected 0 items; got %d / %d", len(items), total)
	}
	for i, a := range q.args[len(q.args)-1] {
		if i == 1 {
			if lim, ok := a.(int); !ok || lim != 50 {
				t.Fatalf("limit 0 must default to 50; got %v", a)
			}
		}
	}
}
