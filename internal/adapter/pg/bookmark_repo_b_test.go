// bookmark_repo_b_test.go — coverage tests for the pgx-backed BookmarkRepo
// (second coverage agent). Exercises Save/Delete/ListByOwner happy + error
// branches via the shared stub harness (+ stubQuerierB for Exec errors).
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/domain/bookmark"
	"github.com/apollo-chora/chora-sharing/internal/keyset"
)

const bookmarkID = "01970000-0000-7000-a000-0000000000c1"

func TestBookmarkRepo_NilTxRunner_ReturnsErrNotImplemented(t *testing.T) {
	t.Parallel()
	r := pg.NewBookmarkRepo(nil)
	if err := r.Save(context.Background(), &bookmark.Bookmark{}); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("Save: expected ErrNotImplemented; got %v", err)
	}
	if err := r.Delete(context.Background(), tenantID, authorGCID, "atom-1"); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("Delete: expected ErrNotImplemented; got %v", err)
	}
	if _, _, err := r.ListByOwner(context.Background(), tenantID, authorGCID, "", 10); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("ListByOwner: expected ErrNotImplemented; got %v", err)
	}
}

func TestBookmarkRepo_Save_RejectsNil(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewBookmarkRepo(&stubTxRunner{q: q})
	if err := r.Save(tracing.WithTenantID(context.Background(), tenantID), nil); !errors.Is(err, bookmark.ErrInvalidArgument) {
		t.Fatalf("expected bookmark.ErrInvalidArgument; got %v", err)
	}
}

func TestBookmarkRepo_Save_AppliesRLSAndInserts(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewBookmarkRepo(&stubTxRunner{q: q})
	b := &bookmark.Bookmark{
		ID:             bookmarkID,
		TenantID:       tenantID,
		GCID:           authorGCID,
		AtomID:         "01970000-0000-7000-a000-0000000000d1",
		AtomRevisionID: "rev-7",
		CreatedAt:      time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
	}
	if err := r.Save(tracing.WithTenantID(context.Background(), tenantID), b); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL chora.tenant_id") {
		t.Fatalf("first SQL must be SET LOCAL; got %q", q.sqls[0])
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "INSERT INTO atom_bookmarks") || !strings.Contains(last, "ON CONFLICT (gcid, atom_id) DO NOTHING") {
		t.Fatalf("expected idempotent INSERT INTO atom_bookmarks; got %q", last)
	}
	args := q.args[len(q.args)-1]
	if len(args) != 6 {
		t.Fatalf("insert binds id, tenant, gcid, atom_id, revision, created = 6 args; got %v", args)
	}
	if rev, ok := args[4].(string); !ok || rev != "rev-7" {
		t.Fatalf("revision arg must be the non-empty string; got %v", args[4])
	}
}

func TestBookmarkRepo_Save_DefaultsCreatedAtAndNilRevision(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewBookmarkRepo(&stubTxRunner{q: q})
	b := &bookmark.Bookmark{ID: bookmarkID, TenantID: tenantID, GCID: authorGCID, AtomID: "a1"}
	if err := r.Save(tracing.WithTenantID(context.Background(), tenantID), b); err != nil {
		t.Fatalf("Save: %v", err)
	}
	args := q.args[len(q.args)-1]
	// CreatedAt (idx 5) must have been defaulted to now (non-zero).
	created, ok := args[5].(time.Time)
	if !ok || created.IsZero() {
		t.Fatalf("created must be defaulted to now; got %v", args[5])
	}
	// Empty revision → nil (NULL), never "" (would fail a uuid cast).
	if args[4] != nil {
		t.Fatalf("empty revision must bind nil; got %v", args[4])
	}
}

func TestBookmarkRepo_Save_ExecErrorPropagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerierB{
		execErrFn: func(sql string) error {
			if strings.Contains(sql, "SET LOCAL") {
				return nil
			}
			return errors.New("boom")
		},
	}
	r := pg.NewBookmarkRepo(&stubTxRunnerB{q: q})
	b := &bookmark.Bookmark{ID: bookmarkID, TenantID: tenantID, GCID: authorGCID, AtomID: "a1"}
	err := r.Save(tracing.WithTenantID(context.Background(), tenantID), b)
	if err == nil || !strings.Contains(err.Error(), "insert bookmark") {
		t.Fatalf("expected wrapped insert error; got %v", err)
	}
}

func TestBookmarkRepo_Delete_AppliesRLSAndDeletes(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewBookmarkRepo(&stubTxRunner{q: q})
	if err := r.Delete(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "atom-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "DELETE FROM atom_bookmarks") || !strings.Contains(last, "gcid = $1") {
		t.Fatalf("expected DELETE FROM atom_bookmarks; got %q", last)
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL") {
		t.Fatalf("first SQL must be SET LOCAL; got %q", q.sqls[0])
	}
}

func TestBookmarkRepo_Delete_ExecErrorPropagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerierB{
		execErrFn: func(sql string) error {
			if strings.Contains(sql, "SET LOCAL") {
				return nil
			}
			return errors.New("boom")
		},
	}
	r := pg.NewBookmarkRepo(&stubTxRunnerB{q: q})
	err := r.Delete(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "atom-1")
	if err == nil || !strings.Contains(err.Error(), "delete bookmark") {
		t.Fatalf("expected wrapped delete error; got %v", err)
	}
}

func TestBookmarkRepo_ListByOwner_PagesAndClamps(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{
				func(dest ...any) error {
					bSetInto(dest, 0, "bm-1")
					bSetInto(dest, 1, tenantID)
					bSetInto(dest, 2, authorGCID)
					bSetInto(dest, 3, "atom-1")
					bSetInto(dest, 4, bPtr("rev-9"))
					bSetInto(dest, 5, time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC))
					return nil
				},
				func(dest ...any) error {
					bSetInto(dest, 0, "bm-2")
					bSetInto(dest, 4, bPtr("rev-2"))
					return nil
				},
			}}, nil
		},
	}
	r := pg.NewBookmarkRepo(&stubTxRunner{q: q})
	// limit 200 → clamped to 100; limit clamps do not affect scan loop.
	items, next, err := r.ListByOwner(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "", 200)
	if err != nil {
		t.Fatalf("ListByOwner: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 bookmarks; got %d", len(items))
	}
	if items[0].AtomRevisionID != "rev-9" || items[0].ID != "bm-1" {
		t.Fatalf("first bookmark scan wrong: %+v", items[0])
	}
	if items[1].AtomRevisionID != "rev-2" {
		t.Fatalf("second bookmark revision wrong: %+v", items[1])
	}
	if next != "" {
		t.Fatalf("2 rows with limit 100 must have no next cursor; got %q", next)
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL") {
		t.Fatalf("first SQL must be SET LOCAL; got %q", q.sqls[0])
	}
	lastArgs := q.args[len(q.args)-1]
	if len(lastArgs) != 4 {
		t.Fatalf("list binds gcid, cursor created_at, cursor id, limit; got %v", lastArgs)
	}
	if lim, ok := lastArgs[3].(int); !ok || lim != 100 {
		t.Fatalf("limit must be clamped to 100; got %v", lastArgs[3])
	}
}

func TestBookmarkRepo_ListByOwner_FullPageReturnsNextCursor(t *testing.T) {
	t.Parallel()
	// created_at is NOT NULL on atom_bookmarks and is now half the page
	// token, so every stub row carries one.
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	row := func(id string, ageMin int) func(dest ...any) error {
		return func(dest ...any) error {
			bSetInto(dest, 0, id)
			bSetInto(dest, 5, base.Add(-time.Duration(ageMin)*time.Minute))
			return nil
		}
	}
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{
				row("bm-1", 0), row("bm-2", 1), row("bm-3", 2), row("bm-4", 3), row("bm-5", 4),
			}}, nil
		},
	}
	r := pg.NewBookmarkRepo(&stubTxRunner{q: q})
	items, next, err := r.ListByOwner(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "", 5)
	if err != nil {
		t.Fatalf("ListByOwner: %v", err)
	}
	if len(items) != 5 {
		t.Fatalf("expected 5 rows back; got %d", len(items))
	}
	// The next cursor is the opaque (created_at, id) keyset token of the
	// last row, not the bare id.
	curTS, curID := keyset.Decode(next)
	if curID != "bm-5" || !curTS.Equal(base.Add(-4*time.Minute)) {
		t.Fatalf("full page must return the last row keyset token; got %q -> (%v, %q)", next, curTS, curID)
	}
}

func TestBookmarkRepo_ListByOwner_EmptyReturnsEmptySlice(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{} // no rows
	r := pg.NewBookmarkRepo(&stubTxRunner{q: q})
	items, next, err := r.ListByOwner(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "cursor", -3)
	if err != nil {
		t.Fatalf("ListByOwner: %v", err)
	}
	if items == nil || len(items) != 0 {
		t.Fatalf("expected non-nil empty slice; got %#v", items)
	}
	if next != "" {
		t.Fatalf("expected empty cursor; got %q", next)
	}
	// limit -3 defaults to 20. Binds are gcid, cursor created_at,
	// cursor id, limit.
	if lim, ok := q.args[len(q.args)-1][3].(int); !ok || lim != 20 {
		t.Fatalf("negative limit must default to 20; got %v", q.args[len(q.args)-1][3])
	}
}

func TestBookmarkRepo_ListByOwner_QueryErrorAndScanError(t *testing.T) {
	t.Parallel()

	t.Run("query error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			queryFn: func(sql string, args ...any) (pg.Rows, error) {
				return nil, errors.New("select failed")
			},
		}
		r := pg.NewBookmarkRepo(&stubTxRunner{q: q})
		_, _, err := r.ListByOwner(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "", 10)
		if err == nil || err.Error() != "select failed" {
			t.Fatalf("expected query error propagated; got %v", err)
		}
	})

	t.Run("rows.Err", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			queryFn: func(sql string, args ...any) (pg.Rows, error) {
				return &stubRows{scans: []func(dest ...any) error{}, err: errors.New("iter failed")}, nil
			},
		}
		r := pg.NewBookmarkRepo(&stubTxRunner{q: q})
		if _, _, err := r.ListByOwner(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "", 10); err == nil {
			t.Fatalf("rows.Err must propagate")
		}
	})

	t.Run("scan error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			queryFn: func(sql string, args ...any) (pg.Rows, error) {
				return &stubRows{scans: []func(dest ...any) error{
					func(dest ...any) error { return errors.New("scan failed") },
				}}, nil
			},
		}
		r := pg.NewBookmarkRepo(&stubTxRunner{q: q})
		if _, _, err := r.ListByOwner(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "", 10); err == nil {
			t.Fatalf("scan error must propagate")
		}
	})
}
