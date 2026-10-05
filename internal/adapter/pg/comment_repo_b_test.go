// comment_repo_b_test.go — coverage tests for the pgx-backed CommentRepo
// (second coverage agent): Create's 1-level-reply parent checks,
// ListByPost keyset pagination, Update/Delete ownership semantics.
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/domain/comment"
	"github.com/apollo-chora/chora-sharing/internal/keyset"
)

const commentID = "01970000-0000-7000-a000-0000000000e1"

func TestCommentRepo_NilTxRunner_ReturnsErrNotImplemented(t *testing.T) {
	t.Parallel()
	r := pg.NewCommentRepo(nil)
	if err := r.Create(context.Background(), &comment.Comment{}); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("Create: expected ErrNotImplemented; got %v", err)
	}
	if _, _, err := r.ListByPost(context.Background(), "p1", 10, ""); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("ListByPost: expected ErrNotImplemented; got %v", err)
	}
	if err := r.Update(context.Background(), commentID, authorGCID, "body"); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("Update: expected ErrNotImplemented; got %v", err)
	}
	if err := r.Delete(context.Background(), commentID, authorGCID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("Delete: expected ErrNotImplemented; got %v", err)
	}
}

func TestCommentRepo_Create_RejectsNil(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewCommentRepo(&stubTxRunner{q: q})
	if err := r.Create(tracing.WithTenantID(context.Background(), tenantID), nil); !errors.Is(err, comment.ErrInvalidArgument) {
		t.Fatalf("expected comment.ErrInvalidArgument; got %v", err)
	}
}

func TestCommentRepo_Create_TopLevel_NoParentCheck(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewCommentRepo(&stubTxRunner{q: q})
	c := &comment.Comment{
		ID: commentID, TenantID: tenantID, PostID: "p1",
		AuthorGCID: authorGCID, Body: "hello",
		CreatedAt: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
	}
	if err := r.Create(tracing.WithTenantID(context.Background(), tenantID), c); err != nil {
		t.Fatalf("Create: %v", err)
	}
	// No parent → exactly SET LOCAL + INSERT (no parent-check QueryRow).
	if len(q.sqls) != 2 {
		t.Fatalf("expected SET LOCAL + INSERT only; got %d SQLs: %v", len(q.sqls), q.sqls)
	}
	if !strings.Contains(q.sqls[1], "INSERT INTO post_comments") {
		t.Fatalf("expected INSERT INTO post_comments; got %q", q.sqls[1])
	}
	// parent_comment_id arg must be nil for top-level.
	args := q.args[1]
	if args[5] != nil {
		t.Fatalf("empty parent must bind nil; got %v", args[5])
	}
}

func TestCommentRepo_Create_WithParent_HappyAndReplyRejected(t *testing.T) {
	t.Parallel()
	parentID := "01970000-0000-7000-a000-0000000000e2"

	t.Run("valid top-level parent", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			rowFn: func(sql string, args ...any) pg.Row {
				return stubRow{scanFn: func(dest ...any) error {
					// parent tenant + parent's parent (NULL → nil).
					bSetInto(dest, 0, tenantID)
					return nil
				}}
			},
		}
		r := pg.NewCommentRepo(&stubTxRunner{q: q})
		c := &comment.Comment{ID: commentID, TenantID: tenantID, PostID: "p1", AuthorGCID: authorGCID, Body: "reply", ParentCommentID: parentID}
		if err := r.Create(tracing.WithTenantID(context.Background(), tenantID), c); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if len(q.sqls) != 3 {
			t.Fatalf("expected SET LOCAL + parent check + insert; got %d", len(q.sqls))
		}
		if !strings.Contains(q.sqls[1], "SELECT tenant_id, parent_comment_id") {
			t.Fatalf("parent check must SELECT tenant_id, parent_comment_id; got %q", q.sqls[1])
		}
	})

	t.Run("parent is itself a reply", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			rowFn: func(sql string, args ...any) pg.Row {
				return stubRow{scanFn: func(dest ...any) error {
					bSetInto(dest, 0, tenantID)
					bSetInto(dest, 1, bPtr("01970000-0000-7000-a000-0000000000e3"))
					return nil
				}}
			},
		}
		r := pg.NewCommentRepo(&stubTxRunner{q: q})
		c := &comment.Comment{ID: commentID, TenantID: tenantID, PostID: "p1", AuthorGCID: authorGCID, Body: "reply", ParentCommentID: parentID}
		err := r.Create(tracing.WithTenantID(context.Background(), tenantID), c)
		if !errors.Is(err, comment.ErrInvalidArgument) {
			t.Fatalf("reply-to-reply must be ErrInvalidArgument; got %v", err)
		}
	})

	t.Run("parent missing", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			rowFn: func(sql string, args ...any) pg.Row {
				return stubRow{scanFn: func(dest ...any) error { return errors.New("no rows") }}
			},
		}
		r := pg.NewCommentRepo(&stubTxRunner{q: q})
		c := &comment.Comment{ID: commentID, TenantID: tenantID, PostID: "p1", AuthorGCID: authorGCID, Body: "reply", ParentCommentID: parentID}
		err := r.Create(tracing.WithTenantID(context.Background(), tenantID), c)
		if !errors.Is(err, comment.ErrNotFound) {
			t.Fatalf("missing parent must map to ErrNotFound; got %v", err)
		}
	})
}

func TestCommentRepo_Create_DefaultCreatedAtAndExecError(t *testing.T) {
	t.Parallel()

	t.Run("created zero defaulted", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{}
		r := pg.NewCommentRepo(&stubTxRunner{q: q})
		c := &comment.Comment{ID: commentID, TenantID: tenantID, PostID: "p1", AuthorGCID: authorGCID, Body: "b"}
		if err := r.Create(tracing.WithTenantID(context.Background(), tenantID), c); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if created, ok := q.args[1][6].(time.Time); !ok || created.IsZero() {
			t.Fatalf("created must be defaulted to now; got %v", q.args[1][6])
		}
	})

	t.Run("insert exec error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerierB{
			execErrFn: func(sql string) error {
				if strings.Contains(sql, "SET LOCAL") {
					return nil
				}
				return errors.New("boom")
			},
		}
		r := pg.NewCommentRepo(&stubTxRunnerB{q: q})
		c := &comment.Comment{ID: commentID, TenantID: tenantID, PostID: "p1", AuthorGCID: authorGCID, Body: "b"}
		err := r.Create(tracing.WithTenantID(context.Background(), tenantID), c)
		if err == nil || !strings.Contains(err.Error(), "insert comment") {
			t.Fatalf("expected wrapped insert error; got %v", err)
		}
	})
}

func TestCommentRepo_ListByPost_PagesAndScans(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{
				func(dest ...any) error {
					bSetInto(dest, 0, "c-1")
					bSetInto(dest, 1, tenantID)
					bSetInto(dest, 2, "p1")
					bSetInto(dest, 3, authorGCID)
					bSetInto(dest, 4, "top-level")
					bSetInto(dest, 5, bPtr(""))
					bSetInto(dest, 6, time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC))
					return nil
				},
				func(dest ...any) error {
					bSetInto(dest, 0, "c-2")
					bSetInto(dest, 4, "reply")
					bSetInto(dest, 5, bPtr("01970000-0000-7000-a000-0000000000e2"))
					bSetInto(dest, 6, time.Date(2026, 7, 10, 11, 59, 0, 0, time.UTC))
					return nil
				},
			}}, nil
		},
	}
	r := pg.NewCommentRepo(&stubTxRunner{q: q})
	items, next, err := r.ListByPost(tracing.WithTenantID(context.Background(), tenantID), "p1", 2, "cursor")
	if err != nil {
		t.Fatalf("ListByPost: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 comments; got %d", len(items))
	}
	if items[1].ParentCommentID != "01970000-0000-7000-a000-0000000000e2" {
		t.Fatalf("parent id scan wrong: %+v", items[1])
	}
	// The next cursor is the opaque (created_at, id) keyset token of the
	// last row, not the bare id.
	curTS, curID := keyset.Decode(next)
	if curID != "c-2" || !curTS.Equal(time.Date(2026, 7, 10, 11, 59, 0, 0, time.UTC)) {
		t.Fatalf("full page must return the last row keyset token; got %q -> (%v, %q)", next, curTS, curID)
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL") {
		t.Fatalf("first SQL must be SET LOCAL; got %q", q.sqls[0])
	}
}

func TestCommentRepo_ListByPost_EmptyReturnsEmptySlice(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewCommentRepo(&stubTxRunner{q: q})
	items, next, err := r.ListByPost(tracing.WithTenantID(context.Background(), tenantID), "p1", 0, "")
	if err != nil {
		t.Fatalf("ListByPost: %v", err)
	}
	if items == nil || len(items) != 0 || next != "" {
		t.Fatalf("expected empty slice + empty cursor; got %#v, %q", items, next)
	}
	// Binds are post_id, cursor created_at, cursor id, limit.
	if lim, ok := q.args[len(q.args)-1][3].(int); !ok || lim != 50 {
		t.Fatalf("limit 0 must default to 50; got %v", q.args[len(q.args)-1][3])
	}
}

func TestCommentRepo_ListByPost_Errors(t *testing.T) {
	t.Parallel()

	t.Run("query error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("q") }}
		r := pg.NewCommentRepo(&stubTxRunner{q: q})
		if _, _, err := r.ListByPost(tracing.WithTenantID(context.Background(), tenantID), "p1", 10, ""); err == nil {
			t.Fatalf("query error must propagate")
		}
	})

	t.Run("scan error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{func(dest ...any) error { return errors.New("s") }}}, nil
		}}
		r := pg.NewCommentRepo(&stubTxRunner{q: q})
		if _, _, err := r.ListByPost(tracing.WithTenantID(context.Background(), tenantID), "p1", 10, ""); err == nil {
			t.Fatalf("scan error must propagate")
		}
	})

	t.Run("rows.Err", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{}, err: errors.New("r")}, nil
		}}
		r := pg.NewCommentRepo(&stubTxRunner{q: q})
		if _, _, err := r.ListByPost(tracing.WithTenantID(context.Background(), tenantID), "p1", 10, ""); err == nil {
			t.Fatalf("rows.Err must propagate")
		}
	})
}

func TestCommentRepo_Update_TrimValidateAndAffected(t *testing.T) {
	t.Parallel()

	t.Run("empty body rejected", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{}
		r := pg.NewCommentRepo(&stubTxRunner{q: q})
		if err := r.Update(context.Background(), commentID, authorGCID, "   "); !errors.Is(err, comment.ErrInvalidArgument) {
			t.Fatalf("expected ErrInvalidArgument; got %v", err)
		}
	})

	t.Run("overlong body rejected", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{}
		r := pg.NewCommentRepo(&stubTxRunner{q: q})
		long := strings.Repeat("x", comment.MaxBodyLen+1)
		if err := r.Update(context.Background(), commentID, authorGCID, long); !errors.Is(err, comment.ErrInvalidArgument) {
			t.Fatalf("expected ErrInvalidArgument; got %v", err)
		}
	})

	t.Run("updated", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{}
		r := pg.NewCommentRepo(&stubTxRunner{q: q})
		if err := r.Update(tracing.WithTenantID(context.Background(), tenantID), commentID, authorGCID, "  new body  "); err != nil {
			t.Fatalf("Update: %v", err)
		}
		last := q.sqls[len(q.sqls)-1]
		if !strings.Contains(last, "UPDATE post_comments") || !strings.Contains(last, "author_gcid = $2") {
			t.Fatalf("expected owner-scoped UPDATE; got %q", last)
		}
		// Trimmed body must be bound ($3).
		if got, _ := q.args[len(q.args)-1][2].(string); got != "new body" {
			t.Fatalf("body must be trimmed before bind; got %q", got)
		}
	})

	t.Run("zero rows affected → ErrNotFound", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{execTagFn: func(sql string) rls.CommandTag { return rls.CommandTag{RowsAffected: 0} }}
		r := pg.NewCommentRepo(&stubTxRunner{q: q})
		err := r.Update(tracing.WithTenantID(context.Background(), tenantID), commentID, authorGCID, "body")
		if !errors.Is(err, comment.ErrNotFound) {
			t.Fatalf("expected ErrNotFound; got %v", err)
		}
	})

	t.Run("exec error propagates", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerierB{execErrFn: func(sql string) error {
			if strings.Contains(sql, "SET LOCAL") {
				return nil
			}
			return errors.New("boom")
		}}
		r := pg.NewCommentRepo(&stubTxRunnerB{q: q})
		err := r.Update(tracing.WithTenantID(context.Background(), tenantID), commentID, authorGCID, "body")
		if err == nil || !strings.Contains(err.Error(), "update comment") {
			t.Fatalf("expected wrapped update error; got %v", err)
		}
	})
}

func TestCommentRepo_Delete_AffectedAndErrors(t *testing.T) {
	t.Parallel()

	t.Run("deleted", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{}
		r := pg.NewCommentRepo(&stubTxRunner{q: q})
		if err := r.Delete(tracing.WithTenantID(context.Background(), tenantID), commentID, authorGCID); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if !strings.Contains(q.sqls[len(q.sqls)-1], "DELETE FROM post_comments") {
			t.Fatalf("expected DELETE FROM post_comments; got %q", q.sqls[len(q.sqls)-1])
		}
	})

	t.Run("zero rows affected → ErrNotFound", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{execTagFn: func(sql string) rls.CommandTag { return rls.CommandTag{RowsAffected: 0} }}
		r := pg.NewCommentRepo(&stubTxRunner{q: q})
		if err := r.Delete(tracing.WithTenantID(context.Background(), tenantID), commentID, authorGCID); !errors.Is(err, comment.ErrNotFound) {
			t.Fatalf("expected ErrNotFound; got %v", err)
		}
	})

	t.Run("exec error propagates", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerierB{execErrFn: func(sql string) error {
			if strings.Contains(sql, "SET LOCAL") {
				return nil
			}
			return errors.New("boom")
		}}
		r := pg.NewCommentRepo(&stubTxRunnerB{q: q})
		err := r.Delete(tracing.WithTenantID(context.Background(), tenantID), commentID, authorGCID)
		if err == nil || !strings.Contains(err.Error(), "delete comment") {
			t.Fatalf("expected wrapped delete error; got %v", err)
		}
	})
}
