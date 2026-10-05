// reaction_repo_b_test.go — coverage tests for ReactionRepo (second coverage
// agent): React insert+read-back (created vs idempotent), Unreact/UnreactByID
// ownership deletes, ListByPost, scanReaction + error branches.
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
	"github.com/apollo-chora/chora-sharing/internal/domain/reaction"
)

const reactionID = "01970000-0000-7000-a000-0000000000f1"

func TestReactionRepo_NilTxRunner_ReturnsErrNotImplemented(t *testing.T) {
	t.Parallel()
	r := pg.NewReactionRepo(nil)
	if _, _, err := r.React(context.Background(), tenantID, authorGCID, "p1", reaction.TypeLike); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("React: expected ErrNotImplemented; got %v", err)
	}
	if _, err := r.Unreact(context.Background(), authorGCID, "p1", reaction.TypeLike); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("Unreact: expected ErrNotImplemented; got %v", err)
	}
	if _, err := r.UnreactByID(context.Background(), reactionID, authorGCID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("UnreactByID: expected ErrNotImplemented; got %v", err)
	}
	if _, err := r.ListByPost(context.Background(), "p1"); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("ListByPost: expected ErrNotImplemented; got %v", err)
	}
}

func TestReactionRepo_React_InvalidTypeIsInvalidReaction(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewReactionRepo(&stubTxRunner{q: q})
	_, _, err := r.React(context.Background(), tenantID, authorGCID, "p1", reaction.Type("nope"))
	if !errors.Is(err, pg.ErrInvalidReaction) {
		t.Fatalf("expected ErrInvalidReaction; got %v", err)
	}
	if len(q.sqls) != 0 {
		t.Fatalf("invalid reaction must issue NO SQL; got %v", q.sqls)
	}
}

func TestReactionRepo_React_CreatedAndIdempotentRepeat(t *testing.T) {
	t.Parallel()
	scanRows := func(dest ...any) error {
		bSetInto(dest, 0, reactionID)
		bSetInto(dest, 1, tenantID)
		bSetInto(dest, 2, authorGCID)
		bSetInto(dest, 3, "p1")
		bSetInto(dest, 4, "like")
		bSetInto(dest, 5, time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC))
		return nil
	}

	t.Run("created=true when inserted", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row { return stubRow{scanFn: scanRows} }}
		r := pg.NewReactionRepo(&stubTxRunner{q: q})
		rx, created, err := r.React(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "p1", reaction.TypeLike)
		if err != nil {
			t.Fatalf("React: %v", err)
		}
		if !created || rx == nil {
			t.Fatalf("expected created=true with reaction; got %v, %+v", created, rx)
		}
		if rx.ID != reactionID || rx.Type != reaction.TypeLike {
			t.Fatalf("read-back scan wrong: %+v", rx)
		}
		if !strings.Contains(q.sqls[0], "SET LOCAL") {
			t.Fatalf("first SQL must be SET LOCAL; got %q", q.sqls[0])
		}
		if !strings.Contains(q.sqls[1], "INSERT INTO reactions") || !strings.Contains(q.sqls[1], "ON CONFLICT (gcid, post_id, reaction_type) DO NOTHING") {
			t.Fatalf("expected idempotent INSERT INTO reactions; got %q", q.sqls[1])
		}
		if !strings.Contains(q.sqls[2], "SELECT reaction_id") {
			t.Fatalf("expected read-back SELECT; got %q", q.sqls[2])
		}
	})

	t.Run("created=false when conflict", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			execTagFn: func(sql string) rls.CommandTag {
				if strings.Contains(sql, "INSERT INTO reactions") {
					return rls.CommandTag{RowsAffected: 0}
				}
				return rls.CommandTag{RowsAffected: 1}
			},
			rowFn: func(sql string, args ...any) pg.Row { return stubRow{scanFn: scanRows} },
		}
		r := pg.NewReactionRepo(&stubTxRunner{q: q})
		_, created, err := r.React(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "p1", reaction.TypeLike)
		if err != nil {
			t.Fatalf("React: %v", err)
		}
		if created {
			t.Fatalf("ON CONFLICT repeat must report created=false")
		}
	})
}

func TestReactionRepo_React_ErrorPaths(t *testing.T) {
	t.Parallel()

	t.Run("insert exec error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerierB{execErrFn: func(sql string) error {
			if strings.Contains(sql, "SET LOCAL") {
				return nil
			}
			return errors.New("boom")
		}}
		r := pg.NewReactionRepo(&stubTxRunnerB{q: q})
		if _, _, err := r.React(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "p1", reaction.TypeLike); err == nil {
			t.Fatalf("exec error must propagate")
		}
	})

	t.Run("read-back scan error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error { return errors.New("scan boom") }}
		}}
		r := pg.NewReactionRepo(&stubTxRunner{q: q})
		if _, _, err := r.React(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "p1", reaction.TypeLike); err == nil {
			t.Fatalf("read-back scan error must propagate")
		}
	})
}

func TestReactionRepo_Unreact_RemovedAndAbsent(t *testing.T) {
	t.Parallel()

	t.Run("removed", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{}
		r := pg.NewReactionRepo(&stubTxRunner{q: q})
		removed, err := r.Unreact(tracing.WithTenantID(context.Background(), tenantID), authorGCID, "p1", reaction.TypeLike)
		if err != nil || !removed {
			t.Fatalf("expected removed=true; got %v, %v", removed, err)
		}
		last := q.sqls[len(q.sqls)-1]
		if !strings.Contains(last, "DELETE FROM reactions") || !strings.Contains(last, "gcid = $1 AND post_id = $2") {
			t.Fatalf("expected keyed DELETE; got %q", last)
		}
	})

	t.Run("absent", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{execTagFn: func(sql string) rls.CommandTag { return rls.CommandTag{RowsAffected: 0} }}
		r := pg.NewReactionRepo(&stubTxRunner{q: q})
		removed, err := r.Unreact(tracing.WithTenantID(context.Background(), tenantID), authorGCID, "p1", reaction.TypeLike)
		if err != nil || removed {
			t.Fatalf("expected removed=false; got %v, %v", removed, err)
		}
	})

	t.Run("exec error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerierB{execErrFn: func(sql string) error {
			if strings.Contains(sql, "SET LOCAL") {
				return nil
			}
			return errors.New("boom")
		}}
		r := pg.NewReactionRepo(&stubTxRunnerB{q: q})
		if _, err := r.Unreact(tracing.WithTenantID(context.Background(), tenantID), authorGCID, "p1", reaction.TypeLike); err == nil {
			t.Fatalf("exec error must propagate")
		}
	})
}

func TestReactionRepo_UnreactByID_OwnershipDelete(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewReactionRepo(&stubTxRunner{q: q})
	removed, err := r.UnreactByID(tracing.WithTenantID(context.Background(), tenantID), reactionID, authorGCID)
	if err != nil || !removed {
		t.Fatalf("expected removed=true; got %v, %v", removed, err)
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "DELETE FROM reactions WHERE reaction_id = $1 AND gcid = $2") {
		t.Fatalf("expected ownership DELETE; got %q", last)
	}

	// absent + error variants
	notRemovedQ := &stubQuerier{execTagFn: func(sql string) rls.CommandTag { return rls.CommandTag{RowsAffected: 0} }}
	r2 := pg.NewReactionRepo(&stubTxRunner{q: notRemovedQ})
	if got, err := r2.UnreactByID(tracing.WithTenantID(context.Background(), tenantID), reactionID, authorGCID); err != nil || got {
		t.Fatalf("expected removed=false; got %v, %v", got, err)
	}

	errQ := &stubQuerierB{execErrFn: func(sql string) error {
		if strings.Contains(sql, "SET LOCAL") {
			return nil
		}
		return errors.New("boom")
	}}
	r3 := pg.NewReactionRepo(&stubTxRunnerB{q: errQ})
	if _, err := r3.UnreactByID(tracing.WithTenantID(context.Background(), tenantID), reactionID, authorGCID); err == nil {
		t.Fatalf("exec error must propagate")
	}
}

func TestReactionRepo_ListByPost_ScansTypes(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{
				func(dest ...any) error {
					bSetInto(dest, 0, reactionID)
					bSetInto(dest, 1, tenantID)
					bSetInto(dest, 2, authorGCID)
					bSetInto(dest, 3, "p1")
					bSetInto(dest, 4, "insightful")
					return nil
				},
			}}, nil
		},
	}
	r := pg.NewReactionRepo(&stubTxRunner{q: q})
	items, err := r.ListByPost(tracing.WithTenantID(context.Background(), tenantID), "p1")
	if err != nil {
		t.Fatalf("ListByPost: %v", err)
	}
	if len(items) != 1 || items[0].Type != reaction.TypeInsightful || items[0].ID != reactionID {
		t.Fatalf("scan wrong: %+v", items)
	}
	if !strings.Contains(q.sqls[len(q.sqls)-1], "ORDER BY created_at ASC") {
		t.Fatalf("expected oldest-first order; got %q", q.sqls[len(q.sqls)-1])
	}
}

func TestReactionRepo_ListByPost_Errors(t *testing.T) {
	t.Parallel()

	t.Run("query error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("q") }}
		r := pg.NewReactionRepo(&stubTxRunner{q: q})
		if _, err := r.ListByPost(tracing.WithTenantID(context.Background(), tenantID), "p1"); err == nil {
			t.Fatalf("query error must propagate")
		}
	})

	t.Run("scan error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{func(dest ...any) error { return errors.New("s") }}}, nil
		}}
		r := pg.NewReactionRepo(&stubTxRunner{q: q})
		if _, err := r.ListByPost(tracing.WithTenantID(context.Background(), tenantID), "p1"); err == nil {
			t.Fatalf("scan error must propagate")
		}
	})

	t.Run("rows.Err", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{}, err: errors.New("r")}, nil
		}}
		r := pg.NewReactionRepo(&stubTxRunner{q: q})
		if _, err := r.ListByPost(tracing.WithTenantID(context.Background(), tenantID), "p1"); err == nil {
			t.Fatalf("rows.Err must propagate")
		}
	})
}

func TestReactionRepo_ScanReaction_BadScan(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: func(dest ...any) error { return errors.New("bad scan") }}
	}}
	r := pg.NewReactionRepo(&stubTxRunner{q: q})
	// React's read-back uses scanReaction; a scan error must propagate as
	// plain error from the tx callback.
	if _, _, err := r.React(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "p1", reaction.TypeLike); err == nil {
		t.Fatalf("scanReaction error must propagate; got nil")
	}
}
