// social_repo_b_test.go — extra coverage for SocialGraphRepo (second
// coverage agent): FollowersGCIDs, FollowSuggestions (guards + scan),
// requireGCIDs guard branches, Exec/query/scan error paths.
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

func TestSocialGraphRepo_FollowersGCIDs_QueriesFollowers(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{
				func(dest ...any) error {
					bSetInto(dest, 0, gcidA)
					return nil
				},
			}}, nil
		},
	}
	r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})
	got, err := r.FollowersGCIDs(context.Background(), tenantID, gcidB)
	if err != nil {
		t.Fatalf("FollowersGCIDs: %v", err)
	}
	if len(got) != 1 || got[0] != gcidA {
		t.Fatalf("expected [%s]; got %v", gcidA, got)
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "FROM social_follows") || !strings.Contains(last, "followee_gcid = $1") {
		t.Fatalf("expected followee-scoped SELECT; got %q", last)
	}
}

func TestSocialGraphRepo_ListGCIDs_EmptyGCIDRejected(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})
	if _, err := r.FollowingGCIDs(context.Background(), tenantID, "   "); !errors.Is(err, social.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument; got %v", err)
	}
	if len(q.sqls) != 0 {
		t.Fatalf("guard failure must issue NO SQL; got %v", q.sqls)
	}
}

func TestSocialGraphRepo_ListGCIDs_QueryAndScanErrors(t *testing.T) {
	t.Parallel()

	t.Run("query error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("q") }}
		r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})
		_, err := r.BlockedBy(context.Background(), tenantID, gcidA)
		if err == nil || !strings.Contains(err.Error(), "social list query") {
			t.Fatalf("expected wrapped query error; got %v", err)
		}
	})

	t.Run("scan error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{func(dest ...any) error { return errors.New("s") }}}, nil
		}}
		r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})
		_, err := r.FollowingGCIDs(context.Background(), tenantID, gcidA)
		if err == nil || !strings.Contains(err.Error(), "social list scan") {
			t.Fatalf("expected wrapped scan error; got %v", err)
		}
	})
}

func TestSocialGraphRepo_Unfollow_EmptyGCIDRejected(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})
	if _, err := r.Unfollow(context.Background(), tenantID, "", gcidB); !errors.Is(err, social.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument; got %v", err)
	}
	if _, err := r.Unblock(context.Background(), tenantID, gcidA, " "); !errors.Is(err, social.ErrInvalidArgument) {
		t.Fatalf("Unblock: expected ErrInvalidArgument; got %v", err)
	}
	if len(q.sqls) != 0 {
		t.Fatalf("guard failure must issue NO SQL; got %v", q.sqls)
	}
}

func TestSocialGraphRepo_FollowSuggestions_Happy(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			if !strings.Contains(sql, "my_tags") || !strings.Contains(sql, "LIMIT $2") {
				t.Fatalf("expected suggestions CTE query; got %q", sql)
			}
			return &stubRows{scans: []func(dest ...any) error{
				func(dest ...any) error {
					bSetInto(dest, 0, otherGCID)
					bSetInto(dest, 1, "Walfa")
					bSetInto(dest, 2, []string{"go"})
					bSetInto(dest, 3, 2)
					return nil
				},
			}}, nil
		},
	}
	r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})
	sugs, err := r.FollowSuggestions(context.Background(), tenantID, gcidA, 5)
	if err != nil {
		t.Fatalf("FollowSuggestions: %v", err)
	}
	if len(sugs) != 1 || sugs[0].GCID != otherGCID || sugs[0].DisplayName != "Walfa" || sugs[0].MutualFollows != 2 {
		t.Fatalf("suggestions wrong: %+v", sugs)
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL") {
		t.Fatalf("first SQL must be SET LOCAL; got %q", q.sqls[0])
	}
}

func TestSocialGraphRepo_FollowSuggestions_Guards(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})

	if _, err := r.FollowSuggestions(context.Background(), tenantID, "  ", 5); !errors.Is(err, social.ErrInvalidArgument) {
		t.Fatalf("empty gcid: expected ErrInvalidArgument; got %v", err)
	}
	if _, err := r.FollowSuggestions(context.Background(), tenantID, gcidA, 0); !errors.Is(err, social.ErrInvalidArgument) {
		t.Fatalf("zero limit: expected ErrInvalidArgument; got %v", err)
	}
	if len(q.sqls) != 0 {
		t.Fatalf("guard failures must issue NO SQL; got %v", q.sqls)
	}
}

func TestSocialGraphRepo_FollowSuggestions_Errors(t *testing.T) {
	t.Parallel()

	t.Run("query error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("q") }}
		r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})
		if _, err := r.FollowSuggestions(context.Background(), tenantID, gcidA, 5); err == nil {
			t.Fatalf("query error must propagate")
		}
	})

	t.Run("scan error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{func(dest ...any) error { return errors.New("s") }}}, nil
		}}
		r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})
		if _, err := r.FollowSuggestions(context.Background(), tenantID, gcidA, 5); err == nil {
			t.Fatalf("scan error must propagate")
		}
	})

	t.Run("rows.Err", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{}, err: errors.New("r")}, nil
		}}
		r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})
		if _, err := r.FollowSuggestions(context.Background(), tenantID, gcidA, 5); err == nil {
			t.Fatalf("rows.Err must propagate")
		}
	})
}

func TestSocialGraphRepo_ExecErrorPaths(t *testing.T) {
	t.Parallel()
	failUserSQL := func(sql string) error {
		if strings.Contains(sql, "SET LOCAL") {
			return nil
		}
		return errors.New("boom")
	}

	t.Run("follow insert error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerierB{execErrFn: failUserSQL}
		r := pg.NewSocialGraphRepo(&stubTxRunnerB{q: q})
		_, _, err := r.Follow(context.Background(), tenantID, gcidA, gcidB)
		if err == nil || !strings.Contains(err.Error(), "insert social follow") {
			t.Fatalf("expected wrapped insert error; got %v", err)
		}
	})

	t.Run("unfollow delete error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerierB{execErrFn: failUserSQL}
		r := pg.NewSocialGraphRepo(&stubTxRunnerB{q: q})
		if _, err := r.Unfollow(context.Background(), tenantID, gcidA, gcidB); err == nil {
			t.Fatalf("delete error must propagate")
		}
	})

	t.Run("block insert error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerierB{execErrFn: failUserSQL}
		r := pg.NewSocialGraphRepo(&stubTxRunnerB{q: q})
		if err := r.Block(context.Background(), tenantID, gcidA, gcidB); err == nil {
			t.Fatalf("insert error must propagate")
		}
	})

	t.Run("unblock delete error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerierB{execErrFn: failUserSQL}
		r := pg.NewSocialGraphRepo(&stubTxRunnerB{q: q})
		if _, err := r.Unblock(context.Background(), tenantID, gcidA, gcidB); err == nil {
			t.Fatalf("delete error must propagate")
		}
	})
}

func TestSocialGraphRepo_FriendSuggestions_AlwaysEmpty(t *testing.T) {
	t.Parallel()
	r := pg.NewSocialGraphRepo(&stubTxRunner{q: &stubQuerier{}})
	got, err := r.FriendSuggestions(context.Background(), tenantID, gcidA, 5)
	if err != nil {
		t.Fatalf("FriendSuggestions: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("friend suggestions must be empty; got %#v", got)
	}
}

func TestSocialGraphRepo_Follow_ConflictSelectError(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		execTagFn: func(sql string) rls.CommandTag {
			if strings.Contains(sql, "INSERT INTO social_follows") {
				return rls.CommandTag{RowsAffected: 0}
			}
			return rls.CommandTag{RowsAffected: 1}
		},
		rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error { return errors.New("select boom") }}
		},
	}
	r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})
	_, _, err := r.Follow(context.Background(), tenantID, gcidA, gcidB)
	if err == nil || !strings.Contains(err.Error(), "select social follow after conflict") {
		t.Fatalf("expected wrapped conflict-select error; got %v", err)
	}
}
