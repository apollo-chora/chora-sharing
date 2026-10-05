// social_repo_test.go — unit tests for the pgx-backed SocialGraphRepo
// (ADR-229 WS-0, CHO-2102): durable follow/block edges + the mutual-follow
// FriendReader behind GetReuseContext. Uses the shared stub harness
// (harness_test.go). Live RLS isolation is verified against the deployed DB
// via the PREPARE-smoke lane at deploy time.
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-common/rls"

	grpcadapter "github.com/apollo-chora/chora-sharing/internal/adapter/grpc"
	httpadapter "github.com/apollo-chora/chora-sharing/internal/adapter/http"
	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

const (
	gcidA = "01970000-0000-7000-c000-00000000000a"
	gcidB = "01970000-0000-7000-c000-00000000000b"
)

func TestSocialGraphRepo_NilTxRunner_ReturnsErrNotImplemented(t *testing.T) {
	t.Parallel()
	r := pg.NewSocialGraphRepo(nil)
	ctx := context.Background()
	if _, _, err := r.Follow(ctx, tenantID, gcidA, gcidB); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("Follow: expected ErrNotImplemented; got %v", err)
	}
	if _, err := r.Unfollow(ctx, tenantID, gcidA, gcidB); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("Unfollow: expected ErrNotImplemented; got %v", err)
	}
	if err := r.Block(ctx, tenantID, gcidA, gcidB); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("Block: expected ErrNotImplemented; got %v", err)
	}
	if _, err := r.FriendSet(ctx, tenantID, gcidA); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("FriendSet: expected ErrNotImplemented; got %v", err)
	}
}

func TestSocialGraphRepo_Follow_AppliesRLSThenInserts(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})

	edge, created, err := r.Follow(context.Background(), tenantID, gcidA, gcidB)
	if err != nil {
		t.Fatalf("Follow: %v", err)
	}
	if !created {
		t.Fatalf("expected created=true on first follow")
	}
	if edge == nil || edge.FollowerGCID != gcidA || edge.FolloweeGCID != gcidB || edge.TenantID != tenantID {
		t.Fatalf("edge mismatched: %+v", edge)
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL chora.tenant_id") {
		t.Fatalf("first SQL must be SET LOCAL chora.tenant_id; got %q", q.sqls[0])
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "INSERT INTO social_follows") || !strings.Contains(last, "ON CONFLICT") {
		t.Fatalf("expected idempotent INSERT INTO social_follows; got %q", last)
	}
}

func TestSocialGraphRepo_Follow_Duplicate_ReturnsExistingUnchanged(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		execTagFn: func(sql string) rls.CommandTag {
			if strings.Contains(sql, "INSERT INTO social_follows") {
				return rls.CommandTag{RowsAffected: 0} // conflict — edge exists
			}
			return rls.CommandTag{RowsAffected: 1}
		},
		rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error {
				if p, ok := dest[0].(*string); ok {
					*p = "existing-edge-id"
				}
				if p, ok := dest[1].(*string); ok {
					*p = tenantID
				}
				return nil // dest[2] (*time.Time) left zero
			}}
		},
	}
	r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})

	edge, created, err := r.Follow(context.Background(), tenantID, gcidA, gcidB)
	if err != nil {
		t.Fatalf("Follow duplicate: %v", err)
	}
	if created {
		t.Fatalf("expected created=false on duplicate follow (append-only)")
	}
	if edge.ID != "existing-edge-id" {
		t.Fatalf("expected the ORIGINAL edge returned unchanged; got %+v", edge)
	}
}

func TestSocialGraphRepo_Follow_SelfFollow_RejectedBeforeSQL(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})
	if _, _, err := r.Follow(context.Background(), tenantID, gcidA, gcidA); !errors.Is(err, social.ErrSelfFollow) {
		t.Fatalf("expected ErrSelfFollow; got %v", err)
	}
	if len(q.sqls) != 0 {
		t.Fatalf("guard failure must issue NO SQL; got %v", q.sqls)
	}
}

func TestSocialGraphRepo_Unfollow_DeletesAndReportsRemoval(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})
	removed, err := r.Unfollow(context.Background(), tenantID, gcidA, gcidB)
	if err != nil {
		t.Fatalf("Unfollow: %v", err)
	}
	if !removed {
		t.Fatalf("expected removed=true when a row was deleted")
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "DELETE FROM social_follows") {
		t.Fatalf("expected DELETE FROM social_follows; got %q", last)
	}
}

func TestSocialGraphRepo_Unfollow_Absent_ReturnsFalse(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		execTagFn: func(sql string) rls.CommandTag {
			if strings.Contains(sql, "DELETE FROM social_follows") {
				return rls.CommandTag{RowsAffected: 0}
			}
			return rls.CommandTag{RowsAffected: 1}
		},
	}
	r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})
	removed, err := r.Unfollow(context.Background(), tenantID, gcidA, gcidB)
	if err != nil {
		t.Fatalf("Unfollow: %v", err)
	}
	if removed {
		t.Fatalf("expected removed=false when no row matched")
	}
}

func TestSocialGraphRepo_Block_SelfBlock_RejectedBeforeSQL(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})
	if err := r.Block(context.Background(), tenantID, gcidA, gcidA); !errors.Is(err, social.ErrSelfBlock) {
		t.Fatalf("expected ErrSelfBlock; got %v", err)
	}
	if len(q.sqls) != 0 {
		t.Fatalf("guard failure must issue NO SQL; got %v", q.sqls)
	}
}

func TestSocialGraphRepo_Block_InsertsIdempotently(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})
	if err := r.Block(context.Background(), tenantID, gcidA, gcidB); err != nil {
		t.Fatalf("Block: %v", err)
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL chora.tenant_id") {
		t.Fatalf("first SQL must be SET LOCAL; got %q", q.sqls[0])
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "INSERT INTO social_blocks") || !strings.Contains(last, "ON CONFLICT") {
		t.Fatalf("expected idempotent INSERT INTO social_blocks; got %q", last)
	}
}

func TestSocialGraphRepo_Unblock_Deletes(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})
	removed, err := r.Unblock(context.Background(), tenantID, gcidA, gcidB)
	if err != nil {
		t.Fatalf("Unblock: %v", err)
	}
	if !removed {
		t.Fatalf("expected removed=true")
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "DELETE FROM social_blocks") {
		t.Fatalf("expected DELETE FROM social_blocks; got %q", last)
	}
}

func TestSocialGraphRepo_FollowingGCIDs_QueriesFollows(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{
				func(dest ...any) error {
					if p, ok := dest[0].(*string); ok {
						*p = gcidB
					}
					return nil
				},
			}}, nil
		},
	}
	r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})
	got, err := r.FollowingGCIDs(context.Background(), tenantID, gcidA)
	if err != nil {
		t.Fatalf("FollowingGCIDs: %v", err)
	}
	if len(got) != 1 || got[0] != gcidB {
		t.Fatalf("expected [%s]; got %v", gcidB, got)
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "FROM social_follows") || !strings.Contains(last, "follower_gcid = $1") {
		t.Fatalf("expected follower-scoped SELECT; got %q", last)
	}
}

func TestSocialGraphRepo_BlockedBy_QueriesBlocks(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{
				func(dest ...any) error {
					if p, ok := dest[0].(*string); ok {
						*p = gcidB
					}
					return nil
				},
			}}, nil
		},
	}
	r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})
	got, err := r.BlockedBy(context.Background(), tenantID, gcidA)
	if err != nil {
		t.Fatalf("BlockedBy: %v", err)
	}
	if len(got) != 1 || got[0] != gcidB {
		t.Fatalf("expected [%s]; got %v", gcidB, got)
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "FROM social_blocks") || !strings.Contains(last, "blocker_gcid = $1") {
		t.Fatalf("expected blocker-scoped SELECT; got %q", last)
	}
}

func TestSocialGraphRepo_FriendSet_QueryShape(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{
				func(dest ...any) error {
					if p, ok := dest[0].(*string); ok {
						*p = gcidB
					}
					return nil
				},
			}}, nil
		},
	}
	r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})
	got, err := r.FriendSet(context.Background(), tenantID, gcidA)
	if err != nil {
		t.Fatalf("FriendSet: %v", err)
	}
	if len(got) != 1 || got[0] != gcidB {
		t.Fatalf("expected friend set [%s]; got %v", gcidB, got)
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL chora.tenant_id") {
		t.Fatalf("first SQL must be SET LOCAL; got %q", q.sqls[0])
	}
	last := q.sqls[len(q.sqls)-1]
	// Friend set = accepted friendships minus blocks in either direction
	// (ADR-230 D2 — explicit friendship replaced derived mutual follows).
	if !strings.Contains(last, "FROM social_friendships") || !strings.Contains(last, "NOT EXISTS") ||
		!strings.Contains(last, "social_blocks") {
		t.Fatalf("expected friendships minus blocks; got %q", last)
	}
	if strings.Contains(last, "JOIN social_follows") {
		t.Fatalf("friend set must NOT derive from mutual follows anymore; got %q", last)
	}
}

func TestSocialGraphRepo_MissingTenant_FailsLoudBeforeUserSQL(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewSocialGraphRepo(&stubTxRunner{q: q})
	_, err := r.FollowingGCIDs(context.Background(), "", gcidA)
	if !errors.Is(err, rls.ErrNoTenantContext) {
		t.Fatalf("expected rls.ErrNoTenantContext; got %v", err)
	}
	for _, sql := range q.sqls {
		if strings.Contains(sql, "social_follows") {
			t.Fatalf("user SQL must not run without tenant context; got %v", q.sqls)
		}
	}
}

func TestSQLSocialTemplates_AreExported(t *testing.T) {
	t.Parallel()
	if !strings.Contains(pg.SQLInsertSocialFollow, "INSERT INTO social_follows") ||
		!strings.Contains(pg.SQLInsertSocialFollow, "ON CONFLICT") {
		t.Fatalf("SQLInsertSocialFollow malformed")
	}
	if !strings.Contains(pg.SQLDeleteSocialFollow, "DELETE FROM social_follows") {
		t.Fatalf("SQLDeleteSocialFollow malformed")
	}
	if !strings.Contains(pg.SQLInsertSocialBlock, "INSERT INTO social_blocks") {
		t.Fatalf("SQLInsertSocialBlock malformed")
	}
	if !strings.Contains(pg.SQLDeleteSocialBlock, "DELETE FROM social_blocks") {
		t.Fatalf("SQLDeleteSocialBlock malformed")
	}
	if !strings.Contains(pg.SQLFriendPartners, "FROM social_friendships") ||
		!strings.Contains(pg.SQLFriendPartners, "social_blocks") {
		t.Fatalf("SQLFriendPartners malformed")
	}
}

// Compile-time proof the repo satisfies the GraphQueries domain port (ADR-230
// D3 — sole doorway for graph-shaped reads) AND both transport-local
// SocialGraph ports (http + grpc).
var (
	_ social.GraphQueries     = (*pg.SocialGraphRepo)(nil)
	_ httpadapter.SocialGraph = (*pg.SocialGraphRepo)(nil)
	_ grpcadapter.SocialGraph = (*pg.SocialGraphRepo)(nil)
)
