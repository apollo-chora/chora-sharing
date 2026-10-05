// relationship_repo_b_test.go — extra coverage for RelationshipRepo (second
// coverage agent): constructor defaults, guard branches, tx-op error paths
// (Exec failures, conflict-read failures), Enqueue zero-timestamp + unknown
// kind + Exec failure.
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

func TestRelationshipRepo_ConstructorDefaults(t *testing.T) {
	t.Parallel()
	r := pg.NewRelationshipRepo(&stubTxRunner{q: &stubQuerier{}}, pg.RelationshipRepoOptions{})
	// Drive Enqueue with a zero OccurredAt so opts.Now (the default) stamps
	// occurred_at — proving the default Now is wired.
	q := &stubQuerier{}
	rr := pg.NewRelationshipRepo(&stubTxRunner{q: q}, pg.RelationshipRepoOptions{})
	err := rr.RunRelationship(context.Background(), tenantID, func(ctx context.Context, tx social.RelationshipTx) error {
		return tx.Enqueue(ctx, social.RelationshipEvent{
			Kind:        social.RelFollowed,
			TenantID:    tenantID,
			ActorGCID:   authorGCID,
			SubjectGCID: otherGCID,
			// OccurredAt left zero → opts.Now must supply it.
		})
	})
	if err != nil {
		t.Fatalf("RunRelationship with defaults: %v", err)
	}
	_ = r // construction with defaults must not panic; covered above via rr
}

func TestRelationshipRepo_NilRunner_ErrNotImplemented(t *testing.T) {
	t.Parallel()
	r := pg.NewRelationshipRepo(nil, pg.RelationshipRepoOptions{})
	var _ = r
	r2 := pg.NewRelationshipRepo(nil, pg.RelationshipRepoOptions{})
	err := r2.RunRelationship(context.Background(), tenantID, func(ctx context.Context, tx social.RelationshipTx) error { return nil })
	if !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

func TestRelationshipRepo_PairState_ScanErrorWraps(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: func(dest ...any) error { return errors.New("scan boom") }}
	}}
	r := newRelRepoB(q)
	err := r.RunRelationship(context.Background(), tenantID, func(ctx context.Context, tx social.RelationshipTx) error {
		_, err := tx.PairState(ctx, authorGCID, otherGCID)
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "pair state") {
		t.Fatalf("expected wrapped pair-state error; got %v", err)
	}
}

func TestRelationshipRepo_TxOpErrorPaths(t *testing.T) {
	t.Parallel()

	t.Run("insert follow exec error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerierB{execErrFn: func(sql string) error {
			if strings.Contains(sql, "SET LOCAL") {
				return nil
			}
			return errors.New("boom")
		}}
		r := newRelRepoB(q)
		err := r.RunRelationship(context.Background(), tenantID, func(ctx context.Context, tx social.RelationshipTx) error {
			e := &social.Edge{ID: "e1", TenantID: tenantID, FollowerGCID: authorGCID, FolloweeGCID: otherGCID}
			_, _, err := tx.InsertFollow(ctx, e)
			return err
		})
		if err == nil || !strings.Contains(err.Error(), "insert social follow") {
			t.Fatalf("expected wrapped insert error; got %v", err)
		}
	})

	t.Run("conflict-read select error", func(t *testing.T) {
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
		r := newRelRepoB(q)
		err := r.RunRelationship(context.Background(), tenantID, func(ctx context.Context, tx social.RelationshipTx) error {
			e := &social.Edge{ID: "e1", TenantID: tenantID, FollowerGCID: authorGCID, FolloweeGCID: otherGCID}
			_, _, err := tx.InsertFollow(ctx, e)
			return err
		})
		if err == nil || !strings.Contains(err.Error(), "select social follow after conflict") {
			t.Fatalf("expected wrapped conflict-read error; got %v", err)
		}
	})

	t.Run("delete follow absent", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{execTagFn: func(sql string) rls.CommandTag { return rls.CommandTag{RowsAffected: 0} }}
		r := newRelRepoB(q)
		err := r.RunRelationship(context.Background(), tenantID, func(ctx context.Context, tx social.RelationshipTx) error {
			removed, err := tx.DeleteFollow(ctx, authorGCID, otherGCID)
			if err != nil || removed {
				t.Fatalf("expected removed=false; got %v, %v", removed, err)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("DeleteFollow: %v", err)
		}
	})

	t.Run("insert block conflict", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{execTagFn: func(sql string) rls.CommandTag { return rls.CommandTag{RowsAffected: 0} }}
		r := newRelRepoB(q)
		err := r.RunRelationship(context.Background(), tenantID, func(ctx context.Context, tx social.RelationshipTx) error {
			b := &social.Block{ID: "b1", TenantID: tenantID, BlockerGCID: authorGCID, BlockedGCID: otherGCID}
			created, err := tx.InsertBlock(ctx, b)
			if err != nil || created {
				t.Fatalf("expected created=false on block conflict; got %v, %v", created, err)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("InsertBlock: %v", err)
		}
	})

	t.Run("insert block exec error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerierB{execErrFn: func(sql string) error {
			if strings.Contains(sql, "SET LOCAL") {
				return nil
			}
			return errors.New("boom")
		}}
		r := newRelRepoB(q)
		err := r.RunRelationship(context.Background(), tenantID, func(ctx context.Context, tx social.RelationshipTx) error {
			b := &social.Block{ID: "b1", TenantID: tenantID, BlockerGCID: authorGCID, BlockedGCID: otherGCID}
			_, err := tx.InsertBlock(ctx, b)
			return err
		})
		if err == nil || !strings.Contains(err.Error(), "insert social block") {
			t.Fatalf("expected wrapped block insert error; got %v", err)
		}
	})

	t.Run("delete block absent", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{execTagFn: func(sql string) rls.CommandTag { return rls.CommandTag{RowsAffected: 0} }}
		r := newRelRepoB(q)
		err := r.RunRelationship(context.Background(), tenantID, func(ctx context.Context, tx social.RelationshipTx) error {
			removed, err := tx.DeleteBlock(ctx, authorGCID, otherGCID)
			if err != nil || removed {
				t.Fatalf("expected removed=false; got %v, %v", removed, err)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("DeleteBlock: %v", err)
		}
	})
}

func TestRelationshipRepo_TxOpDeleteErrors(t *testing.T) {
	t.Parallel()

	t.Run("delete follow exec error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerierB{execErrFn: func(sql string) error {
			if strings.Contains(sql, "SET LOCAL") {
				return nil
			}
			return errors.New("boom")
		}}
		r := newRelRepoB(q)
		err := r.RunRelationship(context.Background(), tenantID, func(ctx context.Context, tx social.RelationshipTx) error {
			_, err := tx.DeleteFollow(ctx, authorGCID, otherGCID)
			return err
		})
		if err == nil || !strings.Contains(err.Error(), "delete social follow") {
			t.Fatalf("expected wrapped delete error; got %v", err)
		}
	})

	t.Run("delete block exec error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerierB{execErrFn: func(sql string) error {
			if strings.Contains(sql, "SET LOCAL") {
				return nil
			}
			return errors.New("boom")
		}}
		r := newRelRepoB(q)
		err := r.RunRelationship(context.Background(), tenantID, func(ctx context.Context, tx social.RelationshipTx) error {
			_, err := tx.DeleteBlock(ctx, authorGCID, otherGCID)
			return err
		})
		if err == nil || !strings.Contains(err.Error(), "delete social block") {
			t.Fatalf("expected wrapped block delete error; got %v", err)
		}
	})
}

func TestRelationshipRepo_Enqueue_UnknownKindFailsLoud(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := newRelRepoB(q)
	err := r.RunRelationship(context.Background(), tenantID, func(ctx context.Context, tx social.RelationshipTx) error {
		return tx.Enqueue(ctx, social.RelationshipEvent{
			Kind:        social.RelationshipEventKind("nope"),
			TenantID:    tenantID,
			ActorGCID:   authorGCID,
			SubjectGCID: otherGCID,
		})
	})
	if err == nil || !strings.Contains(err.Error(), "no topic registered") {
		t.Fatalf("unknown kind must fail loud; got %v", err)
	}
	// Nothing must hit the outbox table.
	for _, sql := range q.sqls {
		if strings.Contains(sql, "sharing_outbox_events") {
			t.Fatalf("outbox must not be written for unknown kind; got %v", q.sqls)
		}
	}
}

func TestRelationshipRepo_Enqueue_OutboxExecError(t *testing.T) {
	t.Parallel()
	q := &stubQuerierB{execErrFn: func(sql string) error {
		if strings.Contains(sql, "SET LOCAL") {
			return nil
		}
		return errors.New("boom")
	}}
	r := newRelRepoB(q)
	err := r.RunRelationship(context.Background(), tenantID, func(ctx context.Context, tx social.RelationshipTx) error {
		return tx.Enqueue(ctx, social.RelationshipEvent{
			Kind:        social.RelUnblocked,
			TenantID:    tenantID,
			ActorGCID:   authorGCID,
			SubjectGCID: otherGCID,
			OccurredAt:  time.Date(2026, 7, 10, 11, 59, 0, 0, time.UTC),
		})
	})
	if err == nil || !strings.Contains(err.Error(), "insert outbox event") {
		t.Fatalf("expected wrapped outbox insert error; got %v", err)
	}
}

func TestRelationshipRepo_Enqueue_EmptyActorBindsNullGCID(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := newRelRepoB(q)
	err := r.RunRelationship(context.Background(), tenantID, func(ctx context.Context, tx social.RelationshipTx) error {
		return tx.Enqueue(ctx, social.RelationshipEvent{
			Kind:        social.RelFollowed,
			TenantID:    tenantID,
			SubjectGCID: otherGCID,
			OccurredAt:  time.Date(2026, 7, 10, 11, 59, 0, 0, time.UTC),
		})
	})
	if err != nil {
		t.Fatalf("Enqueue with empty actor: %v", err)
	}
	// Find the outbox INSERT; $3 (gcid) must be nil.
	for i, sql := range q.sqls {
		if strings.Contains(sql, "sharing_outbox_events") {
			if q.args[i][2] != nil {
				t.Fatalf("empty actor must bind NULL gcid; got %v", q.args[i][2])
			}
			return
		}
	}
	t.Fatalf("no outbox INSERT recorded; sqls=%v", q.sqls)
}
