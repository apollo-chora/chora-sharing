// connection_store_b_test.go — coverage tests for ConnectionStore
// (second coverage agent): follow/block save + delete SQL shapes and
// error branches via the stub harness.
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
)

func TestConnectionStore_SaveFollow_AppliesRLSAndInserts(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	s := pg.NewConnectionStore(&stubTxRunner{q: q})
	if err := s.SaveFollow(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, otherGCID); err != nil {
		t.Fatalf("SaveFollow: %v", err)
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL chora.tenant_id") {
		t.Fatalf("first SQL must be SET LOCAL; got %q", q.sqls[0])
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "INSERT INTO social_follows") || !strings.Contains(last, "ON CONFLICT (follower_gcid, followee_gcid) DO NOTHING") {
		t.Fatalf("expected idempotent INSERT INTO social_follows; got %q", last)
	}
}

func TestConnectionStore_DeleteFollow_Deletes(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	s := pg.NewConnectionStore(&stubTxRunner{q: q})
	if err := s.DeleteFollow(tracing.WithTenantID(context.Background(), tenantID), authorGCID, otherGCID); err != nil {
		t.Fatalf("DeleteFollow: %v", err)
	}
	if !strings.Contains(q.sqls[1], "DELETE FROM social_follows") {
		t.Fatalf("expected DELETE FROM social_follows; got %q", q.sqls[1])
	}
}

func TestConnectionStore_SaveBlock_InsertsBlocked(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	s := pg.NewConnectionStore(&stubTxRunner{q: q})
	if err := s.SaveBlock(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, otherGCID); err != nil {
		t.Fatalf("SaveBlock: %v", err)
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "INSERT INTO connections") || !strings.Contains(last, "'blocked'") {
		t.Fatalf("expected INSERT INTO connections … 'blocked'; got %q", last)
	}
	// Bound status: only 3 placeholders (tenant, requester, addressee).
	if len(q.args[len(q.args)-1]) != 3 {
		t.Fatalf("SaveBlock binds tenant, blocker, blocked; got %v", q.args[len(q.args)-1])
	}
}

func TestConnectionStore_DeleteBlock_DeletesBlocked(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	s := pg.NewConnectionStore(&stubTxRunner{q: q})
	if err := s.DeleteBlock(tracing.WithTenantID(context.Background(), tenantID), authorGCID, otherGCID); err != nil {
		t.Fatalf("DeleteBlock: %v", err)
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "DELETE FROM connections") || !strings.Contains(last, "status = 'blocked'") {
		t.Fatalf("expected DELETE FROM connections … status = 'blocked'; got %q", last)
	}
}

func TestConnectionStore_RLSApplyFailurePropagates(t *testing.T) {
	t.Parallel()
	methods := []struct {
		name string
		run  func(s *pg.ConnectionStore) error
	}{
		{"SaveFollow", func(s *pg.ConnectionStore) error {
			return s.SaveFollow(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, otherGCID)
		}},
		{"DeleteFollow", func(s *pg.ConnectionStore) error {
			return s.DeleteFollow(tracing.WithTenantID(context.Background(), tenantID), authorGCID, otherGCID)
		}},
		{"SaveBlock", func(s *pg.ConnectionStore) error {
			return s.SaveBlock(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, otherGCID)
		}},
		{"DeleteBlock", func(s *pg.ConnectionStore) error {
			return s.DeleteBlock(tracing.WithTenantID(context.Background(), tenantID), authorGCID, otherGCID)
		}},
	}
	for _, m := range methods {
		m := m
		t.Run(m.name, func(t *testing.T) {
			t.Parallel()
			// Fail EVERY exec, including the SET LOCAL — rls.ApplySession must
			// surface the failure before any user SQL runs.
			q := &stubQuerierB{
				execErrFn: func(sql string) error { return errors.New("boom") },
			}
			s := pg.NewConnectionStore(&stubTxRunnerB{q: q})
			err := m.run(s)
			if err == nil || !strings.Contains(err.Error(), "SET LOCAL") {
				t.Fatalf("expected SET LOCAL failure to propagate; got %v", err)
			}
		})
	}
}

func TestConnectionStore_ExecErrorsPropagate(t *testing.T) {
	t.Parallel()
	methods := []struct {
		name string
		run  func(s *pg.ConnectionStore) error
		want string
	}{
		{"SaveFollow", func(s *pg.ConnectionStore) error {
			return s.SaveFollow(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, otherGCID)
		}, "SaveFollow"},
		{"DeleteFollow", func(s *pg.ConnectionStore) error {
			return s.DeleteFollow(tracing.WithTenantID(context.Background(), tenantID), authorGCID, otherGCID)
		}, "DeleteFollow"},
		{"SaveBlock", func(s *pg.ConnectionStore) error {
			return s.SaveBlock(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, otherGCID)
		}, "SaveBlock"},
		{"DeleteBlock", func(s *pg.ConnectionStore) error {
			return s.DeleteBlock(tracing.WithTenantID(context.Background(), tenantID), authorGCID, otherGCID)
		}, "DeleteBlock"},
	}
	for _, m := range methods {
		m := m
		t.Run(m.name, func(t *testing.T) {
			t.Parallel()
			q := &stubQuerierB{
				execErrFn: func(sql string) error {
					if strings.Contains(sql, "SET LOCAL") {
						return nil
					}
					return errors.New("boom")
				},
			}
			s := pg.NewConnectionStore(&stubTxRunnerB{q: q})
			err := m.run(s)
			if err == nil || !strings.Contains(err.Error(), m.want) {
				t.Fatalf("expected wrapped %s error; got %v", m.want, err)
			}
		})
	}
}
