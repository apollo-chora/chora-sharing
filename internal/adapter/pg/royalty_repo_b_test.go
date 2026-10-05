// royalty_repo_b_test.go — coverage tests for RoyaltyRepo (second coverage
// agent): Record insert + idempotent-replay mapping, Exists found/absent.
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
	"github.com/apollo-chora/chora-sharing/internal/domain/grant"
)

func TestRoyaltyRepo_NilTxRunner_ReturnsErrNotImplemented(t *testing.T) {
	t.Parallel()
	r := pg.NewRoyaltyRepo(nil)
	if err := r.Record(context.Background(), &grant.RoyaltySettlement{}); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("Record: expected ErrNotImplemented; got %v", err)
	}
	if _, err := r.Exists(context.Background(), "evt-1"); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("Exists: expected ErrNotImplemented; got %v", err)
	}
}

func TestRoyaltyRepo_Record_RejectsNil(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewRoyaltyRepo(&stubTxRunner{q: q})
	if err := r.Record(tracing.WithTenantID(context.Background(), tenantID), nil); !errors.Is(err, grant.ErrInvalidArgument) {
		t.Fatalf("expected grant.ErrInvalidArgument; got %v", err)
	}
}

func TestRoyaltyRepo_Record_Inserts(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewRoyaltyRepo(&stubTxRunner{q: q})
	s := &grant.RoyaltySettlement{
		GrantID: "grant-1", OwnerGCID: authorGCID, GranteeTenantID: tenantID,
		AtomID: "atom-1", Amount: 3.5, Currency: "reputation", UsageContext: "duel",
		SourceEventID: "evt-1", CreatedAt: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
	}
	if err := r.Record(tracing.WithTenantID(context.Background(), tenantID), s); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL") {
		t.Fatalf("first SQL must be SET LOCAL; got %q", q.sqls[0])
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "INSERT INTO royalty_settlements") || !strings.Contains(last, "ON CONFLICT (source_event_id) DO NOTHING") {
		t.Fatalf("expected idempotent INSERT; got %q", last)
	}
	args := q.args[len(q.args)-1]
	if len(args) != 10 {
		t.Fatalf("insert binds 10 args; got %v", args)
	}
	// tenant_id column = grantee_tenant_id (debit side).
	if tnt, _ := args[0].(string); tnt != tenantID {
		t.Fatalf("tenant_id must be grantee tenant; got %v", args[0])
	}
}

func TestRoyaltyRepo_Record_ConflictReturnsAlreadySettled(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{execTagFn: func(sql string) rls.CommandTag { return rls.CommandTag{RowsAffected: 0} }}
	r := pg.NewRoyaltyRepo(&stubTxRunner{q: q})
	s := &grant.RoyaltySettlement{GranteeTenantID: tenantID, SourceEventID: "evt-1"}
	err := r.Record(tracing.WithTenantID(context.Background(), tenantID), s)
	if !errors.Is(err, grant.ErrRoyaltyAlreadySettled) {
		t.Fatalf("expected ErrRoyaltyAlreadySettled; got %v", err)
	}
}

func TestRoyaltyRepo_Record_ExecErrorPropagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerierB{execErrFn: func(sql string) error {
		if strings.Contains(sql, "SET LOCAL") {
			return nil
		}
		return errors.New("boom")
	}}
	r := pg.NewRoyaltyRepo(&stubTxRunnerB{q: q})
	s := &grant.RoyaltySettlement{GranteeTenantID: tenantID, SourceEventID: "evt-1"}
	err := r.Record(tracing.WithTenantID(context.Background(), tenantID), s)
	if err == nil || !strings.Contains(err.Error(), "insert royalty settlement") {
		t.Fatalf("expected wrapped insert error; got %v", err)
	}
}

func TestRoyaltyRepo_Exists_FoundAndAbsent(t *testing.T) {
	t.Parallel()

	t.Run("found", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error {
				bSetInto(dest, 0, 1)
				return nil
			}}
		}}
		r := pg.NewRoyaltyRepo(&stubTxRunner{q: q})
		exists, err := r.Exists(tracing.WithTenantID(context.Background(), tenantID), "evt-1")
		if err != nil || !exists {
			t.Fatalf("expected exists=true; got %v, %v", exists, err)
		}
		if !strings.Contains(q.sqls[len(q.sqls)-1], "SELECT 1 FROM royalty_settlements") {
			t.Fatalf("expected SELECT 1; got %q", q.sqls[len(q.sqls)-1])
		}
	})

	t.Run("absent", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error { return errors.New("no rows") }}
		}}
		r := pg.NewRoyaltyRepo(&stubTxRunner{q: q})
		exists, err := r.Exists(tracing.WithTenantID(context.Background(), tenantID), "evt-1")
		if err != nil || exists {
			t.Fatalf("expected exists=false, nil err; got %v, %v", exists, err)
		}
	})
}
