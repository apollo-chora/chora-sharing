// duel_sweeper_repo_test.go — unit tests for the pg DuelRepo's
// ListDuelsWithExpiredRounds (WS3 round-timer sweep).
//
// Mirrors the pg unit-test pattern: stub the Querier so the SQL surface +
// RLS ApplySession ordering are exercised without a live DB. Live RLS
// isolation is verified separately in integration_test.go.
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
)

// TestDuelRepo_ListDuelsWithExpiredRounds_NilTxRunner_ReturnsErrNotImplemented
// pins the nil-guard contract shared by every DuelRepo method.
func TestDuelRepo_ListDuelsWithExpiredRounds_NilTxRunner_ReturnsErrNotImplemented(t *testing.T) {
	t.Parallel()
	r := pg.NewDuelRepo(nil)
	if _, err := r.ListDuelsWithExpiredRounds(context.Background(), tenantID, time.Now()); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

// TestDuelRepo_ListDuelsWithExpiredRounds_AppliesRLSBeforeQuery pins fix #1:
// the method MUST call rls.ApplySession BEFORE the SELECT so the FORCE RLS
// policy (tenant_id = current_setting('chora.tenant_id')) matches rows.
// Without ApplySession the GUC is unset and the query returns 0 rows in
// production — the silent no-op bug.
func TestDuelRepo_ListDuelsWithExpiredRounds_AppliesRLSBeforeQuery(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{}, nil
		},
	}
	tx := &stubTxRunner{q: q}
	r := pg.NewDuelRepo(tx)

	ctx := tracing.WithTenantID(context.Background(), tenantID)
	_, _ = r.ListDuelsWithExpiredRounds(ctx, tenantID, time.Now())

	if len(q.sqls) == 0 {
		t.Fatal("no SQL was executed")
	}
	firstSQL := q.sqls[0]
	if !strings.Contains(firstSQL, "SET LOCAL") {
		t.Fatalf("first SQL must be rls.ApplySession's SET LOCAL (RLS must be applied before the query); got: %s", firstSQL)
	}
}

// TestDuelRepo_ListDuelsWithExpiredRounds_NoTenantContext_FailsLoud pins
// fix #1: without a tenant in context, rls.ApplySession returns
// ErrNoTenantContext — the method must propagate it, NOT swallow it.
// This is the regression guard for the silent no-op: a tenantless server
// ctx must not silently return 0 rows.
func TestDuelRepo_ListDuelsWithExpiredRounds_NoTenantContext_FailsLoud(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	tx := &stubTxRunner{q: q}
	r := pg.NewDuelRepo(tx)

	// No tracing.WithTenantID — simulates the tenantless server ctx that
	// caused the original no-op bug.
	_, err := r.ListDuelsWithExpiredRounds(context.Background(), tenantID, time.Now())
	if !errors.Is(err, rls.ErrNoTenantContext) {
		t.Fatalf("expected ErrNoTenantContext (must fail loud without tenant in ctx); got %v", err)
	}
	// No query SQL should have fired — ApplySession must gate the query.
	if len(q.sqls) > 0 {
		t.Fatalf("SQL fired before RLS gate: %v", q.sqls)
	}
}

// TestDuelRepo_ListDuelsWithExpiredRounds_QueryFiltersByTenantID pins the
// SQL shape: the query MUST filter on tenant_id = $1 so the per-tenant
// sweep only returns duels for the registered tenant (defense-in-depth on
// top of RLS).
func TestDuelRepo_ListDuelsWithExpiredRounds_QueryFiltersByTenantID(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{}, nil
		},
	}
	tx := &stubTxRunner{q: q}
	r := pg.NewDuelRepo(tx)

	ctx := tracing.WithTenantID(context.Background(), tenantID)
	_, _ = r.ListDuelsWithExpiredRounds(ctx, tenantID, time.Now())

	// Find the SELECT query (not the SET LOCAL).
	var selectSQL string
	for _, s := range q.sqls {
		if strings.Contains(s, "SELECT DISTINCT") {
			selectSQL = s
			break
		}
	}
	if selectSQL == "" {
		t.Fatal("no SELECT DISTINCT query was executed")
	}
	if !strings.Contains(selectSQL, "ds.tenant_id = $1") {
		t.Errorf("query must filter on tenant_id = $1 (per-tenant sweep); got: %s", selectSQL)
	}
	if !strings.Contains(selectSQL, "dr.deadline_at < $2") {
		t.Errorf("query must filter on deadline_at < $2; got: %s", selectSQL)
	}
	if !strings.Contains(selectSQL, "dr.resolved_at IS NULL") {
		t.Errorf("query must filter on resolved_at IS NULL; got: %s", selectSQL)
	}
}
