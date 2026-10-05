// atom_projection_test.go — unit tests for the pgx-backed AtomProjectionStore
// (Atom Sharing Redesign — the durable read-model behind ShareAtom's R1
// author-of-record gate). Reuses the stub Querier / TxRunner harness defined
// in post_test.go (same pg_test package); live RLS isolation is covered by the
// integration suite.
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-common/tracing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
)

const (
	projAtomID = "01970000-0000-7000-b000-000000000001"
	projRevID  = "01970000-0000-7000-b000-0000000000a1"
)

func validProjection() atom_projection.Projection {
	return atom_projection.Projection{
		AtomID:            projAtomID,
		RevisionID:        projRevID,
		OwnerGCID:         authorGCID, // declared in post_test.go (same pg_test pkg)
		AuthorDisplayName: "Ada Lovelace",
		Stem:              "What is 2+2?",
		QuestionType:      atom_projection.QuestionTypeMCQ,
	}
}

func TestAtomProjectionStore_NilTxRunner_ReturnsErrNotImplemented(t *testing.T) {
	t.Parallel()
	s := pg.NewAtomProjectionStore(nil)
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	if err := s.Upsert(ctx, validProjection()); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("Upsert: expected ErrNotImplemented; got %v", err)
	}
	if _, err := s.Get(ctx, projAtomID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("Get: expected ErrNotImplemented; got %v", err)
	}
	if err := s.Invalidate(ctx, projAtomID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("Invalidate: expected ErrNotImplemented; got %v", err)
	}
}

func TestAtomProjectionStore_Upsert_AppliesRLSThenUpserts(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	s := pg.NewAtomProjectionStore(&stubTxRunner{q: q})

	ctx := tracing.WithTenantID(context.Background(), tenantID)
	if err := s.Upsert(ctx, validProjection()); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if len(q.sqls) < 2 {
		t.Fatalf("expected SET LOCAL + UPSERT; got %d SQLs", len(q.sqls))
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL chora.tenant_id") {
		t.Fatalf("first SQL must be SET LOCAL chora.tenant_id; got %q", q.sqls[0])
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "INSERT INTO atom_projections") {
		t.Fatalf("expected INSERT INTO atom_projections; got %q", last)
	}
	if !strings.Contains(last, "ON CONFLICT (atom_id) DO UPDATE") {
		t.Fatalf("expected idempotent UPSERT clause; got %q", last)
	}
	// tenant_id is bound from ctx (the Projection carries none); it is arg $2.
	lastArgs := q.args[len(q.args)-1]
	if got, ok := lastArgs[1].(string); !ok || got != tenantID {
		t.Fatalf("expected tenant_id bound from ctx as arg[1]=%q; got %v", tenantID, lastArgs[1])
	}
	if got, ok := lastArgs[0].(string); !ok || got != projAtomID {
		t.Fatalf("expected atom_id arg[0]=%q; got %v", projAtomID, lastArgs[0])
	}
}

func TestAtomProjectionStore_Upsert_RejectsMissingTenantContext(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	s := pg.NewAtomProjectionStore(&stubTxRunner{q: q})
	// No tenant on ctx → rls.ApplySession must fail loud BEFORE any upsert.
	err := s.Upsert(context.Background(), validProjection())
	if !errors.Is(err, rls.ErrNoTenantContext) {
		t.Fatalf("expected ErrNoTenantContext; got %v", err)
	}
}

func TestAtomProjectionStore_Get_NoRow_ReturnsErrNotFound(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error {
				return errors.New("no rows in result set")
			}}
		},
	}
	s := pg.NewAtomProjectionStore(&stubTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	_, err := s.Get(ctx, "unknown-atom")
	if !errors.Is(err, atom_projection.ErrNotFound) {
		t.Fatalf("expected atom_projection.ErrNotFound; got %v", err)
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL chora.tenant_id") {
		t.Fatalf("first SQL must be SET LOCAL chora.tenant_id; got %q", q.sqls[0])
	}
}

func TestAtomProjectionStore_Get_HappyPath(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error {
				// 7 columns: atom_id, revision_id, owner_gcid,
				// author_display_name, stem, question_type, published_at.
				set := func(i int, v string) {
					if p, ok := dest[i].(*string); ok {
						*p = v
					}
				}
				set(0, projAtomID)
				set(1, projRevID)
				set(2, authorGCID)
				set(3, "Ada Lovelace")
				set(4, "What is 2+2?")
				set(5, "mcq")
				// dest[6] is *time.Time (nullable) — leave nil (NULL published_at).
				return nil
			}}
		},
	}
	s := pg.NewAtomProjectionStore(&stubTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	got, err := s.Get(ctx, projAtomID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.AtomID != projAtomID || got.RevisionID != projRevID || got.OwnerGCID != authorGCID {
		t.Fatalf("scanned projection mismatched: %+v", got)
	}
	if got.QuestionType != atom_projection.QuestionTypeMCQ {
		t.Fatalf("question_type not mapped: %+v", got)
	}
	// The read-side excludes archived + soft-deleted rows (R1 contract).
	sel := q.sqls[len(q.sqls)-1]
	if !strings.Contains(sel, "FROM atom_projections") ||
		!strings.Contains(sel, "archived = false") ||
		!strings.Contains(sel, "deleted_at IS NULL") {
		t.Fatalf("SELECT must filter archived + soft-delete; got %q", sel)
	}
}

func TestAtomProjectionStore_Invalidate_AppliesRLSThenArchives(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	s := pg.NewAtomProjectionStore(&stubTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	if err := s.Invalidate(ctx, projAtomID); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL chora.tenant_id") {
		t.Fatalf("first SQL must be SET LOCAL chora.tenant_id; got %q", q.sqls[0])
	}
	last := q.sqls[len(q.sqls)-1]
	// Soft-delete invariant: archived flag flip, NEVER a DELETE.
	if !strings.Contains(last, "UPDATE atom_projections") || !strings.Contains(last, "archived = true") {
		t.Fatalf("expected archived-flag UPDATE; got %q", last)
	}
	if strings.Contains(strings.ToUpper(last), "DELETE FROM") {
		t.Fatalf("Invalidate must NEVER hard-delete; got %q", last)
	}
}

// projFailQuerier succeeds on the SET LOCAL (rls.ApplySession) but fails on the
// user query (atom_projections), so the store's write-error-wrap (fail-loud)
// path is exercised — the subscriber NACKs on this → broker retry/DLQ.
type projFailQuerier struct {
	sqls []string
	err  error
}

func (q *projFailQuerier) Exec(_ context.Context, sql string, _ ...any) (rls.CommandTag, error) {
	q.sqls = append(q.sqls, sql)
	if strings.Contains(sql, "atom_projections") {
		return rls.CommandTag{}, q.err
	}
	return rls.CommandTag{RowsAffected: 1}, nil // SET LOCAL succeeds
}
func (q *projFailQuerier) QueryRow(_ context.Context, sql string, _ ...any) pg.Row {
	q.sqls = append(q.sqls, sql)
	return stubRow{scanFn: func(dest ...any) error { return q.err }}
}
func (q *projFailQuerier) Query(_ context.Context, sql string, _ ...any) (pg.Rows, error) {
	q.sqls = append(q.sqls, sql)
	return &stubRows{}, q.err
}

func TestAtomProjectionStore_Upsert_ExecError_FailsLoud(t *testing.T) {
	t.Parallel()
	q := &projFailQuerier{err: errors.New("db down")}
	store := pg.NewAtomProjectionStore(&projFailTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	if err := store.Upsert(ctx, validProjection()); err == nil {
		t.Fatalf("expected upsert exec error to propagate (fail-loud), got nil")
	}
}

func TestAtomProjectionStore_Invalidate_ExecError_FailsLoud(t *testing.T) {
	t.Parallel()
	q := &projFailQuerier{err: errors.New("db down")}
	store := pg.NewAtomProjectionStore(&projFailTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	if err := store.Invalidate(ctx, projAtomID); err == nil {
		t.Fatalf("expected invalidate exec error to propagate (fail-loud), got nil")
	}
}

// projFailTxRunner runs fn against the projFailQuerier (mirrors stubTxRunner).
type projFailTxRunner struct{ q *projFailQuerier }

func (r *projFailTxRunner) RunInTx(ctx context.Context, fn func(ctx context.Context, q pg.Querier) error) error {
	return fn(ctx, r.q)
}

func TestSQLAtomProjectionTemplates_AreExported(t *testing.T) {
	t.Parallel()
	if !strings.Contains(pg.SQLUpsertAtomProjection, "INSERT INTO atom_projections") ||
		!strings.Contains(pg.SQLUpsertAtomProjection, "ON CONFLICT (atom_id) DO UPDATE") {
		t.Fatalf("SQLUpsertAtomProjection malformed")
	}
	if !strings.Contains(pg.SQLSelectAtomProjectionByID, "FROM atom_projections") {
		t.Fatalf("SQLSelectAtomProjectionByID malformed")
	}
	if !strings.Contains(pg.SQLInvalidateAtomProjection, "UPDATE atom_projections") {
		t.Fatalf("SQLInvalidateAtomProjection malformed")
	}
}

// Compile-time proof the store satisfies BOTH domain ports.
var (
	_ atom_projection.AtomProjectionWriter = (*pg.AtomProjectionStore)(nil)
	_ atom_projection.AtomProjectionReader = (*pg.AtomProjectionStore)(nil)
)
