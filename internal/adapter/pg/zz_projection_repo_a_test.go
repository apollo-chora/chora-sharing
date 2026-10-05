// zz_projection_repo_a_test.go — "A"-lane coverage for projection_repo.go
// (Get / Upsert / Invalidate / SetReuseVisibility / ListRandom) and the
// atom_projection.go leftovers the existing suite does not reach
// (ListRandom delegate, exec-error branches, published_at hydration).
//
// Reuses the shared harness + zz_harness_extra_a_test.go helpers.
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
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
)

// zqProjRow fills the 12-column sqlProjectionGet scan surface (tenant_id at
// dest[1]; published_at dest[7] is **time.Time; options dest[10] is *[]string).
func zqProjRow(atomID, revID, owner, display, stem, qType string, publishedAt *time.Time, archived bool, reuseVis string, options []string, correct string) func(dest ...any) error {
	return func(dest ...any) error {
		zqSetString(dest, 0, atomID)
		zqSetString(dest, 1, tenantID)
		zqSetString(dest, 2, revID)
		zqSetString(dest, 3, owner)
		zqSetString(dest, 4, display)
		zqSetString(dest, 5, stem)
		zqSetString(dest, 6, qType)
		if publishedAt != nil {
			zqSetTimePtr(dest, 7, *publishedAt)
		}
		if p, ok := dest[8].(*bool); ok {
			*p = archived
		}
		zqSetString(dest, 9, reuseVis)
		if p, ok := dest[10].(*[]string); ok {
			*p = options
		}
		zqSetString(dest, 11, correct)
		return nil
	}
}

// zqProjRow11 fills the 11-column sqlProjectionListRandom scan surface (NO
// tenant_id; published_at dest[6] is **time.Time; options dest[9] is *[]string).
func zqProjRow11(atomID, revID, owner, display, stem, qType string, publishedAt *time.Time, archived bool, reuseVis string, options []string, correct string) func(dest ...any) error {
	return func(dest ...any) error {
		zqSetString(dest, 0, atomID)
		zqSetString(dest, 1, revID)
		zqSetString(dest, 2, owner)
		zqSetString(dest, 3, display)
		zqSetString(dest, 4, stem)
		zqSetString(dest, 5, qType)
		if publishedAt != nil {
			zqSetTimePtr(dest, 6, *publishedAt)
		}
		if p, ok := dest[7].(*bool); ok {
			*p = archived
		}
		zqSetString(dest, 8, reuseVis)
		if p, ok := dest[9].(*[]string); ok {
			*p = options
		}
		zqSetString(dest, 10, correct)
		return nil
	}
}

// ------------------------------------------------------------------ Get

func TestProjectionRepo_Get_HydratesPublishedAt(t *testing.T) {
	t.Parallel()
	publishedAt := time.Date(2026, 7, 11, 9, 0, 0, 123000000, time.UTC)
	q := &stubQuerier{}
	q.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: zqProjRow("atom-1", "rev-1", authorGCID, "Ada", "stem", "mcq", &publishedAt, false, "tenant", []string{"A"}, "A")}
	}
	r := pg.NewProjectionRepo(&stubTxRunner{q: q})
	p, err := r.Get(aCtx(), "atom-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if p.PublishedAt.IsZero() || !p.PublishedAt.Equal(publishedAt.UTC()) {
		t.Fatalf("published_at not hydrated: %v", p.PublishedAt)
	}
	if p.QuestionType != atom_projection.QuestionTypeMCQ || p.ReuseVisibility != "tenant" || p.CorrectAnswer != "A" {
		t.Fatalf("mapped projection wrong: %+v", p)
	}
}

func TestProjectionRepo_Get_NonNoRowsScanError_Propagates(t *testing.T) {
	t.Parallel()
	boom := errors.New("column does not exist")
	q := &stubQuerier{}
	q.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: func(dest ...any) error { return boom }}
	}
	r := pg.NewProjectionRepo(&stubTxRunner{q: q})
	if _, err := r.Get(aCtx(), "atom-1"); err == nil {
		t.Fatalf("expected the scan error to propagate")
	}
}

func TestProjectionRepo_Get_NilTxRunner(t *testing.T) {
	t.Parallel()
	if _, err := pg.NewProjectionRepo(nil).Get(aCtx(), "atom-1"); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

func TestProjectionRepo_Get_MissingTenant_FailsLoud(t *testing.T) {
	t.Parallel()
	r := pg.NewProjectionRepo(&stubTxRunner{q: &stubQuerier{}})
	if _, err := r.Get(context.Background(), "atom-1"); err == nil {
		t.Fatalf("expected ErrNoTenantContext on a bare context")
	}
}

// ------------------------------------------------------------------ Upsert

func TestProjectionRepo_Upsert_HappyPath_BindsArgs(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewProjectionRepo(&stubTxRunner{q: q})

	publishedAt := time.Date(2026, 7, 11, 9, 0, 0, 0, time.UTC)
	p := atom_projection.Projection{
		AtomID:            "atom-1",
		TenantID:          tenantID,
		RevisionID:        "rev-1",
		OwnerGCID:         authorGCID,
		AuthorDisplayName: "Ada",
		Stem:              "What is 2+2?",
		QuestionType:      atom_projection.QuestionTypeMCQ,
		PublishedAt:       publishedAt,
		ReuseVisibility:   "tenant",
		Options:           []string{"A", "B"},
		CorrectAnswer:     "A",
	}
	if err := r.Upsert(aCtx(), p); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	ins := aLastSQL(q)
	if !strings.Contains(ins, "INSERT INTO atom_projections") || !strings.Contains(ins, "ON CONFLICT (atom_id) DO UPDATE") {
		t.Fatalf("upsert SQL malformed; got %q", ins)
	}
	args := q.args[len(q.args)-1]
	if args[2] != "rev-1" {
		t.Fatalf("revision_id arg = %v", args[2])
	}
	if args[7] == nil {
		t.Fatalf("non-zero published_at must bind a value")
	}
}

func TestProjectionRepo_Upsert_QuestionLess_BindsNullRevisionAndTime(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewProjectionRepo(&stubTxRunner{q: q})
	p := atom_projection.Projection{
		AtomID:    "atom-1",
		TenantID:  tenantID,
		OwnerGCID: authorGCID,
		Stem:      "s",
	}
	if err := r.Upsert(aCtx(), p); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	args := q.args[len(q.args)-1]
	if args[2] != nil {
		t.Fatalf("empty revision_id must bind nil (uuid column); got %v", args[2])
	}
	if args[7] != nil {
		t.Fatalf("zero published_at must bind nil; got %v", args[7])
	}
}

func TestProjectionRepo_Upsert_ExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "INSERT INTO atom_projections", execErr: aErrExec}
	r := pg.NewProjectionRepo(&aTxRunner{q: q})
	if err := r.Upsert(aCtx(), atom_projection.Projection{AtomID: "atom-1", TenantID: tenantID, OwnerGCID: authorGCID}); err == nil {
		t.Fatalf("expected the upsert error to propagate")
	}
}

func TestProjectionRepo_Upsert_NilTxRunner(t *testing.T) {
	t.Parallel()
	if err := pg.NewProjectionRepo(nil).Upsert(aCtx(), atom_projection.Projection{AtomID: "atom-1", TenantID: tenantID, OwnerGCID: authorGCID}); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

func TestProjectionRepo_Upsert_MissingTenant_FailsLoud(t *testing.T) {
	t.Parallel()
	r := pg.NewProjectionRepo(&stubTxRunner{q: &stubQuerier{}})
	if err := r.Upsert(context.Background(), atom_projection.Projection{AtomID: "atom-1", TenantID: tenantID, OwnerGCID: authorGCID}); err == nil {
		t.Fatalf("expected ErrNoTenantContext on a bare context")
	}
}

// -------------------------------------------------------------- Invalidate

func TestProjectionRepo_Invalidate_AppliesRLSThenArchives(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewProjectionRepo(&stubTxRunner{q: q})
	if err := r.Invalidate(aCtx(), "atom-1"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	if len(q.sqls) < 2 || !strings.Contains(q.sqls[0], "SET LOCAL chora.tenant_id") {
		t.Fatalf("first SQL must be SET LOCAL; got %#v", q.sqls)
	}
	if !strings.Contains(aLastSQL(q), "SET archived = TRUE") {
		t.Fatalf("invalidate SQL malformed; got %q", aLastSQL(q))
	}
}

func TestProjectionRepo_Invalidate_ExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "SET archived = TRUE", execErr: aErrExec}
	r := pg.NewProjectionRepo(&aTxRunner{q: q})
	if err := r.Invalidate(aCtx(), "atom-1"); err == nil {
		t.Fatalf("expected the invalidate error to propagate")
	}
}

func TestProjectionRepo_Invalidate_NilTxRunner(t *testing.T) {
	t.Parallel()
	if err := pg.NewProjectionRepo(nil).Invalidate(aCtx(), "atom-1"); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

func TestProjectionRepo_Invalidate_MissingTenant_FailsLoud(t *testing.T) {
	t.Parallel()
	r := pg.NewProjectionRepo(&stubTxRunner{q: &stubQuerier{}})
	if err := r.Invalidate(context.Background(), "atom-1"); err == nil {
		t.Fatalf("expected ErrNoTenantContext on a bare context")
	}
}

func TestProjectionRepo_SetReuseVisibility_NilTxRunner(t *testing.T) {
	t.Parallel()
	if err := pg.NewProjectionRepo(nil).SetReuseVisibility(aCtx(), "atom-1", "friends"); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

func TestProjectionRepo_SetReuseVisibility_MissingTenant_FailsLoud(t *testing.T) {
	t.Parallel()
	r := pg.NewProjectionRepo(&stubTxRunner{q: &stubQuerier{}})
	if err := r.SetReuseVisibility(context.Background(), "atom-1", "friends"); err == nil {
		t.Fatalf("expected ErrNoTenantContext on a bare context")
	}
}

// ------------------------------------------------------- SetReuseVisibility

func TestProjectionRepo_SetReuseVisibility_HappyPath(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewProjectionRepo(&stubTxRunner{q: q})
	if err := r.SetReuseVisibility(aCtx(), "atom-1", "friends"); err != nil {
		t.Fatalf("SetReuseVisibility: %v", err)
	}
	args := q.args[len(q.args)-1]
	if args[1] != "friends" {
		t.Fatalf("visibility arg = %v", args[1])
	}
}

func TestProjectionRepo_SetReuseVisibility_RejectsUnknownLabel(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewProjectionRepo(&stubTxRunner{q: q})
	if err := r.SetReuseVisibility(aCtx(), "atom-1", "everyone-on-internet"); !errors.Is(err, atom_projection.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument; got %v", err)
	}
	if len(q.sqls) != 0 {
		t.Fatalf("invalid label must not reach SQL; got %v", q.sqls)
	}
}

func TestProjectionRepo_SetReuseVisibility_ExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "SET reuse_visibility = $2", execErr: aErrExec}
	r := pg.NewProjectionRepo(&aTxRunner{q: q})
	if err := r.SetReuseVisibility(aCtx(), "atom-1", "friends"); err == nil {
		t.Fatalf("expected the visibility error to propagate")
	}
}

// -------------------------------------------------------------- ListRandom

func TestProjectionRepo_ListRandom_HappyPath(t *testing.T) {
	t.Parallel()
	publishedAt := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		return &stubRows{scans: []func(dest ...any) error{
			zqProjRow11("a-1", "r1", "o1", "One", "stem one", "mcq", nil, false, "private", nil, ""),
			zqProjRow11("a-2", "r2", "o2", "Two", "stem two", "essay", &publishedAt, false, "tenant", []string{"X"}, "X"),
		}}, nil
	}
	r := pg.NewProjectionRepo(&stubTxRunner{q: q})
	out, err := r.ListRandom(aCtx(), tenantID, []string{"exclude-1"}, 10)
	if err != nil {
		t.Fatalf("ListRandom: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("out = %+v", out)
	}
	if out[1].PublishedAt.IsZero() || !out[1].PublishedAt.Equal(publishedAt.UTC()) {
		t.Fatalf("published_at not hydrated for row 2: %v", out[1].PublishedAt)
	}
	sel := aLastSQL(q)
	if !strings.Contains(sel, "owner_gcid <> ALL($2::uuid[])") || !strings.Contains(sel, "ORDER BY RANDOM()") {
		t.Fatalf("ListRandom SELECT malformed; got %q", sel)
	}
}

func TestProjectionRepo_ListRandom_Defaults(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewProjectionRepo(&stubTxRunner{q: q})
	_, err := r.ListRandom(aCtx(), tenantID, nil, 0)
	if err != nil {
		t.Fatalf("ListRandom: %v", err)
	}
	args := q.args[len(q.args)-1]
	if args[2] != 5 {
		t.Fatalf("limit<=0 must default to 5; got args %v", args)
	}
	excludes, ok := args[1].([]string)
	if !ok || len(excludes) != 1 || excludes[0] != "" {
		t.Fatalf("empty excludes must become [\"\"]; got %v", args[1])
	}
}

func TestProjectionRepo_ListRandom_QueryError_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("db down") }
	r := pg.NewProjectionRepo(&stubTxRunner{q: q})
	if _, err := r.ListRandom(aCtx(), tenantID, nil, 1); err == nil {
		t.Fatalf("expected the query error to propagate")
	}
}

func TestProjectionRepo_ListRandom_ScanError_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		return &stubRows{scans: []func(dest ...any) error{
			func(dest ...any) error { return errors.New("scan boom") },
		}}, nil
	}
	r := pg.NewProjectionRepo(&stubTxRunner{q: q})
	if _, err := r.ListRandom(aCtx(), tenantID, nil, 1); err == nil {
		t.Fatalf("expected the scan error to propagate")
	}
}

func TestProjectionRepo_ListRandom_RowsErr_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		return &stubRows{err: errors.New("cursor error"), scans: []func(dest ...any) error{
			zqProjRow11("a-1", "r1", "o1", "One", "s", "mcq", nil, false, "private", nil, ""),
		}}, nil
	}
	r := pg.NewProjectionRepo(&stubTxRunner{q: q})
	if _, err := r.ListRandom(aCtx(), tenantID, nil, 1); err == nil {
		t.Fatalf("expected the rows.Err() to propagate")
	}
}

func TestProjectionRepo_ListRandom_NilTxRunner(t *testing.T) {
	t.Parallel()
	if _, err := pg.NewProjectionRepo(nil).ListRandom(aCtx(), tenantID, nil, 1); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

func TestProjectionRepo_ListRandom_MissingTenant_FailsLoud(t *testing.T) {
	t.Parallel()
	r := pg.NewProjectionRepo(&stubTxRunner{q: &stubQuerier{}})
	if _, err := r.ListRandom(context.Background(), tenantID, nil, 1); err == nil {
		t.Fatalf("expected ErrNoTenantContext on a bare context")
	}
}

// -------------------------------------------------- AtomProjectionStore ties

// AtomProjectionStore.ListRandom is a one-line delegate to ProjectionRepo —
// drive it here so atom_projection.go's delegate statement is covered.
func TestAtomProjectionStore_ListRandom_DelegatesToProjectionRepo(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		return &stubRows{scans: []func(dest ...any) error{
			zqProjRow11("a-1", "r1", "o1", "One", "s", "mcq", nil, false, "private", nil, ""),
		}}, nil
	}
	s := pg.NewAtomProjectionStore(&stubTxRunner{q: q})
	out, err := s.ListRandom(aCtx(), tenantID, nil, 2)
	if err != nil {
		t.Fatalf("ListRandom: %v", err)
	}
	if len(out) != 1 || out[0].AtomID != "a-1" {
		t.Fatalf("out = %+v", out)
	}
}

func TestAtomProjectionStore_Get_HydratesPublishedAt(t *testing.T) {
	t.Parallel()
	publishedAt := time.Date(2026, 7, 11, 9, 0, 0, 0, time.UTC)
	q := &stubQuerier{
		rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error {
				// 10-column SQLSelectAtomProjectionByID surface: atom_id,
				// revision_id, owner_gcid, author_display_name, stem,
				// question_type, published_at, reuse_visibility, options,
				// correct_answer.
				zqSetString(dest, 0, "atom-1")
				zqSetString(dest, 1, "rev-1")
				zqSetString(dest, 2, authorGCID)
				zqSetString(dest, 3, "Ada")
				zqSetString(dest, 4, "stem")
				zqSetString(dest, 5, "mcq")
				zqSetTimePtr(dest, 6, publishedAt)
				zqSetString(dest, 7, "tenant")
				if p, ok := dest[8].(*[]string); ok {
					*p = []string{"A"}
				}
				zqSetString(dest, 9, "A")
				return nil
			}}
		},
	}
	s := pg.NewAtomProjectionStore(&stubTxRunner{q: q})
	got, err := s.Get(aCtx(), "atom-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.PublishedAt.IsZero() || !got.PublishedAt.Equal(publishedAt.UTC()) {
		t.Fatalf("published_at not hydrated: %v", got.PublishedAt)
	}
}

func TestAtomProjectionStore_Upsert_InvalidProjection_FailsLoud(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	s := pg.NewAtomProjectionStore(&stubTxRunner{q: q})
	// Projection without owner_gcid fails p.Validate() BEFORE any SQL.
	if err := s.Upsert(aCtx(), atom_projection.Projection{AtomID: "atom-1", RevisionID: "r1"}); err == nil {
		t.Fatalf("expected the projection validation error to propagate")
	}
	if len(q.sqls) != 0 {
		t.Fatalf("invalid projection must not touch the DB; got %v", q.sqls)
	}
}

func TestAtomProjectionStore_Upsert_RLSFailure_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "SET LOCAL", execErr: aErrExec}
	s := pg.NewAtomProjectionStore(&aTxRunner{q: q})
	if err := s.Upsert(aCtx(), validProjection()); err == nil {
		t.Fatalf("expected the SET LOCAL failure to propagate")
	}
}

func TestAtomProjectionStore_Invalidate_RLSFailure_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "SET LOCAL", execErr: aErrExec}
	s := pg.NewAtomProjectionStore(&aTxRunner{q: q})
	if err := s.Invalidate(aCtx(), projAtomID); err == nil {
		t.Fatalf("expected the SET LOCAL failure to propagate")
	}
}

func TestAtomProjectionStore_SetReuseVisibility_ExecError_FailsLoud(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "UPDATE atom_projections", execErr: aErrExec}
	s := pg.NewAtomProjectionStore(&aTxRunner{q: q})
	if err := s.SetReuseVisibility(aCtx(), projAtomID, "friends"); err == nil {
		t.Fatalf("expected the visibility exec error to propagate (fail-loud)")
	}
}

func TestAtomProjectionStore_SetReuseVisibility_NilTxRunner(t *testing.T) {
	t.Parallel()
	if err := pg.NewAtomProjectionStore(nil).SetReuseVisibility(aCtx(), projAtomID, "friends"); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

func TestAtomProjectionStore_Get_MissingTenant_FailsLoud(t *testing.T) {
	t.Parallel()
	s := pg.NewAtomProjectionStore(&stubTxRunner{q: &stubQuerier{}})
	if _, err := s.Get(context.Background(), projAtomID); err == nil {
		t.Fatalf("expected ErrNoTenantContext on a bare context")
	}
}

// Assert the RLS import stays honest (CommandTag used by aExecFailQuerier).
var _ = rls.CommandTag{}
var _ = tracing.WithTenantID