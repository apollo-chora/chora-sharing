// zz_atom_reuse_a_test.go — "A"-lane coverage for the atom_reuse_repo.go
// error/validation branches the existing suite does not drive (nil TxRunner,
// exec failures, scan/cursor failures, Now() fallback).
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/rls"

	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_reuse"
)

// ---------------------------------------------------- ActiveGrantRefsForAtom

func TestAtomReuseRepo_ActiveGrantRefsForAtom_NilTxRunner(t *testing.T) {
	t.Parallel()
	if _, err := pg.NewAtomReuseRepo(nil, pg.AtomReuseRepoOptions{}).ActiveGrantRefsForAtom(reuseCtx(), reuseAtom); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

func TestAtomReuseRepo_ActiveGrantRefsForAtom_EmptyAtomID(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	repo := pg.NewAtomReuseRepo(&stubTxRunner{q: q}, pg.AtomReuseRepoOptions{})
	if _, err := repo.ActiveGrantRefsForAtom(reuseCtx(), "  "); err == nil {
		t.Fatalf("expected an error for a blank atom id")
	}
	if len(q.sqls) != 0 {
		t.Fatalf("validation failure must not touch the DB; got %v", q.sqls)
	}
}

func TestAtomReuseRepo_ActiveGrantRefsForAtom_QueryError_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("db down") }
	repo := pg.NewAtomReuseRepo(&stubTxRunner{q: q}, pg.AtomReuseRepoOptions{})
	if _, err := repo.ActiveGrantRefsForAtom(reuseCtx(), reuseAtom); err == nil {
		t.Fatalf("expected the query error to propagate")
	}
}

func TestAtomReuseRepo_ActiveGrantRefsForAtom_ScanError_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		return &stubRows{scans: []func(dest ...any) error{
			func(dest ...any) error { return errors.New("scan boom") },
		}}, nil
	}
	repo := pg.NewAtomReuseRepo(&stubTxRunner{q: q}, pg.AtomReuseRepoOptions{})
	if _, err := repo.ActiveGrantRefsForAtom(reuseCtx(), reuseAtom); err == nil {
		t.Fatalf("expected the scan error to propagate")
	}
}

func TestAtomReuseRepo_ActiveGrantRefsForAtom_RowsErr_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		return &stubRows{err: errors.New("cursor error"), scans: []func(dest ...any) error{
			func(dest ...any) error {
				zqSetString(dest, 0, "g-1")
				zqSetString(dest, 1, reuseGrantee)
				zqSetString(dest, 2, reuseAuthor)
				zqSetString(dest, 3, "duel")
				return nil
			},
		}}, nil
	}
	repo := pg.NewAtomReuseRepo(&stubTxRunner{q: q}, pg.AtomReuseRepoOptions{})
	if _, err := repo.ActiveGrantRefsForAtom(reuseCtx(), reuseAtom); err == nil {
		t.Fatalf("expected the rows.Err() to propagate")
	}
}

func TestAtomReuseRepo_ActiveGrantRefsForAtom_MissingTenant_FailsLoud(t *testing.T) {
	t.Parallel()
	repo := pg.NewAtomReuseRepo(&stubTxRunner{q: &stubQuerier{}}, pg.AtomReuseRepoOptions{})
	if _, err := repo.ActiveGrantRefsForAtom(tracingCtxBare(), reuseAtom); err == nil {
		t.Fatalf("expected ErrNoTenantContext on a bare context")
	}
}

// ------------------------------------------------------------- LatestEdition

func TestAtomReuseRepo_LatestEdition_NilTxRunner(t *testing.T) {
	t.Parallel()
	if _, _, err := pg.NewAtomReuseRepo(nil, pg.AtomReuseRepoOptions{}).LatestEdition(reuseCtx(), reuseAtom); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

func TestAtomReuseRepo_LatestEdition_MissingTenant_FailsLoud(t *testing.T) {
	t.Parallel()
	repo := pg.NewAtomReuseRepo(&stubTxRunner{q: &stubQuerier{}}, pg.AtomReuseRepoOptions{})
	if _, _, err := repo.LatestEdition(tracingCtxBare(), reuseAtom); err == nil {
		t.Fatalf("expected ErrNoTenantContext on a bare context")
	}
}

// --------------------------------------------------------------- PutEdition

func TestAtomReuseRepo_PutEdition_NilTxRunner(t *testing.T) {
	t.Parallel()
	repo := pg.NewAtomReuseRepo(nil, pg.AtomReuseRepoOptions{})
	if err := repo.PutEdition(reuseCtx(), atom_reuse.OrphanEdition{
		AtomID: reuseAtom, SourceRevisionID: reuseRev, OrphanAtomID: reuseOrphan,
	}); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

func TestAtomReuseRepo_PutEdition_ExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "INSERT INTO atom_orphan_editions", execErr: aErrExec}
	repo := pg.NewAtomReuseRepo(&aTxRunner{q: q}, pg.AtomReuseRepoOptions{})
	if err := repo.PutEdition(reuseCtx(), atom_reuse.OrphanEdition{
		AtomID: reuseAtom, SourceRevisionID: reuseRev, OrphanAtomID: reuseOrphan, OrphanedAt: time.Now().UTC(),
	}); err == nil {
		t.Fatalf("expected the insert error to propagate")
	}
}

func TestAtomReuseRepo_PutEdition_MissingTenant_FailsLoud(t *testing.T) {
	t.Parallel()
	repo := pg.NewAtomReuseRepo(&stubTxRunner{q: &stubQuerier{}}, pg.AtomReuseRepoOptions{})
	if err := repo.PutEdition(tracingCtxBare(), atom_reuse.OrphanEdition{
		AtomID: reuseAtom, SourceRevisionID: reuseRev, OrphanAtomID: reuseOrphan,
	}); err == nil {
		t.Fatalf("expected ErrNoTenantContext on a bare context")
	}
}

// ----------------------------------------------------------- RepointStranded

func TestAtomReuseRepo_RepointStranded_NilTxRunner(t *testing.T) {
	t.Parallel()
	repo := pg.NewAtomReuseRepo(nil, pg.AtomReuseRepoOptions{})
	if _, err := repo.RepointStranded(reuseCtx(), atom_reuse.RepointCommand{
		OriginalAtomID: reuseAtom, OrphanAtomID: reuseOrphan, Stranded: []atom_reuse.GrantRef{{GrantID: "g"}},
	}); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

func TestAtomReuseRepo_RepointStranded_RepointExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "SET atom_id = $2", execErr: aErrExec}
	repo := pg.NewAtomReuseRepo(&aTxRunner{q: q}, pg.AtomReuseRepoOptions{})
	_, err := repo.RepointStranded(reuseCtx(), atom_reuse.RepointCommand{
		OriginalAtomID: reuseAtom, OrphanAtomID: reuseOrphan,
		Stranded: []atom_reuse.GrantRef{{GrantID: "g-1", GranteeGCID: reuseGrantee, OwnerGCID: reuseAuthor, Scope: "duel"}},
		Trigger:  atom_reuse.TriggerNarrowed,
	})
	if err == nil {
		t.Fatalf("expected the repoint UPDATE error to propagate")
	}
}

func TestAtomReuseRepo_RepointStranded_RepointTrailError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "INSERT INTO grant_events", execErr: aErrExec}
	repo := pg.NewAtomReuseRepo(&aTxRunner{q: q}, pg.AtomReuseRepoOptions{})
	// repoint UPDATE matches (RowsAffected=1 default) then the trail insert fails.
	_, err := repo.RepointStranded(reuseCtx(), atom_reuse.RepointCommand{
		OriginalAtomID: reuseAtom, OrphanAtomID: reuseOrphan,
		Stranded: []atom_reuse.GrantRef{{GrantID: "g-1", GranteeGCID: reuseGrantee, OwnerGCID: reuseAuthor, Scope: "duel"}},
		Trigger:  atom_reuse.TriggerNarrowed,
	})
	if err == nil {
		t.Fatalf("expected the repoint trail error to propagate")
	}
}

func TestAtomReuseRepo_RepointStranded_MergeExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "SET deleted_at = now()", execErr: aErrExec}
	repo := pg.NewAtomReuseRepo(&aTxRunner{q: q}, pg.AtomReuseRepoOptions{})
	// repoint matches nothing (default tag=1? no — we need repoint=0 rows,
	// merge=hit). aExecFailQuerier WITHOUT the SET LOCAL fail hits everything
	// with tag=1, so repoint "succeeds"; instead make repoint match 0 rows via
	// an execTagFn override on the embedded stubQuerier.
	q.execTagFn = func(sql string) rls.CommandTag {
		if strings.Contains(sql, "SET atom_id") {
			return rls.CommandTag{RowsAffected: 0} // repoint blocked
		}
		return rls.CommandTag{RowsAffected: 1} // merge would hit, but fails
	}
	_, err := repo.RepointStranded(reuseCtx(), atom_reuse.RepointCommand{
		OriginalAtomID: reuseAtom, OrphanAtomID: reuseOrphan,
		Stranded: []atom_reuse.GrantRef{{GrantID: "g-1", GranteeGCID: reuseGrantee, OwnerGCID: reuseAuthor, Scope: "duel"}},
		Trigger:  atom_reuse.TriggerNarrowed,
	})
	if err == nil {
		t.Fatalf("expected the merge UPDATE error to propagate")
	}
}

func TestAtomReuseRepo_RepointStranded_MergeTrailError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "INSERT INTO grant_events", execErr: aErrExec}
	q.execTagFn = func(sql string) rls.CommandTag {
		if strings.Contains(sql, "SET atom_id") {
			return rls.CommandTag{RowsAffected: 0} // repoint blocked
		}
		return rls.CommandTag{RowsAffected: 1} // merge hits → its trail insert fails
	}
	repo := pg.NewAtomReuseRepo(&aTxRunner{q: q}, pg.AtomReuseRepoOptions{})
	_, err := repo.RepointStranded(reuseCtx(), atom_reuse.RepointCommand{
		OriginalAtomID: reuseAtom, OrphanAtomID: reuseOrphan,
		Stranded: []atom_reuse.GrantRef{{GrantID: "g-1", GranteeGCID: reuseGrantee, OwnerGCID: reuseAuthor, Scope: "duel"}},
		Trigger:  atom_reuse.TriggerNarrowed,
	})
	if err == nil {
		t.Fatalf("expected the merge trail error to propagate")
	}
}

func TestAtomReuseRepo_RepointStranded_MissingTenant_FailsLoud(t *testing.T) {
	t.Parallel()
	repo := pg.NewAtomReuseRepo(&stubTxRunner{q: &stubQuerier{}}, pg.AtomReuseRepoOptions{})
	if _, err := repo.RepointStranded(tracingCtxBare(), atom_reuse.RepointCommand{
		OriginalAtomID: reuseAtom, OrphanAtomID: reuseOrphan,
		Stranded: []atom_reuse.GrantRef{{GrantID: "g-1"}},
	}); err == nil {
		t.Fatalf("expected ErrNoTenantContext on a bare context")
	}
}

// --------------------------------------------------------------- RequireOrphan

func TestAtomReuseRepo_RequireOrphan_NilTxRunner(t *testing.T) {
	t.Parallel()
	repo := pg.NewAtomReuseRepo(nil, pg.AtomReuseRepoOptions{})
	if err := repo.RequireOrphan(reuseCtx(), atom_reuse.OrphanRequest{
		AtomID: reuseAtom, RevisionID: reuseRev, Trigger: atom_reuse.TriggerNarrowed, StrandedCount: 1, ActorGCID: reuseAuthor,
	}); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

func TestAtomReuseRepo_RequireOrphan_ZeroDetectedAt_UsesNowFn(t *testing.T) {
	t.Parallel()
	called := false
	q := &stubQuerier{}
	repo := pg.NewAtomReuseRepo(&stubTxRunner{q: q}, pg.AtomReuseRepoOptions{
		SourceProject: "chora-489812",
		Now: func() time.Time {
			called = true
			return time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
		},
	})
	err := repo.RequireOrphan(reuseCtx(), atom_reuse.OrphanRequest{
		AtomID: reuseAtom, RevisionID: reuseRev, Trigger: atom_reuse.TriggerUnshared, StrandedCount: 1, ActorGCID: reuseAuthor,
	})
	if err != nil {
		t.Fatalf("RequireOrphan: %v", err)
	}
	if !called {
		t.Fatalf("zero DetectedAt must fall back to opts.Now()")
	}
}

func TestAtomReuseRepo_RequireOrphan_ExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "INSERT INTO sharing_outbox_events", execErr: aErrExec}
	repo := pg.NewAtomReuseRepo(&aTxRunner{q: q}, pg.AtomReuseRepoOptions{SourceProject: "chora-489812"})
	err := repo.RequireOrphan(reuseCtx(), atom_reuse.OrphanRequest{
		AtomID: reuseAtom, RevisionID: reuseRev, Trigger: atom_reuse.TriggerNarrowed, StrandedCount: 1, DetectedAt: time.Now().UTC(), ActorGCID: reuseAuthor,
	})
	if err == nil {
		t.Fatalf("expected the outbox insert error to propagate")
	}
}

func TestAtomReuseRepo_RequireOrphan_MissingTenant_FailsLoud(t *testing.T) {
	t.Parallel()
	repo := pg.NewAtomReuseRepo(&stubTxRunner{q: &stubQuerier{}}, pg.AtomReuseRepoOptions{SourceProject: "chora-489812"})
	if err := repo.RequireOrphan(tracingCtxBare(), atom_reuse.OrphanRequest{
		AtomID: reuseAtom, RevisionID: reuseRev, Trigger: atom_reuse.TriggerNarrowed, StrandedCount: 1, DetectedAt: time.Now().UTC(), ActorGCID: reuseAuthor,
	}); err == nil {
		t.Fatalf("expected ErrNoTenantContext on a bare context")
	}
}

// ----------------------------------------------------------------- GetAnyState

func TestAtomReuseRepo_GetAnyState_NilTxRunner(t *testing.T) {
	t.Parallel()
	if _, err := pg.NewAtomReuseRepo(nil, pg.AtomReuseRepoOptions{}).GetAnyState(reuseCtx(), reuseAtom); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

func TestAtomReuseRepo_GetAnyState_MissingTenant_FailsLoud(t *testing.T) {
	t.Parallel()
	repo := pg.NewAtomReuseRepo(&stubTxRunner{q: &stubQuerier{}}, pg.AtomReuseRepoOptions{})
	if _, err := repo.GetAnyState(tracingCtxBare(), reuseAtom); err == nil {
		t.Fatalf("expected ErrNoTenantContext on a bare context")
	}
}

// tracingCtxBare returns a context WITHOUT a tenant (ApplySession fails loud).
func tracingCtxBare() context.Context { return context.Background() }