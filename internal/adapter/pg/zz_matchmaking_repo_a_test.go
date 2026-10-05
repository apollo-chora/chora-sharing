// zz_matchmaking_repo_a_test.go — "A"-lane coverage for matchmaking_repo.go
// (the default test build: the existing matchmaking_repo_test.go carries an
// `integration` build tag), driving every method through the shared stub
// harness + aExecFailQuerier so both happy and error branches land.
package pg_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/rls"

	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	domainmm "github.com/apollo-chora/chora-sharing/internal/domain/matchmaking"
)

func aSearcher() domainmm.Searcher {
	now := time.Now().UTC()
	return domainmm.Searcher{
		TenantID:      tenantID,
		GCID:          "01970000-0000-7000-a000-0000000000bb",
		Proficiency:   3,
		InterestTags:  []string{"algebra"},
		EnteredAt:     now.Add(-time.Minute),
		ExpiresAt:     now.Add(time.Hour),
		LastHeartbeat: now,
		QuestionCount: 5,
		Category:      "overall",
		Mode:          "classic",
	}
}

// aQueueRowScan fills the 14-column queueColumns scan surface (see
// scanQueueRow's dest list; row UUID/id is dest[0], status dest[3], mode
// dest[12], blitz_variant dest[13]).
func aQueueRowScan(s domainmm.Searcher) func(dest ...any) error {
	return func(dest ...any) error {
		zqSetString(dest, 0, "row-uuid-1")
		zqSetString(dest, 1, s.TenantID)
		zqSetString(dest, 2, s.GCID)
		zqSetString(dest, 3, string(s.Status))
		if p, ok := dest[4].(*int); ok {
			*p = s.Proficiency
		}
		if p, ok := dest[5].(*[]string); ok {
			*p = s.InterestTags
		}
		zqSetTime(dest, 6, s.EnteredAt)
		zqSetTime(dest, 7, s.ExpiresAt)
		zqSetTime(dest, 8, s.LastHeartbeat)
		if s.MatchedDuelID != "" {
			if p, ok := dest[9].(**string); ok {
				d := s.MatchedDuelID
				*p = &d
			}
		}
		if p, ok := dest[10].(*int); ok {
			*p = s.QuestionCount
		}
		zqSetString(dest, 11, s.Category)
		zqSetString(dest, 12, s.Mode)
		if s.BlitzVariant != "" {
			if p, ok := dest[13].(**string); ok {
				d := s.BlitzVariant
				*p = &d
			}
		}
		return nil
	}
}

// All remaining methods carry the same guarded ApplySession — one compact
// sweep proves the SET LOCAL failure propagates through each of them.
func TestMatchmakingRepo_AllMethods_RLSFailure_Propagates(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		run  func(r *pg.MatchmakingQueueRepo) error
	}{
		{"Enqueue", func(r *pg.MatchmakingQueueRepo) error { return r.Enqueue(aCtx(), aSearcher()) }},
		{"UpdateStatus", func(r *pg.MatchmakingQueueRepo) error { return r.UpdateStatus(aCtx(), tenantID, "g-1", domainmm.StatusCancelled, "") }},
		{"FindByStatus", func(r *pg.MatchmakingQueueRepo) error { _, err := r.FindByStatus(aCtx(), tenantID, domainmm.StatusFinding); return err }},
		{"FindCandidates", func(r *pg.MatchmakingQueueRepo) error { _, err := r.FindCandidates(aCtx(), tenantID); return err }},
		{"ClaimMatch", func(r *pg.MatchmakingQueueRepo) error { return r.ClaimMatch(aCtx(), tenantID, "g-a", "g-b", "d-1") }},
		{"RevertToFinding", func(r *pg.MatchmakingQueueRepo) error { return r.RevertToFinding(aCtx(), tenantID, "g-a", "d-1") }},
		{"GetByGCID", func(r *pg.MatchmakingQueueRepo) error { _, err := r.GetByGCID(aCtx(), tenantID, "g-1"); return err }},
		{"CountFinding", func(r *pg.MatchmakingQueueRepo) error { _, err := r.CountFinding(aCtx(), tenantID); return err }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "SET LOCAL", execErr: aErrExec}
			if err := tc.run(pg.NewMatchmakingQueueRepo(&aTxRunner{q: q})); err == nil {
				t.Fatalf("expected the SET LOCAL failure to propagate")
			}
		})
	}
}

// ------------------------------------------------------------------ Enqueue

func TestMatchmakingRepo_Enqueue_HappyPath_DefaultModeAndTags(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})

	s := aSearcher()
	s.Mode = ""        // default → classic
	s.InterestTags = nil // nil → nonNilTags {}
	if err := r.Enqueue(aCtx(), s); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if len(q.sqls) < 2 || !strings.Contains(q.sqls[0], "SET LOCAL chora.tenant_id") {
		t.Fatalf("first SQL must be SET LOCAL; got %#v", q.sqls)
	}
	ins := aLastSQL(q)
	if !strings.Contains(ins, "INSERT INTO matchmaking_queue") ||
		!strings.Contains(ins, "ON CONFLICT (tenant_id, gcid) WHERE status = 'finding'") {
		t.Fatalf("Enqueue INSERT malformed; got %q", ins)
	}
	args := q.args[len(q.args)-1]
	if args[9] != "classic" {
		t.Fatalf("blank mode must bind 'classic'; got %v", args[9])
	}
	tags, ok := args[3].([]string)
	if !ok || tags == nil || len(tags) != 0 {
		t.Fatalf("nil interest_tags must bind an empty array; got %T %v", args[3], args[3])
	}
	if args[10] != nil {
		t.Fatalf("blank blitz_variant must bind nil; got %v", args[10])
	}
}

func TestMatchmakingRepo_Enqueue_ModeAndBlitzVariantBound(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})

	s := aSearcher()
	s.Mode = "blitz"
	s.BlitzVariant = "race"
	s.InterestTags = []string{"geometry"}
	if err := r.Enqueue(aCtx(), s); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	args := q.args[len(q.args)-1]
	if args[9] != "blitz" {
		t.Fatalf("mode = %v", args[9])
	}
	if args[10] != "race" {
		t.Fatalf("blitz_variant = %v", args[10])
	}
}

func TestMatchmakingRepo_Enqueue_ExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "INSERT INTO matchmaking_queue", execErr: aErrExec}
	r := pg.NewMatchmakingQueueRepo(&aTxRunner{q: q})
	if err := r.Enqueue(aCtx(), aSearcher()); err == nil {
		t.Fatalf("expected the enqueue error to propagate")
	}
}

func TestMatchmakingRepo_Enqueue_NilTxRunner(t *testing.T) {
	t.Parallel()
	if err := pg.NewMatchmakingQueueRepo(nil).Enqueue(aCtx(), aSearcher()); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

// ----------------------------------------------------------- UpdateStatus

func TestMatchmakingRepo_UpdateStatus_HappyPath(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	if err := r.UpdateStatus(aCtx(), tenantID, "g-1", domainmm.StatusCancelled, ""); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}
	if !strings.Contains(aLastSQL(q), "SET status = $3") {
		t.Fatalf("status UPDATE malformed; got %q", aLastSQL(q))
	}
	args := q.args[len(q.args)-1]
	if args[3] != nil {
		t.Fatalf("blank matched_duel_id must bind nil; got %v", args[3])
	}
}

func TestMatchmakingRepo_UpdateStatus_MatchedDuelIDBound(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	if err := r.UpdateStatus(aCtx(), tenantID, "g-1", "matched", "duel-9"); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}
	args := q.args[len(q.args)-1]
	if args[3] != "duel-9" {
		t.Fatalf("matched_duel_id = %v", args[3])
	}
}

func TestMatchmakingRepo_UpdateStatus_ExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "SET status = $3", execErr: aErrExec}
	r := pg.NewMatchmakingQueueRepo(&aTxRunner{q: q})
	if err := r.UpdateStatus(aCtx(), tenantID, "g-1", domainmm.StatusCancelled, ""); err == nil {
		t.Fatalf("expected the status error to propagate")
	}
}

func TestMatchmakingRepo_UpdateStatus_NilTxRunner(t *testing.T) {
	t.Parallel()
	if err := pg.NewMatchmakingQueueRepo(nil).UpdateStatus(aCtx(), tenantID, "g-1", domainmm.StatusCancelled, ""); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

// --------------------------------------------------------- UpdateHeartbeat

func TestMatchmakingRepo_UpdateHeartbeat_HappyPath(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	if err := r.UpdateHeartbeat(aCtx(), tenantID, "g-1"); err != nil {
		t.Fatalf("UpdateHeartbeat: %v", err)
	}
	if !strings.Contains(aLastSQL(q), "SET last_heartbeat = now()") {
		t.Fatalf("heartbeat UPDATE malformed; got %q", aLastSQL(q))
	}
}

func TestMatchmakingRepo_UpdateHeartbeat_ExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "SET last_heartbeat", execErr: aErrExec}
	r := pg.NewMatchmakingQueueRepo(&aTxRunner{q: q})
	if err := r.UpdateHeartbeat(aCtx(), tenantID, "g-1"); err == nil {
		t.Fatalf("expected the heartbeat error to propagate")
	}
}

func TestMatchmakingRepo_UpdateHeartbeat_RLSFailure_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "SET LOCAL", execErr: aErrExec}
	r := pg.NewMatchmakingQueueRepo(&aTxRunner{q: q})
	if err := r.UpdateHeartbeat(aCtx(), tenantID, "g-1"); err == nil {
		t.Fatalf("expected the SET LOCAL failure to propagate")
	}
}

func TestMatchmakingRepo_UpdateHeartbeat_NilTxRunner(t *testing.T) {
	t.Parallel()
	if err := pg.NewMatchmakingQueueRepo(nil).UpdateHeartbeat(aCtx(), tenantID, "g-1"); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

// ------------------------------------------------------------- FindByStatus

func TestMatchmakingRepo_FindByStatus_HappyPath(t *testing.T) {
	t.Parallel()
	s1, s2 := aSearcher(), aSearcher()
	s2.GCID = "01970000-0000-7000-a000-0000000000cc"
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			if !strings.Contains(sql, "WHERE tenant_id = $1 AND status = $2") {
				return nil, errors.New("unexpected SQL: " + sql)
			}
			return &stubRows{scans: []func(dest ...any) error{
				aQueueRowScan(s1), aQueueRowScan(s2),
			}}, nil
		},
	}
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	out, err := r.FindByStatus(aCtx(), tenantID, domainmm.StatusFinding)
	if err != nil {
		t.Fatalf("FindByStatus: %v", err)
	}
	if len(out) != 2 || out[1].GCID != s2.GCID {
		t.Fatalf("out = %+v", out)
	}
}

func TestMatchmakingRepo_FindByStatus_QueryError_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("db down") }
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	if _, err := r.FindByStatus(aCtx(), tenantID, domainmm.StatusFinding); err == nil {
		t.Fatalf("expected the query error to propagate")
	}
}

func TestMatchmakingRepo_FindByStatus_ScanError_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		return &stubRows{scans: []func(dest ...any) error{
			func(dest ...any) error { return errors.New("scan boom") },
		}}, nil
	}
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	if _, err := r.FindByStatus(aCtx(), tenantID, domainmm.StatusFinding); err == nil {
		t.Fatalf("expected the scan error to propagate")
	}
}

func TestMatchmakingRepo_FindByStatus_RowsErr_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		return &stubRows{err: errors.New("cursor error"), scans: []func(dest ...any) error{
			aQueueRowScan(aSearcher()),
		}}, nil
	}
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	if _, err := r.FindByStatus(aCtx(), tenantID, domainmm.StatusFinding); err == nil {
		t.Fatalf("expected the rows.Err() to propagate")
	}
}

func TestMatchmakingRepo_FindByStatus_NilTxRunner(t *testing.T) {
	t.Parallel()
	if _, err := pg.NewMatchmakingQueueRepo(nil).FindByStatus(aCtx(), tenantID, domainmm.StatusFinding); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

// -------------------------------------------------------- ExpireStaleForTenant

func TestMatchmakingRepo_ExpireStaleForTenant_HappyPath(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	if err := r.ExpireStaleForTenant(aCtx(), tenantID); err != nil {
		t.Fatalf("ExpireStaleForTenant: %v", err)
	}
	last := aLastSQL(q)
	if !strings.Contains(last, "SET status = 'abandoned'") || !strings.Contains(last, "INTERVAL '30 seconds'") {
		t.Fatalf("expire UPDATE malformed; got %q", last)
	}
}

func TestMatchmakingRepo_ExpireStaleForTenant_ExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "SET status = 'abandoned'", execErr: aErrExec}
	r := pg.NewMatchmakingQueueRepo(&aTxRunner{q: q})
	if err := r.ExpireStaleForTenant(aCtx(), tenantID); err == nil {
		t.Fatalf("expected the expire error to propagate")
	}
}

func TestMatchmakingRepo_ExpireStaleForTenant_RLSFailure_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "SET LOCAL", execErr: aErrExec}
	r := pg.NewMatchmakingQueueRepo(&aTxRunner{q: q})
	if err := r.ExpireStaleForTenant(aCtx(), tenantID); err == nil {
		t.Fatalf("expected the SET LOCAL failure to propagate")
	}
}

func TestMatchmakingRepo_ExpireStaleForTenant_NilTxRunner(t *testing.T) {
	t.Parallel()
	if err := pg.NewMatchmakingQueueRepo(nil).ExpireStaleForTenant(aCtx(), tenantID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

// ---------------------------------------------------------- FindCandidates

func TestMatchmakingRepo_FindCandidates_HappyPath(t *testing.T) {
	t.Parallel()
	s := aSearcher()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{aQueueRowScan(s)}}, nil
		},
	}
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	out, err := r.FindCandidates(aCtx(), tenantID)
	if err != nil {
		t.Fatalf("FindCandidates: %v", err)
	}
	if len(out) != 1 || out[0].GCID != s.GCID {
		t.Fatalf("out = %+v", out)
	}
	sel := aLastSQL(q)
	if !strings.Contains(sel, "last_heartbeat >= now() - INTERVAL '30 seconds'") ||
		!strings.Contains(sel, "expires_at > now()") {
		t.Fatalf("candidate SELECT must filter fresh + unexpired; got %q", sel)
	}
}

func TestMatchmakingRepo_FindCandidates_QueryError_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("db down") }
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	if _, err := r.FindCandidates(aCtx(), tenantID); err == nil {
		t.Fatalf("expected the query error to propagate")
	}
}

func TestMatchmakingRepo_FindCandidates_ScanError_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		return &stubRows{scans: []func(dest ...any) error{
			func(dest ...any) error { return errors.New("scan boom") },
		}}, nil
	}
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	if _, err := r.FindCandidates(aCtx(), tenantID); err == nil {
		t.Fatalf("expected the scan error to propagate")
	}
}

func TestMatchmakingRepo_FindCandidates_NilTxRunner(t *testing.T) {
	t.Parallel()
	if _, err := pg.NewMatchmakingQueueRepo(nil).FindCandidates(aCtx(), tenantID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

// -------------------------------------------------------------- ClaimMatch

func TestMatchmakingRepo_ClaimMatch_HappyPath(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{} // default Exec → RowsAffected=1... but claim needs >=2
	q.execTagFn = func(sql string) rls.CommandTag { return rls.CommandTag{RowsAffected: 2} }
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	if err := r.ClaimMatch(aCtx(), tenantID, "g-a", "g-b", "duel-1"); err != nil {
		t.Fatalf("ClaimMatch: %v", err)
	}
	if !strings.Contains(aLastSQL(q), "gcid IN ($2, $3)") {
		t.Fatalf("claim UPDATE malformed; got %q", aLastSQL(q))
	}
}

func TestMatchmakingRepo_ClaimMatch_UnderTwoRows_ReturnsErrNoMatchFound(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.execTagFn = func(sql string) rls.CommandTag { return rls.CommandTag{RowsAffected: 1} }
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	if err := r.ClaimMatch(aCtx(), tenantID, "g-a", "g-b", "duel-1"); !errors.Is(err, pg.ErrNoMatchFound) {
		t.Fatalf("expected ErrNoMatchFound; got %v", err)
	}
}

func TestMatchmakingRepo_ClaimMatch_ZeroRows_ReturnsErrNoMatchFound(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.execTagFn = func(sql string) rls.CommandTag { return rls.CommandTag{RowsAffected: 0} }
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	if err := r.ClaimMatch(aCtx(), tenantID, "g-a", "g-b", "duel-1"); !errors.Is(err, pg.ErrNoMatchFound) {
		t.Fatalf("expected ErrNoMatchFound; got %v", err)
	}
}

func TestMatchmakingRepo_ClaimMatch_ExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "SET status = 'matched'", execErr: aErrExec}
	r := pg.NewMatchmakingQueueRepo(&aTxRunner{q: q})
	if err := r.ClaimMatch(aCtx(), tenantID, "g-a", "g-b", "duel-1"); err == nil {
		t.Fatalf("expected the claim error to propagate")
	}
}

func TestMatchmakingRepo_ClaimMatch_NilTxRunner(t *testing.T) {
	t.Parallel()
	if err := pg.NewMatchmakingQueueRepo(nil).ClaimMatch(aCtx(), tenantID, "g-a", "g-b", "duel-1"); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

// ---------------------------------------------------------- RevertToFinding

func TestMatchmakingRepo_RevertToFinding_HappyPath(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.execTagFn = func(sql string) rls.CommandTag { return rls.CommandTag{RowsAffected: 1} }
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	if err := r.RevertToFinding(aCtx(), tenantID, "g-a", "duel-1"); err != nil {
		t.Fatalf("RevertToFinding: %v", err)
	}
	last := aLastSQL(q)
	if !strings.Contains(last, "SET status = 'finding'") || !strings.Contains(last, "matched_duel_id = $3") {
		t.Fatalf("revert UPDATE malformed; got %q", last)
	}
}

func TestMatchmakingRepo_RevertToFinding_ZeroRows_ReturnsErrNoMatchFound(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.execTagFn = func(sql string) rls.CommandTag { return rls.CommandTag{RowsAffected: 0} }
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	if err := r.RevertToFinding(aCtx(), tenantID, "g-a", "duel-1"); !errors.Is(err, pg.ErrNoMatchFound) {
		t.Fatalf("expected ErrNoMatchFound; got %v", err)
	}
}

func TestMatchmakingRepo_RevertToFinding_ExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "SET status = 'finding'", execErr: aErrExec}
	r := pg.NewMatchmakingQueueRepo(&aTxRunner{q: q})
	if err := r.RevertToFinding(aCtx(), tenantID, "g-a", "duel-1"); err == nil {
		t.Fatalf("expected the revert error to propagate")
	}
}

func TestMatchmakingRepo_RevertToFinding_NilTxRunner(t *testing.T) {
	t.Parallel()
	if err := pg.NewMatchmakingQueueRepo(nil).RevertToFinding(aCtx(), tenantID, "g-a", "duel-1"); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

// --------------------------------------------------------------- GetByGCID

func TestMatchmakingRepo_GetByGCID_HappyPath(t *testing.T) {
	t.Parallel()
	s := aSearcher()
	s.Status = "matched"
	s.MatchedDuelID = "duel-7"
	q := &stubQuerier{}
	q.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: aQueueRowScan(s)}
	}
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	got, err := r.GetByGCID(aCtx(), tenantID, s.GCID)
	if err != nil {
		t.Fatalf("GetByGCID: %v", err)
	}
	if got == nil || got.GCID != s.GCID || got.MatchedDuelID != "duel-7" || got.Mode != "classic" {
		t.Fatalf("got = %+v", got)
	}
}

func TestMatchmakingRepo_GetByGCID_NoRows_ReturnsNilNil(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: func(dest ...any) error { return errors.New("no rows in result set") }}
	}
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	got, err := r.GetByGCID(aCtx(), tenantID, "g-1")
	if err != nil || got != nil {
		t.Fatalf("expected (nil, nil) on no rows; got (%v, %v)", got, err)
	}
}

func TestMatchmakingRepo_GetByGCID_RealScanError_Propagates(t *testing.T) {
	t.Parallel()
	boom := errors.New("scan boom")
	q := &stubQuerier{}
	q.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: func(dest ...any) error { return boom }}
	}
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	if _, err := r.GetByGCID(aCtx(), tenantID, "g-1"); err == nil {
		t.Fatalf("expected the scan error to propagate")
	}
}

func TestMatchmakingRepo_GetByGCID_NilTxRunner(t *testing.T) {
	t.Parallel()
	if _, err := pg.NewMatchmakingQueueRepo(nil).GetByGCID(aCtx(), tenantID, "g-1"); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

// ------------------------------------------------------------- CountFinding

func TestMatchmakingRepo_CountFinding_HappyPath(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: func(dest ...any) error {
			if p, ok := dest[0].(*int); ok {
				*p = 4
			}
			return nil
		}}
	}
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	n, err := r.CountFinding(aCtx(), tenantID)
	if err != nil || n != 4 {
		t.Fatalf("CountFinding: n=%d err=%v", n, err)
	}
	if !strings.Contains(aLastSQL(q), "COUNT(*)") {
		t.Fatalf("count SELECT malformed; got %q", aLastSQL(q))
	}
}

func TestMatchmakingRepo_CountFinding_ScanError_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: func(dest ...any) error { return errors.New("no rows in result set") }}
	}
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	if _, err := r.CountFinding(aCtx(), tenantID); err == nil {
		t.Fatalf("expected the scan error to propagate")
	}
}

func TestMatchmakingRepo_CountFinding_NilTxRunner(t *testing.T) {
	t.Parallel()
	if _, err := pg.NewMatchmakingQueueRepo(nil).CountFinding(aCtx(), tenantID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

// ---------------------------------------------------------------- scanQueueRow

func TestMatchmakingRepo_scanQueueRow_DefaultsAndBlitz(t *testing.T) {
	t.Parallel()
	// Row with blank mode (→ classic fallback in scanQueueRow) and nil
	// matched/blitz columns.
	s := aSearcher()
	s.Mode = ""
	q := &stubQuerier{}
	q.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: aQueueRowScan(s)}
	}
	r := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q})
	got, err := r.GetByGCID(aCtx(), tenantID, "g-1")
	if err != nil || got == nil {
		t.Fatalf("GetByGCID: got=%v err=%v", got, err)
	}
	if got.Mode != "classic" {
		t.Fatalf("blank mode must default to classic; got %q", got.Mode)
	}

	// Blitz row.
	s = aSearcher()
	s.Mode = "blitz"
	s.BlitzVariant = "timed"
	q2 := &stubQuerier{}
	q2.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: aQueueRowScan(s)}
	}
	r2 := pg.NewMatchmakingQueueRepo(&stubTxRunner{q: q2})
	got2, err := r2.GetByGCID(aCtx(), tenantID, "g-1")
	if err != nil || got2 == nil {
		t.Fatalf("GetByGCID(blitz): got=%v err=%v", got2, err)
	}
	if got2.Mode != "blitz" || got2.BlitzVariant != "timed" {
		t.Fatalf("blitz fields not scanned: %+v", got2)
	}
}