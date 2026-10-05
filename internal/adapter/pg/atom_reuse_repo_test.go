// atom_reuse_repo_test.go — ADR-229 Amendment A1 (CHO-2132) pg slice,
// RED-first. SQL-shape + RLS-ordering + arg coverage via the stub harness
// (live behaviour lands via the PREPARE-smoke lane).
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_reuse"
	sharingv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/sharing/v1"
)

const (
	reuseAtom    = "01970000-0000-7000-8000-0000000000a1"
	reuseOrphan  = "01970000-0000-7000-8000-0000000000e1"
	reuseRev     = "01970000-0000-7000-8000-0000000000f1"
	reuseAuthor  = "01970000-0000-7000-8000-0000000000aa"
	reuseGrantee = "01970000-0000-7000-8000-0000000000b0"
)

func reuseCtx() context.Context {
	return tracing.WithTenantID(context.Background(), tenantID)
}

// errNoStubRow simulates the pgx no-rows scan error.
var errNoStubRow = errors.New("no rows in result set")

// -----------------------------------------------------------------------------
// ActiveGrantRefsForAtom
// -----------------------------------------------------------------------------

func TestAtomReuseRepo_ActiveGrantRefsForAtom_SQLShape(t *testing.T) {
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		return &stubRows{scans: []func(dest ...any) error{
			func(dest ...any) error {
				*(dest[0].(*string)) = "g-1"
				*(dest[1].(*string)) = reuseGrantee
				*(dest[2].(*string)) = reuseAuthor
				*(dest[3].(*string)) = "test_set"
				return nil
			},
		}}, nil
	}
	repo := pg.NewAtomReuseRepo(&stubTxRunner{q: q}, pg.AtomReuseRepoOptions{})

	refs, err := repo.ActiveGrantRefsForAtom(reuseCtx(), reuseAtom)
	if err != nil {
		t.Fatalf("ActiveGrantRefsForAtom: %v", err)
	}
	if len(refs) != 1 || refs[0].GrantID != "g-1" || refs[0].GranteeGCID != reuseGrantee || refs[0].OwnerGCID != reuseAuthor || refs[0].Scope != "test_set" {
		t.Fatalf("refs = %+v", refs)
	}
	// RLS session applied BEFORE the query.
	if len(q.sqls) < 2 || !strings.Contains(q.sqls[0], "chora.tenant_id") {
		t.Fatalf("rls.ApplySession must run first; sqls = %v", q.sqls)
	}
	sel := q.sqls[len(q.sqls)-1]
	for _, want := range []string{"atom_usage_grants", "status = 'active'", "deleted_at IS NULL", "expires_at"} {
		if !strings.Contains(sel, want) {
			t.Errorf("grant-refs SELECT missing %q; got %q", want, sel)
		}
	}
}

// -----------------------------------------------------------------------------
// Edition store
// -----------------------------------------------------------------------------

func TestAtomReuseRepo_LatestEdition_FoundAndMissing(t *testing.T) {
	q := &stubQuerier{}
	orphanedAt := time.Date(2026, 7, 11, 9, 0, 0, 0, time.UTC)
	q.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: func(dest ...any) error {
			*(dest[0].(*string)) = reuseAtom
			*(dest[1].(*string)) = reuseRev
			*(dest[2].(*string)) = reuseOrphan
			*(dest[3].(*time.Time)) = orphanedAt
			return nil
		}}
	}
	repo := pg.NewAtomReuseRepo(&stubTxRunner{q: q}, pg.AtomReuseRepoOptions{})

	ed, ok, err := repo.LatestEdition(reuseCtx(), reuseAtom)
	if err != nil || !ok {
		t.Fatalf("LatestEdition: ok=%v err=%v", ok, err)
	}
	if ed.AtomID != reuseAtom || ed.SourceRevisionID != reuseRev || ed.OrphanAtomID != reuseOrphan || !ed.OrphanedAt.Equal(orphanedAt) {
		t.Fatalf("edition = %+v", ed)
	}
	sel := q.sqls[len(q.sqls)-1]
	for _, want := range []string{"atom_orphan_editions", "ORDER BY orphaned_at DESC"} {
		if !strings.Contains(sel, want) {
			t.Errorf("latest-edition SELECT missing %q; got %q", want, sel)
		}
	}

	// Missing → ok=false, no error (scan error = not-found per the package
	// idiom; the detector then round-trips creation — the safe fallback).
	q2 := &stubQuerier{}
	q2.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: func(...any) error { return errNoStubRow }}
	}
	repo2 := pg.NewAtomReuseRepo(&stubTxRunner{q: q2}, pg.AtomReuseRepoOptions{})
	_, ok, err = repo2.LatestEdition(reuseCtx(), reuseAtom)
	if err != nil || ok {
		t.Fatalf("missing edition: ok=%v err=%v (want false, nil)", ok, err)
	}
}

func TestAtomReuseRepo_PutEdition_IdempotentInsertWithTenant(t *testing.T) {
	q := &stubQuerier{}
	repo := pg.NewAtomReuseRepo(&stubTxRunner{q: q}, pg.AtomReuseRepoOptions{})

	ed := atom_reuse.OrphanEdition{
		AtomID:           reuseAtom,
		SourceRevisionID: reuseRev,
		OrphanAtomID:     reuseOrphan,
		OrphanedAt:       time.Now().UTC(),
	}
	if err := repo.PutEdition(reuseCtx(), ed); err != nil {
		t.Fatalf("PutEdition: %v", err)
	}
	var ins string
	var insArgs []any
	for i, sql := range q.sqls {
		if strings.Contains(sql, "INSERT INTO atom_orphan_editions") {
			ins = sql
			insArgs = q.args[i]
		}
	}
	for _, want := range []string{"INSERT INTO atom_orphan_editions", "ON CONFLICT (atom_id, source_revision_id)", "DO NOTHING"} {
		if !strings.Contains(ins, want) {
			t.Errorf("PutEdition INSERT missing %q; got %q", want, ins)
		}
	}
	// tenant_id bound from ctx as the first column arg.
	if len(insArgs) < 1 || insArgs[0] != tenantID {
		t.Errorf("PutEdition args = %v (want ctx tenant first)", insArgs)
	}

	// Invalid edition refuses before SQL.
	if err := repo.PutEdition(reuseCtx(), atom_reuse.OrphanEdition{}); err == nil {
		t.Fatalf("invalid edition must refuse")
	}
}

// -----------------------------------------------------------------------------
// RepointStranded
// -----------------------------------------------------------------------------

func TestAtomReuseRepo_RepointStranded_RepointsMergesSkips(t *testing.T) {
	q := &stubQuerier{}
	repoints, merges := 0, 0
	q.execTagFn = func(sql string) rls.CommandTag {
		switch {
		case strings.Contains(sql, "SET atom_id"):
			repoints++
			if repoints == 1 {
				return rls.CommandTag{RowsAffected: 1} // g1 repointed
			}
			return rls.CommandTag{RowsAffected: 0} // g2, g3 blocked
		case strings.Contains(sql, "SET deleted_at"):
			merges++
			if merges == 1 {
				return rls.CommandTag{RowsAffected: 1} // g2 merged (dupe coverage)
			}
			return rls.CommandTag{RowsAffected: 0} // g3 already handled
		default:
			return rls.CommandTag{RowsAffected: 1}
		}
	}
	repo := pg.NewAtomReuseRepo(&stubTxRunner{q: q}, pg.AtomReuseRepoOptions{})

	stranded := []atom_reuse.GrantRef{
		{GrantID: "g-1", GranteeGCID: reuseGrantee, OwnerGCID: reuseAuthor, Scope: "test_set"},
		{GrantID: "g-2", GranteeGCID: "01970000-0000-7000-8000-0000000000c0", OwnerGCID: reuseAuthor, Scope: "duel"},
		{GrantID: "g-3", GranteeGCID: "01970000-0000-7000-8000-0000000000d0", OwnerGCID: reuseAuthor, Scope: "collection"},
	}
	res, err := repo.RepointStranded(reuseCtx(), atom_reuse.RepointCommand{
		OriginalAtomID: reuseAtom,
		OrphanAtomID:   reuseOrphan,
		Stranded:       stranded,
		Trigger:        atom_reuse.TriggerNarrowed,
		Note:           "event=01970000-e001",
	})
	if err != nil {
		t.Fatalf("RepointStranded: %v", err)
	}
	if res.Repointed != 1 || res.Merged != 1 || res.Skipped != 1 {
		t.Fatalf("result = %+v, want {1,1,1}", res)
	}

	joined := strings.Join(q.sqls, "\n---\n")
	// Repoint UPDATE: PK-safe NOT EXISTS guard + active-on-original predicate.
	for _, want := range []string{"UPDATE atom_usage_grants", "SET atom_id", "NOT EXISTS", "status = 'active'"} {
		if !strings.Contains(joined, want) {
			t.Errorf("repoint SQL missing %q; sqls = %s", want, joined)
		}
	}
	// Merge = soft-delete, never hard-delete, never a revoke.
	if !strings.Contains(joined, "SET deleted_at = now()") {
		t.Errorf("merge must soft-delete; sqls = %s", joined)
	}
	if strings.Contains(strings.ToUpper(joined), "DELETE FROM ATOM_USAGE_GRANTS") {
		t.Errorf("hard delete forbidden; sqls = %s", joined)
	}
	if strings.Contains(joined, "'revoked'") {
		t.Errorf("repoint must NEVER revoke (A1.1); sqls = %s", joined)
	}
	// Append-only trail: exactly 2 grant_events rows (g1 repoint + g2 merge),
	// event_type 'repointed'.
	trailInserts := 0
	for i, sql := range q.sqls {
		if strings.Contains(sql, "INSERT INTO grant_events") {
			trailInserts++
			args := q.args[i]
			found := false
			for _, a := range args {
				if s, ok := a.(string); ok && s == "repointed" {
					found = true
				}
			}
			if !found {
				t.Errorf("grant_events insert missing event_type 'repointed'; args = %v", args)
			}
		}
	}
	if trailInserts != 2 {
		t.Errorf("grant_events trail inserts = %d, want 2 (repoint + merge; skipped writes none)", trailInserts)
	}
	// RLS applied before the sweep.
	if !strings.Contains(q.sqls[0], "chora.tenant_id") {
		t.Errorf("rls.ApplySession must run first; sqls[0] = %q", q.sqls[0])
	}
}

func TestAtomReuseRepo_RepointStranded_Validation(t *testing.T) {
	repo := pg.NewAtomReuseRepo(&stubTxRunner{q: &stubQuerier{}}, pg.AtomReuseRepoOptions{})
	if _, err := repo.RepointStranded(reuseCtx(), atom_reuse.RepointCommand{
		OrphanAtomID: reuseOrphan, Stranded: []atom_reuse.GrantRef{{GrantID: "g"}},
	}); err == nil {
		t.Fatalf("missing original atom id must refuse")
	}
	if _, err := repo.RepointStranded(reuseCtx(), atom_reuse.RepointCommand{
		OriginalAtomID: reuseAtom, Stranded: []atom_reuse.GrantRef{{GrantID: "g"}},
	}); err == nil {
		t.Fatalf("missing orphan atom id must refuse")
	}
	// Zero stranded = no-op success (nothing to sweep).
	res, err := repo.RepointStranded(reuseCtx(), atom_reuse.RepointCommand{
		OriginalAtomID: reuseAtom, OrphanAtomID: reuseOrphan,
	})
	if err != nil || res.Repointed != 0 {
		t.Fatalf("zero stranded: res=%+v err=%v", res, err)
	}
}

// -----------------------------------------------------------------------------
// RequireOrphan — outbox emission (same-tx idiom)
// -----------------------------------------------------------------------------

func TestAtomReuseRepo_RequireOrphan_WritesBinaryOutboxRow(t *testing.T) {
	q := &stubQuerier{}
	repo := pg.NewAtomReuseRepo(&stubTxRunner{q: q}, pg.AtomReuseRepoOptions{
		SourceProject: "chora-489812",
		SourceService: "chora-sharing",
	})

	detected := time.Date(2026, 7, 11, 10, 30, 0, 0, time.UTC)
	err := repo.RequireOrphan(reuseCtx(), atom_reuse.OrphanRequest{
		AtomID:        reuseAtom,
		RevisionID:    reuseRev,
		Trigger:       atom_reuse.TriggerNarrowed,
		StrandedCount: 2,
		DetectedAt:    detected,
		ActorGCID:     reuseAuthor,
	})
	if err != nil {
		t.Fatalf("RequireOrphan: %v", err)
	}

	// Find the outbox insert + decode its binary payload.
	var outboxArgs []any
	for i, sql := range q.sqls {
		if strings.Contains(sql, "INSERT INTO sharing_outbox_events") {
			outboxArgs = q.args[i]
		}
	}
	if outboxArgs == nil {
		t.Fatalf("no outbox insert; sqls = %v", q.sqls)
	}
	// Column order per SQLInsertOutboxEvent: id, tenant_id, gcid,
	// aggregate_type, aggregate_id, event_type, topic, payload, envelope,
	// idempotency_key, occurred_at.
	if got := outboxArgs[6]; got != "chora.sharing.atom_reuse.orphan_required.v1" {
		t.Fatalf("topic = %v", got)
	}
	if got := outboxArgs[4]; got != reuseAtom {
		t.Errorf("aggregate_id = %v, want the atom", got)
	}
	payload, ok := outboxArgs[7].([]byte)
	if !ok {
		t.Fatalf("payload arg is %T, want []byte (binary protobuf)", outboxArgs[7])
	}
	var m sharingv1.AtomReuseOrphanRequired
	if err := proto.Unmarshal(payload, &m); err != nil {
		t.Fatalf("payload not binary AtomReuseOrphanRequired: %v", err)
	}
	if m.GetAtomId() != reuseAtom || m.GetRevisionId() != reuseRev || m.GetTrigger() != "narrowed" || m.GetStrandedGrantCount() != 2 {
		t.Fatalf("payload fields = %+v", &m)
	}
	if m.GetEnvelope().GetTenantId() != tenantID {
		t.Fatalf("envelope tenant = %q", m.GetEnvelope().GetTenantId())
	}
	if m.GetEnvelope().GetGcid() != reuseAuthor {
		t.Fatalf("envelope gcid = %q, want the withdrawing author", m.GetEnvelope().GetGcid())
	}

	// Invalid request refuses BEFORE any SQL.
	q2 := &stubQuerier{}
	repo2 := pg.NewAtomReuseRepo(&stubTxRunner{q: q2}, pg.AtomReuseRepoOptions{})
	if err := repo2.RequireOrphan(reuseCtx(), atom_reuse.OrphanRequest{AtomID: reuseAtom, Trigger: "narrowed", StrandedCount: 0}); err == nil {
		t.Fatalf("zero-stranded request must refuse (emit nothing)")
	}
	if len(q2.sqls) != 0 {
		t.Fatalf("refused request must not touch the DB; sqls = %v", q2.sqls)
	}
}

// -----------------------------------------------------------------------------
// ProjectionReads — GetAnyState (archived rows INCLUDED)
// -----------------------------------------------------------------------------

func TestAtomReuseRepo_GetAnyState_IncludesArchived(t *testing.T) {
	q := &stubQuerier{}
	q.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: func(dest ...any) error {
			*(dest[0].(*string)) = reuseRev
			*(dest[1].(*string)) = reuseAuthor
			*(dest[2].(*string)) = "private"
			*(dest[3].(*bool)) = true
			return nil
		}}
	}
	repo := pg.NewAtomReuseRepo(&stubTxRunner{q: q}, pg.AtomReuseRepoOptions{})

	st, err := repo.GetAnyState(reuseCtx(), reuseAtom)
	if err != nil {
		t.Fatalf("GetAnyState: %v", err)
	}
	if st.RevisionID != reuseRev || st.OwnerGCID != reuseAuthor || st.ReuseVisibility != "private" || !st.Archived {
		t.Fatalf("state = %+v", st)
	}
	sel := q.sqls[len(q.sqls)-1]
	if strings.Contains(sel, "archived = false") {
		t.Errorf("GetAnyState must include archived projections; got %q", sel)
	}
	if !strings.Contains(sel, "deleted_at IS NULL") {
		t.Errorf("GetAnyState still filters closure-tombstoned rows; got %q", sel)
	}

	// Missing → ErrProjectionNotFound.
	q2 := &stubQuerier{}
	q2.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: func(...any) error { return errNoStubRow }}
	}
	repo2 := pg.NewAtomReuseRepo(&stubTxRunner{q: q2}, pg.AtomReuseRepoOptions{})
	if _, err := repo2.GetAnyState(reuseCtx(), reuseAtom); err != atom_reuse.ErrProjectionNotFound {
		t.Fatalf("missing projection err = %v, want ErrProjectionNotFound", err)
	}
}
