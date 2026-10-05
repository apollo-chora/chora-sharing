// projection_write_paths_questionless_test.go — CHO-2174b.
//
// The two atom_projections code paths that migration 0036 touches but that had
// ZERO test coverage before this change:
//
//   - ProjectionRepo.Upsert  (the second AtomProjectionWriter impl)
//   - GrantRepo.ListEntitled (the `own` leg of the ADR-229 WS-2 picker)
//
// Both bind or scan the now-nullable revision_id. An uncovered NULL/'' handling
// bug here is exactly the class that a green in-memory suite hides until it
// fails in prod, so they get explicit stub-level tests: the recorded SQL must
// COALESCE on read and bind SQL NULL on write.
package pg_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-common/tracing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
	"github.com/apollo-chora/chora-sharing/internal/domain/grant"
)

// ProjectionRepo.Upsert must bind SQL NULL (not "") for an absent revision —
// revision_id is a uuid column and "" is a live 22P02.
func TestProjectionRepo_Upsert_QuestionLess_BindsNullRevision(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewProjectionRepo(&stubTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)

	err := r.Upsert(ctx, atom_projection.Projection{
		AtomID:    projAtomID,
		TenantID:  tenantID,
		OwnerGCID: authorGCID,
		// RevisionID absent — the CHO-2174b consent-only class.
	})
	if err != nil {
		t.Fatalf("Upsert() on a question-less projection = %v; want nil", err)
	}

	var upsertSQL string
	var upsertArgs []any
	for i, s := range q.sqls {
		if strings.Contains(s, "INSERT INTO atom_projections") {
			upsertSQL, upsertArgs = s, q.args[i]
			break
		}
	}
	if upsertSQL == "" {
		t.Fatalf("no INSERT INTO atom_projections was executed; SQLs = %v", q.sqls)
	}
	if got := upsertArgs[2]; got != nil { // $3 = revision_id
		t.Fatalf("revision_id bound as %#v; want nil (SQL NULL)", got)
	}
	// The conflict-UPDATE must PRESERVE a known revision when the incoming event
	// carries none, or a re-emit would erase the pin of an atom that has one.
	if !strings.Contains(upsertSQL, "atom_projections.revision_id") {
		t.Fatalf("ProjectionRepo upsert does not PRESERVE revision_id on conflict-update — a "+
			"revision-less re-emit would clobber a known pin.\nSQL:\n%s", upsertSQL)
	}
}

// ProjectionRepo.Get must COALESCE the nullable question columns: a NULL scanned
// into a plain string is a hard driver error, which the consent gate would
// surface as a phantom 500 rather than a consent decision.
func TestProjectionRepo_Get_CoalescesNullableQuestionColumns(t *testing.T) {
	t.Parallel()
	var seenSQL string
	q := &stubQuerier{rowFn: func(sql string, _ ...any) pg.Row {
		seenSQL = sql
		return stubRow{scanFn: func(dest ...any) error {
			// Post-COALESCE the driver hands back '' for an absent revision.
			*(dest[0].(*string)) = projAtomID
			*(dest[1].(*string)) = tenantID
			*(dest[2].(*string)) = "" // revision_id COALESCEd to ''
			*(dest[3].(*string)) = authorGCID
			*(dest[4].(*string)) = ""
			*(dest[5].(*string)) = "" // stem COALESCEd to ''
			*(dest[6].(*string)) = "" // question_type COALESCEd to ''
			*(dest[8].(*bool)) = false
			*(dest[9].(*string)) = "private"
			return nil
		}}
	}}
	r := pg.NewProjectionRepo(&stubTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)

	p, err := r.Get(ctx, projAtomID)
	if err != nil {
		t.Fatalf("Get() on a question-less projection = %v; want nil", err)
	}
	if p.HasQuestion() {
		t.Fatalf("HasQuestion() = true on a revision-less projection")
	}
	if p.OwnerGCID != authorGCID || p.ReuseVisibility != "private" {
		t.Fatalf("consent facts lost: owner=%q visibility=%q", p.OwnerGCID, p.ReuseVisibility)
	}
	for _, col := range []string{"COALESCE(revision_id", "COALESCE(stem", "COALESCE(question_type"} {
		if !strings.Contains(seenSQL, col) {
			t.Fatalf("projection Get SQL is missing %s) — a NULL would fail the scan.\nSQL:\n%s", col, seenSQL)
		}
	}
}

// GrantRepo.ListEntitled's `own` leg reads atom_projections. A consent-only
// projection (no question revision) must still appear in its OWNER's picker —
// and the SELECT must COALESCE so a NULL never fails the whole listing.
func TestGrantRepo_ListEntitled_OwnLeg_ListsQuestionLessProjection(t *testing.T) {
	t.Parallel()
	var ownSQL string
	q := &stubQuerier{queryFn: func(sql string, _ ...any) (pg.Rows, error) {
		if strings.Contains(sql, "FROM atom_projections") {
			ownSQL = sql
			return &stubRows{scans: []func(dest ...any) error{
				func(dest ...any) error {
					// Post-COALESCE row: revision/stem/question_type all ''.
					*(dest[0].(*string)) = projAtomID
					*(dest[1].(*string)) = "" // revision_id
					*(dest[2].(*string)) = authorGCID
					*(dest[3].(*string)) = ""
					*(dest[4].(*string)) = "" // stem
					*(dest[5].(*string)) = "" // question_type
					return nil
				},
			}}, nil
		}
		return &stubRows{}, nil
	}}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)

	out, err := r.ListEntitled(ctx, authorGCID, grant.ScopeDuel, nil, 100)
	if err != nil {
		t.Fatalf("ListEntitled() = %v; want nil — a question-less projection must not break the "+
			"owner's picker listing", err)
	}
	if !strings.Contains(ownSQL, "COALESCE(revision_id") {
		t.Fatalf("sqlListEntitledOwn does not COALESCE revision_id — a NULL would fail the scan and "+
			"break the whole picker.\nSQL:\n%s", ownSQL)
	}
	var found bool
	for _, a := range out {
		if a.AtomID == projAtomID {
			found = true
			if !a.IsOwn {
				t.Fatalf("own atom not flagged IsOwn")
			}
		}
	}
	if !found {
		t.Fatalf("the owner's question-less atom is MISSING from ListEntitled — the `own` leg of the "+
			"ADR-229 disjunct must resolve for every projected atom (CHO-2174b)")
	}
}
