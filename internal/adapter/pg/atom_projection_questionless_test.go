// atom_projection_questionless_test.go — CHO-2174b.
//
// atom_projections.revision_id was `uuid NOT NULL` (no default), so a
// question-less atom could not be persisted at all: binding "" to a uuid column
// is a live 22P02 (invalid input syntax for type uuid). Migration 0036 makes
// revision_id / stem / question_type NULLABLE, and this suite pins the two
// halves of the pg contract that keeps that safe:
//
//	WRITE — an ABSENT revision binds SQL NULL (never ""), and an absent question
//	        field never CLOBBERS a previously-known one on conflict-update
//	        (question data is enrichment: an event that carries it fills it, an
//	        event that does not leaves what is there — mirroring the existing
//	        reuse_visibility PRESERVE idiom).
//
//	READ  — every SELECT COALESCEs the now-nullable columns, so a NULL can never
//	        blow up a scan into a plain string (that would surface as a phantom
//	        500 on the consent gate rather than a consent decision).
package pg_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-common/tracing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
)

// questionLessProjection carries ONLY the consent facts.
func questionLessProjection() atom_projection.Projection {
	return atom_projection.Projection{
		AtomID:          projAtomID,
		OwnerGCID:       authorGCID,
		ReuseVisibility: "private",
		// RevisionID / Stem / QuestionType absent.
	}
}

// An absent revision must bind SQL NULL — NOT "". revision_id is a uuid column;
// "" is not a valid uuid and would fail 22P02 at runtime (exactly the class of
// defect a green in-memory suite cannot catch).
func TestAtomProjectionStore_Upsert_QuestionLess_BindsNullRevision(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	s := pg.NewAtomProjectionStore(&stubTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)

	if err := s.Upsert(ctx, questionLessProjection()); err != nil {
		t.Fatalf("Upsert() on a question-less projection = %v; want nil (consent facts stand alone)", err)
	}

	args, ok := argsForSQL(q, pg.SQLUpsertAtomProjection)
	if !ok {
		t.Fatalf("SQLUpsertAtomProjection was never executed; SQLs = %v", q.sqls)
	}
	if got := args[2]; got != nil { // $3 = revision_id
		t.Fatalf("revision_id bound as %#v; want nil (SQL NULL). Binding \"\" to a uuid column is a "+
			"live 22P02 — the projection must carry NULL when the atom has no question revision", got)
	}
}

// A revision-bearing projection still binds the revision verbatim.
func TestAtomProjectionStore_Upsert_QuestionBearing_BindsRevision(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	s := pg.NewAtomProjectionStore(&stubTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)

	if err := s.Upsert(ctx, validProjection()); err != nil {
		t.Fatalf("Upsert() = %v; want nil", err)
	}
	args, ok := argsForSQL(q, pg.SQLUpsertAtomProjection)
	if !ok {
		t.Fatalf("SQLUpsertAtomProjection was never executed")
	}
	if got := args[2]; got != projRevID {
		t.Fatalf("revision_id bound as %#v; want %q", got, projRevID)
	}
}

// The conflict-UPDATE must PRESERVE a known revision when the incoming event
// carries none — otherwise a re-emit from a producer that cannot resolve the
// question revision would silently ERASE the pinned revision of an atom that
// has one, breaking every grant snapshot that depends on it.
func TestSQLUpsertAtomProjection_PreservesQuestionFieldsOnAbsentIncoming(t *testing.T) {
	t.Parallel()
	sql := pg.SQLUpsertAtomProjection
	for _, col := range []string{"revision_id", "stem", "question_type"} {
		if !strings.Contains(sql, "atom_projections."+col) {
			t.Fatalf("SQLUpsertAtomProjection does not PRESERVE %s on conflict-update — an event "+
				"without %s would clobber a known value.\nSQL:\n%s", col, col, sql)
		}
	}
}

// Every read of the now-nullable columns must COALESCE. A NULL scanned into a
// plain Go string is a hard driver error, which the gate would surface as a 500
// instead of a consent decision.
func TestProjectionReads_CoalesceNullableRevision(t *testing.T) {
	t.Parallel()
	reads := map[string]string{
		"SQLSelectAtomProjectionByID":          pg.SQLSelectAtomProjectionByID,
		"SQLAtomReuseSelectProjectionAnyState": pg.SQLAtomReuseSelectProjectionAnyState,
	}
	for name, sql := range reads {
		if !strings.Contains(sql, "COALESCE(revision_id") {
			t.Fatalf("%s does not COALESCE revision_id — a NULL revision would fail the scan into a "+
				"string and surface as a 500 on the consent gate.\nSQL:\n%s", name, sql)
		}
	}
}

// argsForSQL returns the bound args of the first recorded execution of sql.
func argsForSQL(q *stubQuerier, sql string) ([]any, bool) {
	for i, s := range q.sqls {
		if s == sql {
			return q.args[i], true
		}
	}
	return nil, false
}
