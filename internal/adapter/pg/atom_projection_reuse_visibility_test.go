// atom_projection_reuse_visibility_test.go — RED specs for the ADR-229 WS-1
// (CHO-2127) pg surface of the reuse-consent flag on atom_projections
// (column landed in mig 0031; CHECK private|friends|tenant, NOT NULL
// DEFAULT 'private').
//
// Contract under test:
//   - SQLUpsertAtomProjection hardens an EMPTY incoming label to 'private'
//     on INSERT but PRESERVES the existing column value on conflict-update
//     (a re-publish from a pre-ADR-229 producer must not reset an audience
//     set via reuse_visibility_changed.v1).
//   - SQLSetAtomProjectionReuseVisibility applies a changed-event audience
//     RLS-scoped; 0 rows (atom never published -> no projection row) is NOT
//     an error — the eventual publish event carries the current flag.
//   - Get scans the column back onto Projection.ReuseVisibility.
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

func TestSQLUpsertAtomProjection_HardensAndPreservesReuseVisibility(t *testing.T) {
	t.Parallel()
	sql := pg.SQLUpsertAtomProjection
	if !strings.Contains(sql, "reuse_visibility") {
		t.Fatalf("SQLUpsertAtomProjection must carry reuse_visibility; got %q", sql)
	}
	// INSERT hardens '' -> 'private' (consent-first default).
	if !strings.Contains(sql, "COALESCE(NULLIF($9, ''), 'private')") {
		t.Errorf("SQLUpsertAtomProjection must harden empty reuse_visibility to 'private' on INSERT; got %q", sql)
	}
	// UPDATE preserves the existing value when the event carries none.
	if !strings.Contains(sql, "CASE WHEN $9 <> '' THEN $9 ELSE atom_projections.reuse_visibility END") {
		t.Errorf("SQLUpsertAtomProjection must preserve the column on empty-label conflict-update; got %q", sql)
	}
}

func TestAtomProjectionStore_Upsert_BindsReuseVisibilityArg(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	s := pg.NewAtomProjectionStore(&stubTxRunner{q: q})

	p := validProjection()
	p.ReuseVisibility = "tenant"
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	if err := s.Upsert(ctx, p); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	lastArgs := q.args[len(q.args)-1]
	if len(lastArgs) != 11 {
		t.Fatalf("Upsert binds %d args; want 11 (reuse_visibility is $9, options is $10, correct_answer is $11)", len(lastArgs))
	}
	if got, ok := lastArgs[8].(string); !ok || got != "tenant" {
		t.Fatalf("arg[8] (reuse_visibility) = %v; want tenant", lastArgs[8])
	}
}

func TestAtomProjectionStore_SetReuseVisibility_AppliesRLSThenUpdates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	s := pg.NewAtomProjectionStore(&stubTxRunner{q: q})

	ctx := tracing.WithTenantID(context.Background(), tenantID)
	if err := s.SetReuseVisibility(ctx, projAtomID, "friends"); err != nil {
		t.Fatalf("SetReuseVisibility: %v", err)
	}
	if len(q.sqls) < 2 {
		t.Fatalf("expected SET LOCAL + UPDATE; got %d SQLs", len(q.sqls))
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL chora.tenant_id") {
		t.Fatalf("first SQL must be SET LOCAL chora.tenant_id; got %q", q.sqls[0])
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "UPDATE atom_projections") || !strings.Contains(last, "reuse_visibility") {
		t.Fatalf("expected UPDATE atom_projections ... reuse_visibility; got %q", last)
	}
	if !strings.Contains(last, "deleted_at IS NULL") {
		t.Fatalf("soft-delete filter missing; got %q", last)
	}
	lastArgs := q.args[len(q.args)-1]
	if got, _ := lastArgs[0].(string); got != projAtomID {
		t.Fatalf("arg[0] = %v; want atom_id %q", lastArgs[0], projAtomID)
	}
	if got, _ := lastArgs[1].(string); got != "friends" {
		t.Fatalf("arg[1] = %v; want friends", lastArgs[1])
	}
}

func TestAtomProjectionStore_SetReuseVisibility_ZeroRowsIsNotAnError(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		execTagFn: func(string) rls.CommandTag { return rls.CommandTag{RowsAffected: 0} },
	}
	s := pg.NewAtomProjectionStore(&stubTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	if err := s.SetReuseVisibility(ctx, projAtomID, "private"); err != nil {
		t.Fatalf("0-row SetReuseVisibility must succeed (atom never published); got %v", err)
	}
}

func TestAtomProjectionStore_SetReuseVisibility_RejectsUnknownLabel(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	s := pg.NewAtomProjectionStore(&stubTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	err := s.SetReuseVisibility(ctx, projAtomID, "everyone")
	if !errors.Is(err, atom_projection.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument for unknown label; got %v", err)
	}
	if len(q.sqls) != 0 {
		t.Fatalf("no SQL may fire on an invalid label; got %v", q.sqls)
	}
}

func TestAtomProjectionStore_SetReuseVisibility_RejectsMissingTenantContext(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	s := pg.NewAtomProjectionStore(&stubTxRunner{q: q})
	err := s.SetReuseVisibility(context.Background(), projAtomID, "private")
	if !errors.Is(err, rls.ErrNoTenantContext) {
		t.Fatalf("expected ErrNoTenantContext; got %v", err)
	}
}

func TestAtomProjectionStore_Get_ScansReuseVisibility(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error {
			// 10 columns: atom_id, revision_id, owner_gcid,
			// author_display_name, stem, question_type, published_at,
			// reuse_visibility, options, correct_answer.
			if len(dest) != 10 {
				t.Fatalf("Get scans %d columns; want 10 (incl. reuse_visibility, options, correct_answer)", len(dest))
			}
			*(dest[0].(*string)) = projAtomID
			*(dest[1].(*string)) = projRevID
			*(dest[2].(*string)) = authorGCID
			*(dest[3].(*string)) = "Ada Lovelace"
			*(dest[4].(*string)) = "What is 2+2?"
			*(dest[5].(*string)) = "mcq"
			// dest[6] published_at (*time.Time) left nil
			*(dest[7].(*string)) = "tenant"
			// dest[8] options (*[]string) — set test MCQ options
			*dest[8].(*[]string) = []string{"2", "3", "4", "5"}
			// dest[9] correct_answer (*string)
			*(dest[9].(*string)) = "4"
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
	if got.ReuseVisibility != "tenant" {
		t.Fatalf("Get projection reuse_visibility = %q; want tenant", got.ReuseVisibility)
	}
}

// -----------------------------------------------------------------------------
// ADR-229 WS-2 (CHO-2133) — ProjectionRepo.Get must scan reuse_visibility so
// AuthorizeAtomUse can resolve the audience-based D2 audit grant (a non-owner
// tenant-visible snapshot mints a free-license grant with NO source share).
// -----------------------------------------------------------------------------

func TestProjectionRepo_Get_ScansReuseVisibility(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{rowFn: func(_ string, _ ...any) pg.Row {
		return stubRow{scanFn: func(dest ...any) error {
			if len(dest) != 12 {
				return errors.New("want 12 scan targets (…, reuse_visibility, options, correct_answer)")
			}
			*(dest[0].(*string)) = "atom-1"
			*(dest[1].(*string)) = tenantID
			*(dest[2].(*string)) = "rev-1"
			*(dest[3].(*string)) = "owner-1"
			*(dest[4].(*string)) = "Author"
			*(dest[5].(*string)) = "stem"
			*(dest[6].(*string)) = "mcq"
			// dest[7] published_at (**time.Time) — leave nil.
			*(dest[8].(*bool)) = false
			*(dest[9].(*string)) = "tenant"
			// dest[10] options (*[]string) + dest[11] correct_answer (*string)
			*(dest[10].(*[]string)) = []string{"A", "B"}
			*(dest[11].(*string)) = "A"
			return nil
		}}
	}}
	r := pg.NewProjectionRepo(&stubTxRunner{q: q})

	ctx := tracing.WithTenantID(context.Background(), tenantID)
	p, err := r.Get(ctx, "atom-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if p.ReuseVisibility != "tenant" {
		t.Errorf("ReuseVisibility = %q; want tenant", p.ReuseVisibility)
	}
	// The SELECT must name the column (not rely on positional drift).
	joined := strings.Join(q.sqls, "\n")
	if !strings.Contains(joined, "reuse_visibility") {
		t.Errorf("sqlProjectionGet must select reuse_visibility; got %q", joined)
	}
}
