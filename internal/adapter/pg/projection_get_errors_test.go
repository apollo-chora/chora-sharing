// projection_get_errors_test.go — live-walk regression (2026-07-11 deploy
// catch, CHO-2133): BOTH projection Get implementations swallowed EVERY scan
// error as ErrNotFound. Subscriber-hydrated rows carry published_at = NULL
// (the atom-projection subscriber never sets it), the legacy ProjectionRepo
// scanned it into a plain time.Time, the scan error became a silent
// "NotFound", and AuthorizeAtomUse 412'd an atom that was demonstrably
// published + tenant-visible. Contract pinned here:
//
//   - a real driver/scan error PROPAGATES (fail-loud) — never ErrNotFound;
//   - only the no-rows case maps to atom_projection.ErrNotFound;
//   - a NULL published_at scans cleanly to the zero time.
package pg_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/tracing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
)

func TestProjectionRepo_Get_RealErrorPropagates(t *testing.T) {
	t.Parallel()
	boom := errors.New("connection reset by peer")
	q := &stubQuerier{
		rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error { return boom }}
		},
	}
	r := pg.NewProjectionRepo(&stubTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	_, err := r.Get(ctx, projAtomID)
	if errors.Is(err, atom_projection.ErrNotFound) {
		t.Fatalf("real scan error was swallowed as ErrNotFound (the live 412 lie); want it propagated")
	}
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("want the driver error propagated; got %v", err)
	}
}

func TestProjectionRepo_Get_NoRowsIsNotFound(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error { return errors.New("no rows in result set") }}
		},
	}
	r := pg.NewProjectionRepo(&stubTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	_, err := r.Get(ctx, projAtomID)
	if !errors.Is(err, atom_projection.ErrNotFound) {
		t.Fatalf("no-rows must stay ErrNotFound; got %v", err)
	}
}

func TestProjectionRepo_Get_NullPublishedAtScansClean(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error {
				// 12 columns: atom_id, tenant_id, revision_id, owner_gcid,
				// author_display_name, stem, question_type, published_at,
				// archived, reuse_visibility, options, correct_answer.
				// (tenant_id joined the SELECT without this stub being
				// updated — pre-existing RED on main, repaired during
				// CHO-2174b. options/correct_answer added by mig 0040 for
				// AI-free duel MCQ.) published_at dest MUST be nullable
				// (**time.Time) — leave it nil (the live NULL).
				*(dest[0].(*string)) = projAtomID
				*(dest[1].(*string)) = tenantID
				*(dest[2].(*string)) = projRevID
				*(dest[3].(*string)) = authorGCID
				*(dest[4].(*string)) = "Ada"
				*(dest[5].(*string)) = "stem"
				*(dest[6].(*string)) = "mcq"
				if _, ok := dest[7].(**time.Time); !ok {
					t.Fatalf("published_at scan dest is %T; want **time.Time (nullable — live rows carry NULL)", dest[7])
				}
				*(dest[8].(*bool)) = false
				*(dest[9].(*string)) = "tenant"
				return nil
			}}
		},
	}
	r := pg.NewProjectionRepo(&stubTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	p, err := r.Get(ctx, projAtomID)
	if err != nil {
		t.Fatalf("Get with NULL published_at: %v", err)
	}
	if !p.PublishedAt.IsZero() {
		t.Fatalf("NULL published_at must scan to the zero time; got %v", p.PublishedAt)
	}
	if p.ReuseVisibility != "tenant" {
		t.Fatalf("reuse_visibility = %q; want tenant", p.ReuseVisibility)
	}
}

func TestAtomProjectionStore_Get_RealErrorPropagates(t *testing.T) {
	t.Parallel()
	boom := errors.New("connection reset by peer")
	q := &stubQuerier{
		rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error { return boom }}
		},
	}
	s := pg.NewAtomProjectionStore(&stubTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	_, err := s.Get(ctx, projAtomID)
	if errors.Is(err, atom_projection.ErrNotFound) {
		t.Fatalf("real scan error was swallowed as ErrNotFound; want it propagated")
	}
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("want the driver error propagated; got %v", err)
	}
}
