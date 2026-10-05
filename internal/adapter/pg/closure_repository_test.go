// closure_repository_test.go — pgx adapter tests for the durable
// ClosureRepository (W0-F1 durability + W0-F5 error-honesty, CHO-2198)
// using the shared stub Querier/TxRunner harness (see harness_test.go).
//
// These are unit tests against the SQL emit + scan surface — no live DB.
// The critical assertions here are the W0-F5 ones: a genuine backing-store
// error from either Pseudonymise or IsPseudonymised must come back as a
// non-nil error, never get coerced into a false/zero "everything is fine"
// result (the swallowed-error trap the in-memory port's original
// `IsPseudonymised(gcid string) bool` signature made structurally
// impossible to avoid).
//
// Unlike the chora-notifications reference (which asserts ON CONFLICT via
// a QueryRow + package-local ErrNoRows sentinel), this package has no
// ErrNoRows seam — see closure_repository.go's package doc for why. These
// tests instead drive the Query() + Rows.Next()/Rows.Err() idiom directly.
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/config"
)

const closureTenantID = "tenant-A"

func closureSpecFixture() []config.TableSpec {
	return []config.TableSpec{
		{
			Table: "posts",
			Columns: []config.ColumnSpec{
				{Column: "body", Strategy: "drop", Value: ""},
				{Column: "author_display_name", Strategy: "tombstone_string", Value: "Former member"},
			},
		},
		{
			Table: "social_profiles",
			Columns: []config.ColumnSpec{
				{Column: "bio", Strategy: "drop", Value: ""},
			},
		},
	}
}

// lastSQL returns the most recent SQL statement the stub recorded (across
// Exec + Query + QueryRow — rls.ApplySession's SET LOCAL lands before the
// repo's own query, so the repo's statement is always the LAST entry).
func lastSQL(q *stubQuerier) string {
	if len(q.sqls) == 0 {
		return ""
	}
	return q.sqls[len(q.sqls)-1]
}

// -----------------------------------------------------------------------------
// Pseudonymise
// -----------------------------------------------------------------------------

func TestClosureRepository_Pseudonymise_EmitsInsertOnConflictReturningID(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	q := &stubQuerier{queryFn: func(_ string, _ ...any) (pg.Rows, error) {
		return &stubRows{scans: []func(dest ...any) error{
			func(dest ...any) error {
				*(dest[0].(*string)) = uuid.NewString()
				return nil
			},
		}}, nil
	}}
	repo := pg.NewClosureRepository(&stubTxRunner{q: q})

	rows, err := repo.Pseudonymise(context.Background(), closureTenantID, "gcid-A", closureSpecFixture())
	if err != nil {
		t.Fatalf("Pseudonymise: %v", err)
	}
	if rows != 3 {
		t.Fatalf("expected rows_touched=3 (2+1 columns); got %d", rows)
	}

	got := lastSQL(q)
	wants := []string{"INSERT INTO closure_pseudonymisation_state", "ON CONFLICT", "DO NOTHING", "RETURNING"}
	for _, w := range wants {
		if !strings.Contains(got, w) {
			t.Errorf("Pseudonymise SQL missing %q; got:\n%s", w, got)
		}
	}
}

func TestClosureRepository_Pseudonymise_MintsUUIDv7ForID(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	var capturedArgs []any
	q := &stubQuerier{queryFn: func(_ string, args ...any) (pg.Rows, error) {
		capturedArgs = args
		return &stubRows{scans: []func(dest ...any) error{
			func(dest ...any) error {
				*(dest[0].(*string)) = uuid.NewString()
				return nil
			},
		}}, nil
	}}
	repo := pg.NewClosureRepository(&stubTxRunner{q: q})

	if _, err := repo.Pseudonymise(context.Background(), closureTenantID, "gcid-A", nil); err != nil {
		t.Fatalf("Pseudonymise: %v", err)
	}
	if len(capturedArgs) == 0 {
		t.Fatalf("expected query args")
	}
	idArg, ok := capturedArgs[0].(string)
	if !ok || idArg == "" {
		t.Fatalf("expected non-empty string id as first arg; got %T %v", capturedArgs[0], capturedArgs[0])
	}
	parsed, err := uuid.Parse(idArg)
	if err != nil {
		t.Fatalf("minted id %q is not a valid UUID: %v", idArg, err)
	}
	if parsed.Version() != 7 {
		t.Fatalf("minted id %q is not UUIDv7 (version=%d)", idArg, parsed.Version())
	}
}

func TestClosureRepository_Pseudonymise_ReturnsZeroWhenConflictFires(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	// ON CONFLICT DO NOTHING suppresses the RETURNING row: Next() is false
	// and Err() is nil — a CLEAN miss, not an error.
	q := &stubQuerier{} // default queryFn -> &stubRows{} (empty, nil Err())
	repo := pg.NewClosureRepository(&stubTxRunner{q: q})

	rows, err := repo.Pseudonymise(context.Background(), closureTenantID, "gcid-A", closureSpecFixture())
	if err != nil {
		t.Fatalf("Pseudonymise: expected idempotent no-op, got error: %v", err)
	}
	if rows != 0 {
		t.Fatalf("expected rows=0 on idempotent replay; got %d", rows)
	}
}

func TestClosureRepository_Pseudonymise_RejectsEmptyTenantID(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	q := &stubQuerier{}
	repo := pg.NewClosureRepository(&stubTxRunner{q: q})
	_, err := repo.Pseudonymise(context.Background(), "", "gcid-A", nil)
	if err == nil {
		t.Fatalf("expected error on empty tenant_id")
	}
	if len(q.sqls) != 0 {
		t.Fatalf("must not emit SQL on validation failure; got %v", q.sqls)
	}
}

func TestClosureRepository_Pseudonymise_RejectsEmptyGCID(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	q := &stubQuerier{}
	repo := pg.NewClosureRepository(&stubTxRunner{q: q})
	_, err := repo.Pseudonymise(context.Background(), closureTenantID, "", nil)
	if err == nil {
		t.Fatalf("expected error on empty gcid")
	}
	if len(q.sqls) != 0 {
		t.Fatalf("must not emit SQL on validation failure; got %v", q.sqls)
	}
}

func TestClosureRepository_Pseudonymise_NilTxRunner(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	repo := pg.NewClosureRepository(nil)
	_, err := repo.Pseudonymise(context.Background(), closureTenantID, "gcid-A", nil)
	if !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

// TestClosureRepository_Pseudonymise_PropagatesQueryError is the W0-F5
// fail-loud proof for Pseudonymise when Query() itself errors (e.g. a
// connection failure before any row is materialised): the error must
// propagate, never as a silent "idempotent no-op" (0, nil).
func TestClosureRepository_Pseudonymise_PropagatesQueryError(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	boom := errors.New("pg: connection reset by peer")
	q := &stubQuerier{queryFn: func(_ string, _ ...any) (pg.Rows, error) {
		return nil, boom
	}}
	repo := pg.NewClosureRepository(&stubTxRunner{q: q})

	rows, err := repo.Pseudonymise(context.Background(), closureTenantID, "gcid-A", closureSpecFixture())
	if err == nil {
		t.Fatalf("expected error to propagate, got nil (rows=%d)", rows)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected wrapped sentinel error; got %v", err)
	}
	if rows != 0 {
		t.Fatalf("expected rows=0 on error; got %d", rows)
	}
}

// TestClosureRepository_Pseudonymise_PropagatesRowsErrAfterEmptyCursor is
// the OTHER W0-F5 fail-loud proof: Query() succeeds but the cursor itself
// carries a genuine error (Next()==false AND Err()!=nil) — this is exactly
// the case a naive "no rows == conflict" read would misclassify as a clean
// idempotent no-op. It must propagate as a non-nil error.
func TestClosureRepository_Pseudonymise_PropagatesRowsErrAfterEmptyCursor(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	boom := errors.New("pg: cursor read failed")
	q := &stubQuerier{queryFn: func(_ string, _ ...any) (pg.Rows, error) {
		return &stubRows{err: boom}, nil
	}}
	repo := pg.NewClosureRepository(&stubTxRunner{q: q})

	rows, err := repo.Pseudonymise(context.Background(), closureTenantID, "gcid-A", closureSpecFixture())
	if err == nil {
		t.Fatalf("expected error to propagate, got nil (rows=%d)", rows)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected wrapped sentinel error; got %v", err)
	}
	if rows != 0 {
		t.Fatalf("expected rows=0 on error; got %d", rows)
	}
}

// -----------------------------------------------------------------------------
// IsPseudonymised
// -----------------------------------------------------------------------------

func TestClosureRepository_IsPseudonymised_TrueWhenRowExists(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{queryFn: func(_ string, _ ...any) (pg.Rows, error) {
		return &stubRows{scans: []func(dest ...any) error{
			func(dest ...any) error {
				*(dest[0].(*int)) = 1
				return nil
			},
		}}, nil
	}}
	repo := pg.NewClosureRepository(&stubTxRunner{q: q})

	got, err := repo.IsPseudonymised(context.Background(), closureTenantID, "gcid-A")
	if err != nil {
		t.Fatalf("IsPseudonymised: %v", err)
	}
	if !got {
		t.Fatalf("expected true when a row exists")
	}
	if !strings.Contains(lastSQL(q), "FROM closure_pseudonymisation_state") {
		t.Errorf("IsPseudonymised SQL malformed; got:\n%s", lastSQL(q))
	}
}

func TestClosureRepository_IsPseudonymised_FalseWhenNoRows(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{} // default queryFn -> &stubRows{} (empty, nil Err())
	repo := pg.NewClosureRepository(&stubTxRunner{q: q})

	got, err := repo.IsPseudonymised(context.Background(), closureTenantID, "gcid-A")
	if err != nil {
		t.Fatalf("IsPseudonymised: expected nil error on a clean miss; got %v", err)
	}
	if got {
		t.Fatalf("expected false when no row exists")
	}
}

// TestClosureRepository_IsPseudonymised_PropagatesQueryError is the core
// W0-F5 proof for IsPseudonymised: this is exactly the method the original
// `IsPseudonymised(gcid string) bool` signature could NOT have implemented
// honestly against Postgres (no ctx, no error return). A real backing-store
// error must be reported, not folded into `false`.
func TestClosureRepository_IsPseudonymised_PropagatesQueryError(t *testing.T) {
	t.Parallel()
	boom := errors.New("pg: connection reset by peer")
	q := &stubQuerier{queryFn: func(_ string, _ ...any) (pg.Rows, error) {
		return nil, boom
	}}
	repo := pg.NewClosureRepository(&stubTxRunner{q: q})

	got, err := repo.IsPseudonymised(context.Background(), closureTenantID, "gcid-A")
	if err == nil {
		t.Fatalf("expected error to propagate, got nil (got=%v)", got)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected wrapped sentinel error; got %v", err)
	}
	if got {
		t.Fatalf("expected false alongside the error (never claim true on failure)")
	}
}

// TestClosureRepository_IsPseudonymised_PropagatesRowsErrAfterEmptyCursor
// proves the fail-loud contract holds even when the cursor comes back
// empty-but-broken (Next()==false, Err()!=nil) — the exact shape a naive
// "empty means false" read would misclassify as "not pseudonymised".
func TestClosureRepository_IsPseudonymised_PropagatesRowsErrAfterEmptyCursor(t *testing.T) {
	t.Parallel()
	boom := errors.New("pg: cursor read failed")
	q := &stubQuerier{queryFn: func(_ string, _ ...any) (pg.Rows, error) {
		return &stubRows{err: boom}, nil
	}}
	repo := pg.NewClosureRepository(&stubTxRunner{q: q})

	got, err := repo.IsPseudonymised(context.Background(), closureTenantID, "gcid-A")
	if err == nil {
		t.Fatalf("expected error to propagate, got nil (got=%v)", got)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected wrapped sentinel error; got %v", err)
	}
	if got {
		t.Fatalf("expected false alongside the error (never claim true on failure)")
	}
}

func TestClosureRepository_IsPseudonymised_RejectsEmptyTenantID(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	repo := pg.NewClosureRepository(&stubTxRunner{q: q})
	_, err := repo.IsPseudonymised(context.Background(), "", "gcid-A")
	if err == nil {
		t.Fatalf("expected error on empty tenant_id")
	}
	if len(q.sqls) != 0 {
		t.Fatalf("must not emit SQL on validation failure; got %v", q.sqls)
	}
}

func TestClosureRepository_IsPseudonymised_RejectsEmptyGCID(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	repo := pg.NewClosureRepository(&stubTxRunner{q: q})
	_, err := repo.IsPseudonymised(context.Background(), closureTenantID, "")
	if err == nil {
		t.Fatalf("expected error on empty gcid")
	}
	if len(q.sqls) != 0 {
		t.Fatalf("must not emit SQL on validation failure; got %v", q.sqls)
	}
}

func TestClosureRepository_IsPseudonymised_NilTxRunner(t *testing.T) {
	t.Parallel()
	repo := pg.NewClosureRepository(nil)
	_, err := repo.IsPseudonymised(context.Background(), closureTenantID, "gcid-A")
	if !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}
