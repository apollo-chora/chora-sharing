// zz_grant_repo_a_test.go — "A"-lane coverage for grant_repo.go +
// grant_repo_reuse.go: Authorize / Revoke / GetActive / loadActive /
// ListEntitled / scanGrant / stemPreviewFromString / ActiveGrantAtomIDs.
//
// Uses the shared stub harness (harness_test.go) + the aExecFailQuerier
// helper from zz_harness_extra_a_test.go so every Exec error branch and
// the RLS SET-LOCAL failure branch is reachable.
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/rls"

	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
	"github.com/apollo-chora/chora-sharing/internal/domain/grant"
)

func aGrant() *grant.AtomUsageGrant {
	g, err := grant.NewGrant(
		authorGCID, "01970000-0000-7000-a000-0000000000bb",
		"01970000-0000-7000-b000-000000000001", "01970000-0000-7000-b000-0000000000a1",
		grant.ScopeTestSet, atom_share.LicenseFree, atom_share.RoyaltyRate{}, "share-1",
	)
	if err != nil {
		panic(err)
	}
	g.ID = "01970000-0000-7000-a000-0000000000dd"
	g.TenantID = tenantID
	return g
}

// ---------------------------------------------------------------- Authorize

func TestGrantRepo_Authorize_NilTxRunner(t *testing.T) {
	t.Parallel()
	if _, err := pg.NewGrantRepo(nil).Authorize(aCtx(), aGrant()); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

func TestGrantRepo_Authorize_NilGrant(t *testing.T) {
	t.Parallel()
	r := pg.NewGrantRepo(&stubTxRunner{q: &stubQuerier{}})
	if _, err := r.Authorize(aCtx(), nil); !errors.Is(err, grant.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument; got %v", err)
	}
}

func TestGrantRepo_Authorize_Inserted_ReturnsInput(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{} // default Exec → RowsAffected=1 → inserted
	r := pg.NewGrantRepo(&stubTxRunner{q: q})

	g := aGrant()
	revokedAt := time.Now().UTC().Add(-time.Hour)
	expiresAt := time.Now().UTC().Add(time.Hour)
	g.RevokedAt = &revokedAt
	g.ExpiresAt = &expiresAt

	got, err := r.Authorize(aCtx(), g)
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if got != g {
		t.Fatalf("expected the input grant back on fresh insert; got %+v", got)
	}
	// RLS SET LOCAL must be the first SQL; the INSERT the last.
	if len(q.sqls) < 2 || !strings.Contains(q.sqls[0], "SET LOCAL chora.tenant_id") {
		t.Fatalf("first SQL must be SET LOCAL; got %#v", q.sqls)
	}
	ins := q.sqls[len(q.sqls)-1]
	if !strings.Contains(ins, "INSERT INTO atom_usage_grants") || !strings.Contains(ins, "ON CONFLICT DO NOTHING") {
		t.Fatalf("expected INSERT ... ON CONFLICT DO NOTHING; got %q", ins)
	}
	// source_share_entry binds NULL when empty? — here non-empty so it is a string.
	insArgs := q.args[len(q.args)-1]
	if insArgs[2] != "share-1" {
		t.Fatalf("source_share_entry arg = %v", insArgs[2])
	}
	if insArgs[11] == nil {
		t.Fatalf("revoked_at must bind the *time.Time value when non-nil")
	}
	if insArgs[12] == nil {
		t.Fatalf("expires_at must bind the *time.Time value when non-nil")
	}
}

func TestGrantRepo_Authorize_Inserted_NullableArgsWhenEmpty(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	g := aGrant()
	g.SourceShareEntry = ""

	if _, err := r.Authorize(aCtx(), g); err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	insArgs := q.args[len(q.args)-1]
	if insArgs[2] != nil {
		t.Fatalf("empty source_share_entry must bind nil; got %v", insArgs[2])
	}
	if insArgs[11] != nil || insArgs[12] != nil {
		t.Fatalf("nil revoked_at/expires_at must bind nil; got %v, %v", insArgs[11], insArgs[12])
	}
}

func TestGrantRepo_Authorize_ConflictReloadsExisting(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	existing := aGrant()
	existing.GranteeGCID = aGrant().GranteeGCID
	q.execTagFn = func(sql string) rls.CommandTag {
		if strings.Contains(sql, "INSERT INTO atom_usage_grants") {
			return rls.CommandTag{RowsAffected: 0} // conflict — frozen snapshot wins
		}
		return rls.CommandTag{RowsAffected: 1}
	}
	q.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: func(dest ...any) error { return aScanGrantRow(dest, existing) }}
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})

	got, err := r.Authorize(aCtx(), aGrant())
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if got == nil {
		t.Fatalf("expected the existing grant back on conflict")
	}
	if !strings.Contains(aLastSQL(q), "WHERE grantee_gcid = $1") {
		t.Fatalf("conflict path must SELECT the existing active grant; got %q", aLastSQL(q))
	}
}

func TestGrantRepo_Authorize_ConflictReloadFails_ReturnsInput(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.execTagFn = func(sql string) rls.CommandTag {
		if strings.Contains(sql, "INSERT INTO atom_usage_grants") {
			return rls.CommandTag{RowsAffected: 0}
		}
		return rls.CommandTag{RowsAffected: 1}
	}
	q.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: func(dest ...any) error { return errors.New("no rows in result set") }}
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})

	got, err := r.Authorize(aCtx(), aGrant())
	if err != nil {
		t.Fatalf("Authorize: conflict-reload failure must not error; got %v", err)
	}
	if got == nil {
		t.Fatalf("expected the input grant back when the reload misses")
	}
}

func TestGrantRepo_Authorize_ExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "INSERT INTO atom_usage_grants", execErr: aErrExec}
	r := pg.NewGrantRepo(&aTxRunner{q: q})
	if _, err := r.Authorize(aCtx(), aGrant()); err == nil {
		t.Fatalf("expected the insert error to propagate")
	}
}

func TestGrantRepo_Authorize_MissingTenant_FailsLoud(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	if _, err := r.Authorize(context.Background(), aGrant()); err == nil {
		t.Fatalf("expected ErrNoTenantContext on a bare context")
	}
}

// ------------------------------------------------------------------ Revoke

func TestGrantRepo_Revoke_NilTxRunner(t *testing.T) {
	t.Parallel()
	if err := pg.NewGrantRepo(nil).Revoke(aCtx(), "g-1", authorGCID, "no reason"); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

func TestGrantRepo_Revoke_MissingGrant_ReturnsErrGrantNotFound(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: func(dest ...any) error { return errors.New("no rows in result set") }}
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	err := r.Revoke(aCtx(), "g-1", authorGCID, "why")
	if !errors.Is(err, grant.ErrGrantNotFound) {
		t.Fatalf("expected ErrGrantNotFound; got %v", err)
	}
}

func TestGrantRepo_Revoke_NonOwner_ReturnsErrForbidden(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: func(dest ...any) error {
			zqSetString(dest, 0, "g-1")
			zqSetString(dest, 1, tenantID)
			zqSetString(dest, 2, "someone-else")
			return nil
		}}
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	err := r.Revoke(aCtx(), "g-1", authorGCID, "why")
	if !errors.Is(err, grant.ErrForbidden) {
		t.Fatalf("expected ErrForbidden; got %v", err)
	}
}

func TestGrantRepo_Revoke_HappyPath_UpdatesAndTrails(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: func(dest ...any) error {
			zqSetString(dest, 0, "g-1")
			zqSetString(dest, 1, tenantID)
			zqSetString(dest, 2, authorGCID)
			return nil
		}}
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	if err := r.Revoke(aCtx(), "g-1", authorGCID, "author withdrew"); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	joined := strings.Join(q.sqls, " || ")
	if !strings.Contains(joined, "SET status = 'revoked'") {
		t.Fatalf("revoke must flip status to revoked; sqls=%s", joined)
	}
	if !strings.Contains(joined, "INSERT INTO grant_events") {
		t.Fatalf("revoke must append a grant_events row; sqls=%s", joined)
	}
}

func TestGrantRepo_Revoke_RevokeExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{}
	q.stubQuerier = &stubQuerier{
		rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error {
				zqSetString(dest, 0, "g-1")
				zqSetString(dest, 1, tenantID)
				zqSetString(dest, 2, authorGCID)
				return nil
			}}
		},
	}
	q.failSubstr = "SET status = 'revoked'"
	q.execErr = aErrExec
	r := pg.NewGrantRepo(&aTxRunner{q: q})
	if err := r.Revoke(aCtx(), "g-1", authorGCID, "x"); err == nil {
		t.Fatalf("expected the revoke UPDATE error to propagate")
	}
}

func TestGrantRepo_Revoke_EventExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{}
	q.stubQuerier = &stubQuerier{
		rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error {
				zqSetString(dest, 0, "g-1")
				zqSetString(dest, 1, tenantID)
				zqSetString(dest, 2, authorGCID)
				return nil
			}}
		},
	}
	q.failSubstr = "INSERT INTO grant_events"
	q.execErr = aErrExec
	r := pg.NewGrantRepo(&aTxRunner{q: q})
	if err := r.Revoke(aCtx(), "g-1", authorGCID, "x"); err == nil {
		t.Fatalf("expected the grant_events insert error to propagate")
	}
}

func TestGrantRepo_Revoke_MissingTenant_FailsLoud(t *testing.T) {
	t.Parallel()
	r := pg.NewGrantRepo(&stubTxRunner{q: &stubQuerier{}})
	if err := r.Revoke(context.Background(), "g-1", authorGCID, "x"); err == nil {
		t.Fatalf("expected ErrNoTenantContext on a bare context")
	}
}

// --------------------------------------------------------------- GetActive

func TestGrantRepo_GetActive_NilTxRunner(t *testing.T) {
	t.Parallel()
	if _, err := pg.NewGrantRepo(nil).GetActive(aCtx(), authorGCID, "atom-1", grant.ScopeTestSet); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

func TestGrantRepo_GetActive_ExactMatch(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	want := aGrant()
	q.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: func(dest ...any) error { return aScanGrantRow(dest, want) }}
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	got, err := r.GetActive(aCtx(), want.GranteeGCID, want.AtomID, want.Scope)
	if err != nil {
		t.Fatalf("GetActive: %v", err)
	}
	if got.AtomID != want.AtomID || got.GranteeGCID != want.GranteeGCID || got.Scope != want.Scope {
		t.Fatalf("got %+v", got)
	}
}

func TestGrantRepo_GetActive_FallsBackToUnlimited(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	want := aGrant()
	want.Scope = grant.ScopeUnlimited
	exact := true // first loadActive call (exact scope) misses
	q.rowFn = func(sql string, args ...any) pg.Row {
		if exact {
			exact = false
			return stubRow{scanFn: func(dest ...any) error { return errors.New("no rows in result set") }}
		}
		return stubRow{scanFn: func(dest ...any) error { return aScanGrantRow(dest, want) }}
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})

	got, err := r.GetActive(aCtx(), want.GranteeGCID, want.AtomID, grant.ScopeTestSet)
	if err != nil {
		t.Fatalf("GetActive (fallback): %v", err)
	}
	if got == nil || got.Scope != grant.ScopeUnlimited {
		t.Fatalf("expected the UNLIMITED fallback grant; got %+v", got)
	}
	// The fallback query must bind scope='unlimited' as $3.
	lastArgs := q.args[len(q.args)-1]
	if len(lastArgs) < 3 || lastArgs[2] != "unlimited" {
		t.Fatalf("fallback SELECT must bind unlimited scope; args=%v", lastArgs)
	}
}

func TestGrantRepo_GetActive_NoFallbackForUnlimitedRequest(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: func(dest ...any) error { return errors.New("no rows in result set") }}
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})

	_, err := r.GetActive(aCtx(), authorGCID, "atom-1", grant.ScopeUnlimited)
	if !errors.Is(err, grant.ErrGrantNotFound) {
		t.Fatalf("expected ErrGrantNotFound; got %v", err)
	}
	// Only ONE user SELECT must run — no needless unlimited fallback.
	if len(q.sqls) != 2 {
		t.Fatalf("expected SET LOCAL + one SELECT; got %d SQLs: %v", len(q.sqls), q.sqls)
	}
}

func TestGrantRepo_GetActive_MissingTenant_FailsLoud(t *testing.T) {
	t.Parallel()
	r := pg.NewGrantRepo(&stubTxRunner{q: &stubQuerier{}})
	if _, err := r.GetActive(context.Background(), authorGCID, "atom-1", grant.ScopeTestSet); err == nil {
		t.Fatalf("expected ErrNoTenantContext on a bare context")
	}
}

// --------------------------------------------------------------- scanGrant

// aScanGrantRow populates the 14-column scanGrant surface from a grant. The
// destinations match scanGrant's scan list: id/tenant/sourceShare/owner/atom/
// revision/grantee are *string and **string; scope, license snapshot and
// status are typed enum destinations; royalty is []byte; times are *time.Time
// and **time.Time (revoked_at/expires_at are nullable pointer columns).
func aScanGrantRow(dest []any, g *grant.AtomUsageGrant) error {
	zqSetString(dest, 0, g.ID)
	zqSetString(dest, 1, g.TenantID)
	if p, ok := dest[2].(**string); ok && g.SourceShareEntry != "" {
		s := g.SourceShareEntry
		*p = &s
	}
	zqSetString(dest, 3, g.OwnerGCID)
	zqSetString(dest, 4, g.AtomID)
	zqSetString(dest, 5, g.RevisionID)
	zqSetString(dest, 6, g.GranteeGCID)
	if p, ok := dest[7].(*grant.Scope); ok {
		*p = g.Scope
	}
	if p, ok := dest[8].(*atom_share.LicenseTerms); ok {
		*p = g.LicenseTermsSnapshot
	}
	zqSetBytes(dest, 9, []byte(`{"kind":"pct","value":10}`))
	if p, ok := dest[10].(*grant.Status); ok {
		*p = g.Status
	}
	zqSetTime(dest, 11, g.GrantedAt)
	if p, ok := dest[12].(**time.Time); ok && g.RevokedAt != nil {
		*p = g.RevokedAt
	}
	if p, ok := dest[13].(**time.Time); ok && g.ExpiresAt != nil {
		*p = g.ExpiresAt
	}
	return nil
}

func TestGrantRepo_loadActive_HappyPath(t *testing.T) {
	t.Parallel()
	g := aGrant()
	g.SourceShareEntry = "share-9"
	revokedAt := time.Now().UTC().Add(-2 * time.Hour)
	expiresAt := time.Now().UTC().Add(2 * time.Hour)
	g.RevokedAt = &revokedAt
	g.ExpiresAt = &expiresAt

	q := &stubQuerier{}
	q.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: func(dest ...any) error { return aScanGrantRow(dest, g) }}
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	got, err := r.GetActive(aCtx(), g.GranteeGCID, g.AtomID, g.Scope)
	if err != nil {
		t.Fatalf("GetActive: %v", err)
	}
	if got.SourceShareEntry != "share-9" {
		t.Fatalf("source_share_entry not hydrated: %q", got.SourceShareEntry)
	}
	if got.RoyaltyRateSnapshot.Kind != "pct" || got.RoyaltyRateSnapshot.Value != 10 {
		t.Fatalf("royalty snapshot not unmarshalled: %+v", got.RoyaltyRateSnapshot)
	}
	if got.RevokedAt == nil || !got.RevokedAt.Equal(revokedAt) {
		t.Fatalf("revoked_at not hydrated: %v", got.RevokedAt)
	}
	if got.ExpiresAt == nil || !got.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("expires_at not hydrated: %v", got.ExpiresAt)
	}
}

func TestGrantRepo_GetActive_ScanError_IsGrantNotFound(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.rowFn = func(sql string, args ...any) pg.Row {
		return stubRow{scanFn: func(dest ...any) error { return errors.New("scan boom") }}
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	_, err := r.GetActive(aCtx(), authorGCID, "atom-1", grant.ScopeTestSet)
	if !errors.Is(err, grant.ErrGrantNotFound) {
		t.Fatalf("any scan error inside loadActive maps to ErrGrantNotFound; got %v", err)
	}
}

// ------------------------------------------------------------ ListEntitled

// aOwnRowScan fills the 6-column own-leg projection scan.
func aOwnRowScan(atomID, revID, owner, display, stem, qType string) func(dest ...any) error {
	return func(dest ...any) error {
		zqSetString(dest, 0, atomID)
		zqSetString(dest, 1, revID)
		zqSetString(dest, 2, owner)
		zqSetString(dest, 3, display)
		zqSetString(dest, 4, stem)
		zqSetString(dest, 5, qType)
		return nil
	}
}

// aFreeRowScan fills the 1-column free-share content JSON scan.
func aFreeRowScan(content string) func(dest ...any) error {
	return func(dest ...any) error {
		zqSetBytes(dest, 0, []byte(content))
		return nil
	}
}

// aGrantRowScan fills the 5-column active-grant scan.
func aGrantRowScan(atomID, revID, owner, license, scope string) func(dest ...any) error {
	return func(dest ...any) error {
		zqSetString(dest, 0, atomID)
		zqSetString(dest, 1, revID)
		zqSetString(dest, 2, owner)
		zqSetString(dest, 3, license)
		zqSetString(dest, 4, scope)
		return nil
	}
}

func TestGrantRepo_ListEntitled_UnionAndSortAndClamp(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			switch {
			case strings.Contains(sql, "owner_gcid = $1"): // own leg
				return &stubRows{scans: []func(dest ...any) error{
					aOwnRowScan("a-2", "r2", authorGCID, "Two", "stem two", "mcq"),
					aOwnRowScan("a-1", "r1", authorGCID, "One", "stem one", "mcq"),
				}}, nil
			case strings.Contains(sql, "social_feed_entries"): // free leg
				return &stubRows{scans: []func(dest ...any) error{
					aFreeRowScan(`{"atom_id":"a-3","atom_revision_id":"r3","owner_gcid":"o3","author_display_name":"Three","stem_preview":"s3","license_terms":"free","question_type":"mcq"}`),
				}}, nil
			default: // grants leg
				return &stubRows{scans: []func(dest ...any) error{
					aGrantRowScan("a-4", "r4", "o4", "royalty_pct", "unlimited"),
				}}, nil
		}
		},
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	out, err := r.ListEntitled(aCtx(), authorGCID, grant.ScopeDuel, nil, 2)
	if err != nil {
		t.Fatalf("ListEntitled: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("limit clamps the result: got %d, want 2", len(out))
	}
	if out[0].AtomID != "a-1" || out[1].AtomID != "a-2" {
		t.Fatalf("result must be sorted by atom_id: %+v", out)
	}
	if !out[0].IsOwn || out[0].License != atom_share.LicenseFree {
		t.Fatalf("own leg must set IsOwn + LicenseFree: %+v", out[0])
	}
}

func TestGrantRepo_ListEntitled_GrantCoversUnlimitedAndScopeMismatchSkipped(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			switch {
			case strings.Contains(sql, "owner_gcid = $1"):
				return &stubRows{}, nil
			case strings.Contains(sql, "social_feed_entries"):
				return &stubRows{}, nil
			default:
				return &stubRows{scans: []func(dest ...any) error{
					aGrantRowScan("b-1", "r1", "o1", "free", "unlimited"), // covers duel
					aGrantRowScan("b-2", "r2", "o2", "free", "test_set"), // does NOT cover duel
				}}, nil
			}
		},
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	out, err := r.ListEntitled(aCtx(), authorGCID, grant.ScopeDuel, nil, 0)
	if err != nil {
		t.Fatalf("ListEntitled: %v", err)
	}
	if len(out) != 1 || out[0].AtomID != "b-1" {
		t.Fatalf("only the UNLIMITED grant covers duel; got %+v", out)
	}
	if !out[0].HasGrant {
		t.Fatalf("grant leg must set HasGrant: %+v", out[0])
	}
}

func TestGrantRepo_ListEntitled_TagFilters_TopicTagsFilterAllLegs(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			switch {
			case strings.Contains(sql, "owner_gcid = $1"):
				return &stubRows{scans: []func(dest ...any) error{
					aOwnRowScan("t-1", "r1", authorGCID, "One", "stem", "mcq"),
					aOwnRowScan("t-2", "r2", authorGCID, "Two", "stem", "essay"),
				}}, nil
			case strings.Contains(sql, "social_feed_entries"):
				return &stubRows{scans: []func(dest ...any) error{
					aFreeRowScan(`{"atom_id":"t-3","atom_revision_id":"r3","owner_gcid":"o3","author_display_name":"Three","stem_preview":"s3","license_terms":"free","question_type":"MCQ"}`),
					aFreeRowScan(`{"atom_id":"t-4","atom_revision_id":"r4","owner_gcid":"o4","author_display_name":"Four","stem_preview":"s4","license_terms":"free","question_type":"essay"}`),
					aFreeRowScan(`not-json{`), // bad JSON is skipped, not fatal
				}}, nil
			default:
				return &stubRows{}, nil
			}
		},
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	out, err := r.ListEntitled(aCtx(), authorGCID, grant.ScopeTestSet, []string{"Mcq"}, 100)
	if err != nil {
		t.Fatalf("ListEntitled: %v", err)
	}
	// t-1 (own, mcq) and t-3 (free, MCQ case-insensitive) survive; t-2 (essay) filtered.
	if len(out) != 2 {
		t.Fatalf("tag filter keeps only mcq rows; got %+v", out)
	}
}

func TestGrantRepo_ListEntitled_OwnScanError_Aborts(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			if strings.Contains(sql, "owner_gcid = $1") {
				return &stubRows{scans: []func(dest ...any) error{
					func(dest ...any) error { return errors.New("scan boom") },
				}}, nil
			}
			return &stubRows{}, nil
		},
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	if _, err := r.ListEntitled(aCtx(), authorGCID, grant.ScopeTestSet, nil, 0); err == nil {
		t.Fatalf("expected the own-leg scan error to abort the listing")
	}
}

func TestGrantRepo_ListEntitled_FreeScanError_Aborts(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			if strings.Contains(sql, "social_feed_entries") {
				return &stubRows{scans: []func(dest ...any) error{
					func(dest ...any) error { return errors.New("scan boom") },
				}}, nil
			}
			return &stubRows{}, nil
		},
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	if _, err := r.ListEntitled(aCtx(), authorGCID, grant.ScopeTestSet, nil, 0); err == nil {
		t.Fatalf("expected the free-leg scan error to abort the listing")
	}
}

func TestGrantRepo_ListEntitled_GrantScanError_Aborts(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			if strings.Contains(sql, "FROM atom_usage_grants") {
				return &stubRows{scans: []func(dest ...any) error{
					func(dest ...any) error { return errors.New("scan boom") },
				}}, nil
			}
			return &stubRows{}, nil
		},
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	if _, err := r.ListEntitled(aCtx(), authorGCID, grant.ScopeTestSet, nil, 0); err == nil {
		t.Fatalf("expected the grants-leg scan error to abort the listing")
	}
}

func TestGrantRepo_ListEntitled_QueryErrors_Propagate(t *testing.T) {
	t.Parallel()
	for _, failOn := range []string{"owner_gcid = $1", "social_feed_entries", "FROM atom_usage_grants"} {
		q := &stubQuerier{}
		q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
			if strings.Contains(sql, failOn) {
				return nil, errors.New("db down")
			}
			return &stubRows{}, nil
		}
		r := pg.NewGrantRepo(&stubTxRunner{q: q})
		if _, err := r.ListEntitled(aCtx(), authorGCID, grant.ScopeTestSet, nil, 0); err == nil {
			t.Fatalf("expected the %q query error to propagate", failOn)
		}
	}
}

func TestGrantRepo_ListEntitled_RowsErr_AfterOwnLeg_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			if strings.Contains(sql, "owner_gcid = $1") {
				return &stubRows{err: errors.New("cursor error"), scans: []func(dest ...any) error{
					func(dest ...any) error {
						zqSetString(dest, 0, "a-1")
						zqSetString(dest, 1, "r1")
						zqSetString(dest, 2, authorGCID)
						zqSetString(dest, 3, "n")
						zqSetString(dest, 4, "s")
						zqSetString(dest, 5, "mcq")
						return nil
					},
				}}, nil
			}
			return &stubRows{}, nil
		},
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	if _, err := r.ListEntitled(aCtx(), authorGCID, grant.ScopeTestSet, nil, 0); err == nil {
		t.Fatalf("expected the own-leg rows.Err() to propagate")
	}
}

func TestGrantRepo_ListEntitled_FreeLegRowsErr_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			if strings.Contains(sql, "social_feed_entries") {
				return &stubRows{err: errors.New("cursor error"), scans: []func(dest ...any) error{
					aFreeRowScan(`{"atom_id":"f-1","atom_revision_id":"r1","owner_gcid":"o1","author_display_name":"One","stem_preview":"s","license_terms":"free","question_type":"mcq"}`),
				}}, nil
			}
			return &stubRows{}, nil
		},
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	if _, err := r.ListEntitled(aCtx(), authorGCID, grant.ScopeTestSet, nil, 0); err == nil {
		t.Fatalf("expected the free-leg rows.Err() to propagate")
	}
}

func TestGrantRepo_ListEntitled_MissingTenant_FailsLoud(t *testing.T) {
	t.Parallel()
	r := pg.NewGrantRepo(&stubTxRunner{q: &stubQuerier{}})
	if _, err := r.ListEntitled(context.Background(), authorGCID, grant.ScopeTestSet, nil, 0); err == nil {
		t.Fatalf("expected ErrNoTenantContext on a bare context")
	}
}

func TestGrantRepo_ListEntitled_DefaultLimits(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	// limit 0 → default 100; limit 600 → clamped 500. No rows → empty slice.
	out, err := r.ListEntitled(aCtx(), authorGCID, grant.ScopeTestSet, nil, 0)
	if err != nil || out == nil {
		t.Fatalf("limit=0: out=%v err=%v", out, err)
	}
	out2, err := r.ListEntitled(aCtx(), authorGCID, grant.ScopeTestSet, nil, 600)
	if err != nil || out2 == nil {
		t.Fatalf("limit=600: out=%v err=%v", out2, err)
	}
	q2 := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			if strings.Contains(sql, "owner_gcid = $1") {
				return &stubRows{scans: []func(dest ...any) error{
					aOwnRowScan("z-1", "r1", authorGCID, "One", "stem one", "mcq"),
					aOwnRowScan("z-2", "r2", authorGCID, "Two", "stem two", "mcq"),
					aOwnRowScan("z-3", "r3", authorGCID, "Three", "stem three", "mcq"),
				}}, nil
			}
			return &stubRows{}, nil
		},
	}
	r2 := pg.NewGrantRepo(&stubTxRunner{q: q2})
	out3, err := r2.ListEntitled(aCtx(), authorGCID, grant.ScopeTestSet, nil, 2)
	if err != nil {
		t.Fatalf("ListEntitled: %v", err)
	}
	if len(out3) != 2 {
		t.Fatalf("expected slice clamp to 2; got %d", len(out3))
	}
}

func TestGrantRepo_ListEntitled_NilTxRunner(t *testing.T) {
	t.Parallel()
	if _, err := pg.NewGrantRepo(nil).ListEntitled(aCtx(), authorGCID, grant.ScopeTestSet, nil, 0); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

// --------------------------------------------------- stemPreviewFromString

func TestGrantRepo_StemPreview_TruncatesLongStemsAtRuneBoundary(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			if strings.Contains(sql, "owner_gcid = $1") {
				long := strings.Repeat("界", 300)
				return &stubRows{scans: []func(dest ...any) error{
					aOwnRowScan("s-1", "r1", authorGCID, "Nome", long, "mcq"),
				}}, nil
			}
			return &stubRows{}, nil
		},
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	out, err := r.ListEntitled(aCtx(), authorGCID, grant.ScopeTestSet, nil, 0)
	if err != nil {
		t.Fatalf("ListEntitled: %v", err)
	}
	preview := out[0].StemPreview
	if len([]rune(preview)) != 140 {
		t.Fatalf("stem preview must truncate to 140 runes; got %d runes", len([]rune(preview)))
	}
}

func TestGrantRepo_StemPreview_ShortStemPassesThrough(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			if strings.Contains(sql, "owner_gcid = $1") {
				return &stubRows{scans: []func(dest ...any) error{
					aOwnRowScan("s-1", "r1", authorGCID, "Nome", "short stem", "mcq"),
				}}, nil
			}
			return &stubRows{}, nil
		},
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	out, err := r.ListEntitled(aCtx(), authorGCID, grant.ScopeTestSet, nil, 0)
	if err != nil {
		t.Fatalf("ListEntitled: %v", err)
	}
	if out[0].StemPreview != "short stem" {
		t.Fatalf("short stem must pass through unchanged; got %q", out[0].StemPreview)
	}
}

// ------------------------------------------------------ ActiveGrantAtomIDs

func TestGrantRepo_ActiveGrantAtomIDs_HappyPath(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{
				func(dest ...any) error { zqSetString(dest, 0, "atom-1"); return nil },
				func(dest ...any) error { zqSetString(dest, 0, "atom-2"); return nil },
			}}, nil
		},
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	ids, err := r.ActiveGrantAtomIDs(aCtx(), reuseGrantee)
	if err != nil {
		t.Fatalf("ActiveGrantAtomIDs: %v", err)
	}
	if len(ids) != 2 || ids[0] != "atom-1" || ids[1] != "atom-2" {
		t.Fatalf("ids = %v", ids)
	}
	// SQL shape + RLS ordering.
	if len(q.sqls) < 2 || !strings.Contains(q.sqls[0], "SET LOCAL chora.tenant_id") {
		t.Fatalf("first SQL must be SET LOCAL; got %#v", q.sqls)
	}
	sel := aLastSQL(q)
	if !strings.Contains(sel, "SELECT DISTINCT atom_id") || !strings.Contains(sel, "atom_usage_grants") ||
		!strings.Contains(sel, "status = 'active'") || !strings.Contains(sel, "expires_at") {
		t.Fatalf("active-grant SELECT malformed; got %q", sel)
	}
}

func TestGrantRepo_ActiveGrantAtomIDs_NilTxRunner(t *testing.T) {
	t.Parallel()
	if _, err := pg.NewGrantRepo(nil).ActiveGrantAtomIDs(aCtx(), reuseGrantee); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

func TestGrantRepo_ActiveGrantAtomIDs_EmptyGrantee(t *testing.T) {
	t.Parallel()
	r := pg.NewGrantRepo(&stubTxRunner{q: &stubQuerier{}})
	if _, err := r.ActiveGrantAtomIDs(aCtx(), "  "); err == nil {
		t.Fatalf("expected an error for a blank grantee")
	}
}

func TestGrantRepo_ActiveGrantAtomIDs_QueryError_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("db down") }
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	if _, err := r.ActiveGrantAtomIDs(aCtx(), reuseGrantee); err == nil {
		t.Fatalf("expected the query error to propagate")
	}
}

func TestGrantRepo_ActiveGrantAtomIDs_ScanError_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{
				func(dest ...any) error { return errors.New("scan boom") },
			}}, nil
		},
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	if _, err := r.ActiveGrantAtomIDs(aCtx(), reuseGrantee); err == nil {
		t.Fatalf("expected the scan error to propagate")
	}
}

func TestGrantRepo_ActiveGrantAtomIDs_RowsErr_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		return &stubRows{err: errors.New("cursor error"), scans: []func(dest ...any) error{
			func(dest ...any) error { zqSetString(dest, 0, "atom-1"); return nil },
		}}, nil
	}
	r := pg.NewGrantRepo(&stubTxRunner{q: q})
	if _, err := r.ActiveGrantAtomIDs(aCtx(), reuseGrantee); err == nil {
		t.Fatalf("expected the rows.Err() to propagate")
	}
}

func TestGrantRepo_ActiveGrantAtomIDs_MissingTenant_FailsLoud(t *testing.T) {
	t.Parallel()
	r := pg.NewGrantRepo(&stubTxRunner{q: &stubQuerier{}})
	if _, err := r.ActiveGrantAtomIDs(context.Background(), reuseGrantee); err == nil {
		t.Fatalf("expected ErrNoTenantContext on a bare context")
	}
}