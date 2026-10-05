// authorize_branches_test.go — statement coverage for the deep branches of
// ShareAtom (§7.1: guardrail screening, revision pinning, license matrix),
// AuthorizeAtomUse (§7.3: share-source resolutions, snapshots, status
// mapping), and settleRoyalty (§3.3 fail-soft legs). Reuses the hand-written
// fakes + seed helpers from the sibling tests.
package grpcadapter

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sharingv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/sharing/v1"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/adapter/modelarmor"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
	"github.com/apollo-chora/chora-sharing/internal/domain/currency"
	"github.com/apollo-chora/chora-sharing/internal/domain/grant"
)

// -----------------------------------------------------------------------------
// Fakes specific to this file
// -----------------------------------------------------------------------------

// fakeGuardrail is a configurable modelarmor.GuardrailPort.
type fakeGuardrail struct {
	verdict   modelarmor.ScreenVerdict
	err       error
	screened  int
	gotTenant string
	gotAuthor string
	gotAgent  string
}

func (f *fakeGuardrail) Screen(ctx context.Context, req modelarmor.ScreenRequest) (modelarmor.ScreenVerdict, error) {
	f.screened++
	f.gotTenant, f.gotAuthor, f.gotAgent = req.TenantID, req.AuthorGCID, req.AgentID
	if f.err != nil {
		return modelarmor.ScreenVerdict{}, f.err
	}
	return f.verdict, nil
}

// errProjectionRepo wraps fakeProjectionRepo so Projections.Get can fail with
// arbitrary (non-ErrNotFound) errors.
type errProjectionRepo struct {
	*fakeProjectionRepo
	err error
}

func (f *errProjectionRepo) Get(ctx context.Context, atomID string) (atom_projection.Projection, error) {
	if f.err != nil {
		return atom_projection.Projection{}, f.err
	}
	return f.fakeProjectionRepo.Get(ctx, atomID)
}

// errShareRepo wraps fakeShareRepo so GetShare can fail without a NotFound.
type errShareRepo struct {
	*fakeShareRepo
	err error
}

func (f *errShareRepo) GetShare(ctx context.Context, feedEntryID string) (*atom_share.Share, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.fakeShareRepo.GetShare(ctx, feedEntryID)
}

// failingGrantRepo wraps fakeGrantRepo so Authorize can fail.
type failingGrantRepo struct {
	*fakeGrantRepo
	authorizeErr error
}

func (f *failingGrantRepo) Authorize(ctx context.Context, g *grant.AtomUsageGrant) (*grant.AtomUsageGrant, error) {
	if f.authorizeErr != nil {
		return nil, f.authorizeErr
	}
	return f.fakeGrantRepo.Authorize(ctx, g)
}

// statusGrantRepo wraps fakeGrantRepo so the Authorize result carries a
// caller-chosen status (for status-snapshot proto mapping).
type statusGrantRepo struct {
	*fakeGrantRepo
	status grant.Status
}

func (f *statusGrantRepo) Authorize(ctx context.Context, g *grant.AtomUsageGrant) (*grant.AtomUsageGrant, error) {
	g.Status = f.status
	return g, nil
}

// failingRoyaltyRepo delegates to the inmem repo but can fail Record.
type failingRoyaltyRepo struct {
	roy        *inmem.RoyaltyRepo
	failRecord bool
	recordErr  error
}

func (f *failingRoyaltyRepo) Record(ctx context.Context, s *grant.RoyaltySettlement) error {
	if f.failRecord {
		return f.recordErr
	}
	return f.roy.Record(ctx, s)
}

func (f *failingRoyaltyRepo) Exists(ctx context.Context, sourceEventID string) (bool, error) {
	return f.roy.Exists(ctx, sourceEventID)
}

// errManaDebiter / errCurrencyStore / errRoyaltyPublisher wrap the shared
// recording fakes so each fail-soft leg of settleRoyalty can be driven to its
// log-and-continue branch.
type errManaDebiter struct {
	*fakeManaDebiter
	err error
}

func (f *errManaDebiter) DebitReuser(_ context.Context, gcid, tenantID string, amount float64, code string) error {
	if f.err != nil {
		return f.err
	}
	return f.fakeManaDebiter.DebitReuser(context.Background(), gcid, tenantID, amount, code)
}

type errCurrencyStore struct {
	*fakeCurrencyStore
	err error
}

func (f *errCurrencyStore) CreditAuthor(_ context.Context, tenantID, gcid string, c currency.Currency, amount float64) error {
	if f.err != nil {
		return f.err
	}
	return f.fakeCurrencyStore.CreditAuthor(context.Background(), tenantID, gcid, c, amount)
}

type errRoyaltyPublisher struct {
	*fakeRoyaltyPublisher
	err error
}

func (f *errRoyaltyPublisher) PublishRoyaltySettled(_ context.Context,
	settlementID, grantID, ownerGCID, granteeTenantID, atomID string,
	amount float64,
	curr, usageContext, sourceEventID, tenantID string,
) error {
	if f.err != nil {
		return f.err
	}
	return f.fakeRoyaltyPublisher.PublishRoyaltySettled(context.Background(),
		settlementID, grantID, ownerGCID, granteeTenantID, atomID,
		amount, curr, usageContext, sourceEventID, tenantID)
}

const (
	abOwner  = "01970000-0000-7000-9000-00000000ab01"
	abOther  = "01970000-0000-7000-9000-00000000ab02"
	abAtom   = "01970000-0000-7000-8000-00000000ab03"
	abTenant = "01970000-0000-7000-8000-00000000ab04"
	abRev    = "01970000-0000-7000-8000-00000000ab05"
)

// abProjection returns a seeded projection with a question (revision + stem)
// owned by abOwner, so ShareAtom/AuthorizeAtomUse pass the CHO-2174b gates.
func abProjection(reuseVisibility string) *fakeProjectionRepo {
	projections := newFakeProjectionRepo()
	projections.seed(atom_projection.Projection{
		AtomID:            abAtom,
		RevisionID:        abRev,
		OwnerGCID:         abOwner,
		AuthorDisplayName: "Ada",
		Stem:              "What is 2+2?",
		QuestionType:      atom_projection.QuestionTypeMCQ,
		PublishedAt:       time.Now().UTC(),
		ReuseVisibility:   reuseVisibility,
	})
	return projections
}

// -----------------------------------------------------------------------------
// ShareAtom — guardrail + branches
// -----------------------------------------------------------------------------

func TestShareAtom_InvalidInput(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Shares: newFakeShareRepo(), Projections: newFakeProjectionRepo()})
	for name, req := range map[string]*sharingv1.ShareAtomRequest{
		"nil request":      nil,
		"all missing":      {},
		"no idempotency":   {AuthorGcid: "a", TenantId: "t", AtomId: "x", LicenseTerms: sharingv1.LicenseTerms_LICENSE_TERMS_FREE},
		"no atom":          {AuthorGcid: "a", TenantId: "t", IdempotencyKey: "k", LicenseTerms: sharingv1.LicenseTerms_LICENSE_TERMS_FREE},
		"no tenant":        {AuthorGcid: "a", AtomId: "x", IdempotencyKey: "k", LicenseTerms: sharingv1.LicenseTerms_LICENSE_TERMS_FREE},
		"no author":        {TenantId: "t", AtomId: "x", IdempotencyKey: "k", LicenseTerms: sharingv1.LicenseTerms_LICENSE_TERMS_FREE},
	} {
		if _, err := srv.ShareAtom(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: expected InvalidArgument, got %v", name, err)
		}
	}
}

func TestShareAtom_Guardrail_ScreenError_Internal(t *testing.T) {
	t.Parallel()
	srv := New(Deps{
		Shares:      newFakeShareRepo(),
		Projections: abProjection("tenant"),
		Guardrail:   &fakeGuardrail{err: errors.New("armor down")},
		Rules:       testRules(),
	})
	_, err := srv.ShareAtom(context.Background(), &sharingv1.ShareAtomRequest{
		AuthorGcid:     abOwner,
		TenantId:       abTenant,
		AtomId:         abAtom,
		Caption:        "hello caption",
		LicenseTerms:   sharingv1.LicenseTerms_LICENSE_TERMS_FREE,
		IdempotencyKey: "idem-guard-err",
	})
	statusIs(t, err, codes.Internal)
}

func TestShareAtom_Guardrail_Blocked_FailedPrecondition(t *testing.T) {
	t.Parallel()
	shares := newFakeShareRepo()
	srv := New(Deps{
		Shares:      shares,
		Projections: abProjection("tenant"),
		Guardrail:   &fakeGuardrail{verdict: modelarmor.ScreenVerdict{Verdict: modelarmor.VerdictBlocked}},
		Rules:       testRules(),
	})
	_, err := srv.ShareAtom(context.Background(), &sharingv1.ShareAtomRequest{
		AuthorGcid:     abOwner,
		TenantId:       abTenant,
		AtomId:         abAtom,
		Caption:        "bad caption",
		LicenseTerms:   sharingv1.LicenseTerms_LICENSE_TERMS_FREE,
		IdempotencyKey: "idem-guard-block",
	})
	statusIs(t, err, codes.FailedPrecondition)
	if len(shares.byID) != 0 {
		t.Error("blocked share must never persist")
	}
}

func TestShareAtom_Guardrail_Approved_Proceeds(t *testing.T) {
	t.Parallel()
	guard := &fakeGuardrail{verdict: modelarmor.ScreenVerdict{Verdict: modelarmor.VerdictApproved}}
	shares := newFakeShareRepo()
	srv := New(Deps{
		Shares:      shares,
		Projections: abProjection("tenant"),
		Guardrail:   guard,
		Rules:       testRules(),
	})
	resp, err := srv.ShareAtom(context.Background(), &sharingv1.ShareAtomRequest{
		AuthorGcid:     abOwner,
		TenantId:       abTenant,
		AtomId:         abAtom,
		Caption:        "clean caption",
		LicenseTerms:   sharingv1.LicenseTerms_LICENSE_TERMS_FREE,
		IdempotencyKey: "idem-guard-ok",
	})
	if err != nil {
		t.Fatalf("ShareAtom: %v", err)
	}
	if resp.GetShareEntryId() == "" {
		t.Error("share_entry_id empty")
	}
	if guard.screened != 1 {
		t.Errorf("guardrail screened %d times, want 1", guard.screened)
	}
	if guard.gotTenant != abTenant || guard.gotAuthor != abOwner || guard.gotAgent != modelarmor.AgentIDSocialModeration {
		t.Errorf("guardrail args mismatch: tenant=%q author=%q agent=%q", guard.gotTenant, guard.gotAuthor, guard.gotAgent)
	}
	if len(shares.byID) != 1 {
		t.Errorf("expected 1 share persisted, got %d", len(shares.byID))
	}
}

func TestShareAtom_ExplicitRevision_Wins(t *testing.T) {
	t.Parallel()
	srv := New(Deps{
		Shares:      newFakeShareRepo(),
		Projections: abProjection("tenant"),
		Rules:       testRules(),
	})
	resp, err := srv.ShareAtom(context.Background(), &sharingv1.ShareAtomRequest{
		AuthorGcid:     abOwner,
		TenantId:       abTenant,
		AtomId:         abAtom,
		AtomRevisionId: "pinned-rev-999", // explicit wins over projection's latest
		LicenseTerms:   sharingv1.LicenseTerms_LICENSE_TERMS_FREE,
		IdempotencyKey: "idem-explicit-rev",
	})
	if err != nil {
		t.Fatalf("ShareAtom: %v", err)
	}
	if resp.GetShareEntryId() == "" {
		t.Error("share_entry_id empty")
	}
}

func TestShareAtom_ProjectionError_Internal(t *testing.T) {
	t.Parallel()
	srv := New(Deps{
		Shares:      newFakeShareRepo(),
		Projections: &errProjectionRepo{fakeProjectionRepo: newFakeProjectionRepo(), err: errors.New("db down")},
		Rules:       testRules(),
	})
	_, err := srv.ShareAtom(context.Background(), &sharingv1.ShareAtomRequest{
		AuthorGcid:     abOwner,
		TenantId:       abTenant,
		AtomId:         abAtom,
		LicenseTerms:   sharingv1.LicenseTerms_LICENSE_TERMS_FREE,
		IdempotencyKey: "idem-proj-err",
	})
	statusIs(t, err, codes.Internal)
}

func TestShareAtom_CaptionTooLong_InvalidArgument(t *testing.T) {
	t.Parallel()
	srv := New(Deps{
		Shares:      newFakeShareRepo(),
		Projections: abProjection("tenant"),
		Rules:       testRules(),
	})
	_, err := srv.ShareAtom(context.Background(), &sharingv1.ShareAtomRequest{
		AuthorGcid:     abOwner,
		TenantId:       abTenant,
		AtomId:         abAtom,
		Caption:        strings.Repeat("x", 600), // > 512 → R2/NewShare guard
		LicenseTerms:   sharingv1.LicenseTerms_LICENSE_TERMS_FREE,
		IdempotencyKey: "idem-long-cap",
	})
	statusIs(t, err, codes.InvalidArgument)
}

// TestShareAtom_LicenseMatrix drives every protoLicenseToDomain branch through
// the RPC: free, royalty_pct, royalty_fixed, cc_by_sa, cc_nd (each gets its
// own idempotency key so the deterministic feed-entry dedup never collides).
func TestShareAtom_LicenseMatrix(t *testing.T) {
	t.Parallel()
	licenses := []struct {
		proto sharingv1.LicenseTerms
		rate  *sharingv1.RoyaltyRate
	}{
		{sharingv1.LicenseTerms_LICENSE_TERMS_FREE, nil},
		{sharingv1.LicenseTerms_LICENSE_TERMS_ROYALTY_PCT, &sharingv1.RoyaltyRate{Kind: "pct", Value: 0.1}},
		{sharingv1.LicenseTerms_LICENSE_TERMS_ROYALTY_FIXED, &sharingv1.RoyaltyRate{Kind: "fixed_per_use", Value: 5}},
		{sharingv1.LicenseTerms_LICENSE_TERMS_CC_BY_SA, nil},
		{sharingv1.LicenseTerms_LICENSE_TERMS_CC_ND, nil},
	}
	srv := New(Deps{
		Shares:      newFakeShareRepo(),
		Projections: abProjection("tenant"),
		Rules:       testRules(),
	})
	for i, tc := range licenses {
		_, err := srv.ShareAtom(context.Background(), &sharingv1.ShareAtomRequest{
			AuthorGcid:     abOwner,
			TenantId:       abTenant,
			AtomId:         abAtom,
			LicenseTerms:   tc.proto,
			RoyaltyRate:    tc.rate,
			IdempotencyKey: "idem-lic-" + string(rune('0'+i)),
		})
		if err != nil {
			t.Errorf("license %v: ShareAtom: %v", tc.proto, err)
		}
	}
}

// -----------------------------------------------------------------------------
// AuthorizeAtomUse — scope, share-source, snapshot + status branches
// -----------------------------------------------------------------------------

func TestAuthorizeAtomUse_InvalidInput(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Grants: newFakeGrantRepo(), Projections: newFakeProjectionRepo()})
	for name, req := range map[string]*sharingv1.AuthorizeAtomUseRequest{
		"nil request":      nil,
		"all missing":      {},
		"no idempotency":   {GranteeGcid: "g", TenantId: "t", AtomId: "a", Scope: sharingv1.GrantScope_GRANT_SCOPE_DUEL},
		"no atom":          {GranteeGcid: "g", TenantId: "t", IdempotencyKey: "k", Scope: sharingv1.GrantScope_GRANT_SCOPE_DUEL},
		"no tenant":        {GranteeGcid: "g", AtomId: "a", IdempotencyKey: "k", Scope: sharingv1.GrantScope_GRANT_SCOPE_DUEL},
		"no grantee":       {TenantId: "t", AtomId: "a", IdempotencyKey: "k", Scope: sharingv1.GrantScope_GRANT_SCOPE_DUEL},
		"scope unspecified": {GranteeGcid: "g", TenantId: "t", AtomId: "a", IdempotencyKey: "k"},
	} {
		if _, err := srv.AuthorizeAtomUse(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: expected InvalidArgument, got %v", name, err)
		}
	}
}

func TestAuthorizeAtomUse_ProjectionError_Internal(t *testing.T) {
	t.Parallel()
	srv := New(Deps{
		Grants:      newFakeGrantRepo(),
		Projections: &errProjectionRepo{fakeProjectionRepo: newFakeProjectionRepo(), err: errors.New("db down")},
	})
	_, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:    abOwner,
		TenantId:       abTenant,
		AtomId:         abAtom,
		Scope:          sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		IdempotencyKey: "idem-proj-err-a",
	})
	statusIs(t, err, codes.Internal)
}

func TestAuthorizeAtomUse_SourceShare_NotFound_412(t *testing.T) {
	t.Parallel()
	srv := New(Deps{
		Grants:      newFakeGrantRepo(),
		Projections: abProjection("private"),
		Shares:      newFakeShareRepo(), // empty → GetShare ErrNotFound
	})
	_, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:      abOther,
		TenantId:         abTenant,
		AtomId:           abAtom,
		Scope:            sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		SourceShareEntry: "missing-share",
		IdempotencyKey:   "idem-share-404",
	})
	statusIs(t, err, codes.FailedPrecondition)
}

func TestAuthorizeAtomUse_SourceShare_RepoError_Internal(t *testing.T) {
	t.Parallel()
	srv := New(Deps{
		Grants:      newFakeGrantRepo(),
		Projections: abProjection("private"),
		Shares:      &errShareRepo{fakeShareRepo: newFakeShareRepo(), err: errors.New("db down")},
	})
	_, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:      abOther,
		TenantId:         abTenant,
		AtomId:           abAtom,
		Scope:            sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		SourceShareEntry: "share-x",
		IdempotencyKey:   "idem-share-err",
	})
	statusIs(t, err, codes.Internal)
}

func TestAuthorizeAtomUse_SourceShare_FreeLicenseFromShare(t *testing.T) {
	t.Parallel()
	shares := newFakeShareRepo()
	ctx := context.Background()
	_ = shares.SaveShare(ctx, &atom_share.Share{
		FeedEntryID: "share-free", TenantID: abTenant, AtomID: abAtom,
		RevisionID: abRev, OwnerGCID: abOwner,
		License: atom_share.LicenseFree, CreatedAt: time.Now().UTC(),
	})
	srv := New(Deps{
		Grants:      newFakeGrantRepo(),
		Projections: abProjection("private"), // private atom, but the share resolves
		Shares:      shares,
	})
	resp, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:      abOther,
		TenantId:         abTenant,
		AtomId:           abAtom,
		Scope:            sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		SourceShareEntry: "share-free",
		IdempotencyKey:   "idem-share-free",
	})
	if err != nil {
		t.Fatalf("AuthorizeAtomUse: %v", err)
	}
	if resp.GetLicenseTermsSnapshot() != sharingv1.LicenseTerms_LICENSE_TERMS_FREE {
		t.Errorf("license=%v want FREE (resolved from the share)", resp.GetLicenseTermsSnapshot())
	}
	if resp.GetStatus() != sharingv1.GrantStatus_GRANT_STATUS_ACTIVE {
		t.Errorf("status=%v want ACTIVE", resp.GetStatus())
	}
}

func TestAuthorizeAtomUse_SourceShare_InvalidLicense_InvalidArgument(t *testing.T) {
	t.Parallel()
	// A share carrying an invalid license term ("" — impossible via ShareAtom,
	// but corrupt rows exist) trips grant.NewGrant's guard → InvalidArgument.
	shares := newFakeShareRepo()
	ctx := context.Background()
	_ = shares.SaveShare(ctx, &atom_share.Share{
		FeedEntryID: "share-bad", TenantID: abTenant, AtomID: abAtom,
		RevisionID: abRev, OwnerGCID: abOwner,
		License: "", CreatedAt: time.Now().UTC(),
	})
	srv := New(Deps{
		Grants:      newFakeGrantRepo(),
		Projections: abProjection("private"),
		Shares:      shares,
	})
	_, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:      abOther,
		TenantId:         abTenant,
		AtomId:           abAtom,
		Scope:            sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		SourceShareEntry: "share-bad",
		IdempotencyKey:   "idem-share-bad",
	})
	statusIs(t, err, codes.InvalidArgument)
}

func TestAuthorizeAtomUse_AuthorizeError_Internal(t *testing.T) {
	t.Parallel()
	srv := New(Deps{
		Grants:      &failingGrantRepo{fakeGrantRepo: newFakeGrantRepo(), authorizeErr: errors.New("db down")},
		Projections: abProjection("tenant"),
	})
	_, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:    abOwner,
		TenantId:       abTenant,
		AtomId:         abAtom,
		Scope:          sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		IdempotencyKey: "idem-auth-err",
	})
	statusIs(t, err, codes.Internal)
}

// TestAuthorizeAtomUse_ScopeMatrix drives every protoGrantScopeToDomain branch
// through AuthorizeAtomUse (own-atom: scope is stamped but license stays free).
func TestAuthorizeAtomUse_ScopeMatrix(t *testing.T) {
	t.Parallel()
	scopes := []sharingv1.GrantScope{
		sharingv1.GrantScope_GRANT_SCOPE_TEST_SET,
		sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		sharingv1.GrantScope_GRANT_SCOPE_LIVE_QUIZ,
		sharingv1.GrantScope_GRANT_SCOPE_COLLECTION,
		sharingv1.GrantScope_GRANT_SCOPE_UNLIMITED,
	}
	srv := New(Deps{
		Grants:      newFakeGrantRepo(),
		Projections: abProjection("tenant"),
	})
	for _, sc := range scopes {
		resp, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
			GranteeGcid:    abOwner, // own-atom — consent passes for any scope
			TenantId:       abTenant,
			AtomId:         abAtom,
			Scope:          sc,
			IdempotencyKey: "idem-scope-" + string(rune(0x30+int(sc))),
		})
		if err != nil {
			t.Errorf("scope %v: AuthorizeAtomUse: %v", sc, err)
			continue
		}
		if resp.GetLicenseTermsSnapshot() != sharingv1.LicenseTerms_LICENSE_TERMS_FREE {
			t.Errorf("scope %v: license=%v want FREE (own-atom)", sc, resp.GetLicenseTermsSnapshot())
		}
	}
}

// TestAuthorizeAtomUse_StatusSnapshot drives domainGrantStatusToProto for the
// REVOKED + EXPIRED statuses the shared fake never returns.
func TestAuthorizeAtomUse_StatusSnapshot(t *testing.T) {
	t.Parallel()
	for _, st := range []grant.Status{grant.StatusRevoked, grant.StatusExpired} {
		srv := New(Deps{
			Grants:      &statusGrantRepo{fakeGrantRepo: newFakeGrantRepo(), status: st},
			Projections: abProjection("tenant"),
		})
		resp, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
			GranteeGcid:    abOwner,
			TenantId:       abTenant,
			AtomId:         abAtom,
			Scope:          sharingv1.GrantScope_GRANT_SCOPE_DUEL,
			IdempotencyKey: "idem-status-" + string(st),
		})
		if err != nil {
			t.Fatalf("status %q: AuthorizeAtomUse: %v", st, err)
		}
		wantProto := sharingv1.GrantStatus_GRANT_STATUS_UNSPECIFIED
		if st == grant.StatusRevoked {
			wantProto = sharingv1.GrantStatus_GRANT_STATUS_REVOKED
		} else {
			wantProto = sharingv1.GrantStatus_GRANT_STATUS_EXPIRED
		}
		if resp.GetStatus() != wantProto {
			t.Errorf("status %q: got %v want %v", st, resp.GetStatus(), wantProto)
		}
	}
}

// -----------------------------------------------------------------------------
// settleRoyalty — zero-amount + fail-soft legs
// -----------------------------------------------------------------------------

// TestSettleRoyalty_ZeroAmountNoop — a royalty_pct license at rate 0 settles
// amount 0 → the whole debit/credit/publish pipeline is skipped.
func TestSettleRoyalty_ZeroAmountNoop(t *testing.T) {
	t.Parallel()
	deps, royalties, mana, curr, pub := royaltyTestDeps(t)
	_, grantee, atomID, tenantID, feedEntry := seedRoyaltyShare(deps,
		atom_share.LicenseRoyaltyPct, atom_share.RoyaltyRate{Kind: "pct", Value: 0})

	srv := New(deps)
	_, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:      grantee,
		TenantId:         tenantID,
		AtomId:           atomID,
		Scope:            sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		IdempotencyKey:   "zero-amount-001",
		SourceShareEntry: feedEntry,
	})
	if err != nil {
		t.Fatalf("AuthorizeAtomUse: %v", err)
	}
	if exists, _ := royalties.Exists(context.Background(), "zero-amount-001"); exists {
		t.Error("expected NO settlement record for zero amount")
	}
	mana.mu.Lock()
	defer mana.mu.Unlock()
	if len(mana.debits) != 0 {
		t.Errorf("expected 0 mana debits, got %d", len(mana.debits))
	}
	curr.mu.Lock()
	defer curr.mu.Unlock()
	if len(curr.credits) != 0 {
		t.Errorf("expected 0 author credits, got %d", len(curr.credits))
	}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.pubs) != 0 {
		t.Errorf("expected 0 publishes, got %d", len(pub.pubs))
	}
}

// TestSettleRoyalty_RecordFailure_FailSoft — a Record failure (not the
// idempotent-replay sentinel) logs and returns: the grant still stands.
func TestSettleRoyalty_RecordFailure_FailSoft(t *testing.T) {
	t.Parallel()
	deps, _, _, _, _ := royaltyTestDeps(t)
	deps.Royalties = &failingRoyaltyRepo{
		roy:        inmem.NewRoyaltyRepo(),
		failRecord: true,
		recordErr:  errors.New("db down"),
	}
	_, grantee, atomID, tenantID, feedEntry := seedRoyaltyShare(deps,
		atom_share.LicenseRoyaltyPct, atom_share.RoyaltyRate{Kind: "pct", Value: 0.1})

	srv := New(deps)
	resp, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:      grantee,
		TenantId:         tenantID,
		AtomId:           atomID,
		Scope:            sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		IdempotencyKey:   "record-fail-001",
		SourceShareEntry: feedEntry,
	})
	if err != nil {
		t.Fatalf("AuthorizeAtomUse must succeed despite record failure: %v", err)
	}
	if resp.GetGrantId() == "" {
		t.Error("grant_id empty")
	}
}

// TestSettleRoyalty_NoManaNoCurrencyNoEvents — when the optional spend/accrue/
// publish deps are all nil, settlement still records but skips every optional
// leg (the dev-wiring shape).
func TestSettleRoyalty_NoManaNoCurrencyNoEvents(t *testing.T) {
	t.Parallel()
	deps, royalties, _, _, _ := royaltyTestDeps(t)
	deps.Mana = nil
	deps.Currency = nil
	deps.RoyaltyEvents = nil
	_, grantee, atomID, tenantID, feedEntry := seedRoyaltyShare(deps,
		atom_share.LicenseRoyaltyFixed, atom_share.RoyaltyRate{Kind: "fixed_per_use", Value: 5})

	srv := New(deps)
	_, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:      grantee,
		TenantId:         tenantID,
		AtomId:           atomID,
		Scope:            sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		IdempotencyKey:   "nil-legs-001",
		SourceShareEntry: feedEntry,
	})
	if err != nil {
		t.Fatalf("AuthorizeAtomUse: %v", err)
	}
	if exists, _ := royalties.Exists(context.Background(), "nil-legs-001"); !exists {
		t.Error("expected settlement record even with optional legs nil")
	}
}

// TestSettleRoyalty_ManaDebitFailure_FailSoft — a mana-debit failure logs and
// continues (credit + publish still fire).
func TestSettleRoyalty_ManaDebitFailure_FailSoft(t *testing.T) {
	t.Parallel()
	deps, _, _, curr, pub := royaltyTestDeps(t)
	deps.Mana = &errManaDebiter{fakeManaDebiter: &fakeManaDebiter{}, err: errors.New("identity down")}
	_, grantee, atomID, tenantID, feedEntry := seedRoyaltyShare(deps,
		atom_share.LicenseRoyaltyPct, atom_share.RoyaltyRate{Kind: "pct", Value: 0.1})

	srv := New(deps)
	if _, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:      grantee,
		TenantId:         tenantID,
		AtomId:           atomID,
		Scope:            sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		IdempotencyKey:   "mana-fail-001",
		SourceShareEntry: feedEntry,
	}); err != nil {
		t.Fatalf("AuthorizeAtomUse must succeed despite mana failure: %v", err)
	}
	curr.mu.Lock()
	defer curr.mu.Unlock()
	if len(curr.credits) != 1 {
		t.Errorf("expected 1 author credit (still fires after debit failure), got %d", len(curr.credits))
	}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.pubs) != 1 {
		t.Errorf("expected 1 publish, got %d", len(pub.pubs))
	}
}

// TestSettleRoyalty_CurrencyCreditFailure_FailSoft — the author-credit failure
// logs and continues (publish still fires).
func TestSettleRoyalty_CurrencyCreditFailure_FailSoft(t *testing.T) {
	t.Parallel()
	deps, _, mana, _, pub := royaltyTestDeps(t)
	deps.Currency = &errCurrencyStore{fakeCurrencyStore: &fakeCurrencyStore{}, err: errors.New("ledger down")}
	_, grantee, atomID, tenantID, feedEntry := seedRoyaltyShare(deps,
		atom_share.LicenseRoyaltyPct, atom_share.RoyaltyRate{Kind: "pct", Value: 0.1})

	srv := New(deps)
	if _, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:      grantee,
		TenantId:         tenantID,
		AtomId:           atomID,
		Scope:            sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		IdempotencyKey:   "credit-fail-001",
		SourceShareEntry: feedEntry,
	}); err != nil {
		t.Fatalf("AuthorizeAtomUse must succeed despite credit failure: %v", err)
	}
	mana.mu.Lock()
	defer mana.mu.Unlock()
	if len(mana.debits) != 1 {
		t.Errorf("expected 1 mana debit, got %d", len(mana.debits))
	}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.pubs) != 1 {
		t.Errorf("expected 1 publish, got %d", len(pub.pubs))
	}
}

// TestSettleRoyalty_PublishFailure_FailSoft — the event-publish failure logs
// but the RPC still returns the grant (best-effort notification).
func TestSettleRoyalty_PublishFailure_FailSoft(t *testing.T) {
	t.Parallel()
	deps, _, mana, curr, _ := royaltyTestDeps(t)
	deps.RoyaltyEvents = &errRoyaltyPublisher{fakeRoyaltyPublisher: &fakeRoyaltyPublisher{}, err: errors.New("bus down")}
	_, grantee, atomID, tenantID, feedEntry := seedRoyaltyShare(deps,
		atom_share.LicenseRoyaltyFixed, atom_share.RoyaltyRate{Kind: "fixed_per_use", Value: 5})

	srv := New(deps)
	resp, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:      grantee,
		TenantId:         tenantID,
		AtomId:           atomID,
		Scope:            sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		IdempotencyKey:   "pub-fail-001",
		SourceShareEntry: feedEntry,
	})
	if err != nil {
		t.Fatalf("AuthorizeAtomUse must succeed despite publish failure: %v", err)
	}
	if resp.GetGrantId() == "" {
		t.Error("grant_id empty")
	}
	mana.mu.Lock()
	defer mana.mu.Unlock()
	if len(mana.debits) != 1 {
		t.Errorf("expected 1 mana debit, got %d", len(mana.debits))
	}
	curr.mu.Lock()
	defer curr.mu.Unlock()
	if len(curr.credits) != 1 {
		t.Errorf("expected 1 author credit, got %d", len(curr.credits))
	}
}