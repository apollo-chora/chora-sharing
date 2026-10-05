// Tests for the greenfield SharingServer. Uses hand-written
// fakes for the domain ports — no mocks, no external test doubles. Each fake
// implements the exact port interface so the assertions exercise real domain
// behaviour (NewShare validation).
package grpcadapter

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	sharingv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/sharing/v1"

	"github.com/apollo-chora/chora-sharing/internal/config"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
	"github.com/apollo-chora/chora-sharing/internal/domain/grant"
)

// =============================================================================
// Fakes
// =============================================================================

// fakeProjectionRepo is an in-memory AtomProjectionReader.
type fakeProjectionRepo struct {
	mu   sync.Mutex
	byID map[string]atom_projection.Projection
}

func newFakeProjectionRepo() *fakeProjectionRepo {
	return &fakeProjectionRepo{byID: make(map[string]atom_projection.Projection)}
}

func (f *fakeProjectionRepo) seed(p atom_projection.Projection) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byID[p.AtomID] = p
}

func (f *fakeProjectionRepo) Get(ctx context.Context, atomID string) (atom_projection.Projection, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.byID[atomID]
	if !ok {
		return atom_projection.Projection{}, atom_projection.ErrNotFound
	}
	return p, nil
}

func (f *fakeProjectionRepo) ListRandom(ctx context.Context, tenantID string, excludeGCIDs []string, limit int) ([]atom_projection.Projection, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	exclude := make(map[string]struct{}, len(excludeGCIDs))
	for _, g := range excludeGCIDs {
		exclude[g] = struct{}{}
	}
	out := make([]atom_projection.Projection, 0, limit)
	for _, p := range f.byID {
		if _, skip := exclude[p.OwnerGCID]; skip {
			continue
		}
		out = append(out, p)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// fakeShareRepo is an in-memory atom_share.ShareRepo.
type fakeShareRepo struct {
	mu     sync.Mutex
	byID   map[string]*atom_share.Share
	listed []atom_share.Share
}

func newFakeShareRepo() *fakeShareRepo {
	return &fakeShareRepo{byID: make(map[string]*atom_share.Share)}
}

func (f *fakeShareRepo) SaveShare(ctx context.Context, s *atom_share.Share) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byID[s.FeedEntryID] = s
	return nil
}

func (f *fakeShareRepo) GetShare(ctx context.Context, feedEntryID string) (*atom_share.Share, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.byID[feedEntryID]
	if !ok {
		return nil, atom_share.ErrNotFound
	}
	return s, nil
}

// GetShareByAtom loads the latest visible (non-revoked) share for an atom by
// its owner. Restores the atom_share.ShareRepo port conformance — the method
// was added to the port without updating this fake, which left the whole grpc
// test package UNCOMPILABLE on main (pre-existing; found during CHO-2174b).
func (f *fakeShareRepo) GetShareByAtom(ctx context.Context, atomID, ownerGCID string) (*atom_share.Share, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.byID {
		if s.AtomID == atomID && s.OwnerGCID == ownerGCID {
			return s, nil
		}
	}
	return nil, atom_share.ErrNotFound
}

func (f *fakeShareRepo) ListSharedAtoms(ctx context.Context, tenantID string, cursor string, limit int, topicFilter, questionTypeFilter string, scope string, followingGCIDs []string, blockedGCIDs []string) ([]atom_share.Share, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]atom_share.Share, 0, len(f.byID))
	for _, s := range f.byID {
		out = append(out, *s)
	}
	return out, "", nil
}

func (f *fakeShareRepo) AppendEvent(ctx context.Context, e *atom_share.ShareEvent) error { return nil }
func (f *fakeShareRepo) ListEvents(ctx context.Context, feedEntryID string) ([]atom_share.ShareEvent, error) {
	return nil, nil
}

// =============================================================================
// Test helpers
// =============================================================================

// testRules returns a valid SharingRules with documented defaults (no env
// needed). Mirrors LoadSharingRules defaults so the combo tiers + ELO K-factor
// are exercised.
func testRules() config.SharingRules {
	return config.SharingRules{
		LicenseVocab: map[string]struct{}{
			"free": {}, "royalty_pct": {}, "royalty_fixed": {},
			"cc_by_sa": {}, "cc_nd": {},
		},
		ELOBaseline:     1200,
		ELOKFactor:      32,
		ComboTiers:      []int{1, 2, 3, 5},
		RoyaltyBase:     10,
		RoyaltyCap:      0,
		RoyaltyCurrency: "reputation",
		ManaActionCode:  "atom_royalty",
		SeasonWindow:    30 * 24 * time.Hour,
		WebSocketGrace:  30 * time.Second,
	}
}

// statusIs reports whether err's gRPC code matches want.
func statusIs(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		if want != codes.OK {
			t.Fatalf("expected %s, got nil error", want)
		}
		return
	}
	got := status.Code(err)
	if got != want {
		t.Fatalf("expected gRPC code %s, got %s (err: %v)", want, got, err)
	}
}

// =============================================================================
// Tests
// =============================================================================

// TestShareAtom_NonAuthor_403 verifies R1: a caller who is NOT the atom's
// owner is rejected with PermissionDenied (403). The projection's OwnerGCID
// != author → must never persist.
func TestShareAtom_NonAuthor_403(t *testing.T) {
	t.Parallel()
	const (
		author    = "01970000-0000-7000-9000-000000000001"
		otherGcid = "01970000-0000-7000-9000-000000000099" // NOT the author
		atomID    = "01970000-0000-7000-9000-atom-0000001"
		tenantID  = "01970000-0000-7000-9000-tenant-aaaaa"
	)
	projections := newFakeProjectionRepo()
	projections.seed(atom_projection.Projection{
		AtomID:            atomID,
		RevisionID:        "rev-001",
		OwnerGCID:         author, // the real author
		AuthorDisplayName: "Ada Lovelace",
		Stem:              "What is 2+2?",
		QuestionType:      atom_projection.QuestionTypeMCQ,
		PublishedAt:       time.Now().UTC(),
	})

	srv := New(Deps{
		Shares:      newFakeShareRepo(),
		Projections: projections,
		Rules:       testRules(),
	})

	// otherGcid attempts to share author's atom → R1 violation.
	_, err := srv.ShareAtom(context.Background(), &sharingv1.ShareAtomRequest{
		AuthorGcid:     otherGcid, // NOT the owner
		TenantId:       tenantID,
		AtomId:         atomID,
		LicenseTerms:   sharingv1.LicenseTerms_LICENSE_TERMS_FREE,
		IdempotencyKey: "idem-001",
	})
	statusIs(t, err, codes.PermissionDenied)
}

// TestShareAtom_NotPublished_412 verifies §7.1 step 7: when the projection
// is missing (atom never published / withdrawn), the response is
// FailedPrecondition (412), NOT 403 (existence never leaks via R1).
func TestShareAtom_NotPublished_412(t *testing.T) {
	t.Parallel()
	srv := New(Deps{
		Shares:      newFakeShareRepo(),
		Projections: newFakeProjectionRepo(), // empty — no projection
		Rules:       testRules(),
	})
	_, err := srv.ShareAtom(context.Background(), &sharingv1.ShareAtomRequest{
		AuthorGcid:     "01970000-0000-7000-9000-000000000001",
		TenantId:       "01970000-0000-7000-9000-tenant-aaaaa",
		AtomId:         "01970000-0000-7000-9000-atom-unknown",
		LicenseTerms:   sharingv1.LicenseTerms_LICENSE_TERMS_FREE,
		IdempotencyKey: "idem-002",
	})
	statusIs(t, err, codes.FailedPrecondition)
}

// TestShareAtom_AuthorOK verifies the happy path: the real author shares
// their atom with a free license → 200 + share_entry_id populated.
func TestShareAtom_AuthorOK(t *testing.T) {
	t.Parallel()
	const (
		author   = "01970000-0000-7000-9000-000000000001"
		atomID   = "01970000-0000-7000-9000-atom-0000001"
		tenantID = "01970000-0000-7000-9000-tenant-aaaaa"
	)
	projections := newFakeProjectionRepo()
	projections.seed(atom_projection.Projection{
		AtomID:            atomID,
		RevisionID:        "rev-001",
		OwnerGCID:         author,
		AuthorDisplayName: "Ada Lovelace",
		Stem:              "What is 2+2?",
		QuestionType:      atom_projection.QuestionTypeMCQ,
		PublishedAt:       time.Now().UTC(),
	})
	shares := newFakeShareRepo()
	srv := New(Deps{
		Shares:      shares,
		Projections: projections,
		Rules:       testRules(),
	})

	resp, err := srv.ShareAtom(context.Background(), &sharingv1.ShareAtomRequest{
		AuthorGcid:     author,
		TenantId:       tenantID,
		AtomId:         atomID,
		LicenseTerms:   sharingv1.LicenseTerms_LICENSE_TERMS_FREE,
		IdempotencyKey: "idem-ok-001",
	})
	if err != nil {
		t.Fatalf("ShareAtom: %v", err)
	}
	if resp.GetShareEntryId() == "" {
		t.Error("share_entry_id empty")
	}
	if resp.GetAuthorDisplayName() != "Ada Lovelace" {
		t.Errorf("author_display_name=%q want %q", resp.GetAuthorDisplayName(), "Ada Lovelace")
	}
	// Verify the share was persisted.
	if len(shares.byID) != 1 {
		t.Fatalf("expected 1 share persisted, got %d", len(shares.byID))
	}
}

// TestShareAtom_RoyaltyLicenseRequiresRate verifies R2: a royalty_pct
// license WITHOUT a royalty_rate is rejected with InvalidArgument.
func TestShareAtom_RoyaltyLicenseRequiresRate(t *testing.T) {
	t.Parallel()
	const (
		author   = "01970000-0000-7000-9000-000000000001"
		atomID   = "01970000-0000-7000-9000-atom-0000001"
		tenantID = "01970000-0000-7000-9000-tenant-aaaaa"
	)
	projections := newFakeProjectionRepo()
	projections.seed(atom_projection.Projection{
		AtomID: atomID, RevisionID: "rev-001", OwnerGCID: author,
		PublishedAt: time.Now().UTC(),
	})
	srv := New(Deps{
		Shares:      newFakeShareRepo(),
		Projections: projections,
		Rules:       testRules(),
	})
	// royalty_pct with NO rate → R2 violation.
	_, err := srv.ShareAtom(context.Background(), &sharingv1.ShareAtomRequest{
		AuthorGcid:     author,
		TenantId:       tenantID,
		AtomId:         atomID,
		LicenseTerms:   sharingv1.LicenseTerms_LICENSE_TERMS_ROYALTY_PCT,
		IdempotencyKey: "idem-roy-001",
	})
	statusIs(t, err, codes.InvalidArgument)
}

// TestFailLoud_NilDeps verifies the fail-loud pattern (§1.1): a SharingServer
// with nil deps for a given RPC returns codes.Unimplemented, never a fake
// success. Each RPC that needs a dep must refuse loudly.
func TestFailLoud_NilDeps(t *testing.T) {
	t.Parallel()
	// Completely empty Deps — every port nil.
	srv := New(Deps{})

	t.Run("ShareAtom_nilShares", func(t *testing.T) {
		_, err := srv.ShareAtom(context.Background(), &sharingv1.ShareAtomRequest{
			AuthorGcid: "a", TenantId: "t", AtomId: "x",
			LicenseTerms:   sharingv1.LicenseTerms_LICENSE_TERMS_FREE,
			IdempotencyKey: "k",
		})
		statusIs(t, err, codes.Unimplemented)
	})

	t.Run("AuthorizeAtomUse_nilGrants", func(t *testing.T) {
		_, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
			GranteeGcid: "g", TenantId: "t", AtomId: "a",
			Scope:          sharingv1.GrantScope_GRANT_SCOPE_DUEL,
			IdempotencyKey: "k",
		})
		statusIs(t, err, codes.Unimplemented)
	})

	t.Run("ListSharedAtoms_nilShares", func(t *testing.T) {
		_, err := srv.ListSharedAtoms(context.Background(), &sharingv1.ListSharedAtomsRequest{
			TenantId: "t",
		})
		statusIs(t, err, codes.Unimplemented)
	})

	t.Run("GenerateQuizFromTopic_nilQuizGen", func(t *testing.T) {
		_, err := srv.GenerateQuizFromTopic(context.Background(), &sharingv1.GenerateQuizFromTopicRequest{
			InstructorGcid: "i", TenantId: "t", Topic: "math",
		})
		statusIs(t, err, codes.Unimplemented)
	})

	t.Run("Follow_nilGraph", func(t *testing.T) {
		_, err := srv.Follow(context.Background(), &sharingv1.FollowRequest{
			FollowerGcid: "a", FolloweeGcid: "b", TenantId: "t",
		})
		statusIs(t, err, codes.Unimplemented)
	})
}

// TestAuthorizeAtomUse_OwnAtom_FreeLicense verifies §7.3 step 3: when
// grantee == owner (own-atom reuse), the license snapshot is `free` with no
// rate, regardless of the atom's share license.
func TestAuthorizeAtomUse_OwnAtom_FreeLicense(t *testing.T) {
	t.Parallel()
	const (
		owner    = "01970000-0000-7000-9000-000000000001"
		atomID   = "01970000-0000-7000-9000-atom-0000001"
		tenantID = "01970000-0000-7000-9000-tenant-aaaaa"
	)
	projections := newFakeProjectionRepo()
	projections.seed(atom_projection.Projection{
		AtomID: atomID, RevisionID: "rev-001", OwnerGCID: owner,
		PublishedAt: time.Now().UTC(),
	})
	srv := New(Deps{
		Grants:      newFakeGrantRepo(),
		Projections: projections,
		Rules:       testRules(),
	})

	resp, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:    owner, // own-atom
		TenantId:       tenantID,
		AtomId:         atomID,
		Scope:          sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		IdempotencyKey: "idem-grant-001",
	})
	if err != nil {
		t.Fatalf("AuthorizeAtomUse: %v", err)
	}
	// Own-atom → license free, no royalty rate.
	if resp.GetLicenseTermsSnapshot() != sharingv1.LicenseTerms_LICENSE_TERMS_FREE {
		t.Errorf("license=%v want FREE for own-atom", resp.GetLicenseTermsSnapshot())
	}
	if resp.GetRoyaltyRateSnapshot() != nil && resp.GetRoyaltyRateSnapshot().GetValue() != 0 {
		t.Errorf("royalty rate non-zero for own-atom: %v", resp.GetRoyaltyRateSnapshot())
	}
	if resp.GetOwnerGcid() != owner {
		t.Errorf("owner_gcid=%q want %q", resp.GetOwnerGcid(), owner)
	}
}

// fakeGrantRepo is an in-memory grant.GrantRepo.
type fakeGrantRepo struct {
	mu   sync.Mutex
	byID map[string]*grant.AtomUsageGrant
}

func newFakeGrantRepo() *fakeGrantRepo {
	return &fakeGrantRepo{byID: make(map[string]*grant.AtomUsageGrant)}
}

func (f *fakeGrantRepo) Authorize(ctx context.Context, g *grant.AtomUsageGrant) (*grant.AtomUsageGrant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Idempotent: if an active grant exists for (grantee, atom, scope), return it.
	for _, existing := range f.byID {
		if existing.GranteeGCID == g.GranteeGCID &&
			existing.AtomID == g.AtomID &&
			existing.Scope == g.Scope &&
			existing.Status == grant.StatusActive {
			return existing, nil
		}
	}
	f.byID[g.ID] = g
	return g, nil
}

func (f *fakeGrantRepo) Revoke(ctx context.Context, grantID, revokerGCID, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	g, ok := f.byID[grantID]
	if !ok {
		return grant.ErrGrantNotFound
	}
	return g.Revoke(time.Now().UTC())
}

func (f *fakeGrantRepo) GetActive(ctx context.Context, grantee, atomID string, scope grant.Scope) (*grant.AtomUsageGrant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, g := range f.byID {
		if g.GranteeGCID == grantee && g.AtomID == atomID && g.Scope.Covers(scope) && g.Status == grant.StatusActive {
			return g, nil
		}
	}
	return nil, grant.ErrGrantNotFound
}

func (f *fakeGrantRepo) ListEntitled(ctx context.Context, gcid string, scope grant.Scope, topicTags []string, limit int) ([]grant.EntitledAtom, error) {
	return nil, nil
}

// ActiveGrantAtomIDs — reuse-context surface (ADR-229 WS-0). The duel/share
// tests never exercise it; reuse_context_test.go covers the real behaviour
// via fakeReuseGrantRepo.
func (f *fakeGrantRepo) ActiveGrantAtomIDs(ctx context.Context, granteeGCID string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]string, 0, len(f.byID))
	for _, g := range f.byID {
		if g.GranteeGCID == granteeGCID && g.Status == grant.StatusActive {
			ids = append(ids, g.AtomID)
		}
	}
	return ids, nil
}

// TestRevokeAtomUse verifies §7.3 contract: revoking an existing grant
// returns status REVOKED + revoked_at. Revoking a missing grant → NotFound.
func TestRevokeAtomUse(t *testing.T) {
	t.Parallel()
	const (
		owner    = "01970000-0000-7000-9000-000000000001"
		atomID   = "01970000-0000-7000-9000-atom-0000001"
		tenantID = "01970000-0000-7000-9000-tenant-aaaaa"
	)
	projections := newFakeProjectionRepo()
	projections.seed(atom_projection.Projection{
		AtomID: atomID, RevisionID: "rev-001", OwnerGCID: owner,
		PublishedAt: time.Now().UTC(),
	})
	grants := newFakeGrantRepo()
	srv := New(Deps{
		Grants:      grants,
		Projections: projections,
		Rules:       testRules(),
	})

	// Authorize first.
	authResp, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:    owner,
		TenantId:       tenantID,
		AtomId:         atomID,
		Scope:          sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		IdempotencyKey: "idem-revoke-001",
	})
	if err != nil {
		t.Fatalf("AuthorizeAtomUse: %v", err)
	}
	grantID := authResp.GetGrantId()

	// Revoke.
	revResp, err := srv.RevokeAtomUse(context.Background(), &sharingv1.RevokeAtomUseRequest{
		GrantId:     grantID,
		RevokerGcid: owner,
		TenantId:    tenantID,
		Reason:      "test revoke",
	})
	if err != nil {
		t.Fatalf("RevokeAtomUse: %v", err)
	}
	if revResp.GetStatus() != sharingv1.GrantStatus_GRANT_STATUS_REVOKED {
		t.Errorf("status=%v want REVOKED", revResp.GetStatus())
	}
	if revResp.GetRevokedAt() == nil || !revResp.GetRevokedAt().IsValid() {
		t.Error("revoked_at not set")
	}

	// Revoke missing → NotFound.
	_, err = srv.RevokeAtomUse(context.Background(), &sharingv1.RevokeAtomUseRequest{
		GrantId: "nonexistent", RevokerGcid: owner, TenantId: tenantID,
	})
	statusIs(t, err, codes.NotFound)
}

// Compile-time check: the fake types satisfy the domain port interfaces.
var (
	_ atom_projection.AtomProjectionReader = (*fakeProjectionRepo)(nil)
	_ atom_share.ShareRepo                 = (*fakeShareRepo)(nil)
	_ grant.GrantRepo                      = (*fakeGrantRepo)(nil)
)

// silence unused import warnings when a sub-test doesn't use timestamppb.
var _ = timestamppb.Now
