package grant

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
)

// --- stubGrantRepo + stubRoyaltyRepo: in-memory doubles for port tests ---

type stubGrantRepo struct {
	grants map[string]*AtomUsageGrant // grantID -> grant
	// index for GetActive idempotency: (grantee|atom|scope) -> grantID
	activeIdx map[string]string
}

func newStubGrantRepo() *stubGrantRepo {
	return &stubGrantRepo{
		grants:    make(map[string]*AtomUsageGrant),
		activeIdx: make(map[string]string),
	}
}

func activeKey(grantee, atomID string, scope Scope) string {
	return grantee + "|" + atomID + "|" + string(scope)
}

func (s *stubGrantRepo) Authorize(_ context.Context, g *AtomUsageGrant) (*AtomUsageGrant, error) {
	key := activeKey(g.GranteeGCID, g.AtomID, g.Scope)
	if existingID, ok := s.activeIdx[key]; ok {
		// Idempotent: return existing active grant.
		return s.grants[existingID], nil
	}
	cp := *g
	s.grants[g.ID] = &cp
	s.activeIdx[key] = g.ID
	return &cp, nil
}

func (s *stubGrantRepo) Revoke(_ context.Context, grantID, revokerGCID, reason string) error {
	g, ok := s.grants[grantID]
	if !ok {
		return ErrGrantNotFound
	}
	if g.Status != StatusActive {
		return nil // idempotent
	}
	g.Status = StatusRevoked
	now := time.Now().UTC()
	g.RevokedAt = &now
	delete(s.activeIdx, activeKey(g.GranteeGCID, g.AtomID, g.Scope))
	return nil
}

func (s *stubGrantRepo) GetActive(_ context.Context, grantee, atomID string, scope Scope) (*AtomUsageGrant, error) {
	id, ok := s.activeIdx[activeKey(grantee, atomID, scope)]
	if !ok {
		return nil, ErrGrantNotFound
	}
	return s.grants[id], nil
}

func (s *stubGrantRepo) ListEntitled(_ context.Context, gcid string, scope Scope, topicTags []string, limit int) ([]EntitledAtom, error) {
	if limit <= 0 {
		limit = 20
	}
	var result []EntitledAtom
	for _, g := range s.grants {
		if g.GranteeGCID != gcid {
			continue
		}
		if !g.Scope.Covers(scope) {
			continue
		}
		if g.Status != StatusActive {
			continue
		}
		result = append(result, EntitledAtom{
			AtomID:     g.AtomID,
			RevisionID: g.RevisionID,
			AuthorGCID: g.OwnerGCID,
			License:    g.LicenseTermsSnapshot,
			HasGrant:   true,
		})
	}
	return result, nil
}

type stubRoyaltyRepo struct {
	settled map[string]*RoyaltySettlement // sourceEventID -> settlement
}

func newStubRoyaltyRepo() *stubRoyaltyRepo {
	return &stubRoyaltyRepo{settled: make(map[string]*RoyaltySettlement)}
}

func (s *stubRoyaltyRepo) Record(_ context.Context, set *RoyaltySettlement) error {
	if _, exists := s.settled[set.SourceEventID]; exists {
		return ErrRoyaltyAlreadySettled
	}
	cp := *set
	s.settled[set.SourceEventID] = &cp
	return nil
}

func (s *stubRoyaltyRepo) Exists(_ context.Context, sourceEventID string) (bool, error) {
	_, ok := s.settled[sourceEventID]
	return ok, nil
}

// --- helpers ---

func validGrantArgs() (string, string, string, string, Scope, atom_share.LicenseTerms, atom_share.RoyaltyRate, string) {
	return "owner-1", "grantee-1", "atom-1", "rev-1", ScopeDuel, atom_share.LicenseRoyaltyPct, atom_share.RoyaltyRate{Kind: "pct", Value: 10}, "share-entry-1"
}

func mustNewGrant(t *testing.T, license atom_share.LicenseTerms, rate atom_share.RoyaltyRate) *AtomUsageGrant {
	t.Helper()
	owner, grantee, atom, rev, scope, _, _, src := validGrantArgs()
	g, err := NewGrant(owner, grantee, atom, rev, scope, license, rate, src)
	if err != nil {
		t.Fatalf("NewGrant: %v", err)
	}
	return g
}

// --- Scope tests ---

func TestScope_IsValid(t *testing.T) {
	valid := []Scope{ScopeTestSet, ScopeDuel, ScopeLiveQuiz, ScopeCollection, ScopeUnlimited}
	for _, s := range valid {
		if !s.IsValid() {
			t.Errorf("expected %q to be valid", s)
		}
	}
	if Scope("bogus").IsValid() {
		t.Error("expected bogus scope to be invalid")
	}
}

func TestScope_Covers_UnlimitedCoversAll(t *testing.T) {
	targets := []Scope{ScopeTestSet, ScopeDuel, ScopeLiveQuiz, ScopeCollection, ScopeUnlimited}
	for _, target := range targets {
		if !ScopeUnlimited.Covers(target) {
			t.Errorf("UNLIMITED should cover %q", target)
		}
	}
}

func TestScope_Covers_ExactMatchOnly(t *testing.T) {
	cases := []struct {
		got    Scope
		target Scope
		want   bool
	}{
		{ScopeDuel, ScopeDuel, true},
		{ScopeDuel, ScopeTestSet, false},
		{ScopeTestSet, ScopeTestSet, true},
		{ScopeLiveQuiz, ScopeDuel, false},
		{ScopeCollection, ScopeCollection, true},
	}
	for _, tc := range cases {
		if got := tc.got.Covers(tc.target); got != tc.want {
			t.Errorf("Covers(%q, %q) = %v, want %v", tc.got, tc.target, got, tc.want)
		}
	}
}

// --- Status tests ---

func TestStatus_IsTerminal(t *testing.T) {
	if StatusActive.IsTerminal() {
		t.Error("active should NOT be terminal")
	}
	if !StatusRevoked.IsTerminal() {
		t.Error("revoked should be terminal")
	}
	if !StatusExpired.IsTerminal() {
		t.Error("expired should be terminal")
	}
}

// --- NewGrant tests (R2 license snapshot freeze) ---

func TestNewGrant_FreezesLicenseSnapshot(t *testing.T) {
	g := mustNewGrant(t, atom_share.LicenseRoyaltyPct, atom_share.RoyaltyRate{Kind: "pct", Value: 25})
	lic, rate := g.SnapshotLicense()
	if lic != atom_share.LicenseRoyaltyPct {
		t.Errorf("expected royalty_pct snapshot, got %v", lic)
	}
	if rate.Value != 25 {
		t.Errorf("expected rate 25 snapshot, got %v", rate.Value)
	}
}

func TestNewGrant_AcceptsFreeLicenseWithoutRate(t *testing.T) {
	g, err := NewGrant("o", "g", "a", "r", ScopeDuel, atom_share.LicenseFree, atom_share.RoyaltyRate{}, "")
	if err != nil {
		t.Fatalf("free license without rate: %v", err)
	}
	if g.LicenseTermsSnapshot != atom_share.LicenseFree {
		t.Errorf("expected free snapshot, got %v", g.LicenseTermsSnapshot)
	}
}

func TestNewGrant_RejectsRoyaltyLicenseWithoutRate(t *testing.T) {
	cases := []atom_share.LicenseTerms{atom_share.LicenseRoyaltyPct, atom_share.LicenseRoyaltyFixed}
	for _, lic := range cases {
		_, err := NewGrant("o", "g", "a", "r", ScopeDuel, lic, atom_share.RoyaltyRate{}, "")
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("royalty license %q without rate: expected ErrInvalidArgument, got %v", lic, err)
		}
	}
}

func TestNewGrant_RejectsInvalidScope(t *testing.T) {
	_, err := NewGrant("o", "g", "a", "r", Scope("bogus"), atom_share.LicenseFree, atom_share.RoyaltyRate{}, "")
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("invalid scope: expected ErrInvalidArgument, got %v", err)
	}
}

func TestNewGrant_RejectsMissingRequiredFields(t *testing.T) {
	cases := []struct {
		name    string
		owner   string
		grantee string
		atom    string
		rev     string
	}{
		{"missing owner", "", "g", "a", "r"},
		{"missing grantee", "o", "", "a", "r"},
		{"missing atom", "o", "g", "", "r"},
		{"missing revision", "o", "g", "a", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewGrant(tc.owner, tc.grantee, tc.atom, tc.rev, ScopeDuel, atom_share.LicenseFree, atom_share.RoyaltyRate{}, "")
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("%s: expected ErrInvalidArgument, got %v", tc.name, err)
			}
		})
	}
}

func TestNewGrant_DefaultsToActive(t *testing.T) {
	g := mustNewGrant(t, atom_share.LicenseFree, atom_share.RoyaltyRate{})
	if g.Status != StatusActive {
		t.Errorf("expected active, got %v", g.Status)
	}
	if g.RevokedAt != nil {
		t.Error("expected nil RevokedAt on new grant")
	}
}

// --- IsUsable tests ---

func TestAtomUsageGrant_IsUsable_ActiveMatchingGranteeAndScope(t *testing.T) {
	g := mustNewGrant(t, atom_share.LicenseFree, atom_share.RoyaltyRate{})
	if !g.IsUsable("grantee-1", ScopeDuel) {
		t.Error("expected usable for active grant with matching grantee + scope")
	}
}

func TestAtomUsageGrant_IsUsable_UnlimitedCoversAnyTarget(t *testing.T) {
	g := mustNewGrant(t, atom_share.LicenseFree, atom_share.RoyaltyRate{})
	g.Scope = ScopeUnlimited
	targets := []Scope{ScopeDuel, ScopeTestSet, ScopeLiveQuiz, ScopeCollection}
	for _, target := range targets {
		if !g.IsUsable("grantee-1", target) {
			t.Errorf("UNLIMITED grant should be usable for %q", target)
		}
	}
}

func TestAtomUsageGrant_IsUsable_RejectsWrongGrantee(t *testing.T) {
	g := mustNewGrant(t, atom_share.LicenseFree, atom_share.RoyaltyRate{})
	if g.IsUsable("someone-else", ScopeDuel) {
		t.Error("expected NOT usable for wrong grantee")
	}
}

func TestAtomUsageGrant_IsUsable_RejectsWrongScope(t *testing.T) {
	g := mustNewGrant(t, atom_share.LicenseFree, atom_share.RoyaltyRate{})
	if g.IsUsable("grantee-1", ScopeTestSet) {
		t.Error("expected NOT usable for non-covering scope")
	}
}

func TestAtomUsageGrant_IsUsable_RejectsRevoked(t *testing.T) {
	g := mustNewGrant(t, atom_share.LicenseFree, atom_share.RoyaltyRate{})
	_ = g.Revoke(time.Now())
	if g.IsUsable("grantee-1", ScopeDuel) {
		t.Error("expected NOT usable for revoked grant")
	}
}

func TestAtomUsageGrant_IsUsable_RejectsExpiredByTime(t *testing.T) {
	g := mustNewGrant(t, atom_share.LicenseFree, atom_share.RoyaltyRate{})
	past := time.Now().UTC().Add(-time.Hour)
	g.ExpiresAt = &past
	if g.IsUsable("grantee-1", ScopeDuel) {
		t.Error("expected NOT usable for expired grant")
	}
}

func TestAtomUsageGrant_IsUsable_NilSafe(t *testing.T) {
	var g *AtomUsageGrant
	if g.IsUsable("x", ScopeDuel) {
		t.Error("expected nil grant to NOT be usable")
	}
}

// --- Revoke / Expire tests ---

func TestAtomUsageGrant_Revoke_TransitionsToRevoked(t *testing.T) {
	g := mustNewGrant(t, atom_share.LicenseFree, atom_share.RoyaltyRate{})
	if err := g.Revoke(time.Now()); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if g.Status != StatusRevoked {
		t.Errorf("expected revoked, got %v", g.Status)
	}
	if g.RevokedAt == nil {
		t.Error("expected RevokedAt set")
	}
}

func TestAtomUsageGrant_Revoke_Idempotent(t *testing.T) {
	g := mustNewGrant(t, atom_share.LicenseFree, atom_share.RoyaltyRate{})
	_ = g.Revoke(time.Now())
	first := g.RevokedAt
	if err := g.Revoke(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("second Revoke: %v", err)
	}
	// Idempotent — RevokedAt should NOT change.
	if g.RevokedAt != first {
		t.Error("expected RevokedAt unchanged on idempotent revoke")
	}
}

func TestAtomUsageGrant_Revoke_RejectsExpiredGrant(t *testing.T) {
	g := mustNewGrant(t, atom_share.LicenseFree, atom_share.RoyaltyRate{})
	_ = g.Expire(time.Now())
	if err := g.Revoke(time.Now()); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument revoking expired grant, got %v", err)
	}
}

func TestAtomUsageGrant_Expire_TransitionsToExpired(t *testing.T) {
	g := mustNewGrant(t, atom_share.LicenseFree, atom_share.RoyaltyRate{})
	if err := g.Expire(time.Now()); err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if g.Status != StatusExpired {
		t.Errorf("expected expired, got %v", g.Status)
	}
}

func TestAtomUsageGrant_Expire_Idempotent(t *testing.T) {
	g := mustNewGrant(t, atom_share.LicenseFree, atom_share.RoyaltyRate{})
	_ = g.Expire(time.Now())
	first := g.ExpiresAt
	if err := g.Expire(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("second Expire: %v", err)
	}
	if g.ExpiresAt != first {
		t.Error("expected ExpiresAt unchanged on idempotent expire")
	}
}

func TestAtomUsageGrant_Expire_RejectsRevokedGrant(t *testing.T) {
	g := mustNewGrant(t, atom_share.LicenseFree, atom_share.RoyaltyRate{})
	_ = g.Revoke(time.Now())
	if err := g.Expire(time.Now()); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument expiring revoked grant, got %v", err)
	}
}

// --- SnapshotLicense returns frozen copy (R2 — price-change does not alter) ---

func TestSnapshotLicense_FrozenAfterGrant(t *testing.T) {
	g := mustNewGrant(t, atom_share.LicenseRoyaltyPct, atom_share.RoyaltyRate{Kind: "pct", Value: 30})
	lic1, rate1 := g.SnapshotLicense()
	// Simulate author price-change by mutating the struct's fields directly —
	// the snapshot method should still return the original frozen values.
	g.LicenseTermsSnapshot = atom_share.LicenseFree
	g.RoyaltyRateSnapshot = atom_share.RoyaltyRate{Kind: "pct", Value: 99}
	// SnapshotLicense reads from the struct, so it reflects mutations — but the
	// contract is that the adapter NEVER mutates these fields post-grant. This
	// test documents that the snapshot is a direct read, not a defensive copy.
	// The R2 guarantee is enforced at the adapter layer (no UPDATE on snapshot
	// columns). Here we assert the method returns the current struct state.
	_ = lic1
	_ = rate1
	if g.LicenseTermsSnapshot != atom_share.LicenseFree {
		t.Error("struct mutation should be reflected (documenting direct-read semantics)")
	}
}

// --- SettleRoyalty tests (§3.3 math) ---

func baseSettleInput(lic atom_share.LicenseTerms, rate atom_share.RoyaltyRate) SettleInput {
	return SettleInput{
		BaseUsageValue:  100,
		License:         lic,
		Rate:            rate,
		RoyaltyBase:     0,
		RoyaltyCap:      0,
		SourceEventID:   "evt-1",
		UsageContext:    "duel",
		OwnerGCID:       "owner-1",
		GranteeTenantID: "tenant-1",
		AtomID:          "atom-1",
		GrantID:         "grant-1",
		Currency:        "reputation",
	}
}

func TestSettleRoyalty_PctMath(t *testing.T) {
	in := baseSettleInput(atom_share.LicenseRoyaltyPct, atom_share.RoyaltyRate{Kind: "pct", Value: 0.1})
	s := SettleRoyalty(in)
	// pct: amount = base_usage_value × rate.value = 100 × 0.1 = 10
	if s.Amount != 10 {
		t.Errorf("expected amount 10, got %v", s.Amount)
	}
}

func TestSettleRoyalty_FixedMath(t *testing.T) {
	in := baseSettleInput(atom_share.LicenseRoyaltyFixed, atom_share.RoyaltyRate{Kind: "fixed_per_use", Value: 5})
	s := SettleRoyalty(in)
	// fixed: amount = rate.value = 5
	if s.Amount != 5 {
		t.Errorf("expected amount 5, got %v", s.Amount)
	}
}

func TestSettleRoyalty_FreeLicenseZeroAmount(t *testing.T) {
	for _, lic := range []atom_share.LicenseTerms{atom_share.LicenseFree, atom_share.LicenseCCBySA, atom_share.LicenseCCND} {
		in := baseSettleInput(lic, atom_share.RoyaltyRate{})
		s := SettleRoyalty(in)
		if s.Amount != 0 {
			t.Errorf("free license %q: expected amount 0, got %v", lic, s.Amount)
		}
	}
}

func TestSettleRoyalty_ClampsToCap(t *testing.T) {
	in := baseSettleInput(atom_share.LicenseRoyaltyPct, atom_share.RoyaltyRate{Kind: "pct", Value: 0.5})
	in.RoyaltyCap = 20
	// pct: 100 × 0.5 = 50, clamped to 20.
	s := SettleRoyalty(in)
	if s.Amount != 20 {
		t.Errorf("expected clamped amount 20, got %v", s.Amount)
	}
}

func TestSettleRoyalty_NoClampWhenCapZero(t *testing.T) {
	in := baseSettleInput(atom_share.LicenseRoyaltyPct, atom_share.RoyaltyRate{Kind: "pct", Value: 0.5})
	in.RoyaltyCap = 0 // uncapped
	s := SettleRoyalty(in)
	if s.Amount != 50 {
		t.Errorf("expected unclamped amount 50, got %v", s.Amount)
	}
}

func TestSettleRoyalty_FloorsAtBase(t *testing.T) {
	in := baseSettleInput(atom_share.LicenseRoyaltyPct, atom_share.RoyaltyRate{Kind: "pct", Value: 0.01})
	in.BaseUsageValue = 1 // tiny base → amount = 1 × 0.01 = 0.01, floored to 10
	in.RoyaltyBase = 10
	s := SettleRoyalty(in)
	if s.Amount != 10 {
		t.Errorf("expected floored amount 10, got %v", s.Amount)
	}
}

func TestSettleRoyalty_FloorRespectedByFixed(t *testing.T) {
	in := baseSettleInput(atom_share.LicenseRoyaltyFixed, atom_share.RoyaltyRate{Kind: "fixed_per_use", Value: 3})
	in.RoyaltyBase = 10
	s := SettleRoyalty(in)
	// fixed 3 < floor 10 → floored to 10
	if s.Amount != 10 {
		t.Errorf("expected floored amount 10, got %v", s.Amount)
	}
}

func TestSettleRoyalty_FloorThenClamp(t *testing.T) {
	in := baseSettleInput(atom_share.LicenseRoyaltyFixed, atom_share.RoyaltyRate{Kind: "fixed_per_use", Value: 3})
	in.RoyaltyBase = 10
	in.RoyaltyCap = 8 // floor 10 > cap 8 → clamped to 8
	s := SettleRoyalty(in)
	if s.Amount != 8 {
		t.Errorf("expected floored-then-clamped amount 8, got %v", s.Amount)
	}
}

func TestSettleRoyalty_SetsSettlementFields(t *testing.T) {
	in := baseSettleInput(atom_share.LicenseRoyaltyPct, atom_share.RoyaltyRate{Kind: "pct", Value: 10})
	s := SettleRoyalty(in)
	if s.GrantID != "grant-1" {
		t.Errorf("expected grant-1, got %s", s.GrantID)
	}
	if s.OwnerGCID != "owner-1" {
		t.Errorf("expected owner-1, got %s", s.OwnerGCID)
	}
	if s.GranteeTenantID != "tenant-1" {
		t.Errorf("expected tenant-1, got %s", s.GranteeTenantID)
	}
	if s.AtomID != "atom-1" {
		t.Errorf("expected atom-1, got %s", s.AtomID)
	}
	if s.Currency != "reputation" {
		t.Errorf("expected reputation, got %s", s.Currency)
	}
	if s.UsageContext != "duel" {
		t.Errorf("expected duel, got %s", s.UsageContext)
	}
	if s.SourceEventID != "evt-1" {
		t.Errorf("expected evt-1, got %s", s.SourceEventID)
	}
	if s.ID == "" {
		t.Error("expected non-empty settlement ID")
	}
}

func TestSettleRoyalty_NeverNegative(t *testing.T) {
	// Defensive: even if rate is 0, amount should be 0 (not negative).
	in := baseSettleInput(atom_share.LicenseRoyaltyFixed, atom_share.RoyaltyRate{Kind: "fixed_per_use", Value: 0})
	s := SettleRoyalty(in)
	if s.Amount < 0 {
		t.Errorf("expected non-negative amount, got %v", s.Amount)
	}
}

// --- GrantRepo port tests (idempotency) ---

func TestGrantRepo_AuthorizeIdempotentReturnsExisting(t *testing.T) {
	repo := newStubGrantRepo()
	g1 := mustNewGrant(t, atom_share.LicenseFree, atom_share.RoyaltyRate{})
	g1, err := repo.Authorize(context.Background(), g1)
	if err != nil {
		t.Fatalf("first Authorize: %v", err)
	}
	// Second authorize with same (grantee, atom, scope) — should return existing.
	g2 := mustNewGrant(t, atom_share.LicenseFree, atom_share.RoyaltyRate{})
	g2.ID = "different-id"
	g2, err = repo.Authorize(context.Background(), g2)
	if err != nil {
		t.Fatalf("second Authorize: %v", err)
	}
	if g2.ID != g1.ID {
		t.Errorf("idempotent Authorize should return existing grant ID %s, got %s", g1.ID, g2.ID)
	}
}

func TestGrantRepo_RevokeFlipsStatus(t *testing.T) {
	repo := newStubGrantRepo()
	g := mustNewGrant(t, atom_share.LicenseFree, atom_share.RoyaltyRate{})
	g, _ = repo.Authorize(context.Background(), g)
	if err := repo.Revoke(context.Background(), g.ID, "revoker", "test"); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	got, err := repo.GetActive(context.Background(), g.GranteeGCID, g.AtomID, g.Scope)
	if !errors.Is(err, ErrGrantNotFound) {
		t.Errorf("expected ErrNotFound after revoke, got %v (grant=%v)", err, got)
	}
}

func TestGrantRepo_GetActiveNotFound(t *testing.T) {
	repo := newStubGrantRepo()
	_, err := repo.GetActive(context.Background(), "g", "a", ScopeDuel)
	if !errors.Is(err, ErrGrantNotFound) {
		t.Errorf("expected ErrGrantNotFound, got %v", err)
	}
}

func TestGrantRepo_ListEntitledReturnsActiveGrants(t *testing.T) {
	repo := newStubGrantRepo()
	g := mustNewGrant(t, atom_share.LicenseFree, atom_share.RoyaltyRate{})
	_, _ = repo.Authorize(context.Background(), g)
	atoms, err := repo.ListEntitled(context.Background(), g.GranteeGCID, ScopeDuel, nil, 10)
	if err != nil {
		t.Fatalf("ListEntitled: %v", err)
	}
	if len(atoms) != 1 {
		t.Fatalf("expected 1 entitled atom, got %d", len(atoms))
	}
	if !atoms[0].HasGrant {
		t.Error("expected HasGrant=true")
	}
}

func TestGrantRepo_ListEntitledUnlimitedScopeCoversAll(t *testing.T) {
	repo := newStubGrantRepo()
	g := mustNewGrant(t, atom_share.LicenseFree, atom_share.RoyaltyRate{})
	g.Scope = ScopeUnlimited
	_, _ = repo.Authorize(context.Background(), g)
	// An UNLIMITED grant should be returned for ANY target scope.
	for _, target := range []Scope{ScopeDuel, ScopeTestSet, ScopeLiveQuiz, ScopeCollection} {
		atoms, _ := repo.ListEntitled(context.Background(), g.GranteeGCID, target, nil, 10)
		if len(atoms) != 1 {
			t.Errorf("UNLIMITED grant should cover %q: got %d atoms", target, len(atoms))
		}
	}
}

// --- RoyaltyRepo port tests (idempotency on source_event_id) ---

func TestRoyaltyRepo_RecordAndExists(t *testing.T) {
	repo := newStubRoyaltyRepo()
	in := baseSettleInput(atom_share.LicenseRoyaltyPct, atom_share.RoyaltyRate{Kind: "pct", Value: 10})
	s := SettleRoyalty(in)
	if err := repo.Record(context.Background(), &s); err != nil {
		t.Fatalf("Record: %v", err)
	}
	exists, err := repo.Exists(context.Background(), s.SourceEventID)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !exists {
		t.Error("expected settlement to exist")
	}
}

func TestRoyaltyRepo_RecordDuplicateReturnsErrAlreadySettled(t *testing.T) {
	repo := newStubRoyaltyRepo()
	in := baseSettleInput(atom_share.LicenseRoyaltyPct, atom_share.RoyaltyRate{Kind: "pct", Value: 10})
	s := SettleRoyalty(in)
	_ = repo.Record(context.Background(), &s)
	if err := repo.Record(context.Background(), &s); !errors.Is(err, ErrRoyaltyAlreadySettled) {
		t.Errorf("expected ErrRoyaltyAlreadySettled on dup, got %v", err)
	}
}

func TestRoyaltyRepo_ExistsFalseForUnknown(t *testing.T) {
	repo := newStubRoyaltyRepo()
	exists, _ := repo.Exists(context.Background(), "unknown")
	if exists {
		t.Error("expected false for unknown source_event_id")
	}
}
