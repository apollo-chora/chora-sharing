package grant

import (
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
)

// A royalty license with a VALID rate falls through to full grant
// construction — the frozen royalty snapshot path (R2).
func TestNewGrant_AcceptsRoyaltyLicenseWithValidRate(t *testing.T) {
	g, err := NewGrant("owner-g1", "grantee-g1", "atom-1", "rev-1", ScopeDuel,
		atom_share.LicenseRoyaltyPct, atom_share.RoyaltyRate{Kind: "pct", Value: 10}, "share-1")
	if err != nil {
		t.Fatalf("NewGrant with a valid royalty license: %v", err)
	}
	if g.LicenseTermsSnapshot != atom_share.LicenseRoyaltyPct {
		t.Errorf("license snapshot = %q, want %q", g.LicenseTermsSnapshot, atom_share.LicenseRoyaltyPct)
	}
	if g.RoyaltyRateSnapshot.Value != 10 {
		t.Errorf("royalty rate snapshot = %v, want 10", g.RoyaltyRateSnapshot.Value)
	}
	if g.Status != StatusActive {
		t.Errorf("status = %q, want active", g.Status)
	}
	if g.SourceShareEntry != "share-1" {
		t.Errorf("source share entry = %q, want share-1", g.SourceShareEntry)
	}
}

// An unknown license term is rejected even when everything else is valid —
// the author must pick one of the five canonical terms.
func TestNewGrant_RejectsInvalidLicense(t *testing.T) {
	if _, err := NewGrant("owner-g1", "grantee-g1", "atom-1", "rev-1", ScopeDuel,
		atom_share.LicenseTerms("bogus"), atom_share.RoyaltyRate{}, ""); err == nil {
		t.Fatal("expected error for invalid license terms")
	}
}

// computeRoyaltyAmount clamps a negative computed amount to zero (a royalty_pct
// with a negative base usage value must never accrue a negative settlement).
func TestSettleRoyalty_NegativeAmountClampedToZero(t *testing.T) {
	got := SettleRoyalty(SettleInput{
		BaseUsageValue:  -5,
		License:         atom_share.LicenseRoyaltyPct,
		Rate:            atom_share.RoyaltyRate{Kind: "pct", Value: 50},
		SourceEventID:   "evt-neg",
		UsageContext:    "duel",
		OwnerGCID:       "owner-g1",
		GranteeTenantID: "tenant-1",
		AtomID:          "atom-1",
		GrantID:         "grant-1",
	})
	if got.Amount != 0 {
		t.Errorf("amount = %v, want 0 (negative clamped)", got.Amount)
	}
}