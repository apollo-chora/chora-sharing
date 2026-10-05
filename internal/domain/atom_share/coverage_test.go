// Package atom_share holds the remaining coverage specs for the SharedAtom
// value object (royalty-rate guard branches, truncation edge, tie-breaks).
package atom_share

import (
	"testing"
	"time"
)

// R2: a royalty license REQUIRES a valid rate — an out-of-range pct rate is
// rejected by NewShare even though license + rate are the only royalty inputs.
func TestNewShare_RejectsRoyaltyLicenseWithInvalidRate(t *testing.T) {
	if _, err := NewShare("t", "atom-1", "rev-1", "owner-1", "Author", "stem", "mcq", nil, "",
		LicenseRoyaltyPct, RoyaltyRate{Kind: "pct", Value: 150}); err == nil {
		t.Fatal("expected error for royalty license with out-of-range rate")
	}
}

// R2 happy path: a royalty license with a valid rate constructs a share whose
// license + rate snapshots are stored verbatim.
func TestNewShare_AcceptsRoyaltyLicenseWithValidRate(t *testing.T) {
	s, err := NewShare("t", "atom-1", "rev-1", "owner-1", "Author", "stem", "mcq", nil, "",
		LicenseRoyaltyPct, RoyaltyRate{Kind: "pct", Value: 10})
	if err != nil {
		t.Fatalf("NewShare: %v", err)
	}
	if s.License != LicenseRoyaltyPct || s.Rate.Value != 10 {
		t.Errorf("license/rate snapshot = %q/%v, want royalty_pct/10", s.License, s.Rate.Value)
	}
}

// truncateRune with a non-positive max is a no-op returning the empty string
// (defensive guard on the internal truncation helper).
func TestTruncateRune_MaxZeroReturnsEmpty(t *testing.T) {
	if got := truncateRune("abc", 0); got != "" {
		t.Errorf("truncateRune(abc, 0) = %q, want empty", got)
	}
	if got := truncateRune("abc", -1); got != "" {
		t.Errorf("truncateRune(abc, -1) = %q, want empty", got)
	}
}

// Derivation stays deterministic when two events share a timestamp AND a
// SourceEventID: the ActorGCID tie-break decides (z > a sorts first, so a
// hidden event by "z" beats a revoked event by "a").
func TestDeriveStatus_TiebreakOnActorGCID(t *testing.T) {
	at := time.Now().UTC()
	evs := []ShareEvent{
		{FeedEntryID: "f1", ActorGCID: "a", Type: EventRevoked, SourceEventID: "same", CreatedAt: at},
		{FeedEntryID: "f1", ActorGCID: "z", Type: EventHidden, SourceEventID: "same", CreatedAt: at},
	}
	if got := DeriveStatus(evs); got != StatusHiddenByAuthor {
		t.Fatalf("DeriveStatus = %s, want %s (actor z must sort before a)", got, StatusHiddenByAuthor)
	}
}