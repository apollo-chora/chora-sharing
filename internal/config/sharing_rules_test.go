package config

import (
	"testing"
	"time"
)

func TestLoadSharingRules_DefaultsMatchShippedDoubles(t *testing.T) {
	r, err := LoadSharingRules()
	if err != nil {
		t.Fatalf("LoadSharingRules: %v", err)
	}
	// R-10: ELO defaults MUST match shipped duel_ratings (0022).
	if r.ELOBaseline != 1200 {
		t.Errorf("ELOBaseline = %d, want 1200 (shipped duel_ratings default)", r.ELOBaseline)
	}
	if r.ELOKFactor != 32 {
		t.Errorf("ELOKFactor = %d, want 32 (shipped duel_match_history default)", r.ELOKFactor)
	}
	// Combo tiers 1→2→3→5 (design §4.4).
	if got := r.ComboTiers; len(got) != 4 || got[0] != 1 || got[3] != 5 {
		t.Errorf("ComboTiers = %v, want [1 2 3 5]", got)
	}
	// Season + WS grace defaults.
	if r.SeasonWindow != 30*24*time.Hour {
		t.Errorf("SeasonWindow = %v, want 30d", r.SeasonWindow)
	}
	if r.WebSocketGrace != 30*time.Second {
		t.Errorf("WebSocketGrace = %v, want 30s", r.WebSocketGrace)
	}
	// WS3: round timer default must be 15s (per-atom deadline).
	if r.RoundTimerSec != 15 {
		t.Errorf("RoundTimerSec = %d, want 15 (WS3 default)", r.RoundTimerSec)
	}
	// License vocab mirrors the atom_license_terms enum.
	for _, l := range []string{"free", "royalty_pct", "royalty_fixed", "cc_by_sa", "cc_nd"} {
		if !r.IsAcceptedLicense(l) {
			t.Errorf("license %q not in vocab", l)
		}
	}
	if r.IsAcceptedLicense("bogus") {
		t.Error("bogus license accepted")
	}
	if r.RoyaltyCurrency != "reputation" {
		t.Errorf("RoyaltyCurrency = %q, want \"reputation\"", r.RoyaltyCurrency)
	}
	if r.ManaActionCode != "atom_royalty" {
		t.Errorf("ManaActionCode = %q, want \"atom_royalty\"", r.ManaActionCode)
	}
}

func TestComboMultiplierForStreak(t *testing.T) {
	r, _ := LoadSharingRules()
	cases := []struct {
		streak int
		want   int
	}{
		{0, 1}, {1, 2}, {2, 3}, {3, 5},
		{4, 5},  // past last tier — holds at 5
		{99, 5}, // long streak — still 5
		{-1, 1}, // negative treated as 0
	}
	for _, c := range cases {
		if got := r.ComboMultiplierForStreak(c.streak); got != c.want {
			t.Errorf("streak %d: got %d, want %d", c.streak, got, c.want)
		}
	}
}

func TestLoadSharingRules_FailLoudOnMalformedInt(t *testing.T) {
	t.Setenv("CHORA_SHARING_ELO_BASELINE", "not-a-number")
	if _, err := LoadSharingRules(); err == nil {
		t.Fatal("expected error for malformed ELO_BASELINE, got nil (silent fallback)")
	}
}

func TestLoadSharingRules_FailLoudOnMalformedDuration(t *testing.T) {
	t.Setenv("CHORA_SHARING_WS_GRACE", "30 bananas")
	if _, err := LoadSharingRules(); err == nil {
		t.Fatal("expected error for malformed WS_GRACE, got nil")
	}
}

func TestLoadSharingRules_FailLoudOnMalformedComboTier(t *testing.T) {
	t.Setenv("CHORA_SHARING_COMBO_TIERS", "1,2,oops,5")
	if _, err := LoadSharingRules(); err == nil {
		t.Fatal("expected error for malformed COMBO_TIERS entry, got nil")
	}
}

func TestLoadSharingRules_RejectsNonIncreasingComboTiers(t *testing.T) {
	t.Setenv("CHORA_SHARING_COMBO_TIERS", "1,3,2,5")
	if _, err := LoadSharingRules(); err == nil {
		t.Fatal("expected error for non-increasing combo tiers, got nil")
	}
}

func TestLoadSharingRules_AcceptsOverride(t *testing.T) {
	t.Setenv("CHORA_SHARING_ELO_BASELINE", "1500")
	t.Setenv("CHORA_SHARING_COMBO_TIERS", "1,2,4,8")
	r, err := LoadSharingRules()
	if err != nil {
		t.Fatalf("LoadSharingRules: %v", err)
	}
	if r.ELOBaseline != 1500 {
		t.Errorf("ELOBaseline = %d, want 1500 override", r.ELOBaseline)
	}
	if got := r.ComboMultiplierForStreak(2); got != 4 {
		t.Errorf("override combo streak 2: got %d, want 4", got)
	}
}

func TestLoadSharingRules_BlitzDefaults(t *testing.T) {
	r, err := LoadSharingRules()
	if err != nil {
		t.Fatalf("LoadSharingRules: %v", err)
	}
	if r.BlitzTimeLimitSec != 120 {
		t.Errorf("BlitzTimeLimitSec = %d, want 120 (2 min default)", r.BlitzTimeLimitSec)
	}
	if r.BlitzRaceTarget != 5 {
		t.Errorf("BlitzRaceTarget = %d, want 5 (default)", r.BlitzRaceTarget)
	}
}

func TestLoadSharingRules_BlitzOverride(t *testing.T) {
	t.Setenv("CHORA_SHARING_BLITZ_TIME_LIMIT_SEC", "90")
	t.Setenv("CHORA_SHARING_BLITZ_RACE_TARGET", "3")
	r, err := LoadSharingRules()
	if err != nil {
		t.Fatalf("LoadSharingRules: %v", err)
	}
	if r.BlitzTimeLimitSec != 90 {
		t.Errorf("BlitzTimeLimitSec = %d, want 90 override", r.BlitzTimeLimitSec)
	}
	if r.BlitzRaceTarget != 3 {
		t.Errorf("BlitzRaceTarget = %d, want 3 override", r.BlitzRaceTarget)
	}
}

func TestLoadSharingRules_FailLoudOnMalformedBlitzTimeLimit(t *testing.T) {
	t.Setenv("CHORA_SHARING_BLITZ_TIME_LIMIT_SEC", "not-a-number")
	if _, err := LoadSharingRules(); err == nil {
		t.Fatal("expected error for malformed BLITZ_TIME_LIMIT_SEC, got nil")
	}
}

func TestLoadSharingRules_RejectsZeroBlitzTimeLimit(t *testing.T) {
	t.Setenv("CHORA_SHARING_BLITZ_TIME_LIMIT_SEC", "0")
	if _, err := LoadSharingRules(); err == nil {
		t.Fatal("expected error for zero BLITZ_TIME_LIMIT_SEC, got nil")
	}
}
