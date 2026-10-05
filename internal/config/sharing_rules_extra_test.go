// Additional sharing-rules coverage: every remaining fail-loud env parse
// branch, the envIntSlice / envDuration value paths, validate()'s failure
// branches, and the empty-ComboTiers guard in ComboMultiplierForStreak.
package config

import (
	"testing"
	"time"
)

func TestLoadSharingRules_FailLoudOnEachMalformedEnv(t *testing.T) {
	cases := []struct {
		key string
		val string
	}{
		{"CHORA_SHARING_ELO_K_FACTOR", "not-an-int"},
		{"CHORA_SHARING_ROYALTY_BASE", "not-an-int"},
		{"CHORA_SHARING_ROYALTY_CAP", "not-an-int"},
		{"CHORA_SHARING_LIVEQUIZ_PER_SESSION_CAP", "not-an-int"},
		{"CHORA_SHARING_SEASON_WINDOW", "not-a-duration"},
		{"CHORA_SHARING_MM_TIMEOUT", "not-a-duration"},
		{"CHORA_SHARING_MM_HEARTBEAT_STALE", "not-a-duration"},
		{"CHORA_SHARING_MM_MATCH_TICK", "not-a-duration"},
		{"CHORA_SHARING_MM_SWEEP_TICK", "not-a-duration"},
		{"CHORA_SHARING_ROUND_TIMER_SEC", "not-an-int"},
		{"CHORA_SHARING_BLITZ_RACE_TARGET", "not-an-int"},
	}
	for _, c := range cases {
		t.Run(c.key, func(t *testing.T) {
			t.Setenv(c.key, c.val)
			if _, err := LoadSharingRules(); err == nil {
				t.Fatalf("expected error for malformed %s, got nil", c.key)
			}
		})
	}
}

func TestLoadSharingRules_HonorsDurationOverride(t *testing.T) {
	t.Setenv("CHORA_SHARING_WS_GRACE", "45s")
	t.Setenv("CHORA_SHARING_SEASON_WINDOW", "720h")
	r, err := LoadSharingRules()
	if err != nil {
		t.Fatalf("LoadSharingRules: %v", err)
	}
	if r.WebSocketGrace != 45*time.Second {
		t.Errorf("WebSocketGrace = %v, want 45s", r.WebSocketGrace)
	}
	// envDuration's value-return path (not just default / error).
	if r.SeasonWindow != 720*time.Hour {
		t.Errorf("SeasonWindow = %v, want 720h", r.SeasonWindow)
	}
}

func TestLoadSharingRules_ComboTiersSkipsEmptyParts(t *testing.T) {
	t.Setenv("CHORA_SHARING_COMBO_TIERS", "1,, 2 ,,5")
	r, err := LoadSharingRules()
	if err != nil {
		t.Fatalf("LoadSharingRules: %v", err)
	}
	if len(r.ComboTiers) != 3 || r.ComboTiers[0] != 1 || r.ComboTiers[1] != 2 || r.ComboTiers[2] != 5 {
		t.Errorf("ComboTiers = %v, want [1 2 5]", r.ComboTiers)
	}
}

func TestLoadSharingRules_ComboTiersAllEmptyFallsBackToDefault(t *testing.T) {
	t.Setenv("CHORA_SHARING_COMBO_TIERS", ",")
	r, err := LoadSharingRules()
	if err != nil {
		t.Fatalf("LoadSharingRules: %v", err)
	}
	if len(r.ComboTiers) != 4 || r.ComboTiers[0] != 1 || r.ComboTiers[3] != 5 {
		t.Errorf("ComboTiers = %v, want default [1 2 3 5]", r.ComboTiers)
	}
}

func TestComboMultiplierForStreak_EmptyTiersReturnsOne(t *testing.T) {
	if got := (SharingRules{}).ComboMultiplierForStreak(7); got != 1 {
		t.Fatalf("empty ComboTiers should yield 1, got %d", got)
	}
}

func TestSharingRulesValidate_EachFailureBranch(t *testing.T) {
	valid := func() SharingRules {
		return SharingRules{
			LicenseVocab:              defaultLicenseVocab(),
			ELOBaseline:               1200,
			ELOKFactor:                32,
			ComboTiers:                []int{1, 2, 3, 5},
			RoyaltyBase:               10,
			RoyaltyCap:                0,
			RoyaltyCurrency:           "reputation",
			ManaActionCode:            "atom_royalty",
			LiveQuizPerSessionCap:     0,
			SeasonWindow:              30 * 24 * time.Hour,
			WebSocketGrace:            30 * time.Second,
			RoundTimerSec:             15,
			MatchmakingTimeout:        90 * time.Second,
			MatchmakingHeartbeatStale: 30 * time.Second,
			MatchmakingMatchTick:      2 * time.Second,
			MatchmakingSweepTick:      15 * time.Second,
			BlitzTimeLimitSec:         120,
			BlitzRaceTarget:           5,
		}
	}
	if err := valid().validate(); err != nil {
		t.Fatalf("base rules should validate, got %v", err)
	}

	cases := []struct {
		name   string
		mutate func(r *SharingRules)
	}{
		{"elo baseline", func(r *SharingRules) { r.ELOBaseline = 0 }},
		{"elo k factor", func(r *SharingRules) { r.ELOKFactor = 0 }},
		{"combo tiers empty", func(r *SharingRules) { r.ComboTiers = nil }},
		{"combo tier non-positive", func(r *SharingRules) { r.ComboTiers = []int{1, 0, 3} }},
		{"royalty base negative", func(r *SharingRules) { r.RoyaltyBase = -1 }},
		{"royalty cap negative", func(r *SharingRules) { r.RoyaltyCap = -1 }},
		{"royalty currency empty", func(r *SharingRules) { r.RoyaltyCurrency = "" }},
		{"mana action code empty", func(r *SharingRules) { r.ManaActionCode = "" }},
		{"livequiz cap negative", func(r *SharingRules) { r.LiveQuizPerSessionCap = -1 }},
		{"season window zero", func(r *SharingRules) { r.SeasonWindow = 0 }},
		{"ws grace zero", func(r *SharingRules) { r.WebSocketGrace = 0 }},
		{"round timer zero", func(r *SharingRules) { r.RoundTimerSec = 0 }},
		{"mm timeout zero", func(r *SharingRules) { r.MatchmakingTimeout = 0 }},
		{"mm heartbeat stale zero", func(r *SharingRules) { r.MatchmakingHeartbeatStale = 0 }},
		{"mm match tick zero", func(r *SharingRules) { r.MatchmakingMatchTick = 0 }},
		{"mm sweep tick zero", func(r *SharingRules) { r.MatchmakingSweepTick = 0 }},
		{"blitz race target zero", func(r *SharingRules) { r.BlitzRaceTarget = 0 }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := valid()
			c.mutate(&r)
			if err := r.validate(); err == nil {
				t.Fatalf("expected validate error for %s", c.name)
			}
		})
	}
}