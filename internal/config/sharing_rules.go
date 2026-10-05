// Package config — SharingRules is the config-driven rules loader for the
// Atom Sharing Redesign (specs/001-atom-sharing-redesign) T012.
//
// All rule values are config-driven (no inline config per
// feedback_no_inline_config + CLAUDE.md "No inline config" invariant): they
// ship as env vars in the chora-sharing-engines ConfigMap
// (chora-infra/k8s/services/chora-sharing/configmap-engines.yaml) and are
// read here into a typed, validated struct that the US1–US6 domain +
// adapter code consumes. Rotating a tier or cap never requires a code change
// or Deployment edit — only a ConfigMap patch.
//
// Source-of-truth: data-model.md (ELO baseline 1200 / K=32 per R-10; combo
// tiers 1/2/3/5; royalty base/cap; LiveQuiz per-session cap; season window;
// WS grace 30s) + ADR-178 (price-plan rules) + ADR-168 (WS sticky-session).
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// SharingRules bundles every config-driven rule for the sharing redesign.
// Zero-value SharingRules is INVALID — always construct via LoadSharingRules
// (which applies the documented defaults + validates).
type SharingRules struct {
	// LicenseVocab is the accepted license_terms label set. A share/grant
	// whose license_terms is not in this set is refused loud (FR-005).
	// Default: free, royalty_pct, royalty_fixed, cc_by_sa, cc_nd (mirrors the
	// atom_license_terms PG enum + proto LicenseTerms).
	LicenseVocab map[string]struct{}

	// ELO baseline + K-factor. R-10: REUSE shipped duel_ratings which already
	// defaults rating=1200 + k_factor=32. These MUST match the shipped DB
	// defaults or ELO updates diverge from the audit in duel_match_history.
	ELOBaseline int
	ELOKFactor  int

	// ComboMultiplier tiers — consecutive correct escalates 1->2->3->5; a wrong
	// answer resets to 0. The next tier is the next value in this ordered
	// slice; past the last tier the multiplier holds at the last value.
	ComboTiers []int

	// RoyaltyRule base points x combo (config-driven ADR-178). RoyaltyBase
	// is the per-use amount floor; RoyaltyCap is the per-settlement ceiling
	// (0 = uncapped).
	RoyaltyBase int
	RoyaltyCap  int

	// RoyaltyCurrency is the non-cash author credit currency recorded in
	// currency_balances (§11). Default "reputation". Mana DEBIT to the
	// reuser tenant goes through chora-identity ManaService (gRPC).
	RoyaltyCurrency string

	// ManaActionCode is the price-plan rule key for the mana debit on royalty
	// settlement (§11). Default "atom_royalty".
	ManaActionCode string

	// LiveQuizPerSessionCap is the per-learner royalty cap for a single
	// LiveQuiz session (R-20 — royalty settles at ARM). 0 = uncapped.
	LiveQuizPerSessionCap int

	// SeasonWindow is the leaderboard rolling-season length. A season
	// recompute prunes scores older than this window.
	SeasonWindow time.Duration

	// WebSocketGrace is the disconnect grace before a duel round is forfeited
	// (ADR-168 sticky-session Redis fan-out). Default 30s per the design.
	WebSocketGrace time.Duration

	// RoundTimerSec is the per-question deadline (WS3 enforced round
	// timer). Stamped on each round when it becomes current. Config-driven
	// per feedback_no_inline_config — no hardcoded 30 in source.
	RoundTimerSec int

	// Matchmaking config (duel-matchmaking-rewrite-plan §4.2).
	// All config-driven per feedback_no_inline_config.
	// WS2/G2: timeout shortened from 10min to 90s now that the countdown
	// is visible — a 10min search is poor UX when the user can see the
	// clock running down.
	MatchmakingTimeout        time.Duration // 90s — how long a searcher stays in the pool
	MatchmakingHeartbeatStale time.Duration // 30s — stale threshold before a searcher is abandoned
	MatchmakingMatchTick      time.Duration // 2s — how often the matching loop runs
	MatchmakingSweepTick      time.Duration // 15s — how often the sweeper loop runs

	// Blitz mode config (duel blitz mode). All config-driven per
	// feedback_no_inline_config. BlitzTimeLimitSec is the default time
	// limit for BlitzVariantTimed duels (most correct within the limit).
	// BlitzRaceTarget is the default target for BlitzVariantRace duels
	// (first to N correct wins). Both are overridable per-queue-request.
	BlitzTimeLimitSec int
	BlitzRaceTarget   int
}

// LoadSharingRules reads the config-driven env vars, applies documented
// defaults for any unset var, and validates the result. Returns a loud error
// (never a silently-degraded config) when a value is malformed — per the
// fail-loud engineering standard (CLAUDE.md: never silently fall back).
func LoadSharingRules() (SharingRules, error) {
	eloBaseline, err := envInt("CHORA_SHARING_ELO_BASELINE", 1200)
	if err != nil {
		return SharingRules{}, fmt.Errorf("config: %w", err)
	}
	eloK, err := envInt("CHORA_SHARING_ELO_K_FACTOR", 32)
	if err != nil {
		return SharingRules{}, fmt.Errorf("config: %w", err)
	}
	comboTiers, err := envIntSlice("CHORA_SHARING_COMBO_TIERS", []int{1, 2, 3, 5})
	if err != nil {
		return SharingRules{}, fmt.Errorf("config: %w", err)
	}
	royaltyBase, err := envInt("CHORA_SHARING_ROYALTY_BASE", 10)
	if err != nil {
		return SharingRules{}, fmt.Errorf("config: %w", err)
	}
	royaltyCap, err := envInt("CHORA_SHARING_ROYALTY_CAP", 0)
	if err != nil {
		return SharingRules{}, fmt.Errorf("config: %w", err)
	}
	royaltyCurrency := strings.TrimSpace(os.Getenv("CHORA_SHARING_ROYALTY_CURRENCY"))
	if royaltyCurrency == "" {
		royaltyCurrency = "reputation"
	}
	manaActionCode := strings.TrimSpace(os.Getenv("CHORA_SHARING_MANA_ACTION_CODE"))
	if manaActionCode == "" {
		manaActionCode = "atom_royalty"
	}
	liveQuizCap, err := envInt("CHORA_SHARING_LIVEQUIZ_PER_SESSION_CAP", 0)
	if err != nil {
		return SharingRules{}, fmt.Errorf("config: %w", err)
	}
	season, err := envDuration("CHORA_SHARING_SEASON_WINDOW", 30*24*time.Hour)
	if err != nil {
		return SharingRules{}, fmt.Errorf("config: %w", err)
	}
	wsGrace, err := envDuration("CHORA_SHARING_WS_GRACE", 30*time.Second)
	if err != nil {
		return SharingRules{}, fmt.Errorf("config: %w", err)
	}
	mmTimeout, err := envDuration("CHORA_SHARING_MM_TIMEOUT", 90*time.Second)
	if err != nil {
		return SharingRules{}, fmt.Errorf("config: %w", err)
	}
	mmStale, err := envDuration("CHORA_SHARING_MM_HEARTBEAT_STALE", 30*time.Second)
	if err != nil {
		return SharingRules{}, fmt.Errorf("config: %w", err)
	}
	mmMatchTick, err := envDuration("CHORA_SHARING_MM_MATCH_TICK", 2*time.Second)
	if err != nil {
		return SharingRules{}, fmt.Errorf("config: %w", err)
	}
	mmSweepTick, err := envDuration("CHORA_SHARING_MM_SWEEP_TICK", 15*time.Second)
	if err != nil {
		return SharingRules{}, fmt.Errorf("config: %w", err)
	}
	roundTimerSec, err := envInt("CHORA_SHARING_ROUND_TIMER_SEC", 15)
	if err != nil {
		return SharingRules{}, fmt.Errorf("config: %w", err)
	}
	blitzTimeLimit, err := envInt("CHORA_SHARING_BLITZ_TIME_LIMIT_SEC", 120)
	if err != nil {
		return SharingRules{}, fmt.Errorf("config: %w", err)
	}
	blitzRaceTarget, err := envInt("CHORA_SHARING_BLITZ_RACE_TARGET", 5)
	if err != nil {
		return SharingRules{}, fmt.Errorf("config: %w", err)
	}
	r := SharingRules{
		LicenseVocab:              defaultLicenseVocab(),
		ELOBaseline:               eloBaseline,
		ELOKFactor:                eloK,
		ComboTiers:                comboTiers,
		RoyaltyBase:               royaltyBase,
		RoyaltyCap:                royaltyCap,
		RoyaltyCurrency:           royaltyCurrency,
		ManaActionCode:            manaActionCode,
		LiveQuizPerSessionCap:     liveQuizCap,
		SeasonWindow:              season,
		WebSocketGrace:            wsGrace,
		RoundTimerSec:             roundTimerSec,
		MatchmakingTimeout:        mmTimeout,
		MatchmakingHeartbeatStale: mmStale,
		MatchmakingMatchTick:      mmMatchTick,
		MatchmakingSweepTick:      mmSweepTick,
		BlitzTimeLimitSec:         blitzTimeLimit,
		BlitzRaceTarget:           blitzRaceTarget,
	}
	if err := r.validate(); err != nil {
		return SharingRules{}, fmt.Errorf("config: sharing rules: %w", err)
	}
	return r, nil
}

// ComboMultiplierForStreak returns the multiplier for a given consecutive-
// correct streak count. streak<0 is treated as 0. Past the last tier the
// multiplier holds at the last tier value (so a 10-streak still scores at
// the top tier, not 0).
func (r SharingRules) ComboMultiplierForStreak(streak int) int {
	if streak < 0 {
		streak = 0
	}
	if len(r.ComboTiers) == 0 {
		return 1
	}
	if streak >= len(r.ComboTiers) {
		return r.ComboTiers[len(r.ComboTiers)-1]
	}
	return r.ComboTiers[streak]
}

// IsAcceptedLicense reports whether a license_terms label is in the
// configured vocab.
func (r SharingRules) IsAcceptedLicense(license string) bool {
	_, ok := r.LicenseVocab[license]
	return ok
}

func (r SharingRules) validate() error {
	if r.ELOBaseline <= 0 {
		return errors.New("ELO_BASELINE must be > 0")
	}
	if r.ELOKFactor <= 0 {
		return errors.New("ELO_K_FACTOR must be > 0")
	}
	if len(r.ComboTiers) == 0 {
		return errors.New("COMBO_TIERS must have at least one tier")
	}
	prev := 0
	for i, t := range r.ComboTiers {
		if t <= 0 {
			return fmt.Errorf("COMBO_TIERS[%d] must be > 0", i)
		}
		if t <= prev {
			return fmt.Errorf("COMBO_TIERS must strictly increase at index %d", i)
		}
		prev = t
	}
	if r.RoyaltyBase < 0 {
		return errors.New("ROYALTY_BASE must be >= 0")
	}
	if r.RoyaltyCap < 0 {
		return errors.New("ROYALTY_CAP must be >= 0 (0 = uncapped)")
	}
	if r.RoyaltyCurrency == "" {
		return errors.New("ROYALTY_CURRENCY must be non-empty")
	}
	if r.ManaActionCode == "" {
		return errors.New("MANA_ACTION_CODE must be non-empty")
	}
	if r.LiveQuizPerSessionCap < 0 {
		return errors.New("LIVEQUIZ_PER_SESSION_CAP must be >= 0 (0 = uncapped)")
	}
	if r.SeasonWindow <= 0 {
		return errors.New("SEASON_WINDOW must be > 0")
	}
	if r.WebSocketGrace <= 0 {
		return errors.New("WS_GRACE must be > 0")
	}
	if r.RoundTimerSec <= 0 {
		return errors.New("ROUND_TIMER_SEC must be > 0")
	}
	if r.MatchmakingTimeout <= 0 {
		return errors.New("MM_TIMEOUT must be > 0")
	}
	if r.MatchmakingHeartbeatStale <= 0 {
		return errors.New("MM_HEARTBEAT_STALE must be > 0")
	}
	if r.MatchmakingMatchTick <= 0 {
		return errors.New("MM_MATCH_TICK must be > 0")
	}
	if r.MatchmakingSweepTick <= 0 {
		return errors.New("MM_SWEEP_TICK must be > 0")
	}
	if r.BlitzTimeLimitSec <= 0 {
		return errors.New("BLITZ_TIME_LIMIT_SEC must be > 0")
	}
	if r.BlitzRaceTarget <= 0 {
		return errors.New("BLITZ_RACE_TARGET must be > 0")
	}
	return nil
}

// defaultLicenseVocab mirrors the atom_license_terms PG enum (greenfield
// migration 0001) + the proto LicenseTerms label set (lowercase domain labels).
func defaultLicenseVocab() map[string]struct{} {
	v := map[string]struct{}{}
	for _, l := range []string{"free", "royalty_pct", "royalty_fixed", "cc_by_sa", "cc_nd"} {
		v[l] = struct{}{}
	}
	return v
}

// envInt reads an int env var with a default. An UNSET var yields the
// default; a non-empty but malformed value is a loud error (fail-loud —
// never silently fall back, per CLAUDE.md).
func envInt(key string, def int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: not an int (%q): %w", key, v, err)
	}
	return n, nil
}

// envIntSlice reads a comma-separated int slice env var with a default.
// Malformed entries are a loud error.
func envIntSlice(key string, def []int) ([]int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	parts := strings.Split(v, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("%s: not an int (%q): %w", key, p, err)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return def, nil
	}
	return out, nil
}

// envDuration reads a duration env var with a default. Malformed values are
// a loud error.
func envDuration(key string, def time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: not a duration (%q): %w", key, v, err)
	}
	return d, nil
}
