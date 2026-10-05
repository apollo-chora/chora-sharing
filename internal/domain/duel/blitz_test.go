package duel

import (
	"errors"
	"testing"
	"time"
)

// --- Blitz mode domain tests (RED phase) ---

func TestNewBlitzPoolDuel_CreatesAcceptedBlitz(t *testing.T) {
	d, err := NewBlitzPoolDuel(
		DuelConfig{},
		"gcid-a", "gcid-b", "tenant-1",
		ScopeRanked, 10, []string{"inheritance"},
		BlitzConfig{Variant: BlitzVariantTimed, TimeLimitSec: 120},
	)
	if err != nil {
		t.Fatalf("NewBlitzPoolDuel: %v", err)
	}
	if d.Mode != ModeBlitz {
		t.Errorf("mode=%s want blitz", d.Mode)
	}
	if d.Status != StatusAccepted {
		t.Errorf("status=%s want accepted (pool model skips pending)", d.Status)
	}
	if d.BlitzConfig.Variant != BlitzVariantTimed {
		t.Errorf("blitz_variant=%s want timed", d.BlitzConfig.Variant)
	}
	if d.BlitzConfig.TimeLimitSec != 120 {
		t.Errorf("blitz_time_limit=%d want 120", d.BlitzConfig.TimeLimitSec)
	}
}

func TestNewBlitzPoolDuel_SelfChallenge(t *testing.T) {
	_, err := NewBlitzPoolDuel(
		DuelConfig{},
		"gcid-a", "gcid-a", "tenant-1",
		ScopeRanked, 10, nil,
		BlitzConfig{Variant: BlitzVariantTimed, TimeLimitSec: 60},
	)
	if err != ErrSelfChallenge {
		t.Errorf("err=%v want ErrSelfChallenge", err)
	}
}

func TestNewBlitzPoolDuel_DefaultsScopeRanked(t *testing.T) {
	d, err := NewBlitzPoolDuel(
		DuelConfig{},
		"gcid-a", "gcid-b", "tenant-1",
		ScopeUnspecified, 10, nil,
		BlitzConfig{Variant: BlitzVariantRace, RaceTarget: 5},
	)
	if err != nil {
		t.Fatalf("NewBlitzPoolDuel: %v", err)
	}
	if d.Scope != ScopeRanked {
		t.Errorf("scope=%s want ranked (pool default)", d.Scope)
	}
}

func TestNewBlitzPoolDuel_RejectsUnspecifiedVariant(t *testing.T) {
	_, err := NewBlitzPoolDuel(
		DuelConfig{},
		"gcid-a", "gcid-b", "tenant-1",
		ScopeRanked, 10, nil,
		BlitzConfig{Variant: BlitzVariantUnspecified},
	)
	if err == nil {
		t.Fatal("NewBlitzPoolDuel with unspecified variant returned nil error (must fail-loud)")
	}
}

func TestStartBattle_BlitzStampsStartedAt(t *testing.T) {
	d := newBlitzTestDuel(t, BlitzVariantTimed, 120, 0)
	picks := []AtomPick{
		{AtomID: "atom-1", Question: "Q1", Options: []string{"A", "B"}, Answer: "A"},
		{AtomID: "atom-2", Question: "Q2", Options: []string{"A", "B"}, Answer: "B"},
	}
	if err := d.StartBattle(picks); err != nil {
		t.Fatalf("StartBattle: %v", err)
	}
	if d.BlitzStartedAt == nil {
		t.Fatal("BlitzStartedAt is nil after StartBattle (must stamp for blitz mode)")
	}
	if d.Status != StatusInProgress {
		t.Errorf("status=%s want in_progress", d.Status)
	}
}

func TestResolveBlitzAnswer_CorrectAwardsPoints(t *testing.T) {
	d := newBlitzTestDuel(t, BlitzVariantTimed, 120, 0)
	_ = d.StartBattle([]AtomPick{
		{AtomID: "atom-1", Question: "Q1", Options: []string{"A", "B"}, Answer: "A"},
		{AtomID: "atom-2", Question: "Q2", Options: []string{"A", "B"}, Answer: "B"},
	})

	res, err := d.ResolveBlitzAnswer("gcid-a", 1, true, 5000, []int{1, 2, 3, 5})
	if err != nil {
		t.Fatalf("ResolveBlitzAnswer: %v", err)
	}
	if !res.Correct {
		t.Error("want correct=true")
	}
	if res.ComboMultiplier != 2 {
		t.Errorf("combo_mult=%d want 2 (first correct → combo=1 → tiers[1]=2)", res.ComboMultiplier)
	}
	if res.PointsAwarded != 25 {
		t.Errorf("points=%d want 25 (10*2 + 5 speed bonus)", res.PointsAwarded)
	}
	// Blitz: a correct answer does NOT resolve the round — the opponent
	// can still answer independently. Only both-answered resolves.
	if res.RoundResolved {
		t.Error("round resolved after only one player answered (blitz keeps round open for opponent)")
	}
	if d.Rounds[0].ResolvedAt != nil {
		t.Error("round 1 ResolvedAt is non-nil after only challenger answered (blitz keeps round open)")
	}
}

func TestResolveBlitzAnswer_BothAnswerResolvesRound(t *testing.T) {
	d := newBlitzTestDuel(t, BlitzVariantTimed, 120, 0)
	_ = d.StartBattle([]AtomPick{
		{AtomID: "atom-1", Question: "Q1", Options: []string{"A", "B"}, Answer: "A"},
		{AtomID: "atom-2", Question: "Q2", Options: []string{"A", "B"}, Answer: "B"},
	})

	// Challenger answers round 1 correct → round stays open.
	res1, err := d.ResolveBlitzAnswer("gcid-a", 1, true, 5000, []int{1, 2, 3, 5})
	if err != nil {
		t.Fatalf("challenger answer: %v", err)
	}
	if res1.RoundResolved {
		t.Error("round resolved after challenger only (should stay open for opponent)")
	}

	// Opponent answers round 1 → round resolves (both answered).
	res2, err := d.ResolveBlitzAnswer("gcid-b", 1, true, 8000, []int{1, 2, 3, 5})
	if err != nil {
		t.Fatalf("opponent answer: %v", err)
	}
	if !res2.RoundResolved {
		t.Error("round not resolved after both players answered")
	}
	if d.Rounds[0].ResolvedAt == nil {
		t.Error("round 1 ResolvedAt is nil after both answered")
	}
}

func TestResolveBlitzAnswer_AlreadyAnswered(t *testing.T) {
	d := newBlitzTestDuel(t, BlitzVariantTimed, 120, 0)
	_ = d.StartBattle([]AtomPick{
		{AtomID: "atom-1", Question: "Q1", Options: []string{"A", "B"}, Answer: "A"},
		{AtomID: "atom-2", Question: "Q2", Options: []string{"A", "B"}, Answer: "B"},
	})

	_, _ = d.ResolveBlitzAnswer("gcid-a", 1, true, 5000, []int{1, 2, 3, 5})
	_, err := d.ResolveBlitzAnswer("gcid-a", 1, true, 3000, []int{1, 2, 3, 5})
	if !errors.Is(err, ErrAlreadyResolved) {
		t.Errorf("re-submit: err=%v want ErrAlreadyResolved", err)
	}
}

func TestResolveBlitzAnswer_WrongResetsCombo(t *testing.T) {
	d := newBlitzTestDuel(t, BlitzVariantTimed, 120, 0)
	_ = d.StartBattle([]AtomPick{
		{AtomID: "atom-1", Question: "Q1", Options: []string{"A", "B"}, Answer: "A"},
		{AtomID: "atom-2", Question: "Q2", Options: []string{"A", "B"}, Answer: "B"},
	})

	// First correct → combo=1.
	_, _ = d.ResolveBlitzAnswer("gcid-a", 1, true, 5000, []int{1, 2, 3, 5})
	if d.ComboChallenger != 1 {
		t.Fatalf("combo after first correct=%d want 1", d.ComboChallenger)
	}

	// Second wrong → combo resets to 0.
	res, err := d.ResolveBlitzAnswer("gcid-a", 2, false, 10000, []int{1, 2, 3, 5})
	if err != nil {
		t.Fatalf("wrong answer: %v", err)
	}
	if res.PointsAwarded != 0 {
		t.Errorf("wrong answer points=%d want 0", res.PointsAwarded)
	}
	if d.ComboChallenger != 0 {
		t.Errorf("combo after wrong=%d want 0 (reset)", d.ComboChallenger)
	}
}

func TestResolveBlitzAnswer_RaceTargetCompletesDuel(t *testing.T) {
	d := newBlitzTestDuel(t, BlitzVariantRace, 0, 2) // race to 2 correct
	_ = d.StartBattle([]AtomPick{
		{AtomID: "atom-1", Question: "Q1", Options: []string{"A", "B"}, Answer: "A"},
		{AtomID: "atom-2", Question: "Q2", Options: []string{"A", "B"}, Answer: "B"},
		{AtomID: "atom-3", Question: "Q3", Options: []string{"A", "B"}, Answer: "A"},
	})

	// Challenger answers round 1 correct (1/2).
	res1, err := d.ResolveBlitzAnswer("gcid-a", 1, true, 5000, []int{1, 2, 3, 5})
	if err != nil {
		t.Fatalf("answer 1: %v", err)
	}
	if res1.DuelStatus == StatusCompleted {
		t.Fatal("duel completed after 1 correct (race target is 2)")
	}

	// Challenger answers round 2 correct (2/2) → duel completes.
	res2, err := d.ResolveBlitzAnswer("gcid-a", 2, true, 7000, []int{1, 2, 3, 5})
	if err != nil {
		t.Fatalf("answer 2: %v", err)
	}
	if res2.DuelStatus != StatusCompleted {
		t.Errorf("duel_status=%s want completed (reached race target 2)", res2.DuelStatus)
	}
	if res2.WinnerGCID != "gcid-a" {
		t.Errorf("winner=%s want gcid-a (first to reach race target)", res2.WinnerGCID)
	}
	if res2.DuelResult == nil {
		t.Fatal("DuelResult nil after race completion")
	}
}

func TestResolveBlitzAnswer_RaceOpponentCanStillWin(t *testing.T) {
	d := newBlitzTestDuel(t, BlitzVariantRace, 0, 2)
	_ = d.StartBattle([]AtomPick{
		{AtomID: "atom-1", Question: "Q1", Options: []string{"A", "B"}, Answer: "A"},
		{AtomID: "atom-2", Question: "Q2", Options: []string{"A", "B"}, Answer: "B"},
		{AtomID: "atom-3", Question: "Q3", Options: []string{"A", "B"}, Answer: "A"},
	})

	// Challenger gets 1 correct, opponent gets 2 correct first.
	_, _ = d.ResolveBlitzAnswer("gcid-a", 1, true, 5000, []int{1, 2, 3, 5})
	_, _ = d.ResolveBlitzAnswer("gcid-b", 1, true, 6000, []int{1, 2, 3, 5})
	res, err := d.ResolveBlitzAnswer("gcid-b", 2, true, 4000, []int{1, 2, 3, 5})
	if err != nil {
		t.Fatalf("opponent answer 2: %v", err)
	}
	if res.DuelStatus != StatusCompleted {
		t.Errorf("duel_status=%s want completed", res.DuelStatus)
	}
	if res.WinnerGCID != "gcid-b" {
		t.Errorf("winner=%s want gcid-b (opponent reached race target first)", res.WinnerGCID)
	}
}

func TestCompleteBlitzTimed_CompletesDuel(t *testing.T) {
	d := newBlitzTestDuel(t, BlitzVariantTimed, 120, 0)
	_ = d.StartBattle([]AtomPick{
		{AtomID: "atom-1", Question: "Q1", Options: []string{"A", "B"}, Answer: "A"},
		{AtomID: "atom-2", Question: "Q2", Options: []string{"A", "B"}, Answer: "B"},
	})

	// Challenger answers round 1 correct, round 2 unanswered.
	_, _ = d.ResolveBlitzAnswer("gcid-a", 1, true, 5000, []int{1, 2, 3, 5})

	// Time expires (120s after blitz start).
	expired := d.BlitzStartedAt.Add(121 * time.Second)
	res, err := d.CompleteBlitzTimed(expired)
	if err != nil {
		t.Fatalf("CompleteBlitzTimed: %v", err)
	}
	if res.DuelStatus != StatusCompleted {
		t.Errorf("duel_status=%s want completed", res.DuelStatus)
	}
	if res.DuelResult == nil {
		t.Fatal("DuelResult nil after timed completion")
	}
	// Challenger answered round 1 correct (25 pts); round 2 unanswered (0 pts).
	// Opponent answered nothing (0 pts). Winner = challenger.
	if res.WinnerGCID != "gcid-a" {
		t.Errorf("winner=%s want gcid-a (higher score)", res.WinnerGCID)
	}
}

func TestCompleteBlitzTimed_RejectsBeforeExpiry(t *testing.T) {
	d := newBlitzTestDuel(t, BlitzVariantTimed, 120, 0)
	_ = d.StartBattle([]AtomPick{
		{AtomID: "atom-1", Question: "Q1", Options: []string{"A", "B"}, Answer: "A"},
	})

	// 60s in — time limit is 120s. Must NOT complete.
	early := d.BlitzStartedAt.Add(60 * time.Second)
	_, err := d.CompleteBlitzTimed(early)
	if err == nil {
		t.Fatal("CompleteBlitzTimed before expiry returned nil error (must fail-loud)")
	}
}

func TestCompleteBlitzTimed_DrawOnEqualScore(t *testing.T) {
	d := newBlitzTestDuel(t, BlitzVariantTimed, 60, 0)
	_ = d.StartBattle([]AtomPick{
		{AtomID: "atom-1", Question: "Q1", Options: []string{"A", "B"}, Answer: "A"},
		{AtomID: "atom-2", Question: "Q2", Options: []string{"A", "B"}, Answer: "B"},
	})

	// Both answer round 1 correct (same speed → same points).
	_, _ = d.ResolveBlitzAnswer("gcid-a", 1, true, 5000, []int{1, 2, 3, 5})
	_, _ = d.ResolveBlitzAnswer("gcid-b", 1, true, 5000, []int{1, 2, 3, 5})

	expired := d.BlitzStartedAt.Add(61 * time.Second)
	res, err := d.CompleteBlitzTimed(expired)
	if err != nil {
		t.Fatalf("CompleteBlitzTimed: %v", err)
	}
	if res.DuelResult == nil || res.DuelResult.Outcome != OutcomeDraw {
		t.Errorf("outcome=%v want draw (equal scores)", res.DuelResult)
	}
}

func TestResolveBlitzAnswer_NotInProgress(t *testing.T) {
	d := newBlitzTestDuel(t, BlitzVariantTimed, 120, 0)
	// Duel is accepted but not in_progress (StartBattle not called).
	_, err := d.ResolveBlitzAnswer("gcid-a", 1, true, 5000, []int{1, 2, 3, 5})
	if err == nil {
		t.Fatal("ResolveBlitzAnswer before StartBattle returned nil error (must fail-loud)")
	}
}

func TestResolveBlitzAnswer_NotParticipant(t *testing.T) {
	d := newBlitzTestDuel(t, BlitzVariantTimed, 120, 0)
	_ = d.StartBattle([]AtomPick{
		{AtomID: "atom-1", Question: "Q1", Options: []string{"A", "B"}, Answer: "A"},
	})
	_, err := d.ResolveBlitzAnswer("gcid-c", 1, true, 5000, []int{1, 2, 3, 5})
	if err != ErrNotParticipant {
		t.Errorf("err=%v want ErrNotParticipant", err)
	}
}

func TestResolveBlitzAnswer_RoundOutOfRange(t *testing.T) {
	d := newBlitzTestDuel(t, BlitzVariantTimed, 120, 0)
	_ = d.StartBattle([]AtomPick{
		{AtomID: "atom-1", Question: "Q1", Options: []string{"A", "B"}, Answer: "A"},
	})
	_, err := d.ResolveBlitzAnswer("gcid-a", 5, true, 5000, []int{1, 2, 3, 5})
	if err == nil {
		t.Fatal("ResolveBlitzAnswer round 5 (max 1) returned nil error (must fail-loud)")
	}
}

func TestIsBlitz(t *testing.T) {
	classic := newTestDuel(t)
	if classic.IsBlitz() {
		t.Error("classic duel reports IsBlitz=true")
	}
	blitz := newBlitzTestDuel(t, BlitzVariantTimed, 60, 0)
	if !blitz.IsBlitz() {
		t.Error("blitz duel reports IsBlitz=false")
	}
}

// --- helpers ---

func newBlitzTestDuel(t *testing.T, variant BlitzVariant, timeLimitSec, raceTarget int) *Duel {
	t.Helper()
	cfg := BlitzConfig{Variant: variant}
	if variant == BlitzVariantTimed {
		cfg.TimeLimitSec = timeLimitSec
	} else if variant == BlitzVariantRace {
		cfg.RaceTarget = raceTarget
	}
	d, err := NewBlitzPoolDuel(
		DuelConfig{},
		"gcid-a", "gcid-b", "tenant-1",
		ScopeRanked, 5, []string{"inheritance"},
		cfg,
	)
	if err != nil {
		t.Fatalf("NewBlitzPoolDuel: %v", err)
	}
	return d
}
