package duel

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestNewDuel_Valid(t *testing.T) {
	d, err := NewDuel(DuelConfig{}, "gcid-a", "gcid-b", "tenant-1", ScopeFriendly, 5, []string{"inheritance"}, 24*time.Hour)
	if err != nil {
		t.Fatalf("NewDuel: %v", err)
	}
	if d.Status != StatusPending {
		t.Errorf("status=%s want pending", d.Status)
	}
	if d.ChallengerGCID != "gcid-a" {
		t.Errorf("challenger=%s want gcid-a", d.ChallengerGCID)
	}
	if d.RoundCount != 5 {
		t.Errorf("round_count=%d want 5", d.RoundCount)
	}
	if len(d.InterestTags) != 1 || d.InterestTags[0] != "inheritance" {
		t.Errorf("interest_tags=%v want [inheritance]", d.InterestTags)
	}
}

func TestNewDuel_SelfChallenge(t *testing.T) {
	_, err := NewDuel(DuelConfig{}, "gcid-a", "gcid-a", "tenant-1", ScopeFriendly, 5, nil, 0)
	if err != ErrSelfChallenge {
		t.Errorf("err=%v want ErrSelfChallenge", err)
	}
}

func TestNewDuel_Defaults(t *testing.T) {
	d, err := NewDuel(DuelConfig{}, "gcid-a", "gcid-b", "tenant-1", ScopeUnspecified, 0, nil, 0)
	if err != nil {
		t.Fatalf("NewDuel: %v", err)
	}
	if d.Scope != ScopeFriendly {
		t.Errorf("scope=%s want friendly", d.Scope)
	}
	if d.RoundCount != 5 {
		t.Errorf("round_count=%d want 5 (default)", d.RoundCount)
	}
	if d.ExpiresAt == nil {
		t.Error("expires_at should be set")
	}
}

func TestAcceptDuel(t *testing.T) {
	d := newTestDuel(t)
	if err := d.Accept("gcid-b"); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if d.Status != StatusAccepted {
		t.Errorf("status=%s want accepted", d.Status)
	}
}

func TestAcceptDuel_NotOpponent(t *testing.T) {
	d := newTestDuel(t)
	if err := d.Accept("gcid-c"); err != ErrNotParticipant {
		t.Errorf("err=%v want ErrNotParticipant", err)
	}
}

func TestStartBattle(t *testing.T) {
	d := newTestDuel(t)
	if err := d.Accept("gcid-b"); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	picks := []AtomPick{
		{AtomID: "atom-1", RevisionID: "rev-1", Question: "What is inheritance?", Options: []string{"A", "B"}, Answer: "A"},
		{AtomID: "atom-2", RevisionID: "rev-2", Question: "What is polymorphism?", Options: []string{"A", "B"}, Answer: "B"},
	}
	if err := d.StartBattle(picks); err != nil {
		t.Fatalf("StartBattle: %v", err)
	}
	if d.Status != StatusInProgress {
		t.Errorf("status=%s want in_progress", d.Status)
	}
	if len(d.Rounds) != 2 {
		t.Errorf("rounds=%d want 2", len(d.Rounds))
	}
	// Embedded content (Question/Options/CorrectAnswer) must be copied
	// from each AtomPick so generated atoms (no projection row) can be
	// served without a cross-domain lookup. Options are SHUFFLED at
	// StartBattle so the correct answer lands at a random position
	// (not the atom author's original ordering). The test asserts:
	//   - Question is preserved verbatim
	//   - Options are a permutation of the input (same set, any order)
	//   - CorrectAnswer is one of the shuffled Options (case-insensitive)
	for i, want := range picks {
		rd := d.Rounds[i]
		if rd.Question != want.Question {
			t.Errorf("round %d Question=%q want %q", i+1, rd.Question, want.Question)
		}
		if len(rd.Options) != len(want.Options) {
			t.Errorf("round %d Options len=%d want %d", i+1, len(rd.Options), len(want.Options))
			continue
		}
		// Options are a permutation of the input — assert set equality,
		// not positional equality (shuffling reorders them).
		wantSet := make(map[string]int)
		for _, opt := range want.Options {
			wantSet[opt]++
		}
		for _, opt := range rd.Options {
			wantSet[opt]--
			if wantSet[opt] < 0 {
				t.Errorf("round %d Options contains %q not in input %v", i+1, opt, want.Options)
			}
		}
		// CorrectAnswer must be one of the shuffled Options (case-insensitive
		// — matchOption resolves it verbatim, shuffleOptions preserves it).
		answerMatched := false
		for _, opt := range rd.Options {
			if strings.EqualFold(strings.TrimSpace(opt), strings.TrimSpace(rd.CorrectAnswer)) {
				answerMatched = true
				break
			}
		}
		if !answerMatched {
			t.Errorf("round %d CorrectAnswer=%q not found in shuffled Options %v", i+1, rd.CorrectAnswer, rd.Options)
		}
	}
}

// TestStartBattle_ShufflesOptions is the regression test for the "all
// answers are B" bug: without shuffling, the atom author's original
// option ordering leaks + the correct answer always lands at the same
// index. This test runs StartBattle many times with the same picks +
// asserts the correct answer lands at different positions across runs
// (statistical — with 4 options, each position should appear at least
// once across 200 runs with overwhelming probability).
func TestStartBattle_ShufflesOptions(t *testing.T) {
	// 4 options, correct answer at index 0 ("A") in the input.
	picks := []AtomPick{{
		AtomID: "atom-1", Question: "Q", Options: []string{"A", "B", "C", "D"}, Answer: "A",
	}}
	positions := make(map[int]int) // answer index → count
	for run := 0; run < 200; run++ {
		d := newTestDuel(t)
		if err := d.Accept("gcid-b"); err != nil {
			t.Fatalf("Accept: %v", err)
		}
		if err := d.StartBattle(picks); err != nil {
			t.Fatalf("StartBattle: %v", err)
		}
		rd := d.Rounds[0]
		for i, opt := range rd.Options {
			if strings.EqualFold(strings.TrimSpace(opt), strings.TrimSpace(rd.CorrectAnswer)) {
				positions[i]++
				break
			}
		}
	}
	// With 4 options + 200 runs, each position should appear ~50 times.
	// Assert each position appears at least once — if the shuffle were
	// broken (always returning the input order), only position 0 would
	// have counts.
	if len(positions) < 4 {
		t.Errorf("shuffle produced only %d distinct answer positions (want 4); distribution=%v", len(positions), positions)
	}
	for i := 0; i < 4; i++ {
		if positions[i] == 0 {
			t.Errorf("answer position %d never appeared in 200 runs; distribution=%v", i, positions)
		}
	}
}

func TestResolveRound_Correct(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	picks := []AtomPick{
		{AtomID: "atom-1", Question: "Q1", Options: []string{"A", "B"}, Answer: "A"},
		{AtomID: "atom-2", Question: "Q2", Options: []string{"A", "B"}, Answer: "B"},
	}
	_ = d.StartBattle(picks)

	res, err := d.ResolveRound("gcid-a", 1, true, 10000, []int{1, 2, 3, 5})
	if err != nil {
		t.Fatalf("ResolveRound: %v", err)
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
	if res.DuelStatus != StatusInProgress {
		t.Errorf("duel_status=%s want in_progress (opponent hasn't answered)", res.DuelStatus)
	}
}

func TestResolveRound_ComboEscalation(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	picks := []AtomPick{
		{AtomID: "atom-1", Question: "Q1"},
		{AtomID: "atom-2", Question: "Q2"},
		{AtomID: "atom-3", Question: "Q3"},
		{AtomID: "atom-4", Question: "Q4"},
	}
	_ = d.StartBattle(picks)
	tiers := []int{1, 2, 3, 5}

	for i := 1; i <= 3; i++ {
		_, _ = d.ResolveRound("gcid-a", i, true, 0, tiers)
		_, _ = d.ResolveRound("gcid-b", i, false, 0, tiers)
	}

	if d.ComboChallenger != 3 {
		t.Errorf("combo=%d want 3", d.ComboChallenger)
	}
}

func TestResolveRound_CompleteDuel(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	picks := []AtomPick{
		{AtomID: "atom-1", Question: "Q1"},
	}
	_ = d.StartBattle(picks)

	// FCFS: a correct answer resolves the round immediately. With only
	// one round, the duel completes on the first correct answer — the
	// opponent never needs to answer.
	res1, err := d.ResolveRound("gcid-a", 1, true, 0, []int{1, 2, 3, 5})
	if err != nil {
		t.Fatalf("ResolveRound challenger correct: %v", err)
	}
	if res1.DuelStatus != StatusCompleted {
		t.Errorf("after challenger correct (FCFS): status=%s want completed", res1.DuelStatus)
	}
	if d.WinnerGCID != "gcid-a" {
		t.Errorf("winner=%s want gcid-a", d.WinnerGCID)
	}
}
func TestResolveRound_AlreadyResolved(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	// 2 rounds: A correct on round 1 resolves round 1 but NOT the duel
	// (round 2 remains). A second attempt on round 1 must be rejected as
	// already resolved (not as "not in progress" — the duel is still live).
	picks := []AtomPick{
		{AtomID: "atom-1", Question: "Q1"},
		{AtomID: "atom-2", Question: "Q2"},
	}
	_ = d.StartBattle(picks)

	if _, err := d.ResolveRound("gcid-a", 1, true, 0, []int{1, 2, 3, 5}); err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	_, err := d.ResolveRound("gcid-a", 1, true, 0, []int{1, 2, 3, 5})
	if !errors.Is(err, ErrAlreadyResolved) {
		t.Errorf("err=%v want ErrAlreadyResolved (round resolved by correct answer; duel still in_progress)", err)
	}
}

func TestComputeELO(t *testing.T) {
	winner, loser := ComputeELO(1200, 1200, 32)
	if winner <= 1200 {
		t.Errorf("winner=%d should be > 1200", winner)
	}
	if loser >= 1200 {
		t.Errorf("loser=%d should be < 1200", loser)
	}
	if winner+loser != 2400 {
		t.Errorf("zero-sum violated: %d+%d=%d want 2400", winner, loser, winner+loser)
	}
}

func TestComboMultiplierForStreak(t *testing.T) {
	tiers := []int{1, 2, 3, 5}
	tests := []struct {
		streak int
		want   int
	}{
		{0, 1},
		{1, 2},
		{2, 3},
		{3, 5},
		{10, 5},
	}
	for _, tt := range tests {
		got := ComboMultiplierForStreak(tt.streak, tiers)
		if got != tt.want {
			t.Errorf("streak=%d: got %d want %d", tt.streak, got, tt.want)
		}
	}
}

func TestForfeit(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1", Question: "Q1"}})

	if err := d.Forfeit("gcid-a"); err != nil {
		t.Fatalf("Forfeit: %v", err)
	}
	if d.Status != StatusForfeited {
		t.Errorf("status=%s want forfeited", d.Status)
	}
	if d.WinnerGCID != "gcid-b" {
		t.Errorf("winner=%s want gcid-b", d.WinnerGCID)
	}
}

func TestNewPoolDuel_CreatesAccepted(t *testing.T) {
	d, err := NewPoolDuel(DuelConfig{}, "gcid-a", "gcid-b", "tenant-1", ScopeRanked, 5, []string{"inheritance"})
	if err != nil {
		t.Fatalf("NewPoolDuel: %v", err)
	}
	if d.Status != StatusAccepted {
		t.Errorf("status=%s want accepted (pool model skips pending)", d.Status)
	}
	if d.Scope != ScopeRanked {
		t.Errorf("scope=%s want ranked (pool matchmaking is competitive)", d.Scope)
	}
	picks := []AtomPick{{AtomID: "atom-1", Question: "Q1"}}
	if err := d.StartBattle(picks); err != nil {
		t.Fatalf("StartBattle after NewPoolDuel: %v (state-machine deadlock — §2.1)", err)
	}
	if d.Status != StatusInProgress {
		t.Errorf("status=%s want in_progress after StartBattle", d.Status)
	}
}

func TestNewPoolDuel_SelfChallenge(t *testing.T) {
	_, err := NewPoolDuel(DuelConfig{}, "gcid-a", "gcid-a", "tenant-1", ScopeRanked, 5, nil)
	if err != ErrSelfChallenge {
		t.Errorf("err=%v want ErrSelfChallenge", err)
	}
}

func TestNewPoolDuel_Defaults(t *testing.T) {
	d, err := NewPoolDuel(DuelConfig{}, "gcid-a", "gcid-b", "tenant-1", ScopeUnspecified, 0, nil)
	if err != nil {
		t.Fatalf("NewPoolDuel: %v", err)
	}
	if d.Scope != ScopeRanked {
		t.Errorf("scope=%s want ranked (pool default)", d.Scope)
	}
	if d.RoundCount != 5 {
		t.Errorf("round_count=%d want 5 (default)", d.RoundCount)
	}
}

func newTestDuel(t *testing.T) *Duel {
	t.Helper()
	d, err := NewDuel(DuelConfig{}, "gcid-a", "gcid-b", "tenant-1", ScopeFriendly, 5, []string{"inheritance"}, 24*time.Hour)
	if err != nil {
		t.Fatalf("NewDuel: %v", err)
	}
	return d
}

// --- DuelResult + RewardConfig tests ---

func TestDuelResult_Winner(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1", Question: "Q1"}})
	_, _ = d.ResolveRound("gcid-a", 1, true, 0, []int{1, 2, 3, 5})
	_, _ = d.ResolveRound("gcid-b", 1, false, 0, []int{1, 2, 3, 5})

	res := d.Result()
	if res.Outcome != OutcomeWin {
		t.Errorf("outcome=%s want win", res.Outcome)
	}
	if res.WinnerGCID != "gcid-a" {
		t.Errorf("winner=%s want gcid-a", res.WinnerGCID)
	}
	if res.OutcomeFor("gcid-a") != OutcomeWin {
		t.Errorf("challenger outcome=%s want win", res.OutcomeFor("gcid-a"))
	}
	if res.OutcomeFor("gcid-b") != OutcomeLoss {
		t.Errorf("opponent outcome=%s want loss", res.OutcomeFor("gcid-b"))
	}
}

func TestDuelResult_Draw(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1", Question: "Q1"}})
	// Both answer incorrectly → equal scores → draw.
	_, _ = d.ResolveRound("gcid-a", 1, false, 0, []int{1, 2, 3, 5})
	_, _ = d.ResolveRound("gcid-b", 1, false, 0, []int{1, 2, 3, 5})

	res := d.Result()
	if res.Outcome != OutcomeDraw {
		t.Errorf("outcome=%s want draw", res.Outcome)
	}
	if res.WinnerGCID != "" {
		t.Errorf("winner=%s want empty for draw", res.WinnerGCID)
	}
	if res.OutcomeFor("gcid-a") != OutcomeDraw {
		t.Errorf("challenger outcome=%s want draw", res.OutcomeFor("gcid-a"))
	}
	if res.OutcomeFor("gcid-b") != OutcomeDraw {
		t.Errorf("opponent outcome=%s want draw", res.OutcomeFor("gcid-b"))
	}
}

func TestDuelResult_Forfeit(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1", Question: "Q1"}})
	_ = d.Forfeit("gcid-a")

	res := d.Result()
	if res.Outcome != OutcomeForfeit {
		t.Errorf("outcome=%s want forfeit", res.Outcome)
	}
	if res.WinnerGCID != "gcid-b" {
		t.Errorf("winner=%s want gcid-b", res.WinnerGCID)
	}
	if res.OutcomeFor("gcid-b") != OutcomeWin {
		t.Errorf("winner outcome=%s want win", res.OutcomeFor("gcid-b"))
	}
	if res.OutcomeFor("gcid-a") != OutcomeForfeit {
		t.Errorf("forfeiter outcome=%s want forfeit", res.OutcomeFor("gcid-a"))
	}
}

func TestRewardFor_Winner(t *testing.T) {
	cfg := DefaultRewardConfig()
	res := DuelResult{Outcome: OutcomeWin, WinnerGCID: "gcid-a"}
	if got := res.RewardFor("gcid-a", cfg); got != cfg.WinnerCoins {
		t.Errorf("winner reward=%d want %d", got, cfg.WinnerCoins)
	}
	if got := res.RewardFor("gcid-b", cfg); got != cfg.LoserCoins {
		t.Errorf("loser reward=%d want %d", got, cfg.LoserCoins)
	}
}

func TestRewardFor_Draw(t *testing.T) {
	cfg := DefaultRewardConfig()
	res := DuelResult{Outcome: OutcomeDraw}
	if got := res.RewardFor("gcid-a", cfg); got != cfg.DrawCoins {
		t.Errorf("draw reward=%d want %d", got, cfg.DrawCoins)
	}
	if got := res.RewardFor("gcid-b", cfg); got != cfg.DrawCoins {
		t.Errorf("draw reward=%d want %d", got, cfg.DrawCoins)
	}
}

func TestRewardFor_Forfeit(t *testing.T) {
	cfg := DefaultRewardConfig()
	res := DuelResult{Outcome: OutcomeForfeit, WinnerGCID: "gcid-b"}
	if got := res.RewardFor("gcid-b", cfg); got != cfg.WinnerCoins {
		t.Errorf("forfeit winner reward=%d want %d", got, cfg.WinnerCoins)
	}
	if got := res.RewardFor("gcid-a", cfg); got != 0 {
		t.Errorf("forfeiter reward=%d want 0", got)
	}
}

func TestDefaultRewardConfig(t *testing.T) {
	cfg := DefaultRewardConfig()
	if cfg.WinnerCoins <= cfg.LoserCoins {
		t.Errorf("winner coins (%d) should exceed loser coins (%d)", cfg.WinnerCoins, cfg.LoserCoins)
	}
	if cfg.DrawCoins <= cfg.LoserCoins {
		t.Errorf("draw coins (%d) should exceed loser coins (%d)", cfg.DrawCoins, cfg.LoserCoins)
	}
	if cfg.DrawCoins >= cfg.WinnerCoins {
		t.Errorf("draw coins (%d) should be less than winner coins (%d)", cfg.DrawCoins, cfg.WinnerCoins)
	}
}

func TestSpeedBonus_TriggersOnFastAnswer(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1", Question: "Q1"}})

	// answerTimeMs < timerMs/2 (15000) → speed bonus should trigger.
	res, _ := d.ResolveRound("gcid-a", 1, true, 5000, []int{1, 2, 3, 5})
	if res.SpeedBonus != 5 {
		t.Errorf("speed_bonus=%d want 5 for fast answer (5000ms)", res.SpeedBonus)
	}
}

func TestSpeedBonus_NoTriggerOnSlowAnswer(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1", Question: "Q1"}})

	// answerTimeMs > timerMs/2 (15000) → no speed bonus.
	res, _ := d.ResolveRound("gcid-a", 1, true, 20000, []int{1, 2, 3, 5})
	if res.SpeedBonus != 0 {
		t.Errorf("speed_bonus=%d want 0 for slow answer (20000ms)", res.SpeedBonus)
	}
}

func TestResolveRound_DuelResultPopulated(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1", Question: "Q1"}})

	// FCFS: A's correct answer resolves the round AND completes the duel
	// (single round). The DuelResult is populated on A's resolution —
	// B never gets to answer a resolved round.
	res, err := d.ResolveRound("gcid-a", 1, true, 0, []int{1, 2, 3, 5})
	if err != nil {
		t.Fatalf("ResolveRound challenger correct: %v", err)
	}
	if res.DuelResult == nil {
		t.Fatal("DuelResult is nil after FCFS completion")
	}
	if res.DuelResult.WinnerGCID != "gcid-a" {
		t.Errorf("DuelResult.WinnerGCID=%s want gcid-a", res.DuelResult.WinnerGCID)
	}
	if res.DuelResult.Outcome != OutcomeWin {
		t.Errorf("DuelResult.Outcome=%s want win", res.DuelResult.Outcome)
	}
}

// TestResolveRound_FCFS_CorrectEndsRoundImmediately is the canonical
// first-come-first-served test: a correct answer resolves the round on
// the spot. The opponent does NOT get to answer a resolved round — the
// round winner is the first correct answerer.
func TestResolveRound_FCFS_CorrectEndsRoundImmediately(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	// 2 rounds so the duel doesn't complete on round 1.
	_ = d.StartBattle([]AtomPick{
		{AtomID: "atom-1", Question: "Q1"},
		{AtomID: "atom-2", Question: "Q2"},
	})

	// A answers round 1 correct → round resolves immediately.
	res, err := d.ResolveRound("gcid-a", 1, true, 5000, []int{1, 2, 3, 5})
	if err != nil {
		t.Fatalf("A correct round 1: %v", err)
	}
	if res.DuelStatus != StatusInProgress {
		t.Errorf("duel status=%s want in_progress (round 2 remains)", res.DuelStatus)
	}
	if d.Rounds[0].ResolvedAt == nil {
		t.Error("round 1 not resolved after A's correct answer (FCFS)")
	}
	if d.Rounds[0].WinnerGCID != "gcid-a" {
		t.Errorf("round 1 winner=%s want gcid-a", d.Rounds[0].WinnerGCID)
	}

	// B tries to answer the resolved round 1 → rejected.
	_, err = d.ResolveRound("gcid-b", 1, true, 0, []int{1, 2, 3, 5})
	if !errors.Is(err, ErrAlreadyResolved) {
		t.Errorf("B answering resolved round 1: err=%v want ErrAlreadyResolved", err)
	}
}

// TestResolveRound_FCFS_WrongKeepsRoundOpenForOpponent verifies the
// "B can keep answering" branch: if A answers wrong, the round stays
// open and B can still steal it with a correct answer.
func TestResolveRound_FCFS_WrongKeepsRoundOpenForOpponent(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	_ = d.StartBattle([]AtomPick{
		{AtomID: "atom-1", Question: "Q1"},
		{AtomID: "atom-2", Question: "Q2"},
	})

	// A answers round 1 WRONG → round does NOT resolve.
	res, err := d.ResolveRound("gcid-a", 1, false, 0, []int{1, 2, 3, 5})
	if err != nil {
		t.Fatalf("A wrong round 1: %v", err)
	}
	if d.Rounds[0].ResolvedAt != nil {
		t.Error("round 1 resolved after A's WRONG answer (should stay open for B)")
	}
	if !d.Rounds[0].ChallengerAnswered {
		t.Error("A's wrong answer should mark ChallengerAnswered=true")
	}

	// B answers round 1 CORRECT → round resolves (B steals it).
	res, err = d.ResolveRound("gcid-b", 1, true, 3000, []int{1, 2, 3, 5})
	if err != nil {
		t.Fatalf("B correct round 1: %v", err)
	}
	if d.Rounds[0].ResolvedAt == nil {
		t.Error("round 1 not resolved after B's correct steal")
	}
	if d.Rounds[0].WinnerGCID != "gcid-b" {
		t.Errorf("round 1 winner=%s want gcid-b (B stole it)", d.Rounds[0].WinnerGCID)
	}
	_ = res
}

// TestResolveRound_FCFS_BothWrongEndsRound verifies the "if both is
// answering wrong, then the round is over" branch: A wrong + B wrong
// → round resolves with no winner, then the duel advances.
func TestResolveRound_FCFS_BothWrongEndsRound(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	_ = d.StartBattle([]AtomPick{
		{AtomID: "atom-1", Question: "Q1"},
		{AtomID: "atom-2", Question: "Q2"},
	})

	// A wrong → round stays open.
	_, _ = d.ResolveRound("gcid-a", 1, false, 0, []int{1, 2, 3, 5})
	if d.Rounds[0].ResolvedAt != nil {
		t.Fatal("round 1 resolved after only A wrong (should wait for B)")
	}

	// B wrong too → round resolves (both wrong, no winner).
	_, err := d.ResolveRound("gcid-b", 1, false, 0, []int{1, 2, 3, 5})
	if err != nil {
		t.Fatalf("B wrong round 1: %v", err)
	}
	if d.Rounds[0].ResolvedAt == nil {
		t.Error("round 1 not resolved after both wrong (should resolve with no winner)")
	}
	if d.Rounds[0].WinnerGCID != "" {
		t.Errorf("round 1 winner=%s want empty (both wrong → no winner)", d.Rounds[0].WinnerGCID)
	}
	// Duel not complete (round 2 remains).
	if d.Status != StatusInProgress {
		t.Errorf("duel status=%s want in_progress (round 2 remains)", d.Status)
	}
}

// --- WS3: enforced round timer (deadline_at + auto-skip on expiry) ---

// newInProgressDuel builds a duel with 2 rounds + RoundTimerSec=30,
// already in_progress. Round 1's DeadlineAt is NOT stamped at StartBattle
// — it is stamped lazily when the first client is served the round (see
// stampRound1).
func newInProgressDuel(t *testing.T) *Duel {
	t.Helper()
	d, err := NewDuel(DuelConfig{RoundTimerSec: 30}, "gcid-a", "gcid-b", "tenant-1", ScopeRanked, 2, nil, 24*time.Hour)
	if err != nil {
		t.Fatalf("NewDuel: %v", err)
	}
	if err := d.Accept("gcid-b"); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if err := d.StartBattle([]AtomPick{
		{AtomID: "atom-1", Question: "Q1", Options: []string{"A", "B"}, Answer: "A"},
		{AtomID: "atom-2", Question: "Q2", Options: []string{"C", "D"}, Answer: "C"},
	}); err != nil {
		t.Fatalf("StartBattle: %v", err)
	}
	return d
}

// stampRound1 simulates the WS adapter's lazy first-serve stamp: round 1's
// deadline is stamped when the first client is served the round, not at
// StartBattle. Players discover a match via heartbeat polling (up to ~10s
// late), so stamping at match time burns their timer before they ever see
// the question.
// stampRound1 models the WS adapter stamping round 1 on FIRST SERVE, which is
// the only way round 1 gets a deadline (StartBattle deliberately leaves it nil).
//
// The serve is dated a few seconds in the PAST on purpose. A learner cannot be
// served a question and answer it in the same instant, and modelling zero
// elapsed time made every deadline assertion depend on clock resolution:
// round 1 stamped from time.Now() and round 2 stamped microseconds later from
// the resolution instant came out EQUAL, so an ordering assertion read as a
// product failure when the product was fine. Dating the serve keeps the
// ordering assertions honest AND keeps their discriminating power, because the
// defect they guard (every round sharing one absolute deadline) still makes
// round 2 equal round 1.
func stampRound1(d *Duel) {
	const servedAgo = 5 * time.Second
	deadline := time.Now().UTC().Add(-servedAgo).Add(30 * time.Second)
	d.Rounds[0].DeadlineAt = &deadline
}

// TestStartBattle_LeavesRound1DeadlineUnstamped pins the lazy-stamp rule:
// StartBattle must NOT stamp round 1's DeadlineAt. The WS adapter stamps
// it on first serve (GetCurrentRound); subsequent rounds are stamped when
// the previous round resolves (not all at once — that would make every
// round share one absolute deadline and the sweeper would mass-skip
// rounds 2–N mid-duel).
func TestStartBattle_LeavesRound1DeadlineUnstamped(t *testing.T) {
	d := newInProgressDuel(t)
	if d.Rounds[0].DeadlineAt != nil {
		t.Fatalf("round 1 DeadlineAt=%v want nil at StartBattle (stamped lazily on first serve — stamping at match time burns the timer before players see the question)", d.Rounds[0].DeadlineAt)
	}
	if d.Rounds[1].DeadlineAt != nil {
		t.Fatalf("round 2 DeadlineAt is non-nil at StartBattle (must be stamped only when round 1 resolves — not all at once)")
	}
}

// TestResolveRoundTimeout_AutoResolvesExpiredRound pins WS3: a round whose
// DeadlineAt has passed MUST auto-resolve as both-unanswered (no points, no
// winner) so the duel advances. The caller passes the current time.
func TestResolveRoundTimeout_AutoResolvesExpiredRound(t *testing.T) {
	d := newInProgressDuel(t)
	stampRound1(d)

	// Round 1 deadline is now+30s. Simulate the timer firing at now+31s.
	expired := d.Rounds[0].DeadlineAt.Add(time.Second)
	res, err := d.ResolveRoundTimeout(1, expired)
	if err != nil {
		t.Fatalf("ResolveRoundTimeout: %v", err)
	}
	if res.DuelStatus != StatusInProgress {
		t.Errorf("duel status=%s want in_progress (round 2 remains)", res.DuelStatus)
	}
	if d.Rounds[0].ResolvedAt == nil {
		t.Error("round 1 not resolved after timeout (should auto-resolve as both-unanswered)")
	}
	if d.Rounds[0].WinnerGCID != "" {
		t.Errorf("round 1 winner=%s want empty (timeout = no winner)", d.Rounds[0].WinnerGCID)
	}
	if d.Rounds[0].PointsChallenger != 0 || d.Rounds[0].PointsOpponent != 0 {
		t.Errorf("round 1 points chal=%d opp=%d want 0/0 (timeout = no points)", d.Rounds[0].PointsChallenger, d.Rounds[0].PointsOpponent)
	}
}

// TestResolveRoundTimeout_CompletesDuel pins WS3: auto-resolving the last
// round completes the duel (all rounds resolved → completeDuel).
func TestResolveRoundTimeout_CompletesDuel(t *testing.T) {
	d := newInProgressDuel(t)
	// Round 1 resolved normally (challenger correct).
	_, _ = d.ResolveRound("gcid-a", 1, true, 5000, []int{1, 2, 3, 5})
	// Round 2 times out.
	expired := d.Rounds[1].DeadlineAt.Add(time.Second)
	res, err := d.ResolveRoundTimeout(2, expired)
	if err != nil {
		t.Fatalf("ResolveRoundTimeout: %v", err)
	}
	if res.DuelStatus != StatusCompleted {
		t.Errorf("duel status=%s want completed (all rounds resolved)", res.DuelStatus)
	}
	if res.DuelResult == nil {
		t.Fatal("DuelResult nil (should be populated on completion)")
	}
}

// TestResolveRoundTimeout_RejectsUnexpiredRound pins WS3 fail-loud: calling
// ResolveRoundTimeout before the deadline is an error (the sweep must not
// prematurely skip a live round).
func TestResolveRoundTimeout_RejectsUnexpiredRound(t *testing.T) {
	d := newInProgressDuel(t)
	stampRound1(d)
	// Timer fires 1s BEFORE the deadline — must NOT resolve.
	early := d.Rounds[0].DeadlineAt.Add(-time.Second)
	_, err := d.ResolveRoundTimeout(1, early)
	if err == nil {
		t.Fatal("ResolveRoundTimeout before deadline returned nil error (must fail-loud — round not yet expired)")
	}
}

// TestResolveRound_RejectsAnswerAfterDeadline pins WS3: a late answer
// (after the round deadline) is rejected with an explicit error so the
// server-side auto-skip is idempotent against late answers.
func TestResolveRound_RejectsAnswerAfterDeadline(t *testing.T) {
	d := newInProgressDuel(t)
	stampRound1(d)
	// Simulate the round timing out first.
	expired := d.Rounds[0].DeadlineAt.Add(time.Second)
	_, _ = d.ResolveRoundTimeout(1, expired)
	// A late answer arrives — must be rejected (round already resolved).
	_, err := d.ResolveRound("gcid-a", 1, true, 5000, []int{1, 2, 3, 5})
	if err == nil {
		t.Fatal("late answer after deadline returned nil error (must fail-loud — idempotent against late answers)")
	}
	if !errors.Is(err, ErrAlreadyResolved) {
		t.Errorf("late answer error=%v want ErrAlreadyResolved (wrapped)", err)
	}
}

// TestStartBattle_PerRoundDeadlineStamping pins WS3 fix #2: round 1's
// deadline is stamped lazily on first serve (simulated here via
// stampRound1); round 2's deadline is stamped when round 1 resolves. This
// prevents the "all rounds share one absolute deadline" bug where a 5×30s
// duel would mass-expire rounds 2–5 at t=30s.
func TestStartBattle_PerRoundDeadlineStamping(t *testing.T) {
	d := newInProgressDuel(t)

	// Neither round has a deadline at StartBattle (round 1 is stamped on
	// first serve; round 2 when round 1 resolves).
	if d.Rounds[0].DeadlineAt != nil {
		t.Fatal("round 1 DeadlineAt is non-nil at StartBattle (must be stamped lazily on first serve)")
	}
	if d.Rounds[1].DeadlineAt != nil {
		t.Fatal("round 2 DeadlineAt is non-nil at StartBattle (must be stamped only when round 1 resolves)")
	}
	stampRound1(d)

	// Resolve round 1 → round 2 becomes current → its deadline is stamped.
	_, _ = d.ResolveRound("gcid-a", 1, true, 5000, []int{1, 2, 3, 5})
	if d.Rounds[1].DeadlineAt == nil {
		t.Fatal("round 2 DeadlineAt is nil after round 1 resolved (must be stamped when round becomes current)")
	}

	// The round 2 deadline must be AFTER the round 1 deadline (stamped at
	// round-1 resolution time, not at StartBattle — the bug was all rounds
	// sharing one absolute deadline).
	if !d.Rounds[1].DeadlineAt.After(*d.Rounds[0].DeadlineAt) {
		t.Errorf("round 2 deadline %v is not after round 1 deadline %v (must be stamped at round-1 resolution, not at StartBattle)",
			d.Rounds[1].DeadlineAt, d.Rounds[0].DeadlineAt)
	}
}
