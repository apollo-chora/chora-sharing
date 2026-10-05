package duel

// Test-only additions covering the validation paths, error branches and
// edge cases not reached by duel_test.go / blitz_test.go. No production
// behavior is changed. Deadline-stamping tests assert a single stamp against
// the wall clock (or a no-op) — never same-instant ordering between two
// stamps, which is the time-resolution trap behind the pre-existing
// TestStartBattle_PerRoundDeadlineStamping failure.

import (
	"errors"
	"testing"
	"time"
)

// --- ComputeProficiency ---

func TestComputeProficiency_NoCourses(t *testing.T) {
	rs := &RatingStats{Rating: 1000}
	rs.ComputeProficiency()
	if rs.CourseBonus != 0 {
		t.Errorf("course_bonus=%d want 0", rs.CourseBonus)
	}
	if rs.Proficiency != 1000 {
		t.Errorf("proficiency=%d want 1000", rs.Proficiency)
	}
}

func TestComputeProficiency_BonusScalesWithCourses(t *testing.T) {
	rs := &RatingStats{Rating: 1200, CompletedCourses: 3}
	rs.ComputeProficiency()
	if rs.CourseBonus != 60 {
		t.Errorf("course_bonus=%d want 60 (3*20)", rs.CourseBonus)
	}
	if rs.Proficiency != 1260 {
		t.Errorf("proficiency=%d want 1260", rs.Proficiency)
	}
}

func TestComputeProficiency_CapsBonusAt200(t *testing.T) {
	rs := &RatingStats{Rating: 1100, CompletedCourses: 15} // 15*20=300 → capped at 200
	rs.ComputeProficiency()
	if rs.CourseBonus != 200 {
		t.Errorf("course_bonus=%d want 200 (capped)", rs.CourseBonus)
	}
	if rs.Proficiency != 1300 {
		t.Errorf("proficiency=%d want 1300", rs.Proficiency)
	}
}

// --- Constructor validation / defaulting ---

func TestNewDuel_ValidationErrors(t *testing.T) {
	cases := []struct {
		name       string
		challenger string
		opponent   string
		tenantID   string
	}{
		{"empty tenant", "gcid-a", "gcid-b", "  "},
		{"empty challenger", "  ", "gcid-b", "tenant-1"},
		{"empty opponent", "gcid-a", " ", "tenant-1"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewDuel(DuelConfig{}, tt.challenger, tt.opponent, tt.tenantID, ScopeFriendly, 5, nil, 0)
			if !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("err=%v want ErrInvalidArgument", err)
			}
		})
	}
}

func TestNewDuel_RoundCountClampedDown(t *testing.T) {
	d, err := NewDuel(DuelConfig{}, "gcid-a", "gcid-b", "tenant-1", ScopeFriendly, 25, nil, 0)
	if err != nil {
		t.Fatalf("NewDuel: %v", err)
	}
	if d.RoundCount != 20 {
		t.Errorf("round_count=%d want 20 (clamped down)", d.RoundCount)
	}
}

func TestNewPoolDuel_ValidationErrors(t *testing.T) {
	cases := []struct {
		name       string
		challenger string
		opponent   string
		tenantID   string
	}{
		{"empty tenant", "gcid-a", "gcid-b", " "},
		{"empty challenger", "  ", "gcid-b", "tenant-1"},
		{"empty opponent", "gcid-a", "", "tenant-1"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewPoolDuel(DuelConfig{}, tt.challenger, tt.opponent, tt.tenantID, ScopeRanked, 5, nil)
			if !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("err=%v want ErrInvalidArgument", err)
			}
		})
	}
}

func TestNewPoolDuel_RoundCountClampedDown(t *testing.T) {
	d, err := NewPoolDuel(DuelConfig{}, "gcid-a", "gcid-b", "tenant-1", ScopeRanked, 25, nil)
	if err != nil {
		t.Fatalf("NewPoolDuel: %v", err)
	}
	if d.RoundCount != 20 {
		t.Errorf("round_count=%d want 20 (clamped down)", d.RoundCount)
	}
}

func TestNewPoolDuelWithID_Valid(t *testing.T) {
	d, err := NewPoolDuelWithID(DuelConfig{}, "gcid-a", "gcid-b", "tenant-1", ScopeRanked, 5, nil, "duel-123")
	if err != nil {
		t.Fatalf("NewPoolDuelWithID: %v", err)
	}
	if d.ID != "duel-123" {
		t.Errorf("ID=%s want duel-123", d.ID)
	}
	if d.Status != StatusAccepted {
		t.Errorf("status=%s want accepted", d.Status)
	}
}

func TestNewPoolDuelWithID_EmptyID(t *testing.T) {
	_, err := NewPoolDuelWithID(DuelConfig{}, "gcid-a", "gcid-b", "tenant-1", ScopeRanked, 5, nil, "  ")
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("err=%v want ErrInvalidArgument", err)
	}
}

func TestNewBlitzPoolDuel_RejectsTimedWithoutTimeLimit(t *testing.T) {
	_, err := NewBlitzPoolDuel(DuelConfig{}, "gcid-a", "gcid-b", "tenant-1", ScopeRanked, 5, nil,
		BlitzConfig{Variant: BlitzVariantTimed, TimeLimitSec: 0})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("err=%v want ErrInvalidArgument", err)
	}
}

func TestNewBlitzPoolDuel_RejectsRaceWithoutTarget(t *testing.T) {
	_, err := NewBlitzPoolDuel(DuelConfig{}, "gcid-a", "gcid-b", "tenant-1", ScopeRanked, 5, nil,
		BlitzConfig{Variant: BlitzVariantRace, RaceTarget: 0})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("err=%v want ErrInvalidArgument", err)
	}
}

func TestNewBlitzPoolDuelWithID_Valid(t *testing.T) {
	d, err := NewBlitzPoolDuelWithID(DuelConfig{}, "gcid-a", "gcid-b", "tenant-1", ScopeRanked, 5, nil,
		BlitzConfig{Variant: BlitzVariantRace, RaceTarget: 3}, "duel-456")
	if err != nil {
		t.Fatalf("NewBlitzPoolDuelWithID: %v", err)
	}
	if d.ID != "duel-456" {
		t.Errorf("ID=%s want duel-456", d.ID)
	}
	if d.Mode != ModeBlitz {
		t.Errorf("mode=%s want blitz", d.Mode)
	}
	if d.BlitzConfig.Variant != BlitzVariantRace {
		t.Errorf("variant=%s want race", d.BlitzConfig.Variant)
	}
}

func TestNewBlitzPoolDuelWithID_SelfChallenge(t *testing.T) {
	// Validation passes (valid variant + ID), but newPoolDuel rejects a
	// self-challenge — covers the NewBlitzPoolDuelWithID error passthrough.
	_, err := NewBlitzPoolDuelWithID(DuelConfig{}, "gcid-a", "gcid-a", "tenant-1", ScopeRanked, 5, nil,
		BlitzConfig{Variant: BlitzVariantTimed, TimeLimitSec: 60}, "duel-1")
	if !errors.Is(err, ErrSelfChallenge) {
		t.Errorf("err=%v want ErrSelfChallenge", err)
	}
}

func TestNewBlitzPoolDuelWithID_ValidationErrors(t *testing.T) {
	call := func(cfg BlitzConfig, id string) (*Duel, error) {
		return NewBlitzPoolDuelWithID(DuelConfig{}, "gcid-a", "gcid-b", "tenant-1", ScopeRanked, 5, nil, cfg, id)
	}
	if _, err := call(BlitzConfig{Variant: BlitzVariantTimed, TimeLimitSec: 60}, "  "); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("empty id: err=%v want ErrInvalidArgument", err)
	}
	if _, err := call(BlitzConfig{Variant: BlitzVariantUnspecified}, "duel-1"); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("unspecified variant: err=%v want ErrInvalidArgument", err)
	}
	if _, err := call(BlitzConfig{Variant: BlitzVariantTimed}, "duel-1"); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("timed without limit: err=%v want ErrInvalidArgument", err)
	}
	if _, err := call(BlitzConfig{Variant: BlitzVariantRace}, "duel-1"); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("race without target: err=%v want ErrInvalidArgument", err)
	}
}

// --- WithRoundTimerSec ---

func TestWithRoundTimerSec_AppliesPositive(t *testing.T) {
	d := newTestDuel(t)
	got := d.WithRoundTimerSec(45)
	if got != d {
		t.Error("WithRoundTimerSec must return the receiver (chainable)")
	}
	if d.roundTimerSec != 45 {
		t.Errorf("roundTimerSec=%d want 45", d.roundTimerSec)
	}
}

// TestWithRoundTimerSec_IgnoresNonPositive pins the rehydration guard: a
// fetched duel has roundTimerSec=0, and 0/negative must NOT overwrite it.
func TestWithRoundTimerSec_IgnoresNonPositive(t *testing.T) {
	d := &Duel{}
	d.WithRoundTimerSec(0)
	if d.roundTimerSec != 0 {
		t.Errorf("roundTimerSec=%d want 0 (0 must not override)", d.roundTimerSec)
	}
	d.roundTimerSec = 30
	d.WithRoundTimerSec(-5)
	if d.roundTimerSec != 30 {
		t.Errorf("roundTimerSec=%d want 30 (negative must not override)", d.roundTimerSec)
	}
}

// --- Accept / StartBattle error branches ---

func TestAccept_NotPending(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	err := d.Accept("gcid-b") // already accepted
	if !errors.Is(err, ErrNotPending) {
		t.Errorf("err=%v want ErrNotPending", err)
	}
}

func TestStartBattle_NotAccepted(t *testing.T) {
	d := newTestDuel(t) // pending — StartBattle requires accepted
	err := d.StartBattle([]AtomPick{{AtomID: "atom-1"}})
	if !errors.Is(err, ErrNotAccepted) {
		t.Errorf("err=%v want ErrNotAccepted", err)
	}
}

func TestStartBattle_NoAtoms(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	err := d.StartBattle(nil)
	if !errors.Is(err, ErrNoAtomsAvailable) {
		t.Errorf("err=%v want ErrNoAtomsAvailable", err)
	}
}

// --- WS3 deadline-stamping edge cases ---
//
// Each case checks a single stamp against the wall clock (with tolerance)
// or a no-op — never ordering between two stamps made at the same instant.
// The pre-existing TestStartBattle_PerRoundDeadlineStamping failure comes
// exactly from that same-instant ordering assertion, so these avoid it.

func TestStampCurrentRoundDeadline_DefaultsTimer(t *testing.T) {
	// roundTimerSec is 0 (zero DuelConfig) → falls back to the 30s default.
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1"}})
	d.stampCurrentRoundDeadline(time.Now().UTC(), d.roundTimerSec)
	if d.Rounds[0].DeadlineAt == nil {
		t.Fatal("deadline not stamped with default timer")
	}
	horizon := time.Until(*d.Rounds[0].DeadlineAt)
	if horizon < 28*time.Second || horizon > 32*time.Second {
		t.Errorf("deadline horizon=%v want ~30s", horizon)
	}
}

func TestStampCurrentRoundDeadline_AlreadyStampedIsNoOp(t *testing.T) {
	d := newInProgressDuel(t)
	manual := time.Now().UTC().Add(90 * time.Second)
	d.Rounds[0].DeadlineAt = &manual
	d.stampCurrentRoundDeadline(time.Now().UTC(), d.roundTimerSec)
	if !d.Rounds[0].DeadlineAt.Equal(manual) {
		t.Error("already-stamped round 1 deadline was rewritten (must be idempotent)")
	}
	if d.Rounds[1].DeadlineAt != nil {
		t.Error("round 2 was stamped while round 1 was already stamped")
	}
}

func TestStampCurrentRoundDeadline_NoOpWhenAllResolved(t *testing.T) {
	d := newInProgressDuel(t)
	_, _ = d.ResolveRound("gcid-a", 1, true, 0, []int{1, 2, 3, 5})
	expired := d.Rounds[1].DeadlineAt.Add(time.Second)
	_, _ = d.ResolveRoundTimeout(2, expired)
	d.stampCurrentRoundDeadline(time.Now().UTC(), 30) // must be a no-op, not a panic
	if d.Status != StatusCompleted {
		t.Errorf("status=%s want completed", d.Status)
	}
}

// --- ResolveRound error branches ---

func TestResolveRound_NotInProgress(t *testing.T) {
	d := newTestDuel(t) // pending
	_, err := d.ResolveRound("gcid-a", 1, true, 0, []int{1, 2, 3, 5})
	if !errors.Is(err, ErrNotInProgress) {
		t.Errorf("err=%v want ErrNotInProgress", err)
	}
}

func TestResolveRound_RoundOutOfRange(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1"}})
	_, err := d.ResolveRound("gcid-a", 5, true, 0, []int{1, 2, 3, 5})
	if !errors.Is(err, ErrRoundOutOfRange) {
		t.Errorf("err=%v want ErrRoundOutOfRange", err)
	}
}

func TestResolveRound_NotParticipant(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1"}})
	_, err := d.ResolveRound("gcid-c", 1, true, 0, []int{1, 2, 3, 5})
	if !errors.Is(err, ErrNotParticipant) {
		t.Errorf("err=%v want ErrNotParticipant", err)
	}
}

func TestResolveRound_ChallengerResubmitAfterWrong(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1"}, {AtomID: "atom-2"}})
	if _, err := d.ResolveRound("gcid-a", 1, false, 0, []int{1, 2, 3, 5}); err != nil {
		t.Fatalf("first (wrong) answer: %v", err)
	}
	// A wrong answer keeps the round open, but the answerer may not re-submit.
	_, err := d.ResolveRound("gcid-a", 1, true, 0, []int{1, 2, 3, 5})
	if !errors.Is(err, ErrAlreadyResolved) {
		t.Errorf("err=%v want ErrAlreadyResolved", err)
	}
}

func TestResolveRound_OpponentResubmitAfterWrong(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1"}, {AtomID: "atom-2"}})
	if _, err := d.ResolveRound("gcid-b", 1, false, 0, []int{1, 2, 3, 5}); err != nil {
		t.Fatalf("first (wrong) answer: %v", err)
	}
	_, err := d.ResolveRound("gcid-b", 1, true, 0, []int{1, 2, 3, 5})
	if !errors.Is(err, ErrAlreadyResolved) {
		t.Errorf("err=%v want ErrAlreadyResolved", err)
	}
}

func TestResolveRound_ChallengerWrongAfterOpponentWrong(t *testing.T) {
	// Mirror image of BothWrongEndsRound: the OPPONENT answers wrong first,
	// then the challenger answers wrong — the challenger-last branch of the
	// both-wrong FCFS resolution (challenger resolves with no winner).
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1"}, {AtomID: "atom-2"}})
	if _, err := d.ResolveRound("gcid-b", 1, false, 0, []int{1, 2, 3, 5}); err != nil {
		t.Fatalf("opponent wrong: %v", err)
	}
	if d.Rounds[0].ResolvedAt != nil {
		t.Fatal("round 1 resolved after only opponent wrong (should wait for challenger)")
	}
	_, err := d.ResolveRound("gcid-a", 1, false, 0, []int{1, 2, 3, 5})
	if err != nil {
		t.Fatalf("challenger wrong: %v", err)
	}
	if d.Rounds[0].ResolvedAt == nil {
		t.Error("round 1 not resolved after both wrong (should resolve with no winner)")
	}
	if d.Rounds[0].WinnerGCID != "" {
		t.Errorf("round 1 winner=%s want empty", d.Rounds[0].WinnerGCID)
	}
	if d.Status != StatusInProgress {
		t.Errorf("duel status=%s want in_progress (round 2 remains)", d.Status)
	}
}

func TestResolveRound_OpponentWinsDuel(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1"}})
	// A wrong, B correct → B wins the single round and the duel.
	_, _ = d.ResolveRound("gcid-a", 1, false, 0, []int{1, 2, 3, 5})
	res, err := d.ResolveRound("gcid-b", 1, true, 0, []int{1, 2, 3, 5})
	if err != nil {
		t.Fatalf("B correct: %v", err)
	}
	if res.DuelStatus != StatusCompleted {
		t.Errorf("duel_status=%s want completed", res.DuelStatus)
	}
	if d.WinnerGCID != "gcid-b" {
		t.Errorf("winner=%s want gcid-b", d.WinnerGCID)
	}
	if res.WinnerGCID != "gcid-b" {
		t.Errorf("res.winner=%s want gcid-b", res.WinnerGCID)
	}
}

// --- ResolveRoundTimeout error branches ---

func TestResolveRoundTimeout_NotInProgress(t *testing.T) {
	d := newTestDuel(t)
	_, err := d.ResolveRoundTimeout(1, time.Now().UTC())
	if !errors.Is(err, ErrNotInProgress) {
		t.Errorf("err=%v want ErrNotInProgress", err)
	}
}

func TestResolveRoundTimeout_RoundOutOfRange(t *testing.T) {
	d := newInProgressDuel(t)
	_, err := d.ResolveRoundTimeout(5, time.Now().UTC().Add(time.Hour))
	if !errors.Is(err, ErrRoundOutOfRange) {
		t.Errorf("err=%v want ErrRoundOutOfRange", err)
	}
}

func TestResolveRoundTimeout_AlreadyResolved(t *testing.T) {
	d := newInProgressDuel(t)
	_, _ = d.ResolveRound("gcid-a", 1, true, 0, []int{1, 2, 3, 5})
	_, err := d.ResolveRoundTimeout(1, time.Now().UTC().Add(time.Hour))
	if !errors.Is(err, ErrAlreadyResolved) {
		t.Errorf("err=%v want ErrAlreadyResolved", err)
	}
}

func TestResolveRoundTimeout_NoDeadlineStamped(t *testing.T) {
	d := newInProgressDuel(t) // round 1 deadline is nil until lazily stamped
	_, err := d.ResolveRoundTimeout(1, time.Now().UTC().Add(time.Hour))
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("err=%v want ErrInvalidArgument (no deadline stamped)", err)
	}
}

// --- ResolveBlitzAnswer gaps ---

func TestResolveBlitzAnswer_NotBlitz(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1"}})
	_, err := d.ResolveBlitzAnswer("gcid-a", 1, true, 0, []int{1, 2, 3, 5})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("err=%v want ErrInvalidArgument (not a blitz duel)", err)
	}
}

func TestResolveBlitzAnswer_RoundAlreadyResolved(t *testing.T) {
	// Two rounds so the duel stays in_progress after round 1 resolves: the
	// re-submission must hit the round.ResolvedAt branch, not the status gate.
	d := newBlitzTestDuel(t, BlitzVariantTimed, 120, 0)
	_ = d.StartBattle([]AtomPick{
		{AtomID: "atom-1"},
		{AtomID: "atom-2"},
	})
	_, _ = d.ResolveBlitzAnswer("gcid-a", 1, true, 0, []int{1, 2, 3, 5})
	_, _ = d.ResolveBlitzAnswer("gcid-b", 1, true, 0, []int{1, 2, 3, 5})
	if d.Rounds[0].ResolvedAt == nil {
		t.Fatal("round 1 not resolved after both answered")
	}
	if d.Status != StatusInProgress {
		t.Fatalf("status=%s want in_progress (round 2 remains)", d.Status)
	}
	_, err := d.ResolveBlitzAnswer("gcid-a", 1, true, 0, []int{1, 2, 3, 5})
	if !errors.Is(err, ErrAlreadyResolved) {
		t.Errorf("err=%v want ErrAlreadyResolved", err)
	}
}

func TestResolveBlitzAnswer_OpponentAlreadyAnswered(t *testing.T) {
	d := newBlitzTestDuel(t, BlitzVariantTimed, 120, 0)
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1"}, {AtomID: "atom-2"}})
	if _, err := d.ResolveBlitzAnswer("gcid-b", 1, true, 0, []int{1, 2, 3, 5}); err != nil {
		t.Fatalf("opponent answer: %v", err)
	}
	// Opponent already answered round 1 — re-submission rejected.
	_, err := d.ResolveBlitzAnswer("gcid-b", 1, true, 0, []int{1, 2, 3, 5})
	if !errors.Is(err, ErrAlreadyResolved) {
		t.Errorf("err=%v want ErrAlreadyResolved", err)
	}
}

// TestResolveBlitzAnswer_TimedNaturalCompletion pins the rare timed-variant
// path where both players answer every round before the timer expires — the
// duel completes naturally (CompleteBlitzTimed is never needed).
func TestResolveBlitzAnswer_TimedNaturalCompletion(t *testing.T) {
	d := newBlitzTestDuel(t, BlitzVariantTimed, 120, 0)
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1"}})
	res1, err := d.ResolveBlitzAnswer("gcid-a", 1, true, 0, []int{1, 2, 3, 5})
	if err != nil {
		t.Fatalf("challenger answer: %v", err)
	}
	if res1.DuelStatus == StatusCompleted {
		t.Fatal("duel completed after first answer (round still open)")
	}
	res2, err := d.ResolveBlitzAnswer("gcid-b", 1, true, 0, []int{1, 2, 3, 5})
	if err != nil {
		t.Fatalf("opponent answer: %v", err)
	}
	if !res2.RoundResolved {
		t.Error("round not resolved after both answered")
	}
	if res2.DuelStatus != StatusCompleted {
		t.Errorf("duel_status=%s want completed (natural completion before timer)", res2.DuelStatus)
	}
}

// --- CompleteBlitzTimed error branches ---

func TestCompleteBlitzTimed_NotBlitz(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1"}})
	_, err := d.CompleteBlitzTimed(time.Now().UTC().Add(time.Hour))
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("err=%v want ErrInvalidArgument (not a blitz duel)", err)
	}
}

func TestCompleteBlitzTimed_NotTimedVariant(t *testing.T) {
	d := newBlitzTestDuel(t, BlitzVariantRace, 0, 2)
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1"}})
	_, err := d.CompleteBlitzTimed(time.Now().UTC().Add(time.Hour))
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("err=%v want ErrInvalidArgument (race variant)", err)
	}
}

func TestCompleteBlitzTimed_NotInProgress(t *testing.T) {
	d := newBlitzTestDuel(t, BlitzVariantTimed, 60, 0) // accepted, not started
	_, err := d.CompleteBlitzTimed(time.Now().UTC().Add(time.Hour))
	if !errors.Is(err, ErrNotInProgress) {
		t.Errorf("err=%v want ErrNotInProgress", err)
	}
}

func TestCompleteBlitzTimed_NotStarted(t *testing.T) {
	d := newBlitzTestDuel(t, BlitzVariantTimed, 60, 0)
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1"}})
	d.BlitzStartedAt = nil // simulate a rehydrated/corrupted duel
	_, err := d.CompleteBlitzTimed(time.Now().UTC().Add(time.Hour))
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("err=%v want ErrInvalidArgument (blitz not started)", err)
	}
}

// --- Forfeit error branches + opponent-forfeits ---

func TestForfeit_NotInProgressOrAccepted(t *testing.T) {
	d := newTestDuel(t) // pending
	err := d.Forfeit("gcid-a")
	if !errors.Is(err, ErrNotInProgress) {
		t.Errorf("err=%v want ErrNotInProgress", err)
	}
}

func TestForfeit_FromAccepted(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b") // accepted, battle not started — forfeit still legal
	if err := d.Forfeit("gcid-b"); err != nil {
		t.Fatalf("Forfeit from accepted: %v", err)
	}
	if d.WinnerGCID != "gcid-a" {
		t.Errorf("winner=%s want gcid-a", d.WinnerGCID)
	}
}

func TestForfeit_NotParticipant(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1"}})
	err := d.Forfeit("gcid-c")
	if !errors.Is(err, ErrNotParticipant) {
		t.Errorf("err=%v want ErrNotParticipant", err)
	}
}

func TestForfeit_OpponentForfeits(t *testing.T) {
	d := newTestDuel(t)
	_ = d.Accept("gcid-b")
	_ = d.StartBattle([]AtomPick{{AtomID: "atom-1"}})
	if err := d.Forfeit("gcid-b"); err != nil {
		t.Fatalf("Forfeit: %v", err)
	}
	if d.Status != StatusForfeited {
		t.Errorf("status=%s want forfeited", d.Status)
	}
	if d.WinnerGCID != "gcid-a" {
		t.Errorf("winner=%s want gcid-a (challenger wins by opponent forfeit)", d.WinnerGCID)
	}
}

// --- Math helpers ---

func TestExpApprox_LargePositiveClamped(t *testing.T) {
	if got := ExpApprox(100); got != ExpApprox(20) {
		t.Errorf("ExpApprox(100)=%v want equal to ExpApprox(20)=%v (clamped at 20)", got, ExpApprox(20))
	}
}

func TestExpApprox_LargeNegativeIsZero(t *testing.T) {
	if got := ExpApprox(-100); got != 0 {
		t.Errorf("ExpApprox(-100)=%v want 0", got)
	}
	if got := ExpApprox(-20); got == 0 {
		t.Error("ExpApprox(-20) should be positive (only x < -20 clamps to 0)")
	}
}

func TestComboMultiplierForStreak_NegativeStreak(t *testing.T) {
	if got := ComboMultiplierForStreak(-3, []int{1, 2, 3, 5}); got != 1 {
		t.Errorf("negative streak: got %d want 1", got)
	}
}

func TestComboMultiplierForStreak_NoTiers(t *testing.T) {
	if got := ComboMultiplierForStreak(7, nil); got != 1 {
		t.Errorf("no tiers: got %d want 1", got)
	}
}

func TestComputeELO_StrongerPlayerWinsFewerPoints(t *testing.T) {
	// The "loser" is the higher-rated player → winner gains fewer points.
	winner, loser := ComputeELO(800, 1600, 32)
	if winner <= 800 {
		t.Errorf("winner=%d should exceed 800", winner)
	}
	if loser >= 1600 {
		t.Errorf("loser=%d should be below 1600", loser)
	}
	if winner+loser != 2400 {
		t.Errorf("zero-sum violated: %d+%d=%d want 2400", winner, loser, winner+loser)
	}
}

// --- shuffleOptions edges ---

func TestShuffleOptions_SingleOption(t *testing.T) {
	opts, ans := shuffleOptions([]string{"A"}, "A")
	if len(opts) != 1 || opts[0] != "A" {
		t.Errorf("opts=%v want [A]", opts)
	}
	if ans != "A" {
		t.Errorf("ans=%q want A", ans)
	}
}

func TestShuffleOptions_EmptyOptions(t *testing.T) {
	opts, ans := shuffleOptions(nil, "")
	if len(opts) != 0 {
		t.Errorf("opts=%v want empty", opts)
	}
	if ans != "" {
		t.Errorf("ans=%q want empty", ans)
	}
}

func TestShuffleOptions_AnswerNotFoundKeepsAnswer(t *testing.T) {
	opts, ans := shuffleOptions([]string{"A", "B", "C"}, "Z")
	if ans != "Z" {
		t.Errorf("ans=%q want Z (fallback keeps original answer)", ans)
	}
	if len(opts) != 3 {
		t.Errorf("opts len=%d want 3", len(opts))
	}
	// Options must still be a permutation of the input.
	seen := map[string]bool{}
	for _, o := range opts {
		seen[o] = true
	}
	for _, want := range []string{"A", "B", "C"} {
		if !seen[want] {
			t.Errorf("opts=%v missing %q", opts, want)
		}
	}
}