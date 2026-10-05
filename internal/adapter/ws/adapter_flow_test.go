package ws

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
)

// fakeELO records ApplyELO invocations so tests can assert the adapter only
// applies ratings on duel completion, only for ranked duels, and only once.
type fakeELO struct {
	mu    sync.Mutex
	calls []string // "<duelID>:k<kFactor>"
	err   error
}

func (f *fakeELO) ApplyELO(_ context.Context, d *duel.Duel, kFactor int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, d.ID+":k"+itoa(kFactor))
	return f.err
}

func (f *fakeELO) got() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [12]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// fakeEventPub counts PublishDuelCompleted calls so tests can assert the
// duel.completed.v1 event fires exactly once on completion.
type fakeEventPub struct {
	mu    sync.Mutex
	calls int
}

func (f *fakeEventPub) PublishDuelCompleted(context.Context, string, string, string, string, string, string, int, int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return nil
}

func (f *fakeEventPub) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// failingResolveRepo embeds fakeAdapterRepo and forces ResolveRound to fail,
// exercising the adapter's persist-error branch after a round resolves.
type failingResolveRepo struct {
	*fakeAdapterRepo
}

func (r *failingResolveRepo) ResolveRound(context.Context, *duel.Duel, int, string, duel.RoundResolution) error {
	return errors.New("persist failed")
}

// failingSaveRepo embeds fakeAdapterRepo and forces SaveDuel to fail,
// exercising the blitz persist-error branch.
type failingSaveRepo struct {
	*fakeAdapterRepo
}

func (r *failingSaveRepo) SaveDuel(context.Context, *duel.Duel) error {
	return errors.New("save failed")
}

// TestOptioners_WireDependencies pins the option setters: WithELO and
// WithEventPublisher must wire their dependencies and return the receiver
// for chaining.
func TestOptioners_WireDependencies(t *testing.T) {
	repo := &fakeAdapterRepo{duel: newServableDuel(t)}
	elo := &fakeELO{}
	pub := &fakeEventPub{}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil)
	if got := a.WithELO(elo, 32); got != a {
		t.Fatal("WithELO must return the receiver")
	}
	if got := a.WithEventPublisher(pub); got != a {
		t.Fatal("WithEventPublisher must return the receiver")
	}
	if a.eloApplier != elo || a.eloKFactor != 32 {
		t.Error("WithELO did not wire the applier + K-factor")
	}
	if a.eventPub != pub {
		t.Error("WithEventPublisher did not wire the publisher")
	}
}

// TestGetDuel_ReturnsSnapshot pins the lookup path: a duel owned by the
// requesting tenant returns a snapshot; a tenant mismatch or a missing
// duel returns ok=false.
func TestGetDuel_ReturnsSnapshot(t *testing.T) {
	d := newServableDuel(t)
	repo := &fakeAdapterRepo{duel: d}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil)

	snap, ok := a.GetDuel(context.Background(), d.ID, "tenant-1")
	if !ok {
		t.Fatal("GetDuel returned ok=false for the owning tenant")
	}
	if snap.DuelID != d.ID {
		t.Errorf("snap.DuelID=%q want %q", snap.DuelID, d.ID)
	}
	if snap.Status != "in_progress" {
		t.Errorf("snap.Status=%q want in_progress", snap.Status)
	}
	if snap.CurrentRound != 1 {
		t.Errorf("snap.CurrentRound=%d want 1 (first unresolved round)", snap.CurrentRound)
	}
	if snap.TotalRounds != 2 {
		t.Errorf("snap.TotalRounds=%d want 2", snap.TotalRounds)
	}
	if len(snap.Rounds) != 2 {
		t.Fatalf("snap.Rounds len=%d want 2", len(snap.Rounds))
	}
	if snap.Rounds[0].RoundNo != 1 || snap.Rounds[1].RoundNo != 2 {
		t.Error("snapshot round numbers not preserved")
	}
}

func TestGetDuel_MissingDuel(t *testing.T) {
	repo := &fakeAdapterRepo{duel: newServableDuel(t)}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil)
	if snap, ok := a.GetDuel(context.Background(), "no-such-duel", "tenant-1"); ok || snap != nil {
		t.Error("GetDuel must return ok=false for an unknown duel")
	}
}

func TestGetDuel_TenantMismatch(t *testing.T) {
	repo := &fakeAdapterRepo{duel: newServableDuel(t)}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil)
	if snap, ok := a.GetDuel(context.Background(), repo.duel.ID, "tenant-other"); ok || snap != nil {
		t.Error("GetDuel must return ok=false when the tenant does not own the duel")
	}
}

// TestGetDuel_SnapshotReflectsRoundState pins toSnapshot's field mapping:
// per-round correctness flags, resolved rounds (current round advances to
// the first unresolved one), and the duel winner.
func TestGetDuel_SnapshotReflectsRoundState(t *testing.T) {
	d := newServableDuel(t)
	now := time.Now().UTC()
	d.WinnerGCID = "gcid-b"
	d.Rounds[0].ChallengerCorrect = true
	d.Rounds[0].ResolvedAt = &now
	d.Rounds[1].OpponentCorrect = true
	d.Rounds[1].ResolvedAt = &now
	d.ScoreChallenger = 15
	d.ScoreOpponent = 10

	repo := &fakeAdapterRepo{duel: d}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil)
	snap, ok := a.GetDuel(context.Background(), d.ID, "tenant-1")
	if !ok {
		t.Fatal("GetDuel returned ok=false")
	}
	if snap.CurrentRound != 0 {
		t.Errorf("snap.CurrentRound=%d want 0 (no unresolved rounds)", snap.CurrentRound)
	}
	if !snap.Rounds[0].ChallengerCorrect || snap.Rounds[1].ChallengerCorrect {
		t.Error("challenger correctness flags not mapped (round 0 correct, round 1 not)")
	}
	if snap.Rounds[0].OpponentCorrect || !snap.Rounds[1].OpponentCorrect {
		t.Error("opponent correctness flags not mapped (round 1 correct, round 0 not)")
	}
	if snap.ScoreChallenger != 15 || snap.ScoreOpponent != 10 {
		t.Errorf("scores not mapped: challenger=%d opponent=%d", snap.ScoreChallenger, snap.ScoreOpponent)
	}
	if snap.WinnerGCID != "gcid-b" {
		t.Errorf("snap.WinnerGCID=%q want gcid-b", snap.WinnerGCID)
	}
}

// TestSubmitAnswer_DuelNotFound pins the not-found branch: an unknown duel
// returns an error (the handler turns it into an error frame).
func TestSubmitAnswer_DuelNotFound(t *testing.T) {
	repo := &fakeAdapterRepo{}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil)
	_, _, err := a.SubmitAnswer(context.Background(), "missing", "gcid-a", 1, "A", 1000)
	if err == nil {
		t.Fatal("SubmitAnswer must error for an unknown duel")
	}
}

// TestSubmitAnswer_ResolveRoundFailure pins the domain-rejection branch:
// an out-of-range round number must surface the resolve error.
func TestSubmitAnswer_ResolveRoundFailure(t *testing.T) {
	repo := &fakeAdapterRepo{duel: newServableDuel(t)}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil)
	_, _, err := a.SubmitAnswer(context.Background(), repo.duel.ID, "gcid-a", 99, "A", 1000)
	if err == nil {
		t.Fatal("SubmitAnswer must error for an out-of-range round")
	}
}

// TestSubmitAnswer_PersistFailure pins the repo-persist branch: when
// ResolveRound fails after the domain resolved the round, the error
// surfaces and the call returns.
func TestSubmitAnswer_PersistFailure(t *testing.T) {
	d := newServableDuel(t)
	repo := &failingResolveRepo{fakeAdapterRepo: &fakeAdapterRepo{duel: d}}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil)
	_, _, err := a.SubmitAnswer(context.Background(), d.ID, "gcid-a", 1, "A", 1000)
	if err == nil {
		t.Fatal("SubmitAnswer must error when repo.ResolveRound fails")
	}
}

// TestSubmitAnswer_CompletesDuel_AppliesELOAndEvent pins the completion
// path: resolving the final round completes the duel, applies ELO exactly
// once with the K-factor default (0 → 32), and publishes the event.
func TestSubmitAnswer_CompletesDuel_AppliesELOAndEvent(t *testing.T) {
	d := newServableDuel(t)
	repo := &fakeAdapterRepo{duel: d}
	elo := &fakeELO{}
	pub := &fakeEventPub{}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil).
		WithELO(elo, 0).
		WithEventPublisher(pub)

	_, completed, err := a.SubmitAnswer(context.Background(), d.ID, "gcid-a", 1, "A", 1000)
	if err != nil {
		t.Fatalf("round 1: %v", err)
	}
	if completed {
		t.Error("duel completed after round 1 of 2")
	}

	_, completed, err = a.SubmitAnswer(context.Background(), d.ID, "gcid-a", 2, "A", 1000)
	if err != nil {
		t.Fatalf("round 2: %v", err)
	}
	if !completed {
		t.Error("duel must complete after the final round resolves")
	}

	calls := elo.got()
	if len(calls) != 1 {
		t.Fatalf("ApplyELO calls=%v want exactly 1 (only on completion)", calls)
	}
	if calls[0] != d.ID+":k32" {
		t.Errorf("ApplyELO call=%q want %q (kFactor 0 must fall back to 32)", calls[0], d.ID+":k32")
	}
	if n := pub.count(); n != 1 {
		t.Errorf("PublishDuelCompleted calls=%d want 1", n)
	}
}

// TestSubmitAnswer_ELOFailureIsLogged pins the best-effort contract: an ELO
// failure must not break the gameplay response frame.
func TestSubmitAnswer_ELOFailureIsLogged(t *testing.T) {
	d := newServableDuel(t)
	repo := &fakeAdapterRepo{duel: d}
	elo := &fakeELO{err: errors.New("rating boom")}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil).WithELO(elo, 40)

	// Round 1 first, then the completing round 2.
	if _, _, err := a.SubmitAnswer(context.Background(), d.ID, "gcid-a", 1, "A", 1000); err != nil {
		t.Fatalf("round 1: %v", err)
	}
	_, completed, err := a.SubmitAnswer(context.Background(), d.ID, "gcid-a", 2, "A", 1000)
	if err != nil {
		t.Fatalf("round 2: %v", err)
	}
	if !completed {
		t.Error("duel must complete after the final round")
	}
	if calls := elo.got(); len(calls) != 1 || calls[0] != d.ID+":k40" {
		t.Errorf("ApplyELO calls=%v want [%s:k40]", calls, d.ID)
	}
}

// TestSubmitAnswer_SkipsELOForFriendlyScope pins the scope guard: ELO must
// not be applied to non-ranked duels even when completing.
func TestSubmitAnswer_SkipsELOForFriendlyScope(t *testing.T) {
	d := newServableDuel(t)
	d.Scope = duel.ScopeFriendly
	repo := &fakeAdapterRepo{duel: d}
	elo := &fakeELO{}
	pub := &fakeEventPub{}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil).
		WithELO(elo, 32).
		WithEventPublisher(pub)

	if _, _, err := a.SubmitAnswer(context.Background(), d.ID, "gcid-a", 1, "A", 1000); err != nil {
		t.Fatalf("round 1: %v", err)
	}
	if _, _, err := a.SubmitAnswer(context.Background(), d.ID, "gcid-a", 2, "A", 1000); err != nil {
		t.Fatalf("round 2: %v", err)
	}
	if calls := elo.got(); len(calls) != 0 {
		t.Errorf("ApplyELO calls=%v want none for a friendly duel", calls)
	}
	if n := pub.count(); n != 1 {
		t.Errorf("PublishDuelCompleted calls=%d want 1 (event fires regardless of scope)", n)
	}
}

// TestGetCurrentRound_MissingDuel pins the not-found branch of
// GetCurrentRound.
func TestGetCurrentRound_MissingDuel(t *testing.T) {
	repo := &fakeAdapterRepo{duel: newServableDuel(t)}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil)
	if _, ok := a.GetCurrentRound(context.Background(), "missing"); ok {
		t.Error("GetCurrentRound must return ok=false for an unknown duel")
	}
}

// TestGetCurrentRound_DefaultTimerIs30 pins the fallback: an adapter with
// no WithRoundTimerSec still advertises + stamps the 30s default.
func TestGetCurrentRound_DefaultTimerIs30(t *testing.T) {
	repo := &fakeAdapterRepo{duel: newServableDuel(t)}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil) // no timer wired

	frame, ok := a.GetCurrentRound(context.Background(), repo.duel.ID)
	if !ok {
		t.Fatal("GetCurrentRound returned ok=false")
	}
	if frame.TimerSec != 30 {
		t.Errorf("frame.TimerSec=%d want 30 (default fallback)", frame.TimerSec)
	}
	if frame.DeadlineAt == "" {
		t.Error("frame.DeadlineAt empty (lazy stamp must still happen with the default timer)")
	}
}

// TestGetCurrentRound_StampFailureServesWithoutDeadline pins the FAIL-LOUD
// contract: a transient stamp error must not break the serve — the frame is
// delivered without a deadline (the stamp retries on the next serve).
func TestGetCurrentRound_StampFailureServesWithoutDeadline(t *testing.T) {
	repo := &fakeAdapterRepo{duel: newServableDuel(t)}
	repo.stampErr = errors.New("db down")
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil).WithRoundTimerSec(30)

	frame, ok := a.GetCurrentRound(context.Background(), repo.duel.ID)
	if !ok {
		t.Fatal("GetCurrentRound must still serve when the stamp fails")
	}
	if frame.DeadlineAt != "" {
		t.Errorf("frame.DeadlineAt=%q want empty (stamp failed, deadline must be omitted)", frame.DeadlineAt)
	}
}

// TestGetCurrentRound_NoUnresolvedRounds pins the exhausted-loop branch:
// a duel whose rounds are all resolved has no current round.
func TestGetCurrentRound_NoUnresolvedRounds(t *testing.T) {
	d := newServableDuel(t)
	now := time.Now().UTC()
	d.Rounds[0].ResolvedAt = &now
	d.Rounds[1].ResolvedAt = &now
	repo := &fakeAdapterRepo{duel: d}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil).WithRoundTimerSec(30)

	if _, ok := a.GetCurrentRound(context.Background(), d.ID); ok {
		t.Error("GetCurrentRound must return ok=false when every round is resolved")
	}
}

// TestGetBlitzStart_MissingDuel pins the not-found branch.
func TestGetBlitzStart_MissingDuel(t *testing.T) {
	repo := &fakeAdapterRepo{duel: newBlitzServableDuel(t)}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil)
	if _, ok := a.GetBlitzStart(context.Background(), "missing"); ok {
		t.Error("GetBlitzStart must return ok=false for an unknown duel")
	}
}

// TestGetBlitzStart_PickerFallbackForProjectionRounds pins the embedded
// vs picker fallback: a round without embedded content (projection-backed
// rounds) must fall back to the atom picker.
func TestGetBlitzStart_PickerFallbackForProjectionRounds(t *testing.T) {
	d := newBlitzServableDuel(t)
	d.Rounds[0].Question = "" // projection-backed: no embedded content
	d.Rounds[0].Options = nil
	repo := &fakeAdapterRepo{duel: d}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil)

	frame, ok := a.GetBlitzStart(context.Background(), d.ID)
	if !ok {
		t.Fatal("GetBlitzStart returned ok=false")
	}
	if frame.Questions[0].Question != "Q" {
		t.Errorf("round 1 question=%q want %q (picker fallback for empty embedded content)", frame.Questions[0].Question, "Q")
	}
	if len(frame.Questions[0].Options) != 2 {
		t.Errorf("round 1 options len=%d want 2", len(frame.Questions[0].Options))
	}
}

// TestGetBlitzStart_RaceVariantCarriesTarget pins the race-variant fields:
// a race blitz frame must carry race_target (not time_limit_sec).
func TestGetBlitzStart_RaceVariantCarriesTarget(t *testing.T) {
	d, err := duel.NewBlitzPoolDuel(
		duel.DuelConfig{},
		"gcid-a", "gcid-b", "tenant-1",
		duel.ScopeRanked, 3, nil,
		duel.BlitzConfig{Variant: duel.BlitzVariantRace, RaceTarget: 2},
	)
	if err != nil {
		t.Fatalf("NewBlitzPoolDuel: %v", err)
	}
	if err := d.StartBattle([]duel.AtomPick{
		{AtomID: "atom-1", Question: "Q1", Options: []string{"A", "B"}, Answer: "A"},
		{AtomID: "atom-2", Question: "Q2", Options: []string{"C", "D"}, Answer: "C"},
		{AtomID: "atom-3", Question: "Q3", Options: []string{"E", "F"}, Answer: "E"},
	}); err != nil {
		t.Fatalf("StartBattle: %v", err)
	}
	repo := &fakeAdapterRepo{duel: d}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil)

	frame, ok := a.GetBlitzStart(context.Background(), d.ID)
	if !ok {
		t.Fatal("GetBlitzStart returned ok=false")
	}
	if frame.RaceTarget != 2 {
		t.Errorf("frame.RaceTarget=%d want 2", frame.RaceTarget)
	}
	if frame.TimeLimitSec != 0 {
		t.Errorf("frame.TimeLimitSec=%d want 0 for a race blitz", frame.TimeLimitSec)
	}
}

// --- SubmitBlitzAnswer branches ---

func TestSubmitBlitzAnswer_DuelNotFound(t *testing.T) {
	repo := &fakeAdapterRepo{}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil)
	_, _, err := a.SubmitBlitzAnswer(context.Background(), "missing", "gcid-a", 1, "A", 1000)
	if err == nil {
		t.Fatal("SubmitBlitzAnswer must error for an unknown duel")
	}
}

func TestSubmitBlitzAnswer_ResolveFailure(t *testing.T) {
	repo := &fakeAdapterRepo{duel: newBlitzServableDuel(t)}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil)
	_, _, err := a.SubmitBlitzAnswer(context.Background(), repo.duel.ID, "gcid-a", 99, "A", 1000)
	if err == nil {
		t.Fatal("SubmitBlitzAnswer must error for an out-of-range round")
	}
}

func TestSubmitBlitzAnswer_PersistFailure(t *testing.T) {
	d := newBlitzServableDuel(t)
	repo := &failingSaveRepo{fakeAdapterRepo: &fakeAdapterRepo{duel: d}}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil)
	_, _, err := a.SubmitBlitzAnswer(context.Background(), d.ID, "gcid-a", 1, "A", 1000)
	if err == nil {
		t.Fatal("SubmitBlitzAnswer must error when repo.SaveDuel fails")
	}
}

// TestSubmitBlitzAnswer_CompletesDuel_AppliesELOAndEvent pins the blitz
// completion path: when both players answer every round, the duel
// completes, ELO is applied once, and the event fires.
func TestSubmitBlitzAnswer_CompletesDuel_AppliesELOAndEvent(t *testing.T) {
	d := newBlitzServableDuel(t)
	repo := &fakeAdapterRepo{duel: d}
	elo := &fakeELO{}
	pub := &fakeEventPub{}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil).
		WithELO(elo, 0).
		WithEventPublisher(pub)

	answers := []struct {
		gcid   string
		round  int32
		answer string
	}{
		{"gcid-a", 1, "A"},
		{"gcid-b", 1, "A"},
		{"gcid-a", 2, "C"},
		{"gcid-b", 2, "C"},
	}
	var completed bool
	var err error
	for i, ans := range answers {
		_, completed, err = a.SubmitBlitzAnswer(context.Background(), d.ID, ans.gcid, ans.round, ans.answer, 1000)
		if err != nil {
			t.Fatalf("answer %d: %v", i, err)
		}
	}
	if !completed {
		t.Fatal("blitz duel must complete when both players answer every round")
	}

	calls := elo.got()
	if len(calls) != 1 {
		t.Fatalf("ApplyELO calls=%v want exactly 1", calls)
	}
	if calls[0] != d.ID+":k32" {
		t.Errorf("ApplyELO call=%q want %q", calls[0], d.ID+":k32")
	}
	if n := pub.count(); n != 1 {
		t.Errorf("PublishDuelCompleted calls=%d want 1", n)
	}
}

// TestSubmitBlitzAnswer_ELOFailureIsLogged pins the best-effort contract on
// the blitz path: an ELO failure never breaks the answer response.
func TestSubmitBlitzAnswer_ELOFailureIsLogged(t *testing.T) {
	d := newBlitzServableDuel(t)
	repo := &fakeAdapterRepo{duel: d}
	elo := &fakeELO{err: errors.New("rating boom")}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil).WithELO(elo, 32)

	for _, ans := range []struct {
		gcid   string
		round  int32
		answer string
	}{
		{"gcid-a", 1, "A"},
		{"gcid-b", 1, "A"},
		{"gcid-a", 2, "C"},
		{"gcid-b", 2, "C"},
	} {
		if _, _, err := a.SubmitBlitzAnswer(context.Background(), d.ID, ans.gcid, ans.round, ans.answer, 1000); err != nil {
			t.Fatalf("answer: %v", err)
		}
	}
	if calls := elo.got(); len(calls) != 1 {
		t.Errorf("ApplyELO calls=%v want 1 (blitz completion applies ELO once)", calls)
	}
}