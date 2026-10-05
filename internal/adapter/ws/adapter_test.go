package ws

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
)

// fakeAdapterRepo is a test double for DuelRepoPort. It stores one duel
// and records StampRoundDeadline calls so tests can assert the lazy
// first-serve deadline stamp.
type fakeAdapterRepo struct {
	mu         sync.Mutex
	duel       *duel.Duel
	stampCalls []stampCall
	stampErr   error
}

type stampCall struct {
	duelID   string
	roundNo  int
	deadline time.Time
}

func (r *fakeAdapterRepo) GetDuel(_ context.Context, duelID string) (*duel.Duel, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.duel == nil || r.duel.ID != duelID {
		return nil, nil
	}
	return r.duel, nil
}

func (r *fakeAdapterRepo) GetDuelForUpdate(ctx context.Context, duelID string) (*duel.Duel, error) {
	return r.GetDuel(ctx, duelID)
}

func (r *fakeAdapterRepo) ResolveRound(_ context.Context, _ *duel.Duel, _ int, _ string, _ duel.RoundResolution) error {
	return nil
}

func (r *fakeAdapterRepo) SaveDuel(_ context.Context, d *duel.Duel) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.duel = d
	return nil
}

func (r *fakeAdapterRepo) StampRoundDeadline(_ context.Context, duelID string, roundNo int, deadline time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stampCalls = append(r.stampCalls, stampCall{duelID: duelID, roundNo: roundNo, deadline: deadline})
	if r.stampErr != nil {
		return r.stampErr
	}
	if r.duel != nil && r.duel.ID == duelID && roundNo >= 1 && roundNo <= len(r.duel.Rounds) {
		if r.duel.Rounds[roundNo-1].DeadlineAt == nil {
			r.duel.Rounds[roundNo-1].DeadlineAt = &deadline
		}
	}
	return nil
}

func (r *fakeAdapterRepo) stamps() []stampCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]stampCall(nil), r.stampCalls...)
}

type fakePicker struct{}

func (fakePicker) GetAtomForRound(_ context.Context, _ *duel.Duel, _ int) (duel.AtomPick, error) {
	return duel.AtomPick{AtomID: "atom-1", Question: "Q", Options: []string{"A", "B"}, Answer: "A"}, nil
}

// newServableDuel builds an in_progress 2-round duel whose round 1 has NO
// stamped deadline (the post-fix StartBattle behaviour — round 1 is
// stamped lazily on first serve).
func newServableDuel(t *testing.T) *duel.Duel {
	t.Helper()
	d, err := duel.NewDuel(duel.DuelConfig{RoundTimerSec: 30}, "gcid-a", "gcid-b", "tenant-1", duel.ScopeRanked, 2, nil, 24*time.Hour)
	if err != nil {
		t.Fatalf("NewDuel: %v", err)
	}
	if err := d.Accept("gcid-b"); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if err := d.StartBattle([]duel.AtomPick{
		{AtomID: "atom-1", Question: "Q1", Options: []string{"A", "B"}, Answer: "A"},
		{AtomID: "atom-2", Question: "Q2", Options: []string{"C", "D"}, Answer: "C"},
	}); err != nil {
		t.Fatalf("StartBattle: %v", err)
	}
	return d
}

// TestGetCurrentRound_StampsDeadlineLazily pins the lazy-stamp rule: the
// first serve of a round with no deadline MUST stamp now + timerSec and
// persist it, so the enforced timer starts when a player actually sees
// the question — not at match time (heartbeat polling discovers matches
// up to ~10s late).
func TestGetCurrentRound_StampsDeadlineLazily(t *testing.T) {
	repo := &fakeAdapterRepo{duel: newServableDuel(t)}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil).WithRoundTimerSec(30)

	before := time.Now().UTC()
	frame, ok := a.GetCurrentRound(context.Background(), repo.duel.ID)
	if !ok {
		t.Fatal("GetCurrentRound returned ok=false for an in-progress duel")
	}
	if frame.DeadlineAt == "" {
		t.Fatal("frame.DeadlineAt is empty (must stamp now + timerSec on first serve)")
	}
	parsed, err := time.Parse("2006-01-02T15:04:05.000000Z", frame.DeadlineAt)
	if err != nil {
		t.Fatalf("frame.DeadlineAt %q unparseable: %v", frame.DeadlineAt, err)
	}
	if parsed.Before(before.Add(29*time.Second)) || parsed.After(time.Now().UTC().Add(31*time.Second)) {
		t.Errorf("frame.DeadlineAt=%s want ≈ now+30s", frame.DeadlineAt)
	}

	calls := repo.stamps()
	if len(calls) != 1 {
		t.Fatalf("StampRoundDeadline calls=%d want 1 (persist the lazy stamp)", len(calls))
	}
	if calls[0].roundNo != 1 {
		t.Errorf("StampRoundDeadline roundNo=%d want 1", calls[0].roundNo)
	}

	// The frame must carry the server's clock so the FE can sync its
	// countdown offset from the question frame itself.
	if frame.ServerNow == "" {
		t.Fatal("frame.ServerNow is empty (FE needs it for clock-skew correction)")
	}
	nowParsed, err := time.Parse("2006-01-02T15:04:05.000000Z", frame.ServerNow)
	if err != nil {
		t.Fatalf("frame.ServerNow %q unparseable: %v", frame.ServerNow, err)
	}
	if nowParsed.Before(before.Add(-time.Second)) || nowParsed.After(time.Now().UTC().Add(time.Second)) {
		t.Errorf("frame.ServerNow=%s want ≈ now", frame.ServerNow)
	}
}

// TestSubmitAnswer_StampsNextRoundWithConfiguredTimer pins the
// rehydration bug: a duel fetched from the DB has roundTimerSec=0 (the
// field is not persisted), so the domain's next-round stamp fell back to
// 30s even though the service is configured for 15s. The adapter must
// re-apply the configured timer after fetching so rounds 2+ get the same
// 15s as round 1.
func TestSubmitAnswer_StampsNextRoundWithConfiguredTimer(t *testing.T) {
	// Duel built with NO RoundTimerSec — simulates a DB re-read.
	d, err := duel.NewDuel(duel.DuelConfig{}, "gcid-a", "gcid-b", "tenant-1", duel.ScopeRanked, 2, nil, 24*time.Hour)
	if err != nil {
		t.Fatalf("NewDuel: %v", err)
	}
	if err := d.Accept("gcid-b"); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if err := d.StartBattle([]duel.AtomPick{
		{AtomID: "atom-1", Question: "Q1", Options: []string{"A", "B"}, Answer: "A"},
		{AtomID: "atom-2", Question: "Q2", Options: []string{"C", "D"}, Answer: "C"},
	}); err != nil {
		t.Fatalf("StartBattle: %v", err)
	}
	repo := &fakeAdapterRepo{duel: d}
	a := NewDuelSessionAdapter(repo, fakePicker{}, []int{1}).WithRoundTimerSec(15)

	before := time.Now().UTC()
	if _, _, err := a.SubmitAnswer(context.Background(), d.ID, "gcid-a", 1, "A", 3000); err != nil {
		t.Fatalf("SubmitAnswer: %v", err)
	}
	next := d.Rounds[1].DeadlineAt
	if next == nil {
		t.Fatal("round 2 DeadlineAt nil after round 1 resolved")
	}
	if next.Before(before.Add(14*time.Second)) || next.After(time.Now().UTC().Add(16*time.Second)) {
		t.Errorf("round 2 deadline=%v want ≈ now+15s (rehydration bug: domain fell back to 30s on a DB-fetched duel)", next)
	}
}

// TestGetCurrentRound_FormatsDeadlineInUTC pins the timezone bug: pgx
// returns timestamptz in the machine's LOCAL zone (e.g. UTC+7), and the
// wire layout "2006-01-02T15:04:05.000000Z" carries a LITERAL Z — so
// formatting a local-zoned time directly prints local wall time mislabeled
// as UTC, throwing the FE countdown off by the tz offset (7h → "420:07"
// on a 15s round). The frame must format the instant in UTC.
func TestGetCurrentRound_FormatsDeadlineInUTC(t *testing.T) {
	d := newServableDuel(t)
	ict := time.FixedZone("ICT", 7*3600)
	existing := time.Now().UTC().Add(12 * time.Second).In(ict) // same instant, +07 location
	d.Rounds[0].DeadlineAt = &existing
	repo := &fakeAdapterRepo{duel: d}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil).WithRoundTimerSec(30)

	frame, ok := a.GetCurrentRound(context.Background(), d.ID)
	if !ok {
		t.Fatal("GetCurrentRound returned ok=false")
	}
	parsed, err := time.Parse("2006-01-02T15:04:05.000000Z", frame.DeadlineAt)
	if err != nil {
		t.Fatalf("frame.DeadlineAt %q unparseable: %v", frame.DeadlineAt, err)
	}
	// The parsed instant must match the stored instant — a local-zoned
	// format would parse 7h in the future.
	if parsed.Sub(existing).Abs() > time.Second {
		t.Errorf("frame.DeadlineAt=%s parses to a different instant than stored %s (tz-mislabel bug — must format in UTC)", frame.DeadlineAt, existing)
	}
}
// round keeps its existing deadline (the second player to connect must NOT
// reset the timer) and no persistence call is made.
func TestGetCurrentRound_DoesNotRestamp(t *testing.T) {
	d := newServableDuel(t)
	existing := time.Now().UTC().Add(12 * time.Second)
	d.Rounds[0].DeadlineAt = &existing
	repo := &fakeAdapterRepo{duel: d}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil).WithRoundTimerSec(30)

	frame, ok := a.GetCurrentRound(context.Background(), d.ID)
	if !ok {
		t.Fatal("GetCurrentRound returned ok=false")
	}
	parsed, err := time.Parse("2006-01-02T15:04:05.000000Z", frame.DeadlineAt)
	if err != nil {
		t.Fatalf("frame.DeadlineAt %q unparseable: %v", frame.DeadlineAt, err)
	}
	if parsed.Sub(existing).Abs() > time.Second {
		t.Errorf("frame.DeadlineAt=%s want existing stamp %s (must not reset the timer)", frame.DeadlineAt, existing)
	}
	if n := len(repo.stamps()); n != 0 {
		t.Errorf("StampRoundDeadline calls=%d want 0 (already stamped)", n)
	}
}

// --- Blitz mode adapter tests ---

// newBlitzServableDuel builds an in_progress blitz duel with 2 rounds.
func newBlitzServableDuel(t *testing.T) *duel.Duel {
	t.Helper()
	d, err := duel.NewBlitzPoolDuel(
		duel.DuelConfig{},
		"gcid-a", "gcid-b", "tenant-1",
		duel.ScopeRanked, 2, nil,
		duel.BlitzConfig{Variant: duel.BlitzVariantTimed, TimeLimitSec: 120},
	)
	if err != nil {
		t.Fatalf("NewBlitzPoolDuel: %v", err)
	}
	if err := d.StartBattle([]duel.AtomPick{
		{AtomID: "atom-1", Question: "Q1", Options: []string{"A", "B"}, Answer: "A"},
		{AtomID: "atom-2", Question: "Q2", Options: []string{"C", "D"}, Answer: "C"},
	}); err != nil {
		t.Fatalf("StartBattle: %v", err)
	}
	return d
}

// TestGetBlitzStart_ReturnsAllQuestions pins the blitz delivery model:
// GetBlitzStart returns ALL questions at once (not one at a time like
// classic GetCurrentRound). The frame carries the mode, variant, and
// time limit so the FE can render the blitz UI.
func TestGetBlitzStart_ReturnsAllQuestions(t *testing.T) {
	d := newBlitzServableDuel(t)
	repo := &fakeAdapterRepo{duel: d}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil)

	frame, ok := a.GetBlitzStart(context.Background(), d.ID)
	if !ok {
		t.Fatal("GetBlitzStart returned ok=false for an in-progress blitz duel")
	}
	if len(frame.Questions) != 2 {
		t.Fatalf("questions=%d want 2 (all questions delivered at once)", len(frame.Questions))
	}
	if frame.Mode != "blitz" {
		t.Errorf("mode=%q want blitz", frame.Mode)
	}
	if frame.BlitzVariant != "timed" {
		t.Errorf("blitz_variant=%q want timed", frame.BlitzVariant)
	}
	if frame.TimeLimitSec != 120 {
		t.Errorf("time_limit_sec=%d want 120", frame.TimeLimitSec)
	}
}

// TestGetBlitzStart_ReturnsFalseForClassicDuel pins the routing: a classic
// duel must NOT produce a blitz_start frame (the handler uses GetBlitzStart
// to detect blitz mode — a false positive would send the wrong frame kind).
func TestGetBlitzStart_ReturnsFalseForClassicDuel(t *testing.T) {
	d := newServableDuel(t)
	repo := &fakeAdapterRepo{duel: d}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil)

	_, ok := a.GetBlitzStart(context.Background(), d.ID)
	if ok {
		t.Error("GetBlitzStart returned ok=true for a classic duel (must be false)")
	}
}

// TestGetCurrentRound_ReturnsFalseForBlitzDuel pins the routing: a blitz
// duel must NOT produce a round_start frame via GetCurrentRound (blitz
// uses GetBlitzStart instead).
func TestGetCurrentRound_ReturnsFalseForBlitzDuel(t *testing.T) {
	d := newBlitzServableDuel(t)
	repo := &fakeAdapterRepo{duel: d}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil).WithRoundTimerSec(30)

	_, ok := a.GetCurrentRound(context.Background(), d.ID)
	if ok {
		t.Error("GetCurrentRound returned ok=true for a blitz duel (must be false — blitz uses GetBlitzStart)")
	}
}

// TestSubmitBlitzAnswer_RecordsAnswer pins the blitz answer flow: a correct
// answer awards points but does NOT resolve the round (the opponent can
// still answer independently). The duel stays in_progress.
func TestSubmitBlitzAnswer_RecordsAnswer(t *testing.T) {
	d := newBlitzServableDuel(t)
	repo := &fakeAdapterRepo{duel: d}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil)

	frame, completed, err := a.SubmitBlitzAnswer(context.Background(), d.ID, "gcid-a", 1, "A", 5000)
	if err != nil {
		t.Fatalf("SubmitBlitzAnswer: %v", err)
	}
	if !frame.Correct {
		t.Error("want correct=true")
	}
	if frame.RoundResolved {
		t.Error("round resolved after only one player answered (blitz keeps round open)")
	}
	if completed {
		t.Error("duel completed after one answer (should stay in_progress)")
	}
	if frame.DuelStatus != "in_progress" {
		t.Errorf("duel_status=%q want in_progress", frame.DuelStatus)
	}
}

// TestSubmitBlitzAnswer_BothAnswerResolvesRound pins the blitz round
// resolution: when both players answer, the round resolves.
func TestSubmitBlitzAnswer_BothAnswerResolvesRound(t *testing.T) {
	d := newBlitzServableDuel(t)
	repo := &fakeAdapterRepo{duel: d}
	a := NewDuelSessionAdapter(repo, fakePicker{}, nil)

	_, _, err := a.SubmitBlitzAnswer(context.Background(), d.ID, "gcid-a", 1, "A", 5000)
	if err != nil {
		t.Fatalf("challenger answer: %v", err)
	}

	frame, _, err := a.SubmitBlitzAnswer(context.Background(), d.ID, "gcid-b", 1, "A", 8000)
	if err != nil {
		t.Fatalf("opponent answer: %v", err)
	}
	if !frame.RoundResolved {
		t.Error("round not resolved after both players answered")
	}
}
