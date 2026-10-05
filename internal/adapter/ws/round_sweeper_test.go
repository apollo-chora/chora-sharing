package ws

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
)

// fakeSweeperRepo is a test double for RoundSweeperRepo. It records calls
// and returns canned data so tests can assert the sweeper's per-tenant loop,
// RLS context propagation, and resolution behaviour without a live DB.
type fakeSweeperRepo struct {
	mu sync.Mutex
	// duelsByTenant holds in-progress duels keyed by tenantID.
	duelsByTenant map[string][]*duel.Duel
	// storedDuels mirrors the inmem repo's byID so ResolveRound mutations
	// persist for subsequent GetDuelForUpdate calls.
	storedDuels map[string]*duel.Duel

	listCalls           []listCall
	listErr             error
	getDuelForUpdateErr error
	resolveCalls        []resolveCall
	resolveErr          error
}

type listCall struct {
	tenantID string
	now      time.Time
}

type resolveCall struct {
	duelID  string
	roundNo int
	gcid    string
}

func newFakeSweeperRepo() *fakeSweeperRepo {
	return &fakeSweeperRepo{
		duelsByTenant: make(map[string][]*duel.Duel),
		storedDuels:   make(map[string]*duel.Duel),
	}
}

func (r *fakeSweeperRepo) addDuel(d *duel.Duel) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.duelsByTenant[d.TenantID] = append(r.duelsByTenant[d.TenantID], d)
	r.storedDuels[d.ID] = d
}

// roundDeadline reads a stored duel's round deadline UNDER THE REPO LOCK and
// returns a copy.
//
// The sweeper mutates the duel in place (ResolveRoundTimeout) on its own
// goroutine and then calls ResolveRound, which takes r.mu. Reading the same
// pointer straight from the test body therefore races the sweep; reading here
// puts the read behind that mutex, so the sweeper's write happens-before it.
// In production the pg repo hydrates a fresh struct per call, so the aliasing
// is an artefact of the double rather than a production hazard.
func (r *fakeSweeperRepo) roundDeadline(duelID string, roundIdx int) *time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	d := r.storedDuels[duelID]
	if d == nil || roundIdx < 0 || roundIdx >= len(d.Rounds) || d.Rounds[roundIdx].DeadlineAt == nil {
		return nil
	}
	cp := *d.Rounds[roundIdx].DeadlineAt
	return &cp
}

func (r *fakeSweeperRepo) ListDuelsWithExpiredRounds(_ context.Context, tenantID string, now time.Time) ([]*duel.Duel, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listCalls = append(r.listCalls, listCall{tenantID: tenantID, now: now})
	if r.listErr != nil {
		return nil, r.listErr
	}
	var out []*duel.Duel
	for _, d := range r.duelsByTenant[tenantID] {
		if d.Status != duel.StatusInProgress {
			continue
		}
		for _, rd := range d.Rounds {
			if rd.ResolvedAt == nil && rd.DeadlineAt != nil && now.After(*rd.DeadlineAt) {
				out = append(out, d)
				break
			}
		}
	}
	return out, nil
}

func (r *fakeSweeperRepo) GetDuelForUpdate(_ context.Context, duelID string) (*duel.Duel, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.getDuelForUpdateErr != nil {
		return nil, r.getDuelForUpdateErr
	}
	d, ok := r.storedDuels[duelID]
	if !ok {
		return nil, errors.New("duel not found")
	}
	return d, nil
}

func (r *fakeSweeperRepo) ResolveRound(_ context.Context, d *duel.Duel, roundNo int, gcid string, _ duel.RoundResolution) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resolveCalls = append(r.resolveCalls, resolveCall{duelID: d.ID, roundNo: roundNo, gcid: gcid})
	if r.resolveErr != nil {
		return r.resolveErr
	}
	r.storedDuels[d.ID] = d
	return nil
}

// newExpiredDuel builds a duel with one round whose deadline has passed,
// already in_progress. Used by sweeper tests.
func newExpiredDuel(t *testing.T, tenantID string) *duel.Duel {
	t.Helper()
	d, err := duel.NewDuel(duel.DuelConfig{RoundTimerSec: 30}, "gcid-a", "gcid-b", tenantID, duel.ScopeRanked, 1, nil, 24*time.Hour)
	if err != nil {
		t.Fatalf("NewDuel: %v", err)
	}
	if err := d.Accept("gcid-b"); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if err := d.StartBattle([]duel.AtomPick{
		{AtomID: "atom-1", Question: "Q", Options: []string{"A", "B"}, Answer: "A"},
	}); err != nil {
		t.Fatalf("StartBattle: %v", err)
	}
	// Force the deadline into the past so the sweeper picks it up.
	past := time.Now().UTC().Add(-time.Minute)
	d.Rounds[0].DeadlineAt = &past
	return d
}

// TestRoundSweeper_ResolvesExpiredRound pins the happy path: the sweeper
// finds an expired round, resolves it as both-unanswered, persists the
// resolution, and broadcasts a round_timeout frame.
func TestRoundSweeper_ResolvesExpiredRound(t *testing.T) {
	const tenantID = "tenant-1"
	repo := newFakeSweeperRepo()
	d := newExpiredDuel(t, tenantID)
	repo.addDuel(d)

	broker := NewBroker()
	ch, unsub := broker.Subscribe(d.ID, "test-sub")
	defer unsub()

	// Appended from the sweeper's goroutine, so it needs a lock even though
	// the test never reads it back.
	var logMu sync.Mutex
	var logMsgs []string
	sweeper := NewRoundSweeper(repo, broker, 50*time.Millisecond,
		WithRoundSweeperLogger(func(format string, args ...any) {
			logMu.Lock()
			logMsgs = append(logMsgs, format)
			logMu.Unlock()
		}),
	)
	sweeper.RegisterTenant(tenantID)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sweeper.Start(ctx)

	// Wait for the round_timeout frame.
	select {
	case msg := <-ch:
		if msg.Kind != KindRoundTimeout {
			t.Fatalf("frame kind=%s want %s", msg.Kind, KindRoundTimeout)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for round_timeout frame")
	}

	// The repo must have received a ResolveRound call.
	repo.mu.Lock()
	if len(repo.resolveCalls) == 0 {
		t.Fatal("ResolveRound was never called (sweeper did not persist the timeout)")
	}
	if repo.resolveCalls[0].roundNo != 1 {
		t.Errorf("ResolveRound roundNo=%d want 1", repo.resolveCalls[0].roundNo)
	}
	repo.mu.Unlock()
}

// fakeRoundStarter is a test double for the sweeper's RoundStarter port —
// returns a canned round_start frame.
type fakeRoundStarter struct {
	frame RoundStartFrame
	ok    bool
}

func (f fakeRoundStarter) GetCurrentRound(_ context.Context, _ string) (RoundStartFrame, bool) {
	return f.frame, f.ok
}

// TestRoundSweeper_PublishesNextRoundStart pins the auto-advance: after a
// non-final round times out, the sweeper must push the NEXT round's
// round_start frame so both clients move to the next question instead of
// staying stuck on the timed-out one.
func TestRoundSweeper_PublishesNextRoundStart(t *testing.T) {
	const tenantID = "tenant-1"
	repo := newFakeSweeperRepo()

	// 2-round duel; round 1 expired.
	d, err := duel.NewDuel(duel.DuelConfig{RoundTimerSec: 30}, "gcid-a", "gcid-b", tenantID, duel.ScopeRanked, 2, nil, 24*time.Hour)
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
	past := time.Now().UTC().Add(-time.Minute)
	d.Rounds[0].DeadlineAt = &past
	repo.addDuel(d)

	broker := NewBroker()
	ch, unsub := broker.Subscribe(d.ID, "test-sub")
	defer unsub()

	starter := fakeRoundStarter{
		frame: RoundStartFrame{RoundNo: 2, AtomID: "atom-2", Question: "Q2", Options: []string{"C", "D"}, TimerSec: 30},
		ok:    true,
	}
	sweeper := NewRoundSweeper(repo, broker, 50*time.Millisecond,
		WithRoundSweeperRoundStarter(starter),
	)
	sweeper.RegisterTenant(tenantID)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sweeper.Start(ctx)

	// Expect round_timeout FIRST, then round_start for round 2.
	kinds := map[string]bool{}
	timeout := time.After(2 * time.Second)
	for len(kinds) < 2 {
		select {
		case msg := <-ch:
			kinds[string(msg.Kind)] = true
			if msg.Kind == KindRoundStart {
				var frame RoundStartFrame
				if err := json.Unmarshal(msg.Payload, &frame); err != nil {
					t.Fatalf("round_start payload: %v", err)
				}
				if frame.RoundNo != 2 {
					t.Errorf("round_start round_no=%d want 2 (the NEXT round)", frame.RoundNo)
				}
			}
		case <-timeout:
			t.Fatalf("timed out waiting for frames; got %v", kinds)
		}
	}
	if !kinds[string(KindRoundTimeout)] {
		t.Error("round_timeout frame missing")
	}
	if !kinds[string(KindRoundStart)] {
		t.Error("round_start frame missing (sweeper must auto-advance the duel after a timeout)")
	}
}

// TestRoundSweeper_PublishesDuelCompletedFrame pins the terminal path:
// when the LAST round times out, the sweeper must broadcast a
// duel_completed frame — otherwise both clients stay stuck in the arena
// after the final timeout.
func TestRoundSweeper_PublishesDuelCompletedFrame(t *testing.T) {
	const tenantID = "tenant-1"
	repo := newFakeSweeperRepo()
	d := newExpiredDuel(t, tenantID) // 1 round, expired
	repo.addDuel(d)

	broker := NewBroker()
	ch, unsub := broker.Subscribe(d.ID, "test-sub")
	defer unsub()

	sweeper := NewRoundSweeper(repo, broker, 50*time.Millisecond)
	sweeper.RegisterTenant(tenantID)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sweeper.Start(ctx)

	kinds := map[string]bool{}
	timeout := time.After(2 * time.Second)
	for len(kinds) < 2 {
		select {
		case msg := <-ch:
			kinds[string(msg.Kind)] = true
		case <-timeout:
			t.Fatalf("timed out waiting for frames; got %v", kinds)
		}
	}
	if !kinds[string(KindRoundTimeout)] {
		t.Error("round_timeout frame missing")
	}
	if !kinds[string(KindDuelCompleted)] {
		t.Error("duel_completed frame missing (final-round timeout must broadcast duel_completed)")
	}
}

// TestRoundSweeper_StampsNextRoundWithConfiguredTimer pins the
// rehydration bug on the timeout path: the duel the sweeper fetches has
// roundTimerSec=0 (not persisted), so the domain's next-round stamp fell
// back to 30s. The sweeper must re-apply the configured timer so rounds
// 2+ get the same 15s as round 1.
func TestRoundSweeper_StampsNextRoundWithConfiguredTimer(t *testing.T) {
	const tenantID = "tenant-1"
	repo := newFakeSweeperRepo()

	// 2-round duel, roundTimerSec=0 (simulates a DB re-read), round 1 expired.
	d, err := duel.NewDuel(duel.DuelConfig{}, "gcid-a", "gcid-b", tenantID, duel.ScopeRanked, 2, nil, 24*time.Hour)
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
	past := time.Now().UTC().Add(-time.Minute)
	d.Rounds[0].DeadlineAt = &past
	repo.addDuel(d)

	broker := NewBroker()
	sweeper := NewRoundSweeper(repo, broker, 50*time.Millisecond,
		WithRoundSweeperTimerSec(15),
	)
	sweeper.RegisterTenant(tenantID)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sweeper.Start(ctx)
	time.Sleep(300 * time.Millisecond)

	// Stop the sweeper before asserting so no further sweep can mutate the
	// duel while we read it, then read through the repo's lock.
	cancel()
	time.Sleep(100 * time.Millisecond)
	next := repo.roundDeadline(d.ID, 1)
	if next == nil {
		t.Fatal("round 2 DeadlineAt nil after round 1 timed out")
	}
	now := time.Now().UTC()
	if next.Before(now.Add(13*time.Second)) || next.After(now.Add(17*time.Second)) {
		t.Errorf("round 2 deadline=%v want ≈ now+15s (rehydration bug: domain fell back to 30s on a DB-fetched duel)", next)
	}
}

// TestRoundSweeper_PerTenantLoop pins fix #1: the sweeper only sweeps
// tenants that have been registered, and it passes the tenantID into
// ListDuelsWithExpiredRounds so the pg adapter can apply RLS.
func TestRoundSweeper_PerTenantLoop(t *testing.T) {
	const tenantA = "tenant-a"
	const tenantB = "tenant-b"
	repo := newFakeSweeperRepo()

	// tenantA has an expired duel; tenantB has one too but is NOT registered.
	dA := newExpiredDuel(t, tenantA)
	repo.addDuel(dA)
	dB := newExpiredDuel(t, tenantB)
	repo.addDuel(dB)

	broker := NewBroker()
	sweeper := NewRoundSweeper(repo, broker, 50*time.Millisecond)
	sweeper.RegisterTenant(tenantA) // only A

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	sweeper.Start(ctx)
	time.Sleep(300 * time.Millisecond)

	repo.mu.Lock()
	defer repo.mu.Unlock()
	// Only tenantA should have been polled.
	tenantsPolled := make(map[string]bool)
	for _, c := range repo.listCalls {
		tenantsPolled[c.tenantID] = true
	}
	if !tenantsPolled[tenantA] {
		t.Error("tenantA was not polled (must be polled — it was registered)")
	}
	if tenantsPolled[tenantB] {
		t.Error("tenantB was polled (must NOT be — it was not registered)")
	}
}

// TestRoundSweeper_LoudLogOnGetDuelForUpdateFailure pins fix #1: a
// GetDuelForUpdate failure must be logged (not silently swallowed as
// before). This is the regression guard for the RLS no-op bug.
func TestRoundSweeper_LoudLogOnGetDuelForUpdateFailure(t *testing.T) {
	const tenantID = "tenant-1"
	repo := newFakeSweeperRepo()
	d := newExpiredDuel(t, tenantID)
	repo.addDuel(d)
	repo.getDuelForUpdateErr = errors.New("rls: tenant_id missing on context")

	broker := NewBroker()
	var logCount int32
	sweeper := NewRoundSweeper(repo, broker, 50*time.Millisecond,
		WithRoundSweeperLogger(func(format string, args ...any) {
			atomic.AddInt32(&logCount, 1)
		}),
	)
	sweeper.RegisterTenant(tenantID)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	sweeper.Start(ctx)
	time.Sleep(300 * time.Millisecond)

	if atomic.LoadInt32(&logCount) == 0 {
		t.Fatal("GetDuelForUpdate failure was not logged (must be loud — the RLS no-op bug was caused by silent swallowing)")
	}
}

// TestRoundSweeper_KeepsTenantWithNoExpiredDuels pins the behaviour: a
// tenant with no expired duels STAYS registered (unlike the matchmaker
// which prunes drained pools). An in-progress duel's rounds expire on a
// timer, so the tenant must stay registered until the duel completes.
func TestRoundSweeper_KeepsTenantWithNoExpiredDuels(t *testing.T) {
	const tenantID = "tenant-1"
	repo := newFakeSweeperRepo()
	// Add a duel that is in_progress but NOT expired (deadline in the future).
	d, err := duel.NewDuel(duel.DuelConfig{RoundTimerSec: 30}, "gcid-a", "gcid-b", tenantID, duel.ScopeRanked, 1, nil, 24*time.Hour)
	if err != nil {
		t.Fatalf("NewDuel: %v", err)
	}
	if err := d.Accept("gcid-b"); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if err := d.StartBattle([]duel.AtomPick{
		{AtomID: "atom-1", Question: "Q", Options: []string{"A", "B"}, Answer: "A"},
	}); err != nil {
		t.Fatalf("StartBattle: %v", err)
	}
	repo.addDuel(d)

	broker := NewBroker()
	sweeper := NewRoundSweeper(repo, broker, 50*time.Millisecond)
	sweeper.RegisterTenant(tenantID)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	sweeper.Start(ctx)
	time.Sleep(300 * time.Millisecond)

	// The tenant should STILL be registered (not pruned).
	tenants := sweeper.activeTenants()
	found := false
	for _, tID := range tenants {
		if tID == tenantID {
			found = true
		}
	}
	if !found {
		t.Fatal("tenant was pruned after having no expired duels (must stay registered — rounds expire on a timer)")
	}
}
