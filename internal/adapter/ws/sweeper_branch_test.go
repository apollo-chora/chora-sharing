package ws

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
)

// sweeperRepoStub is a fully scriptable RoundSweeperRepo. Function fields
// default to the safe behaviour (lookup by ID, record + persist resolutions)
// so tests only override what they need.
type sweeperRepoStub struct {
	mu    sync.Mutex
	byID  map[string]*duel.Duel
	listErr    error
	getErr     error
	resolveErr error
	listCalls  []string
	resolveCalls []string
}

func newSweeperRepoStub() *sweeperRepoStub {
	return &sweeperRepoStub{byID: make(map[string]*duel.Duel)}
}

func (r *sweeperRepoStub) ListDuelsWithExpiredRounds(_ context.Context, tenantID string, _ time.Time) ([]*duel.Duel, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listCalls = append(r.listCalls, tenantID)
	if r.listErr != nil {
		return nil, r.listErr
	}
	var out []*duel.Duel
	for _, d := range r.byID {
		if d.TenantID == tenantID && d.Status == duel.StatusInProgress {
			out = append(out, d)
		}
	}
	return out, nil
}

func (r *sweeperRepoStub) GetDuelForUpdate(_ context.Context, duelID string) (*duel.Duel, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.getErr != nil {
		return nil, r.getErr
	}
	return r.byID[duelID], nil // nil, nil when missing → exercises the d==nil branch
}

func (r *sweeperRepoStub) ResolveRound(_ context.Context, d *duel.Duel, roundNo int, _ string, _ duel.RoundResolution) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resolveCalls = append(r.resolveCalls, d.ID+":"+itoa(roundNo))
	if r.resolveErr != nil {
		return r.resolveErr
	}
	r.byID[d.ID] = d
	return nil
}

func (r *sweeperRepoStub) calls() ([]string, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.listCalls...), append([]string(nil), r.resolveCalls...)
}

// TestRoundSweeper_OptionSettersAndTenantManagement pins the option setters
// and the tenant hint-set management: WithRoundSweeperELO + WithRoundSweeper
// EventPublisher wire their deps, RegisterTenant is idempotent and ignores
// empty IDs, and unregisterTenant drops a tenant.
func TestRoundSweeper_OptionSettersAndTenantManagement(t *testing.T) {
	repo := newSweeperRepoStub()
	broker := NewBroker()
	elo := &fakeELO{}
	pub := &fakeEventPub{}

	s := NewRoundSweeper(repo, broker, time.Hour,
		WithRoundSweeperELO(elo, 16),
		WithRoundSweeperEventPublisher(pub),
	)
	if s.eloApplier != elo || s.eloKFactor != 16 {
		t.Error("WithRoundSweeperELO did not wire the applier + K-factor")
	}
	if s.eventPub != pub {
		t.Error("WithRoundSweeperEventPublisher did not wire the publisher")
	}

	s.RegisterTenant("")
	if n := len(s.activeTenants()); n != 0 {
		t.Fatalf("activeTenants=%d want 0 (empty tenant must be ignored)", n)
	}
	s.RegisterTenant("t1")
	s.RegisterTenant("t1") // idempotent
	if n := len(s.activeTenants()); n != 1 {
		t.Fatalf("activeTenants=%d want 1 (duplicate registration must be idempotent)", n)
	}
	s.unregisterTenant("t1")
	if n := len(s.activeTenants()); n != 0 {
		t.Fatalf("activeTenants=%d want 0 after unregisterTenant", n)
	}
}

// TestRoundSweeper_StartGuard pins the bootstrap guard: Start with a nil
// repo must return without launching a goroutine (no panic, no sweep).
func TestRoundSweeper_StartGuard(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	NewRoundSweeper(nil, NewBroker(), time.Millisecond).Start(ctx) // nil repo
	NewRoundSweeper(newSweeperRepoStub(), nil, time.Millisecond).Start(ctx) // nil broker
	NewRoundSweeper(newSweeperRepoStub(), NewBroker(), 0).Start(ctx)       // zero tick
}

// TestRoundSweeper_SweepListErrorIsLogged pins the fail-loud contract: a
// ListDuelsWithExpiredRounds error is logged and the tenant is skipped
// (the sweep continues with the remaining tenants).
func TestRoundSweeper_SweepListErrorIsLogged(t *testing.T) {
	repo := newSweeperRepoStub()
	repo.listErr = errors.New("rls: tenant_id missing on context")

	var logCount int32
	s := NewRoundSweeper(repo, NewBroker(), time.Hour,
		WithRoundSweeperLogger(func(string, ...any) { atomic.AddInt32(&logCount, 1) }),
	)
	s.RegisterTenant("t1")
	s.sweep(context.Background())

	if n := atomic.LoadInt32(&logCount); n != 1 {
		t.Errorf("log calls=%d want 1 (list failure must be logged)", n)
	}
	if _, resolveCalls := repo.calls(); len(resolveCalls) != 0 {
		t.Errorf("resolveCalls=%v want none (tenant skipped on list error)", resolveCalls)
	}
}

// TestRoundSweeper_ResolveSkipsMissingDuel pins the d==nil and
// not-in-progress branches: a missing duel (or one that is no longer
// in_progress) is skipped without a resolution.
func TestRoundSweeper_ResolveSkipsMissingDuel(t *testing.T) {
	repo := newSweeperRepoStub()
	broker := NewBroker()
	s := NewRoundSweeper(repo, broker, time.Hour)
	s.RegisterTenant("t1")

	// Missing duel → GetDuelForUpdate returns (nil, nil).
	s.resolveExpiredRounds(context.Background(), "missing", "t1")

	// Completed duel → the status guard bails.
	done := newExpiredDuel(t, "t1")
	done.Status = duel.StatusCompleted
	repo.byID[done.ID] = done
	s.resolveExpiredRounds(context.Background(), done.ID, "t1")

	if _, resolveCalls := repo.calls(); len(resolveCalls) != 0 {
		t.Errorf("resolveCalls=%v want none for missing/completed duels", resolveCalls)
	}
}

// TestRoundSweeper_ResolveSkipsAlreadyResolvedRound pins the per-round skip:
// a resolved round (ResolvedAt set) is never re-resolved by the sweeper.
func TestRoundSweeper_ResolveSkipsAlreadyResolvedRound(t *testing.T) {
	repo := newSweeperRepoStub()
	d := newExpiredDuel(t, "t1")
	now := time.Now().UTC()
	d.Rounds[0].ResolvedAt = &now // already resolved
	repo.byID[d.ID] = d

	s := NewRoundSweeper(repo, NewBroker(), time.Hour)
	s.resolveExpiredRounds(context.Background(), d.ID, "t1")

	if _, resolveCalls := repo.calls(); len(resolveCalls) != 0 {
		t.Errorf("resolveCalls=%v want none (resolved round must be skipped)", resolveCalls)
	}
}

// TestRoundSweeper_PersistTimeoutFailure pins the persist-error branch: when
// the repo rejects the timeout resolution, no frame is broadcast and the
// sweeper moves on.
func TestRoundSweeper_PersistTimeoutFailure(t *testing.T) {
	repo := newSweeperRepoStub()
	d := newExpiredDuel(t, "t1")
	repo.byID[d.ID] = d
	repo.resolveErr = errors.New("persist failed")

	broker := NewBroker()
	ch, unsub := broker.Subscribe(d.ID, "sub")
	defer unsub()

	s := NewRoundSweeper(repo, broker, time.Hour)
	s.resolveExpiredRounds(context.Background(), d.ID, "t1")

	select {
	case msg := <-ch:
		t.Fatalf("frame published despite persist error: kind=%s", msg.Kind)
	default:
	}
}

// TestRoundSweeper_CompletedTimeout_AppliesELOAndEvent pins the terminal
// path end-to-end: a ranked duel's final round expiring applies ELO exactly
// once (K-factor default 0 → 32), publishes the completion event, and
// broadcasts round_timeout + duel_completed frames.
func TestRoundSweeper_CompletedTimeout_AppliesELOAndEvent(t *testing.T) {
	const tenantID = "tenant-1"
	repo := newSweeperRepoStub()
	d := newExpiredDuel(t, tenantID) // 1-round ranked duel, deadline in the past
	repo.byID[d.ID] = d

	broker := NewBroker()
	ch, unsub := broker.Subscribe(d.ID, "sub")
	defer unsub()

	elo := &fakeELO{}
	pub := &fakeEventPub{}
	s := NewRoundSweeper(repo, broker, time.Hour,
		WithRoundSweeperELO(elo, 0), // 0 → must fall back to 32
		WithRoundSweeperEventPublisher(pub),
	)
	s.resolveExpiredRounds(context.Background(), d.ID, tenantID)

	kinds := map[string]bool{}
	for len(kinds) < 2 {
		select {
		case msg := <-ch:
			kinds[string(msg.Kind)] = true
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for frames; got %v", kinds)
		}
	}
	if !kinds[string(KindRoundTimeout)] {
		t.Error("round_timeout frame missing")
	}
	if !kinds[string(KindDuelCompleted)] {
		t.Error("duel_completed frame missing (final-round timeout must end the duel)")
	}

	if calls := elo.got(); len(calls) != 1 || calls[0] != d.ID+":k32" {
		t.Errorf("ApplyELO calls=%v want [%s:k32] (K-factor 0 falls back to 32)", calls, d.ID)
	}
	if n := pub.count(); n != 1 {
		t.Errorf("PublishDuelCompleted calls=%d want 1", n)
	}
}

// TestRoundSweeper_ELOFailureIsLogged pins the best-effort contract on the
// timeout path: an ELO failure logs and does not break the sweep.
func TestRoundSweeper_ELOFailureIsLogged(t *testing.T) {
	const tenantID = "tenant-1"
	repo := newSweeperRepoStub()
	d := newExpiredDuel(t, tenantID)
	repo.byID[d.ID] = d

	elo := &fakeELO{err: errors.New("rating boom")}
	var logCount int32
	s := NewRoundSweeper(repo, NewBroker(), time.Hour,
		WithRoundSweeperELO(elo, 32),
		WithRoundSweeperLogger(func(string, ...any) { atomic.AddInt32(&logCount, 1) }),
	)
	s.resolveExpiredRounds(context.Background(), d.ID, tenantID)

	if n := atomic.LoadInt32(&logCount); n == 0 {
		t.Error("ApplyELO failure was not logged")
	}
	if calls := elo.got(); len(calls) != 1 {
		t.Errorf("ApplyELO calls=%v want 1", calls)
	}
}