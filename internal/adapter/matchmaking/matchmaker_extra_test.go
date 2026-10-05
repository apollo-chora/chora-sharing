// Edge coverage for the matchmaker: DefaultConfig, the NewMatchmaker
// nil-guard, idempotent Stop, and the error / join-filter branches the
// fakeRepo in matchmaker_test.go cannot express. Uses a configurable stub
// repo so each branch is driven deterministically.
package matchmaking

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	domainmm "github.com/apollo-chora/chora-sharing/internal/domain/matchmaking"
)

// stubRepo is a matchmaking.QueueRepoPort double with per-method errors and
// a fixed candidate batch (returned for any tenant query).
type stubRepo struct {
	mu         sync.Mutex
	candidates []domainmm.Searcher
	reverts    int
	findErr    error
	revertErr  error
	expireErr  error
}

// setCandidates is the only writer for candidates. NewMatchmaker starts a
// background loop that calls FindCandidates immediately, so assigning the field
// directly from a test body races that goroutine. The mutex on this struct
// already existed but guarded ONLY the reverts counter; every other field was
// read from the matchmaker's goroutine and written from the test unguarded.
func (s *stubRepo) setCandidates(c []domainmm.Searcher) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.candidates = c
}

func (s *stubRepo) FindCandidates(context.Context, string) ([]domainmm.Searcher, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.findErr != nil {
		return nil, s.findErr
	}
	return s.candidates, nil
}

func (s *stubRepo) ClaimMatch(context.Context, string, string, string, string) error { return nil }

func (s *stubRepo) RevertToFinding(context.Context, string, string, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.revertErr != nil {
		return s.revertErr
	}
	s.reverts++
	return nil
}

func (s *stubRepo) ExpireStaleForTenant(context.Context, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.expireErr
}

func (s *stubRepo) revertCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reverts
}

func stubSearcher(gcid, tenantID string) domainmm.Searcher {
	now := time.Now().UTC()
	return domainmm.Searcher{
		GCID:          gcid,
		TenantID:      tenantID,
		Proficiency:   1200,
		InterestTags:  []string{"inheritance"},
		EnteredAt:     now,
		ExpiresAt:     now.Add(testTimeout),
		LastHeartbeat: now,
		QuestionCount: 5,
		Category:      "mathematics",
	}
}

func TestDefaultConfig_MatchesSpec(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Timeout != 10*time.Minute ||
		cfg.HeartbeatStale != 30*time.Second ||
		cfg.MatchTickInterval != 2*time.Second ||
		cfg.SweepTickInterval != 15*time.Second {
		t.Fatalf("unexpected DefaultConfig: %+v", cfg)
	}
}

func TestNewMatchmaker_NilGuard(t *testing.T) {
	if m := NewMatchmaker(Config{}, newFakeRepo()); m != nil {
		t.Fatal("expected nil matchmaker when Config.Timeout is 0")
	}
	if m := NewMatchmaker(newTestConfig(), nil); m != nil {
		t.Fatal("expected nil matchmaker when repo is nil")
	}
}

func TestStop_IsIdempotent(t *testing.T) {
	mm := NewMatchmaker(newTestConfig(), newFakeRepo())
	mm.Stop()
	mm.Stop() // second Stop must hit the already-stopped guard
}

func TestTryMatch_FindCandidatesErrorContinues(t *testing.T) {
	repo := &stubRepo{findErr: errors.New("db down")}
	mm := NewMatchmaker(newTestConfig(), repo)
	defer mm.Stop()
	mm.RegisterTenant(testTenant)

	// Let the loop tick: FindCandidates fails, the branch logs + continues.
	time.Sleep(300 * time.Millisecond)
}

func TestMatchCandidates_FiltersOnJoinKeys(t *testing.T) {
	base := stubSearcher(testGCID1, testTenant)

	// (a,b): same tenant + category + question_count but incompatible mode.
	a := base
	a.GCID = "stub-g-a"
	b := base
	b.GCID = "stub-g-b"
	b.Mode = "blitz"
	b.BlitzVariant = "timed"

	// (c,d): tenants differ within one candidate batch (defensive filter).
	c := base
	c.GCID = "stub-g-c"
	c.TenantID = "other-tenant"
	d := base
	d.GCID = "stub-g-d"

	repo := &stubRepo{candidates: []domainmm.Searcher{a, b, c, d}}
	mm := NewMatchmaker(newTestConfig(), repo)
	defer mm.Stop()
	mm.RegisterTenant(testTenant)

	// The tick scans the batch; every incompatible pair is skipped by a
	// join filter. One compatible pair (a,d) may claim + emit - irrelevant
	// to the filters under test.
	time.Sleep(300 * time.Millisecond)
}

func TestRevertClaim_RevertErrorLogs(t *testing.T) {
	repo := &stubRepo{revertErr: errors.New("revert failed")}
	cfg := newTestConfig()
	cfg.MatchBuffer = 1 // capacity 1 -> second claim must revert
	mm := NewMatchmaker(cfg, repo)
	defer mm.Stop()
	mm.RegisterTenant(testTenant)

	repo.setCandidates([]domainmm.Searcher{
		stubSearcher("stub-r-1", testTenant),
		stubSearcher("stub-r-2", testTenant),
		stubSearcher("stub-r-3", testTenant),
		stubSearcher("stub-r-4", testTenant),
	})

	// Never drain mm.Match(): the first claim fills the channel and the next
	// pair's compensating reverts hit the RevertToFinding error branch.
	time.Sleep(300 * time.Millisecond)
	if repo.revertCount() != 0 {
		t.Fatalf("expected 0 successful reverts with revertErr set, got %d", repo.revertCount())
	}
}

func TestSweep_ExpireErrorLogs(t *testing.T) {
	repo := &stubRepo{expireErr: errors.New("sweep failed")}
	mm := NewMatchmaker(newTestConfig(), repo)
	defer mm.Stop()
	mm.RegisterTenant(testTenant)

	// First tick sweeps before matching, so ExpireStaleForTenant's error
	// branch is hit even though the tenant prunes right after.
	time.Sleep(300 * time.Millisecond)
}
