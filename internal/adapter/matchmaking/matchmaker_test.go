package matchmaking

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	domainmm "github.com/apollo-chora/chora-sharing/internal/domain/matchmaking"
)

const (
	testTenant    = "01970000-0000-7000-8000-0000000000a1"
	testGCID1     = "01970000-0000-7000-9000-0000000000a1"
	testGCID2     = "01970000-0000-7000-9000-0000000000a2"
	testGCID3     = "01970000-0000-7000-9000-0000000000a3"
	testGCID4     = "01970000-0000-7000-9000-0000000000a4"
	testTimeout   = 10 * time.Minute
	testMatchTick = 100 * time.Millisecond
	testSweepTick = 100 * time.Millisecond
)

func newTestConfig() Config {
	return Config{
		Timeout:           testTimeout,
		HeartbeatStale:    30 * time.Second,
		MatchTickInterval: testMatchTick,
		SweepTickInterval: testSweepTick,
	}
}

func newSearcher(gcid string, prof int, tags []string, now time.Time) domainmm.Searcher {
	return domainmm.Searcher{
		GCID:          gcid,
		TenantID:      testTenant,
		Proficiency:   prof,
		InterestTags:  tags,
		EnteredAt:     now,
		ExpiresAt:     now.Add(testTimeout),
		LastHeartbeat: now,
	}
}

// fakeRepo is a test double for QueueRepoPort. It mimics the DB semantics:
// ClaimMatch removes the pair from the finding pool (and fails if either
// row is not finding), RevertToFinding returns a claimed searcher to the
// pool.
type fakeRepo struct {
	mu             sync.Mutex
	candidates     map[string][]domainmm.Searcher // tenantID → finding searchers
	claimed        map[string]claimedRecord       // gcid → claimed searcher
	claims         []claimRecord                  // recorded claims
	reverts        []revertRecord                 // recorded reverts
	expiredTenants []string                       // recorded sweeps
	claimErr       error                          // if set, ClaimMatch returns this
}

type claimRecord struct {
	tenantID, gcidA, gcidB, duelID string
}

type claimedRecord struct {
	searcher domainmm.Searcher
	duelID   string
}

type revertRecord struct {
	tenantID, gcid, duelID string
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		candidates: make(map[string][]domainmm.Searcher),
		claimed:    make(map[string]claimedRecord),
	}
}

func (f *fakeRepo) add(s domainmm.Searcher) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.candidates[s.TenantID] = append(f.candidates[s.TenantID], s)
}

func (f *fakeRepo) FindCandidates(_ context.Context, tenantID string) ([]domainmm.Searcher, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	candidates := make([]domainmm.Searcher, len(f.candidates[tenantID]))
	copy(candidates, f.candidates[tenantID])
	return candidates, nil
}

func (f *fakeRepo) ClaimMatch(_ context.Context, tenantID, gcidA, gcidB, duelID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimErr != nil {
		return f.claimErr
	}
	a := f.takeLocked(tenantID, gcidA)
	b := f.takeLocked(tenantID, gcidB)
	if a == nil || b == nil {
		// Mirror the DB guard: either row not finding → claim lost.
		if a != nil {
			f.candidates[tenantID] = append(f.candidates[tenantID], *a)
		}
		if b != nil {
			f.candidates[tenantID] = append(f.candidates[tenantID], *b)
		}
		return errors.New("rows no longer finding")
	}
	f.claims = append(f.claims, claimRecord{tenantID, gcidA, gcidB, duelID})
	f.claimed[gcidA] = claimedRecord{searcher: *a, duelID: duelID}
	f.claimed[gcidB] = claimedRecord{searcher: *b, duelID: duelID}
	return nil
}

// takeLocked removes and returns the searcher for gcid, or nil if absent.
func (f *fakeRepo) takeLocked(tenantID, gcid string) *domainmm.Searcher {
	list := f.candidates[tenantID]
	for i, s := range list {
		if s.GCID == gcid {
			f.candidates[tenantID] = append(list[:i], list[i+1:]...)
			return &s
		}
	}
	return nil
}

func (f *fakeRepo) RevertToFinding(_ context.Context, tenantID, gcid, duelID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.claimed[gcid]
	if !ok {
		return errors.New("not claimed")
	}
	delete(f.claimed, gcid)
	f.candidates[tenantID] = append(f.candidates[tenantID], rec.searcher)
	f.reverts = append(f.reverts, revertRecord{tenantID, gcid, duelID})
	return nil
}

func (f *fakeRepo) ExpireStaleForTenant(_ context.Context, tenantID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.expiredTenants = append(f.expiredTenants, tenantID)
	return nil
}

func (f *fakeRepo) revertCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reverts)
}

func (f *fakeRepo) sweepCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.expiredTenants)
}

// waitFor polls cond until it holds or the deadline elapses.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestMatchmaker_EnqueueAndMatch(t *testing.T) {
	repo := newFakeRepo()
	mm := NewMatchmaker(newTestConfig(), repo)
	defer mm.Stop()
	mm.RegisterTenant(testTenant)

	now := time.Now().UTC()
	repo.add(newSearcher(testGCID1, 1200, []string{"inheritance"}, now))
	repo.add(newSearcher(testGCID2, 1200, []string{"inheritance"}, now))

	select {
	case result := <-mm.Match():
		if result.SearcherA.GCID != testGCID1 && result.SearcherB.GCID != testGCID1 {
			t.Errorf("searcher A=%s B=%s, expected one to be %s", result.SearcherA.GCID, result.SearcherB.GCID, testGCID1)
		}
		if len(result.SharedTags) != 1 || result.SharedTags[0] != "inheritance" {
			t.Errorf("shared tags=%v want [inheritance]", result.SharedTags)
		}
		if result.MatchedDuelID == "" {
			t.Error("MatchedDuelID should be set by claim")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for match")
	}
}

func TestMatchmaker_MatchedDuelIDIsUUIDv7(t *testing.T) {
	repo := newFakeRepo()
	mm := NewMatchmaker(newTestConfig(), repo)
	defer mm.Stop()
	mm.RegisterTenant(testTenant)

	now := time.Now().UTC()
	repo.add(newSearcher(testGCID1, 1200, []string{"inheritance"}, now))
	repo.add(newSearcher(testGCID2, 1200, []string{"inheritance"}, now))

	select {
	case result := <-mm.Match():
		// UUIDv7 canonical form: xxxxxxxx-xxxx-7xxx-yxxx-xxxxxxxxxxxx.
		id := result.MatchedDuelID
		if len(id) != 36 || id[14] != '7' {
			t.Errorf("MatchedDuelID=%q is not a UUIDv7 (version nibble at [14] must be '7')", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for match")
	}
}

func TestMatchmaker_NoMatchStaysInPool(t *testing.T) {
	repo := newFakeRepo()
	mm := NewMatchmaker(newTestConfig(), repo)
	defer mm.Stop()
	mm.RegisterTenant(testTenant)

	now := time.Now().UTC()
	repo.add(newSearcher(testGCID1, 1200, []string{"inheritance"}, now))

	select {
	case <-mm.Match():
		t.Fatal("should not match with only one searcher")
	case <-time.After(300 * time.Millisecond):
		// good — no match found yet
	}
}

func TestMatchmaker_BestMatchSelected(t *testing.T) {
	repo := newFakeRepo()
	mm := NewMatchmaker(newTestConfig(), repo)
	defer mm.Stop()
	mm.RegisterTenant(testTenant)

	now := time.Now().UTC()
	// searcher1 shares "inheritance" with searcher2 but is closer in ELO to searcher3.
	s1 := newSearcher(testGCID1, 1200, []string{"inheritance"}, now)
	s2 := newSearcher(testGCID2, 1200, []string{"inheritance", "recursion"}, now)
	s3 := newSearcher(testGCID3, 1300, []string{"inheritance"}, now)

	repo.add(s1)
	repo.add(s2)
	repo.add(s3)

	select {
	case result := <-mm.Match():
		// s1 should match s2 (shared tag, same ELO = score 100) rather
		// than s3 (shared tag, 100 ELO delta = score 0).
		gcids := map[string]bool{result.SearcherA.GCID: true, result.SearcherB.GCID: true}
		if !gcids[testGCID1] || !gcids[testGCID2] {
			t.Errorf("expected s1(%s) to match s2(%s), got A=%s B=%s",
				testGCID1, testGCID2, result.SearcherA.GCID, result.SearcherB.GCID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for match")
	}
}

func TestMatchmaker_CrossTenantNoMatch(t *testing.T) {
	repo := newFakeRepo()
	mm := NewMatchmaker(newTestConfig(), repo)
	defer mm.Stop()
	mm.RegisterTenant(testTenant)
	mm.RegisterTenant("other-tenant")

	now := time.Now().UTC()
	s1 := newSearcher(testGCID1, 1200, []string{"inheritance"}, now)
	s1.TenantID = "other-tenant"
	s2 := newSearcher(testGCID2, 1200, []string{"inheritance"}, now)
	repo.add(s1)
	repo.add(s2)

	select {
	case <-mm.Match():
		t.Fatal("should not match across tenants")
	case <-time.After(300 * time.Millisecond):
		// good — no cross-tenant match
	}
}

func TestMatchmaker_Stop(t *testing.T) {
	repo := newFakeRepo()
	mm := NewMatchmaker(newTestConfig(), repo)
	mm.Stop()

	// After Stop, RegisterTenant should not panic.
	mm.RegisterTenant(testTenant)
}

func TestMatchmaker_ClaimFailureRetriesNextPair(t *testing.T) {
	repo := newFakeRepo()
	repo.claimErr = errors.New("claim failed")
	mm := NewMatchmaker(newTestConfig(), repo)
	defer mm.Stop()
	mm.RegisterTenant(testTenant)

	now := time.Now().UTC()
	repo.add(newSearcher(testGCID1, 1200, []string{"inheritance"}, now))
	repo.add(newSearcher(testGCID2, 1200, []string{"inheritance"}, now))

	select {
	case <-mm.Match():
		t.Fatal("should not emit match when claim fails")
	case <-time.After(300 * time.Millisecond):
		// good — claim failed, no match emitted
	}
}

// TestMatchmaker_RevertsClaimWhenMatchChannelFull pins the compensation
// path: a claim that lands while the result channel is full MUST be
// reverted, or both users strand in 'matched' with no duel.
func TestMatchmaker_RevertsClaimWhenMatchChannelFull(t *testing.T) {
	repo := newFakeRepo()
	cfg := newTestConfig()
	cfg.MatchBuffer = 1 // first pair fills the channel; the rest must revert
	mm := NewMatchmaker(cfg, repo)
	defer mm.Stop()
	mm.RegisterTenant(testTenant)

	now := time.Now().UTC()
	repo.add(newSearcher(testGCID1, 1200, []string{"inheritance"}, now))
	repo.add(newSearcher(testGCID2, 1200, []string{"inheritance"}, now))
	repo.add(newSearcher(testGCID3, 1200, []string{"inheritance"}, now))
	repo.add(newSearcher(testGCID4, 1200, []string{"inheritance"}, now))

	// Never drain mm.Match() — the channel stays full, so every claim
	// beyond the first must be reverted (2 reverts per reverted pair).
	waitFor(t, "claim reverts", func() bool { return repo.revertCount() >= 2 })

	repo.mu.Lock()
	defer repo.mu.Unlock()
	for _, rev := range repo.reverts {
		if rev.tenantID != testTenant {
			t.Errorf("revert tenant=%s want %s", rev.tenantID, testTenant)
		}
		if rev.duelID == "" {
			t.Errorf("revert for %s has empty duelID", rev.gcid)
		}
	}
}

// TestMatchmaker_SweepsActiveTenants pins the sweeper phase: each active
// tenant gets a per-tenant ExpireStaleForTenant call (RLS forbids a
// cross-tenant sweep).
func TestMatchmaker_SweepsActiveTenants(t *testing.T) {
	repo := newFakeRepo()
	mm := NewMatchmaker(newTestConfig(), repo)
	defer mm.Stop()
	mm.RegisterTenant(testTenant)

	// One live candidate so the tenant is not pruned before sweeping.
	now := time.Now().UTC()
	repo.add(newSearcher(testGCID1, 1200, []string{"inheritance"}, now))

	waitFor(t, "sweep of tenant", func() bool { return repo.sweepCount() > 0 })

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.expiredTenants[0] != testTenant {
		t.Errorf("swept tenant=%s want %s", repo.expiredTenants[0], testTenant)
	}
}

// TestMatchmaker_PrunesDrainedTenant pins the hint-set hygiene: a tenant
// with zero candidates leaves the polling set.
func TestMatchmaker_PrunesDrainedTenant(t *testing.T) {
	repo := newFakeRepo()
	mm := NewMatchmaker(newTestConfig(), repo)
	defer mm.Stop()
	mm.RegisterTenant(testTenant)

	waitFor(t, "tenant prune", func() bool { return len(mm.activeTenants()) == 0 })
}

// --- WS1: per-category matchmaking (matchmaker pairs only same-category) ---

// newSearcherWithCategory is newSearcher but also sets Category (WS1).
func newSearcherWithCategory(gcid string, prof int, tags []string, now time.Time, category string) domainmm.Searcher {
	s := newSearcher(gcid, prof, tags, now)
	s.Category = category
	return s
}

// TestMatchmaker_PairsOnlySameCategory pins WS1: the matchmaker must NOT
// pair two searchers whose Category differs. Players pick a category when
// queueing (category-scoped matchmaking).
func TestMatchmaker_PairsOnlySameCategory(t *testing.T) {
	repo := newFakeRepo()
	mm := NewMatchmaker(newTestConfig(), repo)
	defer mm.Stop()
	mm.RegisterTenant(testTenant)

	now := time.Now().UTC()
	// s1 wants mathematics, s2 wants programming — must NOT match.
	s1 := newSearcherWithCategory(testGCID1, 1200, []string{"inheritance"}, now, "mathematics")
	s2 := newSearcherWithCategory(testGCID2, 1200, []string{"inheritance"}, now, "programming")
	repo.add(s1)
	repo.add(s2)

	select {
	case result := <-mm.Match():
		t.Fatalf("should NOT match differing categories, got A=%s B=%s",
			result.SearcherA.GCID, result.SearcherB.GCID)
	case <-time.After(300 * time.Millisecond):
		// good — no match with mismatched categories
	}
}

// TestMatchmaker_PairsSameCategory pins WS1: two searchers with the same
// Category DO match (the constraint is a filter, not a block).
func TestMatchmaker_PairsSameCategory(t *testing.T) {
	repo := newFakeRepo()
	mm := NewMatchmaker(newTestConfig(), repo)
	defer mm.Stop()
	mm.RegisterTenant(testTenant)

	now := time.Now().UTC()
	s1 := newSearcherWithCategory(testGCID1, 1200, []string{"calculus"}, now, "mathematics")
	s2 := newSearcherWithCategory(testGCID2, 1200, []string{"calculus"}, now, "mathematics")
	repo.add(s1)
	repo.add(s2)

	select {
	case result := <-mm.Match():
		gcids := map[string]bool{result.SearcherA.GCID: true, result.SearcherB.GCID: true}
		if !gcids[testGCID1] || !gcids[testGCID2] {
			t.Errorf("expected s1(%s) to match s2(%s), got A=%s B=%s",
				testGCID1, testGCID2, result.SearcherA.GCID, result.SearcherB.GCID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for match with same category")
	}
}

// --- WS4: configurable question count (matchmaker pairs only matching counts) ---

// newSearcherWithCount is newSearcher but also sets QuestionCount.
func newSearcherWithCount(gcid string, prof int, tags []string, now time.Time, qCount int) domainmm.Searcher {
	s := newSearcher(gcid, prof, tags, now)
	s.QuestionCount = qCount
	return s
}

// TestMatchmaker_PairsOnlyMatchingQuestionCount pins WS4: the matchmaker
// must NOT pair two searchers whose QuestionCount differs. Both players
// must agree on the preset (Quick 5 / Standard 10 / Marathon 15).
func TestMatchmaker_PairsOnlyMatchingQuestionCount(t *testing.T) {
	repo := newFakeRepo()
	mm := NewMatchmaker(newTestConfig(), repo)
	defer mm.Stop()
	mm.RegisterTenant(testTenant)

	now := time.Now().UTC()
	// s1 wants 5 questions, s2 wants 10 — must NOT match despite shared tags.
	s1 := newSearcherWithCount(testGCID1, 1200, []string{"inheritance"}, now, 5)
	s2 := newSearcherWithCount(testGCID2, 1200, []string{"inheritance"}, now, 10)
	repo.add(s1)
	repo.add(s2)

	select {
	case result := <-mm.Match():
		t.Fatalf("should NOT match differing question counts, got A=%s B=%s",
			result.SearcherA.GCID, result.SearcherB.GCID)
	case <-time.After(300 * time.Millisecond):
		// good — no match with mismatched counts
	}
}

// TestMatchmaker_PairsMatchingQuestionCount pins WS4: two searchers with
// the same QuestionCount DO match (the constraint is a filter, not a block).
func TestMatchmaker_PairsMatchingQuestionCount(t *testing.T) {
	repo := newFakeRepo()
	mm := NewMatchmaker(newTestConfig(), repo)
	defer mm.Stop()
	mm.RegisterTenant(testTenant)

	now := time.Now().UTC()
	s1 := newSearcherWithCount(testGCID1, 1200, []string{"inheritance"}, now, 15)
	s2 := newSearcherWithCount(testGCID2, 1200, []string{"inheritance"}, now, 15)
	repo.add(s1)
	repo.add(s2)

	select {
	case result := <-mm.Match():
		gcids := map[string]bool{result.SearcherA.GCID: true, result.SearcherB.GCID: true}
		if !gcids[testGCID1] || !gcids[testGCID2] {
			t.Errorf("expected s1(%s) to match s2(%s), got A=%s B=%s",
				testGCID1, testGCID2, result.SearcherA.GCID, result.SearcherB.GCID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for match with matching question counts")
	}
}
