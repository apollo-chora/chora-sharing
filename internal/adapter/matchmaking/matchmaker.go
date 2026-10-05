// Package matchmaking — PG-backed Matchmaker adapter.
//
// The Matchmaker holds a set of tenant IDs that have active finding entries
// (an in-memory hint for the polling loop — RLS forbids a cross-tenant
// tenant discovery query, so the set is populated via RegisterTenant on
// enqueue/heartbeat and pruned when a tenant's pool drains). One background
// loop runs per active tenant, in two ordered phases:
//
//   - sweep (every SweepTickInterval): abandon the tenant's stale/expired
//     finding rows via ExpireStaleForTenant. Runs BEFORE matching so stale
//     rows never linger and the drain check below is accurate.
//   - match (every MatchTickInterval): fetch candidates from the DB (plain
//     SELECT), score pairs with MatchScore, and claim matches atomically
//     via ClaimMatch's UPDATE ... WHERE status='finding' guard. Multi-pod
//     safety is optimistic: two pods may attempt the same pair, exactly one
//     claim succeeds; the loser gets ErrNoMatchFound and moves to the next
//     pair. A tenant with zero candidates is pruned from the hint set (the
//     next enqueue or heartbeat re-registers it).
//
// Every DB call runs with the tenant injected into the context
// (tracing.WithTenantID) — rls.ApplySession fails loud without it.
//
// The in-memory pool from the previous design has been retired. The DB is
// the sole source of truth for finding searchers.
package matchmaking

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
	domainmm "github.com/apollo-chora/chora-sharing/internal/domain/matchmaking"
)

// defaultMatchBuffer bounds the result channel when Config.MatchBuffer is 0.
const defaultMatchBuffer = 32

// QueueRepoPort is the minimal port the Matchmaker needs from the PG repo.
// All methods are per-tenant and require the tenant in the context.
type QueueRepoPort interface {
	// FindCandidates returns the tenant's valid finding searchers
	// (plain SELECT — no locks held).
	FindCandidates(ctx context.Context, tenantID string) ([]domainmm.Searcher, error)
	// ClaimMatch atomically transitions two finding rows to matched with
	// the given duel ID; returns ErrNoMatchFound when a row was lost to
	// another pod.
	ClaimMatch(ctx context.Context, tenantID, gcidA, gcidB, duelID string) error
	// RevertToFinding returns one claimed row to finding after the
	// downstream duel pipeline failed (compensating action).
	RevertToFinding(ctx context.Context, tenantID, gcid, duelID string) error
	// ExpireStaleForTenant abandons the tenant's stale/expired finding rows.
	ExpireStaleForTenant(ctx context.Context, tenantID string) error
}

// Config holds the tunable parameters for the matchmaker.
type Config struct {
	Timeout           time.Duration // 10 min — how long a searcher stays in the pool
	HeartbeatStale    time.Duration // 30s — stale threshold before a searcher is abandoned
	MatchTickInterval time.Duration // 2s — how often the matching loop runs
	SweepTickInterval time.Duration // 15s — how often the sweeper loop runs
	MatchBuffer       int           // result channel capacity (0 → defaultMatchBuffer)
}

// DefaultConfig returns the shipped defaults per the spec.
func DefaultConfig() Config {
	return Config{
		Timeout:           10 * time.Minute,
		HeartbeatStale:    30 * time.Second,
		MatchTickInterval: 2 * time.Second,
		SweepTickInterval: 15 * time.Second,
	}
}

// Matchmaker is the PG-backed matching loop. It tracks active tenants
// as a polling hint and delegates all state to the QueueRepo.
type Matchmaker struct {
	cfg       Config
	repo      QueueRepoPort
	mu        sync.Mutex
	tenants   map[string]struct{} // active tenant hint set
	matchCh   chan domainmm.MatchResult
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	stopped   bool
	lastSweep time.Time
}

// NewMatchmaker starts the background loop.
func NewMatchmaker(cfg Config, repo QueueRepoPort) *Matchmaker {
	if cfg.Timeout == 0 || repo == nil {
		return nil
	}
	if cfg.MatchBuffer <= 0 {
		cfg.MatchBuffer = defaultMatchBuffer
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Matchmaker{
		cfg:     cfg,
		repo:    repo,
		tenants: make(map[string]struct{}),
		matchCh: make(chan domainmm.MatchResult, cfg.MatchBuffer),
		cancel:  cancel,
	}
	m.wg.Add(1)
	go m.loop(ctx)
	return m
}

// RegisterTenant adds a tenant to the polling hint set. Idempotent.
// Called when Enqueue (or a heartbeat for an existing entry) arrives.
func (m *Matchmaker) RegisterTenant(tenantID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tenants[tenantID] = struct{}{}
}

// unregisterTenant drops a tenant from the polling hint set. Called when
// the tenant's pool has drained; the next enqueue/heartbeat re-registers.
func (m *Matchmaker) unregisterTenant(tenantID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tenants, tenantID)
}

// activeTenants returns a snapshot of the tenant hint set.
func (m *Matchmaker) activeTenants() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.tenants))
	for t := range m.tenants {
		out = append(out, t)
	}
	return out
}

// Match returns the channel that receives MatchResult when a pair is found.
// The MatchedDuelID field is populated by the DB claim.
func (m *Matchmaker) Match() <-chan domainmm.MatchResult {
	return m.matchCh
}

// Stop shuts down the background goroutines.
func (m *Matchmaker) Stop() {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return
	}
	m.stopped = true
	m.mu.Unlock()
	m.cancel()
	m.wg.Wait()
}

// loop is the single background goroutine. Every MatchTickInterval it
// sweeps (when due) then matches, per active tenant, in that order — so a
// tenant whose rows all went stale is swept BEFORE it can be pruned for
// having zero candidates.
func (m *Matchmaker) loop(ctx context.Context) {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.MatchTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.tick(ctx)
		}
	}
}

func (m *Matchmaker) tick(ctx context.Context) {
	m.mu.Lock()
	sweepDue := m.cfg.SweepTickInterval > 0 && time.Since(m.lastSweep) >= m.cfg.SweepTickInterval
	if sweepDue {
		m.lastSweep = time.Now()
	}
	m.mu.Unlock()

	if sweepDue {
		m.sweep(ctx)
	}
	m.tryMatch(ctx)
}

// tenantCtx derives a per-tenant, per-tick context. rls.ApplySession
// refuses to run without a tenant in context, so every repo call below
// goes through this.
func (m *Matchmaker) tenantCtx(ctx context.Context, tenantID string) (context.Context, context.CancelFunc) {
	return context.WithTimeout(tracing.WithTenantID(ctx, tenantID), 5*time.Second)
}

func (m *Matchmaker) tryMatch(ctx context.Context) {
	for _, tenantID := range m.activeTenants() {
		matchCtx, cancel := m.tenantCtx(ctx, tenantID)
		candidates, err := m.repo.FindCandidates(matchCtx, tenantID)
		if err != nil {
			cancel()
			log.Printf("matchmaker: FindCandidates for tenant %s: %v", tenantID, err)
			continue
		}
		if len(candidates) == 0 {
			cancel()
			// Pool drained — stop polling this tenant. The next enqueue or
			// heartbeat re-registers it.
			m.unregisterTenant(tenantID)
			continue
		}
		m.matchCandidates(matchCtx, tenantID, candidates)
		cancel()
	}
}

// matchCandidates greedily pairs candidates by descending MatchScore and
// claims each pair atomically. Pairs involving an already-claimed searcher
// are skipped.
func (m *Matchmaker) matchCandidates(ctx context.Context, tenantID string, candidates []domainmm.Searcher) {
	if len(candidates) < 2 {
		return
	}

	// Build all candidate pairs sorted by score descending.
	type candidate struct {
		i, j  int
		score int
	}
	var pairs []candidate
	for i := 0; i < len(candidates); i++ {
		for j := i + 1; j < len(candidates); j++ {
			if candidates[i].TenantID != candidates[j].TenantID {
				continue
			}
			// WS1: matchmaker pairs only same-category entries.
			if candidates[i].Category != candidates[j].Category {
				continue
			}
			// WS4: matchmaker pairs only matching question_count presets.
			// Both players must agree on the duel length (5/10/15).
			if candidates[i].QuestionCount != candidates[j].QuestionCount {
				continue
			}
			// Blitz: matchmaker pairs only same-mode + same-blitz-variant
			// entries. A classic player never faces a blitz player; a
			// timed-blitz player never faces a race-blitz player.
			if !domainmm.IsCompatible(candidates[i], candidates[j]) {
				continue
			}
			pairs = append(pairs, candidate{
				i:     i,
				j:     j,
				score: domainmm.MatchScore(candidates[i], candidates[j]),
			})
		}
	}
	sort.Slice(pairs, func(a, b int) bool {
		return pairs[a].score > pairs[b].score
	})

	matched := make(map[int]bool, len(candidates))
	for _, p := range pairs {
		if matched[p.i] || matched[p.j] {
			continue
		}
		duelID := duel.NewUUIDv7()
		if err := m.repo.ClaimMatch(ctx, tenantID, candidates[p.i].GCID, candidates[p.j].GCID, duelID); err != nil {
			// Lost the race (another pod claimed one row) or transient DB
			// error — skip to the next pair; the rows stay finding.
			log.Printf("matchmaker: claim failed for %s + %s: %v",
				candidates[p.i].GCID, candidates[p.j].GCID, err)
			continue
		}
		matched[p.i] = true
		matched[p.j] = true
		result := domainmm.MatchResult{
			SearcherA:     candidates[p.i],
			SearcherB:     candidates[p.j],
			SharedTags:    domainmm.SharedTags(candidates[p.i].InterestTags, candidates[p.j].InterestTags),
			MatchedAt:     time.Now().UTC(),
			MatchedDuelID: duelID,
		}
		select {
		case m.matchCh <- result:
		default:
			// The claim already landed — dropping the result would strand
			// both users in 'matched' with no duel. Compensate: revert
			// both rows to finding so a later tick re-pairs them.
			log.Printf("matchmaker: match channel full, reverting claim for %s + %s (duel %s)",
				result.SearcherA.GCID, result.SearcherB.GCID, duelID)
			m.revertClaim(ctx, tenantID, result.SearcherA.GCID, duelID)
			m.revertClaim(ctx, tenantID, result.SearcherB.GCID, duelID)
		}
	}
}

// revertClaim is the loud compensating action for a claim that cannot be
// fulfilled. Errors surface in logs — a failed revert leaves the user in
// 'matched' until the row is cancelled or expires, which must be visible.
func (m *Matchmaker) revertClaim(ctx context.Context, tenantID, gcid, duelID string) {
	if err := m.repo.RevertToFinding(ctx, tenantID, gcid, duelID); err != nil {
		log.Printf("matchmaker: revert claim for %s (duel %s) failed: %v — user may be stranded in matched",
			gcid, duelID, err)
	}
}

// sweep abandons stale/expired finding rows for each active tenant.
func (m *Matchmaker) sweep(ctx context.Context) {
	for _, tenantID := range m.activeTenants() {
		sweepCtx, cancel := m.tenantCtx(ctx, tenantID)
		if err := m.repo.ExpireStaleForTenant(sweepCtx, tenantID); err != nil {
			log.Printf("matchmaker: ExpireStaleForTenant for tenant %s: %v", tenantID, err)
		}
		cancel()
	}
}
