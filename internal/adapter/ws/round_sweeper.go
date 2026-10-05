package ws

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
)

// RoundSweeperRepo is the port the round-timer sweep depends on. It lists
// in-progress duels whose current round deadline has passed, and persists
// timeout resolutions. WS3: the sweeper polls this port on a fixed tick
// and auto-resolves expired rounds so a duel advances instead of hanging
// when a player disconnects or goes idle.
//
// All methods are per-tenant and require the tenant in the context
// (tracing.WithTenantID) — rls.ApplySession fails loud without it,
// matching the matchmaker's QueueRepoPort contract.
type RoundSweeperRepo interface {
	// ListDuelsWithExpiredRounds returns in-progress duels for the given
	// tenant that have at least one unresolved round whose DeadlineAt < now.
	// The caller must hold no locks — each duel is fetched for update
	// before resolving.
	ListDuelsWithExpiredRounds(ctx context.Context, tenantID string, now time.Time) ([]*duel.Duel, error)
	// GetDuelForUpdate fetches a duel for an exclusive update (row lock).
	GetDuelForUpdate(ctx context.Context, duelID string) (*duel.Duel, error)
	// ResolveRound persists a round resolution (timeout or answer).
	ResolveRound(ctx context.Context, d *duel.Duel, roundNo int, gcid string, res duel.RoundResolution) error
}

// RoundStarter builds the round_start frame for a duel's current round.
// Implemented by *DuelSessionAdapter. The sweeper uses it to auto-advance
// both clients to the next question after a timeout.
type RoundStarter interface {
	GetCurrentRound(ctx context.Context, duelID string) (RoundStartFrame, bool)
}

// RoundSweeper is the server-side round-timer sweep. It polls for duels
// with expired rounds on a fixed tick, auto-resolves them as
// both-unanswered (no points, no winner), and broadcasts a round_timeout
// WS frame so both participants see the skip. WS3.
//
// Per-tenant pattern (mirrors the matchmaker): RLS forbids a cross-tenant
// discovery query, so the sweeper holds an in-memory tenant hint set
// populated via RegisterTenant when a duel starts. Each tick iterates the
// active tenants, derives a per-tenant context with tracing.WithTenantID,
// and runs ListDuelsWithExpiredRounds + resolution under that context so
// rls.ApplySession succeeds. A tenant with no expired duels is pruned.
type RoundSweeper struct {
	repo         RoundSweeperRepo
	broker       *Broker
	eloApplier   ELOApplier
	eloKFactor   int
	eventPub     DuelEventPublisher
	roundStarter RoundStarter
	roundTimerSec int
	tick         time.Duration
	logf         func(format string, args ...any)

	mu      sync.Mutex
	tenants map[string]struct{}
}

// RoundSweeperOption configures a RoundSweeper.
type RoundSweeperOption func(*RoundSweeper)

func WithRoundSweeperLogger(logf func(format string, args ...any)) RoundSweeperOption {
	return func(s *RoundSweeper) { s.logf = logf }
}

func WithRoundSweeperELO(applier ELOApplier, kFactor int) RoundSweeperOption {
	return func(s *RoundSweeper) { s.eloApplier = applier; s.eloKFactor = kFactor }
}

func WithRoundSweeperEventPublisher(pub DuelEventPublisher) RoundSweeperOption {
	return func(s *RoundSweeper) { s.eventPub = pub }
}

// WithRoundSweeperRoundStarter wires the round_start frame builder so the
// sweeper can auto-advance both clients to the next question after a
// timeout (mirrors the answer-resolution advance in the WS handler).
func WithRoundSweeperRoundStarter(rs RoundStarter) RoundSweeperOption {
	return func(s *RoundSweeper) { s.roundStarter = rs }
}

// WithRoundSweeperTimerSec wires the configured round timer so the
// sweeper can re-apply it to DB-fetched duels (the timer is not
// persisted; without this the next-round deadline stamp falls back to
// the 30s default).
func WithRoundSweeperTimerSec(sec int) RoundSweeperOption {
	return func(s *RoundSweeper) { s.roundTimerSec = sec }
}

// NewRoundSweeper constructs a sweeper with the given tick interval.
// The tick should be short enough to auto-resolve promptly (e.g. 1s)
// but long enough to avoid hammering the DB in production.
func NewRoundSweeper(repo RoundSweeperRepo, broker *Broker, tick time.Duration, opts ...RoundSweeperOption) *RoundSweeper {
	s := &RoundSweeper{
		repo:    repo,
		broker:  broker,
		tick:    tick,
		logf:    log.Printf,
		tenants: make(map[string]struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// RegisterTenant adds a tenant to the polling hint set. Idempotent.
// Called when a duel transitions to in_progress (StartBattle) so the
// sweeper knows to poll that tenant for expired rounds.
func (s *RoundSweeper) RegisterTenant(tenantID string) {
	if tenantID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tenants[tenantID] = struct{}{}
}

// activeTenants returns a snapshot of the tenant hint set.
func (s *RoundSweeper) activeTenants() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.tenants))
	for t := range s.tenants {
		out = append(out, t)
	}
	return out
}

// unregisterTenant drops a tenant from the polling hint set. Called when
// the tenant has no expired duels; the next StartBattle re-registers.
func (s *RoundSweeper) unregisterTenant(tenantID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tenants, tenantID)
}

// tenantCtx derives a per-tenant, per-tick context. rls.ApplySession
// refuses to run without a tenant in context, so every repo call below
// goes through this. Mirrors the matchmaker's tenantCtx.
func (s *RoundSweeper) tenantCtx(ctx context.Context, tenantID string) (context.Context, context.CancelFunc) {
	return context.WithTimeout(tracing.WithTenantID(ctx, tenantID), 5*time.Second)
}

// Start launches the sweep goroutine. It runs until ctx is cancelled.
func (s *RoundSweeper) Start(ctx context.Context) {
	if s.repo == nil || s.broker == nil || s.tick <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(s.tick)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.sweep(ctx)
			}
		}
	}()
}

func (s *RoundSweeper) sweep(ctx context.Context) {
	now := time.Now().UTC()
	for _, tenantID := range s.activeTenants() {
		sweepCtx, cancel := s.tenantCtx(ctx, tenantID)
		duels, err := s.repo.ListDuelsWithExpiredRounds(sweepCtx, tenantID, now)
		if err != nil {
			cancel()
			s.logf("round-sweeper: ListDuelsWithExpiredRounds for tenant %s: %v", tenantID, err)
			continue
		}
		// Resolve expired rounds for each duel found. Do NOT unregister the
		// tenant when no expired duels are found — unlike the matchmaker
		// (which drains), an in-progress duel's rounds expire on a timer,
		// so the tenant must stay registered until the duel completes.
		for _, d := range duels {
			s.resolveExpiredRounds(sweepCtx, d.ID, tenantID)
		}
		cancel()
	}
}

// resolveExpiredRounds resolves all expired unresolved rounds in a duel
// and broadcasts the result. Each resolution is fetched for update so the
// row lock prevents concurrent answers from racing the timeout.
func (s *RoundSweeper) resolveExpiredRounds(ctx context.Context, duelID, tenantID string) {
	d, err := s.repo.GetDuelForUpdate(ctx, duelID)
	if err != nil {
		// FAIL-LOUD: a GetDuelForUpdate failure was previously swallowed
		// silently (no log) — this masked the RLS no-op bug (#1). Log at
		// error level so the failure is visible.
		s.logf("round-sweeper: GetDuelForUpdate failed duel=%s tenant=%s: %v", duelID, tenantID, err)
		return
	}
	if d == nil || d.Status != duel.StatusInProgress {
		return
	}
	// Rehydrate the configured round timer (not persisted) so the
	// next-round deadline stamp uses it instead of the 30s fallback.
	d = d.WithRoundTimerSec(s.roundTimerSec)
	for i, r := range d.Rounds {
		if r.ResolvedAt != nil || r.DeadlineAt == nil {
			continue
		}
		if time.Now().UTC().Before(*r.DeadlineAt) {
			continue
		}
		res, err := d.ResolveRoundTimeout(i+1, time.Now().UTC())
		if err != nil {
			// ErrAlreadyResolved is expected if a player answered between
			// the list + the lock — log at debug, not error.
			s.logf("round-sweeper: ResolveRoundTimeout duel=%s round=%d: %v", duelID, i+1, err)
			continue
		}
		// gcid is empty for a timeout (both-unanswered) — the repo must
		// handle an empty gcid as "no player" for the timeout path.
		if err := s.repo.ResolveRound(ctx, d, i+1, "", res); err != nil {
			s.logf("round-sweeper: persist ResolveRound duel=%s round=%d: %v", duelID, i+1, err)
			continue
		}
		// Broadcast a round_timeout frame so both participants see the skip.
		timeoutFrame := RoundTimeoutFrame{
			RoundNo:         int32(i + 1),
			DuelStatus:      string(res.DuelStatus),
			ScoreChallenger: int32(d.ScoreChallenger),
			ScoreOpponent:   int32(d.ScoreOpponent),
			WinnerGCID:      res.WinnerGCID,
		}
		s.broker.Publish(Message{DuelID: duelID, Kind: KindRoundTimeout, Payload: mustMarshal(timeoutFrame)})

		// If the duel completed, broadcast duel_completed (both clients
		// leave the arena — mirrors the answer-completion path in the WS
		// handler), then apply ELO + publish the event.
		if res.DuelStatus == duel.StatusCompleted {
			completedFrame := DuelCompletedFrame{
				WinnerGCID:      res.WinnerGCID,
				ScoreChallenger: int32(d.ScoreChallenger),
				ScoreOpponent:   int32(d.ScoreOpponent),
			}
			s.broker.Publish(Message{DuelID: duelID, Kind: KindDuelCompleted, Payload: mustMarshal(completedFrame)})
			if s.eloApplier != nil && d.Scope == duel.ScopeRanked {
				kFactor := s.eloKFactor
				if kFactor <= 0 {
					kFactor = 32
				}
				if err := s.eloApplier.ApplyELO(ctx, d, kFactor); err != nil {
					s.logf("round-sweeper: ApplyELO duel=%s: %v", duelID, err)
				}
			}
			if s.eventPub != nil {
				_ = s.eventPub.PublishDuelCompleted(ctx,
					string(d.Scope), d.ID, d.TenantID,
					d.ChallengerGCID, d.OpponentGCID, d.WinnerGCID,
					d.ScoreChallenger, d.ScoreOpponent,
				)
			}
			continue
		}

		// Duel still in progress: push the NEXT round's round_start so
		// both clients auto-advance to the next question instead of
		// staying stuck on the timed-out one (mirrors the handler's
		// post-answer advance).
		if s.roundStarter != nil {
			if rf, ok := s.roundStarter.GetCurrentRound(ctx, duelID); ok && int(rf.RoundNo) > i+1 {
				s.broker.Publish(Message{DuelID: duelID, Kind: KindRoundStart, Payload: mustMarshal(rf)})
			}
		}
	}
}
