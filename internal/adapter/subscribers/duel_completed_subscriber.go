// duel_completed_subscriber.go
//
// DuelCompletedSubscriber consumes the Sharing event
//
//	chora.sharing.duel.completed.v1
//
// published when a duel reaches a terminal state (completed or forfeited).
// It credits participation + performance rewards (Coins) to both players
// via the CurrencyRepo, and feeds the durable Leaderboard (XP axis) with
// duel XP for both players.
//
// Reward structure (per duel.RewardConfig):
//   - Winner gets WinnerCoins (default 50)
//   - Loser gets LoserCoins (default 15) — participation reward
//   - Draw gives DrawCoins to both (default 25)
//   - Forfeit: winner gets WinnerCoins; forfeiting player gets 0
//
// Leaderboard XP credits both players for participation (the duel itself
// is the engagement signal). The winner gets bonus XP.
//
// Idempotency (CHO-2263 seen→process→mark): (handler, event_id) is peeked,
// the reward + XP work runs, then the pair is marked — a post-peek failure
// re-runs on redelivery instead of being ACK-dropped. Ack-after-processing.
// DLQ on persistent failure.
package subscribers

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protodecode"
	"github.com/apollo-chora/chora-sharing/internal/domain/currency"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
)

// TopicDuelCompleted is the Sharing event this subscriber consumes.
const TopicDuelCompleted = "chora.sharing.duel.completed.v1"

// HandlerDuelCompleted is the idempotency-key prefix.
const HandlerDuelCompleted = "duel:completed"

// DuelCompletedEnvelope mirrors the duel.completed.v1 payload plus the
// canonical envelope fields the publisher places on Pub/Sub attributes.
type DuelCompletedEnvelope struct {
	// Envelope fields (carried on Pub/Sub attributes).
	EventID   string `json:"event_id"`
	TenantID  string `json:"tenant_id"`
	OccurredAt time.Time `json:"occurred_at"`

	// Payload fields.
	DuelID          string `json:"duel_id"`
	ChallengerGCID  string `json:"challenger_gcid"`
	OpponentGCID    string `json:"opponent_gcid"`
	WinnerGCID      string `json:"winner_gcid"`
	Scope           string `json:"scope"`
	ScoreChallenger int    `json:"score_challenger"`
	ScoreOpponent   int    `json:"score_opponent"`
}

// CurrencyCrediter is the port for crediting Coins to players.
// Implemented by currency.CurrencyRepo (pg + inmem).
type CurrencyCrediter interface {
	CreditAuthor(ctx context.Context, tenantID, holderGCID string, c currency.Currency, amount float64) error
}

// DuelCompletedConfig bundles the subscriber's dependencies.
type DuelCompletedConfig struct {
	Currency    CurrencyCrediter
	Ranker      *leaderboard.Ranker
	Idempotency IdempotencyStore
	Rewards     duel.RewardConfig
	Logger      Logger
}

// DuelCompletedSubscriber credits participation rewards + leaderboard XP
// on duel completion.
type DuelCompletedSubscriber struct {
	cfg DuelCompletedConfig
}

// NewDuelCompletedSubscriber builds a subscriber with sane defaults.
func NewDuelCompletedSubscriber(cfg DuelCompletedConfig) *DuelCompletedSubscriber {
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	if cfg.Rewards.WinnerCoins == 0 && cfg.Rewards.LoserCoins == 0 && cfg.Rewards.DrawCoins == 0 {
		cfg.Rewards = duel.DefaultRewardConfig()
	}
	return &DuelCompletedSubscriber{cfg: cfg}
}

// SubscribedTopics returns the single topic this subscriber listens to.
func (s *DuelCompletedSubscriber) SubscribedTopics() []string {
	return []string{TopicDuelCompleted}
}

// HandleDuelCompleted processes one duel.completed.v1 event: it idempotently
// credits Coins to both players + feeds the leaderboard.
func (s *DuelCompletedSubscriber) HandleDuelCompleted(ctx context.Context, env DuelCompletedEnvelope) error {
	// The outbox dispatcher delivers on a background goroutine with no
	// tenant in ctx. RLS-gated repos (CurrencyRepo.CreditAuthor) read the
	// tenant from tracing.TenantIDFromContext(ctx) via rls.ApplySession —
	// without it the INSERT fails + the bus retries, but Claim already
	// marked the event idempotent, so retries skip as "duplicate" + the
	// leaderboard XP never credits. Inject the envelope's tenant here.
	ctx = tracing.WithTenantID(ctx, env.TenantID)
	if s.cfg.Currency == nil {
		return errors.New("subscribers: currency crediter not configured")
	}
	if s.cfg.Ranker == nil {
		return errors.New("subscribers: leaderboard ranker not configured")
	}
	if s.cfg.Idempotency == nil {
		return errors.New("subscribers: idempotency store not configured")
	}

	// Peek → work → mark (CHO-2263), keyed on event_id: mark AFTER the Coin
	// credits + leaderboard XP land, so a transient failure NACKs unmarked and
	// the redelivery re-runs the idempotent reward work (CreditAuthor +
	// SubmitTenant are idempotent on (tenant,gcid,duel) in practice) rather
	// than ACK-dropping.
	return processOnce(ctx, s.cfg.Idempotency, HandlerDuelCompleted, env.EventID,
		func() {
			s.cfg.Logger.Printf("duplicate_event_skipped handler=%s event_id=%s", HandlerDuelCompleted, env.EventID)
		},
		func() error {
			result := duel.DuelResult{
				DuelID:          env.DuelID,
				TenantID:        env.TenantID,
				Scope:           duel.Scope(env.Scope),
				ChallengerGCID:  env.ChallengerGCID,
				OpponentGCID:    env.OpponentGCID,
				WinnerGCID:      env.WinnerGCID,
				ScoreChallenger: env.ScoreChallenger,
				ScoreOpponent:   env.ScoreOpponent,
			}
			if env.WinnerGCID == "" {
				result.Outcome = duel.OutcomeDraw
			} else {
				result.Outcome = duel.OutcomeWin
			}

			// Credit Coins to both players.
			challengerCoins := result.RewardFor(env.ChallengerGCID, s.cfg.Rewards)
			opponentCoins := result.RewardFor(env.OpponentGCID, s.cfg.Rewards)

			if challengerCoins > 0 {
				if err := s.cfg.Currency.CreditAuthor(ctx, env.TenantID, env.ChallengerGCID, currency.CurrencyCoins, float64(challengerCoins)); err != nil {
					return fmt.Errorf("subscribers: credit challenger coins: %w", err)
				}
			}
			if opponentCoins > 0 {
				if err := s.cfg.Currency.CreditAuthor(ctx, env.TenantID, env.OpponentGCID, currency.CurrencyCoins, float64(opponentCoins)); err != nil {
					return fmt.Errorf("subscribers: credit opponent coins: %w", err)
				}
			}

			// Feed the leaderboard: XP for participation + performance.
			// Winner gets 2x the base XP; loser gets 1x; draw gives 1.5x to both.
			const baseDuelXP = 20
			challengerXP := baseDuelXP
			opponentXP := baseDuelXP
			if result.Outcome == duel.OutcomeWin {
				if env.WinnerGCID == env.ChallengerGCID {
					challengerXP = baseDuelXP * 2
				} else {
					opponentXP = baseDuelXP * 2
				}
			} else if result.Outcome == duel.OutcomeDraw {
				challengerXP = baseDuelXP + baseDuelXP/2
				opponentXP = baseDuelXP + baseDuelXP/2
			}

			tenantScope := leaderboard.Scope{Kind: leaderboard.ScopeTenant}
			s.cfg.Ranker.SubmitTenant(tenantScope, leaderboard.PeriodWeekly, env.TenantID, env.ChallengerGCID, challengerXP)
			s.cfg.Ranker.SubmitTenant(tenantScope, leaderboard.PeriodAllTime, env.TenantID, env.ChallengerGCID, challengerXP)
			s.cfg.Ranker.SubmitTenant(tenantScope, leaderboard.PeriodWeekly, env.TenantID, env.OpponentGCID, opponentXP)
			s.cfg.Ranker.SubmitTenant(tenantScope, leaderboard.PeriodAllTime, env.TenantID, env.OpponentGCID, opponentXP)

			s.cfg.Logger.Printf("duel_completed duel=%s winner=%s challenger_coins=%d opponent_coins=%d challenger_xp=%d opponent_xp=%d event_id=%s",
				env.DuelID, env.WinnerGCID, challengerCoins, opponentCoins, challengerXP, opponentXP, env.EventID)
			return nil
		})
}

// -----------------------------------------------------------------------------
// Wire decoder
// -----------------------------------------------------------------------------

// DecodeDuelCompleted decodes the duel.completed.v1 payload from the bus.
// It routes through protodecode so a future binaryDecoders entry
// transparently takes over without changing this call-site.
func DecodeDuelCompleted(blob []byte, attrs map[string]string) (DuelCompletedEnvelope, error) {
	m, err := protodecode.DecodePayloadMapWithAttrs(TopicDuelCompleted, blob, attrs)
	if err != nil {
		return DuelCompletedEnvelope{}, fmt.Errorf("subscribers: decode duel_completed: %w", err)
	}
	occurredAt := time.Now().UTC()
	if s := asString(m["occurred_at"]); s != "" {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			occurredAt = t
		}
	}
	return DuelCompletedEnvelope{
		EventID:         asString(m["event_id"]),
		TenantID:        asString(m["tenant_id"]),
		OccurredAt:      occurredAt,
		DuelID:          asString(m["duel_id"]),
		ChallengerGCID:  asString(m["challenger_gcid"]),
		OpponentGCID:    asString(m["opponent_gcid"]),
		WinnerGCID:      asString(m["winner_gcid"]),
		Scope:           asString(m["scope"]),
		ScoreChallenger: asInt(m["score_challenger"]),
		ScoreOpponent:   asInt(m["score_opponent"]),
	}, nil
}
