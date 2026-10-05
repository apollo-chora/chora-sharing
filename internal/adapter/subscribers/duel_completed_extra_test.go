// duel_completed_extra_test.go — branches of the DuelCompletedSubscriber not
// covered by the core specs: the opponent-wins / forfeit reward+XP paths, the
// nil-dependency preconditions, SubscribedTopics, and DecodeDuelCompleted
// (JSON happy path, attrs merge, RFC3339 occurred_at parsing, malformed input).
package subscribers_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/adapter/subscribers"
	"github.com/apollo-chora/chora-sharing/internal/domain/currency"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
)

func TestHandleDuelCompleted_OpponentWins(t *testing.T) {
	sub, coins, ranker, _ := newDuelSub(t)

	env := winEnvelope()
	env.EventID = "evt-duel-opponent-win-1"
	env.WinnerGCID = env.OpponentGCID // the opponent won

	if err := sub.HandleDuelCompleted(context.Background(), env); err != nil {
		t.Fatalf("HandleDuelCompleted: %v", err)
	}

	ctx := context.Background()
	wantWinner := float64(duel.DefaultRewardConfig().WinnerCoins)
	wantLoser := float64(duel.DefaultRewardConfig().LoserCoins)

	gotOpponent, err := coins.GetBalance(ctx, env.TenantID, env.OpponentGCID, string(currency.CurrencyCoins))
	if err != nil {
		t.Fatalf("GetBalance opponent: %v", err)
	}
	if gotOpponent != wantWinner {
		t.Fatalf("opponent (winner) coins = %v, want %v", gotOpponent, wantWinner)
	}
	gotChallenger, err := coins.GetBalance(ctx, env.TenantID, env.ChallengerGCID, string(currency.CurrencyCoins))
	if err != nil {
		t.Fatalf("GetBalance challenger: %v", err)
	}
	if gotChallenger != wantLoser {
		t.Fatalf("challenger (loser) coins = %v, want %v", gotChallenger, wantLoser)
	}

	// XP: winner (opponent) gets 2x base; loser (challenger) gets 1x.
	const baseDuelXP = 20
	tenantScope := leaderboard.Scope{Kind: leaderboard.ScopeTenant}
	weekly := ranker.TopByPeriod(tenantScope, leaderboard.PeriodWeekly, env.TenantID, 10)
	if len(weekly) != 2 {
		t.Fatalf("weekly top = %+v, want 2 entries", weekly)
	}
	xpByGCID := map[string]int{}
	for _, e := range weekly {
		xpByGCID[e.GCID] = e.Score
	}
	if xpByGCID[env.OpponentGCID] != baseDuelXP*2 {
		t.Fatalf("opponent weekly XP = %d, want %d", xpByGCID[env.OpponentGCID], baseDuelXP*2)
	}
	if xpByGCID[env.ChallengerGCID] != baseDuelXP {
		t.Fatalf("challenger weekly XP = %d, want %d", xpByGCID[env.ChallengerGCID], baseDuelXP)
	}
}

func TestHandleDuelCompleted_ZeroRewardSkipsCreditCall(t *testing.T) {
	// The subscriber maps every non-draw to OutcomeWin (the envelope has no
	// forfeit flag), so a "forfeit" is a plain loss: the losing side earns
	// LoserCoins. When the configured loser reward is 0, the >0 guard must
	// skip the CreditAuthor call for that side entirely.
	cases := []struct {
		name       string
		winner     func(env subscribers.DuelCompletedEnvelope) string // returns the winner gcid
		zeroSide   string                                             // the gcid that must receive nothing
		creditSide string                                             // the gcid that must receive WinnerCoins
	}{
		{"forfeited challenger earns nothing", func(e subscribers.DuelCompletedEnvelope) string { return e.OpponentGCID }, "gcid-challenger", "gcid-opponent"},
		{"losing opponent earns nothing", func(e subscribers.DuelCompletedEnvelope) string { return e.ChallengerGCID }, "gcid-opponent", "gcid-challenger"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			coins := inmem.NewCurrencyRepo()
			sub := subscribers.NewDuelCompletedSubscriber(subscribers.DuelCompletedConfig{
				Currency:    coins,
				Ranker:      leaderboard.NewRanker(),
				Idempotency: subscribers.NewInMemoryIdempotencyStore(),
				Rewards:     duel.RewardConfig{WinnerCoins: 50, LoserCoins: 0},
			})
			env := winEnvelope()
			env.WinnerGCID = c.winner(env)

			if err := sub.HandleDuelCompleted(context.Background(), env); err != nil {
				t.Fatalf("HandleDuelCompleted: %v", err)
			}

			ctx := context.Background()
			gotCredit, err := coins.GetBalance(ctx, env.TenantID, c.creditSide, string(currency.CurrencyCoins))
			if err != nil {
				t.Fatalf("GetBalance: %v", err)
			}
			if gotCredit != 50 {
				t.Fatalf("%s coins = %v, want 50", c.creditSide, gotCredit)
			}
			gotZero, err := coins.GetBalance(ctx, env.TenantID, c.zeroSide, string(currency.CurrencyCoins))
			if err != nil {
				t.Fatalf("GetBalance: %v", err)
			}
			if gotZero != 0 {
				t.Fatalf("%s coins = %v, want 0 (zero-reward side must skip the credit)", c.zeroSide, gotZero)
			}
		})
	}
}

func TestHandleDuelCompleted_MissingDeps_FailsLoud(t *testing.T) {
	idem := subscribers.NewInMemoryIdempotencyStore()
	ranker := leaderboard.NewRanker()

	noCurrency := subscribers.NewDuelCompletedSubscriber(subscribers.DuelCompletedConfig{
		Currency:    nil,
		Ranker:      ranker,
		Idempotency: idem,
	})
	if err := noCurrency.HandleDuelCompleted(context.Background(), winEnvelope()); err == nil {
		t.Fatalf("missing currency crediter must fail loud")
	}

	noRanker := subscribers.NewDuelCompletedSubscriber(subscribers.DuelCompletedConfig{
		Currency:    inmem.NewCurrencyRepo(),
		Ranker:      nil,
		Idempotency: idem,
	})
	if err := noRanker.HandleDuelCompleted(context.Background(), winEnvelope()); err == nil {
		t.Fatalf("missing ranker must fail loud")
	}

	noIdem := subscribers.NewDuelCompletedSubscriber(subscribers.DuelCompletedConfig{
		Currency:    inmem.NewCurrencyRepo(),
		Ranker:      ranker,
		Idempotency: nil,
	})
	if err := noIdem.HandleDuelCompleted(context.Background(), winEnvelope()); err == nil {
		t.Fatalf("missing idempotency store must fail loud")
	}
}

// errCurrency is a CurrencyCrediter that fails on demand (failOn = Nth call).
type errCurrency struct {
	mu     int
	failOn int
}

func (e *errCurrency) CreditAuthor(_ context.Context, _, _ string, _ currency.Currency, _ float64) error {
	e.mu++
	if e.failOn > 0 && e.mu >= e.failOn {
		return errors.New("ledger down")
	}
	return nil
}

func TestHandleDuelCompleted_ChallengerCreditError_NACKs(t *testing.T) {
	sub := subscribers.NewDuelCompletedSubscriber(subscribers.DuelCompletedConfig{
		Currency:    &errCurrency{failOn: 1},
		Ranker:      leaderboard.NewRanker(),
		Idempotency: subscribers.NewInMemoryIdempotencyStore(),
	})
	env := winEnvelope() // winner == challenger → the challenger is credited first

	if err := sub.HandleDuelCompleted(context.Background(), env); err == nil {
		t.Fatalf("challenger coin credit failure must NACK")
	}
}

func TestHandleDuelCompleted_OpponentCreditError_NACKs(t *testing.T) {
	sub := subscribers.NewDuelCompletedSubscriber(subscribers.DuelCompletedConfig{
		Currency:    &errCurrency{failOn: 2},
		Ranker:      leaderboard.NewRanker(),
		Idempotency: subscribers.NewInMemoryIdempotencyStore(),
	})
	env := winEnvelope() // challenger credit lands first; the opponent credit fails

	if err := sub.HandleDuelCompleted(context.Background(), env); err == nil {
		t.Fatalf("opponent coin credit failure must NACK")
	}
}

func TestDuelCompleted_SubscribedTopics(t *testing.T) {
	sub, _, _, _ := newDuelSub(t)
	topics := sub.SubscribedTopics()
	if len(topics) != 1 || topics[0] != subscribers.TopicDuelCompleted {
		t.Fatalf("SubscribedTopics = %v, want [%s]", topics, subscribers.TopicDuelCompleted)
	}
}

func TestDecodeDuelCompleted_JSON(t *testing.T) {
	body := []byte(`{
		"duel_id": "duel-7",
		"challenger_gcid": "gcid-challenger",
		"opponent_gcid": "gcid-opponent",
		"winner_gcid": "gcid-challenger",
		"scope": "ranked",
		"score_challenger": 10,
		"score_opponent": 4,
		"occurred_at": "2026-07-12T09:00:00Z"
	}`)
	attrs := map[string]string{
		"event_id":   "evt-duel-json-1",
		"tenant_id":  "tenant-1",
		"occurred_at": "2026-07-12T09:00:00Z",
	}
	env, err := subscribers.DecodeDuelCompleted(body, attrs)
	if err != nil {
		t.Fatalf("DecodeDuelCompleted: %v", err)
	}
	if env.EventID != "evt-duel-json-1" || env.TenantID != "tenant-1" {
		t.Fatalf("envelope fields not merged from attrs: %+v", env)
	}
	if env.DuelID != "duel-7" || env.Scope != "ranked" || env.ScoreChallenger != 10 || env.ScoreOpponent != 4 {
		t.Fatalf("payload fields not decoded: %+v", env)
	}
	want, _ := time.Parse(time.RFC3339Nano, "2026-07-12T09:00:00Z")
	if !env.OccurredAt.Equal(want) {
		t.Fatalf("occurred_at = %v, want %v", env.OccurredAt, want)
	}
}

func TestDecodeDuelCompleted_UnparsableOccurredAtFallsBackToNow(t *testing.T) {
	before := time.Now().UTC()
	body := []byte(`{"duel_id":"duel-8","occurred_at":"not-a-timestamp","winner_gcid":"g"}`)
	env, err := subscribers.DecodeDuelCompleted(body, nil)
	if err != nil {
		t.Fatalf("DecodeDuelCompleted: %v", err)
	}
	if env.OccurredAt.Before(before.Add(-time.Second)) || env.OccurredAt.After(time.Now().UTC().Add(time.Second)) {
		t.Fatalf("occurred_at = %v, want ≈ now (fallback on unparsable wire value)", env.OccurredAt)
	}
}

func TestDecodeDuelCompleted_Error(t *testing.T) {
	if _, err := subscribers.DecodeDuelCompleted([]byte("{not json"), nil); err == nil {
		t.Fatalf("malformed bytes must error")
	}
}