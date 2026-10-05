// Package subscribers_test holds behavioural specs for the
// DuelCompletedSubscriber.
//
// The subscriber consumes chora.sharing.duel.completed.v1 and:
//
//   - credits Coins to both players (winner/loser/draw amounts from
//     duel.RewardConfig), and
//   - feeds the durable leaderboard (XP axis) for both players across
//     weekly + all-time periods under the tenant scope.
//
// Idempotency: (handler, event_id) is claimed BEFORE any side effect;
// replays are a no-op.
package subscribers_test

import (
	"context"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/adapter/subscribers"
	"github.com/apollo-chora/chora-sharing/internal/domain/currency"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
)

// newDuelSub builds a DuelCompletedSubscriber wired to a fresh in-memory
// CurrencyRepo, Ranker, and idempotency store with default rewards.
func newDuelSub(t *testing.T) (*subscribers.DuelCompletedSubscriber, *inmem.CurrencyRepo, *leaderboard.Ranker, *subscribers.InMemoryIdempotencyStore) {
	t.Helper()
	coins := inmem.NewCurrencyRepo()
	ranker := leaderboard.NewRanker()
	idem := subscribers.NewInMemoryIdempotencyStore()
	sub := subscribers.NewDuelCompletedSubscriber(subscribers.DuelCompletedConfig{
		Currency:    coins,
		Ranker:      ranker,
		Idempotency: idem,
		Rewards:     duel.DefaultRewardConfig(),
	})
	return sub, coins, ranker, idem
}

// winEnvelope builds a completed-duel envelope where challenger wins.
func winEnvelope() subscribers.DuelCompletedEnvelope {
	return subscribers.DuelCompletedEnvelope{
		EventID:         "evt-duel-win-1",
		TenantID:        "tenant-1",
		DuelID:          "duel-1",
		ChallengerGCID:  "gcid-challenger",
		OpponentGCID:    "gcid-opponent",
		WinnerGCID:      "gcid-challenger",
		Scope:           string(duel.ScopeRanked),
		ScoreChallenger: 10,
		ScoreOpponent:   4,
		OccurredAt:      time.Date(2026, 7, 12, 9, 0, 0, 0, time.UTC),
	}
}

func TestHandleDuelCompleted_CreditsCoins(t *testing.T) {
	sub, coins, _, _ := newDuelSub(t)
	env := winEnvelope()

	if err := sub.HandleDuelCompleted(context.Background(), env); err != nil {
		t.Fatalf("HandleDuelCompleted: %v", err)
	}

	ctx := context.Background()
	gotWinner, err := coins.GetBalance(ctx, env.TenantID, env.ChallengerGCID, string(currency.CurrencyCoins))
	if err != nil {
		t.Fatalf("GetBalance winner: %v", err)
	}
	if gotWinner != float64(duel.DefaultRewardConfig().WinnerCoins) {
		t.Fatalf("winner coins = %v, want %d", gotWinner, duel.DefaultRewardConfig().WinnerCoins)
	}

	gotLoser, err := coins.GetBalance(ctx, env.TenantID, env.OpponentGCID, string(currency.CurrencyCoins))
	if err != nil {
		t.Fatalf("GetBalance loser: %v", err)
	}
	if gotLoser != float64(duel.DefaultRewardConfig().LoserCoins) {
		t.Fatalf("loser coins = %v, want %d", gotLoser, duel.DefaultRewardConfig().LoserCoins)
	}
}

func TestHandleDuelCompleted_DrawCreditsBoth(t *testing.T) {
	sub, coins, _, _ := newDuelSub(t)

	env := winEnvelope()
	env.EventID = "evt-duel-draw-1"
	env.WinnerGCID = "" // draw
	env.ScoreChallenger = 7
	env.ScoreOpponent = 7

	if err := sub.HandleDuelCompleted(context.Background(), env); err != nil {
		t.Fatalf("HandleDuelCompleted draw: %v", err)
	}

	ctx := context.Background()
	wantDraw := float64(duel.DefaultRewardConfig().DrawCoins)

	gotChallenger, err := coins.GetBalance(ctx, env.TenantID, env.ChallengerGCID, string(currency.CurrencyCoins))
	if err != nil {
		t.Fatalf("GetBalance challenger: %v", err)
	}
	if gotChallenger != wantDraw {
		t.Fatalf("challenger draw coins = %v, want %v", gotChallenger, wantDraw)
	}

	gotOpponent, err := coins.GetBalance(ctx, env.TenantID, env.OpponentGCID, string(currency.CurrencyCoins))
	if err != nil {
		t.Fatalf("GetBalance opponent: %v", err)
	}
	if gotOpponent != wantDraw {
		t.Fatalf("opponent draw coins = %v, want %v", gotOpponent, wantDraw)
	}
}

func TestHandleDuelCompleted_Idempotent(t *testing.T) {
	sub, coins, _, idem := newDuelSub(t)
	env := winEnvelope()

	if err := sub.HandleDuelCompleted(context.Background(), env); err != nil {
		t.Fatalf("first HandleDuelCompleted: %v", err)
	}
	// Replay same event_id — must be a no-op (no double credit).
	if err := sub.HandleDuelCompleted(context.Background(), env); err != nil {
		t.Fatalf("replay HandleDuelCompleted: %v", err)
	}

	if !idem.Recorded(subscribers.HandlerDuelCompleted, env.EventID) {
		t.Fatalf("idempotency store did not record the event")
	}

	ctx := context.Background()
	got, err := coins.GetBalance(ctx, env.TenantID, env.ChallengerGCID, string(currency.CurrencyCoins))
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if got != float64(duel.DefaultRewardConfig().WinnerCoins) {
		t.Fatalf("after replay winner coins = %v, want %d (no double-credit)", got, duel.DefaultRewardConfig().WinnerCoins)
	}
}

func TestHandleDuelCompleted_FeedLeaderboard(t *testing.T) {
	sub, _, ranker, _ := newDuelSub(t)
	env := winEnvelope()

	if err := sub.HandleDuelCompleted(context.Background(), env); err != nil {
		t.Fatalf("HandleDuelCompleted: %v", err)
	}

	tenantScope := leaderboard.Scope{Kind: leaderboard.ScopeTenant}

	// Both players must appear on the weekly board.
	weekly := ranker.TopByPeriod(tenantScope, leaderboard.PeriodWeekly, env.TenantID, 10)
	if len(weekly) != 2 {
		t.Fatalf("weekly top = %+v, want 2 entries", weekly)
	}

	// Winner (challenger) gets 2x base XP (40); loser (opponent) gets 1x (20).
	const baseDuelXP = 20
	wantWinnerXP := baseDuelXP * 2
	wantLoserXP := baseDuelXP

	xpByGCID := map[string]int{}
	for _, e := range weekly {
		xpByGCID[e.GCID] = e.Score
	}
	if got := xpByGCID[env.ChallengerGCID]; got != wantWinnerXP {
		t.Fatalf("challenger weekly XP = %d, want %d", got, wantWinnerXP)
	}
	if got := xpByGCID[env.OpponentGCID]; got != wantLoserXP {
		t.Fatalf("opponent weekly XP = %d, want %d", got, wantLoserXP)
	}

	// Both players must also appear on the all-time board with the same XP.
	allTime := ranker.TopByPeriod(tenantScope, leaderboard.PeriodAllTime, env.TenantID, 10)
	if len(allTime) != 2 {
		t.Fatalf("all-time top = %+v, want 2 entries", allTime)
	}
	xpAllTime := map[string]int{}
	for _, e := range allTime {
		xpAllTime[e.GCID] = e.Score
	}
	if got := xpAllTime[env.ChallengerGCID]; got != wantWinnerXP {
		t.Fatalf("challenger all-time XP = %d, want %d", got, wantWinnerXP)
	}
	if got := xpAllTime[env.OpponentGCID]; got != wantLoserXP {
		t.Fatalf("opponent all-time XP = %d, want %d", got, wantLoserXP)
	}

	// Winner ranks above loser (higher XP).
	if rankWinner := ranker.RankOf(tenantScope, leaderboard.PeriodWeekly, env.TenantID, env.ChallengerGCID); rankWinner != 1 {
		t.Fatalf("winner weekly rank = %d, want 1", rankWinner)
	}
	if rankLoser := ranker.RankOf(tenantScope, leaderboard.PeriodWeekly, env.TenantID, env.OpponentGCID); rankLoser != 2 {
		t.Fatalf("loser weekly rank = %d, want 2", rankLoser)
	}
}
