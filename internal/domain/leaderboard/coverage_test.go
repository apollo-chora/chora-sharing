// Package leaderboard_test holds the remaining coverage specs for the
// LeaderboardReader port adapter (RankerReader).
package leaderboard_test

import (
	"context"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
)

// RankerReader wraps a Ranker behind the LeaderboardReader port: ReadTop maps
// the typed scope + period onto TopByPeriod.
func TestRankerReader_ReadTop_DelegatesToRanker(t *testing.T) {
	t.Parallel()
	r := leaderboard.NewRanker()
	r.Submit(leaderboard.Scope{Kind: leaderboard.ScopeGlobal}, leaderboard.PeriodWeekly, "g1", 40)
	r.Submit(leaderboard.Scope{Kind: leaderboard.ScopeGlobal}, leaderboard.PeriodWeekly, "g2", 10)

	rr := leaderboard.NewRankerReader(r)
	entries, err := rr.ReadTop(context.Background(), leaderboard.ScopeGlobal, "", "", leaderboard.PeriodWeekly, 10)
	if err != nil {
		t.Fatalf("ReadTop: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].GCID != "g1" || entries[0].Rank != 1 {
		t.Fatalf("expected g1 at rank 1, got %+v", entries[0])
	}
	if entries[1].GCID != "g2" || entries[1].Rank != 2 {
		t.Fatalf("expected g2 at rank 2, got %+v", entries[1])
	}
}

func TestRankerReader_ReadTop_EmptyBoard(t *testing.T) {
	t.Parallel()
	rr := leaderboard.NewRankerReader(leaderboard.NewRanker())
	entries, err := rr.ReadTop(context.Background(), leaderboard.ScopeGlobal, "", "", leaderboard.PeriodWeekly, 5)
	if err != nil {
		t.Fatalf("ReadTop: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected an empty board, got %d entries", len(entries))
	}
}