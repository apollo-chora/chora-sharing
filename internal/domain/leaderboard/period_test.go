// Package leaderboard_test holds RED-phase TDD specs for periodised
// leaderboards + per-user rank lookup (Phase 60.x extension).
package leaderboard_test

import (
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
)

func TestParsePeriod_Weekly(t *testing.T) {
	t.Parallel()
	p, err := leaderboard.ParsePeriod("weekly")
	if err != nil {
		t.Fatalf("ParsePeriod(weekly): %v", err)
	}
	if p != leaderboard.PeriodWeekly {
		t.Fatalf("expected PeriodWeekly, got %q", p)
	}
}

func TestParsePeriod_Monthly(t *testing.T) {
	t.Parallel()
	p, err := leaderboard.ParsePeriod("monthly")
	if err != nil {
		t.Fatalf("ParsePeriod(monthly): %v", err)
	}
	if p != leaderboard.PeriodMonthly {
		t.Fatalf("expected PeriodMonthly, got %q", p)
	}
}

func TestParsePeriod_AllTime(t *testing.T) {
	t.Parallel()
	p, err := leaderboard.ParsePeriod("all-time")
	if err != nil {
		t.Fatalf("ParsePeriod(all-time): %v", err)
	}
	if p != leaderboard.PeriodAllTime {
		t.Fatalf("expected PeriodAllTime, got %q", p)
	}
}

func TestParsePeriod_RejectsUnknown(t *testing.T) {
	t.Parallel()
	if _, err := leaderboard.ParsePeriod("yearly"); err == nil {
		t.Fatalf("expected error for unknown period")
	}
}

// Submit + TopByPeriod: deterministic tie-breaker (xp desc, gcid asc).
func TestRanker_Submit_TopByPeriodTieBreakerByGCID(t *testing.T) {
	t.Parallel()
	r := leaderboard.NewRanker()
	scope, _ := leaderboard.ParseScope("global")
	// Three with same XP; expect ordering by gcid ascending.
	r.Submit(scope, leaderboard.PeriodWeekly, "01970000-0000-7000-9000-000000000003", 100)
	r.Submit(scope, leaderboard.PeriodWeekly, "01970000-0000-7000-9000-000000000001", 100)
	r.Submit(scope, leaderboard.PeriodWeekly, "01970000-0000-7000-9000-000000000002", 100)

	out := r.TopByPeriod(scope, leaderboard.PeriodWeekly, "", 10)
	if len(out) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(out))
	}
	if out[0].GCID != "01970000-0000-7000-9000-000000000001" ||
		out[1].GCID != "01970000-0000-7000-9000-000000000002" ||
		out[2].GCID != "01970000-0000-7000-9000-000000000003" {
		t.Fatalf("tie-breaker by gcid asc failed: %#v", out)
	}
	for i, e := range out {
		if e.Rank != i+1 {
			t.Fatalf("rank not assigned monotonically: %#v", out)
		}
	}
}

func TestRanker_Submit_TopByPeriodScoreDescending(t *testing.T) {
	t.Parallel()
	r := leaderboard.NewRanker()
	scope, _ := leaderboard.ParseScope("global")
	r.Submit(scope, leaderboard.PeriodMonthly, "g-low", 50)
	r.Submit(scope, leaderboard.PeriodMonthly, "g-mid", 100)
	r.Submit(scope, leaderboard.PeriodMonthly, "g-hi", 200)
	out := r.TopByPeriod(scope, leaderboard.PeriodMonthly, "", 10)
	if out[0].Score != 200 || out[1].Score != 100 || out[2].Score != 50 {
		t.Fatalf("expected score desc: got %#v", out)
	}
}

// Submit accumulates XP across multiple submissions for the same gcid.
func TestRanker_Submit_AccumulatesXP(t *testing.T) {
	t.Parallel()
	r := leaderboard.NewRanker()
	scope, _ := leaderboard.ParseScope("tenant")
	r.Submit(scope, leaderboard.PeriodAllTime, "alpha", 30)
	r.Submit(scope, leaderboard.PeriodAllTime, "alpha", 70)
	out := r.TopByPeriod(scope, leaderboard.PeriodAllTime, "", 10)
	if len(out) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(out))
	}
	if out[0].Score != 100 {
		t.Fatalf("expected accumulated score 100, got %d", out[0].Score)
	}
}

// TopByPeriod is capped at 100 (per the brief).
func TestRanker_TopByPeriod_Capped100(t *testing.T) {
	t.Parallel()
	r := leaderboard.NewRanker()
	scope, _ := leaderboard.ParseScope("global")
	for i := 0; i < 150; i++ {
		gcid := "g-" + string(rune('a'+i%26))
		r.Submit(scope, leaderboard.PeriodWeekly, gcid+"-"+string(rune('0'+i/26)), i+1)
	}
	out := r.TopByPeriod(scope, leaderboard.PeriodWeekly, "", 100)
	if len(out) > 100 {
		t.Fatalf("expected ≤100 entries, got %d", len(out))
	}
}

// RankOf returns the rank (1-based) of a gcid within scope+period; 0 if absent.
func TestRanker_RankOf_ReturnsRank(t *testing.T) {
	t.Parallel()
	r := leaderboard.NewRanker()
	scope, _ := leaderboard.ParseScope("global")
	r.Submit(scope, leaderboard.PeriodWeekly, "g-1", 100)
	r.Submit(scope, leaderboard.PeriodWeekly, "g-2", 200)
	r.Submit(scope, leaderboard.PeriodWeekly, "g-3", 50)
	if rk := r.RankOf(scope, leaderboard.PeriodWeekly, "", "g-2"); rk != 1 {
		t.Fatalf("expected rank 1 for g-2, got %d", rk)
	}
	if rk := r.RankOf(scope, leaderboard.PeriodWeekly, "", "g-1"); rk != 2 {
		t.Fatalf("expected rank 2 for g-1, got %d", rk)
	}
	if rk := r.RankOf(scope, leaderboard.PeriodWeekly, "", "g-3"); rk != 3 {
		t.Fatalf("expected rank 3 for g-3, got %d", rk)
	}
}

func TestRanker_RankOf_ZeroWhenAbsent(t *testing.T) {
	t.Parallel()
	r := leaderboard.NewRanker()
	scope, _ := leaderboard.ParseScope("global")
	if rk := r.RankOf(scope, leaderboard.PeriodWeekly, "", "ghost"); rk != 0 {
		t.Fatalf("expected 0 for absent gcid, got %d", rk)
	}
}

// Tenant-scoped boards are isolated by tenantID.
func TestRanker_Submit_TenantScopeIsolated(t *testing.T) {
	t.Parallel()
	r := leaderboard.NewRanker()
	scope, _ := leaderboard.ParseScope("tenant")
	r.SubmitTenant(scope, leaderboard.PeriodAllTime, "tenant-x", "alpha", 100)
	r.SubmitTenant(scope, leaderboard.PeriodAllTime, "tenant-y", "alpha", 999)

	outX := r.TopByPeriod(scope, leaderboard.PeriodAllTime, "tenant-x", 10)
	if len(outX) != 1 || outX[0].Score != 100 {
		t.Fatalf("tenant-x board polluted by tenant-y: %#v", outX)
	}
	outY := r.TopByPeriod(scope, leaderboard.PeriodAllTime, "tenant-y", 10)
	if len(outY) != 1 || outY[0].Score != 999 {
		t.Fatalf("tenant-y board polluted: %#v", outY)
	}
}
