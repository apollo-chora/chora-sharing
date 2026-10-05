package leaderboard_test

import (
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
)

const (
	tenantA = "01970000-0000-7000-8000-000000000001"
	gcidA   = "01970000-0000-7000-9000-000000000001"
	gcidB   = "01970000-0000-7000-9000-000000000002"
	gcidC   = "01970000-0000-7000-9000-000000000003"
)

func TestParseScope_Global(t *testing.T) {
	t.Parallel()
	s, err := leaderboard.ParseScope("global")
	if err != nil {
		t.Fatalf("ParseScope(global): %v", err)
	}
	if s.Kind != leaderboard.ScopeGlobal {
		t.Fatalf("expected ScopeGlobal, got %q", s.Kind)
	}
	if s.ID != "" {
		t.Fatalf("expected empty ID for global")
	}
}

func TestParseScope_Tenant(t *testing.T) {
	t.Parallel()
	s, err := leaderboard.ParseScope("tenant")
	if err != nil {
		t.Fatalf("ParseScope(tenant): %v", err)
	}
	if s.Kind != leaderboard.ScopeTenant {
		t.Fatalf("expected ScopeTenant, got %q", s.Kind)
	}
}

func TestParseScope_Cohort(t *testing.T) {
	t.Parallel()
	s, err := leaderboard.ParseScope("cohort:abc-123")
	if err != nil {
		t.Fatalf("ParseScope(cohort:abc-123): %v", err)
	}
	if s.Kind != leaderboard.ScopeCohort {
		t.Fatalf("expected ScopeCohort, got %q", s.Kind)
	}
	if s.ID != "abc-123" {
		t.Fatalf("expected ID abc-123, got %q", s.ID)
	}
}

func TestParseScope_RejectsCohortWithoutID(t *testing.T) {
	t.Parallel()
	if _, err := leaderboard.ParseScope("cohort:"); err == nil {
		t.Fatalf("expected error for cohort: with empty id")
	}
}

func TestParseScope_RejectsUnknown(t *testing.T) {
	t.Parallel()
	if _, err := leaderboard.ParseScope("bogus"); err == nil {
		t.Fatalf("expected error for unknown scope")
	}
}

func TestRanker_TopByPeriod_ReturnsSubmittedEntries(t *testing.T) {
	t.Parallel()
	r := leaderboard.NewRanker()
	scope, _ := leaderboard.ParseScope("global")
	r.Submit(scope, leaderboard.PeriodAllTime, gcidA, 500)
	r.Submit(scope, leaderboard.PeriodAllTime, gcidB, 300)
	out := r.TopByPeriod(scope, leaderboard.PeriodAllTime, "", 10)
	if len(out) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(out))
	}
	if out[0].GCID != gcidA || out[0].Score != 500 {
		t.Fatalf("expected gcidA first with 500, got %q score %d", out[0].GCID, out[0].Score)
	}
}

func TestRanker_TopByPeriod_DescendingScore(t *testing.T) {
	t.Parallel()
	r := leaderboard.NewRanker()
	scope, _ := leaderboard.ParseScope("global")
	r.Submit(scope, leaderboard.PeriodAllTime, gcidA, 500)
	r.Submit(scope, leaderboard.PeriodAllTime, gcidB, 300)
	r.Submit(scope, leaderboard.PeriodAllTime, gcidC, 800)
	out := r.TopByPeriod(scope, leaderboard.PeriodAllTime, "", 10)
	for i := 1; i < len(out); i++ {
		if out[i-1].Score < out[i].Score {
			t.Fatalf("ranking not in descending score order at index %d", i)
		}
	}
}

func TestRanker_TopByPeriod_AssignsRanks(t *testing.T) {
	t.Parallel()
	r := leaderboard.NewRanker()
	scope, _ := leaderboard.ParseScope("global")
	r.Submit(scope, leaderboard.PeriodAllTime, gcidA, 500)
	r.Submit(scope, leaderboard.PeriodAllTime, gcidB, 300)
	out := r.TopByPeriod(scope, leaderboard.PeriodAllTime, "", 10)
	for i, e := range out {
		if e.Rank != i+1 {
			t.Fatalf("expected rank %d at index %d, got %d", i+1, i, e.Rank)
		}
	}
}

func TestRanker_TopByPeriod_TenantScopePropagatesTenant(t *testing.T) {
	t.Parallel()
	r := leaderboard.NewRanker()
	scope, _ := leaderboard.ParseScope("tenant")
	r.SubmitTenant(scope, leaderboard.PeriodAllTime, tenantA, gcidA, 500)
	r.SubmitTenant(scope, leaderboard.PeriodAllTime, "other-tenant", gcidB, 300)
	out := r.TopByPeriod(scope, leaderboard.PeriodAllTime, tenantA, 10)
	if len(out) != 1 {
		t.Fatalf("expected 1 tenant-scoped entry, got %d", len(out))
	}
	if out[0].GCID != gcidA {
		t.Fatalf("expected gcidA, got %q", out[0].GCID)
	}
}

func TestRanker_TopByPeriod_NormalisesLimit(t *testing.T) {
	t.Parallel()
	r := leaderboard.NewRanker()
	scope, _ := leaderboard.ParseScope("global")
	r.Submit(scope, leaderboard.PeriodAllTime, gcidA, 500)
	out := r.TopByPeriod(scope, leaderboard.PeriodAllTime, "", 0)
	if len(out) == 0 {
		t.Fatalf("TopByPeriod(0) should default to non-zero size")
	}
	out = r.TopByPeriod(scope, leaderboard.PeriodAllTime, "", 999)
	if len(out) > 100 {
		t.Fatalf("expected cap at maxTopN=100, got %d", len(out))
	}
}
