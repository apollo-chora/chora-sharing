// duel_test.go — exercises the in-memory DuelRepo adapter end-to-end:
// CRUD, expired-round sweep, keyset pagination, ELO application (draw /
// win / loss / per-category), and rating leaderboards. The repo stores
// duels by reference swap, so mutation-heavy tests use fresh repos and
// are safe to parallelise.
package inmem_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
)

var duelCfg = duel.DuelConfig{ELOKFactor: 32, RoundTimerSec: 30}

func mustDuel(t *testing.T, tenantID, challenger, opponent string, scope duel.Scope) *duel.Duel {
	t.Helper()
	d, err := duel.NewDuel(duelCfg, challenger, opponent, tenantID, scope, 3, nil, 0)
	if err != nil {
		t.Fatalf("NewDuel(%s,%s,%s): %v", tenantID, challenger, opponent, err)
	}
	return d
}

func TestDuelRepo_SaveGetAndForUpdate(t *testing.T) {
	t.Parallel()
	repo := inmem.NewDuelRepo()
	ctx := context.Background()

	d := mustDuel(t, tenantA, gcidA, "opp-1", duel.ScopeFriendly)
	if err := repo.SaveDuel(ctx, d); err != nil {
		t.Fatalf("SaveDuel: %v", err)
	}
	// Save is an upsert keyed by ID — a second save with the same ID overwrites.
	if err := repo.SaveDuel(ctx, d); err != nil {
		t.Fatalf("SaveDuel replay: %v", err)
	}

	got, err := repo.GetDuel(ctx, d.ID)
	if err != nil {
		t.Fatalf("GetDuel: %v", err)
	}
	if got.ID != d.ID {
		t.Fatalf("GetDuel: ID mismatch")
	}

	forLock, err := repo.GetDuelForUpdate(ctx, d.ID)
	if err != nil {
		t.Fatalf("GetDuelForUpdate: %v", err)
	}
	if forLock.ID != d.ID {
		t.Fatalf("GetDuelForUpdate: ID mismatch")
	}

	if _, err := repo.GetDuel(ctx, "missing"); !errors.Is(err, inmem.ErrDuelNotFound) {
		t.Fatalf("GetDuel missing: expected ErrDuelNotFound, got %v", err)
	}
	if _, err := repo.GetDuelForUpdate(ctx, "missing"); !errors.Is(err, inmem.ErrDuelNotFound) {
		t.Fatalf("GetDuelForUpdate missing: expected ErrDuelNotFound, got %v", err)
	}
}

func TestDuelRepo_ListDuelsWithExpiredRounds(t *testing.T) {
	t.Parallel()
	repo := inmem.NewDuelRepo()
	ctx := context.Background()

	now := time.Now().UTC()
	past := now.Add(-time.Minute)
	future := now.Add(time.Minute)

	// In-progress with an unresolved round whose deadline has passed → must be listed.
	expired := mustDuel(t, tenantA, gcidA, "opp-1", duel.ScopeRanked)
	expired.Status = duel.StatusInProgress
	expired.Rounds = []duel.RoundSnapshot{{RoundNumber: 1, DeadlineAt: &past}}

	// Same shape but wrong tenant → skipped.
	otherTenant := mustDuel(t, tenantB, gcidA, "opp-1", duel.ScopeRanked)
	otherTenant.Status = duel.StatusInProgress
	otherTenant.Rounds = []duel.RoundSnapshot{{RoundNumber: 1, DeadlineAt: &past}}

	// Correct tenant but not in progress → skipped.
	pending := mustDuel(t, tenantA, gcidA, "opp-2", duel.ScopeFriendly)
	pending.Rounds = []duel.RoundSnapshot{{RoundNumber: 1, DeadlineAt: &past}}

	// Round already resolved → skipped even with a past deadline.
	resolvedRound := mustDuel(t, tenantA, gcidA, "opp-3", duel.ScopeRanked)
	resolvedRound.Status = duel.StatusInProgress
	resolvedAt := now.Add(-2 * time.Minute)
	resolvedRound.Rounds = []duel.RoundSnapshot{
		{RoundNumber: 1, ResolvedAt: &resolvedAt, DeadlineAt: &past},
		{RoundNumber: 2},
	}

	// No deadline stamp → skipped.
	noDeadline := mustDuel(t, tenantA, gcidA, "opp-4", duel.ScopeRanked)
	noDeadline.Status = duel.StatusInProgress
	noDeadline.Rounds = []duel.RoundSnapshot{{RoundNumber: 1}}

	// Deadline in the future → skipped.
	futureDeadline := mustDuel(t, tenantA, gcidA, "opp-5", duel.ScopeRanked)
	futureDeadline.Status = duel.StatusInProgress
	futureDeadline.Rounds = []duel.RoundSnapshot{{RoundNumber: 1, DeadlineAt: &future}}

	for _, d := range []*duel.Duel{expired, otherTenant, pending, resolvedRound, noDeadline, futureDeadline} {
		if err := repo.SaveDuel(ctx, d); err != nil {
			t.Fatalf("SaveDuel: %v", err)
		}
	}

	out, err := repo.ListDuelsWithExpiredRounds(ctx, tenantA, now)
	if err != nil {
		t.Fatalf("ListDuelsWithExpiredRounds: %v", err)
	}
	if len(out) != 1 || out[0].ID != expired.ID {
		t.Fatalf("expected only the expired-round duel; got %d duels", len(out))
	}
}

func TestDuelRepo_ListDuels_PaginationAndFilters(t *testing.T) {
	t.Parallel()
	repo := inmem.NewDuelRepo()
	ctx := context.Background()

	base := time.Now().UTC().Add(-time.Hour)
	mk := func(offMinutes int, challenger, opponent string, status duel.Status) *duel.Duel {
		d := mustDuel(t, tenantA, challenger, opponent, duel.ScopeRanked)
		d.CreatedAt = base.Add(time.Duration(offMinutes) * time.Minute)
		d.Status = status
		return d
	}

	d1 := mk(1, gcidA, "p-1", duel.StatusPending)   // challenger == gcidA, oldest
	d2 := mk(2, "p-2", gcidA, duel.StatusPending)    // opponent == gcidA
	d3 := mk(3, gcidA, "p-3", duel.StatusPending)    // challenger == gcidA
	d4 := mk(4, "p-4", gcidA, duel.StatusCompleted)  // opponent == gcidA, newest
	otherTenant := mustDuel(t, tenantB, gcidA, "p-9", duel.ScopeRanked)
	otherTenant.CreatedAt = base.Add(10 * time.Minute)

	for _, d := range []*duel.Duel{d1, d2, d3, d4, otherTenant} {
		if err := repo.SaveDuel(ctx, d); err != nil {
			t.Fatalf("SaveDuel: %v", err)
		}
	}

	// Newest-first: d4, d3, d2, d1 (tenant B duel excluded).
	page, next, err := repo.ListDuels(ctx, tenantA, gcidA, "", 1, "")
	if err != nil {
		t.Fatalf("ListDuels: %v", err)
	}
	if len(page) != 1 || page[0].ID != d4.ID {
		t.Fatalf("page1: want [d4], got %d duels", len(page))
	}
	if next != d4.ID {
		t.Fatalf("page1 next: want %s, got %q", d4.ID, next)
	}

	page, next, _ = repo.ListDuels(ctx, tenantA, gcidA, "", 1, next)
	if len(page) != 1 || page[0].ID != d3.ID || next != d3.ID {
		t.Fatalf("page2: want [d3] next=%s; got %d duels next=%q", d3.ID, len(page), next)
	}

	// Page 3: cursor = d2.ID → start=2 → [d2]; d1 still follows → next=d2.ID.
	page, next, _ = repo.ListDuels(ctx, tenantA, gcidA, "", 1, next)
	if len(page) != 1 || page[0].ID != d2.ID || next != d2.ID {
		t.Fatalf("page3: want [d2] next=%s; got %d duels next=%q", d2.ID, len(page), next)
	}

	// Page 4: cursor = d2.ID → [d1], no next cursor (last row).
	page, next, _ = repo.ListDuels(ctx, tenantA, gcidA, "", 1, next)
	if len(page) != 1 || page[0].ID != d1.ID || next != "" {
		t.Fatalf("page4: want [d1] with no next; got %d duels next=%q", len(page), next)
	}

	// Cursor past the last row → nil slice, empty cursor.
	page, next, _ = repo.ListDuels(ctx, tenantA, gcidA, "", 1, d1.ID)
	if page != nil || len(page) != 0 || next != "" {
		t.Fatalf("past-end page: want nil/empty; got %d duels next=%q", len(page), next)
	}

	// Unknown cursor → treated as first page.
	page, next, _ = repo.ListDuels(ctx, tenantA, gcidA, "", 10, "unknown-cursor")
	if len(page) != 4 || page[0].ID != d4.ID {
		t.Fatalf("unknown cursor: want 4 duels starting d4; got %d", len(page))
	}

	// Status filter: only completed duels for gcidA.
	page, _, _ = repo.ListDuels(ctx, tenantA, gcidA, string(duel.StatusCompleted), 10, "")
	if len(page) != 1 || page[0].ID != d4.ID {
		t.Fatalf("status filter completed: want [d4]; got %d", len(page))
	}
	page, _, _ = repo.ListDuels(ctx, tenantA, gcidA, string(duel.StatusExpired), 10, "")
	if len(page) != 0 {
		t.Fatalf("status filter expired: want none; got %d", len(page))
	}

	// limit <= 0 → default of 20 (single page with everything).
	page, next, _ = repo.ListDuels(ctx, tenantA, gcidA, "", 0, "")
	if len(page) != 4 || next != "" {
		t.Fatalf("limit<=0: want 4 duels with no next; got %d next=%q", len(page), next)
	}

	// A duel where gcidA is neither challenger nor opponent is excluded.
	neither := mustDuel(t, tenantA, "x-1", "x-2", duel.ScopeRanked)
	neither.CreatedAt = base.Add(30 * time.Minute)
	if err := repo.SaveDuel(ctx, neither); err != nil {
		t.Fatalf("SaveDuel: %v", err)
	}
	page, _, err = repo.ListDuels(ctx, tenantA, gcidA, "", 10, "")
	if err != nil {
		t.Fatalf("ListDuels neither: %v", err)
	}
	if len(page) != 4 {
		t.Fatalf("neither-match duel must be excluded; got %d", len(page))
	}
}

func TestDuelRepo_ResolveRound(t *testing.T) {
	t.Parallel()
	repo := inmem.NewDuelRepo()
	ctx := context.Background()

	d := mustDuel(t, tenantA, gcidA, "opp-1", duel.ScopeRanked)
	if err := repo.SaveDuel(ctx, d); err != nil {
		t.Fatalf("SaveDuel: %v", err)
	}
	if err := repo.ResolveRound(ctx, d, 1, gcidA, duel.RoundResolution{Correct: true}); err != nil {
		t.Fatalf("ResolveRound: %v", err)
	}
	// The mutated duel is persisted by reference swap — GetDuel must see it.
	got, err := repo.GetDuel(ctx, d.ID)
	if err != nil {
		t.Fatalf("GetDuel: %v", err)
	}
	if got != d {
		t.Fatal("ResolveRound must persist the mutated duel by reference swap")
	}

	// A duel that was never saved → ErrDuelNotFound.
	unsaved := mustDuel(t, tenantA, "opp-7", "opp-8", duel.ScopeRanked)
	if err := repo.ResolveRound(ctx, unsaved, 1, gcidA, duel.RoundResolution{}); !errors.Is(err, inmem.ErrDuelNotFound) {
		t.Fatalf("unsaved duel: expected ErrDuelNotFound, got %v", err)
	}
	// Nil duel → ErrInvalidArgument.
	if err := repo.ResolveRound(ctx, nil, 1, gcidA, duel.RoundResolution{}); !errors.Is(err, duel.ErrInvalidArgument) {
		t.Fatalf("nil duel: expected ErrInvalidArgument, got %v", err)
	}
}

func TestDuelRepo_StampRoundDeadline(t *testing.T) {
	t.Parallel()
	repo := inmem.NewDuelRepo()
	ctx := context.Background()

	d := mustDuel(t, tenantA, gcidA, "opp-1", duel.ScopeRanked)
	d.Rounds = []duel.RoundSnapshot{{RoundNumber: 1}, {RoundNumber: 2}}
	if err := repo.SaveDuel(ctx, d); err != nil {
		t.Fatalf("SaveDuel: %v", err)
	}

	dl := time.Now().UTC().Add(30 * time.Second)
	if err := repo.StampRoundDeadline(ctx, d.ID, 1, dl); err != nil {
		t.Fatalf("StampRoundDeadline: %v", err)
	}
	if d.Rounds[0].DeadlineAt == nil || !d.Rounds[0].DeadlineAt.Equal(dl) {
		t.Fatal("round 1 deadline not stamped")
	}
	// Idempotent: an already-stamped round is never re-stamped.
	replaced := dl.Add(time.Hour)
	if err := repo.StampRoundDeadline(ctx, d.ID, 1, replaced); err != nil {
		t.Fatalf("re-stamp: %v", err)
	}
	if !d.Rounds[0].DeadlineAt.Equal(dl) {
		t.Fatal("re-stamp must NOT reset an already-stamped deadline")
	}
	// A resolved round must never be stamped.
	resolvedAt := time.Now().UTC().Add(-time.Minute)
	d.Rounds[1].ResolvedAt = &resolvedAt
	if err := repo.StampRoundDeadline(ctx, d.ID, 2, replaced); err != nil {
		t.Fatalf("stamp resolved round: %v", err)
	}
	if d.Rounds[1].DeadlineAt != nil {
		t.Fatal("resolved round must not receive a deadline")
	}

	// Out of range: below 1 and above len(Rounds).
	if err := repo.StampRoundDeadline(ctx, d.ID, 0, dl); !errors.Is(err, duel.ErrRoundOutOfRange) {
		t.Fatalf("round 0: expected ErrRoundOutOfRange, got %v", err)
	}
	if err := repo.StampRoundDeadline(ctx, d.ID, 3, dl); !errors.Is(err, duel.ErrRoundOutOfRange) {
		t.Fatalf("round 3: expected ErrRoundOutOfRange, got %v", err)
	}
	// Unknown duel → ErrDuelNotFound.
	if err := repo.StampRoundDeadline(ctx, "missing", 1, dl); !errors.Is(err, inmem.ErrDuelNotFound) {
		t.Fatalf("missing duel: expected ErrDuelNotFound, got %v", err)
	}
}

func TestDuelRepo_ApplyELO(t *testing.T) {
	t.Parallel()
	repo := inmem.NewDuelRepo()
	ctx := context.Background()

	// Non-ranked scope: ELO is marked applied but never written.
	friendly := mustDuel(t, tenantA, gcidA, "opp-1", duel.ScopeFriendly)
	if err := repo.SaveDuel(ctx, friendly); err != nil {
		t.Fatalf("SaveDuel: %v", err)
	}
	if err := repo.ApplyELO(ctx, friendly, 32); err != nil {
		t.Fatalf("ApplyELO friendly: %v", err)
	}
	if rating, _ := repo.GetRating(ctx, tenantA, gcidA); rating != 1200 {
		t.Fatalf("friendly duel must not write ratings; got %d", rating)
	}
	// Idempotent: re-applying the same duel is a no-op.
	if err := repo.ApplyELO(ctx, friendly, 32); err != nil {
		t.Fatalf("ApplyELO replay: %v", err)
	}

	// Background: the stats for a player who never played default to 1200
	// (ratingOrDefault + peakOrDefault absent-key branches).
	if st0, err := repo.GetRatingStats(ctx, tenantA, "never-played"); err != nil {
		t.Fatalf("GetRatingStats fresh: %v", err)
	} else if st0.Rating != 1200 || st0.PeakELO != 1200 {
		t.Fatalf("fresh stats must default to 1200; got %+v", st0)
	}

	// Draw: no winner — both players move toward the midpoint.
	draw1 := mustDuel(t, tenantA, gcidA, "opp-2", duel.ScopeRanked)
	if err := repo.SaveDuel(ctx, draw1); err != nil {
		t.Fatalf("SaveDuel: %v", err)
	}
	if err := repo.ApplyELO(ctx, draw1, 32); err != nil {
		t.Fatalf("ApplyELO draw: %v", err)
	}
	stats, err := repo.GetRatingStats(ctx, tenantA, gcidA)
	if err != nil {
		t.Fatalf("GetRatingStats: %v", err)
	}
	if stats.Draws != 1 || stats.Rating != 1200 {
		t.Fatalf("after draw1: want draws=1 rating=1200; got draws=%d rating=%d", stats.Draws, stats.Rating)
	}
	// Second draw: rating unchanged (setRating peak-unchanged branch).
	draw2 := mustDuel(t, tenantA, gcidA, "opp-3", duel.ScopeRanked)
	if err := repo.SaveDuel(ctx, draw2); err != nil {
		t.Fatalf("SaveDuel: %v", err)
	}
	if err := repo.ApplyELO(ctx, draw2, 32); err != nil {
		t.Fatalf("ApplyELO draw2: %v", err)
	}

	// Ranked win for the challenger.
	win := mustDuel(t, tenantA, gcidA, "opp-4", duel.ScopeRanked)
	win.WinnerGCID = gcidA
	if err := repo.SaveDuel(ctx, win); err != nil {
		t.Fatalf("SaveDuel: %v", err)
	}
	if err := repo.ApplyELO(ctx, win, 32); err != nil {
		t.Fatalf("ApplyELO win: %v", err)
	}

	// Ranked loss for gcidA (challenger opp-5 wins) — rating drops below peak.
	lose := mustDuel(t, tenantA, "opp-5", gcidA, duel.ScopeRanked)
	lose.WinnerGCID = "opp-5"
	if err := repo.SaveDuel(ctx, lose); err != nil {
		t.Fatalf("SaveDuel: %v", err)
	}
	if err := repo.ApplyELO(ctx, lose, 32); err != nil {
		t.Fatalf("ApplyELO loss: %v", err)
	}

	stats, err = repo.GetRatingStats(ctx, tenantA, gcidA)
	if err != nil {
		t.Fatalf("GetRatingStats: %v", err)
	}
	if stats.Wins != 1 || stats.Losses != 1 || stats.Draws != 2 {
		t.Fatalf("record: want W1/L1/D2; got W%d/L%d/D%d", stats.Wins, stats.Losses, stats.Draws)
	}
	if stats.Rating < 1190 || stats.Rating > 1210 {
		t.Fatalf("post-loss rating should be back near 1200; got %d", stats.Rating)
	}
	if stats.PeakELO != 1216 {
		t.Fatalf("peak after a 1200→1216 win must be 1216; got %d", stats.PeakELO)
	}
	if rating, _ := repo.GetRating(ctx, tenantA, gcidA); rating != stats.Rating {
		t.Fatalf("GetRating = %d, want %d", rating, stats.Rating)
	}

	// Opponent-wins branch: WinnerGCID == OpponentGCID → the CHALLENGER is
	// the loser. opp-9 loses (1184), never having played before.
	oppWin := mustDuel(t, tenantA, "opp-9", gcidA, duel.ScopeRanked)
	oppWin.WinnerGCID = gcidA
	if err := repo.SaveDuel(ctx, oppWin); err != nil {
		t.Fatalf("SaveDuel: %v", err)
	}
	if err := repo.ApplyELO(ctx, oppWin, 32); err != nil {
		t.Fatalf("ApplyELO opponent-win: %v", err)
	}
	opp9, err := repo.GetRatingStats(ctx, tenantA, "opp-9")
	if err != nil {
		t.Fatalf("GetRatingStats opp-9: %v", err)
	}
	if opp9.Losses != 1 || opp9.Rating != 1184 {
		t.Fatalf("opp-9 (challenger side) must take the loss; got %+v", opp9)
	}

	// Per-category ELO (WS1): category defaults to "overall" for unspecified.
	cat := mustDuel(t, tenantA, gcidA, "opp-6", duel.ScopeRanked)
	cat.Category = "mathematics"
	cat.WinnerGCID = gcidA
	if err := repo.SaveDuel(ctx, cat); err != nil {
		t.Fatalf("SaveDuel: %v", err)
	}
	if err := repo.ApplyELO(ctx, cat, 32); err != nil {
		t.Fatalf("ApplyELO category: %v", err)
	}
	catRating, err := repo.GetRatingForCategory(ctx, tenantA, gcidA, "mathematics")
	if err != nil {
		t.Fatalf("GetRatingForCategory: %v", err)
	}
	if catRating != 1216 {
		t.Fatalf("mathematics rating: want 1216, got %d", catRating)
	}
	// Empty category resolves to "overall".
	overall, err := repo.GetRatingForCategory(ctx, tenantA, gcidA, "")
	if err != nil {
		t.Fatalf("GetRatingForCategory(overall): %v", err)
	}
	catStats, err := repo.GetRatingStatsForCategory(ctx, tenantA, gcidA, "mathematics")
	if err != nil {
		t.Fatalf("GetRatingStatsForCategory: %v", err)
	}
	if catStats.GCID != gcidA || catStats.Wins != 1 || catStats.PeakELO != 1216 {
		t.Fatalf("category stats mismatch: %+v", catStats)
	}
	// Empty-category stats resolve to overall bucket.
	overallStats, err := repo.GetRatingStatsForCategory(ctx, tenantA, gcidA, "")
	if err != nil {
		t.Fatalf("GetRatingStatsForCategory(overall): %v", err)
	}
	if overallStats.Rating != overall {
		t.Fatalf("empty-category stats should match overall; got %d vs %d", overallStats.Rating, overall)
	}
}

func TestDuelRepo_TopRatingsAndNearby(t *testing.T) {
	t.Parallel()
	repo := inmem.NewDuelRepo()
	ctx := context.Background()

	seed := func(challenger, opponent, winner string) {
		t.Helper()
		d := mustDuel(t, tenantA, challenger, opponent, duel.ScopeRanked)
		d.WinnerGCID = winner
		if err := repo.SaveDuel(ctx, d); err != nil {
			t.Fatalf("SaveDuel: %v", err)
		}
		if err := repo.ApplyELO(ctx, d, 32); err != nil {
			t.Fatalf("ApplyELO: %v", err)
		}
	}
	seed(gcidA, "p-1", gcidA) // gcidA 1216, p-1 1184
	seed("p-2", gcidA, "p-2") // p-2 1216, gcidA 1184
	seed("p-3", "p-4", "p-3") // p-3 1216, p-4 1184

	// A category-scoped duel must not leak into the "overall" bucket.
	arts := mustDuel(t, tenantA, "p-5", "p-6", duel.ScopeRanked)
	arts.Category = "arts"
	arts.WinnerGCID = "p-5"
	if err := repo.SaveDuel(ctx, arts); err != nil {
		t.Fatalf("SaveDuel: %v", err)
	}
	if err := repo.ApplyELO(ctx, arts, 32); err != nil {
		t.Fatalf("ApplyELO arts: %v", err)
	}

	// TopRatings: tie at 1216 among p-2/p-3 → gcid ascending; limit truncates.
	top, err := repo.TopRatings(ctx, tenantA, 2)
	if err != nil {
		t.Fatalf("TopRatings: %v", err)
	}
	if len(top) != 2 || top[0].GCID != "p-2" || top[1].GCID != "p-3" {
		t.Fatalf("TopRatings(2): want [p-2 p-3], got %+v", top)
	}
	// limit <= 0 or > 100 → clamped to 20 (all five overall entries).
	top, err = repo.TopRatings(ctx, tenantA, 0)
	if err != nil {
		t.Fatalf("TopRatings(0): %v", err)
	}
	if len(top) != 5 {
		t.Fatalf("TopRatings(0): want 5 entries, got %d", len(top))
	}

	// TopRatingsForCategory: "" → overall; "arts" → only the arts bucket
	// (both the winner and the loser of the arts duel).
	artsTop, err := repo.TopRatingsForCategory(ctx, tenantA, "arts", 0)
	if err != nil {
		t.Fatalf("TopRatingsForCategory(arts): %v", err)
	}
	if len(artsTop) != 2 || artsTop[0].GCID != "p-5" || artsTop[1].GCID != "p-6" {
		t.Fatalf("arts top: want [p-5 p-6], got %+v", artsTop)
	}
	overallTop, err := repo.TopRatingsForCategory(ctx, tenantA, "", 1000)
	if err != nil {
		t.Fatalf("TopRatingsForCategory(overall): %v", err)
	}
	if len(overallTop) != 5 {
		t.Fatalf("overall top: want 5 entries, got %d", len(overallTop))
	}

	// FindNearbyRatings: caller gcidA's rating is 1200 (won once, lost once),
	// so all four overall candidates are equidistant (16) → tie-break by gcid
	// ascending → [p-1, p-2, p-3, p-4]. limit 3 truncates.
	nearby, err := repo.FindNearbyRatings(ctx, tenantA, gcidA, 3)
	if err != nil {
		t.Fatalf("FindNearbyRatings: %v", err)
	}
	if len(nearby) != 3 {
		t.Fatalf("nearby: want 3, got %d", len(nearby))
	}
	if nearby[0].GCID != "p-1" || nearby[1].GCID != "p-2" || nearby[2].GCID != "p-3" {
		t.Fatalf("nearby order: want [p-1 p-2 p-3], got %+v", nearby)
	}
	// limit <= 0 or > 50 → clamped to 10.
	nearby, err = repo.FindNearbyRatings(ctx, tenantA, gcidA, 0)
	if err != nil {
		t.Fatalf("FindNearbyRatings(0): %v", err)
	}
	if len(nearby) != 4 {
		t.Fatalf("nearby(0): want 4, got %d", len(nearby))
	}

	// Negative-distance branch: a caller with the top rating (p-2, 1216)
	// sees equal- (p-3, 0) and lower-rated (gcidA, p-1, p-4) candidates, so
	// abs() must handle both signs. Order: p-3 (0), gcidA (16), p-1/p-4 (32).
	nearbyNeg, err := repo.FindNearbyRatings(ctx, tenantA, "p-2", 10)
	if err != nil {
		t.Fatalf("FindNearbyRatings(neg): %v", err)
	}
	if len(nearbyNeg) != 4 || nearbyNeg[0].GCID != "p-3" || nearbyNeg[1].GCID != gcidA {
		t.Fatalf("nearby from top caller: got %+v", nearbyNeg)
	}

	// Fresh repo: no ratings → empty result, no error.
	fresh := inmem.NewDuelRepo()
	empty, err := fresh.FindNearbyRatings(ctx, tenantA, gcidA, 10)
	if err != nil {
		t.Fatalf("fresh FindNearbyRatings: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("fresh repo: want 0, got %d", len(empty))
	}

	// Rating() direct accessor returns the raw stored value (0 for fresh).
	if got := fresh.Rating(gcidA); got != 0 {
		t.Fatalf("fresh Rating(): want 0, got %d", got)
	}
}