// duel_rating_handlers_test.go — tests for GET /v1/duels/my-rating and
// GET /v1/duels/leaderboard HTTP handlers.
//
// Driver-free: the inmem DuelRepo satisfies DuelStore, so we exercise the
// full HTTP path (identity middleware → handler → JSON) without a database.
package httpadapter_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	httpadapter "github.com/apollo-chora/chora-sharing/internal/adapter/http"
	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/config"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
)

const (
	ratingTenant     = "01970000-0000-7000-8000-0000000000a1"
	ratingChallenger = "01970000-0000-7000-9000-0000000000a1"
	ratingOpponent   = "01970000-0000-7000-9000-0000000000a2"
)

// duelRatingRules returns the shipped SharingRules needed by the duel handlers.
func duelRatingRules() config.SharingRules {
	return config.SharingRules{
		ELOBaseline: 1200,
		ELOKFactor:  32,
		ComboTiers:  []int{1, 2, 3, 5},
	}
}

// duelRatingHandler builds a Handler wired with an inmem DuelRepo + the
// shipped SharingRules, returning both so tests can seed duels directly.
func duelRatingHandler() (http.Handler, *inmem.DuelRepo) {
	repo := inmem.NewDuelRepo()
	h := httpadapter.NewHandler(httpadapter.Deps{
		Duels: repo,
		Rules: duelRatingRules(),
	})
	return h, repo
}

// ratingRequest builds a GET request with identity headers for the given gcid.
func ratingRequest(gcid string, path string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("gcid", gcid)
	req.Header.Set("X-Tenant-Id", ratingTenant)
	return req
}

func decodeRatingBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode body %q: %v", rec.Body.String(), err)
	}
	return out
}

// completeRankedDuel drives a ranked duel through the full state machine so
// that WinnerGCID is set + the duel reaches StatusCompleted. The challenger
// answers correctly and the opponent answers incorrectly, making the
// challenger the winner.
func completeRankedDuel(t *testing.T) *duel.Duel {
	t.Helper()
	cfg := duel.DuelConfig{
		ELOBaseline:  1200,
		ELOKFactor:   32,
		ComboTiers:   []int{1, 2, 3, 5},
		RoundTimerSec: 30,
	}
	d, err := duel.NewDuel(cfg, ratingChallenger, ratingOpponent, ratingTenant,
		duel.ScopeRanked, 1, nil, 24*time.Hour)
	if err != nil {
		t.Fatalf("NewDuel: %v", err)
	}
	if err := d.Accept(ratingOpponent); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	picks := []duel.AtomPick{
		{AtomID: "atom-1", RevisionID: "rev-1", Question: "Q1",
			Options: []string{"A", "B"}, Answer: "A"},
	}
	if err := d.StartBattle(picks); err != nil {
		t.Fatalf("StartBattle: %v", err)
	}
	// FCFS: a correct answer on the single round resolves the round and
	// completes the duel immediately. The opponent (loser) does not
	// answer — the round is closed by the first correct answer.
	if _, err := d.ResolveRound(ratingChallenger, 1, true, 5000, []int{1, 2, 3, 5}); err != nil {
		t.Fatalf("ResolveRound challenger: %v", err)
	}
	if d.Status != duel.StatusCompleted {
		t.Fatalf("duel status=%s want completed", d.Status)
	}
	if d.WinnerGCID != ratingChallenger {
		t.Fatalf("winner=%s want %s", d.WinnerGCID, ratingChallenger)
	}
	return d
}

// -----------------------------------------------------------------------------
// GET /v1/duels/my-rating
// -----------------------------------------------------------------------------

func TestGetMyRating_Defaults(t *testing.T) {
	h, _ := duelRatingHandler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, ratingRequest(ratingChallenger, "/v1/duels/my-rating"))

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := decodeRatingBody(t, rec)
	if body["rating"] != float64(1200) {
		t.Errorf("rating=%v want 1200", body["rating"])
	}
	if body["wins"] != float64(0) {
		t.Errorf("wins=%v want 0", body["wins"])
	}
	if body["losses"] != float64(0) {
		t.Errorf("losses=%v want 0", body["losses"])
	}
	if body["draws"] != float64(0) {
		t.Errorf("draws=%v want 0", body["draws"])
	}
	if body["peak_elo"] != float64(1200) {
		t.Errorf("peak_elo=%v want 1200", body["peak_elo"])
	}
	if body["gcid"] != ratingChallenger {
		t.Errorf("gcid=%v want %s", body["gcid"], ratingChallenger)
	}
}

func TestGetMyRating_AfterDuel(t *testing.T) {
	h, repo := duelRatingHandler()

	d := completeRankedDuel(t)
	if err := repo.ApplyELO(context.Background(), d, 32); err != nil {
		t.Fatalf("ApplyELO: %v", err)
	}

	// Winner's rating should increase above 1200 and wins=1.
	recWinner := httptest.NewRecorder()
	h.ServeHTTP(recWinner, ratingRequest(ratingChallenger, "/v1/duels/my-rating"))
	if recWinner.Code != http.StatusOK {
		t.Fatalf("winner status=%d body=%s", recWinner.Code, recWinner.Body.String())
	}
	wBody := decodeRatingBody(t, recWinner)
	wRating, ok := wBody["rating"].(float64)
	if !ok {
		t.Fatalf("winner rating not a number: %v", wBody["rating"])
	}
	if wRating <= 1200 {
		t.Errorf("winner rating=%v want > 1200", wRating)
	}
	if wBody["wins"] != float64(1) {
		t.Errorf("winner wins=%v want 1", wBody["wins"])
	}
	if wBody["losses"] != float64(0) {
		t.Errorf("winner losses=%v want 0", wBody["losses"])
	}
	// Peak ELO should equal the new rating since it only went up.
	if wBody["peak_elo"] != wBody["rating"] {
		t.Errorf("winner peak_elo=%v want %v", wBody["peak_elo"], wBody["rating"])
	}

	// Loser's rating should drop below 1200 and losses=1.
	recLoser := httptest.NewRecorder()
	h.ServeHTTP(recLoser, ratingRequest(ratingOpponent, "/v1/duels/my-rating"))
	if recLoser.Code != http.StatusOK {
		t.Fatalf("loser status=%d body=%s", recLoser.Code, recLoser.Body.String())
	}
	lBody := decodeRatingBody(t, recLoser)
	lRating, ok := lBody["rating"].(float64)
	if !ok {
		t.Fatalf("loser rating not a number: %v", lBody["rating"])
	}
	if lRating >= 1200 {
		t.Errorf("loser rating=%v want < 1200", lRating)
	}
	if lBody["losses"] != float64(1) {
		t.Errorf("loser losses=%v want 1", lBody["losses"])
	}
	if lBody["wins"] != float64(0) {
		t.Errorf("loser wins=%v want 0", lBody["wins"])
	}
	// Peak ELO equals the loser's current rating: the inmem repo seeds
	// peakELO on the first setRating call, so even a rating below the
	// 1200 baseline becomes the initial peak.
	if lBody["peak_elo"] != lBody["rating"] {
		t.Errorf("loser peak_elo=%v want %v (current rating)", lBody["peak_elo"], lBody["rating"])
	}
}

// -----------------------------------------------------------------------------
// GET /v1/duels/leaderboard
// -----------------------------------------------------------------------------

func TestGetDuelLeaderboard_Empty(t *testing.T) {
	h, _ := duelRatingHandler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, ratingRequest(ratingChallenger, "/v1/duels/leaderboard"))

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := decodeRatingBody(t, rec)
	entries, ok := body["entries"].([]any)
	if !ok {
		t.Fatalf("entries not an array: %T", body["entries"])
	}
	if len(entries) != 0 {
		t.Errorf("entries len=%d want 0", len(entries))
	}
	if _, ok := body["computed_at"]; !ok {
		t.Error("missing computed_at field")
	}
}

func TestGetDuelLeaderboard_ReturnsEntries(t *testing.T) {
	h, repo := duelRatingHandler()

	// Create several ranked duels between distinct pairs so each player ends
	// up with a different rating. We use 4 players in a small tournament.
	players := []string{
		"01970000-0000-7000-9000-0000000000b1",
		"01970000-0000-7000-9000-0000000000b2",
		"01970000-0000-7000-9000-0000000000b3",
		"01970000-0000-7000-9000-0000000000b4",
	}

	// Duel 1: player[0] beats player[1] → player[0] up, player[1] down.
	playRankedDuel(t, repo, players[0], players[1], players[0])
	// Duel 2: player[2] beats player[3] → player[2] up, player[3] down.
	playRankedDuel(t, repo, players[2], players[3], players[2])
	// Duel 3: player[0] beats player[3] → player[0] up further, player[3] down further.
	playRankedDuel(t, repo, players[0], players[3], players[0])

	// Collect the ratings directly from the repo so we know the expected order.
	expected := make([]int, len(players))
	for i, p := range players {
		stats, err := repo.GetRatingStats(context.Background(), ratingTenant, p)
		if err != nil {
			t.Fatalf("GetRatingStats player[%d]: %v", i, err)
		}
		expected[i] = stats.Rating
	}

	// Fetch the leaderboard via HTTP.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, ratingRequest(ratingChallenger, "/v1/duels/leaderboard?limit=10"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := decodeRatingBody(t, rec)
	entries, ok := body["entries"].([]any)
	if !ok {
		t.Fatalf("entries not an array: %T", body["entries"])
	}

	// All 4 players should appear (each has a non-default rating after ELO).
	if len(entries) != 4 {
		t.Fatalf("entries len=%d want 4", len(entries))
	}

	// Verify entries are sorted by rating DESC.
	prev := 1 << 30 // start higher than any possible ELO
	gcidToRating := make(map[string]int)
	for i, e := range entries {
		entry, ok := e.(map[string]any)
		if !ok {
			t.Fatalf("entry[%d] not an object: %T", i, e)
		}
		rating, ok := entry["rating"].(float64)
		if !ok {
			t.Fatalf("entry[%d] rating not a number: %T", i, entry["rating"])
		}
		gcid, _ := entry["gcid"].(string)
		gcidToRating[gcid] = int(rating)
		if int(rating) > prev {
			t.Errorf("entry[%d] rating=%d not ≤ prev=%d (not DESC sorted)", i, int(rating), prev)
		}
		prev = int(rating)
	}

	// Verify each player's rating matches the repo.
	for i, p := range players {
		if gcidToRating[p] != expected[i] {
			t.Errorf("leaderboard rating for %s = %d, repo = %d", p, gcidToRating[p], expected[i])
		}
	}

	// The top entry should be player[0] (won 2 duels, highest rating).
	topEntry := entries[0].(map[string]any)
	if topEntry["gcid"] != players[0] {
		t.Errorf("top gcid=%v want %s", topEntry["gcid"], players[0])
	}
	if topEntry["wins"] != float64(2) {
		t.Errorf("top wins=%v want 2", topEntry["wins"])
	}
}

// playRankedDuel creates a ranked duel between challenger + opponent,
// drives it to completion with winnerGCID as the winner, and applies ELO
// to the repo. The winner answers correctly; the loser answers incorrectly.
func playRankedDuel(t *testing.T, repo *inmem.DuelRepo, challenger, opponent, winnerGCID string) {
	t.Helper()
	cfg := duel.DuelConfig{
		ELOBaseline:   1200,
		ELOKFactor:    32,
		ComboTiers:    []int{1, 2, 3, 5},
		RoundTimerSec: 30,
	}
	d, err := duel.NewDuel(cfg, challenger, opponent, ratingTenant,
		duel.ScopeRanked, 1, nil, 24*time.Hour)
	if err != nil {
		t.Fatalf("NewDuel: %v", err)
	}
	if err := d.Accept(opponent); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if err := d.StartBattle([]duel.AtomPick{
		{AtomID: "atom-x", RevisionID: "rev-x", Question: "Q",
			Options: []string{"A", "B"}, Answer: "A"},
	}); err != nil {
		t.Fatalf("StartBattle: %v", err)
	}
	if _, err := d.ResolveRound(winnerGCID, 1, true, 5000, []int{1, 2, 3, 5}); err != nil {
		t.Fatalf("ResolveRound winner: %v", err)
	}
	if d.Status != duel.StatusCompleted {
		t.Fatalf("duel status=%s want completed", d.Status)
	}
	if d.WinnerGCID != winnerGCID {
		t.Fatalf("winner=%s want %s", d.WinnerGCID, winnerGCID)
	}
	if err := repo.ApplyELO(context.Background(), d, 32); err != nil {
		t.Fatalf("ApplyELO: %v", err)
	}
}

// -----------------------------------------------------------------------------
// WS1: per-category leaderboard (GET /v1/duels/leaderboard?category=…)
// -----------------------------------------------------------------------------

// TestGetDuelLeaderboard_CategoryFilter pins WS1: the leaderboard endpoint
// accepts a ?category= query param and returns only ratings for that
// category. Without the filter, the overall bucket is returned.
func TestGetDuelLeaderboard_CategoryFilter(t *testing.T) {
	h, repo := duelRatingHandler()

	// Play a duel in "mathematics" — the winner gets a mathematics rating.
	playRankedDuelWithCategory(t, repo, ratingChallenger, ratingOpponent, ratingChallenger, "mathematics")

	// Play a duel in "programming" — the winner gets a programming rating.
	playerC := "01970000-0000-7000-9000-0000000000b3"
	playerD := "01970000-0000-7000-9000-0000000000b4"
	playRankedDuelWithCategory(t, repo, playerC, playerD, playerC, "programming")

	// Fetch the mathematics leaderboard.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, ratingRequest(ratingChallenger, "/v1/duels/leaderboard?category=mathematics"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := decodeRatingBody(t, rec)
	entries, ok := body["entries"].([]any)
	if !ok {
		t.Fatalf("entries not an array: %T", body["entries"])
	}
	// Only the 2 mathematics players should appear.
	if len(entries) != 2 {
		t.Errorf("mathematics entries len=%d want 2", len(entries))
	}
	// The programming players must NOT appear in the mathematics board.
	for _, e := range entries {
		entry := e.(map[string]any)
		gcid, _ := entry["gcid"].(string)
		if gcid == playerC || gcid == playerD {
			t.Errorf("programming player %s appeared in mathematics leaderboard", gcid)
		}
	}
}

// playRankedDuelWithCategory is playRankedDuel but also sets the duel's
// Category so ELO writes to the per-category rating bucket.
func playRankedDuelWithCategory(t *testing.T, repo *inmem.DuelRepo, challenger, opponent, winnerGCID, category string) {
	t.Helper()
	cfg := duel.DuelConfig{
		ELOBaseline:   1200,
		ELOKFactor:    32,
		ComboTiers:    []int{1, 2, 3, 5},
		RoundTimerSec: 30,
	}
	d, err := duel.NewDuel(cfg, challenger, opponent, ratingTenant,
		duel.ScopeRanked, 1, nil, 24*time.Hour)
	if err != nil {
		t.Fatalf("NewDuel: %v", err)
	}
	d.Category = category
	if err := d.Accept(opponent); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if err := d.StartBattle([]duel.AtomPick{
		{AtomID: "atom-x", RevisionID: "rev-x", Question: "Q",
			Options: []string{"A", "B"}, Answer: "A"},
	}); err != nil {
		t.Fatalf("StartBattle: %v", err)
	}
	if _, err := d.ResolveRound(winnerGCID, 1, true, 5000, []int{1, 2, 3, 5}); err != nil {
		t.Fatalf("ResolveRound winner: %v", err)
	}
	if d.Status != duel.StatusCompleted {
		t.Fatalf("duel status=%s want completed", d.Status)
	}
	if err := repo.ApplyELO(context.Background(), d, 32); err != nil {
		t.Fatalf("ApplyELO: %v", err)
	}
}
