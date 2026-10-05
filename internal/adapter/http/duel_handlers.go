package httpadapter

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"

	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
)

func (h *Handler) submitDuelAnswer(w http.ResponseWriter, r *http.Request) {
	if h.deps.Duels == nil {
		writeErr(w, http.StatusNotImplemented, "duel deps not wired")
		return
	}

	duelID := pathParam(r, "duel_id")
	gcid := gcidFrom(r)

	var req struct {
		RoundNo      int    `json:"round_no"`
		Answer       string `json:"answer"`
		AnswerTimeMs int64  `json:"answer_time_ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "malformed body")
		return
	}

	d, err := h.deps.Duels.GetDuelForUpdate(r.Context(), duelID)
	if err != nil || d == nil {
		writeErr(w, http.StatusNotFound, "duel not found")
		return
	}

	// Determine correctness: prefer the round's embedded CorrectAnswer
	// (shuffled at StartBattle — the source of truth for the shuffled
	// option order). Fall back to the atom picker for legacy rounds that
	// have no embedded content.
	correct := false
	if req.RoundNo >= 1 && req.RoundNo <= len(d.Rounds) {
		round := d.Rounds[req.RoundNo-1]
		if round.CorrectAnswer != "" {
			correct = strings.EqualFold(strings.TrimSpace(req.Answer), strings.TrimSpace(round.CorrectAnswer))
		} else if h.deps.AtomSelector != nil {
			atomPick, err := h.deps.AtomSelector.GetAtomForRound(r.Context(), d, req.RoundNo)
			if err == nil && atomPick.Answer != "" {
				correct = strings.EqualFold(strings.TrimSpace(req.Answer), strings.TrimSpace(atomPick.Answer))
			}
		}
	}

	// Blitz duels use ResolveBlitzAnswer (answers don't close the round
	// until both players answer; race variant completes on target).
	// Classic duels use ResolveRound (FCFS — correct answer closes
	// the round immediately).
	if d.IsBlitz() {
		h.submitBlitzAnswer(w, r, d, gcid, req.RoundNo, correct, req.AnswerTimeMs)
		return
	}

	res, err := d.ResolveRound(gcid, req.RoundNo, correct, req.AnswerTimeMs, h.deps.Rules.ComboTiers)
	if err != nil {
		if errors.Is(err, duel.ErrAlreadyResolved) {
			writeErr(w, http.StatusConflict, "round already resolved")
			return
		}
		writeErr(w, http.StatusConflict, err.Error())
		return
	}

	if err := h.deps.Duels.ResolveRound(r.Context(), d, req.RoundNo, gcid, res); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	if res.DuelStatus == duel.StatusCompleted && d.Scope == duel.ScopeRanked {
	if err := h.deps.Duels.ApplyELO(r.Context(), d, h.deps.Rules.ELOKFactor); err != nil {
		log.Printf("duel: ApplyELO failed duel=%s err=%v", d.ID, err)
	}
	}

	// Publish duel.completed.v1 for both ranked + friendly duels so
	// downstream consumers (reward crediting, leaderboard refresh) fire.
	// Best-effort — the gameplay response is already determined.
	if res.DuelStatus == duel.StatusCompleted && h.deps.DuelEvents != nil {
		_ = h.deps.DuelEvents.PublishDuelCompleted(r.Context(),
			string(d.Scope), d.ID, d.TenantID,
			d.ChallengerGCID, d.OpponentGCID, d.WinnerGCID,
			d.ScoreChallenger, d.ScoreOpponent,
		)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"correct":          res.Correct,
		"combo_multiplier": res.ComboMultiplier,
		"points_awarded":   res.PointsAwarded,
		"current_combo":    res.CurrentCombo,
		"speed_bonus":      res.SpeedBonus,
		"duel_status":      string(res.DuelStatus),
		"winner_gcid":      res.WinnerGCID,
	})
}

// submitBlitzAnswer handles the blitz-mode answer path. Unlike classic,
// a blitz answer does NOT resolve the round immediately — the opponent
// can still answer independently. The round resolves when both players
// have answered. For race variant, the duel completes when a player
// reaches the target.
func (h *Handler) submitBlitzAnswer(w http.ResponseWriter, r *http.Request, d *duel.Duel, gcid string, roundNo int, correct bool, answerTimeMs int64) {
	res, err := d.ResolveBlitzAnswer(gcid, roundNo, correct, answerTimeMs, h.deps.Rules.ComboTiers)
	if err != nil {
		if errors.Is(err, duel.ErrAlreadyResolved) {
			writeErr(w, http.StatusConflict, "round already resolved")
			return
		}
		writeErr(w, http.StatusConflict, err.Error())
		return
	}

	// Blitz mutates the duel's score/combo/round state directly — persist
	// the full duel (classic uses ResolveRound for per-round persistence).
	if err := h.deps.Duels.SaveDuel(r.Context(), d); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	if res.DuelStatus == duel.StatusCompleted && d.Scope == duel.ScopeRanked {
		if err := h.deps.Duels.ApplyELO(r.Context(), d, h.deps.Rules.ELOKFactor); err != nil {
			log.Printf("duel: ApplyELO failed blitz duel=%s err=%v", d.ID, err)
		}
	}

	if res.DuelStatus == duel.StatusCompleted && h.deps.DuelEvents != nil {
		_ = h.deps.DuelEvents.PublishDuelCompleted(r.Context(),
			string(d.Scope), d.ID, d.TenantID,
			d.ChallengerGCID, d.OpponentGCID, d.WinnerGCID,
			d.ScoreChallenger, d.ScoreOpponent,
		)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"correct":          res.Correct,
		"combo_multiplier": res.ComboMultiplier,
		"points_awarded":   res.PointsAwarded,
		"current_combo":    res.CurrentCombo,
		"speed_bonus":      res.SpeedBonus,
		"round_resolved":   res.RoundResolved,
		"duel_status":      string(res.DuelStatus),
		"winner_gcid":      res.WinnerGCID,
		"score_challenger": d.ScoreChallenger,
		"score_opponent":   d.ScoreOpponent,
	})
}

type duelSummaryJSON struct {
	DuelID          string `json:"duel_id"`
	Status          string `json:"status"`
	Scope           string `json:"scope"`
	ChallengerGCID  string `json:"challenger_gcid"`
	OpponentGCID    string `json:"opponent_gcid"`
	ScoreChallenger int    `json:"score_challenger"`
	ScoreOpponent   int    `json:"score_opponent"`
	WinnerGCID      string `json:"winner_gcid"`
	CreatedAt       string `json:"created_at"`
}

func (h *Handler) listDuels(w http.ResponseWriter, r *http.Request) {
	if h.deps.Duels == nil {
		writeErr(w, http.StatusNotImplemented, "duel deps not wired")
		return
	}
	tenantID := tenantFrom(r)
	gcid := gcidFrom(r)
	limit := parseLimit(r.URL.Query().Get("limit"), 20, 100)
	status := r.URL.Query().Get("status")
	cursor := r.URL.Query().Get("cursor")

	duels, nextCursor, err := h.deps.Duels.ListDuels(r.Context(), tenantID, gcid, status, limit, cursor)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list duels: "+err.Error())
		return
	}
	out := make([]duelSummaryJSON, 0, len(duels))
	for _, d := range duels {
		out = append(out, duelSummaryJSON{
			DuelID:          d.ID,
			Status:          string(d.Status),
			Scope:           string(d.Scope),
			ChallengerGCID:  d.ChallengerGCID,
			OpponentGCID:    d.OpponentGCID,
			ScoreChallenger: d.ScoreChallenger,
			ScoreOpponent:   d.ScoreOpponent,
			WinnerGCID:      d.WinnerGCID,
			CreatedAt:       d.CreatedAt.UTC().Format("2006-01-02T15:04:05.000000Z"),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"data":        out,
		"next_cursor": nextCursor,
	})
}

func (h *Handler) getDuel(w http.ResponseWriter, r *http.Request) {
	if h.deps.Duels == nil {
		writeErr(w, http.StatusNotImplemented, "duel deps not wired")
		return
	}

	duelID := pathParam(r, "duel_id")
	tenantID := tenantFrom(r)

	d, err := h.deps.Duels.GetDuel(r.Context(), duelID)
	if err != nil || d == nil {
		writeErr(w, http.StatusNotFound, "duel not found")
		return
	}
	if d.TenantID != tenantID {
		writeErr(w, http.StatusNotFound, "duel not found")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"duel_id":          d.ID,
		"status":           string(d.Status),
		"scope":            string(d.Scope),
		"challenger_gcid":  d.ChallengerGCID,
		"opponent_gcid":    d.OpponentGCID,
		"score_challenger": d.ScoreChallenger,
		"score_opponent":   d.ScoreOpponent,
		"combo_challenger": d.ComboChallenger,
		"combo_opponent":   d.ComboOpponent,
		"winner_gcid":      d.WinnerGCID,
		"interest_tags":    d.InterestTags,
		"round_count":      d.RoundCount,
	})
}

func (h *Handler) duelWS(w http.ResponseWriter, r *http.Request) {
	if h.deps.WSHandler == nil {
		writeErr(w, http.StatusNotImplemented, "WebSocket handler not wired")
		return
	}
	h.deps.WSHandler.ServeHTTP(w, r)
}

var _ = strings.TrimSpace

// getMyRating — GET /v1/duels/my-rating.
//
// Returns the caller's duel rating + W/L/D record + peak ELO + proficiency.
// Proficiency = duel_elo + course_bonus (design §1), where course_bonus =
// min(200, completed_courses * 20). Completed course count comes from the
// user's profiler profile (which stores course titles from the generation
// step). Returns defaults (1200, 0/0/0, peak 1200, proficiency 1200) for a
// player with no duels.
func (h *Handler) getMyRating(w http.ResponseWriter, r *http.Request) {
	if h.deps.Duels == nil {
		writeErr(w, http.StatusNotImplemented, "duel deps not wired")
		return
	}

	tenantID := tenantFrom(r)
	gcid := gcidFrom(r)

	stats, err := h.deps.Duels.GetRatingStats(r.Context(), tenantID, gcid)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "get rating: "+err.Error())
		return
	}

	// Fetch completed course count from the profiler profile.
	if h.deps.Profiles != nil {
		if profile, err := h.deps.Profiles.GetProfile(r.Context(), gcid); err == nil && profile != nil {
			stats.CompletedCourses = len(profile.CourseTitles)
		}
	}
	stats.ComputeProficiency()

	writeJSON(w, http.StatusOK, map[string]any{
		"gcid":              stats.GCID,
		"rating":            stats.Rating,
		"wins":              stats.Wins,
		"losses":            stats.Losses,
		"draws":             stats.Draws,
		"peak_elo":          stats.PeakELO,
		"completed_courses": stats.CompletedCourses,
		"course_bonus":      stats.CourseBonus,
		"proficiency":       stats.Proficiency,
	})
}

// getDuelLeaderboard — GET /v1/duels/leaderboard.
//
// Returns the top-N duel players by ELO rating for the caller's tenant.
// Query params: limit (default 20, max 100), category (WS1: per-category
// board; "overall" or omitted = the default board).
func (h *Handler) getDuelLeaderboard(w http.ResponseWriter, r *http.Request) {
	if h.deps.Duels == nil {
		writeErr(w, http.StatusNotImplemented, "duel deps not wired")
		return
	}

	tenantID := tenantFrom(r)
	limit := parseLimit(r.URL.Query().Get("limit"), 20, 100)
	category := strings.TrimSpace(r.URL.Query().Get("category"))

	var stats []duel.RatingStats
	var err error
	if category != "" && category != "overall" {
		stats, err = h.deps.Duels.TopRatingsForCategory(r.Context(), tenantID, category, limit)
	} else {
		stats, err = h.deps.Duels.TopRatings(r.Context(), tenantID, limit)
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "duel leaderboard: "+err.Error())
		return
	}

	type entryJSON struct {
		GCID        string `json:"gcid"`
		DisplayName string `json:"display_name"`
		Rating      int    `json:"rating"`
		Wins        int    `json:"wins"`
		Losses      int    `json:"losses"`
		Draws       int    `json:"draws"`
		PeakELO     int    `json:"peak_elo"`
	}
	out := make([]entryJSON, 0, len(stats))
	for _, s := range stats {
		out = append(out, entryJSON{
			GCID:        s.GCID,
			DisplayName: s.DisplayName,
			Rating:      s.Rating,
			Wins:        s.Wins,
			Losses:      s.Losses,
			Draws:       s.Draws,
			PeakELO:     s.PeakELO,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"entries":     out,
		"computed_at": nowUTC().Format("2006-01-02T15:04:05.000000Z"),
	})
}
