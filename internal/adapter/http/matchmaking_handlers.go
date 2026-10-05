// matchmaking_handlers.go — HTTP handlers for the pool-based duel
// matchmaking endpoints.
//
// Routes:
//
//	POST   /v1/duels/queue           — enter the matchmaking pool
//	DELETE /v1/duels/queue           — cancel matchmaking
//	POST   /v1/duels/queue/heartbeat — ping that the user is still searching
//	GET    /v1/duels/queue/status    — poll queue status (fallback)
//
// The matchmaking_queue table is the source of truth. A background
// Matchmaker polls it per tenant and claims pairs atomically; the handler
// consumes MatchResults to create Duels directly as in_progress (no
// pending/accepted states — pool matching IS the acceptance per the spec).
// Clients learn their duel ID from the queue row's matched_duel_id (or,
// same-pod, from the in-memory pending-match fast path).
package httpadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
	domainmm "github.com/apollo-chora/chora-sharing/internal/domain/matchmaking"
)

// MatchmakerPort is the port the matchmaking handlers depend on.
// The matchmaker runs a background loop that polls the DB and emits
// MatchResults; the handler consumes them to create Duels.
type MatchmakerPort interface {
	Match() <-chan domainmm.MatchResult
	RegisterTenant(tenantID string)
	Stop()
}

// RoundSweeperRegistrar registers a tenant with the WS3 round-timer sweeper
// when a duel transitions to in_progress. Implemented by *ws.RoundSweeper.
type RoundSweeperRegistrar interface {
	RegisterTenant(tenantID string)
}

// MatchmakingQueueRepo persists queue state and drives multi-pod matching.
type MatchmakingQueueRepo interface {
	Enqueue(ctx context.Context, s domainmm.Searcher) error
	UpdateStatus(ctx context.Context, tenantID, gcid string, status domainmm.QueueStatus, matchedDuelID string) error
	UpdateHeartbeat(ctx context.Context, tenantID, gcid string) error
	GetByGCID(ctx context.Context, tenantID, gcid string) (*domainmm.Searcher, error)
	RevertToFinding(ctx context.Context, tenantID, gcid, duelID string) error
}

// AtomSelectionRequest carries the inputs the duel_atom_smith agent needs
// to personalize picks: both players' GCIDs (never duel on your own
// atom), shared interest tags, per-player proficiencies (ELO + course
// bonus; empty → 1200 default), and the round count.
type AtomSelectionRequest struct {
	TenantID      string
	ExcludeGCIDs  []string // both players — never duel on your own atom
	Tags          []string // shared interest tags (may be empty)
	Proficiencies []int    // one per player (ELO + course bonus); empty → 1200
	Count         int
}

// AtomSelectorPort selects atoms for duel rounds. SelectAtoms is the
// AI-driven pick+generate path (duel_atom_smith agent); GetAtomForRound
// is the embedded-first round-delivery path (no AI — reads the round
// snapshot's embedded Question/Options/CorrectAnswer, falling back to
// the projection read-model for legacy rows).
type AtomSelectorPort interface {
	SelectAtoms(ctx context.Context, req AtomSelectionRequest) ([]duel.AtomPick, error)
	GetAtomForRound(ctx context.Context, d *duel.Duel, roundNo int) (duel.AtomPick, error)
}

type queueRequest struct {
	InterestTags  []string `json:"interest_tags"`
	QuestionCount int      `json:"question_count"`
	Category      string   `json:"category"`
	// Mode is the gameplay model: "classic" (sequential FCFS rounds) or
	// "blitz" (all questions open at once). Empty defaults to "classic".
	Mode string `json:"mode"`
	// BlitzVariant is the blitz win condition: "timed" (most correct
	// within the time limit) or "race" (first to N correct). Only
	// meaningful when Mode=="blitz". If omitted for blitz mode, the
	// handler defaults to "timed".
	BlitzVariant string `json:"blitz_variant"`
}

// validQuestionCountPresets is the accepted duel-length presets (WS4).
// Both players must agree on the preset; the matchmaker pairs only
// matching counts.
var validQuestionCountPresets = map[int]struct{ }{5: {}, 10: {}, 15: {}}

// isValidQuestionCount reports whether n is one of the accepted presets.
// Zero is valid (means "default" — the handler replaces it with 5).
func isValidQuestionCount(n int) bool {
	_, ok := validQuestionCountPresets[n]
	return ok
}

// validCategories is the accepted per-category taxonomy (WS1). Mirrors the
// profiler's 6 categories + "overall" (the default / un-categorized bucket).
var validCategories = map[string]struct{}{
	"overall":      {},
	"programming":  {},
	"mathematics":  {},
	"science":      {},
	"humanities":   {},
	"arts":         {},
	"languages":    {},
}

// isValidCategory reports whether c is one of the accepted categories.
func isValidCategory(c string) bool {
	_, ok := validCategories[c]
	return ok
}

// validModes is the accepted duel gameplay models.
var validModes = map[string]struct{}{
	"classic": {},
	"blitz":   {},
}

// isValidMode reports whether m is one of the accepted modes.
func isValidMode(m string) bool {
	_, ok := validModes[m]
	return ok
}

// validBlitzVariants is the accepted blitz win conditions.
var validBlitzVariants = map[string]struct{}{
	"timed": {},
	"race":  {},
}

// isValidBlitzVariant reports whether v is one of the accepted blitz variants.
func isValidBlitzVariant(v string) bool {
	_, ok := validBlitzVariants[v]
	return ok
}

type queueResponse struct {
	Status    string `json:"status"`
	ExpiresAt string `json:"expires_at,omitempty"`
	Now       string `json:"now,omitempty"`
	DuelID    string `json:"duel_id,omitempty"`
}

// enterQueue — POST /v1/duels/queue.
//
// Enters the caller into the matchmaking pool. Returns 202 Accepted with
// {status: "finding", expires_at}. The Matchmaker background loop will pair
// the caller with a compatible opponent when one becomes available.
func (h *Handler) enterQueue(w http.ResponseWriter, r *http.Request) {
	if h.deps.Matchmaker == nil {
		writeErr(w, http.StatusNotImplemented, "matchmaker not wired")
		return
	}

	idemKey := idempotencyKey(r)
	if idemKey == "" {
		writeErr(w, http.StatusBadRequest, "Idempotency-Key required")
		return
	}

	tenantID := tenantFrom(r)
	gcid := gcidFrom(r)

	var req queueRequest
	if r.Body != nil && r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "malformed body")
			return
		}
	}

	// WS4: validate question_count. Zero means "default" (5); any other
	// non-preset value (0 excluded) is rejected fail-loud.
	qCount := req.QuestionCount
	if qCount == 0 {
		qCount = 5
	} else if !isValidQuestionCount(qCount) {
		writeErr(w, http.StatusBadRequest, "question_count must be one of: 5, 10, 15")
		return
	}

	// WS1: validate category. Empty means "overall" (default); any other
	// value must be one of the 6 profiler categories.
	category := strings.TrimSpace(req.Category)
	if category == "" {
		category = "overall"
	} else if !isValidCategory(category) {
		writeErr(w, http.StatusBadRequest, "category must be one of: overall, programming, mathematics, science, humanities, arts, languages")
		return
	}

	// Blitz: validate mode + blitz_variant. Mode defaults to "classic"
	// (backward-compatible). When mode is "blitz", blitz_variant defaults
	// to "timed" if omitted. An unknown mode or variant is rejected
	// fail-loud.
	mode := strings.TrimSpace(req.Mode)
	if mode == "" {
		mode = "classic"
	} else if !isValidMode(mode) {
		writeErr(w, http.StatusBadRequest, "mode must be one of: classic, blitz")
		return
	}
	blitzVariant := strings.TrimSpace(req.BlitzVariant)
	if mode == "blitz" {
		if blitzVariant == "" {
			blitzVariant = "timed"
		} else if !isValidBlitzVariant(blitzVariant) {
			writeErr(w, http.StatusBadRequest, "blitz_variant must be one of: timed, race")
			return
		}
	} else if blitzVariant != "" {
		writeErr(w, http.StatusBadRequest, "blitz_variant only valid when mode=blitz")
		return
	}

	// Fetch the caller's proficiency (ELO + course bonus).
	proficiency := h.deps.Rules.ELOBaseline
	if h.deps.Duels != nil {
		stats, err := h.deps.Duels.GetRatingStats(r.Context(), tenantID, gcid)
		if err == nil {
			stats.ComputeProficiency()
			proficiency = stats.Proficiency
		}
	}

	// Fetch interest tags from profiler profile if not provided in the request.
	tags := req.InterestTags
	if len(tags) == 0 && h.deps.Profiles != nil {
		if profile, err := h.deps.Profiles.GetProfile(r.Context(), gcid); err == nil && profile != nil {
			tags = profile.FlatTags()
		}
	}

	now := time.Now().UTC()
	searcher := domainmm.Searcher{
		GCID:          gcid,
		TenantID:      tenantID,
		Proficiency:   proficiency,
		InterestTags:  tags,
		QuestionCount: qCount,
		Category:      category,
		Mode:          mode,
		BlitzVariant:  blitzVariant,
		EnteredAt:     now,
		ExpiresAt:     now.Add(h.deps.Rules.MatchmakingTimeout),
		LastHeartbeat: now,
	}

	// Persist to DB — this is the source of truth for the matchmaker.
	if h.deps.MatchmakingQueue == nil {
		writeErr(w, http.StatusNotImplemented, "matchmaking queue not wired")
		return
	}
	if err := h.deps.MatchmakingQueue.Enqueue(r.Context(), searcher); err != nil {
		log.Printf("matchmaking: Enqueue failed for %s: %v", gcid, err)
		writeErr(w, http.StatusInternalServerError, "failed to enter queue")
		return
	}

	// Register the tenant with the matchmaker's polling hint set.
	if h.deps.Matchmaker != nil {
		h.deps.Matchmaker.RegisterTenant(tenantID)
	}

	writeJSON(w, http.StatusAccepted, queueResponse{
		Status:    string(domainmm.StatusFinding),
		ExpiresAt: searcher.ExpiresAt.Format(time.RFC3339),
		Now:       now.Format(time.RFC3339),
	})
}

// cancelQueue — DELETE /v1/duels/queue.
//
// Removes the caller from the matchmaking pool. Returns 200 with
// {status: "cancelled"}. Idempotent: cancelling an entry that is no longer
// finding (already matched/expired) is a no-op, but a real persistence
// failure surfaces as 500.
func (h *Handler) cancelQueue(w http.ResponseWriter, r *http.Request) {
	if h.deps.MatchmakingQueue == nil {
		writeErr(w, http.StatusNotImplemented, "matchmaking queue not wired")
		return
	}

	gcid := gcidFrom(r)
	tenantID := tenantFrom(r)

	if err := h.deps.MatchmakingQueue.UpdateStatus(r.Context(), tenantID, gcid, domainmm.StatusCancelled, ""); err != nil {
		log.Printf("matchmaking: cancel failed for %s: %v", gcid, err)
		writeErr(w, http.StatusInternalServerError, "failed to cancel matchmaking")
		return
	}

	writeJSON(w, http.StatusOK, queueResponse{
		Status: string(domainmm.StatusCancelled),
	})
}

// heartbeatQueue — POST /v1/duels/queue/heartbeat.
//
// Updates the caller's last_heartbeat timestamp. Returns 200 with
// {status: "finding"} or {status: "matched", duel_id} if a match was found.
func (h *Handler) heartbeatQueue(w http.ResponseWriter, r *http.Request) {
	h.writeQueueState(w, r, true)
}

// queueStatus — GET /v1/duels/queue/status.
//
// Returns the caller's current queue status. This is a polling fallback
// for environments where the heartbeat response is not sufficient.
func (h *Handler) queueStatus(w http.ResponseWriter, r *http.Request) {
	h.writeQueueState(w, r, false)
}

// writeQueueState backs the heartbeat and status endpoints. With touch=true
// (heartbeat) it also refreshes last_heartbeat and re-registers the tenant
// with the matchmaker's hint set (covers rolling deploys, where a fresh pod
// has an empty hint set).
//
// Match discovery is durable and multi-pod safe: after the in-memory
// pending-match fast path (same pod that created the duel), it falls back
// to the queue row — a 'matched' row carries matched_duel_id written by
// the claiming pod. The duel row's existence is verified before reporting
// a match, because there is a brief window between the atomic claim and
// SaveDuel (and a failed creation reverts the row to finding).
func (h *Handler) writeQueueState(w http.ResponseWriter, r *http.Request, touch bool) {
	if h.deps.MatchmakingQueue == nil {
		writeErr(w, http.StatusNotImplemented, "matchmaking queue not wired")
		return
	}

	gcid := gcidFrom(r)
	tenantID := tenantFrom(r)

	// Fast path: pending match created by this pod, not yet picked up.
	if duelID := h.consumePendingMatch(gcid); duelID != "" {
		writeJSON(w, http.StatusOK, queueResponse{
			Status: string(domainmm.StatusMatched),
			DuelID: duelID,
			Now:    time.Now().UTC().Format(time.RFC3339),
		})
		return
	}

	s, err := h.deps.MatchmakingQueue.GetByGCID(r.Context(), tenantID, gcid)
	if err != nil {
		log.Printf("matchmaking: GetByGCID failed for %s: %v", gcid, err)
		writeErr(w, http.StatusInternalServerError, "failed to read queue state")
		return
	}
	if s == nil {
		writeErr(w, http.StatusNotFound, "not in queue")
		return
	}

	switch s.Status {
	case domainmm.StatusMatched:
		if s.MatchedDuelID == "" {
			// Claim writes matched_duel_id in the same statement — a
			// matched row without one is data corruption; be loud.
			log.Printf("matchmaking: matched queue row for %s has no matched_duel_id", gcid)
			writeErr(w, http.StatusInternalServerError, "corrupt queue state")
			return
		}
		d, ok := h.duelForMatch(r.Context(), s.MatchedDuelID)
		if !ok || d == nil {
			// Duel row not visible yet — creating pod is mid-pipeline,
			// or creation failed and the row is about to be reverted.
			// Report finding so the client keeps polling.
			writeJSON(w, http.StatusOK, queueResponse{
				Status: string(domainmm.StatusFinding),
			})
			return
		}
		if !isPlayableDuel(d.Status) {
			// The duel reached a terminal state (completed/expired/
			// forfeited) but the queue row still points at it. This is
			// a stale match — clean up the row and return "not in queue"
			// so the user returns to the lobby instead of being routed
			// into a dead arena. This kills the stuck-arena trap.
			log.Printf("matchmaking: matched duel %s for %s has terminal status %s — clearing stale queue row",
				s.MatchedDuelID, gcid, d.Status)
			_ = h.deps.MatchmakingQueue.UpdateStatus(r.Context(), tenantID, gcid, domainmm.StatusAbandoned, "")
			writeErr(w, http.StatusNotFound, "not in queue")
			return
		}
		writeJSON(w, http.StatusOK, queueResponse{
			Status: string(domainmm.StatusMatched),
			DuelID: s.MatchedDuelID,
			Now:    time.Now().UTC().Format(time.RFC3339),
		})

	case domainmm.StatusFinding:
		if touch {
			if err := h.deps.MatchmakingQueue.UpdateHeartbeat(r.Context(), tenantID, gcid); err != nil {
				log.Printf("matchmaking: UpdateHeartbeat failed for %s: %v", gcid, err)
				writeErr(w, http.StatusInternalServerError, "failed to update heartbeat")
				return
			}
			if h.deps.Matchmaker != nil {
				h.deps.Matchmaker.RegisterTenant(tenantID)
			}
		}
		// expires_at + now are surfaced so the FE can render a clock-skew-
		// safe matchmaking countdown (WS2/G2). The queue row already stores
		// ExpiresAt (now + MatchmakingTimeout at enqueue time); without it
		// the searching screen has no deadline to count down from.
		writeJSON(w, http.StatusOK, queueResponse{
			Status:    string(domainmm.StatusFinding),
			ExpiresAt: s.ExpiresAt.Format(time.RFC3339),
			Now:       time.Now().UTC().Format(time.RFC3339),
		})

	default:
		// cancelled / expired / abandoned — no longer in the pool.
		writeErr(w, http.StatusNotFound, "not in queue")
	}
}

// duelForMatch fetches the duel for a claimed match. Returns (nil, false)
// on error or when the row doesn't exist yet — the caller treats that as
// "duel not yet visible, keep polling".
func (h *Handler) duelForMatch(ctx context.Context, duelID string) (*duel.Duel, bool) {
	if h.deps.Duels == nil || duelID == "" {
		return nil, false
	}
	d, err := h.deps.Duels.GetDuel(ctx, duelID)
	if err != nil {
		log.Printf("matchmaking: GetDuel(%s) during match check: %v", duelID, err)
		return nil, false
	}
	return d, d != nil
}

// isPlayableDuel reports whether the duel status is still active (not
// completed/expired/forfeited). Used to detect stale matched queue rows
// that point at a dead arena — the user should return to the lobby
// instead of being routed into a duel that's already over.
func isPlayableDuel(status duel.Status) bool {
	switch status {
	case duel.StatusInProgress, duel.StatusPending, duel.StatusAccepted:
		return true
	}
	return false
}

// pendingMatches stores duel IDs for matched users that haven't been
// notified yet (via heartbeat or status polling).
// This is an in-memory fast path for same-pod pickup; the queue row's
// matched_duel_id is the durable multi-pod path.
type pendingMatchStore struct {
	mu      sync.Mutex
	matches map[string]string // gcid → duel_id
}

// processMatch creates a Duel from a MatchResult and stores the duel_id
// for both users to pick up via heartbeat/status. Per spec §4.8 the duel
// is created directly as accepted (pool matching IS the acceptance) via
// NewPoolDuelWithID, then StartBattle transitions it to in_progress.
//
// The matchmaker has already atomically claimed the queue rows with
// match.MatchedDuelID — processMatch creates the Duel with that same ID.
//
// Fail-loud (§2.2/§2.3): errors are logged, and on failure both searchers
// are restored to the pool so they can be re-matched instead of being
// silently stranded in "matched" with no duel.
func (h *Handler) processMatch(ctx context.Context, match domainmm.MatchResult) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Repo calls below need the tenant for RLS — the background consumer
	// has no request context to inherit it from.
	ctx = tracing.WithTenantID(ctx, match.SearcherA.TenantID)
	// 60s timeout — the duel_atom_smith agent's LLM call (UMANS) takes
	// 6-30s to generate MCQ atoms. The previous 10s timeout fired before
	// the agent finished → "context deadline exceeded" → both searchers
	// restored to pool → infinite "Finding an opponent..." loop. This
	// runs on the background match consumer goroutine, not a request
	// handler, so a longer timeout is safe.
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	if h.deps.Duels == nil {
		log.Printf("matchmaking: processMatch — Duels repo nil, restoring searchers %s + %s",
			match.SearcherA.GCID, match.SearcherB.GCID)
		h.restoreSearchers(match)
		return
	}

	cfg := duel.DuelConfig{
		ELOBaseline:   h.deps.Rules.ELOBaseline,
		ELOKFactor:    h.deps.Rules.ELOKFactor,
		ComboTiers:    h.deps.Rules.ComboTiers,
		RoundTimerSec: h.deps.Rules.RoundTimerSec,
	}

	// WS4: use the matched question count (both searchers have the same
	// count — the matchmaker guarantees it). Fall back to 5 for safety.
	roundCount := match.SearcherA.QuestionCount
	if roundCount <= 0 {
		roundCount = 5
	}

	// Blitz: if both searchers are in blitz mode (the matchmaker guarantees
	// same-mode + same-variant), create a blitz duel instead of a classic
	// pool duel. The blitz config is derived from the shared variant +
	// the configured defaults.
	mode := match.SearcherA.Mode
	if mode == "" {
		mode = "classic"
	}

	if mode == "blitz" {
		blitzCfg := duel.BlitzConfig{Variant: duel.BlitzVariant(match.SearcherA.BlitzVariant)}
		switch blitzCfg.Variant {
		case duel.BlitzVariantTimed:
			blitzCfg.TimeLimitSec = h.deps.Rules.BlitzTimeLimitSec
		case duel.BlitzVariantRace:
			blitzCfg.RaceTarget = h.deps.Rules.BlitzRaceTarget
		}
		d, err := duel.NewBlitzPoolDuelWithID(cfg, match.SearcherA.GCID, match.SearcherB.GCID,
			match.SearcherA.TenantID, duel.ScopeRanked, roundCount, match.SharedTags,
			blitzCfg, match.MatchedDuelID)
		if err != nil {
			log.Printf("matchmaking: NewBlitzPoolDuelWithID failed for %s + %s: %v — restoring to pool",
				match.SearcherA.GCID, match.SearcherB.GCID, err)
			h.restoreSearchers(match)
			return
		}
		d.Category = match.SearcherA.Category
		if err := h.startBlitzBattle(ctx, d, match); err != nil {
			log.Printf("matchmaking: blitz StartBattle failed for %s + %s: %v — restoring to pool",
				match.SearcherA.GCID, match.SearcherB.GCID, err)
			h.restoreSearchers(match)
			return
		}
		if err := h.deps.Duels.SaveDuel(ctx, d); err != nil {
			log.Printf("matchmaking: SaveDuel failed for %s + %s: %v — restoring to pool",
				match.SearcherA.GCID, match.SearcherB.GCID, err)
			h.restoreSearchers(match)
			return
		}
		if h.deps.RoundSweeperRegistrar != nil {
			h.deps.RoundSweeperRegistrar.RegisterTenant(d.TenantID)
		}
		h.storePendingMatch(match.SearcherA.GCID, d.ID)
		h.storePendingMatch(match.SearcherB.GCID, d.ID)
		return
	}

	d, err := duel.NewPoolDuelWithID(cfg, match.SearcherA.GCID, match.SearcherB.GCID,
		match.SearcherA.TenantID, duel.ScopeRanked, roundCount, match.SharedTags, match.MatchedDuelID)
	if err != nil {
		log.Printf("matchmaking: NewPoolDuelWithID failed for %s + %s: %v — restoring to pool",
			match.SearcherA.GCID, match.SearcherB.GCID, err)
		h.restoreSearchers(match)
		return
	}
	// WS1: stamp the duel's category from the matched searcher (both
	// searchers have the same category — the matchmaker guarantees it).
	d.Category = match.SearcherA.Category

	// Select atoms via the duel_atom_smith agent (AI pick+generate).
	if h.deps.AtomSelector != nil {
		picks, err := h.deps.AtomSelector.SelectAtoms(ctx, AtomSelectionRequest{
			TenantID:      match.SearcherA.TenantID,
			ExcludeGCIDs:  []string{match.SearcherA.GCID, match.SearcherB.GCID},
			Tags:          match.SharedTags,
			Proficiencies: []int{match.SearcherA.Proficiency, match.SearcherB.Proficiency},
			Count:         d.RoundCount,
		})
		if err != nil {
			log.Printf("matchmaking: SelectAtoms failed for %s + %s: %v — restoring to pool",
				match.SearcherA.GCID, match.SearcherB.GCID, err)
			h.restoreSearchers(match)
			return
		}
		if len(picks) == 0 {
			log.Printf("matchmaking: SelectAtoms returned 0 picks for %s + %s — restoring to pool",
				match.SearcherA.GCID, match.SearcherB.GCID)
			h.restoreSearchers(match)
			return
		}
		if err := d.StartBattle(picks); err != nil {
			log.Printf("matchmaking: StartBattle failed for %s + %s: %v — restoring to pool",
				match.SearcherA.GCID, match.SearcherB.GCID, err)
			h.restoreSearchers(match)
			return
		}
	} else {
		log.Printf("matchmaking: AtomSelector nil for %s + %s — restoring to pool",
			match.SearcherA.GCID, match.SearcherB.GCID)
		h.restoreSearchers(match)
		return
	}

	if err := h.deps.Duels.SaveDuel(ctx, d); err != nil {
		log.Printf("matchmaking: SaveDuel failed for %s + %s: %v — restoring to pool",
			match.SearcherA.GCID, match.SearcherB.GCID, err)
		h.restoreSearchers(match)
		return
	}

	// WS3: register the tenant with the round-timer sweeper so it polls
	// this duel's rounds for expiry. The sweeper is per-tenant (RLS) —
	// without this registration the sweep never runs for this duel.
	if h.deps.RoundSweeperRegistrar != nil {
		h.deps.RoundSweeperRegistrar.RegisterTenant(d.TenantID)
	}

	// Store the duel_id for both users to pick up via heartbeat/status.
	h.storePendingMatch(match.SearcherA.GCID, d.ID)
	h.storePendingMatch(match.SearcherB.GCID, d.ID)
}

// startBlitzBattle selects atoms and starts a blitz duel's battle. Mirrors
// the classic path (SelectAtoms → StartBattle) but for a blitz duel — the
// only difference is the duel is a BlitzPoolDuel, so StartBattle stamps
// BlitzStartedAt instead of per-round deadlines.
func (h *Handler) startBlitzBattle(ctx context.Context, d *duel.Duel, match domainmm.MatchResult) error {
	if h.deps.AtomSelector == nil {
		log.Printf("matchmaking: AtomSelector nil for %s + %s — cannot start blitz",
			match.SearcherA.GCID, match.SearcherB.GCID)
		return fmt.Errorf("atom selector not configured")
	}
	picks, err := h.deps.AtomSelector.SelectAtoms(ctx, AtomSelectionRequest{
		TenantID:      match.SearcherA.TenantID,
		ExcludeGCIDs:  []string{match.SearcherA.GCID, match.SearcherB.GCID},
		Tags:          match.SharedTags,
		Proficiencies: []int{match.SearcherA.Proficiency, match.SearcherB.Proficiency},
		Count:         d.RoundCount,
	})
	if err != nil {
		return fmt.Errorf("select atoms: %w", err)
	}
	if len(picks) == 0 {
		return fmt.Errorf("no atoms available")
	}
	return d.StartBattle(picks)
}

// restoreSearchers reverts both claimed queue rows to finding so the
// matchmaker can re-pair the users (or pair them with others) instead of
// stranding them in "matched" with no duel (§2.3 compensating action).
// Every failure is logged loud — a failed revert leaves the user in
// 'matched' until the row is cancelled, which must be visible.
func (h *Handler) restoreSearchers(match domainmm.MatchResult) {	if h.deps.MatchmakingQueue == nil {
		return
	}
	ctx, cancel := context.WithTimeout(
		tracing.WithTenantID(context.Background(), match.SearcherA.TenantID), 5*time.Second)
	defer cancel()
	for _, gcid := range []string{match.SearcherA.GCID, match.SearcherB.GCID} {
		if err := h.deps.MatchmakingQueue.RevertToFinding(ctx, match.SearcherA.TenantID, gcid, match.MatchedDuelID); err != nil {
			log.Printf("matchmaking: restore %s to finding (duel %s) failed: %v — user may be stranded in matched",
				gcid, match.MatchedDuelID, err)
		}
	}
	// Re-register the tenant so the reverted rows are picked up promptly
	// even if the hint set was pruned while the pool looked empty.
	if h.deps.Matchmaker != nil {
		h.deps.Matchmaker.RegisterTenant(match.SearcherA.TenantID)
	}
}

func (h *Handler) storePendingMatch(gcid, duelID string) {
	h.pendingMatches.mu.Lock()
	defer h.pendingMatches.mu.Unlock()
	if h.pendingMatches.matches == nil {
		h.pendingMatches.matches = make(map[string]string)
	}
	h.pendingMatches.matches[gcid] = duelID
}

func (h *Handler) consumePendingMatch(gcid string) string {
	h.pendingMatches.mu.Lock()
	defer h.pendingMatches.mu.Unlock()
	duelID, ok := h.pendingMatches.matches[gcid]
	if ok {
		delete(h.pendingMatches.matches, gcid)
	}
	return duelID
}

// startMatchConsumer starts a background goroutine that consumes match
// results from the Matchmaker channel and creates duels. This is called
// once during handler construction. The consumer uses context.Background()
// (with per-match timeout set in processMatch) because matches fire on the
// matchmaker's background loop, not on any request goroutine.
func (h *Handler) startMatchConsumer() {
	if h.deps.Matchmaker == nil {
		return
	}
	go func() {
		for match := range h.deps.Matchmaker.Match() {
			h.processMatch(context.Background(), match)
		}
	}()
}
