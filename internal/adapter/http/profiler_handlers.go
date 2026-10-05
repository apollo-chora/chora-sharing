package httpadapter

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/domain/profiler"
)

type profilerResponse struct {
	GCID         string                 `json:"gcid"`
	Bio          string                 `json:"bio"`
	Tags         []profiler.InterestTag `json:"tags"`
	Proficiency  profiler.Proficiency   `json:"proficiency"`
	CourseTitles []string               `json:"course_titles,omitempty"`
	UpdatedAt    string                 `json:"updated_at"`
}

// generateResponse is the 202 Accepted body returned by the async
// generateProfile handler. The profile is the persisted "generating"
// state (bio saved, tags + proficiency empty) so the FE can render the
// bio immediately + show a generating spinner.
type generateResponse struct {
	Status  string          `json:"status"` // always "generating"
	Profile profilerResponse `json:"profile"`
}

func (h *Handler) getProfile(w http.ResponseWriter, r *http.Request) {
	if h.deps.Profiles == nil {
		writeErr(w, http.StatusNotImplemented, "profiler not wired")
		return
	}
	gcid := gcidFrom(r)

	p, err := h.deps.Profiles.GetProfile(r.Context(), gcid)
	if err != nil || p == nil {
		writeJSON(w, http.StatusOK, profilerResponse{
			GCID: gcid,
			Tags: []profiler.InterestTag{},
		})
		return
	}
	writeJSON(w, http.StatusOK, toProfilerResponse(p))
}

// generateProfile is the fire-and-forget entry point. It:
//  1. Validates the request + creates the Profile (bio + courses).
//  2. Persists the empty-tags "generating" state so the FE can render
//     the bio immediately + a generating spinner.
//  3. Spawns a goroutine that calls the conjurer agent (30-60s LLM call),
//     applies the result, saves the final profile, + publishes a
//     profile_ready (or profile_error) frame to the ProfileBroker.
//  4. Returns 202 Accepted immediately with the generating-state profile.
//
// The goroutine uses context.Background() — the request ctx dies the
// moment the handler returns. tenantID + gcid are captured in the
// closure + re-injected into a fresh context for the conjurer call +
// repo writes (they need the tracing context for RLS).
func (h *Handler) generateProfile(w http.ResponseWriter, r *http.Request) {
	if h.deps.Profiles == nil || h.deps.Conjurer == nil {
		writeErr(w, http.StatusNotImplemented, "profiler deps not wired")
		return
	}
	gcid := gcidFrom(r)
	tenantID := tenantFrom(r)

	var req struct {
		Bio          string   `json:"bio"`
		CourseTitles []string `json:"course_titles"`
	}
	if !readJSON(w, r, &req) {
		return
	}

	p, err := profiler.NewProfile(gcid, tenantID, req.Bio, req.CourseTitles)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	// Resolve display_name from chora-identity GetMe. Best-effort: if the
	// identity client is nil (dev/tests) or the call fails, display_name
	// stays empty — the FE falls back to shortGcid().
	if h.deps.DisplayNameResolver != nil {
		name, err := h.deps.DisplayNameResolver.ResolveDisplayName(r.Context(), gcid)
		if err != nil {
			log.Printf("profiler: GetMe display_name resolve failed gcid=%s err=%v", gcid, err)
		} else if name != "" {
			p.DisplayName = name
			log.Printf("profiler: display_name resolved gcid=%s name=%q", gcid, name)
		} else {
			log.Printf("profiler: GetMe returned empty display_name gcid=%s", gcid)
		}
	}

	// Persist the generating state. If this fails the FE can't show a
	// spinner against a saved bio anyway, so fail-loud (500) here
	// rather than spawn a goroutine that can't recover.
	if err := h.deps.Profiles.SaveProfile(r.Context(), p); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Fire the async generation. The goroutine captures tenant + gcid
	// (not the request ctx — it dies when the handler returns) and
	// re-injects them into a fresh context for the conjurer + repo
	// calls. Best-effort publish: if no WS subscriber is connected
	// (broker returns silently) the FE's 5s polling fallback still
	// recovers the final profile via GET /v1/me/profile.
	go h.generateProfileAsync(tenantID, gcid, req.Bio, req.CourseTitles)

	writeJSON(w, http.StatusAccepted, generateResponse{
		Status:  "generating",
		Profile: toProfilerResponse(p),
	})
}

// generateProfileAsync runs the 30-60s conjurer call + persists the
// result + notifies the WS subscriber. All errors are swallowed here
// (the HTTP response already returned 202) — failures are surfaced to
// the FE via the profile_error WS frame, not a retry.
func (h *Handler) generateProfileAsync(tenantID, gcid, bio string, courses []string) {
	// Fresh context — the request ctx is dead. Re-inject tenant + gcid
	// so the conjurer (gRPC to the agent) + repo writes carry the RLS
	// session.
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	ctx = tracing.WithGCID(ctx, gcid)

	result, err := h.deps.Conjurer.Conjure(ctx, tenantID, gcid, bio, courses)
	if err != nil {
		h.publishProfileError(gcid, "profile conjuring failed: "+err.Error())
		return
	}

	// Re-read the persisted profile so we carry forward the GCID +
	// CreatedAt from the generating-state write (NewProfile would
	// reset CreatedAt). Fall back to NewProfile if the row vanished
	// (e.g. DB reset between the 202 + the async completion).
	p, perr := h.deps.Profiles.GetProfile(ctx, gcid)
	if perr != nil || p == nil {
		p, err = profiler.NewProfile(gcid, tenantID, bio, courses)
		if err != nil {
			h.publishProfileError(gcid, "profile rebuild failed: "+err.Error())
			return
		}
	}
	p.SetTags(result.Tags)
	p.SetProficiency(result.Proficiency)

	if err := h.deps.Profiles.SaveProfile(ctx, p); err != nil {
		h.publishProfileError(gcid, "profile save failed: "+err.Error())
		return
	}

	h.publishProfileReady(gcid, toProfilerResponse(p))
}

// publishProfileReady marshals the final profile + publishes a
// profile_ready frame. Silent no-op if no broker wired (tests/dev
// without the WS path) or no subscriber connected (FE will poll-fall-
// back per the component's 5s recovery).
func (h *Handler) publishProfileReady(gcid string, resp profilerResponse) {
	if h.deps.ProfileBroker == nil {
		return
	}
	payload, err := json.Marshal(resp)
	if err != nil {
		return
	}
	h.deps.ProfileBroker.PublishProfile(gcid, ProfileKindReady, payload)
}

// publishProfileError publishes a profile_error frame with the failure
// message so the FE can surface it instead of hanging on "generating".
func (h *Handler) publishProfileError(gcid, message string) {
	if h.deps.ProfileBroker == nil {
		return
	}
	payload, _ := json.Marshal(map[string]string{"error": message})
	h.deps.ProfileBroker.PublishProfile(gcid, ProfileKindError, payload)
}

// profileWS delegates to deps.ProfileWSHandler (the ws.ProfileHandler)
// when wired, else 501 — fail-loud per §1.1. Mirrors duelWS in
// duel_handlers.go:169.
func (h *Handler) profileWS(w http.ResponseWriter, r *http.Request) {
	if h.deps.ProfileWSHandler == nil {
		writeErr(w, http.StatusNotImplemented, "profile WebSocket handler not wired")
		return
	}
	h.deps.ProfileWSHandler.ServeHTTP(w, r)
}

func (h *Handler) updateProfileTags(w http.ResponseWriter, r *http.Request) {
	if h.deps.Profiles == nil {
		writeErr(w, http.StatusNotImplemented, "profiler not wired")
		return
	}
	gcid := gcidFrom(r)
	tenantID := tenantFrom(r)

	p, err := h.deps.Profiles.GetProfile(r.Context(), gcid)
	if err != nil || p == nil {
		p, err = profiler.NewProfile(gcid, tenantID, "", nil)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	var req struct {
		Tags []profiler.InterestTag `json:"tags"`
	}
	if !readJSON(w, r, &req) {
		return
	}

	p.SetTags(req.Tags)

	if err := h.deps.Profiles.SaveProfile(r.Context(), p); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, toProfilerResponse(p))
}

// toProfilerResponse maps the domain Profile to its JSON shape. Tags
// is NEVER nil — the agent's taxonomy is constrained (6 categories,
// ~30 tags) so a bio about "golang" (no matching tag) legitimately
// yields an empty tag set; serialising that as `null` breaks the FE
// ([null] vs []) + the FE's tagCount() === 0 check. We coerce nil → []
// here as a defense-in-depth (NewProfile also initialises Tags to an
// empty slice, but a profile loaded from a DB row with NULL tags
// would still be nil without this guard).
func toProfilerResponse(p *profiler.Profile) profilerResponse {
	tags := p.Tags
	if tags == nil {
		tags = []profiler.InterestTag{}
	}
	courses := p.CourseTitles
	if courses == nil {
		courses = []string{}
	}
	return profilerResponse{
		GCID:         p.GCID,
		Bio:          p.Bio,
		Tags:         tags,
		Proficiency:  p.Proficiency,
		CourseTitles: courses,
		UpdatedAt:    p.UpdatedAt.UTC().Format("2006-01-02T15:04:05.000000Z"),
	}
}
