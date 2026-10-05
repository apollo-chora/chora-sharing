// milestone_drafts_handlers.go — the owner-facing C+ Familiar-milestone surface
// (CHO-2258, parent CHO-1889; completes the CHO-2203 lane).
//
// CHO-2203 made the milestone lane durable: a Familiar stage-up / hatch / breed
// reveal now lands a real post_drafts row instead of a map that died with the
// pod. What it did not build is the half a learner touches. The default policy
// is `draft`, and nothing could read a draft back — every milestone since PROD-G
// queued for an audience of nobody. Nor could anyone leave that default:
// PreferenceStore.Upsert had no caller, so `user_preferences` could never
// acquire a row and `auto` was unreachable by construction.
//
// Five routes close it:
//
//	GET  /v1/me/post-drafts                          — what is waiting for me
//	POST /v1/me/post-drafts/{draft_id}/publish       — share it
//	POST /v1/me/post-drafts/{draft_id}/discard       — bin it (soft-delete)
//	GET  /v1/me/preferences/familiar-milestone-share — how am I set
//	POST /v1/me/preferences/familiar-milestone-share — auto | draft | suppress
//
// ⚠ The paths are NOT free. The Istio AuthorizationPolicy on ns/sharing is a
// strict per-path allowlist with no wildcard allow-all, and it ALREADY carries
// /v1/me/post-drafts, /v1/me/post-drafts/* and
// /v1/me/preferences/familiar-milestone-share (live-verified 2026-07-17 —
// they predate this code). Serving these operations anywhere else mesh-403s at
// the edge while every unit test still passes.
//
// ⚠ Methods are NOT free either. Cloud Armor rule 1005 (OWASP
// methodenforcement) denies PUT/PATCH/DELETE at the edge, and the rule-994
// bypass allowlist is AT the CEL 5-sub-expression cap on a policy at its 21-rule
// cap. So discard is POST .../discard rather than DELETE, and the preference
// write is POST rather than PUT. This costs nothing in expressiveness here:
// publish and discard are the two transitions of the draft state machine and
// read symmetrically, exactly like POST /v1/duels/{id}/accept next door.
package httpadapter

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/adapter/subscribers"
)

// =============================================================================
// Ports
// =============================================================================

// MilestoneDrafts is the owner-facing post_drafts port: what the LEARNER does
// with a queued milestone. Every operation is scoped by (tenant, gcid) because
// RLS scopes the TENANT only — without the gcid, one learner reaches another
// learner's drafts inside the same tenant.
//
// The subscriber's write side is a separate, narrower port
// (subscribers.DraftStore = Insert). Same pg type behind both; different
// consumers, different needs.
type MilestoneDrafts interface {
	ListPending(ctx context.Context, tenantID, gcid string) ([]subscribers.Draft, error)
	PublishDraft(ctx context.Context, tenantID, gcid, draftID string) (postID string, err error)
	Discard(ctx context.Context, tenantID, gcid, draftID string) error
}

// MilestonePrefs is the user_preferences port for the Familiar-milestone share
// policy. Get is already live — the subscriber reads it to route every
// milestone. Upsert is the half that had no caller, which is why no learner
// could ever be anything other than the default.
type MilestonePrefs interface {
	Get(ctx context.Context, tenantID, gcid string) (subscribers.Policy, bool, error)
	Upsert(ctx context.Context, tenantID, gcid string, p subscribers.Policy) error
}

// =============================================================================
// DTOs
// =============================================================================

// postDraftJSON is a queued milestone as the owner sees it.
type postDraftJSON struct {
	DraftID           string                 `json:"draft_id"`
	Body              string                 `json:"body"`
	ComposedFromTopic string                 `json:"composed_from_topic"`
	Metadata          map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt         string                 `json:"created_at"`
}

type listPostDraftsResponse struct {
	Drafts []postDraftJSON `json:"drafts"`
}

type publishPostDraftResponse struct {
	DraftID string `json:"draft_id"`
	PostID  string `json:"post_id"`
}

// sharePrefResponse carries the policy IN FORCE. IsDefault distinguishes "the
// learner chose draft" from "the learner has never chosen and the subscriber's
// fallback is draft" — the two are indistinguishable in the value alone, and a
// UI that cannot tell them apart shows a toggle the learner never set.
type sharePrefResponse struct {
	Policy    string `json:"policy"`
	IsDefault bool   `json:"is_default"`
}

type setSharePrefRequest struct {
	Policy string `json:"policy"`
}

// =============================================================================
// Handlers
// =============================================================================

// listPostDrafts serves GET /v1/me/post-drafts.
func (h *Handler) listPostDrafts(w http.ResponseWriter, r *http.Request) {
	if h.deps.MilestoneDrafts == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("MilestoneDrafts"))
		return
	}
	// Scope comes from the gateway-stamped identity, never from a param.
	drafts, err := h.deps.MilestoneDrafts.ListPending(r.Context(), tenantFrom(r), gcidFrom(r))
	if err != nil {
		// A failed read must never be shaped as "you have no milestones".
		writeErr(w, http.StatusInternalServerError, "list post drafts: "+err.Error())
		return
	}
	out := make([]postDraftJSON, 0, len(drafts))
	for _, d := range drafts {
		out = append(out, postDraftJSON{
			DraftID:           d.DraftID,
			Body:              d.Body,
			ComposedFromTopic: d.ComposedFromTopic,
			Metadata:          d.Metadata,
			CreatedAt:         d.CreatedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	writeJSON(w, http.StatusOK, listPostDraftsResponse{Drafts: out})
}

// publishPostDraft serves POST /v1/me/post-drafts/{draft_id}/publish.
//
// The store does the post insert, the outbox emit and the status flip in ONE
// transaction, so there is no window where a published post leaves its draft
// pending (which would let the learner publish the same milestone twice).
func (h *Handler) publishPostDraft(w http.ResponseWriter, r *http.Request) {
	if h.deps.MilestoneDrafts == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("MilestoneDrafts"))
		return
	}
	draftID := strings.TrimSpace(r.PathValue("draft_id"))
	if draftID == "" {
		writeErr(w, http.StatusBadRequest, "draft_id path segment required")
		return
	}
	postID, err := h.deps.MilestoneDrafts.PublishDraft(r.Context(), tenantFrom(r), gcidFrom(r), draftID)
	if err != nil {
		writeDraftErr(w, err, "publish post draft")
		return
	}
	w.Header().Set("Location", "/v1/posts/"+postID)
	writeJSON(w, http.StatusCreated, publishPostDraftResponse{DraftID: draftID, PostID: postID})
}

// discardPostDraft serves POST /v1/me/post-drafts/{draft_id}/discard.
// Discard is a SOFT-delete — the draft is user-owned content (ddd-enforcement #6).
func (h *Handler) discardPostDraft(w http.ResponseWriter, r *http.Request) {
	if h.deps.MilestoneDrafts == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("MilestoneDrafts"))
		return
	}
	draftID := strings.TrimSpace(r.PathValue("draft_id"))
	if draftID == "" {
		writeErr(w, http.StatusBadRequest, "draft_id path segment required")
		return
	}
	if err := h.deps.MilestoneDrafts.Discard(r.Context(), tenantFrom(r), gcidFrom(r), draftID); err != nil {
		writeDraftErr(w, err, "discard post draft")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// getSharePref serves GET /v1/me/preferences/familiar-milestone-share.
func (h *Handler) getSharePref(w http.ResponseWriter, r *http.Request) {
	if h.deps.MilestonePrefs == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("MilestonePrefs"))
		return
	}
	pol, found, err := h.deps.MilestonePrefs.Get(r.Context(), tenantFrom(r), gcidFrom(r))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "get share preference: "+err.Error())
		return
	}
	if !found {
		// No row is the NORMAL state: user_preferences starts empty and the
		// subscriber applies its DefaultPolicy. Report the policy actually IN
		// FORCE, from the same value the composition root gives the subscriber
		// — a second hardcoded default here would drift the moment one changed.
		writeJSON(w, http.StatusOK, sharePrefResponse{
			Policy:    string(h.milestoneDefaultPolicy()),
			IsDefault: true,
		})
		return
	}
	writeJSON(w, http.StatusOK, sharePrefResponse{Policy: string(pol)})
}

// setSharePref serves POST /v1/me/preferences/familiar-milestone-share.
//
// POST, not PUT: Cloud Armor denies PUT/PATCH at the edge (see the file header).
// The operation is an upsert on the natural key (tenant_id, gcid), so it is
// idempotent in effect regardless of the verb.
func (h *Handler) setSharePref(w http.ResponseWriter, r *http.Request) {
	if h.deps.MilestonePrefs == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("MilestonePrefs"))
		return
	}
	var body setSharePrefRequest
	if !readJSON(w, r, &body) {
		return
	}
	// Validate at the boundary against the canonical set rather than letting
	// Postgres reject an unknown enum label as a 500-shaped fault.
	pol, err := subscribers.ParsePolicy(body.Policy)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.deps.MilestonePrefs.Upsert(r.Context(), tenantFrom(r), gcidFrom(r), pol); err != nil {
		writeErr(w, http.StatusInternalServerError, "set share preference: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, sharePrefResponse{Policy: string(pol)})
}

// =============================================================================
// Helpers
// =============================================================================

// milestoneDefaultPolicy is the policy in force for a learner with no
// user_preferences row. It mirrors FamiliarMilestoneSubscriber's own empty ->
// PolicyDraft defaulting so the two cannot disagree about what "unset" means.
func (h *Handler) milestoneDefaultPolicy() subscribers.Policy {
	if h.deps.MilestoneDefaultPolicy == "" {
		return subscribers.PolicyDraft
	}
	return h.deps.MilestoneDefaultPolicy
}

// writeDraftErr maps a draft mutation error to its status.
//
// ErrDraftNotPending -> 404 for all of "no such draft" / "not yours" / "already
// published": collapsing them is deliberate, since a distinct 403 would confirm
// the existence of another learner's draft. Everything else is OUR fault and
// must surface as a 5xx — a backing-store fault dressed as a 404 tells the
// learner their milestone never existed.
func writeDraftErr(w http.ResponseWriter, err error, op string) {
	if errors.Is(err, subscribers.ErrDraftNotPending) {
		writeErr(w, http.StatusNotFound, "no pending draft with that id")
		return
	}
	writeErr(w, http.StatusInternalServerError, op+": "+err.Error())
}
