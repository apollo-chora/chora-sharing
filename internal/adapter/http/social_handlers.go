package httpadapter

import (
	"errors"
	"net/http"
	"strings"

	"github.com/apollo-chora/chora-sharing/internal/domain/comment"
	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
	"github.com/apollo-chora/chora-sharing/internal/domain/reaction"
)

// =============================================================================
// Social surface (§6): reactions, comments, leaderboard
// =============================================================================

// ---------------------------------------------------------------------------
// Reactions
// ---------------------------------------------------------------------------

// reactToPostRequest is the POST /v1/posts/{post_id}/reactions body. The
// post_id comes from the path; reactor_gcid + tenant_id from identity headers.
type reactToPostRequest struct {
	Kind string `json:"kind"`
}

// reactionJSON is the reaction DTO mirrored on the wire.
type reactionJSON struct {
	ReactionID  string `json:"reaction_id"`
	PostID      string `json:"post_id"`
	ReactorGCID string `json:"reactor_gcid"`
	TenantID    string `json:"tenant_id"`
	Kind        string `json:"kind"`
	ReactedAt   string `json:"reacted_at"`
}

// reactToPostResponse carries the user's active reaction (null when toggled
// off) + the updated per-type counts so the frontend can refresh the UI
// without a separate fetch.
type reactToPostResponse struct {
	Reaction       *reactionJSON    `json:"reaction"`
	ReactionCounts map[string]int32 `json:"reaction_counts"`
}

// reactToPost — POST /v1/posts/{post_id}/reactions.
//
// Toggle semantics (backend-owned, not frontend):
//   - No existing reaction by this user → add it.
//   - Same kind already active → remove it (toggle off).
//   - Different kind active → remove old, add new (switch).
//
// Returns the updated per-type counts + the user's active reaction (null
// when toggled off). The frontend just POSTs and reads the response — no
// client-side toggle logic, no separate DELETE call.
func (h *Handler) reactToPost(w http.ResponseWriter, r *http.Request) {
	if h.deps.Reactions == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("Reactions"))
		return
	}
	postID := strings.TrimSpace(r.PathValue("post_id"))
	if postID == "" {
		writeErr(w, http.StatusBadRequest, "post_id path segment required")
		return
	}
	reactor := gcidFrom(r)
	tenant := tenantFrom(r)

	// Existence check — cross-tenant returns 404 (existence never leaks).
	// First check PostStore (social posts), then fall back to ShareRepo
	// (shared-atom feed entries) so reactions work on feed cards too.
	if h.deps.Posts != nil {
		p, ok, err := h.deps.Posts.Get(r.Context(), postID)
		if err != nil {
			// A guard read that FAILS must not fall through to the next branch.
			// Before CHO-2193/W0-F1 this lookup could not return an error, so a
			// broken read was indistinguishable from "absent" and the existence
			// check silently failed OPEN (CHO-2184's two guard reads, again).
			writeErr(w, http.StatusInternalServerError, "post lookup failed")
			return
		}
		if ok {
			if p.TenantID != tenant {
				writeErr(w, http.StatusNotFound, "post not found")
				return
			}
		} else {
			if h.deps.Shares == nil {
				writeErr(w, http.StatusNotFound, "post not found")
				return
			}
			sh, err := h.deps.Shares.GetShare(r.Context(), postID)
			if err != nil || sh == nil {
				writeErr(w, http.StatusNotFound, "post not found")
				return
			}
		}
	} else {
		if h.deps.Shares == nil {
			writeErr(w, http.StatusNotFound, "post not found")
			return
		}
		sh, err := h.deps.Shares.GetShare(r.Context(), postID)
		if err != nil || sh == nil {
			writeErr(w, http.StatusNotFound, "post not found")
			return
		}
	}

	var body reactToPostRequest
	if !readJSON(w, r, &body) {
		return
	}
	rt := reactionKindStringToDomain(body.Kind)
	if rt == "" {
		writeErr(w, http.StatusBadRequest, "unknown reaction kind")
		return
	}

	// Find the user's existing reaction for this post.
	existing, err := h.deps.Reactions.ListByPost(r.Context(), postID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list reactions failed")
		return
	}
	var userReaction *reaction.Reaction
	for _, rx := range existing {
		if rx.GCID == reactor {
			userReaction = rx
			break
		}
	}

	// Toggle logic:
	//   - Same kind → remove (toggle off).
	//   - Different kind → remove old, add new.
	//   - None → add new.
	//
	// Both Unreact calls previously DISCARDED their result entirely. With the
	// in-memory registry that was merely sloppy; against Postgres a failed
	// DELETE would leave the old reaction in place while we told the caller the
	// toggle succeeded — and on the switch path, would leave the user holding
	// TWO reactions. The error is now handled (CHO-2193/W0-F1).
	var activeReaction *reaction.Reaction
	if userReaction != nil {
		if userReaction.Type == rt {
			// Toggle off.
			if _, err := h.deps.Reactions.Unreact(r.Context(), reactor, postID, rt); err != nil {
				writeErr(w, http.StatusInternalServerError, "unreact: "+err.Error())
				return
			}
			activeReaction = nil
		} else {
			// Switch: remove old, add new.
			if _, err := h.deps.Reactions.Unreact(r.Context(), reactor, postID, userReaction.Type); err != nil {
				writeErr(w, http.StatusInternalServerError, "unreact: "+err.Error())
				return
			}
			activeReaction, _, err = h.deps.Reactions.React(r.Context(), tenant, reactor, postID, rt)
			if err != nil {
				writeErr(w, http.StatusInternalServerError, "react: "+err.Error())
				return
			}
		}
	} else {
		activeReaction, _, err = h.deps.Reactions.React(r.Context(), tenant, reactor, postID, rt)
		if err != nil {
			if errors.Is(err, reaction.ErrInvalidArgument) {
				writeErr(w, http.StatusBadRequest, err.Error())
				return
			}
			writeErr(w, http.StatusInternalServerError, "react: "+err.Error())
			return
		}
	}

	// Build per-type counts from the current state.
	rxs, err := h.deps.Reactions.ListByPost(r.Context(), postID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list reactions failed")
		return
	}
	counts := make(map[string]int32, 4)
	for _, rx := range rxs {
		counts[string(rx.Type)]++
	}

	resp := reactToPostResponse{
		ReactionCounts: counts,
	}
	if activeReaction != nil {
		resp.Reaction = &reactionJSON{
			ReactionID:  activeReaction.ID,
			PostID:      activeReaction.PostID,
			ReactorGCID: activeReaction.GCID,
			TenantID:    activeReaction.TenantID,
			Kind:        domainReactionKindToString(activeReaction.Type),
			ReactedAt:   activeReaction.CreatedAt.UTC().Format("2006-01-02T15:04:05.000000Z"),
		}
		w.Header().Set("Location", "/v1/posts/"+postID+"/reactions/"+activeReaction.ID)
		writeJSON(w, http.StatusCreated, resp)
	} else {
		writeJSON(w, http.StatusOK, resp)
	}
}

// removeReaction — DELETE /v1/posts/{post_id}/reactions/{reaction_id}.
//
// Ownership-checked removal (§10.6): the reaction is removed only if the
// caller (gcid) owns it. Not-owned / not-found → 404 (existence never leaks).
// Idempotent — a repeat DELETE on an already-removed reaction returns 204.
func (h *Handler) removeReaction(w http.ResponseWriter, r *http.Request) {
	if h.deps.Reactions == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("Reactions"))
		return
	}
	reactionID := strings.TrimSpace(r.PathValue("reaction_id"))
	if reactionID == "" {
		writeErr(w, http.StatusBadRequest, "reaction_id path segment required")
		return
	}
	caller := gcidFrom(r)

	removed, err := h.deps.Reactions.UnreactByID(r.Context(), reactionID, caller)
	if err != nil {
		// UnreactByID returns ErrInvalidArgument when the reaction doesn't
		// exist OR the caller is not the owner — both map to 404 so existence
		// never leaks (§6: cross-tenant → 404).
		if errors.Is(err, reaction.ErrInvalidArgument) {
			writeErr(w, http.StatusNotFound, "reaction not found")
			return
		}
		writeErr(w, http.StatusInternalServerError, "remove reaction: "+err.Error())
		return
	}
	_ = removed // idempotent — 204 whether or not it was present this call
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Comments
// ---------------------------------------------------------------------------

// listCommentsResponse is the GET /v1/posts/{post_id}/comments payload.
type listCommentsResponse struct {
	Data       []commentJSON `json:"data"`
	NextCursor string        `json:"next_cursor"`
}

// listComments — GET /v1/posts/{post_id}/comments.
//
// Lists comments for a post with cursor pagination. Query params: limit
// (default 20, max 100), cursor. Returns a JSON array under "data" with a
// "next_cursor" field for pagination.
func (h *Handler) listComments(w http.ResponseWriter, r *http.Request) {
	if h.deps.Comments == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("Comments"))
		return
	}
	postID := strings.TrimSpace(r.PathValue("post_id"))
	if postID == "" {
		writeErr(w, http.StatusBadRequest, "post_id path segment required")
		return
	}
	limit := parseLimit(r.URL.Query().Get("limit"), 20, 100)
	cursor := r.URL.Query().Get("cursor")

	comments, nextCursor, err := h.deps.Comments.ListByPost(r.Context(), postID, limit, cursor)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list comments: "+err.Error())
		return
	}
	out := make([]commentJSON, 0, len(comments))
	for _, c := range comments {
		out = append(out, commentJSON{
			ID:              c.ID,
			PostID:          c.PostID,
			AuthorGCID:      c.AuthorGCID,
			Body:            c.Body,
			ParentCommentID: c.ParentCommentID,
			CreatedAt:       c.CreatedAt.UTC().Format("2006-01-02T15:04:05.000000Z"),
		})
	}
	writeJSON(w, http.StatusOK, listCommentsResponse{
		Data:       out,
		NextCursor: nextCursor,
	})
}

// createCommentRequest is the POST /v1/posts/{post_id}/comments body. The
// post_id comes from the path; author_gcid + tenant_id from identity headers.
type createCommentRequest struct {
	Body            string `json:"body"`
	ParentCommentID string `json:"parent_comment_id,omitempty"`
}

// commentJSON is the comment DTO mirrored on the wire.
type commentJSON struct {
	ID              string `json:"id"`
	PostID          string `json:"post_id"`
	AuthorGCID      string `json:"author_gcid"`
	Body            string `json:"body"`
	ParentCommentID string `json:"parent_comment_id,omitempty"`
	CreatedAt       string `json:"created_at"`
}

// createComment — POST /v1/posts/{post_id}/comments.
//
// Creates a 1-level reply on a post. The post MUST exist (else 404). The
// domain NewComment validates body + parent format; the adapter enforces the
// post existence + tenant scope. Idempotency-Key is required on this write.
func (h *Handler) createComment(w http.ResponseWriter, r *http.Request) {
	if h.deps.Comments == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("Comments"))
		return
	}
	if h.deps.Posts == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("Posts"))
		return
	}
	postID := strings.TrimSpace(r.PathValue("post_id"))
	if postID == "" {
		writeErr(w, http.StatusBadRequest, "post_id path segment required")
		return
	}
	if idempotencyKey(r) == "" {
		writeErr(w, http.StatusBadRequest, "Idempotency-Key header required")
		return
	}
	author := gcidFrom(r)
	tenant := tenantFrom(r)

	// Existence check — cross-tenant returns 404.
	p, ok, err := h.deps.Posts.Get(r.Context(), postID)
	if err != nil {
		// OUR fault (500), not a missing post (404). A failed read must never
		// be reported to the caller as an absence.
		writeErr(w, http.StatusInternalServerError, "post lookup failed")
		return
	}
	if !ok {
		writeErr(w, http.StatusNotFound, "post not found")
		return
	}
	if p.TenantID != tenant {
		writeErr(w, http.StatusNotFound, "post not found")
		return
	}

	var body createCommentRequest
	if !readJSON(w, r, &body) {
		return
	}

	c, err := comment.NewComment(tenant, postID, author, body.Body, body.ParentCommentID)
	if err != nil {
		if errors.Is(err, comment.ErrInvalidArgument) {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		writeErr(w, http.StatusInternalServerError, "new comment: "+err.Error())
		return
	}
	if err := h.deps.Comments.Create(r.Context(), c); err != nil {
		writeErr(w, http.StatusInternalServerError, "create comment: "+err.Error())
		return
	}
	w.Header().Set("Location", "/v1/posts/"+c.PostID+"/comments/"+c.ID)
	writeJSON(w, http.StatusCreated, commentJSON{
		ID:              c.ID,
		PostID:          c.PostID,
		AuthorGCID:      c.AuthorGCID,
		Body:            c.Body,
		ParentCommentID: c.ParentCommentID,
		CreatedAt:       c.CreatedAt.UTC().Format("2006-01-02T15:04:05.000000Z"),
	})
}

// updateCommentRequest is the PATCH /v1/posts/{post_id}/comments/{comment_id} body.
type updateCommentRequest struct {
	Body string `json:"body"`
}

// updateComment — PATCH /v1/posts/{post_id}/comments/{comment_id}.
//
// Updates the body of a comment. Only the author can update — the repo
// enforces ownership. Returns 404 when the comment doesn't exist or the
// caller is not the author (existence never leaks).
func (h *Handler) updateComment(w http.ResponseWriter, r *http.Request) {
	if h.deps.Comments == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("Comments"))
		return
	}
	commentID := strings.TrimSpace(r.PathValue("comment_id"))
	if commentID == "" {
		writeErr(w, http.StatusBadRequest, "comment_id path segment required")
		return
	}
	author := gcidFrom(r)

	var body updateCommentRequest
	if !readJSON(w, r, &body) {
		return
	}

	if err := h.deps.Comments.Update(r.Context(), commentID, author, body.Body); err != nil {
		if errors.Is(err, comment.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "comment not found")
			return
		}
		if errors.Is(err, comment.ErrInvalidArgument) {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		writeErr(w, http.StatusInternalServerError, "update comment: "+err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// deleteComment — DELETE /v1/posts/{post_id}/comments/{comment_id}.
//
// Deletes a comment. Only the author can delete — the repo enforces ownership.
// Returns 404 when the comment doesn't exist or the caller is not the author
// (existence never leaks). Idempotent — a repeat DELETE returns 204.
func (h *Handler) deleteComment(w http.ResponseWriter, r *http.Request) {
	if h.deps.Comments == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("Comments"))
		return
	}
	commentID := strings.TrimSpace(r.PathValue("comment_id"))
	if commentID == "" {
		writeErr(w, http.StatusBadRequest, "comment_id path segment required")
		return
	}
	author := gcidFrom(r)

	if err := h.deps.Comments.Delete(r.Context(), commentID, author); err != nil {
		if errors.Is(err, comment.ErrNotFound) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeErr(w, http.StatusInternalServerError, "delete comment: "+err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Leaderboard
// ---------------------------------------------------------------------------

// leaderboardEntryJSON is a single ranked row on the wire.
type leaderboardEntryJSON struct {
	GCID        string `json:"gcid"`
	DisplayName string `json:"display_name"`
	Score       int64  `json:"score"`
	Rank        int32  `json:"rank"`
}

// getLeaderboardResponse is the GET /v1/leaderboard payload.
type getLeaderboardResponse struct {
	Entries    []leaderboardEntryJSON `json:"entries"`
	NextCursor string                 `json:"next_cursor,omitempty"`
	ComputedAt string                 `json:"computed_at"`
}

// getLeaderboard — GET /v1/leaderboard.
//
// Ranked board (§3.6, §6). Query params: scope (global|tenant|class|course),
// scope_target_id (for class/course), metric (xp|duel_wins|duel_elo|...),
// period (weekly|monthly|all-time), limit (default 20, max 100).
// Ties share a rank; tie-break gcid ASC. Display name is pseudonymised at
// the read-model layer (FR-029/SC-011).
func (h *Handler) getLeaderboard(w http.ResponseWriter, r *http.Request) {
	if h.deps.Leaderboards == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("Leaderboards"))
		return
	}
	q := r.URL.Query()
	tenant := tenantFrom(r)
	scopeKind, scopeID := leaderboardScopeFromQuery(
		q.Get("scope"), strings.TrimSpace(q.Get("scope_target_id")),
	)
	if scopeKind == leaderboard.ScopeTenant && tenant == "" {
		writeErr(w, http.StatusBadRequest, "tenant required for tenant scope")
		return
	}
	if scopeKind == leaderboard.ScopeCohort && scopeID == "" {
		writeErr(w, http.StatusBadRequest, "scope_target_id required for class/course scope")
		return
	}
	_ = leaderboardMetricFromQuery(q.Get("metric")) // tracked by pg reader; inmem returns xp
	limit := parseLimit(q.Get("limit"), 20, 100)
	period := leaderboardPeriodFromQuery(q.Get("period"))

	entries, err := h.deps.Leaderboards.ReadTop(r.Context(), scopeKind, scopeID, tenant, period, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "leaderboard read: "+err.Error())
		return
	}
	// Batch-resolve display names from profiler_profiles (mirrors the
	// duels TopRatings LEFT JOIN profiler_profiles pattern). Per-entry
	// resolveDisplayName reads social_feed_entries (only users who shared
	// atoms have rows) — the profiler_profiles join covers all users with
	// a saved interest profile.
	names := map[string]string{}
	if h.deps.Profiles != nil && len(entries) > 0 {
		gcids := make([]string, len(entries))
		for i, e := range entries {
			gcids[i] = e.GCID
		}
		names, _ = h.deps.Profiles.ResolveDisplayNames(r.Context(), gcids)
	}
	out := make([]leaderboardEntryJSON, 0, len(entries))
	for _, e := range entries {
		out = append(out, leaderboardEntryJSON{
			GCID:        e.GCID,
			DisplayName: names[e.GCID],
			Score:       int64(e.Score),
			Rank:        int32(e.Rank),
		})
	}
	writeJSON(w, http.StatusOK, getLeaderboardResponse{
		Entries:    out,
		ComputedAt: nowUTC().Format("2006-01-02T15:04:05.000000Z"),
	})
}
