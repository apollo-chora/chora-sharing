package httpadapter

import (
	"errors"
	"net/http"
	"strings"

	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

// relationshipErrToStatus maps relationship domain errors to HTTP statuses.
// ErrConnectionNotPermitted passes through VERBATIM — the unspecific message
// is the privacy contract (never disclose block existence or direction).
func relationshipErrToStatus(err error) (int, string) {
	switch {
	case errors.Is(err, social.ErrConnectionNotPermitted):
		return http.StatusConflict, social.ErrConnectionNotPermitted.Error()
	case errors.Is(err, social.ErrSelfFollow),
		errors.Is(err, social.ErrSelfBlock),
		errors.Is(err, social.ErrInvalidArgument):
		return http.StatusBadRequest, err.Error()
	default:
		return http.StatusInternalServerError, "relationship: " + err.Error()
	}
}
// =============================================================================
// Connections surface (following / followers / blocked)
// =============================================================================

// connectionJSON is the wire representation of a connection edge.
type connectionJSON struct {
	GCID        string `json:"gcid"`
	DisplayName string `json:"display_name,omitempty"`
	CreatedAt   string `json:"created_at"`
}

// listConnectionsResponse is the GET /v1/connections payload.
type listConnectionsResponse struct {
	Connections []connectionJSON `json:"connections"`
	NextCursor  string           `json:"next_cursor"`
}

// listConnections — GET /v1/connections?type=following|followers|blocked.
//
// Lists the caller's social connections. type=following (people the caller
// follows), type=followers (people who follow the caller), type=blocked
// (people the caller has blocked — visible to the blocker ONLY, ADR-230 D8).
// Default type=following. Reads are served from the durable, RLS-scoped
// graph (ADR-229 WS-0) under the caller's tenant context. Per §1.1, a nil
// dep returns 501 and a graph read error returns 500 (fail-loud — never a
// fake success).
func (h *Handler) listConnections(w http.ResponseWriter, r *http.Request) {
	gcid := gcidFrom(r)
	tenant := tenantFrom(r)
	connType := strings.TrimSpace(r.URL.Query().Get("type"))
	if connType == "" {
		connType = "following"
	}

	if h.deps.Graph == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("Graph"))
		return
	}

	var (
		gcids []string
		err   error
	)
	switch connType {
	case "following":
		gcids, err = h.deps.Graph.FollowingGCIDs(r.Context(), tenant, gcid)
	case "followers":
		gcids, err = h.deps.Graph.FollowersGCIDs(r.Context(), tenant, gcid)
	case "blocked":
		gcids, err = h.deps.Graph.BlockedBy(r.Context(), tenant, gcid)
	default:
		writeErr(w, http.StatusBadRequest, "invalid type: must be following, followers, or blocked")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list connections: "+err.Error())
		return
	}

	// Batch-resolve display names from profiler_profiles (not
	// social_feed_entries which only has rows for users who shared atoms).
	// Mirrors the leaderboard + duels TopRatings pattern.
	names := map[string]string{}
	if h.deps.Profiles != nil && len(gcids) > 0 {
		names, _ = h.deps.Profiles.ResolveDisplayNames(r.Context(), gcids)
	}
	out := make([]connectionJSON, 0, len(gcids))
	for _, g := range gcids {
		out = append(out, connectionJSON{
			GCID:        g,
			DisplayName: names[g],
		})
	}
	writeJSON(w, http.StatusOK, listConnectionsResponse{
		Connections: out,
		NextCursor:  "",
	})
}

// =============================================================================
// Relationship writes (follow / block planes)
//
// The caller is ALWAYS the identity-header GCID; the other member of the
// pair comes from the body (POST) or path (DELETE). Refusals map via
// relationshipErrToStatus; the blocked-pair refusal stays deliberately
// unspecific (ADR-230 D1).
// =============================================================================

// relationshipTargetRequest is the shared POST body: the other member.
type relationshipTargetRequest struct {
	GCID string `json:"gcid"`
}

// followResponse mirrors the follow write result.
type followResponse struct {
	GCID      string `json:"gcid"`
	Created   bool   `json:"created"`
	CreatedAt string `json:"created_at"`
}

const wireTimeLayout = "2006-01-02T15:04:05.000000Z"

// targetFromBody decodes + validates the shared {gcid} POST body.
func targetFromBody(w http.ResponseWriter, r *http.Request) (string, bool) {
	var body relationshipTargetRequest
	if !readJSON(w, r, &body) {
		return "", false
	}
	target := strings.TrimSpace(body.GCID)
	if target == "" {
		writeErr(w, http.StatusBadRequest, "gcid required in body")
		return "", false
	}
	return target, true
}

// targetFromPath extracts + validates the {gcid} path segment.
func targetFromPath(w http.ResponseWriter, r *http.Request) (string, bool) {
	target := strings.TrimSpace(r.PathValue("gcid"))
	if target == "" {
		writeErr(w, http.StatusBadRequest, "gcid path segment required")
		return "", false
	}
	return target, true
}

// followMember creates the caller→target follow edge. 201 on a new edge,
// 200 with created=false on a duplicate (the aggregate emits no event then).
func (h *Handler) followMember(w http.ResponseWriter, r *http.Request) {
	if h.deps.Graph == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("Graph"))
		return
	}
	target, ok := targetFromBody(w, r)
	if !ok {
		return
	}
	edge, created, err := h.deps.Graph.Follow(r.Context(), tenantFrom(r), gcidFrom(r), target)
	if err != nil {
		code, msg := relationshipErrToStatus(err)
		writeErr(w, code, msg)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, followResponse{
		GCID:      edge.FolloweeGCID,
		Created:   created,
		CreatedAt: edge.CreatedAt.UTC().Format(wireTimeLayout),
	})
}

// unfollowMember removes the caller→target edge. Idempotent — 204 whether or
// not an edge existed this call.
func (h *Handler) unfollowMember(w http.ResponseWriter, r *http.Request) {
	if h.deps.Graph == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("Graph"))
		return
	}
	target, ok := targetFromPath(w, r)
	if !ok {
		return
	}
	if _, err := h.deps.Graph.Unfollow(r.Context(), tenantFrom(r), gcidFrom(r), target); err != nil {
		code, msg := relationshipErrToStatus(err)
		writeErr(w, code, msg)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// blockMember blocks target (severance happens inside the aggregate: both
// follow directions die in the same tx). Idempotent 204.
func (h *Handler) blockMember(w http.ResponseWriter, r *http.Request) {
	if h.deps.Graph == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("Graph"))
		return
	}
	target, ok := targetFromBody(w, r)
	if !ok {
		return
	}
	if err := h.deps.Graph.Block(r.Context(), tenantFrom(r), gcidFrom(r), target); err != nil {
		code, msg := relationshipErrToStatus(err)
		writeErr(w, code, msg)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// unblockMember removes the caller's block on target. Restores NOTHING
// (ADR-230 D1). Idempotent 204.
func (h *Handler) unblockMember(w http.ResponseWriter, r *http.Request) {
	if h.deps.Graph == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("Graph"))
		return
	}
	target, ok := targetFromPath(w, r)
	if !ok {
		return
	}
	if _, err := h.deps.Graph.Unblock(r.Context(), tenantFrom(r), gcidFrom(r), target); err != nil {
		code, msg := relationshipErrToStatus(err)
		writeErr(w, code, msg)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// =============================================================================
// Suggestions (hybrid discovery — who to follow)
// =============================================================================

// followSuggestionJSON is one "who to follow" candidate on the wire.
type followSuggestionJSON struct {
	GCID          string   `json:"gcid"`
	DisplayName   string   `json:"display_name"`
	SharedTags    []string `json:"shared_tags"`
	MutualFollows int      `json:"mutual_follows"`
}

// listFriendSuggestionsResponse is the GET /v1/connections/suggestions payload.
type listFriendSuggestionsResponse struct {
	Suggestions []followSuggestionJSON `json:"suggestions"`
}

// listFriendSuggestions — GET /v1/connections/suggestions?limit=.
//
// Hybrid "who to follow" candidates via the SuggestionQueries port:
// interest-based ranking (shared profiler tags) + follow-graph proximity
// (2-hop mutual follows). Replaces the old FoF query that required the
// friendship graph. Default limit 10, hard cap 25. Exclusion filtering
// (self / already-followed / blocked-either-direction) happens IN-QUERY
// before the LIMIT so the result fills to the cap.
func (h *Handler) listFriendSuggestions(w http.ResponseWriter, r *http.Request) {
	if h.deps.Suggestions == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("Suggestions"))
		return
	}
	limit := parseLimit(r.URL.Query().Get("limit"), 10, 25)
	suggestions, err := h.deps.Suggestions.FollowSuggestions(r.Context(), tenantFrom(r), gcidFrom(r), limit)
	if err != nil {
		code, msg := relationshipErrToStatus(err)
		writeErr(w, code, msg)
		return
	}
	resp := listFriendSuggestionsResponse{
		Suggestions: make([]followSuggestionJSON, 0, len(suggestions)),
	}
	for _, s := range suggestions {
		tags := s.SharedTags
		if tags == nil {
			tags = []string{}
		}
		resp.Suggestions = append(resp.Suggestions, followSuggestionJSON{
			GCID:          s.GCID,
			DisplayName:   s.DisplayName,
			SharedTags:    tags,
			MutualFollows: s.MutualFollows,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}
