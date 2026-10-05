package httpadapter

import (
	"net/http"
	"strings"

	"github.com/apollo-chora/chora-sharing/internal/domain/bookmark"
)

// =============================================================================
// Atom Bookmark surface (personal collection)
// =============================================================================

// bookmarkAtomRequest is the optional POST /v1/atoms/{atom_id}/bookmark body.
// atom_id comes from the path; gcid + tenant_id come from the gateway-stamped
// identity headers (not the body). atom_revision_id is optional — a user may
// bookmark without pinning a specific revision.
type bookmarkAtomRequest struct {
	AtomRevisionID string `json:"atom_revision_id,omitempty"`
}

// bookmarkJSON is the wire representation of a Bookmark.
type bookmarkJSON struct {
	ID             string `json:"id"`
	AtomID         string `json:"atom_id"`
	AtomRevisionID string `json:"atom_revision_id,omitempty"`
	CreatedAt      string `json:"created_at"` // RFC 3339
}

// listBookmarksResponse is the GET /v1/me/bookmarks payload.
type listBookmarksResponse struct {
	Bookmarks  []bookmarkJSON `json:"bookmarks"`
	NextCursor string         `json:"next_cursor,omitempty"`
}

// bookmarkAtom — POST /v1/atoms/{atom_id}/bookmark.
//
// Saves an atom to the caller's personal collection. Idempotent on
// (gcid, atom_id) — re-bookmarking is a no-op. Idempotency-Key is required
// (same discipline as shareAtom). Per §1.1, a nil deps.Bookmarks returns 501
// (fail-loud — never a fake success).
func (h *Handler) bookmarkAtom(w http.ResponseWriter, r *http.Request) {
	if h.deps.Bookmarks == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("Bookmarks"))
		return
	}

	atomID, ok := pathUUID(w, r, "atom_id")
	if !ok {
		return
	}

	gcid := gcidFrom(r)
	tenant := tenantFrom(r)

	idemKey := idempotencyKey(r)
	if idemKey == "" {
		writeErr(w, http.StatusBadRequest, "Idempotency-Key header required")
		return
	}

	// Optional body — atom_revision_id may be omitted (nullable in DB).
	var body bookmarkAtomRequest
	if r.Body != nil && r.ContentLength != 0 {
		if !readJSON(w, r, &body) {
			return
		}
	}

	bm, err := bookmark.NewBookmark(tenant, gcid, atomID, body.AtomRevisionID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := h.deps.Bookmarks.Save(r.Context(), bm); err != nil {
		writeErr(w, http.StatusInternalServerError, "save bookmark: "+err.Error())
		return
	}

	w.Header().Set("Location", "/v1/me/bookmarks/"+bm.ID)
	resp := bookmarkJSON{
		ID:        bm.ID,
		AtomID:    bm.AtomID,
		CreatedAt: bm.CreatedAt.UTC().Format("2006-01-02T15:04:05.000000Z"),
	}
	if bm.AtomRevisionID != "" {
		resp.AtomRevisionID = bm.AtomRevisionID
	}
	writeJSON(w, http.StatusCreated, resp)
}

// unbookmarkAtom — DELETE /v1/atoms/{atom_id}/bookmark.
//
// Removes the atom from the caller's personal collection. Idempotent —
// returns 204 whether or not the bookmark existed. Per §1.1, a nil
// deps.Bookmarks returns 501.
func (h *Handler) unbookmarkAtom(w http.ResponseWriter, r *http.Request) {
	if h.deps.Bookmarks == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("Bookmarks"))
		return
	}

	atomID, ok := pathUUID(w, r, "atom_id")
	if !ok {
		return
	}

	gcid := gcidFrom(r)
	tenant := tenantFrom(r)

	if err := h.deps.Bookmarks.Delete(r.Context(), tenant, gcid, atomID); err != nil {
		writeErr(w, http.StatusInternalServerError, "delete bookmark: "+err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// listBookmarks — GET /v1/me/bookmarks.
//
// Paginated read of the caller's bookmarks, newest-first. limit default 20,
// hard cap 100. cursor = opaque keyset (last bookmark id). Per §1.1, a nil
// deps.Bookmarks returns 501.
func (h *Handler) listBookmarks(w http.ResponseWriter, r *http.Request) {
	if h.deps.Bookmarks == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("Bookmarks"))
		return
	}

	gcid := gcidFrom(r)
	tenant := tenantFrom(r)
	limit := parseLimit(r.URL.Query().Get("limit"), 20, 100)
	cursor := strings.TrimSpace(r.URL.Query().Get("cursor"))

	bookmarks, nextCursor, err := h.deps.Bookmarks.ListByOwner(
		r.Context(), tenant, gcid, cursor, limit,
	)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list bookmarks: "+err.Error())
		return
	}

	cards := make([]bookmarkJSON, 0, len(bookmarks))
	for _, bm := range bookmarks {
		card := bookmarkJSON{
			ID:        bm.ID,
			AtomID:    bm.AtomID,
			CreatedAt: bm.CreatedAt.UTC().Format("2006-01-02T15:04:05.000000Z"),
		}
		if bm.AtomRevisionID != "" {
			card.AtomRevisionID = bm.AtomRevisionID
		}
		cards = append(cards, card)
	}

	writeJSON(w, http.StatusOK, listBookmarksResponse{
		Bookmarks:  cards,
		NextCursor: nextCursor,
	})
}
