package httpadapter

import (
	"errors"
	"net/http"
	"strings"

	"github.com/apollo-chora/chora-sharing/internal/adapter/modelarmor"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
)

// =============================================================================
// Atom Sharing surface (§7.1–§7.2)
// =============================================================================

// shareAtomRequest is the POST /v1/atoms/{atom_id}/share body. atom_id comes
// from the path; author_gcid + tenant_id come from the gateway-stamped
// identity headers (not the body). Per §7.1 step 4.
type shareAtomRequest struct {
	AtomRevisionID string           `json:"atom_revision_id,omitempty"`
	Caption        string           `json:"caption,omitempty"`
	LicenseTerms   string           `json:"license_terms"`
	RoyaltyRate    *royaltyRateJSON `json:"royalty_rate,omitempty"`
}

// shareAtomResponse is the 201 Created payload (§7.1 step 13).
type shareAtomResponse struct {
	ShareEntryID      string `json:"share_entry_id"`
	AuthorDisplayName string `json:"author_display_name"`
	CreatedAt         string `json:"created_at"` // RFC 3339
}

// shareAtom — POST /v1/atoms/{atom_id}/share.
//
// An author shares their own atom to the feed with a per-atom license (R1 +
// R2). Author validation via AtomProjectionReader (R1: caller MUST equal
// projection owner_gcid). Caption moderation via Cloud Model Armor BEFORE
// persisting (FR-037). Per §7.1.
func (h *Handler) shareAtom(w http.ResponseWriter, r *http.Request) {
	// §7.1 step 1: deps.Shares + deps.Projections non-nil (else 501).
	if h.deps.Shares == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("Shares"))
		return
	}
	if h.deps.Projections == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("Projections"))
		return
	}

	// §7.1 step 2: path parse.
	atomID, ok := pathUUID(w, r, "atom_id")
	if !ok {
		return
	}

	// §7.1 step 3: identity headers (enforced by middleware; read from ctx).
	author := gcidFrom(r)
	tenant := tenantFrom(r)

	// §7.1 step 5: Idempotency-Key required.
	idemKey := idempotencyKey(r)
	if idemKey == "" {
		writeErr(w, http.StatusBadRequest, "Idempotency-Key header required")
		return
	}

	// §7.1 step 4: body.
	var body shareAtomRequest
	if !readJSON(w, r, &body) {
		return
	}
	caption := body.Caption

	// §7.1 step 6: caption moderation (FR-037). When Guardrail is wired +
	// caption non-empty, screen BEFORE persisting. blocked → 422 (never
	// persisted); flagged → proceed + log (audit-only).
	if h.deps.Guardrail != nil && strings.TrimSpace(caption) != "" {
		verdict, err := h.deps.Guardrail.Screen(r.Context(), modelarmor.ScreenRequest{
			TenantID:   tenant,
			AuthorGCID: author,
			AgentID:    modelarmor.AgentIDSocialModeration,
			Content:    caption,
		})
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "guardrail screen: "+err.Error())
			return
		}
		if verdict.Verdict == modelarmor.VerdictBlocked {
			// §6: 422 — guardrail blocked, never persisted.
			writeErr(w, http.StatusUnprocessableEntity, "caption blocked by guardrail")
			return
		}
	}

	// §7.1 step 7: R1 author validation — load AtomProjection.
	proj, err := h.deps.Projections.Get(r.Context(), atomID)
	if err != nil {
		if errors.Is(err, atom_projection.ErrNotFound) {
			// §6: 412 — atom not published / withdrawn.
			writeErr(w, http.StatusPreconditionFailed, "atom not published or withdrawn")
			return
		}
		writeErr(w, http.StatusInternalServerError, "projection lookup: "+err.Error())
		return
	}
	if proj.OwnerGCID != author {
		// §6: 403 — R1 violation (not the author).
		writeErr(w, http.StatusForbidden, "R1: caller is not the atom owner")
		return
	}

	// §7.1 step 8: pin revision — explicit wins; else projection's latest.
	revisionID := strings.TrimSpace(body.AtomRevisionID)
	if revisionID == "" {
		revisionID = proj.RevisionID
	}

	license := licenseStringToDomain(body.LicenseTerms)
	if license == "" {
		writeErr(w, http.StatusBadRequest, "invalid license_terms")
		return
	}
	rate := protoRoyaltyRateToDomain(body.RoyaltyRate)

	// §7.1 step 9–10: NewShare validates R2 (royalty_rate required iff
	// royalty license) + caption ≤ 512 + denormalises R1 snapshot.
	share, err := atom_share.NewShare(
		tenant, atomID, revisionID, proj.OwnerGCID,
		proj.AuthorDisplayName, proj.Stem, string(proj.QuestionType),
		proj.Options, caption, license, rate,
	)
	if err != nil {
		if errors.Is(err, atom_share.ErrInvalidArgument) {
			// §6: 400 — invalid argument (bad license / caption / royalty).
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		writeErr(w, http.StatusInternalServerError, "new share: "+err.Error())
		return
	}

	// §7.1 step 11: assign the deterministic feed entry id (UUIDv5 — the pg
	// UNIQUE constraint dedups replays). Domain NewShare leaves FeedEntryID
	// empty; the adapter owns ID assignment so the domain stays uuid-free.
	share.FeedEntryID = deriveFeedEntryID(idemKey, tenant, atomID)
	if err := h.deps.Shares.SaveShare(r.Context(), share); err != nil {
		writeErr(w, http.StatusInternalServerError, "save share: "+err.Error())
		return
	}

	// §7.1 step 12: event publish (fail-soft) — the outbox dispatcher
	// publishes chora.sharing.atom.shared.v1 atomically with SaveShare in
	// the pg adapter. The HTTP surface returns 201.
	w.Header().Set("Location", "/v1/feed/shared-atoms/"+share.FeedEntryID)
	writeJSON(w, http.StatusCreated, shareAtomResponse{
		ShareEntryID:      share.FeedEntryID,
		AuthorDisplayName: share.AuthorDisplayName,
		CreatedAt:         share.CreatedAt.UTC().Format("2006-01-02T15:04:05.000000Z"),
	})
}

// revokeShare — DELETE /v1/atoms/{atom_id}/share.
//
// Revokes the caller's share of the given atom from the feed. The feed entry
// is NOT deleted (R-15-A: immutable) — instead, an EventRevoked is appended to
// atom_share_events, which DeriveStatus reads as StatusRevoked, excluding it
// from feed reads. Idempotent: 204 even when already revoked or never shared.
//
// Authorisation: the caller must own the share (R1 — caller GCID must equal
// the share's OwnerGCID). A non-owner gets 403. Per §1.1, a nil deps.Shares
// returns 501.
func (h *Handler) revokeShare(w http.ResponseWriter, r *http.Request) {
	if h.deps.Shares == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("Shares"))
		return
	}

	atomID, ok := pathUUID(w, r, "atom_id")
	if !ok {
		return
	}

	author := gcidFrom(r)

	// Load the caller's visible share for this atom. ErrNotFound → idempotent
	// 204 (never shared or already revoked — both are "not in the feed").
	share, err := h.deps.Shares.GetShareByAtom(r.Context(), atomID, author)
	if err != nil {
		if errors.Is(err, atom_share.ErrNotFound) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeErr(w, http.StatusInternalServerError, "lookup share: "+err.Error())
		return
	}

	// R1: caller must be the owner. GetShareByAtom already filters on
	// owner_gcid, so a mismatch here would be a bug — but we double-check
	// for defence in depth.
	if share.OwnerGCID != author {
		writeErr(w, http.StatusForbidden, "R1: caller is not the atom owner")
		return
	}

	// Append the terminal EventRevoked. SourceEventID is left empty — this is
	// an author-initiated revoke (not a subscriber replay). The event is
	// NOT idempotent on its own (a second call would append a second row),
	// but GetShareByAtom already returned ErrNotFound for a revoked share, so
	// the idempotent guard is the read above, not the append.
	ev := &atom_share.ShareEvent{
		FeedEntryID: share.FeedEntryID,
		ActorGCID:   author,
		Type:        atom_share.EventRevoked,
		CreatedAt:   nowUTC(),
	}
	if err := h.deps.Shares.AppendEvent(r.Context(), ev); err != nil {
		writeErr(w, http.StatusInternalServerError, "revoke share: "+err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// sharedAtomCard is the feed-list DTO (§5.3 SharedAtomCard, §7.2). Mirrors the
// proto field names for BFF parity.
type sharedAtomCard struct {
	ShareEntryID      string           `json:"share_entry_id"`
	AuthorGCID        string           `json:"author_gcid"`
	AuthorDisplayName string           `json:"author_display_name"`
	AtomID            string           `json:"atom_id"`
	AtomRevisionID    string           `json:"atom_revision_id"`
	AtomStemPreview   string           `json:"atom_stem_preview"`
	QuestionType      string           `json:"question_type"`
	AtomOptions       []string         `json:"atom_options"`
	Caption           string           `json:"caption,omitempty"`
	LicenseTerms      string           `json:"license_terms"`
	RoyaltyRate       *royaltyRateJSON `json:"royalty_rate,omitempty"`
	ReactionCount     int32            `json:"reaction_count"`
	ReactionCounts    map[string]int32 `json:"reaction_counts"`
	MyReaction        string           `json:"my_reaction,omitempty"`
	CreatedAt         string           `json:"created_at"`
}

// listSharedAtomsResponse is the GET /v1/feed/shared-atoms payload.
type listSharedAtomsResponse struct {
	Cards      []sharedAtomCard `json:"cards"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

// listSharedAtoms — GET /v1/feed/shared-atoms.
//
// Paginated read (§7.2). X-Tenant-Id required (RLS — enforced by middleware).
// limit default 20, hard cap 100. cursor = opaque keyset (last entry id).
// Revoked + hidden shares excluded by derived read-side status (R-15-A).
// Newest-first by UUIDv7 entry id. royalty_rate only when license is royalty.
func (h *Handler) listSharedAtoms(w http.ResponseWriter, r *http.Request) {
	if h.deps.Shares == nil {
		writeErr(w, http.StatusNotImplemented, notWiredMsg("Shares"))
		return
	}
	tenant := tenantFrom(r)
	gcid := gcidFrom(r)
	limit := parseLimit(r.URL.Query().Get("limit"), 20, 100)
	cursor := strings.TrimSpace(r.URL.Query().Get("cursor"))
	topicFilter := strings.TrimSpace(r.URL.Query().Get("topic_filter"))
	questionTypeFilter := strings.TrimSpace(r.URL.Query().Get("question_type_filter"))
	scope := strings.TrimSpace(r.URL.Query().Get("scope"))
	if scope == "" {
		scope = "tenant"
	}

	var followingGCIDs []string
	if scope == "following" {
		if h.deps.Graph == nil {
			writeErr(w, http.StatusNotImplemented, notWiredMsg("Graph"))
			return
		}
		var gerr error
		followingGCIDs, gerr = h.deps.Graph.FollowingGCIDs(r.Context(), tenant, gcid)
		if gerr != nil {
			writeErr(w, http.StatusInternalServerError, "feed following scope: "+gerr.Error())
			return
		}
		if len(followingGCIDs) == 0 {
			writeJSON(w, http.StatusOK, listSharedAtomsResponse{
				Cards:      []sharedAtomCard{},
				NextCursor: "",
			})
			return
		}
	}

	var blockedGCIDs []string
	if h.deps.Graph != nil {
		if b, err := h.deps.Graph.BlockedBy(r.Context(), tenant, gcid); err == nil {
			blockedGCIDs = b
		}
	}

	shares, nextCursor, err := h.deps.Shares.ListSharedAtoms(
		r.Context(), tenant, cursor, limit, topicFilter, questionTypeFilter, scope, followingGCIDs, blockedGCIDs,
	)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list shared atoms: "+err.Error())
		return
	}
	cards := make([]sharedAtomCard, 0, len(shares))
	for _, sh := range shares {
		card := sharedAtomCard{
			ShareEntryID:      sh.FeedEntryID,
			AuthorGCID:        sh.OwnerGCID,
			AuthorDisplayName: sh.AuthorDisplayName,
			AtomID:            sh.AtomID,
			AtomRevisionID:    sh.RevisionID,
			AtomStemPreview:   sh.StemPreview,
			QuestionType:      sh.QuestionType,
			AtomOptions:       sh.Options,
			Caption:           sh.Caption,
			LicenseTerms:      domainLicenseToString(sh.License),
			CreatedAt:         sh.CreatedAt.UTC().Format("2006-01-02T15:04:05.000000Z"),
		}
		if sh.License.IsRoyalty() {
			card.RoyaltyRate = domainRoyaltyRateToJSON(sh.Rate)
		}
		if h.deps.Reactions != nil {
			// A failed reaction read must not silently render a card with zero
			// reactions — that is an absence assertion encoding the bug. Fail loud.
			rxs, err := h.deps.Reactions.ListByPost(r.Context(), sh.FeedEntryID)
			if err != nil {
				writeErr(w, http.StatusInternalServerError, "list reactions failed")
				return
			}
			card.ReactionCount = int32(len(rxs))
			counts := make(map[string]int32, 4)
			for _, rx := range rxs {
				counts[string(rx.Type)]++
				if rx.GCID == gcid {
					card.MyReaction = domainReactionKindToString(rx.Type)
				}
			}
			card.ReactionCounts = counts
		}
		cards = append(cards, card)
	}

	// Enrich with current display names — the feed entry's
	// author_display_name is a stale snapshot from share time (may be
	// empty if the user had no display_name when they shared). Resolve
	// the latest name from profiler_profiles (which is populated by the
	// identity GetMe call at profile generation time).
	if h.deps.Profiles != nil && len(cards) > 0 {
		gcids := make([]string, 0, len(cards))
		for _, c := range cards {
			gcids = append(gcids, c.AuthorGCID)
		}
		names, _ := h.deps.Profiles.ResolveDisplayNames(r.Context(), gcids)
		for i := range cards {
			if name, ok := names[cards[i].AuthorGCID]; ok && name != "" {
				cards[i].AuthorDisplayName = name
			}
		}
	}

	writeJSON(w, http.StatusOK, listSharedAtomsResponse{
		Cards:      cards,
		NextCursor: nextCursor,
	})
}
