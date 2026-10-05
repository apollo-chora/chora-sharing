// Package atom_share is the pure-domain core for the shared-atom feed entry.
//
// The shared-atom feed entry is the EXISTING immutable Post (the
// social_feed_entries row with entry_type='share'), extended additively to
// carry LicenseTerms + RoyaltyRate in its content JSONB. There is NO new
// SharedAtomEntry aggregate — the immutable feed entry IS the share record.
//
// Per the R-15-A invariant: the feed entry is IMMUTABLE. Hide / price-change /
// revoke are emitted as append-only ShareEvents onto atom_share_events —
// NEVER a mutation of the entry itself. The read-side Status (visible /
// hidden_by_author / moderation_hidden / revoked) is a DERIVED projection
// over those events — never a column on social_feed_entries.
//
// Aggregate root : Post (this package only holds the Share value object +
//
//	license value objects + the derived-status projection + the ShareEvent
//	child + the ShareRepo port).
//
// Owning domain : Content Sharing (chora_sharing DB).
// Owning team   : Team 1 (Content).
package atom_share

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// MaxCaptionLen caps the share caption. Per §7.1 step 9, captions are capped
// at 512 chars so the immutable feed row + the JSONB content budget stay
// bounded.
const MaxCaptionLen = 512

// StemPreviewMax caps the denormalised stem preview stored on the feed entry
// (§7.1 step 10).
const StemPreviewMax = 140

// ErrInvalidArgument is returned for guard-clause failures inside NewShare.
var ErrInvalidArgument = errors.New("invalid argument")

// LicenseTerms governs reuse of a shared atom (R2). The author decides
// licensing per atom at share time; the chosen terms are frozen onto each
// AtomUsageGrant at grant time.
//
// The string values mirror the atom_license_terms PG enum labels exactly
// (lowercase snake_case). The adapter maps between these and the proto
// UPPER_CASE enum at the boundary.
type LicenseTerms string

const (
	// LicenseFree is free + attribution (≈ CC BY).
	LicenseFree LicenseTerms = "free"
	// LicenseRoyaltyPct is paid reuse, revenue-share % of base_usage_value.
	LicenseRoyaltyPct LicenseTerms = "royalty_pct"
	// LicenseRoyaltyFixed is paid reuse, fixed per-use fee.
	LicenseRoyaltyFixed LicenseTerms = "royalty_fixed"
	// LicenseCCBySA is free + attribution + share-alike.
	LicenseCCBySA LicenseTerms = "cc_by_sa"
	// LicenseCCND is free + attribution + no-derivatives.
	LicenseCCND LicenseTerms = "cc_nd"
)

// IsValid reports whether the license is one of the five canonical terms.
// An empty / unspecified license is NOT valid — the author must pick one.
func (l LicenseTerms) IsValid() bool {
	switch l {
	case LicenseFree, LicenseRoyaltyPct, LicenseRoyaltyFixed, LicenseCCBySA, LicenseCCND:
		return true
	}
	return false
}

// IsRoyalty reports whether the license requires a royalty rate + debit at
// reuse time (R2: royalty_rate required iff license ∈ {royalty_pct,
// royalty_fixed}). Free licenses (free / cc_by_sa / cc_nd) incur no
// settlement.
func (l LicenseTerms) IsRoyalty() bool {
	return l == LicenseRoyaltyPct || l == LicenseRoyaltyFixed
}

// RoyaltyRate is the per-atom royalty rate frozen at share time and snapshotted
// onto each grant. Kind is "pct" (0..100) or "fixed_per_use" (>= 0).
//
// For royalty_pct  → amount = base_usage_value × rate.value
// For royalty_fixed → amount = rate.value
// Amount is clamped to RoyaltyCap when cap > 0 (see grant.SettleRoyalty).
type RoyaltyRate struct {
	Kind  string  `json:"kind"`
	Value float64 `json:"value"`
}

// Validate enforces the rate invariants per R2:
//   - kind must be "pct" or "fixed_per_use".
//   - pct enforces 0 ≤ value ≤ 100.
//   - fixed_per_use enforces value ≥ 0.
//
// A zero RoyaltyRate is valid ONLY for free licenses (the caller checks
// IsRoyalty before requiring a rate).
func (r RoyaltyRate) Validate() error {
	switch r.Kind {
	case "pct":
		if r.Value < 0 || r.Value > 100 {
			return fmt.Errorf("%w: pct royalty rate value %v out of range [0,100]", ErrInvalidArgument, r.Value)
		}
	case "fixed_per_use":
		if r.Value < 0 {
			return fmt.Errorf("%w: fixed royalty rate value %v must be >= 0", ErrInvalidArgument, r.Value)
		}
	default:
		return fmt.Errorf("%w: royalty rate kind %q must be pct or fixed_per_use", ErrInvalidArgument, r.Kind)
	}
	return nil
}

// ShareEventType is the type of an append-only ShareEvent child.
//
// Mirrors the atom_share_event_type PG enum labels.
type ShareEventType string

const (
	// EventHidden — the author hid their own share (status → hidden_by_author).
	EventHidden ShareEventType = "hidden"
	// EventPriceChanged — the author changed the license terms (payload carries
	// the new snapshot; existing grants keep their frozen snapshot — R2).
	EventPriceChanged ShareEventType = "price_changed"
	// EventRevoked — the author revoked the share entirely (status → revoked).
	EventRevoked ShareEventType = "revoked"
	// EventModerationHidden — a governance/m moderation event hid the share
	// (status → moderation_hidden).
	EventModerationHidden ShareEventType = "moderation_hidden"
)

// ShareEvent is the append-only child of the immutable share feed entry. Each
// event records a hide / price-change / revoke / moderation-hide action; the
// read-side Status is DERIVED from the newest event of each relevant type —
// NEVER stored as a column on the entry itself.
//
// Payload is a free-form snapshot of the event's effect (e.g. for
// price_changed: the new LicenseTerms + RoyaltyRate). SourceEventID is the
// upstream Pub/Sub event_id when this event was triggered by a subscriber
// (governance violation → moderation_hidden); idempotency is on SourceEventID.
type ShareEvent struct {
	FeedEntryID   string         `json:"feed_entry_id"`
	ActorGCID     string         `json:"actor_gcid"`
	Type          ShareEventType `json:"type"`
	Payload       map[string]any `json:"payload,omitempty"`
	SourceEventID string         `json:"source_event_id,omitempty"`
	CreatedAt     time.Time      `json:"created_at"`
}

// Status is the derived read-side status of a share. Computed from the
// ShareEvents appended to the feed entry (newest-first), never stored as a
// column.
//
// Derivation precedence (newest relevant event wins):
//   - moderation_hidden → StatusModerationHidden (governance override beats author)
//   - revoked           → StatusRevoked (terminal)
//   - hidden            → StatusHiddenByAuthor
//   - no hiding event   → StatusVisible
type Status string

const (
	// StatusVisible — the share is live in the feed (default).
	StatusVisible Status = "visible"
	// StatusHiddenByAuthor — the author hid it; reversible by re-sharing.
	StatusHiddenByAuthor Status = "hidden_by_author"
	// StatusModerationHidden — governance hid it; takes precedence over author.
	StatusModerationHidden Status = "moderation_hidden"
	// StatusRevoked — the author revoked it; terminal for that entry.
	StatusRevoked Status = "revoked"
)

// Share is the immutable value object for a shared-atom feed entry. It is the
// social_feed_entries row with entry_type='share', denormalised with the R1
// attribution snapshot (OwnerGCID + AuthorDisplayName + StemPreview +
// QuestionType) so the feed never does a cross-DB read of chora_creation or
// chora_identity.
//
// Cross-domain references (all opaque UUID, no FK):
//   - AtomID     → chora_creation.LearningAtom
//   - RevisionID → chora_creation.AtomRevision (append-only; pinned at share
//     time so reuse targets a frozen revision)
//   - OwnerGCID  → chora_identity.Account (the AUTHOR of record — R1)
type Share struct {
	FeedEntryID       string       `json:"feed_entry_id"`
	TenantID          string       `json:"tenant_id"`
	AtomID            string       `json:"atom_id"`
	RevisionID        string       `json:"atom_revision_id"`
	OwnerGCID         string       `json:"owner_gcid"`
	AuthorDisplayName string       `json:"author_display_name"`
	StemPreview       string       `json:"stem_preview"`
	QuestionType      string       `json:"question_type"`
	Options           []string     `json:"options"`
	Caption           string       `json:"caption"`
	License           LicenseTerms `json:"license_terms"`
	Rate              RoyaltyRate  `json:"royalty_rate,omitempty"`
	CreatedAt         time.Time    `json:"created_at"`
}

// NewShare validates the license rules (R2) + caption length + R1 attribution
// snapshot and returns an immutable Share value object. The caller (the
// ShareAtom handler §7.1) is responsible for R1 author validation BEFORE
// calling this — NewShare only enforces R2 (royalty_rate required iff
// license ∈ {royalty_pct, royalty_fixed}).
//
// Validation guards rejected with ErrInvalidArgument:
//   - tenantID, atomID, revisionID, ownerGCID required (R1 attribution snapshot)
//   - license must be one of the five canonical terms
//   - caption ≤ MaxCaptionLen
//   - R2: royalty_rate required (and valid) iff license.IsRoyalty()
//   - R2: royalty_rate ignored (zero OK) for free licenses
//   - StemPreview truncated to StemPreviewMax on a rune boundary
func NewShare(tenantID, atomID, revisionID, ownerGCID, authorDisplayName, stemPreview, questionType string, options []string, caption string, license LicenseTerms, rate RoyaltyRate) (*Share, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if atomID == "" {
		return nil, fmt.Errorf("%w: atom_id required", ErrInvalidArgument)
	}
	if revisionID == "" {
		return nil, fmt.Errorf("%w: atom_revision_id required", ErrInvalidArgument)
	}
	if ownerGCID == "" {
		return nil, fmt.Errorf("%w: owner_gcid (R1 author) required", ErrInvalidArgument)
	}
	if !license.IsValid() {
		return nil, fmt.Errorf("%w: invalid license_terms %q", ErrInvalidArgument, license)
	}
	if err := validateCaption(caption); err != nil {
		return nil, err
	}
	if license.IsRoyalty() {
		if err := rate.Validate(); err != nil {
			return nil, fmt.Errorf("%w: royalty license %q requires a valid royalty_rate", ErrInvalidArgument, license)
		}
	}
	return &Share{
		TenantID:          tenantID,
		AtomID:            atomID,
		RevisionID:        revisionID,
		OwnerGCID:         ownerGCID,
		AuthorDisplayName: authorDisplayName,
		StemPreview:       truncateRune(stemPreview, StemPreviewMax),
		QuestionType:      questionType,
		Options:           append([]string(nil), options...),
		Caption:           caption,
		License:           license,
		Rate:              rate,
		CreatedAt:         time.Now().UTC(),
	}, nil
}

// validateCaption enforces the caption guard (≤ MaxCaptionLen). An empty
// caption is valid — caption is optional per §7.1 step 4.
func validateCaption(caption string) error {
	if len([]rune(caption)) > MaxCaptionLen {
		return fmt.Errorf("%w: caption length %d exceeds max %d", ErrInvalidArgument, len([]rune(caption)), MaxCaptionLen)
	}
	return nil
}

// truncateRune truncates s to max runes on a rune boundary (no mid-glyph split).
func truncateRune(s string, max int) string {
	if max <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}

// DeriveStatus computes the read-side Status from the append-only ShareEvents
// for a feed entry. Newest-first derivation: the most recent relevant hiding
// event determines the status.
//
// Precedence (highest first):
//  1. moderation_hidden → StatusModerationHidden (governance override beats
//     author hide — a moderator hide cannot be author-unhidden).
//  2. revoked → StatusRevoked (terminal).
//  3. hidden → StatusHiddenByAuthor (author hid; reversible).
//  4. (none of the above) → StatusVisible.
//
// price_changed never affects status — it only updates the live license on
// NEW grants (existing grants keep their frozen snapshot — R2).
//
// events MAY be unsorted; DeriveStatus sorts by CreatedAt descending. Ties
// break on a stable secondary (SourceEventID then ActorGCID) to keep
// derivation deterministic across replays.
func DeriveStatus(events []ShareEvent) Status {
	if len(events) == 0 {
		return StatusVisible
	}
	// Copy + sort newest-first by CreatedAt, then stable tie-breaks.
	ordered := make([]ShareEvent, len(events))
	copy(ordered, events)
	sortEventsNewestFirst(ordered)

	for _, e := range ordered {
		switch e.Type {
		case EventModerationHidden:
			return StatusModerationHidden
		case EventRevoked:
			return StatusRevoked
		case EventHidden:
			return StatusHiddenByAuthor
		}
	}
	return StatusVisible
}

// sortEventsNewestFirst sorts in-place by CreatedAt descending. Stable tie-
// break on SourceEventID then ActorGCID keeps derivation deterministic across
// replays when two events share a timestamp (rare but possible under clock
// skew).
func sortEventsNewestFirst(events []ShareEvent) {
	// Insertion sort — n is tiny (handful of events per share).
	for i := 1; i < len(events); i++ {
		for j := i; j > 0; j-- {
			if before(events[j], events[j-1]) {
				events[j], events[j-1] = events[j-1], events[j]
				continue
			}
			break
		}
	}
}

// before reports whether a should sort before b (newest-first: a is "more
// recent"). Equal timestamps tie-break on SourceEventID then ActorGCID.
func before(a, b ShareEvent) bool {
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.After(b.CreatedAt)
	}
	if a.SourceEventID != b.SourceEventID {
		return a.SourceEventID > b.SourceEventID
	}
	return a.ActorGCID > b.ActorGCID
}

// ShareRepo is the hexagonal port the ShareAtom + ListSharedAtoms + grant
// handlers use to persist + read shares. The pg adapter implements it with
// RLS-aware pgx; the inmem double implements it for unit tests. The port
// lives in the domain so adapters depend on the domain, never the reverse.
//
// Contract notes:
//   - SaveShare persists the immutable feed entry (entry_type='share'). It is
//     idempotent on FeedEntryID (the idempotency_key-derived UUIDv5).
//   - GetShare loads a single share by feed entry id; ErrNotFound when the
//     entry is missing or revoked (the read-side status excludes revoked).
//   - GetShareByAtom loads the latest visible (non-revoked) share for a given
//     atom + owner. Used by the DELETE /v1/atoms/{id}/share revoke flow to
//     resolve which feed entry to append EventRevoked onto. ErrNotFound when
//     no visible share exists (never shared or already revoked).
//   - ListSharedAtoms is a keyset-paginated read (cursor = last feed entry id,
//     newest-first). Revoked + hidden shares are excluded by derived read-side
//     status. Filters: topic (question_type) + question_type.
//   - AppendEvent appends an append-only ShareEvent child row. Idempotent on
//     SourceEventID when non-empty.
//   - ListEvents returns the events for a feed entry, newest-first.
type ShareRepo interface {
	SaveShare(ctx context.Context, s *Share) error
	GetShare(ctx context.Context, feedEntryID string) (*Share, error)
	GetShareByAtom(ctx context.Context, atomID, ownerGCID string) (*Share, error)
	ListSharedAtoms(ctx context.Context, tenantID string, cursor string, limit int, topicFilter, questionTypeFilter string, scope string, followingGCIDs []string, blockedGCIDs []string) ([]Share, string, error)
	AppendEvent(ctx context.Context, e *ShareEvent) error
	ListEvents(ctx context.Context, feedEntryID string) ([]ShareEvent, error)
}

// ErrNotFound is returned by ShareRepo.GetShare when the feed entry is missing
// or has been revoked (revoked entries are not readable — existence would leak
// the share history to non-authors).
var ErrNotFound = errors.New("share not found")

// ErrConflict signals a conflicting idempotent replay (e.g. AppendEvent with
// a duplicate SourceEventID that carries a different payload).
var ErrConflict = errors.New("share event conflict")
