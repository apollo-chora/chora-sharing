// Package atom_projection is the pure-domain read-model for the cached
// LearningAtom projection.
//
// AtomProjection is NOT an aggregate — it is a cached, event-fed read-model
// of a LearningAtom owned by chora_creation. Sharing NEVER edits it; it only
// consumes chora.creation.atom.published.v1 (upsert) + chora.creation.atom.
// archived.v1 (invalidate). It exists in chora_sharing solely as the R1
// attribution source: the ShareAtom endpoint validates
//
//	projection.owner_gcid == caller_gcid
//
// before allowing a share, so the author of record stays the original
// creator (R1). It also supplies the denormalised card fields (stem preview,
// author display name, question type) so the feed never does a cross-DB read
// of chora_creation.
//
// Owning domain : Content Sharing (chora_sharing DB).
// Owning team   : Team 1 (Content).
//
// Hard rules (per .claude/rules/ddd-enforcement.md):
//   - atom_id / revision_id are opaque UUID refs to chora_creation — NO FK
//     (cross-DB queries forbidden; validated via Pub/Sub events).
//   - owner_gcid / author_display_name are opaque cross-domain refs to
//     chora_identity — NO FK.
//   - This file holds the domain only — NO HTTP, NO persistence. The
//     AtomProjectionReader port is an interface so the pg adapter +
//     subscribers depend on the domain, not the reverse.
//   - Event-fed + idempotent on event_id (the subscriber claims the
//     idempotency token before upserting).
package atom_projection

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrInvalidArgument is returned for guard-clause failures.
var ErrInvalidArgument = errors.New("invalid argument")

// ErrNotFound is returned by AtomProjectionReader.Get when the atom is not
// in the cache (never published, or archived/withdrawn).
var ErrNotFound = errors.New("atom projection not found")

// QuestionType mirrors the chora_creation AtomType enum label stored on the
// projection. Kept as a string (not the proto enum) so the domain stays
// proto-free; the adapter maps between the two.
type QuestionType string

const (
	QuestionTypeUnspecified QuestionType = ""
	QuestionTypeMCQ         QuestionType = "mcq"
	QuestionTypeOE          QuestionType = "oe"
)

// Projection is the cached read-model value object for a published
// LearningAtom. Immutable from sharing's perspective — the subscriber
// replaces the whole row on each published.v1 event.
//
// Cross-domain references (all opaque UUID, no FK):
//   - AtomID      → chora_creation.LearningAtom
//   - RevisionID  → chora_creation.AtomRevision (append-only; pinned at
//     share/grant/round time so reuse targets a frozen revision)
//   - OwnerGCID   → chora_identity.Account (the AUTHOR of record — R1)
type Projection struct {
	AtomID            string
	TenantID          string
	RevisionID        string
	OwnerGCID         string
	AuthorDisplayName string
	Stem              string
	QuestionType      QuestionType
	PublishedAt       time.Time
	// Options holds the MCQ answer choices (spec §5.1 Option A — extended
	// projection so duels don't need AI). Empty for open-ended atoms.
	Options []string
	// CorrectAnswer is the correct option string (matches one of Options
	// for MCQ). Empty for open-ended atoms.
	CorrectAnswer string
	// Archived=true marks the atom as withdrawn in chora_creation. The
	// subscriber sets this on chora.creation.atom.archived.v1 and the feed
	// read-side excludes archived projections (and revokes outstanding
	// grants/feed entries per R1).
	Archived bool
	// ReuseVisibility is the ADR-229 author-consent audience label
	// (private | friends | tenant) — the event-fed copy of
	// chora_creation.learning_atoms.reuse_visibility (WS-1, CHO-2127).
	// EMPTY means the producing event pre-dates ADR-229: the pg layer
	// hardens '' to 'private' on INSERT and PRESERVES the existing column
	// value on conflict-update, so a re-publish from an old producer never
	// resets an audience set via reuse_visibility_changed.v1.
	ReuseVisibility string
}

// ValidReuseVisibility reports whether s is a recognised ADR-229 audience
// label. Sharing never mints values — this guards the event-fed copy before
// it reaches the CHECK-constrained column (fail loud in the subscriber, not
// as an opaque constraint violation).
func ValidReuseVisibility(s string) bool {
	switch s {
	case "private", "friends", "tenant":
		return true
	}
	return false
}

// StemPreview returns the first 140 chars of Stem for feed-card rendering,
// truncated on a rune boundary so multi-byte stems aren't split mid-glyph.
func (p Projection) StemPreview(max int) string {
	if max <= 0 {
		max = 140
	}
	r := []rune(p.Stem)
	if len(r) <= max {
		return p.Stem
	}
	return string(r[:max])
}

// Validate enforces the projection invariants. Used by the subscriber before
// caching so a malformed event never poisons the R1 attribution source.
//
// CONSENT-FIRST (CHO-2174b). The projection is a CONSENT read-model first and a
// question-display cache second, so ONLY the consent facts are mandatory:
//
//	atom_id    — the subject of the consent decision
//	owner_gcid — the R1 author of record; the `own` leg of the ADR-229 disjunct
//
// The question fields (RevisionID / Stem / QuestionType) are OPTIONAL
// enrichment. Requiring RevisionID here was a modelling defect: an atom whose
// consent is perfectly well-defined but which carries no QUESTION revision
// (it is revisioned on the atom itself) could not be projected at all — so the
// ADR-229 gate saw no projection row and refused it forever
// (AuthorizeAtomUse → ErrNotFound → FailedPrecondition "atom not published or
// withdrawn (412)"), blocking a live user path (a non-owner converting the atom
// into a study list, ADR-233 WS-4).
//
// Consumers that genuinely NEED a question keep their own precondition and
// refuse EXPLICITLY — see HasQuestion / HasPinnedRevision. Absent question data
// is never fabricated and never silently tolerated by a consumer that needs it.
func (p Projection) Validate() error {
	if strings.TrimSpace(p.AtomID) == "" {
		return fmt.Errorf("%w: atom_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(p.OwnerGCID) == "" {
		return fmt.Errorf("%w: owner_gcid required (R1 attribution source)", ErrInvalidArgument)
	}
	return nil
}

// HasQuestion reports whether the atom has a QUESTION REVISION — the single
// reliable discriminator between a question-bearing atom and a consent-only one.
// Every consumer that genuinely needs a question (ShareAtom's feed card,
// AuthorizeAtomUse's grant snapshot, a LiveQuiz arm) MUST gate on this and
// refuse EXPLICITLY, so a question-less atom is never used as though it had one.
//
// ⚠ It deliberately does NOT consider Stem. Verified against the live DB
// (2026-07-14): stem is EMPTY on 100% of projected rows (97/97) — the publish
// event does not populate it in practice — so requiring a stem here would refuse
// EVERY share in production. `revision_id` is the honest signal: it resolves in
// chora_creation.question_revisions, so its presence means "this atom has a
// question revision" and its absence means "it does not".
func (p Projection) HasQuestion() bool {
	return p.HasPinnedRevision()
}

// HasPinnedRevision reports whether the projection carries a revision that can
// be PINNED into a snapshot. atom_usage_grants.atom_revision_id is NOT NULL, so
// a grant physically cannot be minted without one: AuthorizeAtomUse MUST gate on
// this and refuse loudly rather than fabricate a revision or mint an unpinned
// grant.
func (p Projection) HasPinnedRevision() bool {
	return strings.TrimSpace(p.RevisionID) != ""
}

// AtomProjectionReader is the port (hexagonal seam) the ShareAtom + grant
// handlers use to resolve the cached projection for R1 author-validation.
// The pg adapter implements it; tests inject an in-memory double.
//
// The reader is a READ port only — mutation (upsert/invalidate) happens in
// the subscriber via the AtomProjectionWriter port below, keeping the
// read-path free of write side-effects.
type AtomProjectionReader interface {
	// Get returns the cached projection for atom_id, or ErrNotFound when the
	// atom has never been published (or has been archived). Callers MUST treat
	// a not-found as a hard refusal (R1 — cannot share/reuse an unknown atom).
	Get(ctx context.Context, atomID string) (Projection, error)
	// ListRandom returns up to `limit` random published atoms that are NOT
	// owned by either excludeGCID. Used by the matchmake handler as a
	// fallback when the AI atom picker fails.
	ListRandom(ctx context.Context, tenantID string, excludeGCIDs []string, limit int) ([]Projection, error)
}

// AtomProjectionWriter is the port the atom.published / atom.archived
// subscribers use to keep the cache fresh. The pg adapter implements it; the
// subscriber claims idempotency BEFORE calling these.
type AtomProjectionWriter interface {
	// Upsert caches (or refreshes) a projection from a published.v1 event.
	// Idempotent on atom_id — re-publishing the same event overwrites with
	// the latest revision without error.
	Upsert(ctx context.Context, p Projection) error
	// Invalidate marks the projection archived on atom.archived.v1 (the
	// creation "withdrawn" equivalent). Does NOT hard-delete — the row stays
	// for audit; the read-side + grant/entry revocation fan-out key off the
	// archived flag. Idempotent.
	Invalidate(ctx context.Context, atomID string) error
	// SetReuseVisibility applies an author audience change from
	// chora.creation.atom.reuse_visibility_changed.v1 (ADR-229 WS-1) onto
	// the cached projection. 0 matched rows is NOT an error — an atom that
	// was never published has no projection row; the eventual publish event
	// carries the current flag. Per Amendment A1.1 this NEVER touches
	// grants — narrowing enforcement is discovery-side only here.
	SetReuseVisibility(ctx context.Context, atomID, visibility string) error
}
