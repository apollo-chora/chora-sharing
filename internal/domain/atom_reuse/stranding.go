// Package atom_reuse is the pure-domain half of the ADR-229 Amendment A1
// orphan-edition saga inside Content Sharing (CHO-2132).
//
// chora-sharing owns the AtomUsageGrant table — the audit record of every
// consumed reuse — which makes it the authoritative registry of stranded
// consumers. When a withdrawal event arrives (reuse-visibility narrowing via
// chora.creation.atom.reuse_visibility_changed.v1, or an archive via
// chora.creation.atom.archived.v1), this package classifies WHICH active
// grants are stranded (left outside the new audience):
//
//   - narrow-to-private / archive → every non-author grant,
//   - tenant → friends            → grants whose grantee is not currently a
//     friend of the author (friend set = in-domain social.GraphQueries),
//   - widening / same-value       → nobody.
//
// Grants are NEVER revoked by a withdrawal (A1.1 — consumed-continuity is
// absolute): stranded grants REPOINT onto the singleton orphan edition that
// chora-creation mints (answered on chora.creation.atom.orphan_created.v1),
// with an append-only grant_events trail. Consumers still inside the
// audience keep their live reference.
//
// This package holds domain logic + ports only — NO HTTP, NO persistence,
// NO protobuf (hexagonal: the pg adapter + subscribers depend on it, never
// the reverse).
package atom_reuse

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// The three consent audiences (mirrors chora-creation's reuse_visibility).
const (
	AudiencePrivate = "private"
	AudienceFriends = "friends"
	AudienceTenant  = "tenant"
)

// Withdrawal trigger labels — the wire values on orphan_required.v1 /
// orphan_created.v1. "unshared" is RESERVED for a future feed-unshare
// surface; today's real triggers are narrowed + archived.
const (
	TriggerNarrowed = "narrowed"
	TriggerUnshared = "unshared"
	TriggerArchived = "archived"
)

// ValidAudience reports whether s is a canonical audience label (exact
// lowercase match — the wire contract).
func ValidAudience(s string) bool {
	switch s {
	case AudiencePrivate, AudienceFriends, AudienceTenant:
		return true
	}
	return false
}

// audienceRank orders the audiences by reach: private(0) < friends(1) <
// tenant(2).
func audienceRank(s string) (int, bool) {
	switch s {
	case AudiencePrivate:
		return 0, true
	case AudienceFriends:
		return 1, true
	case AudienceTenant:
		return 2, true
	}
	return 0, false
}

// Narrowed reports whether an audience change previous→next shrinks reach.
// Unknown/empty labels classify as NOT narrowing — callers must validate
// labels loud BEFORE classification (this keeps the classifier fail-safe:
// junk labels can never trigger a stranding sweep).
func Narrowed(previous, next string) bool {
	p, okP := audienceRank(previous)
	n, okN := audienceRank(next)
	if !okP || !okN {
		return false
	}
	return n < p
}

// GrantRef is the stranding-relevant slice of an active AtomUsageGrant.
type GrantRef struct {
	GrantID     string
	GranteeGCID string
	OwnerGCID   string // the atom author (R1)
	Scope       string
}

// isAuthorSelfGrant — own-atom grants (grantee == owner) are never stranded:
// the author cannot be outside their own audience.
func (g GrantRef) isAuthorSelfGrant() bool { return g.GranteeGCID == g.OwnerGCID }

// StrandedByAudience returns the grants left OUTSIDE the new audience.
// friends is the author's CURRENT friend set (only consulted for
// AudienceFriends). Unknown audiences strand nobody (fail-safe — validate
// upstream).
func StrandedByAudience(grants []GrantRef, audience string, friends map[string]bool) []GrantRef {
	switch audience {
	case AudiencePrivate:
		return strandedNonAuthor(grants, nil)
	case AudienceFriends:
		return strandedNonAuthor(grants, friends)
	case AudienceTenant:
		return nil
	default:
		return nil
	}
}

// StrandedByArchive returns every non-author grant — an archived atom has no
// audience left.
func StrandedByArchive(grants []GrantRef) []GrantRef {
	return strandedNonAuthor(grants, nil)
}

// strandedNonAuthor filters non-author grants, keeping those whose grantee
// is NOT in keep (nil keep = strand all non-author).
func strandedNonAuthor(grants []GrantRef, keep map[string]bool) []GrantRef {
	out := make([]GrantRef, 0, len(grants))
	for _, g := range grants {
		if g.isAuthorSelfGrant() {
			continue
		}
		if keep != nil && keep[g.GranteeGCID] {
			continue
		}
		out = append(out, g)
	}
	return out
}

// -----------------------------------------------------------------------------
// OrphanEdition — sharing's event-fed record of the singleton orphan
// -----------------------------------------------------------------------------

// OrphanEdition maps (original atom, last-published source revision) → the
// orphan edition chora-creation minted. Event-fed from
// chora.creation.atom.orphan_created.v1 and persisted in
// atom_orphan_editions (migration 0034) so a REPEAT withdrawal at the same
// revision repoints locally without round-tripping creation (whose idempotent
// mint deliberately emits nothing on a singleton conflict).
type OrphanEdition struct {
	AtomID           string // the withdrawn original
	SourceRevisionID string // the pinned last-published revision
	OrphanAtomID     string // the frozen orphan edition
	OrphanedAt       time.Time
}

// Validate enforces the mapping invariants.
func (e OrphanEdition) Validate() error {
	if strings.TrimSpace(e.AtomID) == "" {
		return errors.New("atom_reuse: orphan edition atom_id required")
	}
	if strings.TrimSpace(e.SourceRevisionID) == "" {
		return errors.New("atom_reuse: orphan edition source_revision_id required")
	}
	if strings.TrimSpace(e.OrphanAtomID) == "" {
		return errors.New("atom_reuse: orphan edition orphan_atom_id required")
	}
	return nil
}

// -----------------------------------------------------------------------------
// Ports (hexagonal seams — pg adapter implements; subscribers consume)
// -----------------------------------------------------------------------------

// GrantReads lists the ACTIVE, unexpired grants on an atom (RLS-scoped to
// the ctx tenant).
type GrantReads interface {
	ActiveGrantRefsForAtom(ctx context.Context, atomID string) ([]GrantRef, error)
}

// EditionStore persists + resolves the (atom, revision) → orphan mapping.
type EditionStore interface {
	// LatestEdition returns the most recent orphan edition for the atom
	// (false when none exists).
	LatestEdition(ctx context.Context, atomID string) (OrphanEdition, bool, error)
	// PutEdition upserts the mapping (idempotent on (atom, source revision)).
	PutEdition(ctx context.Context, e OrphanEdition) error
}

// RepointCommand instructs the repointer to move stranded grants
// original→orphan.
type RepointCommand struct {
	OriginalAtomID string
	OrphanAtomID   string
	Stranded       []GrantRef
	Trigger        string // narrowed | unshared | archived
	Note           string // audit context (source event id etc.)
}

// RepointResult summarises a repoint sweep.
type RepointResult struct {
	// Repointed — grants whose atom_id moved original→orphan.
	Repointed int
	// Merged — duplicate-coverage grants soft-deleted because the grantee
	// already holds an active grant on the orphan for the same scope (a
	// dedupe-merge, NEVER a revoke).
	Merged int
	// Skipped — grants no longer active on the original (already handled by
	// a previous delivery).
	Skipped int
}

// GrantRepointer executes the repoint sweep transactionally with append-only
// grant_events trail entries.
type GrantRepointer interface {
	RepointStranded(ctx context.Context, cmd RepointCommand) (RepointResult, error)
}

// OrphanRequest asks chora-creation to mint the singleton orphan edition.
type OrphanRequest struct {
	AtomID        string
	RevisionID    string // sharing's pinned-revision HINT (creation resolves authoritatively)
	Trigger       string
	StrandedCount int
	DetectedAt    time.Time
	ActorGCID     string // the withdrawing author (envelope gcid)
}

// Validate enforces the request invariants (fail loud before the outbox).
func (r OrphanRequest) Validate() error {
	if strings.TrimSpace(r.AtomID) == "" {
		return errors.New("atom_reuse: orphan request atom_id required")
	}
	switch r.Trigger {
	case TriggerNarrowed, TriggerUnshared, TriggerArchived:
	default:
		return fmt.Errorf("atom_reuse: orphan request trigger %q invalid", r.Trigger)
	}
	if r.StrandedCount < 1 {
		return fmt.Errorf("atom_reuse: orphan request stranded_count %d < 1 (zero-stranded withdrawals emit nothing)", r.StrandedCount)
	}
	return nil
}

// OrphanRequirer publishes chora.sharing.atom_reuse.orphan_required.v1 via
// the same-transaction outbox.
type OrphanRequirer interface {
	RequireOrphan(ctx context.Context, req OrphanRequest) error
}

// ProjectionState is the stranding-relevant slice of the cached atom
// projection, INCLUDING archived rows (the repoint recompute needs the
// archived flag + pinned revision).
type ProjectionState struct {
	RevisionID      string
	OwnerGCID       string
	ReuseVisibility string
	Archived        bool
}

// ErrProjectionNotFound is returned by ProjectionReads.GetAnyState when the
// atom was never projected (never published). Callers treat it as
// "no audience left" — strand all non-author grants.
var ErrProjectionNotFound = errors.New("atom_reuse: atom projection not found")

// ProjectionReads resolves the projection state for stranding decisions.
type ProjectionReads interface {
	GetAnyState(ctx context.Context, atomID string) (ProjectionState, error)
}

// FriendReads resolves the author's current friend set (implemented by the
// social graph repo behind social.GraphQueries — structurally identical, no
// package coupling).
type FriendReads interface {
	FriendSet(ctx context.Context, tenantID, gcid string) ([]string, error)
}
