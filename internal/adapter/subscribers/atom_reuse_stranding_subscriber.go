// atom_reuse_stranding_subscriber.go — ADR-229 Amendment A1 (CHO-2132): the
// sharing legs of the two-leg orphan-edition saga.
//
// LEG 1 — detection. chora-sharing owns the AtomUsageGrant table (the audit
// record of every consumed reuse), which makes it the authoritative registry
// of stranded consumers. On a withdrawal event —
//
//	chora.creation.atom.reuse_visibility_changed.v1 (narrowing only; the
//	  event carries previous_visibility so classification is stateless)
//	chora.creation.atom.archived.v1
//
// — the detector computes the STRANDED active grants (narrow-to-private /
// archive strand every non-author grant; tenant→friends strands grantees
// outside the author's current friend set). Then:
//
//   - zero stranded → emit nothing (the Zero-stranded AC);
//   - an orphan edition is already known for the atom's CURRENT pinned
//     revision (the event-fed atom_orphan_editions map) → repoint LOCALLY.
//     Creation's idempotent mint deliberately emits nothing on a singleton
//     conflict, so a repeat withdrawal at the same revision must never
//     depend on a fresh orphan_created round-trip;
//   - otherwise → publish chora.sharing.atom_reuse.orphan_required.v1 via
//     the same-tx outbox; creation answers with atom.orphan_created.v1.
//
// LEG 3 — repoint. On chora.creation.atom.orphan_created.v1 the subscriber
// stores the (atom, source-revision) → orphan mapping and repoints the
// grants stranded per the CURRENT projection state (a RECOMPUTE — any wave
// stranded between detection and mint still repoints; an atom re-widened
// in flight repoints nobody: live reference while shared). Grants are NEVER
// revoked (A1.1); the repointer writes the append-only grant_events trail.
//
// KNOWN BOUNDED GAP (documented per fail-loud): if a fresh stranding
// withdrawal reaches creation while sharing's projection revision is
// desynced from the edition map (cross-topic ordering race: orphan_created
// processed while the atom.published that bumped the projection revision is
// still in flight, plus an author flip-flop inside that window), creation's
// mint conflicts and emits nothing, and that wave stays un-repointed until
// the NEXT stranding trigger after the projection catches up (the detector
// sweeps ALL currently-stranded actives, healing prior waves). The upgrade
// path — creation re-affirming orphan_created on a fresh conflict — is
// recorded in the Jira completion comment.
//
// Composition: the wiring runs these handlers AFTER the AtomProjection
// handlers on the SAME deliveries (separate idempotency keys per handler, each
// peek→process→mark per CHO-2263, so a crash between the two legs replays only
// the unfinished one — and a post-peek failure re-runs rather than ACK-dropping).
package subscribers

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protodecode"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_reuse"
)

// TopicAtomOrphanCreated is the creation-side answer leg this subscriber
// consumes for repoints.
const TopicAtomOrphanCreated = "chora.creation.atom.orphan_created.v1"

// Idempotency-key prefixes (one per handler so a crash between the
// projection leg and the stranding leg replays only the unfinished one).
const (
	HandlerReuseStrandingVisibility = "atom_reuse:stranding:visibility"
	HandlerReuseStrandingArchived   = "atom_reuse:stranding:archived"
	HandlerReuseOrphanCreated       = "atom_reuse:repoint:orphan_created"
)

// AtomOrphanCreatedEnvelope mirrors the orphan_created.v1 payload + envelope.
type AtomOrphanCreatedEnvelope struct {
	EventID  string `json:"event_id"`
	TenantID string `json:"tenant_id"`
	GCID     string `json:"gcid"`

	OrphanAtomID       string `json:"orphan_atom_id"`
	OrphanedFromAtomID string `json:"orphaned_from_atom_id"`
	SourceRevisionID   string `json:"source_revision_id"`
	AuthorGCID         string `json:"author_gcid"`
	Trigger            string `json:"trigger"`
}

// AtomReuseStrandingConfig bundles the subscriber's ports.
type AtomReuseStrandingConfig struct {
	Grants      atom_reuse.GrantReads
	Editions    atom_reuse.EditionStore
	Repointer   atom_reuse.GrantRepointer
	Requirer    atom_reuse.OrphanRequirer
	Projections atom_reuse.ProjectionReads
	Friends     atom_reuse.FriendReads
	Idempotency IdempotencyStore
	Logger      Logger
	Now         func() time.Time
}

// AtomReuseStrandingSubscriber handles the sharing legs of the orphan saga.
type AtomReuseStrandingSubscriber struct {
	cfg AtomReuseStrandingConfig
}

// NewAtomReuseStrandingSubscriber builds the subscriber with sane defaults.
func NewAtomReuseStrandingSubscriber(cfg AtomReuseStrandingConfig) *AtomReuseStrandingSubscriber {
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	return &AtomReuseStrandingSubscriber{cfg: cfg}
}

func (s *AtomReuseStrandingSubscriber) preflight() error {
	c := s.cfg
	if c.Grants == nil || c.Editions == nil || c.Repointer == nil ||
		c.Requirer == nil || c.Projections == nil || c.Friends == nil || c.Idempotency == nil {
		return errors.New("subscribers: atom_reuse stranding missing dependency")
	}
	return nil
}

// HandleReuseVisibilityChanged is the LEG-1 narrowing detector.
func (s *AtomReuseStrandingSubscriber) HandleReuseVisibilityChanged(ctx context.Context, e AtomReuseVisibilityChangedEnvelope) error {
	if err := s.preflight(); err != nil {
		return err
	}
	if err := validateBase(e.EventID, e.TenantID, e.GCID); err != nil {
		return err
	}
	if strings.TrimSpace(e.AtomID) == "" {
		return errors.New("subscribers: atom_id required for reuse_visibility stranding")
	}
	if !atom_reuse.ValidAudience(e.ReuseVisibility) {
		return fmt.Errorf("subscribers: stranding: new audience %q invalid", e.ReuseVisibility)
	}
	// previous_visibility is the stateless-classification input (WS-1 pinned
	// it on the wire) — an event without it cannot be classified; NACK loud
	// rather than guessing (a wrong guess either over-strands or silently
	// breaks continuity).
	if !atom_reuse.ValidAudience(e.PreviousVisibility) {
		return fmt.Errorf("subscribers: stranding: previous audience %q invalid (cannot classify narrowing statelessly)", e.PreviousVisibility)
	}
	if !atom_reuse.Narrowed(e.PreviousVisibility, e.ReuseVisibility) {
		return nil // widening / same-value — never strands
	}

	return processOnce(ctx, s.cfg.Idempotency, HandlerReuseStrandingVisibility, e.EventID,
		func() { s.logDuplicate(HandlerReuseStrandingVisibility, e.EventID, e.TenantID) },
		func() error {
			return s.detectAndAct(ctx, strandingInput{
				TenantID:    e.TenantID,
				AtomID:      e.AtomID,
				NewAudience: e.ReuseVisibility,
				Trigger:     atom_reuse.TriggerNarrowed,
				ActorGCID:   firstNonEmpty(e.AuthorGCID, e.GCID),
				EventID:     e.EventID,
			})
		})
}

// HandleAtomArchivedStranding is the LEG-1 archive detector (an archived atom
// has no audience left — every non-author grant strands).
func (s *AtomReuseStrandingSubscriber) HandleAtomArchivedStranding(ctx context.Context, e AtomArchivedEnvelope) error {
	if err := s.preflight(); err != nil {
		return err
	}
	if err := validateBase(e.EventID, e.TenantID, e.GCID); err != nil {
		return err
	}
	if strings.TrimSpace(e.AtomID) == "" {
		return errors.New("subscribers: atom_id required for archived stranding")
	}

	return processOnce(ctx, s.cfg.Idempotency, HandlerReuseStrandingArchived, e.EventID,
		func() { s.logDuplicate(HandlerReuseStrandingArchived, e.EventID, e.TenantID) },
		func() error {
			return s.detectAndAct(ctx, strandingInput{
				TenantID:  e.TenantID,
				AtomID:    e.AtomID,
				Archived:  true,
				Trigger:   atom_reuse.TriggerArchived,
				ActorGCID: e.GCID,
				EventID:   e.EventID,
			})
		})
}

// strandingInput normalises the two detection triggers.
type strandingInput struct {
	TenantID    string
	AtomID      string
	NewAudience string // narrowing only
	Archived    bool
	Trigger     string
	ActorGCID   string
	EventID     string
}

// detectAndAct computes the stranded set and either repoints locally (known
// current edition) or asks creation to mint.
func (s *AtomReuseStrandingSubscriber) detectAndAct(ctx context.Context, in strandingInput) error {
	grants, err := s.cfg.Grants.ActiveGrantRefsForAtom(ctx, in.AtomID)
	if err != nil {
		return fmt.Errorf("subscribers: stranding: list active grants for %s: %w", in.AtomID, err)
	}
	stranded, err := s.classify(ctx, in.TenantID, in.NewAudience, in.Archived, grants)
	if err != nil {
		return err
	}
	if len(stranded) == 0 {
		s.cfg.Logger.Printf(`{"event":"atom_reuse_zero_stranded","atom_id":"%s","tenant_id":"%s","trigger":"%s","active_grants":%d}`,
			in.AtomID, in.TenantID, in.Trigger, len(grants))
		return nil
	}

	// Projection state — the pinned revision decides edition freshness (read
	// ANY-state: the archive leg runs after Invalidate flipped the flag).
	var pinnedRevision string
	if st, perr := s.cfg.Projections.GetAnyState(ctx, in.AtomID); perr == nil {
		pinnedRevision = st.RevisionID
	} else if !errors.Is(perr, atom_reuse.ErrProjectionNotFound) {
		return fmt.Errorf("subscribers: stranding: read projection %s: %w", in.AtomID, perr)
	}

	if edition, known, eerr := s.cfg.Editions.LatestEdition(ctx, in.AtomID); eerr != nil {
		return fmt.Errorf("subscribers: stranding: read orphan edition %s: %w", in.AtomID, eerr)
	} else if known && pinnedRevision != "" && edition.SourceRevisionID == pinnedRevision {
		// The singleton for this revision already exists — repoint locally.
		// (Creation's mint emits nothing on conflict by design.) The sweep
		// covers ALL currently-stranded actives, healing any wave a prior
		// race left behind.
		res, rerr := s.cfg.Repointer.RepointStranded(ctx, atom_reuse.RepointCommand{
			OriginalAtomID: in.AtomID,
			OrphanAtomID:   edition.OrphanAtomID,
			Stranded:       stranded,
			Trigger:        in.Trigger,
			Note:           "local repoint on repeat withdrawal; source_event=" + in.EventID,
		})
		if rerr != nil {
			return fmt.Errorf("subscribers: stranding: local repoint %s->%s: %w", in.AtomID, edition.OrphanAtomID, rerr)
		}
		s.cfg.Logger.Printf(`{"event":"atom_reuse_local_repoint","atom_id":"%s","orphan_atom_id":"%s","tenant_id":"%s","trigger":"%s","repointed":%d,"merged":%d,"skipped":%d}`,
			in.AtomID, edition.OrphanAtomID, in.TenantID, in.Trigger, res.Repointed, res.Merged, res.Skipped)
		return nil
	}

	// No usable edition — ask creation to mint (it resolves the authoritative
	// last-published revision itself; ours is a hint).
	req := atom_reuse.OrphanRequest{
		AtomID:        in.AtomID,
		RevisionID:    pinnedRevision,
		Trigger:       in.Trigger,
		StrandedCount: len(stranded),
		DetectedAt:    s.cfg.Now(),
		ActorGCID:     in.ActorGCID,
	}
	if err := s.cfg.Requirer.RequireOrphan(ctx, req); err != nil {
		return fmt.Errorf("subscribers: stranding: require orphan for %s: %w", in.AtomID, err)
	}
	s.cfg.Logger.Printf(`{"event":"atom_reuse_orphan_required","atom_id":"%s","tenant_id":"%s","trigger":"%s","stranded":%d,"revision_hint":"%s"}`,
		in.AtomID, in.TenantID, in.Trigger, len(stranded), pinnedRevision)
	return nil
}

// classify maps the trigger onto the domain stranding rules.
func (s *AtomReuseStrandingSubscriber) classify(ctx context.Context, tenantID, newAudience string, archived bool, grants []atom_reuse.GrantRef) ([]atom_reuse.GrantRef, error) {
	if archived {
		return atom_reuse.StrandedByArchive(grants), nil
	}
	var friends map[string]bool
	if newAudience == atom_reuse.AudienceFriends {
		set, err := s.friendSetOfAuthor(ctx, tenantID, grants)
		if err != nil {
			return nil, err
		}
		friends = set
	}
	return atom_reuse.StrandedByAudience(grants, newAudience, friends), nil
}

// friendSetOfAuthor resolves the ATOM AUTHOR's current friend set (the
// author is every grant's OwnerGCID — same atom, same author).
func (s *AtomReuseStrandingSubscriber) friendSetOfAuthor(ctx context.Context, tenantID string, grants []atom_reuse.GrantRef) (map[string]bool, error) {
	author := ""
	for _, g := range grants {
		if g.OwnerGCID != "" {
			author = g.OwnerGCID
			break
		}
	}
	if author == "" {
		return nil, nil // no grants → no stranding anyway
	}
	list, err := s.cfg.Friends.FriendSet(ctx, tenantID, author)
	if err != nil {
		return nil, fmt.Errorf("subscribers: stranding: friend set for author %s: %w", author, err)
	}
	set := make(map[string]bool, len(list))
	for _, f := range list {
		set[f] = true
	}
	return set, nil
}

// HandleAtomOrphanCreated is LEG 3 — store the edition + repoint the grants
// stranded per the CURRENT projection state.
func (s *AtomReuseStrandingSubscriber) HandleAtomOrphanCreated(ctx context.Context, e AtomOrphanCreatedEnvelope) error {
	if err := s.preflight(); err != nil {
		return err
	}
	if err := validateBase(e.EventID, e.TenantID, e.GCID); err != nil {
		return err
	}
	if strings.TrimSpace(e.OrphanAtomID) == "" || strings.TrimSpace(e.OrphanedFromAtomID) == "" || strings.TrimSpace(e.SourceRevisionID) == "" {
		return errors.New("subscribers: orphan_created requires orphan_atom_id + orphaned_from_atom_id + source_revision_id")
	}

	return processOnce(ctx, s.cfg.Idempotency, HandlerReuseOrphanCreated, e.EventID,
		func() { s.logDuplicate(HandlerReuseOrphanCreated, e.EventID, e.TenantID) },
		func() error { return s.repointOnOrphanCreated(ctx, e) })
}

// repointOnOrphanCreated is the post-peek work for HandleAtomOrphanCreated
// (LEG 3): store the edition mapping, recompute the stranded set from CURRENT
// state, and repoint. Extracted so processOnce wraps a single fn and marks the
// idempotency key ONLY after this returns nil (CHO-2263) — a failure anywhere
// returns unmarked and the redelivery re-runs, safe because every write is
// idempotent (PutEdition upserts, RepointStranded skips already-repointed grants).
func (s *AtomReuseStrandingSubscriber) repointOnOrphanCreated(ctx context.Context, e AtomOrphanCreatedEnvelope) error {
	// 1. Store the edition mapping FIRST — even a zero-repoint delivery must
	// leave the map behind so the next withdrawal repoints locally.
	if err := s.cfg.Editions.PutEdition(ctx, atom_reuse.OrphanEdition{
		AtomID:           e.OrphanedFromAtomID,
		SourceRevisionID: e.SourceRevisionID,
		OrphanAtomID:     e.OrphanAtomID,
		OrphanedAt:       s.cfg.Now(),
	}); err != nil {
		return fmt.Errorf("subscribers: store orphan edition (%s,%s): %w", e.OrphanedFromAtomID, e.SourceRevisionID, err)
	}

	// 2. Recompute the stranded set from CURRENT state (self-healing under
	// in-flight waves and re-widenings).
	grants, err := s.cfg.Grants.ActiveGrantRefsForAtom(ctx, e.OrphanedFromAtomID)
	if err != nil {
		return fmt.Errorf("subscribers: orphan_created: list active grants for %s: %w", e.OrphanedFromAtomID, err)
	}
	if len(grants) == 0 {
		s.cfg.Logger.Printf(`{"event":"orphan_created_no_active_grants","atom_id":"%s","orphan_atom_id":"%s","tenant_id":"%s"}`,
			e.OrphanedFromAtomID, e.OrphanAtomID, e.TenantID)
		return nil
	}
	var stranded []atom_reuse.GrantRef
	st, perr := s.cfg.Projections.GetAnyState(ctx, e.OrphanedFromAtomID)
	switch {
	case errors.Is(perr, atom_reuse.ErrProjectionNotFound):
		// Never projected → no audience anyone could be inside of.
		stranded = atom_reuse.StrandedByArchive(grants)
	case perr != nil:
		return fmt.Errorf("subscribers: orphan_created: read projection %s: %w", e.OrphanedFromAtomID, perr)
	case st.Archived:
		stranded = atom_reuse.StrandedByArchive(grants)
	default:
		var err2 error
		stranded, err2 = s.classify(ctx, e.TenantID, st.ReuseVisibility, false, grants)
		if err2 != nil {
			return err2
		}
	}
	if len(stranded) == 0 {
		s.cfg.Logger.Printf(`{"event":"orphan_created_nobody_stranded","atom_id":"%s","orphan_atom_id":"%s","tenant_id":"%s"}`,
			e.OrphanedFromAtomID, e.OrphanAtomID, e.TenantID)
		return nil
	}

	// 3. Repoint (grants NEVER revoked — A1.1; append-only trail inside).
	res, err := s.cfg.Repointer.RepointStranded(ctx, atom_reuse.RepointCommand{
		OriginalAtomID: e.OrphanedFromAtomID,
		OrphanAtomID:   e.OrphanAtomID,
		Stranded:       stranded,
		Trigger:        e.Trigger,
		Note:           "orphan_created source_event=" + e.EventID,
	})
	if err != nil {
		return fmt.Errorf("subscribers: orphan_created: repoint %s->%s: %w", e.OrphanedFromAtomID, e.OrphanAtomID, err)
	}
	s.cfg.Logger.Printf(`{"event":"orphan_created_repointed","atom_id":"%s","orphan_atom_id":"%s","tenant_id":"%s","trigger":"%s","repointed":%d,"merged":%d,"skipped":%d}`,
		e.OrphanedFromAtomID, e.OrphanAtomID, e.TenantID, e.Trigger, res.Repointed, res.Merged, res.Skipped)
	return nil
}

func (s *AtomReuseStrandingSubscriber) logDuplicate(handler, eventID, tenantID string) {
	s.cfg.Logger.Printf(`{"event":"duplicate_event_skipped","handler":"%s","source_event_id":"%s","tenant_id":"%s"}`,
		handler, eventID, tenantID)
}

// -----------------------------------------------------------------------------
// Wire decode — orphan_created.v1
// -----------------------------------------------------------------------------

// DecodeAtomOrphanCreatedWithAttrs decodes orphan_created.v1 wire bytes via
// the protodecode registry (binary-first, attrs envelope fallback).
func DecodeAtomOrphanCreatedWithAttrs(blob []byte, attrs map[string]string) (AtomOrphanCreatedEnvelope, error) {
	m, err := protodecode.DecodePayloadMapWithAttrs(TopicAtomOrphanCreated, blob, attrs)
	if err != nil {
		return AtomOrphanCreatedEnvelope{}, fmt.Errorf("subscribers: decode atom.orphan_created: %w", err)
	}
	return AtomOrphanCreatedEnvelope{
		EventID:            asString(m["event_id"]),
		TenantID:           asString(m["tenant_id"]),
		GCID:               firstNonEmpty(asString(m["gcid"]), asString(m["author_gcid"])),
		OrphanAtomID:       asString(m["orphan_atom_id"]),
		OrphanedFromAtomID: asString(m["orphaned_from_atom_id"]),
		SourceRevisionID:   asString(m["source_revision_id"]),
		AuthorGCID:         asString(m["author_gcid"]),
		Trigger:            asString(m["trigger"]),
	}, nil
}
