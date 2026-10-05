// atom_projection_subscriber.go — Atom Sharing Redesign (specs/001-atom-sharing-redesign) T011.
//
// AtomProjectionSubscriber keeps the cached AtomProjection read-model fresh
// so the ShareAtom endpoint can enforce R1 (author-of-record) without a
// cross-DB read of chora_creation. It consumes two Content Creation events:
//
//	chora.creation.atom.published.v1  → Upsert (cache/refresh the projection)
//	chora.creation.atom.archived.v1   → Invalidate (mark withdrawn; revoke
//	                                   outstanding feed entries + grants)
//
// Reconciliation note: the plan/spec refer to "atom.withdrawn"; the actual
// shipped topic is chora.creation.atom.archived.v1 (AtomArchived carries a
// status + reason — the creation-domain "withdrawn" equivalent). This
// subscriber maps archived → Invalidate. There is no separate withdrawn topic.
//
// Idempotency (CHO-2263 seen→process→mark): the (handler, event_id) pair is
// PEEKED in the shared IdempotencyStore, the upsert/invalidate runs, and the
// pair is MARKED only AFTER it lands (via processOnce). A post-peek failure
// returns unmarked, so the redelivery re-projects rather than being ACK-dropped.
// Re-delivery (Pub/Sub at-least-once) skips with a "duplicate_event_skipped"
// structured-log span event. The HTTP push handler / bus handler acks AFTER this
// method returns nil; a returned error nacks → broker retries → eventually
// deadletters (DLQ), mirroring the LiveQuizScoreSubscriber +
// FamiliarMilestoneSubscriber delivery-resilience pattern.
//
// Hexagonal: depends on the atom_projection domain (Reader/Writer ports) +
// the internal IdempotencyStore port. cmd/server wires the production pg
// adapter + idempotency store; tests inject in-memory doubles.
package subscribers

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protodecode"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
)

// TopicAtomPublished is the Content Creation event that refreshes the cache.
const TopicAtomPublished = "chora.creation.atom.published.v1"

// TopicAtomArchived is the Content Creation event that withdraws an atom
// (the "withdrawn" equivalent — see package doc).
const TopicAtomArchived = "chora.creation.atom.archived.v1"

// TopicAtomReuseVisibilityChanged is the ADR-229 WS-1 (CHO-2127) author
// audience-change event; the subscriber maps the NEW audience onto
// atom_projections.reuse_visibility. Per Amendment A1.1 narrowing NEVER
// revokes grants — this handler is discovery-side only.
const TopicAtomReuseVisibilityChanged = "chora.creation.atom.reuse_visibility_changed.v1"

// HandlerAtomPublished / HandlerAtomArchived / HandlerAtomReuseVisibility are
// the idempotency-key prefixes + OTLP span attribute
// "chora.subscriber.handler" for each path.
const (
	HandlerAtomPublished       = "atom_projection:published"
	HandlerAtomArchived        = "atom_projection:archived"
	HandlerAtomReuseVisibility = "atom_projection:reuse_visibility"
)

// AtomPublishedEnvelope mirrors the published.v1 payload + the canonical
// envelope fields the publisher places on Pub/Sub msg.Attributes.
type AtomPublishedEnvelope struct {
	// Envelope fields (carried on Pub/Sub attributes).
	EventID  string `json:"event_id"`
	TenantID string `json:"tenant_id"`
	GCID     string `json:"gcid"`

	// Payload fields (AtomPublished).
	AtomID            string `json:"atom_id"`
	RevisionID        string `json:"current_revision_id"`
	AuthorGCID        string `json:"author_gcid"` // = owner_gcid (R1)
	AuthorDisplayName string `json:"author_display_name"`
	Title             string `json:"title"`
	Stem              string `json:"stem"`
	QuestionType      string `json:"question_type"`
	Status            string `json:"status"`
	// Options — MCQ answer choices (spec §5.1 Option A). Empty for OE atoms.
	Options []string `json:"options"`
	// CorrectAnswer — the correct option string (spec §5.1 Option A). Empty for OE.
	CorrectAnswer string `json:"correct_answer"`
	// ReuseVisibility — ADR-229 WS-1 audience label (field 30). EMPTY when
	// the producer pre-dates ADR-229; the pg layer hardens/preserves.
	ReuseVisibility string `json:"reuse_visibility"`
}

// AtomReuseVisibilityChangedEnvelope mirrors the reuse_visibility_changed.v1
// payload (ADR-229 WS-1, CHO-2127) + the canonical envelope fields.
type AtomReuseVisibilityChangedEnvelope struct {
	EventID  string `json:"event_id"`
	TenantID string `json:"tenant_id"`
	GCID     string `json:"gcid"`

	AtomID             string `json:"atom_id"`
	ReuseVisibility    string `json:"reuse_visibility"`    // the NEW audience
	PreviousVisibility string `json:"previous_visibility"` // before the change
	AuthorGCID         string `json:"author_gcid"`
}

// AtomArchivedEnvelope mirrors the archived.v1 payload.
type AtomArchivedEnvelope struct {
	EventID  string `json:"event_id"`
	TenantID string `json:"tenant_id"`
	GCID     string `json:"gcid"`
	AtomID   string `json:"atom_id"`
	Status   string `json:"status"`
	Reason   string `json:"reason"`
}

// AtomProjectionConfig bundles the subscriber's dependencies.
type AtomProjectionConfig struct {
	// Writer mutates the cached projection. Required.
	Writer atom_projection.AtomProjectionWriter
	// Idempotency dedups (handler, event_id). Required.
	Idempotency IdempotencyStore
	// Logger is optional; falls back to log.Default().
	Logger Logger
}

// AtomProjectionSubscriber refreshes / invalidates the cached AtomProjection.
type AtomProjectionSubscriber struct {
	cfg AtomProjectionConfig
}

// NewAtomProjectionSubscriber builds a subscriber with sane defaults.
func NewAtomProjectionSubscriber(cfg AtomProjectionConfig) *AtomProjectionSubscriber {
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	return &AtomProjectionSubscriber{cfg: cfg}
}

// SubscribedTopics returns the three Creation events this subscriber listens to.
func (s *AtomProjectionSubscriber) SubscribedTopics() []string {
	return []string{TopicAtomPublished, TopicAtomArchived, TopicAtomReuseVisibilityChanged}
}

// HandleAtomPublished processes one published.v1 event: it idempotently
// upserts the cached projection so the ShareAtom endpoint can validate
// authorship (R1) + render the feed card without a cross-DB read.
func (s *AtomProjectionSubscriber) HandleAtomPublished(ctx context.Context, e AtomPublishedEnvelope) error {
	if err := s.preflight(); err != nil {
		return err
	}
	if err := validateBase(e.EventID, e.TenantID, e.GCID); err != nil {
		return err
	}
	if strings.TrimSpace(e.AtomID) == "" {
		return fmt.Errorf("subscribers: atom_id required for published event")
	}

	// Peek → work → mark (CHO-2263): the dedup row is written ONLY after the
	// Upsert lands, so a transient failure NACKs unmarked and the redelivery
	// re-projects rather than being silently ACK-dropped.
	return processOnce(ctx, s.cfg.Idempotency, HandlerAtomPublished, e.EventID,
		func() { s.spanEventDuplicate(HandlerAtomPublished, e.EventID, e.TenantID, e.GCID) },
		func() error {
			// Build + validate the projection before caching so a malformed event
			// never poisons the R1 attribution source. ReuseVisibility rides
			// verbatim — EMPTY (pre-ADR-229 producer) is legal; the pg layer hardens
			// on insert and preserves on conflict-update.
			p := atom_projection.Projection{
				AtomID:            e.AtomID,
				TenantID:          e.TenantID,
				RevisionID:        e.RevisionID,
				OwnerGCID:         firstNonEmpty(e.AuthorGCID, e.GCID),
				AuthorDisplayName: e.AuthorDisplayName,
				Stem:              e.Stem,
			QuestionType:      atom_projection.QuestionType(e.QuestionType),
			ReuseVisibility:   e.ReuseVisibility,
			Options:           append([]string(nil), e.Options...),
			CorrectAnswer:     e.CorrectAnswer,
			}
			if err := p.Validate(); err != nil {
				// A published event without an owner_gcid breaks R1 — fail loud so
				// the broker retries / deadletters rather than silently dropping.
				return fmt.Errorf("subscribers: atom.published projection invalid: %w", err)
			}

			if err := s.cfg.Writer.Upsert(ctx, p); err != nil {
				return fmt.Errorf("subscribers: upsert atom projection: %w", err)
			}

			s.logAudit(HandlerAtomPublished, e.EventID, e.TenantID, e.GCID, "upserted")
			return nil
		})
}

// HandleAtomArchived processes one archived.v1 event: it marks the cached
// projection archived (withdrawn). The read-side excludes archived
// projections; the grant/feed-entry revocation fan-out keys off the archived
// flag. Hard-delete is NEVER performed (soft-delete + pseudonymise invariant).
func (s *AtomProjectionSubscriber) HandleAtomArchived(ctx context.Context, e AtomArchivedEnvelope) error {
	if err := s.preflight(); err != nil {
		return err
	}
	if err := validateBase(e.EventID, e.TenantID, e.GCID); err != nil {
		return err
	}
	if strings.TrimSpace(e.AtomID) == "" {
		return fmt.Errorf("subscribers: atom_id required for archived event")
	}

	return processOnce(ctx, s.cfg.Idempotency, HandlerAtomArchived, e.EventID,
		func() { s.spanEventDuplicate(HandlerAtomArchived, e.EventID, e.TenantID, e.GCID) },
		func() error {
			if err := s.cfg.Writer.Invalidate(ctx, e.AtomID); err != nil {
				return fmt.Errorf("subscribers: invalidate atom projection: %w", err)
			}
			s.logAudit(HandlerAtomArchived, e.EventID, e.TenantID, e.GCID, "invalidated")
			return nil
		})
}

// HandleAtomReuseVisibilityChanged processes one reuse_visibility_changed.v1
// event (ADR-229 WS-1): it idempotently applies the NEW audience onto the
// cached projection so pickers/snapshots/quiz-arms see the change
// immediately. An unknown label NACKs (fail loud) — silently applying it
// would corrupt the consent gate or trip the DB CHECK with less context. Per
// Amendment A1.1 grants are NEVER touched here.
func (s *AtomProjectionSubscriber) HandleAtomReuseVisibilityChanged(ctx context.Context, e AtomReuseVisibilityChangedEnvelope) error {
	if err := s.preflight(); err != nil {
		return err
	}
	if err := validateBase(e.EventID, e.TenantID, e.GCID); err != nil {
		return err
	}
	if strings.TrimSpace(e.AtomID) == "" {
		return fmt.Errorf("subscribers: atom_id required for reuse_visibility_changed event")
	}
	if !atom_projection.ValidReuseVisibility(e.ReuseVisibility) {
		return fmt.Errorf("subscribers: reuse_visibility %q invalid on changed event (want private|friends|tenant)", e.ReuseVisibility)
	}

	return processOnce(ctx, s.cfg.Idempotency, HandlerAtomReuseVisibility, e.EventID,
		func() { s.spanEventDuplicate(HandlerAtomReuseVisibility, e.EventID, e.TenantID, e.GCID) },
		func() error {
			if err := s.cfg.Writer.SetReuseVisibility(ctx, e.AtomID, e.ReuseVisibility); err != nil {
				return fmt.Errorf("subscribers: set atom projection reuse_visibility: %w", err)
			}
			s.logAudit(HandlerAtomReuseVisibility, e.EventID, e.TenantID, e.GCID, "reuse_visibility_"+e.ReuseVisibility)
			return nil
		})
}

func (s *AtomProjectionSubscriber) preflight() error {
	if s.cfg.Writer == nil || s.cfg.Idempotency == nil {
		return errors.New("subscribers: missing dependency (Writer | Idempotency)")
	}
	return nil
}

func (s *AtomProjectionSubscriber) spanEventDuplicate(handler, eventID, tenantID, gcid string) {
	s.cfg.Logger.Printf(`{"event":"duplicate_event_skipped","handler":"%s","source_event_id":"%s","tenant_id":"%s","gcid":"%s"}`,
		handler, eventID, tenantID, gcid)
}

func (s *AtomProjectionSubscriber) logAudit(handler, eventID, tenantID, gcid, outcome string) {
	s.cfg.Logger.Printf(`{"event":"atom_projection_handled","handler":"%s","source_event_id":"%s","tenant_id":"%s","gcid":"%s","outcome":"%s"}`,
		handler, eventID, tenantID, gcid, outcome)
}

// -----------------------------------------------------------------------------
// Wire decoders
// -----------------------------------------------------------------------------

// DecodeAtomPublishedWithAttrs is the attribute-aware variant. It routes
// through protodecode so a future binaryDecoders entry (once the generated
// proto lands) transparently takes over without changing this call-site.
func DecodeAtomPublishedWithAttrs(blob []byte, attrs map[string]string) (AtomPublishedEnvelope, error) {
	m, err := protodecode.DecodePayloadMapWithAttrs(TopicAtomPublished, blob, attrs)
	if err != nil {
		return AtomPublishedEnvelope{}, fmt.Errorf("subscribers: decode atom.published: %w", err)
	}
	return AtomPublishedEnvelope{
		EventID:           asString(m["event_id"]),
		TenantID:          asString(m["tenant_id"]),
		GCID:              firstNonEmpty(asString(m["gcid"]), asString(m["author_gcid"])),
		AtomID:            asString(m["atom_id"]),
		RevisionID:        asString(m["current_revision_id"]),
		AuthorGCID:        asString(m["author_gcid"]),
		AuthorDisplayName: asString(m["author_display_name"]),
		Title:             asString(m["title"]),
		Stem:              asString(m["stem"]),
		QuestionType:      asString(m["question_type"]),
		Status:            asString(m["status"]),
		Options:           asStringSlice(m["options"]),
		CorrectAnswer:     asString(m["correct_answer"]),
		ReuseVisibility:   asString(m["reuse_visibility"]),
	}, nil
}

// DecodeAtomReuseVisibilityChangedWithAttrs is the attribute-aware decode for
// reuse_visibility_changed.v1 (ADR-229 WS-1).
func DecodeAtomReuseVisibilityChangedWithAttrs(blob []byte, attrs map[string]string) (AtomReuseVisibilityChangedEnvelope, error) {
	m, err := protodecode.DecodePayloadMapWithAttrs(TopicAtomReuseVisibilityChanged, blob, attrs)
	if err != nil {
		return AtomReuseVisibilityChangedEnvelope{}, fmt.Errorf("subscribers: decode atom.reuse_visibility_changed: %w", err)
	}
	return AtomReuseVisibilityChangedEnvelope{
		EventID:            asString(m["event_id"]),
		TenantID:           asString(m["tenant_id"]),
		GCID:               firstNonEmpty(asString(m["gcid"]), asString(m["author_gcid"])),
		AtomID:             asString(m["atom_id"]),
		ReuseVisibility:    asString(m["reuse_visibility"]),
		PreviousVisibility: asString(m["previous_visibility"]),
		AuthorGCID:         asString(m["author_gcid"]),
	}, nil
}

// DecodeAtomArchivedWithAttrs is the attribute-aware variant for archived.v1.
func DecodeAtomArchivedWithAttrs(blob []byte, attrs map[string]string) (AtomArchivedEnvelope, error) {
	m, err := protodecode.DecodePayloadMapWithAttrs(TopicAtomArchived, blob, attrs)
	if err != nil {
		return AtomArchivedEnvelope{}, fmt.Errorf("subscribers: decode atom.archived: %w", err)
	}
	return AtomArchivedEnvelope{
		EventID:  asString(m["event_id"]),
		TenantID: asString(m["tenant_id"]),
		GCID:     asString(m["gcid"]),
		AtomID:   asString(m["atom_id"]),
		Status:   asString(m["status"]),
		Reason:   asString(m["reason"]),
	}, nil
}
