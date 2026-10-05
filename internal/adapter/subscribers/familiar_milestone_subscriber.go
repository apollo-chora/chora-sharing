// Package subscribers holds inbound Pub/Sub subscriber adapters for the
// Content Sharing domain.
//
// FamiliarMilestoneSubscriber (PROD-G, ADR-149 Iter G.7) consumes 4 events
// from the Content Consumption domain and queues / auto-publishes
// shareable C+ posts:
//
//	chora.consumption.familiar.stage_up.v1
//	chora.consumption.familiar.breed_revealed.v1
//	chora.consumption.familiar.hatched.v1
//	chora.consumption.familiar.source_revelation.v1
//
// Per-user preference (chora_sharing.user_preferences) chooses one of:
//
//	auto      → compose Post directly + outbox chora.sharing.post.created.v1
//	draft     → insert post_drafts row (default — pending user review)
//	suppress  → no-op (still logged for IMDA D1 audit)
//
// Idempotency: composite key (subscriber_handler, source_event_id) in
// chora_sharing.subscriber_idempotency. Re-delivery skips with an OTLP
// span event "duplicate_event_skipped".
//
// Hexagonal: this package depends on the domain (post_template + post)
// and on internal ports DraftStore / PostPublisher / PreferenceStore /
// IdempotencyStore. cmd/server wires Postgres implementations of those
// ports; tests inject in-memory doubles defined here.
package subscribers

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protodecode"
	"github.com/apollo-chora/chora-sharing/internal/domain/post"
	tmpl "github.com/apollo-chora/chora-sharing/internal/domain/post_template"
)

// -----------------------------------------------------------------------------
// Topic constants
// -----------------------------------------------------------------------------

const (
	TopicFamiliarStageUp          = "chora.consumption.familiar.stage_up.v1"
	TopicFamiliarBreedRevealed    = "chora.consumption.familiar.breed_revealed.v1"
	TopicFamiliarHatched          = "chora.consumption.familiar.hatched.v1"
	TopicFamiliarSourceRevelation = "chora.consumption.familiar.source_revelation.v1"
)

// Subscriber handler ids — used as the composite idempotency key prefix
// and as the OTLP span attribute "chora.subscriber.handler".
const (
	HandlerStageUp          = "familiar_milestone:stage_up"
	HandlerBreedRevealed    = "familiar_milestone:breed_revealed"
	HandlerHatched          = "familiar_milestone:hatched"
	HandlerSourceRevelation = "familiar_milestone:source_revelation"
)

// -----------------------------------------------------------------------------
// Policy
// -----------------------------------------------------------------------------

// Policy is the per-user share preference for Familiar milestones.
type Policy string

// Policy values match the familiar_milestone_share_pref enum on
// chora_sharing.user_preferences.
const (
	PolicyAuto     Policy = "auto"
	PolicyDraft    Policy = "draft"
	PolicySuppress Policy = "suppress"
)

// ParsePolicy validates an input string and returns the canonical Policy.
func ParsePolicy(s string) (Policy, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "auto":
		return PolicyAuto, nil
	case "draft":
		return PolicyDraft, nil
	case "suppress":
		return PolicySuppress, nil
	default:
		return "", fmt.Errorf("subscribers: unknown policy %q (allowed: auto|draft|suppress)", s)
	}
}

// -----------------------------------------------------------------------------
// Envelopes — payloads as the subscriber consumes them
//
// Each carries the canonical envelope event_id + tenant_id + owner_gcid plus
// the payload fields protodecode can actually produce.
//
// ⚠ This doc used to say these carry "denormalised display fields that the
// publishing chora-consumption service is responsible for filling", defaulted to
// "Your Familiar" / "Your friend" so "copy stays safe". That was a PHANTOM — the
// third in this file, after "cmd/server wires Postgres implementations"
// (CHO-2203) and the callerless DraftStore ports (CHO-2258). familiar.proto has
// never had familiar_display_name, owner_display_name, breed_adjective,
// stage_6_adjective, breed_themed_effect or stage_3_form on ANY milestone
// message, so the publisher had nowhere to fill them and the defaults fired
// 100% of the time. Far from keeping copy safe, they printed "Their friend's
// Your Familiar grew" to real learners (CHO-2259).
//
// Only FamiliarHatched.display_name (field 4) exists. A field kept here that the
// wire cannot fill must say so on its own line.
// -----------------------------------------------------------------------------

// StageUpEnvelope mirrors the FamiliarStageUp v1 payload.
//
// FamiliarDisplayName is retained but is NOT on the wire today (see the package
// doc): it stays so the copy starts naming the Familiar for free if the contract
// gains the field. owner_display_name and breed_adjective are GONE — they were
// never proto fields, so decoding them was decoding nothing.
type StageUpEnvelope struct {
	EventID                  string   `json:"event_id"`
	TenantID                 string   `json:"tenant_id"`
	OwnerGCID                string   `json:"owner_gcid"`
	FamiliarID               string   `json:"familiar_id"`
	FamiliarDisplayName      string   `json:"familiar_display_name"` // not on the wire yet
	StageFromName            string   `json:"stage_from_name"`
	StageToName              string   `json:"stage_to_name"`
	NewlyUnlockedTools       []string `json:"newly_unlocked_tools"`
	NewlyRevealedKgNeighbors []string `json:"newly_revealed_kg_neighbors"`
	NewLLMTier               string   `json:"new_llm_tier"`
	NewMemoryMode            string   `json:"new_memory_mode"`
}

// BreedRevealedEnvelope mirrors FamiliarBreedRevealed v1.
type BreedRevealedEnvelope struct {
	EventID             string  `json:"event_id"`
	TenantID            string  `json:"tenant_id"`
	OwnerGCID           string  `json:"owner_gcid"`
	FamiliarID          string  `json:"familiar_id"`
	FamiliarDisplayName string  `json:"familiar_display_name"` // not on the wire yet
	Species             string  `json:"species"`               // plain-language label; may be absent
	ShinyVariant        bool    `json:"shiny_variant"`
	Rarity              string  `json:"rarity"`
	EggSKU              string  `json:"egg_sku"`
	RolledProbability   float64 `json:"rolled_probability"`
}

// HatchedEnvelope mirrors FamiliarHatched v1.
type HatchedEnvelope struct {
	EventID        string `json:"event_id"`
	TenantID       string `json:"tenant_id"`
	OwnerGCID      string `json:"owner_gcid"`
	FamiliarID     string `json:"familiar_id"`
	DisplayName    string `json:"display_name"` // the ONE display name on any milestone wire
	Species        string `json:"species"`      // plain-language label; may be absent
	ShinyVariant   bool   `json:"shiny_variant"`
	ResonantAtomID string `json:"resonant_atom_id"`
	Specialization string `json:"specialization"`
	Tone           string `json:"tone"`
	LearnerPersona string `json:"learner_persona"`
}

// SourceRevelationEnvelope mirrors FamiliarSourceRevelation v1 enriched
// with the breed adjective + future-form descriptors needed by the
// ceremony copy. The chora-consumption publisher computes these from the
// FamiliarInstance snapshot.
type SourceRevelationEnvelope struct {
	EventID             string   `json:"event_id"`
	TenantID            string   `json:"tenant_id"`
	OwnerGCID           string   `json:"owner_gcid"`
	FamiliarID          string   `json:"familiar_id"`
	FamiliarDisplayName string   `json:"familiar_display_name"` // not on the wire yet
	PreviewTools        []string `json:"preview_tools"`
	PreviewLLMTier      string   `json:"preview_llm_tier"`
}

// -----------------------------------------------------------------------------
// Ports
// -----------------------------------------------------------------------------

// ErrDraftNotPending means no pending, non-deleted draft with that id is owned
// by the caller.
//
// It deliberately does NOT distinguish "never existed" from "owned by someone
// else" from "already published": the HTTP adapter maps all three to one 404, so
// the API cannot be used to probe for the existence of another learner's drafts.
// It lives here beside the Draft it describes rather than in the pg adapter —
// "this draft is not publishable" is a rule of the aggregate, not a fact about
// Postgres, and homing it in pg would make every consumer of the port depend on
// the storage adapter to interpret its own errors.
var ErrDraftNotPending = errors.New("subscribers: no pending draft for this owner")

// Draft is the payload persisted in chora_sharing.post_drafts.
type Draft struct {
	DraftID             string
	TenantID            string
	AuthorGCID          string
	ComposedFromTopic   string
	ComposedFromEventID string
	Body                string
	Metadata            map[string]interface{}
	Status              string
	CreatedAt           time.Time
}

// DraftStore is the port for persisting post_drafts.
//
// Insert is idempotent on (composed_from_topic, composed_from_event_id).
// Duplicate insertion returns (existing, false, nil). The subscriber
// treats `!created` as "already drafted" and skips re-publishing.
//
// Insert is ALL the subscriber does — it writes drafts, it never reads them
// back. The owner-facing half (list / publish / discard) is a different
// consumer with a different port: httpadapter.MilestoneDrafts, whose operations
// are owner-scoped because they serve a learner rather than an event. This port
// once also declared ListPending/Get/MarkPublished/MarkDiscarded; they had zero
// callers for the whole life of the lane, and they were tenant-scoped only, so
// the first caller to arrive would have been able to publish another learner's
// draft. Segregated per consumer (CHO-2258).
type DraftStore interface {
	Insert(ctx context.Context, d Draft) (created bool, err error)
}

// PostPublisher is the port for auto-posting under PolicyAuto. The
// production adapter wraps inmem.PostRepo + events.Publisher; tests
// inject InMemoryPostPublisher.
type PostPublisher interface {
	Publish(ctx context.Context, p PostRecord) error
}

// PostRecord is the payload published by PostPublisher.
type PostRecord struct {
	PostID     string
	TenantID   string
	AuthorGCID string
	Body       string
	Visibility string
	Metadata   map[string]interface{}
}

// PreferenceStore is the port for chora_sharing.user_preferences.
type PreferenceStore interface {
	Get(ctx context.Context, tenantID, gcid string) (Policy, bool, error)
	Upsert(ctx context.Context, tenantID, gcid string, p Policy) error
}

// IdempotencyStore is the port for chora_sharing.subscriber_idempotency.
//
// It is split into a peek and a commit (CHO-2263, mirroring chora-consumption's
// CHO-2130 seen→process→mark). The single committing Claim it replaced INSERTed
// and COMMITTED the dedup row in its OWN transaction BEFORE the handler's side
// effect ran, so a first delivery whose work then failed NACKed with the row
// already committed, and the redelivery saw the row → ACKed having done nothing
// → the event was silently discarded. One transient blip burned the claim.
//
//   - Seen is a READ-ONLY peek: it reports whether (handler, eventID) is already
//     durably recorded (true on a redelivery). It NEVER writes.
//   - Mark durably records (handler, eventID) — an idempotent INSERT ON CONFLICT
//     DO NOTHING. It MUST be called ONLY AFTER the side effect has landed, so a
//     post-peek failure returns the pair UNMARKED and the Pub/Sub redelivery
//     re-runs the idempotent work instead of being ACK-dropped.
//
// Use processOnce to get this ordering right in one place.
type IdempotencyStore interface {
	Seen(ctx context.Context, handler, eventID string) (bool, error)
	Mark(ctx context.Context, handler, eventID string) error
}

// processOnce runs fn exactly once per (handler, id) across Pub/Sub
// redeliveries, in the CHO-2130 seen→process→mark order: peek the durable
// idempotency store; on a redelivery skip (ACK) via onDuplicate; otherwise run
// fn and mark the pair ONLY after fn succeeds. A fn error — or a Seen/Mark error
// — returns UNMARKED, so the redelivery re-runs fn.
//
// This closes the CHO-2263 loss window structurally: marking is the last thing
// that happens, and only on success, so a handler can never burn its idempotency
// key before its work lands. Every idempotency-guarded handler in this package
// funnels through here so the ordering has exactly one home.
func processOnce(ctx context.Context, idem IdempotencyStore, handler, id string, onDuplicate func(), fn func() error) error {
	seen, err := idem.Seen(ctx, handler, id)
	if err != nil {
		return fmt.Errorf("subscribers: check %s idempotency: %w", handler, err)
	}
	if seen {
		if onDuplicate != nil {
			onDuplicate()
		}
		return nil
	}
	if err := fn(); err != nil {
		return err // UNMARKED → NACK → redelivery re-runs the idempotent work
	}
	if err := idem.Mark(ctx, handler, id); err != nil {
		return fmt.Errorf("subscribers: mark %s idempotency: %w", handler, err)
	}
	return nil
}

// -----------------------------------------------------------------------------
// FamiliarMilestoneSubscriber
// -----------------------------------------------------------------------------

// Config bundles the subscriber's dependencies.
type Config struct {
	Drafts        DraftStore
	Posts         PostPublisher
	Preferences   PreferenceStore
	Idempotency   IdempotencyStore
	Composer      *tmpl.Composer
	DefaultPolicy Policy // applied when user has no row in user_preferences
	Logger        Logger // optional; falls back to log.Default()
}

// Logger is the minimal structured-log contract.
type Logger interface {
	Printf(format string, args ...interface{})
}

// FamiliarMilestoneSubscriber routes incoming Familiar milestone events
// to draft / auto-post / suppress per user preference.
type FamiliarMilestoneSubscriber struct {
	cfg Config
}

// NewFamiliarMilestoneSubscriber builds a subscriber with sane defaults.
func NewFamiliarMilestoneSubscriber(cfg Config) *FamiliarMilestoneSubscriber {
	if cfg.DefaultPolicy == "" {
		cfg.DefaultPolicy = PolicyDraft
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	return &FamiliarMilestoneSubscriber{cfg: cfg}
}

// SubscribedTopics returns the 4 topics this subscriber listens to.
func (s *FamiliarMilestoneSubscriber) SubscribedTopics() []string {
	return []string{
		TopicFamiliarStageUp,
		TopicFamiliarBreedRevealed,
		TopicFamiliarHatched,
		TopicFamiliarSourceRevelation,
	}
}

// -----------------------------------------------------------------------------
// HandleStageUp
// -----------------------------------------------------------------------------

// HandleStageUp processes one chora.consumption.familiar.stage_up.v1 event.
func (s *FamiliarMilestoneSubscriber) HandleStageUp(ctx context.Context, e StageUpEnvelope) error {
	if err := s.preflight(); err != nil {
		return err
	}
	if err := validateBase(e.EventID, e.TenantID, e.OwnerGCID); err != nil {
		return err
	}

	// Peek → work → mark (CHO-2263): the dedup row is written ONLY after the
	// draft/post lands, so a transient failure here NACKs unmarked and the
	// redelivery re-runs rather than being silently ACK-dropped.
	return processOnce(ctx, s.cfg.Idempotency, HandlerStageUp, e.EventID,
		func() { s.spanEventDuplicate(HandlerStageUp, e.EventID, e.TenantID, e.OwnerGCID) },
		func() error {
			policy, err := s.resolvePolicy(ctx, e.TenantID, e.OwnerGCID)
			if err != nil {
				return err
			}
			if policy == PolicySuppress {
				s.logAudit(HandlerStageUp, e.EventID, e.TenantID, e.OwnerGCID, "suppressed")
				return nil
			}

			body, err := s.cfg.Composer.StageUp(tmpl.StageUpInput{
				// No display-name fallback here any more: the template owns the neutral
				// form via familiarName(). Two fallbacks chosen independently HERE is
				// what printed "Their friend's Your Familiar" (CHO-2259).
				FamiliarDisplayName:      e.FamiliarDisplayName,
				StageToName:              e.StageToName,
				NewlyUnlockedTools:       e.NewlyUnlockedTools,
				NewlyRevealedKgNeighbors: e.NewlyRevealedKgNeighbors,
				NewLLMTier:               e.NewLLMTier,
			})
			if err != nil {
				return fmt.Errorf("subscribers: render stage_up: %w", err)
			}

			metadata := map[string]interface{}{
				"familiar_id":          e.FamiliarID,
				"stage_from_name":      e.StageFromName,
				"stage_to_name":        e.StageToName,
				"newly_unlocked":       e.NewlyUnlockedTools,
				"kg_neighbors_count":   len(e.NewlyRevealedKgNeighbors),
				"new_llm_tier":         e.NewLLMTier,
				"chora_imda_dimension": "transparency",
				"imda_lifecycle_stage": "runtime",
			}

			return s.dispatch(ctx, policy, dispatchInput{
				Handler:    HandlerStageUp,
				Topic:      TopicFamiliarStageUp,
				EventID:    e.EventID,
				TenantID:   e.TenantID,
				AuthorGCID: e.OwnerGCID,
				Body:       body,
				Metadata:   metadata,
			})
		})
}

// -----------------------------------------------------------------------------
// HandleBreedRevealed
// -----------------------------------------------------------------------------

// HandleBreedRevealed processes one chora.consumption.familiar.breed_revealed.v1.
func (s *FamiliarMilestoneSubscriber) HandleBreedRevealed(ctx context.Context, e BreedRevealedEnvelope) error {
	if err := s.preflight(); err != nil {
		return err
	}
	if err := validateBase(e.EventID, e.TenantID, e.OwnerGCID); err != nil {
		return err
	}

	return processOnce(ctx, s.cfg.Idempotency, HandlerBreedRevealed, e.EventID,
		func() { s.spanEventDuplicate(HandlerBreedRevealed, e.EventID, e.TenantID, e.OwnerGCID) },
		func() error {
			policy, err := s.resolvePolicy(ctx, e.TenantID, e.OwnerGCID)
			if err != nil {
				return err
			}
			if policy == PolicySuppress {
				s.logAudit(HandlerBreedRevealed, e.EventID, e.TenantID, e.OwnerGCID, "suppressed")
				return nil
			}

			rarity := strings.ToLower(strings.TrimSpace(e.Rarity))
			if rarity == "" {
				rarity = "common"
			}

			body, err := s.cfg.Composer.BreedReveal(tmpl.BreedRevealInput{
				FamiliarDisplayName: e.FamiliarDisplayName,
				Species:             e.Species,
				Rarity:              rarity,
				ShinyVariant:        e.ShinyVariant,
				EggSKU:              e.EggSKU,
			})
			if err != nil {
				return fmt.Errorf("subscribers: render breed_reveal: %w", err)
			}

			metadata := map[string]interface{}{
				"familiar_id":          e.FamiliarID,
				"species":              e.Species,
				"shiny_variant":        e.ShinyVariant,
				"rarity":               rarity,
				"egg_sku":              e.EggSKU,
				"rolled_probability":   e.RolledProbability,
				"chora_imda_dimension": "transparency", // IMDA D2 — lootbox audit
				"imda_lifecycle_stage": "runtime",
			}

			return s.dispatch(ctx, policy, dispatchInput{
				Handler:    HandlerBreedRevealed,
				Topic:      TopicFamiliarBreedRevealed,
				EventID:    e.EventID,
				TenantID:   e.TenantID,
				AuthorGCID: e.OwnerGCID,
				Body:       body,
				Metadata:   metadata,
			})
		})
}

// -----------------------------------------------------------------------------
// HandleHatched
// -----------------------------------------------------------------------------

// HandleHatched processes one chora.consumption.familiar.hatched.v1.
//
// Reuses the breed-reveal template — Hatched IS the social companion event
// to BreedRevealed (they fire seconds apart at the hatching ceremony) and
// the copy is intentionally the same shape ("Meet {name}, a {rarity}
// {species}"). The composed_from_topic + composed_from_event_id keep the
// two drafts distinct, so a user can publish or discard either.
func (s *FamiliarMilestoneSubscriber) HandleHatched(ctx context.Context, e HatchedEnvelope) error {
	if err := s.preflight(); err != nil {
		return err
	}
	if err := validateBase(e.EventID, e.TenantID, e.OwnerGCID); err != nil {
		return err
	}

	return processOnce(ctx, s.cfg.Idempotency, HandlerHatched, e.EventID,
		func() { s.spanEventDuplicate(HandlerHatched, e.EventID, e.TenantID, e.OwnerGCID) },
		func() error {
			policy, err := s.resolvePolicy(ctx, e.TenantID, e.OwnerGCID)
			if err != nil {
				return err
			}
			if policy == PolicySuppress {
				s.logAudit(HandlerHatched, e.EventID, e.TenantID, e.OwnerGCID, "suppressed")
				return nil
			}

			body, err := s.cfg.Composer.BreedReveal(tmpl.BreedRevealInput{
				FamiliarDisplayName: e.DisplayName, // the ONE name the wire carries
				Species:             e.Species,
				Rarity:              "common", // hatched event does not carry rarity; the breed-revealed template default applies
				ShinyVariant:        e.ShinyVariant,
			})
			if err != nil {
				return fmt.Errorf("subscribers: render hatched: %w", err)
			}

			metadata := map[string]interface{}{
				"familiar_id":          e.FamiliarID,
				"display_name":         e.DisplayName,
				"species":              e.Species,
				"shiny_variant":        e.ShinyVariant,
				"resonant_atom_id":     e.ResonantAtomID,
				"specialization":       e.Specialization,
				"tone":                 e.Tone,
				"learner_persona":      e.LearnerPersona,
				"chora_imda_dimension": "accountability",
				"imda_lifecycle_stage": "runtime",
			}

			return s.dispatch(ctx, policy, dispatchInput{
				Handler:    HandlerHatched,
				Topic:      TopicFamiliarHatched,
				EventID:    e.EventID,
				TenantID:   e.TenantID,
				AuthorGCID: e.OwnerGCID,
				Body:       body,
				Metadata:   metadata,
			})
		})
}

// -----------------------------------------------------------------------------
// HandleSourceRevelation
// -----------------------------------------------------------------------------

// HandleSourceRevelation processes one chora.consumption.familiar.source_revelation.v1.
func (s *FamiliarMilestoneSubscriber) HandleSourceRevelation(ctx context.Context, e SourceRevelationEnvelope) error {
	if err := s.preflight(); err != nil {
		return err
	}
	if err := validateBase(e.EventID, e.TenantID, e.OwnerGCID); err != nil {
		return err
	}

	return processOnce(ctx, s.cfg.Idempotency, HandlerSourceRevelation, e.EventID,
		func() { s.spanEventDuplicate(HandlerSourceRevelation, e.EventID, e.TenantID, e.OwnerGCID) },
		func() error {
			policy, err := s.resolvePolicy(ctx, e.TenantID, e.OwnerGCID)
			if err != nil {
				return err
			}
			if policy == PolicySuppress {
				s.logAudit(HandlerSourceRevelation, e.EventID, e.TenantID, e.OwnerGCID, "suppressed")
				return nil
			}

			body, err := s.cfg.Composer.SourceRevelation(tmpl.SourceRevelationInput{
				FamiliarDisplayName: e.FamiliarDisplayName,
				PreviewTools:        e.PreviewTools,
				PreviewLLMTier:      e.PreviewLLMTier,
			})
			if err != nil {
				return fmt.Errorf("subscribers: render source_revelation: %w", err)
			}

			metadata := map[string]interface{}{
				"familiar_id":          e.FamiliarID,
				"preview_llm_tier":     e.PreviewLLMTier,
				"preview_tools":        e.PreviewTools,
				"chora_imda_dimension": "transparency",
				"imda_lifecycle_stage": "runtime",
			}

			return s.dispatch(ctx, policy, dispatchInput{
				Handler:    HandlerSourceRevelation,
				Topic:      TopicFamiliarSourceRevelation,
				EventID:    e.EventID,
				TenantID:   e.TenantID,
				AuthorGCID: e.OwnerGCID,
				Body:       body,
				Metadata:   metadata,
			})
		})
}

// -----------------------------------------------------------------------------
// Internals
// -----------------------------------------------------------------------------

type dispatchInput struct {
	Handler    string
	Topic      string
	EventID    string
	TenantID   string
	AuthorGCID string
	Body       string
	Metadata   map[string]interface{}
}

// dispatch routes a rendered post body to either Post auto-publish or
// post_drafts insert based on the resolved Policy.
func (s *FamiliarMilestoneSubscriber) dispatch(ctx context.Context, policy Policy, in dispatchInput) error {
	switch policy {
	case PolicyAuto:
		// Auto: construct Post aggregate + delegate publish to PostPublisher.
		// We use VisibilityTenant so the share doesn't escape the user's
		// org by default; user can edit visibility on follow-up update.
		p, err := post.NewPostWithVisibility(in.TenantID, in.AuthorGCID, in.Body, "", nil, post.VisibilityTenant)
		if err != nil {
			return fmt.Errorf("subscribers: new post: %w", err)
		}
		rec := PostRecord{
			PostID:     p.ID,
			TenantID:   p.TenantID,
			AuthorGCID: p.AuthorGCID,
			Body:       p.Body,
			Visibility: string(p.Visibility),
			Metadata:   in.Metadata,
		}
		if err := s.cfg.Posts.Publish(ctx, rec); err != nil {
			return fmt.Errorf("subscribers: publish post: %w", err)
		}
		s.logAudit(in.Handler, in.EventID, in.TenantID, in.AuthorGCID, "auto_posted")
		return nil

	case PolicyDraft:
		draft := Draft{
			DraftID:             post.NewUUIDv7(),
			TenantID:            in.TenantID,
			AuthorGCID:          in.AuthorGCID,
			ComposedFromTopic:   in.Topic,
			ComposedFromEventID: in.EventID,
			Body:                in.Body,
			Metadata:            in.Metadata,
			Status:              "pending",
			CreatedAt:           time.Now().UTC(),
		}
		created, err := s.cfg.Drafts.Insert(ctx, draft)
		if err != nil {
			return fmt.Errorf("subscribers: insert draft: %w", err)
		}
		if !created {
			s.spanEventDuplicate(in.Handler, in.EventID, in.TenantID, in.AuthorGCID)
			return nil
		}
		s.logAudit(in.Handler, in.EventID, in.TenantID, in.AuthorGCID, "drafted")
		return nil
	}
	return fmt.Errorf("subscribers: unreachable policy %q", policy)
}

// resolvePolicy reads user preference + falls back to DefaultPolicy.
func (s *FamiliarMilestoneSubscriber) resolvePolicy(ctx context.Context, tenantID, gcid string) (Policy, error) {
	pol, ok, err := s.cfg.Preferences.Get(ctx, tenantID, gcid)
	if err != nil {
		return "", fmt.Errorf("subscribers: get preference: %w", err)
	}
	if !ok {
		return s.cfg.DefaultPolicy, nil
	}
	return pol, nil
}

func (s *FamiliarMilestoneSubscriber) preflight() error {
	if s.cfg.Drafts == nil || s.cfg.Posts == nil || s.cfg.Preferences == nil ||
		s.cfg.Idempotency == nil || s.cfg.Composer == nil {
		return errors.New("subscribers: missing dependency (Drafts | Posts | Preferences | Idempotency | Composer)")
	}
	return nil
}

func (s *FamiliarMilestoneSubscriber) spanEventDuplicate(handler, eventID, tenantID, gcid string) {
	// OTLP span event "duplicate_event_skipped" — emitted via structured
	// log line until the OTel SDK lands per CLAUDE.md §6. The Cloud Trace
	// span exporter joins these on trace_id.
	s.cfg.Logger.Printf(`{"event":"duplicate_event_skipped","handler":"%s","source_event_id":"%s","tenant_id":"%s","gcid":"%s"}`,
		handler, eventID, tenantID, gcid)
}

func (s *FamiliarMilestoneSubscriber) logAudit(handler, eventID, tenantID, gcid, outcome string) {
	s.cfg.Logger.Printf(`{"event":"familiar_milestone_handled","handler":"%s","source_event_id":"%s","tenant_id":"%s","gcid":"%s","outcome":"%s"}`,
		handler, eventID, tenantID, gcid, outcome)
}

func validateBase(eventID, tenantID, gcid string) error {
	if strings.TrimSpace(eventID) == "" {
		return errors.New("subscribers: event_id required")
	}
	if strings.TrimSpace(tenantID) == "" {
		return errors.New("subscribers: tenant_id required")
	}
	if strings.TrimSpace(gcid) == "" {
		return errors.New("subscribers: owner_gcid required")
	}
	return nil
}

// -----------------------------------------------------------------------------
// Wire decoders — invoked by cmd/server + the HTTP push handler when the
// inbound Pub/Sub message arrives. The decoder is wire-format-tolerant:
// proto.Unmarshal for the registered binary topics (per producer-side flip),
// json.Unmarshal fallback for topics still on JSON. See
// internal/adapter/events/protodecode.
// -----------------------------------------------------------------------------

// DecodeStageUpWithAttrs is the attribute-aware variant.
func DecodeStageUpWithAttrs(blob []byte, attrs map[string]string) (StageUpEnvelope, error) {
	m, err := protodecode.DecodePayloadMapWithAttrs(TopicFamiliarStageUp, blob, attrs)
	if err != nil {
		return StageUpEnvelope{}, fmt.Errorf("subscribers: decode stage_up: %w", err)
	}
	return StageUpEnvelope{
		EventID:                  asString(m["event_id"]),
		TenantID:                 asString(m["tenant_id"]),
		OwnerGCID:                firstNonEmpty(asString(m["owner_gcid"]), asString(m["gcid"])),
		FamiliarID:               asString(m["familiar_id"]),
		FamiliarDisplayName:      asString(m["familiar_display_name"]),
		StageFromName:            asString(m["stage_from_name"]),
		StageToName:              asString(m["stage_to_name"]),
		NewlyUnlockedTools:       asStringSlice(m["newly_unlocked_tools"]),
		NewlyRevealedKgNeighbors: asStringSlice(m["newly_revealed_kg_neighbors"]),
		NewLLMTier:               asString(m["new_llm_tier"]),
		NewMemoryMode:            asString(m["new_memory_mode"]),
	}, nil
}

// DecodeBreedRevealedWithAttrs is the attribute-aware variant.
func DecodeBreedRevealedWithAttrs(blob []byte, attrs map[string]string) (BreedRevealedEnvelope, error) {
	m, err := protodecode.DecodePayloadMapWithAttrs(TopicFamiliarBreedRevealed, blob, attrs)
	if err != nil {
		return BreedRevealedEnvelope{}, fmt.Errorf("subscribers: decode breed_revealed: %w", err)
	}
	return BreedRevealedEnvelope{
		EventID:             asString(m["event_id"]),
		TenantID:            asString(m["tenant_id"]),
		OwnerGCID:           firstNonEmpty(asString(m["owner_gcid"]), asString(m["gcid"])),
		FamiliarID:          asString(m["familiar_id"]),
		FamiliarDisplayName: asString(m["familiar_display_name"]),
		Species:             asString(m["species"]),
		ShinyVariant:        asBool(m["shiny_variant"]),
		Rarity:              asString(m["rarity"]),
		EggSKU:              asString(m["egg_sku"]),
		RolledProbability:   asFloat64(m["rolled_probability"]),
	}, nil
}

// DecodeHatchedWithAttrs is the attribute-aware variant.
func DecodeHatchedWithAttrs(blob []byte, attrs map[string]string) (HatchedEnvelope, error) {
	m, err := protodecode.DecodePayloadMapWithAttrs(TopicFamiliarHatched, blob, attrs)
	if err != nil {
		return HatchedEnvelope{}, fmt.Errorf("subscribers: decode hatched: %w", err)
	}
	return HatchedEnvelope{
		EventID:        asString(m["event_id"]),
		TenantID:       asString(m["tenant_id"]),
		OwnerGCID:      firstNonEmpty(asString(m["owner_gcid"]), asString(m["gcid"])),
		FamiliarID:     asString(m["familiar_id"]),
		DisplayName:    asString(m["display_name"]),
		Species:        asString(m["species"]),
		ShinyVariant:   asBool(m["shiny_variant"]),
		ResonantAtomID: asString(m["resonant_atom_id"]),
		Specialization: asString(m["specialization"]),
		Tone:           asString(m["tone"]),
		LearnerPersona: asString(m["learner_persona"]),
	}, nil
}

// DecodeSourceRevelationWithAttrs is the attribute-aware variant.
func DecodeSourceRevelationWithAttrs(blob []byte, attrs map[string]string) (SourceRevelationEnvelope, error) {
	m, err := protodecode.DecodePayloadMapWithAttrs(TopicFamiliarSourceRevelation, blob, attrs)
	if err != nil {
		return SourceRevelationEnvelope{}, fmt.Errorf("subscribers: decode source_revelation: %w", err)
	}
	return SourceRevelationEnvelope{
		EventID:             asString(m["event_id"]),
		TenantID:            asString(m["tenant_id"]),
		OwnerGCID:           firstNonEmpty(asString(m["owner_gcid"]), asString(m["gcid"])),
		FamiliarID:          asString(m["familiar_id"]),
		FamiliarDisplayName: asString(m["familiar_display_name"]),
		PreviewTools:        asStringSlice(m["preview_tools"]),
		PreviewLLMTier:      asString(m["preview_llm_tier"]),
	}, nil
}

// asString coerces a map value to string.
func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// asBool coerces a map value to bool.
func asBool(v any) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}

// asFloat64 coerces a map value to float64.
func asFloat64(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	}
	return 0
}

// asStringSlice coerces a map value to []string. Handles both []string (from
// proto projector) and []any (from json.Unmarshal).
func asStringSlice(v any) []string {
	switch s := v.(type) {
	case []string:
		return s
	case []any:
		out := make([]string, 0, len(s))
		for _, e := range s {
			if str, ok := e.(string); ok {
				out = append(out, str)
			}
		}
		return out
	}
	return nil
}

// firstNonEmpty returns the first non-empty argument; "" if all empty.
func firstNonEmpty(args ...string) string {
	for _, s := range args {
		if strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}

// -----------------------------------------------------------------------------
// In-memory test doubles
// -----------------------------------------------------------------------------

// InMemoryDraftStore is a thread-safe in-memory DraftStore.
type InMemoryDraftStore struct {
	mu    sync.Mutex
	byKey map[string]Draft // key = topic+":"+event_id
	byID  map[string]Draft
}

// NewInMemoryDraftStore returns an empty in-memory draft store.
func NewInMemoryDraftStore() *InMemoryDraftStore {
	return &InMemoryDraftStore{
		byKey: make(map[string]Draft),
		byID:  make(map[string]Draft),
	}
}

// Insert idempotently records a Draft.
func (s *InMemoryDraftStore) Insert(_ context.Context, d Draft) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := d.ComposedFromTopic + ":" + d.ComposedFromEventID
	if _, exists := s.byKey[key]; exists {
		return false, nil
	}
	s.byKey[key] = d
	s.byID[d.DraftID] = d
	return true, nil
}

// ListPending returns drafts in pending status for the caller.
func (s *InMemoryDraftStore) ListPending(_ context.Context, tenantID, gcid string) ([]Draft, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Draft, 0)
	for _, d := range s.byID {
		if d.TenantID == tenantID && d.AuthorGCID == gcid && d.Status == "pending" {
			out = append(out, d)
		}
	}
	return out, nil
}

// Get returns a draft by id.
func (s *InMemoryDraftStore) Get(_ context.Context, tenantID, draftID string) (Draft, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.byID[draftID]
	if !ok || d.TenantID != tenantID {
		return Draft{}, false, nil
	}
	return d, true, nil
}

// MarkPublished flips status to "published".
func (s *InMemoryDraftStore) MarkPublished(_ context.Context, tenantID, draftID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.byID[draftID]
	if !ok || d.TenantID != tenantID {
		return errors.New("subscribers: draft not found")
	}
	d.Status = "published"
	s.byID[draftID] = d
	s.byKey[d.ComposedFromTopic+":"+d.ComposedFromEventID] = d
	return nil
}

// MarkDiscarded flips status to "discarded".
func (s *InMemoryDraftStore) MarkDiscarded(_ context.Context, tenantID, draftID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.byID[draftID]
	if !ok || d.TenantID != tenantID {
		return errors.New("subscribers: draft not found")
	}
	d.Status = "discarded"
	s.byID[draftID] = d
	s.byKey[d.ComposedFromTopic+":"+d.ComposedFromEventID] = d
	return nil
}

// All returns a defensive snapshot of all drafts (test convenience).
func (s *InMemoryDraftStore) All() []Draft {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Draft, 0, len(s.byID))
	for _, d := range s.byID {
		out = append(out, d)
	}
	return out
}

// InMemoryPostPublisher is a thread-safe in-memory PostPublisher.
type InMemoryPostPublisher struct {
	mu      sync.Mutex
	records []PostRecord
}

// NewInMemoryPostPublisher returns an empty in-memory publisher.
func NewInMemoryPostPublisher() *InMemoryPostPublisher {
	return &InMemoryPostPublisher{records: make([]PostRecord, 0, 4)}
}

// Publish records a PostRecord.
func (p *InMemoryPostPublisher) Publish(_ context.Context, rec PostRecord) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.records = append(p.records, rec)
	return nil
}

// All returns a defensive snapshot (test convenience).
func (p *InMemoryPostPublisher) All() []PostRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]PostRecord, len(p.records))
	copy(out, p.records)
	return out
}

// InMemoryPreferenceStore is a thread-safe in-memory PreferenceStore.
type InMemoryPreferenceStore struct {
	mu sync.Mutex
	by map[string]Policy
}

// NewInMemoryPreferenceStore returns an empty in-memory preference store.
func NewInMemoryPreferenceStore() *InMemoryPreferenceStore {
	return &InMemoryPreferenceStore{by: make(map[string]Policy)}
}

// Get returns the preference for (tenant, gcid) or (zero, false).
func (p *InMemoryPreferenceStore) Get(_ context.Context, tenantID, gcid string) (Policy, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pol, ok := p.by[tenantID+":"+gcid]
	return pol, ok, nil
}

// Set persists a preference. Both the port-compliant (3-arg + ctx-erased)
// and the convenience 3-arg-no-ctx forms are exposed via overloads — tests
// use Set(tenantID, gcid, pol); the port satisfies the SetCtx form.
//
// Per the PreferenceStore interface contract Set returns an error; in
// the in-memory variant it never errors.
func (p *InMemoryPreferenceStore) Set(tenantID, gcid string, pol Policy) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.by[tenantID+":"+gcid] = pol
}

// Upsert satisfies PreferenceStore.Upsert (the port version with ctx).
func (p *InMemoryPreferenceStore) Upsert(_ context.Context, tenantID, gcid string, pol Policy) error {
	p.Set(tenantID, gcid, pol)
	return nil
}

// Compile-time port assertion.
var _ PreferenceStore = (*InMemoryPreferenceStore)(nil)

// InMemoryIdempotencyStore is a thread-safe in-memory IdempotencyStore.
type InMemoryIdempotencyStore struct {
	mu   sync.Mutex
	seen map[string]bool
}

// NewInMemoryIdempotencyStore returns an empty idempotency store.
func NewInMemoryIdempotencyStore() *InMemoryIdempotencyStore {
	return &InMemoryIdempotencyStore{seen: make(map[string]bool)}
}

// Seen reports whether (handler, eventID) is already recorded — the READ-ONLY
// peek half of the port. It NEVER writes (CHO-2263).
func (i *InMemoryIdempotencyStore) Seen(_ context.Context, handler, eventID string) (bool, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.seen[handler+":"+eventID], nil
}

// Mark durably records (handler, eventID) — the commit half of the port,
// idempotent (a repeat Mark is a no-op). Handlers call it ONLY after the side
// effect lands.
func (i *InMemoryIdempotencyStore) Mark(_ context.Context, handler, eventID string) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.seen[handler+":"+eventID] = true
	return nil
}

// Recorded reports whether (handler, eventID) has been marked. Test inspector
// (ctx-free) so specs can assert the dedup state directly.
func (i *InMemoryIdempotencyStore) Recorded(handler, eventID string) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.seen[handler+":"+eventID]
}

// Compile-time port assertion.
var _ IdempotencyStore = (*InMemoryIdempotencyStore)(nil)
