// Package events is the event publisher adapter for chora-sharing
// (S4.4 — A-Content-Sharing).
//
// Wraps eventbus.Publisher (chora-common/eventbus) so:
//
//   - **Production**: the NATS JetStream bus — payloads are binary protobuf
//     wire bytes per the canonical schema contract.
//   - **Tests**: eventbus.NewInMemoryBus() — same envelope projection,
//     synchronous delivery for deterministic assertions.
//
// Topics emitted:
//
//	chora.sharing.post.created.v1       (D2 transparency for public, else D1)
//	chora.sharing.reaction.added.v1     (D1 accountability)
//	chora.sharing.reaction.removed.v1   (D1 accountability)
//
// Relationship lifecycle events (chora.sharing.relationship.*.v1, ADR-230
// D4) do NOT flow through this adapter: the pg RelationshipRepo enqueues
// them onto sharing_outbox_events INSIDE the domain transaction (atomic
// state-write + publish). The legacy follow.{created,removed}.v1 publish
// methods are RETIRED — that family was declared but never published.
//
// IMDA evidence per ADR-141:
//   - Reaction = D1 accountability (audit trail of social actions).
//   - Post = D2 transparency when visibility=public (the post is on the
//     platform-public surface); else D1 accountability.
//
// Payload encoding: producer-side binary protobuf wire bytes via
// internal/adapter/events/protomarshal. Topics with a registered encoder
// emit the canonical binary wire format. Topics without an encoder fall
// back to JSON with a one-shot WARN. Per the codebase-wide outbox protobuf
// encoding gap surfaced in task #33.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"

	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protomarshal"
)

// Topic constants — canonical taxonomy per pub-sub-topology skill + §8.1.
// The 10 published events mandated by docs/chora-sharing.md §8.1.
const (
	TopicPostCreated     = "chora.sharing.post.created.v1"
	TopicReactionAdded   = "chora.sharing.reaction.added.v1"
	TopicReactionRemoved = "chora.sharing.reaction.removed.v1"

	// Atom sharing surface (§8.1).
	TopicAtomShared         = "chora.sharing.atom.shared.v1"
	TopicAtomLicensed       = "chora.sharing.atom.licensed.v1"
	TopicAtomRevoked        = "chora.sharing.atom.revoked.v1"
	TopicRoyaltySettled     = "chora.sharing.royalty.settled.v1"
	TopicLeaderboardUpdated = "chora.sharing.leaderboard.updated.v1"

	// Duel surface — emitted when a duel reaches a terminal state
	// (completed / forfeited). Consumed by the DuelCompletedSubscriber
	// to credit participation rewards + refresh the leaderboard.
	TopicDuelCompleted = "chora.sharing.duel.completed.v1"
)

// IMDA dimension labels (canonical ADR-141 values).
const (
	IMDAAccountability = "accountability" // D1
	IMDATransparency   = "transparency"   // D2
)

// Bus is the underlying publisher contract.
//
// Production: the NATS JetStream bus (chora-common/eventbus.JetStreamBus).
// Tests     : eventbus.InMemoryBus (synchronous mode for assertions).
type Bus = eventbus.Publisher

// Config wires the Publisher.
type Config struct {
	Bus           Bus
	SourceProject string // e.g. "chora-local"; defaults to env or "chora-local"
	SourceService string // defaults to "chora-sharing"
	Now           func() time.Time
}

// Publisher emits chora-sharing domain events to the supplied Bus.
type Publisher struct {
	cfg Config
}

// NewPublisher constructs a Publisher with sane defaults.
func NewPublisher(cfg Config) *Publisher {
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.SourceService == "" {
		cfg.SourceService = "chora-sharing"
	}
	if cfg.SourceProject == "" {
		cfg.SourceProject = os.Getenv("CHORA_SOURCE_PROJECT")
		if cfg.SourceProject == "" {
			cfg.SourceProject = "chora-local"
		}
	}
	return &Publisher{cfg: cfg}
}

// -----------------------------------------------------------------------------
// Event payloads
// -----------------------------------------------------------------------------

// PostEvent describes a Post creation. Visibility steers the IMDA
// dimension stamped on the envelope (public → transparency D2).
type PostEvent struct {
	PostID     string
	TenantID   string
	AuthorGCID string
	Visibility string
	AtomID     string
	Body       string
	OccurredAt time.Time
}

// ReactionEvent describes a reaction add / remove. TargetType is "post"
// or "atom" — the consumer differentiates by reading the field.
type ReactionEvent struct {
	ReactionID   string
	TenantID     string
	GCID         string
	TargetType   string // "post" | "atom"
	TargetID     string
	ReactionType string // "curious" | "insightful" | "like" | "inspired"
	OccurredAt   time.Time
}

// DuelEvent describes a duel completion. Emitted when a duel reaches a
// terminal state (completed or forfeited). The ChallengerGCID is always
// the initiator; OpponentGCID is the challenged player. WinnerGCID is
// empty for a draw. Scope is "ranked" or "friendly".
type DuelEvent struct {
	DuelID          string
	TenantID        string
	ChallengerGCID  string
	OpponentGCID    string
	WinnerGCID      string
	Scope           string
	ScoreChallenger int
	ScoreOpponent   int
	OccurredAt      time.Time
}

// RoyaltySettledEvent describes a royalty settlement — the double-entry
// accrual published when a grantee reuses a licensed atom. Mirrors the
// chora.sharing.royalty.settled.v1 proto payload. The caller MUST skip
// publish when the settlement amount is 0 (free / cc_* licenses incur no
// royalty — the domain's SettleRoyalty still returns a settlement struct
// carrying the IDs for audit, but no event is emitted).
type RoyaltySettledEvent struct {
	SettlementID    string
	GrantID         string
	OwnerGCID       string  // the author (credit side)
	GranteeTenantID string  // the reuser tenant (debit side)
	AtomID          string
	Amount          float64
	Currency        string  // mana / coins / reputation / usd
	UsageContext    string  // duel / live_quiz / question_set
	SourceEventID   string  // idempotency key (the triggering event)
	TenantID        string  // for envelope stamping
	OccurredAt      time.Time
}
// -----------------------------------------------------------------------------
// Publish methods
// -----------------------------------------------------------------------------

// PublishPostCreated emits chora.sharing.post.created.v1.
//
// Visibility steers IMDA dimension:
//   - public → transparency (D2) — public-platform audit trail.
//   - tenant / private → accountability (D1) — internal audit only.
func (p *Publisher) PublishPostCreated(ctx context.Context, e PostEvent) error {
	dim := IMDAAccountability
	if e.Visibility == "public" {
		dim = IMDATransparency
	}
	env := p.buildEnvelope(ctx, dim)
	if e.TenantID != "" {
		env.TenantID = e.TenantID
	}
	if e.AuthorGCID != "" {
		env.GCID = e.AuthorGCID
	}
	occurred := p.timestamp(e.OccurredAt)
	payload := map[string]any{
		"post_id":              e.PostID,
		"tenant_id":            e.TenantID,
		"author_gcid":          e.AuthorGCID,
		"visibility":           e.Visibility,
		"atom_id":              e.AtomID,
		"body":                 e.Body,
		"created_at":           occurred,
		"occurred_at":          occurred.Format(time.RFC3339Nano),
		"chora_imda_dimension": dim,
		"imda_lifecycle_stage": "runtime",
	}
	return p.publish(ctx, TopicPostCreated, env, payload)
}

// PublishReactionAdded emits chora.sharing.reaction.added.v1.
func (p *Publisher) PublishReactionAdded(ctx context.Context, e ReactionEvent) error {
	env := p.buildEnvelope(ctx, IMDAAccountability)
	if e.TenantID != "" {
		env.TenantID = e.TenantID
	}
	if e.GCID != "" {
		env.GCID = e.GCID
	}
	occurred := p.timestamp(e.OccurredAt)
	payload := map[string]any{
		"reaction_id":          e.ReactionID,
		"tenant_id":            e.TenantID,
		"actor_gcid":           e.GCID,
		"target_type":          e.TargetType,
		"target_id":            e.TargetID,
		"reaction_type":        e.ReactionType,
		"created_at":           occurred,
		"occurred_at":          occurred.Format(time.RFC3339Nano),
		"chora_imda_dimension": IMDAAccountability,
		"imda_lifecycle_stage": "runtime",
	}
	return p.publish(ctx, TopicReactionAdded, env, payload)
}

// PublishReactionRemoved emits chora.sharing.reaction.removed.v1.
func (p *Publisher) PublishReactionRemoved(ctx context.Context, e ReactionEvent) error {
	env := p.buildEnvelope(ctx, IMDAAccountability)
	if e.TenantID != "" {
		env.TenantID = e.TenantID
	}
	if e.GCID != "" {
		env.GCID = e.GCID
	}
	occurred := p.timestamp(e.OccurredAt)
	payload := map[string]any{
		"reaction_id":          e.ReactionID,
		"tenant_id":            e.TenantID,
		"actor_gcid":           e.GCID,
		"target_type":          e.TargetType,
		"target_id":            e.TargetID,
		"reaction_type":        e.ReactionType,
		"removed_at":           occurred,
		"occurred_at":          occurred.Format(time.RFC3339Nano),
		"chora_imda_dimension": IMDAAccountability,
		"imda_lifecycle_stage": "runtime",
	}
	return p.publish(ctx, TopicReactionRemoved, env, payload)
}

// PublishDuelCompleted emits chora.sharing.duel.completed.v1.
//
// Emitted when a duel reaches a terminal state (completed or forfeited).
// IMDA dimension = accountability (D1) — internal audit trail of social
// competitive actions. The winner_gcid is empty for a draw.
func (p *Publisher) PublishDuelCompleted(ctx context.Context, e DuelEvent) error {
	env := p.buildEnvelope(ctx, IMDAAccountability)
	if e.TenantID != "" {
		env.TenantID = e.TenantID
	}
	if e.ChallengerGCID != "" {
		env.GCID = e.ChallengerGCID
	}
	occurred := p.timestamp(e.OccurredAt)
	payload := map[string]any{
		"duel_id":              e.DuelID,
		"tenant_id":            e.TenantID,
		"challenger_gcid":      e.ChallengerGCID,
		"opponent_gcid":        e.OpponentGCID,
		"winner_gcid":          e.WinnerGCID,
		"scope":                e.Scope,
		"score_challenger":     e.ScoreChallenger,
		"score_opponent":       e.ScoreOpponent,
		"completed_at":         occurred,
		"occurred_at":          occurred.Format(time.RFC3339Nano),
		"chora_imda_dimension": IMDAAccountability,
		"imda_lifecycle_stage": "runtime",
	}
	return p.publish(ctx, TopicDuelCompleted, env, payload)
}

// PublishRoyaltySettled emits chora.sharing.royalty.settled.v1.
//
// Published when a royalty accrues on real reuse — a grantee's use of a
// licensed atom triggers a non-cash credit to the author. Double-entry:
// debit the reuser's tenant mana, credit the author's non-cash currency.
// Idempotent on source_event_id (the triggering event) — replays never
// double-credit. Free licenses incur no settlement (the caller skips
// publish when amount == 0).
//
// IMDA dimension = accountability (D1) — financial audit trail of atom
// royalty accrual.
func (p *Publisher) PublishRoyaltySettled(ctx context.Context, e RoyaltySettledEvent) error {
	env := p.buildEnvelope(ctx, IMDAAccountability)
	if e.TenantID != "" {
		env.TenantID = e.TenantID
	}
	if e.OwnerGCID != "" {
		env.GCID = e.OwnerGCID
	}
	occurred := p.timestamp(e.OccurredAt)
	payload := map[string]any{
		"settlement_id":        e.SettlementID,
		"grant_id":             e.GrantID,
		"owner_gcid":           e.OwnerGCID,
		"grantee_tenant_id":    e.GranteeTenantID,
		"atom_id":              e.AtomID,
		"amount":               e.Amount,
		"currency":             e.Currency,
		"usage_context":        e.UsageContext,
		"source_event_id":      e.SourceEventID,
		"occurred_at":          occurred.Format(time.RFC3339Nano),
		"chora_imda_dimension": IMDAAccountability,
		"imda_lifecycle_stage": "runtime",
	}
	return p.publish(ctx, TopicRoyaltySettled, env, payload)
}
// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

func (p *Publisher) buildEnvelope(ctx context.Context, imdaDim string) cgcenvelope.Envelope {
	now := p.cfg.Now()
	return cgcenvelope.Build(ctx, cgcenvelope.BuildOpts{
		SchemaVersion:      1,
		SourceProject:      p.cfg.SourceProject,
		SourceService:      p.cfg.SourceService,
		ChoraImdaDimension: imdaDim,
		ImdaLifecycleStage: "runtime",
		Now:                func() time.Time { return now },
	})
}

func (p *Publisher) timestamp(t time.Time) time.Time {
	if t.IsZero() {
		return p.cfg.Now()
	}
	return t.UTC()
}

// publish marshals the payload via internal/adapter/events/protomarshal
// (canonical binary protobuf wire bytes) and hands the bytes to the Bus.
// Topics without a registered encoder fall back to JSON with a one-shot
// WARN. Add a case to protomarshal.MarshalPayload.
func (p *Publisher) publish(ctx context.Context, topic string, env cgcenvelope.Envelope, payload map[string]any) error {
	if p.cfg.Bus == nil {
		return fmt.Errorf("events: bus not configured")
	}
	if err := cgcenvelope.Validate(env); err != nil {
		return fmt.Errorf("events: envelope validation: %w", err)
	}
	body, err := encodePayload(topic, env, payload)
	if err != nil {
		return fmt.Errorf("events: marshal %s: %w", topic, err)
	}
	if err := p.cfg.Bus.Publish(ctx, topic, env, body); err != nil {
		return fmt.Errorf("events: publish %s: %w", topic, err)
	}
	return nil
}

// -----------------------------------------------------------------------------
// Payload encoding — binary protobuf for Schema-Registry-attached topics,
// JSON fallback for topics that don't yet have a binary encoder. New topics
// MUST add a case in internal/adapter/events/protomarshal/MarshalPayload.
// -----------------------------------------------------------------------------

var (
	publisherWarnedTopicsMu sync.Mutex
	publisherWarnedTopics   = map[string]bool{}
)

func encodePayload(topic string, env cgcenvelope.Envelope, payload map[string]any) ([]byte, error) {
	bz, err := protomarshal.MarshalPayload(topic, protomarshal.Envelope{
		EventID:        env.EventID,
		IdempotencyKey: env.IdempotencyKey,
		TenantID:       env.TenantID,
		GCID:           env.GCID,
		OccurredAt:     env.OccurredAt,
		PublishedAt:    env.PublishedAt,
		Traceparent:    env.Traceparent,
		Tracestate:     env.Tracestate,
		SourceProject:  env.SourceProject,
		SourceService:  env.SourceService,
		SchemaVersion:  env.SchemaVersion,
	}, payload)
	if err == nil {
		return bz, nil
	}
	if !protomarshal.IsUnsupportedTopic(err) {
		return nil, err
	}

	// Topic not yet wired for binary protobuf. Log a one-shot WARN and fall
	// back to JSON so the pre-existing path doesn't regress. Track + add
	// encoders.
	publisherWarnedTopicsMu.Lock()
	if !publisherWarnedTopics[topic] {
		publisherWarnedTopics[topic] = true
		log.Printf("WARN events.Publisher: topic %q has no binary protobuf encoder — payload will JSON-marshal; add a case to internal/adapter/events/protomarshal/MarshalPayload.", topic)
	}
	publisherWarnedTopicsMu.Unlock()

	body, jErr := json.Marshal(payload)
	if jErr != nil {
		return nil, fmt.Errorf("events.Publisher: json fallback marshal: %w", jErr)
	}
	return body, nil
}
