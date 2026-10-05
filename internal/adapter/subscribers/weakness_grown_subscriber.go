// weakness_grown_subscriber.go — ADR-196 tranche item B3 (chora-sharing half).
//
// WeaknessGrownSubscriber consumes the durable Content Consumption event
//
//	chora.consumption.weakness.grown.v1
//
// published (ADR-196 B1 / CHO-1897) on a real active->grown Growth-Edge
// transition — the learner recovered a demonstrated weakness (via a drill atom
// or sustained Ebbinghaus mastery). chora-sharing rewards the social/economy
// half of the Curiosity->Reward->Social loop by crediting XP to the learner on
// the Three-Currency economy (XP axis): the same durable Leaderboard the
// LiveQuizScoreSubscriber feeds, via leaderboard.Ranker.SubmitTenant on the
// tenant-scoped weekly + all-time boards.
//
// XP amount: a milestone-sized award (DefaultWeaknessGrownXP), config-
// overridable via WeaknessGrownConfig.XPAwardPoints (wired from
// CHORA_SHARING_WEAKNESS_GROWN_XP at cmd/server boot per the no-inline-config
// rule). Growing a weakness is the culmination of a focused-practice loop, so
// it warrants more than a single quiz answer (royalty base = 10).
//
// Coins are NOT awarded here: the Coins ledger (CurrencyLedger) is an unwired
// S5+ stub (internal/domain/stubs) with no repository — crediting it would be a
// fake adapter (no-stubs rule). XP via the Ranker is the only real wired economy
// path today; Coins is a documented follow-up gated on the real CurrencyLedger
// aggregate landing.
//
// Idempotency — one reward per real grow: the (handler, dedup-key) pair is
// PEEKED in the shared IdempotencyStore, the credit runs, and the pair is MARKED
// only AFTER it lands (CHO-2263 seen→process→mark; a post-peek failure re-runs
// on redelivery). The dedup key is
// the DOMAIN identity (growth_edge_id:grown_at), mirroring the producer's
// idempotency_key (chora.consumption.weakness.grown:{edge}:{grown_at}), so BOTH
// Pub/Sub at-least-once redelivery (same event_id) AND a producer re-publish
// (fresh event_id, same grow) are deduped to a single award. A genuine RE-grow
// of the same edge at a later time carries a distinct grown_at, so it is
// correctly rewarded again. When grown_at is absent the key falls back to
// event_id (redelivery still safe). Re-delivery skips with a
// "duplicate_event_skipped" structured-log span event; the HTTP push handler /
// bus handler acks AFTER this method returns nil, a returned error nacks ->
// broker retries -> DLQ.
//
// Hexagonal: this adapter depends on the leaderboard domain (Ranker) + the
// internal IdempotencyStore port. cmd/server wires the production Ranker +
// idempotency store; tests inject the in-memory doubles in this package.
package subscribers

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protodecode"
	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
)

// TopicWeaknessGrown is the durable Content Consumption topic this subscriber
// consumes (ADR-196 B1).
const TopicWeaknessGrown = "chora.consumption.weakness.grown.v1"

// HandlerWeaknessGrown is the idempotency-key prefix + the OTLP span attribute
// "chora.subscriber.handler" for this subscriber.
const HandlerWeaknessGrown = "weakness_grown:grown"

// DefaultWeaknessGrownXP is the milestone XP awarded for a real weakness grow
// when WeaknessGrownConfig.XPAwardPoints is unset (<= 0). Config-overridable at
// cmd/server boot from CHORA_SHARING_WEAKNESS_GROWN_XP.
const DefaultWeaknessGrownXP = 50

// WeaknessGrownEnvelope mirrors the chora.consumption.v1.WeaknessGrown payload
// (the body inside the standard event envelope) plus the canonical envelope
// fields the producer places on the binary Envelope / Pub/Sub msg.Attributes.
type WeaknessGrownEnvelope struct {
	// Envelope fields.
	EventID  string
	TenantID string
	// LearnerGCID is the learner who grew the edge (payload learner_gcid; falls
	// back to the envelope gcid).
	LearnerGCID string

	// Payload fields.
	GrowthEdgeID   string
	ConceptLabel   string
	ConceptKey     string
	FinalStrength  float64
	RecoverySource string
	Tags           []string
	// GrownAt is the RFC3339 grow timestamp; with GrowthEdgeID it forms the
	// dedup key.
	GrownAt string
}

// WeaknessGrownConfig bundles the subscriber's dependencies.
type WeaknessGrownConfig struct {
	// Ranker is the cross-session leaderboard aggregator (XP axis). Required.
	Ranker *leaderboard.Ranker
	// Idempotency dedups (handler, dedup-key). Required.
	Idempotency IdempotencyStore
	// XPAwardPoints is the XP credited per grow; <= 0 falls back to
	// DefaultWeaknessGrownXP.
	XPAwardPoints int
	// Logger is optional; falls back to log.Default().
	Logger Logger
}

// WeaknessGrownSubscriber credits XP for a real weakness grow.
type WeaknessGrownSubscriber struct {
	cfg WeaknessGrownConfig
}

// NewWeaknessGrownSubscriber builds a subscriber with sane defaults.
func NewWeaknessGrownSubscriber(cfg WeaknessGrownConfig) *WeaknessGrownSubscriber {
	if cfg.XPAwardPoints <= 0 {
		cfg.XPAwardPoints = DefaultWeaknessGrownXP
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	return &WeaknessGrownSubscriber{cfg: cfg}
}

// XPAward returns the effective XP credited per grow (after defaulting).
func (s *WeaknessGrownSubscriber) XPAward() int { return s.cfg.XPAwardPoints }

// SubscribedTopics returns the single topic this subscriber listens to.
func (s *WeaknessGrownSubscriber) SubscribedTopics() []string {
	return []string{TopicWeaknessGrown}
}

// HandleWeaknessGrown processes one weakness.grown.v1 event: it idempotently
// credits XPAward to (tenant, learner) on the weekly + all-time tenant boards.
func (s *WeaknessGrownSubscriber) HandleWeaknessGrown(ctx context.Context, e WeaknessGrownEnvelope) error {
	if err := s.preflight(); err != nil {
		return err
	}
	if err := validateBase(e.EventID, e.TenantID, e.LearnerGCID); err != nil {
		return err
	}
	if strings.TrimSpace(e.GrowthEdgeID) == "" {
		return errors.New("subscribers: growth_edge_id required")
	}

	// Idempotency on the domain key (edge:grown_at) — one reward per real grow,
	// surviving event_id churn (producer re-publish) + Pub/Sub at-least-once
	// redelivery. Peek → credit → mark (CHO-2263): the key is marked AFTER the
	// credit, so a post-peek failure re-runs on redelivery rather than ACK-dropping.
	dedupKey := e.dedupKey()
	return processOnce(ctx, s.cfg.Idempotency, HandlerWeaknessGrown, dedupKey,
		func() { s.spanEventDuplicate(HandlerWeaknessGrown, e.EventID, e.TenantID, e.LearnerGCID) },
		func() error {
			// Credit the XP axis of the Three-Currency economy on both rolling windows
			// so the grow feeds the weekly + all-time tenant boards.
			xp := s.cfg.XPAwardPoints
			s.cfg.Ranker.SubmitTenant(tenantScope, leaderboard.PeriodWeekly, e.TenantID, e.LearnerGCID, xp)
			s.cfg.Ranker.SubmitTenant(tenantScope, leaderboard.PeriodAllTime, e.TenantID, e.LearnerGCID, xp)

			s.logAudit(HandlerWeaknessGrown, e.EventID, e.TenantID, e.LearnerGCID, "credited")
			return nil
		})
}

// dedupKey is the domain-identity dedup key: growth_edge_id:grown_at (mirrors
// the producer idempotency_key). Falls back to event_id when grown_at is absent
// so redelivery stays safe.
func (e WeaknessGrownEnvelope) dedupKey() string {
	edge := strings.TrimSpace(e.GrowthEdgeID)
	grownAt := strings.TrimSpace(e.GrownAt)
	if edge != "" && grownAt != "" {
		return edge + ":" + grownAt
	}
	return e.EventID
}

func (s *WeaknessGrownSubscriber) preflight() error {
	if s.cfg.Ranker == nil || s.cfg.Idempotency == nil {
		return errors.New("subscribers: missing dependency (Ranker | Idempotency)")
	}
	return nil
}

func (s *WeaknessGrownSubscriber) spanEventDuplicate(handler, eventID, tenantID, gcid string) {
	s.cfg.Logger.Printf(`{"event":"duplicate_event_skipped","handler":"%s","source_event_id":"%s","tenant_id":"%s","gcid":"%s"}`,
		handler, eventID, tenantID, gcid)
}

func (s *WeaknessGrownSubscriber) logAudit(handler, eventID, tenantID, gcid, outcome string) {
	s.cfg.Logger.Printf(`{"event":"weakness_grown_handled","handler":"%s","source_event_id":"%s","tenant_id":"%s","gcid":"%s","outcome":"%s"}`,
		handler, eventID, tenantID, gcid, outcome)
}

// -----------------------------------------------------------------------------
// Wire decoder
// -----------------------------------------------------------------------------

// DecodeWeaknessGrownWithAttrs is the attribute-aware variant. It routes through
// protodecode so the registered binary decoder for the topic transparently
// handles the canonical binary wire shape, with JSON + attrs as fallback.
func DecodeWeaknessGrownWithAttrs(blob []byte, attrs map[string]string) (WeaknessGrownEnvelope, error) {
	m, err := protodecode.DecodePayloadMapWithAttrs(TopicWeaknessGrown, blob, attrs)
	if err != nil {
		return WeaknessGrownEnvelope{}, fmt.Errorf("subscribers: decode weakness_grown: %w", err)
	}
	return WeaknessGrownEnvelope{
		EventID:        asString(m["event_id"]),
		TenantID:       asString(m["tenant_id"]),
		LearnerGCID:    firstNonEmpty(asString(m["learner_gcid"]), asString(m["gcid"]), asString(m["owner_gcid"])),
		GrowthEdgeID:   asString(m["growth_edge_id"]),
		ConceptLabel:   asString(m["concept_label"]),
		ConceptKey:     asString(m["concept_key"]),
		FinalStrength:  asFloat64(m["final_strength"]),
		RecoverySource: asString(m["recovery_source"]),
		Tags:           asStringSlice(m["tags"]),
		GrownAt:        asString(m["grown_at"]),
	}, nil
}
