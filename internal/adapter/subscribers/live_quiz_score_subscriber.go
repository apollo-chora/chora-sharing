// live_quiz_score_subscriber.go — ADR-168 Task #8.
//
// LiveQuizScoreSubscriber consumes the durable Content Delivery event
//
//	chora.delivery.live_quiz_session.score_awarded.v1
//
// published each time a learner's answer is graded in a Live Classroom
// quiz, and rolls the awarded points into the cross-session Leaderboard
// (the Three-Currency XP economy — XP axis).
//
// Live-quiz performance therefore feeds the durable leaderboard: each award
// credits the learner's gcid on the TENANT-scoped weekly + all-time boards
// via leaderboard.Ranker.SubmitTenant.
//
// Idempotency (CHO-2263 seen→process→mark): the (handler, event_id) pair is
// PEEKED in the shared IdempotencyStore, the credit runs, and the pair is MARKED
// only AFTER it lands (via processOnce) — a post-peek failure re-runs on
// redelivery instead of being ACK-dropped. Re-delivery (Pub/Sub at-least-once)
// skips with a "duplicate_event_skipped" structured-log span event. The HTTP
// push handler / bus handler acks AFTER this method returns nil; a returned
// error nacks → broker retries → eventually deadletters (DLQ), mirroring the
// FamiliarMilestoneSubscriber delivery-resilience pattern.
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

	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protodecode"
	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
)

// TopicLiveQuizScoreAwarded is the durable Content Delivery topic this
// subscriber consumes.
const TopicLiveQuizScoreAwarded = "chora.delivery.live_quiz_session.score_awarded.v1"

// HandlerLiveQuizScoreAwarded is the idempotency-key prefix + the OTLP span
// attribute "chora.subscriber.handler" for this subscriber.
const HandlerLiveQuizScoreAwarded = "live_quiz_score:score_awarded"

// ScoreAwardedEnvelope mirrors the score_awarded.v1 payload (the body inside
// the standard event envelope) plus the canonical envelope fields the
// publisher places on Pub/Sub msg.Attributes.
type ScoreAwardedEnvelope struct {
	// Envelope fields (carried on Pub/Sub attributes).
	EventID  string `json:"event_id"`
	TenantID string `json:"tenant_id"`
	GCID     string `json:"gcid"`

	// Payload fields.
	SessionID       string `json:"session_id"`
	LiveQuizID      string `json:"live_quiz_id"`
	QuestionID      string `json:"question_id"`
	AwardedPoints   int    `json:"awarded_points"`
	CumulativeScore int    `json:"cumulative_score"`
	Correct         bool   `json:"correct"`
	AnswerMillis    int64  `json:"answer_millis"`
}

// LiveQuizScoreConfig bundles the subscriber's dependencies.
type LiveQuizScoreConfig struct {
	// Ranker is the cross-session leaderboard aggregator. Required.
	Ranker *leaderboard.Ranker
	// Idempotency dedups (handler, event_id). Required.
	Idempotency IdempotencyStore
	// Logger is optional; falls back to log.Default().
	Logger Logger
}

// LiveQuizScoreSubscriber rolls live-quiz score_awarded events into the
// durable Leaderboard.
type LiveQuizScoreSubscriber struct {
	cfg LiveQuizScoreConfig
}

// NewLiveQuizScoreSubscriber builds a subscriber with sane defaults.
func NewLiveQuizScoreSubscriber(cfg LiveQuizScoreConfig) *LiveQuizScoreSubscriber {
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	return &LiveQuizScoreSubscriber{cfg: cfg}
}

// SubscribedTopics returns the single topic this subscriber listens to.
func (s *LiveQuizScoreSubscriber) SubscribedTopics() []string {
	return []string{TopicLiveQuizScoreAwarded}
}

// tenantScope is the leaderboard scope live-quiz awards credit. Live quizzes
// are run by a tenant's instructor for that tenant's learners, so the award
// is tenant-segregated (never global / cohort) per the SubmitTenant key.
var tenantScope = leaderboard.Scope{Kind: leaderboard.ScopeTenant}

// HandleScoreAwarded processes one score_awarded.v1 event: it idempotently
// credits AwardedPoints to (tenant, gcid) on the weekly + all-time tenant
// boards.
func (s *LiveQuizScoreSubscriber) HandleScoreAwarded(ctx context.Context, e ScoreAwardedEnvelope) error {
	if err := s.preflight(); err != nil {
		return err
	}
	if err := validateBase(e.EventID, e.TenantID, e.GCID); err != nil {
		return err
	}

	// Peek → credit → mark (CHO-2263): the key is marked AFTER the credit lands,
	// so a post-peek failure NACKs unmarked and the redelivery re-credits rather
	// than being silently ACK-dropped.
	return processOnce(ctx, s.cfg.Idempotency, HandlerLiveQuizScoreAwarded, e.EventID,
		func() { s.spanEventDuplicate(HandlerLiveQuizScoreAwarded, e.EventID, e.TenantID, e.GCID) },
		func() error {
			// Non-positive awards (wrong answer, no-credit) are a no-op on the board
			// but still mark the idempotency key so a replay stays a no-op.
			if e.AwardedPoints <= 0 {
				s.logAudit(HandlerLiveQuizScoreAwarded, e.EventID, e.TenantID, e.GCID, "no_credit")
				return nil
			}

			// Credit the XP axis of the Three-Currency economy on both rolling
			// windows so the live-quiz performance feeds weekly + all-time boards.
			s.cfg.Ranker.SubmitTenant(tenantScope, leaderboard.PeriodWeekly, e.TenantID, e.GCID, e.AwardedPoints)
			s.cfg.Ranker.SubmitTenant(tenantScope, leaderboard.PeriodAllTime, e.TenantID, e.GCID, e.AwardedPoints)

			s.logAudit(HandlerLiveQuizScoreAwarded, e.EventID, e.TenantID, e.GCID, "credited")
			return nil
		})
}

func (s *LiveQuizScoreSubscriber) preflight() error {
	if s.cfg.Ranker == nil || s.cfg.Idempotency == nil {
		return errors.New("subscribers: missing dependency (Ranker | Idempotency)")
	}
	return nil
}

func (s *LiveQuizScoreSubscriber) spanEventDuplicate(handler, eventID, tenantID, gcid string) {
	s.cfg.Logger.Printf(`{"event":"duplicate_event_skipped","handler":"%s","source_event_id":"%s","tenant_id":"%s","gcid":"%s"}`,
		handler, eventID, tenantID, gcid)
}

func (s *LiveQuizScoreSubscriber) logAudit(handler, eventID, tenantID, gcid, outcome string) {
	s.cfg.Logger.Printf(`{"event":"live_quiz_score_handled","handler":"%s","source_event_id":"%s","tenant_id":"%s","gcid":"%s","outcome":"%s"}`,
		handler, eventID, tenantID, gcid, outcome)
}

// -----------------------------------------------------------------------------
// Wire decoder
// -----------------------------------------------------------------------------

// DecodeScoreAwardedWithAttrs is the attribute-aware variant. It routes
// through protodecode so a future binaryDecoders entry (once the generated
// proto lands) transparently takes over without changing this call-site.
func DecodeScoreAwardedWithAttrs(blob []byte, attrs map[string]string) (ScoreAwardedEnvelope, error) {
	m, err := protodecode.DecodePayloadMapWithAttrs(TopicLiveQuizScoreAwarded, blob, attrs)
	if err != nil {
		return ScoreAwardedEnvelope{}, fmt.Errorf("subscribers: decode score_awarded: %w", err)
	}
	return ScoreAwardedEnvelope{
		EventID:         asString(m["event_id"]),
		TenantID:        asString(m["tenant_id"]),
		GCID:            firstNonEmpty(asString(m["gcid"]), asString(m["owner_gcid"])),
		SessionID:       asString(m["session_id"]),
		LiveQuizID:      asString(m["live_quiz_id"]),
		QuestionID:      asString(m["question_id"]),
		AwardedPoints:   asInt(m["awarded_points"]),
		CumulativeScore: asInt(m["cumulative_score"]),
		Correct:         asBool(m["correct"]),
		AnswerMillis:    asInt64(m["answer_millis"]),
	}, nil
}

// asInt coerces a map value to int (json.Unmarshal yields float64 for numbers).
func asInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return 0
}

// asInt64 coerces a map value to int64.
func asInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	}
	return 0
}
