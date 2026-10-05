// governance_policy_violation_subscriber.go — §8.2.
//
// GovernancePolicyViolationSubscriber consumes
//
//	chora.governance.policy.violation_detected.v1
//
// to moderation-hide flagged shared atoms. It appends a
// 'moderation_hidden' ShareEvent to the atom_share_events audit log
// (the immutable feed entry is NEVER mutated — R-15-A).
//
// Idempotency (CHO-2263 seen→process→mark): (handler, event_id) is peeked, the
// moderation-hide appends run, then the pair is marked — a post-peek failure
// re-runs on redelivery instead of being ACK-dropped. Ack-after-processing. DLQ
// on persistent failure.
package subscribers

import (
	"context"
	"errors"
	"fmt"
	"log"
)

// TopicGovernancePolicyViolation is the Governance event.
const TopicGovernancePolicyViolation = "chora.governance.policy.violation_detected.v1"

// HandlerGovernancePolicyViolation is the idempotency-key prefix.
const HandlerGovernancePolicyViolation = "governance:policy_violation"

// GovernancePolicyViolationEnvelope mirrors the consumed event payload.
type GovernancePolicyViolationEnvelope struct {
	EventID       string   `json:"event_id"`
	TenantID      string   `json:"tenant_id"`
	GCID          string   `json:"gcid"`            // the actor whose content was flagged
	ShareEntryIDs []string `json:"share_entry_ids"` // shared atoms to hide
	Reason        string   `json:"reason"`
	Severity      string   `json:"severity"`
}

// ShareEventAppender is the port for appending moderation_hidden events.
// The pg adapter implements this; tests inject an in-memory double.
type ShareEventAppender interface {
	AppendModerationHidden(ctx context.Context, tenantID, feedEntryID, actorGCID, reason, sourceEventID string) error
}

// GovernancePolicyViolationConfig bundles the subscriber's dependencies.
type GovernancePolicyViolationConfig struct {
	Appender    ShareEventAppender
	Idempotency IdempotencyStore
	Logger      Logger
}

// GovernancePolicyViolationSubscriber moderation-hides flagged shared atoms.
type GovernancePolicyViolationSubscriber struct {
	cfg GovernancePolicyViolationConfig
}

// NewGovernancePolicyViolationSubscriber builds the subscriber.
func NewGovernancePolicyViolationSubscriber(cfg GovernancePolicyViolationConfig) *GovernancePolicyViolationSubscriber {
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	return &GovernancePolicyViolationSubscriber{cfg: cfg}
}

// HandleGovernancePolicyViolation processes a governance.policy.violation_detected.v1 event.
// Idempotent on (handler, event_id). Appends 'moderation_hidden' ShareEvent
// to each flagged share entry (the immutable entry is NEVER mutated).
func (s *GovernancePolicyViolationSubscriber) HandleGovernancePolicyViolation(ctx context.Context, env GovernancePolicyViolationEnvelope) error {
	if s.cfg.Appender == nil {
		return errors.New("subscribers: share event appender not configured")
	}
	if s.cfg.Idempotency == nil {
		return errors.New("subscribers: idempotency store not configured")
	}

	// Peek → append → mark (CHO-2263): mark event_id AFTER the moderation-hide
	// appends land, so a transient failure NACKs unmarked and the redelivery
	// re-applies (AppendModerationHidden is idempotent on source_event_id)
	// rather than being silently ACK-dropped.
	return processOnce(ctx, s.cfg.Idempotency, HandlerGovernancePolicyViolation, env.EventID,
		func() {
			s.cfg.Logger.Printf("duplicate_event_skipped handler=%s event_id=%s", HandlerGovernancePolicyViolation, env.EventID)
		},
		func() error {
			// Append 'moderation_hidden' ShareEvent to each flagged share entry.
			for _, shareEntryID := range env.ShareEntryIDs {
				if err := s.cfg.Appender.AppendModerationHidden(ctx, env.TenantID, shareEntryID, env.GCID, env.Reason, env.EventID); err != nil {
					return fmt.Errorf("subscribers: append moderation_hidden for %s: %w", shareEntryID, err)
				}
			}

			s.cfg.Logger.Printf("moderation_hidden_applied event_id=%s entries=%d reason=%s",
				env.EventID, len(env.ShareEntryIDs), env.Reason)
			return nil
		})
}
