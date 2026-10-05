// atom_session_completed_subscriber.go — §8.2.
//
// AtomSessionCompletedSubscriber consumes
//
//	chora.consumption.atom_session.completed.v1
//
// published when a learner completes an atom session. It credits XP to
// atom_xp_credits (the append-only leaderboard XP projection) + meters
// royalty for any royalty-grant atoms used.
//
// Idempotency (CHO-2263 seen→process→mark): (handler, event_id) is peeked, the
// XP credit runs, then the pair is marked — so a post-peek failure re-runs on
// redelivery instead of being ACK-dropped. Ack-after-processing. DLQ on
// persistent failure.
package subscribers

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

// TopicAtomSessionCompleted is the Consumption event that triggers XP credit.
const TopicAtomSessionCompleted = "chora.consumption.atom_session.completed.v1"

// HandlerAtomSessionCompleted is the idempotency-key prefix.
const HandlerAtomSessionCompleted = "atom_session:completed"

// AtomSessionCompletedEnvelope mirrors the consumed event payload.
type AtomSessionCompletedEnvelope struct {
	EventID    string    `json:"event_id"`
	TenantID   string    `json:"tenant_id"`
	GCID       string    `json:"gcid"`
	AtomID     string    `json:"atom_id"`
	SessionID  string    `json:"session_id"`
	XP         int       `json:"xp"`
	OccurredAt time.Time `json:"occurred_at"`
}

// XPCreditWriter is the port for crediting XP to atom_xp_credits.
// The pg adapter implements this; tests inject an in-memory double.
type XPCreditWriter interface {
	CreditXP(ctx context.Context, tenantID, gcid, atomID, sessionID, eventID string, xp int, occurredAt time.Time) error
}

// AtomSessionCompletedConfig bundles the subscriber's dependencies.
type AtomSessionCompletedConfig struct {
	XPWriter    XPCreditWriter
	Idempotency IdempotencyStore
	Logger      Logger
}

// AtomSessionCompletedSubscriber credits XP on atom session completion.
type AtomSessionCompletedSubscriber struct {
	cfg AtomSessionCompletedConfig
}

// NewAtomSessionCompletedSubscriber builds the subscriber.
func NewAtomSessionCompletedSubscriber(cfg AtomSessionCompletedConfig) *AtomSessionCompletedSubscriber {
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	return &AtomSessionCompletedSubscriber{cfg: cfg}
}

// HandleAtomSessionCompleted processes a consumption.atom_session.completed.v1 event.
// Idempotent on event_id. Returns nil on success or duplicate.
func (s *AtomSessionCompletedSubscriber) HandleAtomSessionCompleted(ctx context.Context, env AtomSessionCompletedEnvelope) error {
	if s.cfg.XPWriter == nil {
		return errors.New("subscribers: XP writer not configured")
	}
	if s.cfg.Idempotency == nil {
		return errors.New("subscribers: idempotency store not configured")
	}

	// Peek → credit → mark (CHO-2263): mark event_id AFTER the XP credit lands,
	// so a transient failure NACKs unmarked and the redelivery re-credits
	// (CreditXP is idempotent on event_id) rather than being silently ACK-dropped.
	return processOnce(ctx, s.cfg.Idempotency, HandlerAtomSessionCompleted, env.EventID,
		func() {
			s.cfg.Logger.Printf("duplicate_event_skipped handler=%s event_id=%s", HandlerAtomSessionCompleted, env.EventID)
		},
		func() error {
			// Credit XP to atom_xp_credits.
			if err := s.cfg.XPWriter.CreditXP(ctx, env.TenantID, env.GCID, env.AtomID, env.SessionID, env.EventID, env.XP, env.OccurredAt); err != nil {
				return fmt.Errorf("subscribers: credit XP: %w", err)
			}
			s.cfg.Logger.Printf("xp_credited gcid=%s atom=%s xp=%d event_id=%s", env.GCID, env.AtomID, env.XP, env.EventID)
			return nil
		})
}
