// outbox_bus.go — outbox-backed Bus implementation for chora-sharing.
//
// OutboxBus satisfies the events.Bus interface by writing every Publish
// call to the chora_sharing outbox_events table via
// chora-common/outbox.Recorder. A separate dispatcher drains pending rows
// to the event bus.
//
// Why a Bus implementation (vs decorator)?
//
// chora-sharing's Publisher takes a Bus dependency directly — every Publish
// call (PublishFollowCreated, PublishPostCreated, …) ultimately calls
// bus.Publish(ctx, topic, env, payload). Replacing the Bus with an outbox-
// backed one is the smallest possible delta and keeps the Publisher's
// envelope minting + IMDA dimension stamping logic untouched.
//
// Resilience properties (per `feedback_resilience_priority`):
//   - Atomic outbox: domain state + event-publish in one txn (no orphans)
//   - Dead-pod resume: SKIP-LOCKED claim contract on the Relay side
//   - DLQ: ultimate publish failure -> status=deadlettered with reason
package events

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	cgcoutbox "github.com/apollo-chora/chora-common/outbox"
)

// OutboxBus is an outbox-backed Bus.
type OutboxBus struct {
	rec cgcoutbox.Recorder
}

// NewOutboxBus constructs an OutboxBus.
func NewOutboxBus(rec cgcoutbox.Recorder) *OutboxBus {
	return &OutboxBus{rec: rec}
}

// Publish satisfies events.Bus. Validates topic + envelope, writes a
// pending outbox row, and returns. The outbox dispatcher drains pending
// rows to the event bus.
func (b *OutboxBus) Publish(ctx context.Context, topic string, env cgcenvelope.Envelope, payload []byte) error {
	if err := validateTopic(topic); err != nil {
		return fmt.Errorf("events.OutboxBus: %w", err)
	}
	if err := cgcenvelope.Validate(env); err != nil {
		return fmt.Errorf("events.OutboxBus: envelope: %w", err)
	}
	aggregateType, aggregateID := deriveAggregate(topic, env)

	row := &cgcoutbox.Row{
		ID:            newRowID(),
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		EventType:     deriveEventType(topic),
		Topic:         topic,
		Payload:       append([]byte(nil), payload...),
		Envelope:      env,
		OccurredAt:    env.OccurredAt,
		Status:        cgcoutbox.StatusPending,
	}
	return b.rec.Record(ctx, nil, row)
}

// validateTopic enforces chora.{domain}.{aggregate}.{event_type}.v{N} shape
// and locks domain to "sharing" or "governance" (governance for IMDA evidence).
func validateTopic(topic string) error {
	t := strings.TrimSpace(topic)
	if t == "" {
		return errors.New("topic required")
	}
	parts := strings.Split(t, ".")
	if len(parts) < 5 {
		return errors.New("topic must follow chora.{domain}.{aggregate}.{event_type}.v{N}")
	}
	if parts[0] != "chora" {
		return errors.New("topic must start with 'chora.'")
	}
	if parts[1] != "sharing" && parts[1] != "governance" && parts[1] != "closure" {
		return fmt.Errorf("topic domain must be 'sharing'/'governance'/'closure'; got %q", parts[1])
	}
	last := parts[len(parts)-1]
	if !strings.HasPrefix(last, "v") || len(last) < 2 {
		return errors.New("topic must end with v{N} version suffix")
	}
	for _, ch := range last[1:] {
		if ch < '0' || ch > '9' {
			return errors.New("topic version suffix must be numeric")
		}
	}
	return nil
}

// deriveAggregate returns (aggregate_type, aggregate_id) for a topic +
// envelope. Defaults: aggregate_type = parts[2] of topic; aggregate_id =
// envelope.GCID (else event_id).
func deriveAggregate(topic string, env cgcenvelope.Envelope) (string, string) {
	parts := strings.Split(topic, ".")
	aggregateType := "post"
	if len(parts) >= 3 {
		aggregateType = parts[2]
	}
	aggregateID := env.GCID
	if aggregateID == "" {
		aggregateID = env.EventID
	}
	return aggregateType, aggregateID
}

func deriveEventType(topic string) string {
	parts := strings.SplitN(topic, ".", 3)
	if len(parts) < 3 {
		return topic
	}
	return parts[2]
}

// newRowID mints the outbox row id as a UUIDv7, per the platform rule for
// new rows. There is deliberately no v4 fallback: uuid.NewV7 fails only when
// the process CSPRNG fails, and quietly substituting a timeless v4 id would
// bury that rather than surface it.
func newRowID() string {
	return uuid.Must(uuid.NewV7()).String()
}
