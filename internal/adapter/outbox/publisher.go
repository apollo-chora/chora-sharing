// Package outbox — Publisher implementation.
//
// Publisher satisfies the events.Bus interface used by
// `internal/adapter/events.Publisher` by writing every Publish call to the
// sharing_outbox_events table instead of publishing directly to the event
// bus. The Dispatcher (see dispatcher.go) drains the table to the bus on
// a separate goroutine. This decouples emission from bus availability
// — a crash between the domain state-write and Bus.Publish no longer
// loses events.
//
// The wire shape (topic + envelope + payload) is identical to what the
// legacy OutboxBus enqueued, so the existing events.Publisher and HTTP
// handlers are unchanged — only the Bus seam swaps.
//
// Topic validation:
//
//	The Publisher PRESERVES the topic-validation logic that previously
//	lived in OutboxBus.validateTopic via the new exported
//	ValidateTopicName function. The same chora.{domain}.{aggregate}.
//	{event_type}.v{N} shape is enforced and the same domain allowlist
//	(sharing / governance / closure) applies.
//
// Per `feedback_d6_resilience_first_class` B.6.2.a producer-side durable
// emission for the chora-sharing event streams.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"strings"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
)

// PublisherConfig wires the Publisher.
type PublisherConfig struct {
	// Store is the outbox table backend. Required.
	Store Store

	// SourceProject is the project label stamped into event envelopes.
	// Defaults to "chora-local".
	SourceProject string

	// SourceService is the publisher's service name. Defaults to
	// "chora-sharing".
	SourceService string
}

// Publisher satisfies the events.Bus interface (Publish(ctx, topic, env,
// payload) error) by enqueueing the event into sharing_outbox_events.
type Publisher struct {
	cfg PublisherConfig
}

// NewPublisher constructs a Publisher.
func NewPublisher(cfg PublisherConfig) *Publisher {
	if cfg.SourceProject == "" {
		cfg.SourceProject = "chora-local"
	}
	if cfg.SourceService == "" {
		cfg.SourceService = "chora-sharing"
	}
	return &Publisher{cfg: cfg}
}

// Publish satisfies the events.Bus interface. Writes the event as a
// pending row in sharing_outbox_events. The Dispatcher publishes to the
// event bus asynchronously.
//
// Validates:
//   - topic shape (via ValidateTopicName — ported from OutboxBus)
//   - envelope mandatory fields (via cgcenvelope.Validate)
//
// On a duplicate idempotency_key, Publish returns
// ErrDuplicateIdempotencyKey — callers (e.g. the legacy events.Publisher
// wrapper) MAY treat this as a soft-success since the prior emission is
// already durably queued.
func (p *Publisher) Publish(ctx context.Context, topic string, env cgcenvelope.Envelope, payload []byte) error {
	if p.cfg.Store == nil {
		return errors.New("outbox: store not wired")
	}
	row, err := BuildRow(topic, env, payload)
	if err != nil {
		return err
	}
	return p.cfg.Store.Insert(ctx, row)
}

// BuildRow validates topic + envelope and projects them onto the canonical
// sharing_outbox_events Row shape. Shared by the Publisher (Bus seam) and the
// pg RelationshipRepo, which inserts the row INSIDE the domain transaction so
// relationship state-write + event-publish are atomic (ADR-230 D4).
func BuildRow(topic string, env cgcenvelope.Envelope, payload []byte) (Row, error) {
	if err := ValidateTopicName(topic); err != nil {
		return Row{}, fmt.Errorf("outbox.Publisher: %w", err)
	}
	if err := cgcenvelope.Validate(env); err != nil {
		return Row{}, fmt.Errorf("outbox.Publisher: envelope: %w", err)
	}
	aggType, aggID := deriveAggregateFromTopic(topic, env)
	return Row{
		ID:             env.EventID,
		TenantID:       env.TenantID,
		GCID:           env.GCID,
		AggregateType:  aggType,
		AggregateID:    aggID,
		EventType:      deriveEventTypeFromTopic(topic),
		Topic:          topic,
		Payload:        append([]byte(nil), payload...),
		Envelope:       envelopeToStringMap(env),
		IdempotencyKey: env.IdempotencyKey,
		OccurredAt:     env.OccurredAt,
	}, nil
}

// envelopeToStringMap projects a cgcenvelope.Envelope onto the flat
// string map persisted in the JSONB envelope column. The Dispatcher
// reverses this projection in reconstructEnvelope.
func envelopeToStringMap(env cgcenvelope.Envelope) map[string]string {
	out := map[string]string{
		"event_id":             env.EventID,
		"idempotency_key":      env.IdempotencyKey,
		"tenant_id":            env.TenantID,
		"gcid":                 env.GCID,
		"occurred_at":          env.OccurredAt.UTC().Format(timeRFC3339Nano),
		"published_at":         env.PublishedAt.UTC().Format(timeRFC3339Nano),
		"traceparent":          env.Traceparent,
		"tracestate":           env.Tracestate,
		"source_project":       env.SourceProject,
		"source_service":       env.SourceService,
		"schema_version":       fmt.Sprintf("%d", env.SchemaVersion),
		"correlation_id":       env.CorrelationID,
		"causation_id":         env.CausationID,
		"chora_imda_dimension": env.ChoraImdaDimension,
		"imda_lifecycle_stage": env.ImdaLifecycleStage,
	}
	return out
}

// deriveEventTypeFromTopic returns the event_type portion of the topic
// (everything after chora.). For chora.sharing.follow.created.v1 the
// derived event_type is sharing.follow.created.
func deriveEventTypeFromTopic(topic string) string {
	parts := strings.SplitN(topic, ".", 2)
	if len(parts) < 2 {
		return topic
	}
	rest := parts[1]
	// Strip trailing v{N} segment for human-friendly event_type.
	if dot := strings.LastIndex(rest, "."); dot >= 0 {
		last := rest[dot+1:]
		if len(last) >= 2 && last[0] == 'v' {
			return rest[:dot]
		}
	}
	return rest
}

// deriveAggregateFromTopic returns (aggregate_type, aggregate_id) for the
// outbox row. aggregate_type = parts[2] of the topic (e.g. "follow" for
// chora.sharing.follow.created.v1); aggregate_id = env.GCID (the subject
// of the event), falling back to env.EventID for system-emitted events
// with no GCID. Matches the canonical chora-go-common/outbox Row shape +
// the sharing_outbox_events migration columns (aggregate_type, aggregate_id).
func deriveAggregateFromTopic(topic string, env cgcenvelope.Envelope) (string, string) {
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

// timeRFC3339Nano matches the layout used in the chora-go-common
// envelope JSON wire format and the chora-guardrail outbox envelope
// reconstruction.
const timeRFC3339Nano = "2006-01-02T15:04:05.999999999Z07:00"

// -----------------------------------------------------------------------------
// ValidateTopicName — ported from the legacy OutboxBus.validateTopic.
// -----------------------------------------------------------------------------

// ValidateTopicName enforces the chora.{domain}.{aggregate}.{event_type}.v{N}
// shape and locks the domain segment to one of the allowed values:
//
//   - sharing (chora-sharing's own events)
//   - governance (IMDA evidence events emitted through the sharing service)
//   - closure (federated closure-saga events fanned out via the sharing
//     service's per-domain pseudonymise pipeline)
//
// This function preserves the validation contract that previously lived
// in the alt-pattern OutboxBus.validateTopic so the canonical outbox
// adapter remains drop-in compatible with the existing events.Publisher
// call-sites.
func ValidateTopicName(topic string) error {
	t := strings.TrimSpace(topic)
	if t == "" {
		return errors.New("outbox: topic required")
	}
	parts := strings.Split(t, ".")
	if len(parts) < 5 {
		return errors.New("outbox: topic must follow chora.{domain}.{aggregate}.{event_type}.v{N} (need at least 5 segments)")
	}
	if parts[0] != "chora" {
		return errors.New("outbox: topic must start with 'chora.'")
	}
	if !isAllowedDomain(parts[1]) {
		return fmt.Errorf("outbox: topic domain must be 'sharing'/'governance'/'closure'; got %q", parts[1])
	}
	last := parts[len(parts)-1]
	if !strings.HasPrefix(last, "v") || len(last) < 2 {
		return errors.New("outbox: topic must end with v{N} version suffix")
	}
	for _, ch := range last[1:] {
		if ch < '0' || ch > '9' {
			return errors.New("outbox: topic version suffix must be numeric")
		}
	}
	return nil
}

// allowedDomains is the closed set of domain segments permitted in
// chora-sharing's outbox topics. Aligned with the legacy OutboxBus
// allowlist for behavioural parity.
var allowedDomains = map[string]struct{}{
	"sharing":    {},
	"governance": {},
	"closure":    {},
}

func isAllowedDomain(d string) bool {
	_, ok := allowedDomains[d]
	return ok
}
