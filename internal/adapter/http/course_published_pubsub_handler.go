// course_published_pubsub_handler.go — CHO-2155 follow-up (chora-sharing half).
//
// HTTP push handler fronting the C+ discovery-feed projection. The event
// gateway posts chora.delivery.course.published.v1 deliveries to this endpoint
// (binary protobuf payload).
//
// Why this exists
// ---------------
// The topic, its binary schema, chora-delivery's publisher, the outbox tee and
// the binary encoder have ALL existed and been correct since 2026-07-01. What
// did not exist was any consumer: the topic had ZERO subscriptions, so the
// broker discarded every message (a topic with no subscription does not
// dead-letter — it evaporates). chora-sharing's discovery.HandleCoursePublished
// was wired to an in-process eventbus.InMemoryBus, so it could never see a
// cross-service event. The C+ public discovery feed has therefore been
// permanently empty.
//
// Per ddd-enforcement this handler is a thin transport-only translator — no
// business logic; the projection lives in internal/domain/discovery.
//
// ⚠ ORDERING — this handler must be DEPLOYED before the gateway routes the
// inbox to this service. The gateway routes an UNLISTED
// /api/internal/pubsub/{inbox} to chora-delivery by default (see chora-gateway
// internal_pubsub_passthrough.go), so enabling the route ahead of the
// SharingInboxes allowlist entry would 404 into chora-delivery, retry 5x, and
// dead-letter every message. A premature route is strictly worse than no
// route.
//
// Local delivery: the gateway forwards /api/internal/pubsub/course-published
// over HTTP to this service — no cloud subscription provisioning is
// involved. The optional bearer verifier (CHORA_PUBSUB_PUSH_AUDIENCE_BASE)
// enforces token auth at the edge when configured; otherwise the mesh /
// network layer is the enforcing gate.
package httpadapter

import (
	"context"
	"time"

	"net/http"

	"github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-sharing/internal/adapter/eventpush"

	"github.com/apollo-chora/chora-sharing/internal/domain/discovery"
)

// CoursePublishedPushDeps wires the push handler.
type CoursePublishedPushDeps struct {
	// Registry is the discovery-feed projection. Required.
	Registry *discovery.Registry
	// Verifier is the OIDC token verifier. Use
	// eventpush.NewVerifier(eventpush.VerifierConfig{}) for dev.
	Verifier *eventpush.Verifier
}

// NewCoursePublishedPushHandler returns the http.Handler bound to the discovery
// registry. Panics on nil Registry.
func NewCoursePublishedPushHandler(deps CoursePublishedPushDeps) http.Handler {
	if deps.Registry == nil {
		panic("httpapi: CoursePublishedPushHandler requires Registry")
	}
	return eventpush.NewHandler(eventpush.HandlerConfig{
		Verifier: deps.Verifier,
		Dispatch: dispatchCoursePublished(discovery.HandleCoursePublished(deps.Registry)),
	})
}

func dispatchCoursePublished(handle eventbus.Handler) eventpush.DispatchFunc {
	return func(ctx context.Context, msg eventpush.PushMessage) error {
		switch msg.Attributes["topic"] {
		case discovery.TopicCoursePublished, "":
			// Single-topic subscription — accept the registered topic, and an
			// empty `topic` attribute (the push envelope does not require one
			// when the subscription is bound to exactly one topic).
			return handle(ctx, busMessageFromPush(discovery.TopicCoursePublished, msg))
		default:
			// Unrelated topic — ack so a fan-out misconfiguration does not
			// become an infinite retry storm.
			return nil
		}
	}
}

// busMessageFromPush adapts an event-push message onto the eventbus.Message
// the domain handler consumes.
//
// For the binary path the envelope on the wire (inside the proto) wins — see
// protodecode.DecodePayloadMapWithAttrs, which lays attributes down first and
// lets the binary projection override them. The attribute-sourced envelope
// below therefore matters only for the JSON fallback and for trace propagation.
func busMessageFromPush(topic string, msg eventpush.PushMessage) eventbus.Message {
	a := msg.Attributes
	env := envelope.Envelope{
		EventID:     a["event_id"],
		TenantID:    a["tenant_id"],
		GCID:        a["gcid"],
		Traceparent: a["traceparent"],
		Tracestate:  a["tracestate"],
	}
	if t, err := time.Parse(time.RFC3339Nano, a["occurred_at"]); err == nil {
		env.OccurredAt = t
	}
	if t, err := time.Parse(time.RFC3339Nano, a["published_at"]); err == nil {
		env.PublishedAt = t
	}
	return eventbus.Message{
		Subject:  topic,
		Envelope: env,
		Payload:  msg.Data,
	}
}
