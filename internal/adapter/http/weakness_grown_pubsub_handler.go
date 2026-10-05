// weakness_grown_pubsub_handler.go — ADR-196 B3 (chora-sharing half).
//
// HTTP push handler fronting the WeaknessGrownSubscriber. The event gateway
// posts chora.consumption.weakness.grown.v1 deliveries to this endpoint.
//
// The handler:
//  1. Verifies the optional bearer token on the Authorization header.
//  2. Decodes the push envelope (base64 -> bytes).
//  3. Dispatches to subscriber.HandleWeaknessGrown.
//  4. Returns 200 on ack, 5xx on subscriber failure (-> gateway retry).
//
// Per the always-loaded ddd-enforcement rule the HTTP handler is a thin
// transport-only translator — no business logic. The verifier wiring comes
// from env at cmd/server boot.
//
// Local delivery: the gateway forwards
// /api/internal/pubsub/weakness-grown over HTTP to this service — no cloud
// subscription provisioning is involved. The optional bearer verifier
// (CHORA_PUBSUB_PUSH_AUDIENCE_BASE) enforces token auth at the edge when
// configured; otherwise the mesh / network layer is the enforcing gate.
package httpadapter

import (
	"context"
	"fmt"
	"net/http"

	"github.com/apollo-chora/chora-sharing/internal/adapter/eventpush"

	"github.com/apollo-chora/chora-sharing/internal/adapter/subscribers"
)

// WeaknessGrownPushDeps wires the push handler.
type WeaknessGrownPushDeps struct {
	// Subscriber is the inbound adapter from internal/adapter/subscribers.
	// Required.
	Subscriber *subscribers.WeaknessGrownSubscriber
	// Verifier is the OIDC token verifier. Use
	// eventpush.NewVerifier(eventpush.VerifierConfig{}) for dev.
	Verifier *eventpush.Verifier
}

// NewWeaknessGrownPushHandler returns the http.Handler bound to the
// WeaknessGrownSubscriber. Panics on nil Subscriber.
func NewWeaknessGrownPushHandler(deps WeaknessGrownPushDeps) http.Handler {
	if deps.Subscriber == nil {
		panic("httpapi: WeaknessGrownPushHandler requires Subscriber")
	}
	return eventpush.NewHandler(eventpush.HandlerConfig{
		Verifier: deps.Verifier,
		Dispatch: dispatchWeaknessGrown(deps.Subscriber),
	})
}

func dispatchWeaknessGrown(sub *subscribers.WeaknessGrownSubscriber) eventpush.DispatchFunc {
	return func(ctx context.Context, msg eventpush.PushMessage) error {
		topic := msg.Attributes["topic"]
		switch topic {
		case subscribers.TopicWeaknessGrown, "":
			// Single-topic subscription — accept the registered topic, and an
			// empty `topic` attribute (Pub/Sub push does not require it when the
			// subscription is bound to exactly one topic).
			env, err := subscribers.DecodeWeaknessGrownWithAttrs(msg.Data, msg.Attributes)
			if err != nil {
				return fmt.Errorf("weakness_grown_push: weakness_grown decode: %w", err)
			}
			return sub.HandleWeaknessGrown(ctx, env)
		default:
			// FAIL LOUD. This arm used to `return nil` — an ACK — "so Pub/Sub
			// doesn't infinitely retry on fan-out misconfiguration". But an ack
			// tells Pub/Sub the event was handled and discards it PERMANENTLY. An
			// ack from the wrong place is strictly worse than a dead-letter: the
			// dead-letter is recoverable and visible, the ack is neither. Retry
			// and dead-letter is the correct failure mode for a message this
			// handler cannot process.
			return fmt.Errorf("weakness_grown_push: UNEXPECTED topic %q on a subscription bound "+
				"to %s — refusing to ack an event this handler cannot process; it will retry and "+
				"dead-letter (recoverable) rather than vanish", topic, subscribers.TopicWeaknessGrown)
		}
	}
}
