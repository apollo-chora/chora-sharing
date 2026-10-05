// live_quiz_score_pubsub_handler.go — ADR-168 Task #8 (chora-sharing half).
//
// HTTP push handler fronting the LiveQuizScoreSubscriber. Until 2026-07-15 this
// file did not exist: the subscriber was bound only to the in-process
// InMemoryBus, which NOTHING outside the process ever publishes to. chora-sharing
// therefore consumed nothing from the event bus on this lane — a `Subscribe()`
// call is not evidence that anything is consumed. Meanwhile chora-gateway
// faithfully forwarded every score_awarded push to
// /api/internal/pubsub/live-quiz-scores, which 404'd, and the broker retried it
// five times and dead-lettered it. (CHO-2195.)
//
// The handler:
//  1. Verifies the optional bearer token on the Authorization header.
//  2. Decodes the push envelope (base64 → bytes).
//  3. Dispatches to subscriber.HandleScoreAwarded (idempotent by event_id).
//  4. Returns 200 on ack, 5xx on failure → gateway retry.
//
// Per ddd-enforcement the HTTP handler is a thin transport-only translator — no
// business logic. The verifier wiring comes from env at cmd/server boot.
//
// Local delivery: the gateway forwards
// /api/internal/pubsub/live-quiz-scores over HTTP to this service — no cloud
// subscription provisioning is involved. The optional bearer verifier
// (CHORA_PUBSUB_PUSH_AUDIENCE_BASE) enforces token auth at the edge when
// configured; otherwise the mesh / network layer is the enforcing gate.
//
// NB chora-consumption owns a SEPARATE inbox (`live-quiz-score-awarded`) fed by
// the same topic for the derived Growth-Edge projection. Two consumers, two
// inboxes — do not collapse them.
package httpadapter

import (
	"context"
	"fmt"
	"net/http"

	"github.com/apollo-chora/chora-sharing/internal/adapter/eventpush"

	"github.com/apollo-chora/chora-sharing/internal/adapter/subscribers"
)

// LiveQuizScorePushDeps wires the push handler.
type LiveQuizScorePushDeps struct {
	// Subscriber is the inbound adapter from internal/adapter/subscribers.
	// Required.
	Subscriber *subscribers.LiveQuizScoreSubscriber
	// Verifier is the OIDC token verifier. Use
	// eventpush.NewVerifier(eventpush.VerifierConfig{}) for dev.
	Verifier *eventpush.Verifier
}

// NewLiveQuizScorePushHandler returns the http.Handler bound to the
// LiveQuizScoreSubscriber. Panics on nil Subscriber — a push route that exists
// but dispatches nowhere would 200 and discard the event permanently.
func NewLiveQuizScorePushHandler(deps LiveQuizScorePushDeps) http.Handler {
	if deps.Subscriber == nil {
		panic("httpapi: LiveQuizScorePushHandler requires Subscriber")
	}
	return eventpush.NewHandler(eventpush.HandlerConfig{
		Verifier: deps.Verifier,
		Dispatch: dispatchLiveQuizScore(deps.Subscriber),
	})
}

func dispatchLiveQuizScore(sub *subscribers.LiveQuizScoreSubscriber) eventpush.DispatchFunc {
	return func(ctx context.Context, msg eventpush.PushMessage) error {
		// Single-topic subscription: accept the registered topic, and an empty
		// `topic` attribute (Pub/Sub push does not require one when the
		// subscription is bound to exactly one topic).
		switch topic := msg.Attributes["topic"]; topic {
		case subscribers.TopicLiveQuizScoreAwarded, "":
			env, err := subscribers.DecodeScoreAwardedWithAttrs(msg.Data, msg.Attributes)
			if err != nil {
				return fmt.Errorf("live_quiz_score_push: score_awarded decode: %w", err)
			}
			return sub.HandleScoreAwarded(ctx, env)
		default:
			// FAIL LOUD. An unexpected topic on a single-topic subscription is a
			// fan-out misconfiguration — and returning nil here would ACK it,
			// telling Pub/Sub the event was handled and discarding it forever. An
			// ack from the wrong place is strictly worse than a dead-letter: the
			// dead-letter is recoverable and visible, the ack is neither.
			return fmt.Errorf("live_quiz_score_push: UNEXPECTED topic %q on a subscription bound "+
				"to %s — refusing to ack an event this handler cannot process; it will retry and "+
				"dead-letter (recoverable) rather than vanish", topic, subscribers.TopicLiveQuizScoreAwarded)
		}
	}
}
