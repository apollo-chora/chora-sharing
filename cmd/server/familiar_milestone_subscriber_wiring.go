// familiar_milestone_subscriber_wiring.go — CHO-2203 (parent CHO-1889): the
// DURABLE event-bus lane for the FamiliarMilestoneSubscriber (PROD-G).
//
// Replaces the dead in-process bus binding (registerMilestoneSubscriber, main.go)
// that consumed NOTHING: the four milestone events are published by
// chora-consumption to the NATS JetStream bus, so a handler bound to the
// in-process fallback drained no real traffic. The four durable consumers
// that carried them were deleted under CHO-2195 because the subscriber's ports
// were all in-memory doubles (binding a broker receiver into evaporating stores
// is a hollow fix). With the pg ports + tables now landed (migrations
// 0039-0041), this file re-establishes the durable receiver — one consumer per
// topic:
//
//	chora.consumption.companion.stage_up.v1          → chora-sharing-companion-milestone-stage-up
//	chora.consumption.companion.breed_revealed.v1    → chora-sharing-companion-milestone-breed-revealed
//	chora.consumption.companion.hatched.v1           → chora-sharing-companion-milestone-hatched
//	chora.consumption.companion.source_revelation.v1 → chora-sharing-companion-milestone-source-revelation
//	chora.consumption.familiar.stage_up.v1           → chora-sharing-familiar-milestone-stage-up
//	chora.consumption.familiar.breed_revealed.v1     → chora-sharing-familiar-milestone-breed-revealed
//	chora.consumption.familiar.hatched.v1            → chora-sharing-familiar-milestone-hatched
//	chora.consumption.familiar.source_revelation.v1  → chora-sharing-familiar-milestone-source-revelation
//
// ADR-254 renamed the wire subjects familiar→companion: chora-consumption
// emits ONLY the canonical companion.* set today, so the four familiar.*
// consumers are kept purely for the mixed-version rollout window (an old
// consumption pod still emitting familiar.*). Both sets dispatch to the same
// Handle* methods under the same handler ids, so the subscriber_idempotency
// (handler, event_id) dedups a dual delivery across the two subjects instead
// of drafting twice.
//
// These topics keep their existing consumers — this ADDS durable consumers, it
// does not steal messages. Mirrors atom_projection_subscriber_wiring.go exactly
// (same one-goroutine-per-subject binding pattern).
//
// The handler stamps tracing.WithTenantID(ctx, env.TenantID) BEFORE dispatch so
// the pg IdempotencyStore.Claim (which reads the tenant from ctx — its port
// carries no tenant arg) applies RLS correctly; a decode/handler error NACKs →
// broker retry (→ DLQ).
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/adapter/subscribers"
)

// familiarMilestoneSubscriptions maps each consumed Consumption topic to its
// consumer-owned durable consumer name (chora-{service}-{purpose} convention).
// The companion.* entries are the canonical ADR-254 subjects chora-consumption
// emits today; the familiar.* entries are the legacy subjects kept for the
// mixed-version rollout window.
var familiarMilestoneSubscriptions = map[string]string{
	subscribers.TopicCompanionStageUp:          "chora-sharing-companion-milestone-stage-up",
	subscribers.TopicCompanionBreedRevealed:    "chora-sharing-companion-milestone-breed-revealed",
	subscribers.TopicCompanionHatched:          "chora-sharing-companion-milestone-hatched",
	subscribers.TopicCompanionSourceRevelation: "chora-sharing-companion-milestone-source-revelation",
	subscribers.TopicFamiliarStageUp:          "chora-sharing-familiar-milestone-stage-up",
	subscribers.TopicFamiliarBreedRevealed:    "chora-sharing-familiar-milestone-breed-revealed",
	subscribers.TopicFamiliarHatched:          "chora-sharing-familiar-milestone-hatched",
	subscribers.TopicFamiliarSourceRevelation: "chora-sharing-familiar-milestone-source-revelation",
}

// registerFamiliarMilestoneSubscriber spawns one durable-consumer goroutine per
// consumed topic. `bus` may be nil in dev (NATS_URL unset) — the caller falls
// back to the in-process bus in that case. `sub` MUST be non-nil.
func registerFamiliarMilestoneSubscriber(
	ctx context.Context,
	bus eventbus.Bus,
	sub *subscribers.FamiliarMilestoneSubscriber,
) error {
	if sub == nil {
		return errors.New("registerFamiliarMilestoneSubscriber: nil FamiliarMilestoneSubscriber")
	}
	if bus == nil {
		log.Printf("sharing: familiar-milestone subscriber SKIPPED (NATS_URL unset; in-memory fallback has no cross-service surface)")
		return nil
	}

	for topic, subscription := range familiarMilestoneSubscriptions {
		handler := buildFamiliarMilestoneHandler(sub, topic)
		go func(topic, subscription string) {
			log.Printf("sharing: familiar-milestone subscriber started (topic=%s consumer=%s, CHO-2203)", topic, subscription)
			if err := bus.Subscribe(ctx, consumerConfig(subscription, topic), handler); err != nil &&
				!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				log.Printf("sharing: familiar-milestone subscriber exited (topic=%s): %v", topic, err)
			}
		}(topic, subscription)
	}
	log.Printf("sharing: FamiliarMilestoneSubscriber bound to %d durable consumers (CHO-2203, durable)", len(familiarMilestoneSubscriptions))
	return nil
}

// buildFamiliarMilestoneHandler decodes one message (hand-written, attrs-aware
// decode pinned to today's stable fields) and forwards to the typed Handle*
// method, stamping tenant_id from the envelope onto ctx so the pg ports'
// rls.ApplySession can SET LOCAL chora.tenant_id.
//
// The companion.* (canonical, ADR-254) and familiar.* (legacy) subjects carry
// the same envelope, so both dispatch to the same Handle* method; the topic is
// threaded into the decoder only to select the right binary-proto mapping in
// protodecode.
func buildFamiliarMilestoneHandler(sub *subscribers.FamiliarMilestoneSubscriber, topic string) eventbus.Handler {
	return func(ctx context.Context, msg eventbus.Message) error {
		attrs := attrsFromBusEnvelope(msg) // defined in main.go (same package)
		switch topic {
		case subscribers.TopicFamiliarStageUp, subscribers.TopicCompanionStageUp:
			env, err := subscribers.DecodeStageUpWithAttrs(topic, msg.Payload, attrs)
			if err != nil {
				return err
			}
			return sub.HandleStageUp(tracing.WithTenantID(ctx, env.TenantID), env)
		case subscribers.TopicFamiliarBreedRevealed, subscribers.TopicCompanionBreedRevealed:
			env, err := subscribers.DecodeBreedRevealedWithAttrs(topic, msg.Payload, attrs)
			if err != nil {
				return err
			}
			return sub.HandleBreedRevealed(tracing.WithTenantID(ctx, env.TenantID), env)
		case subscribers.TopicFamiliarHatched, subscribers.TopicCompanionHatched:
			env, err := subscribers.DecodeHatchedWithAttrs(topic, msg.Payload, attrs)
			if err != nil {
				return err
			}
			return sub.HandleHatched(tracing.WithTenantID(ctx, env.TenantID), env)
		case subscribers.TopicFamiliarSourceRevelation, subscribers.TopicCompanionSourceRevelation:
			env, err := subscribers.DecodeSourceRevelationWithAttrs(topic, msg.Payload, attrs)
			if err != nil {
				return err
			}
			return sub.HandleSourceRevelation(tracing.WithTenantID(ctx, env.TenantID), env)
		default:
			return fmt.Errorf("familiar-milestone handler: unknown topic %q", topic)
		}
	}
}
