// atom_projection_subscriber_wiring.go — Atom Sharing Redesign (CHO-1972) +
// ADR-229 Amendment A1 orphan saga (CHO-2132).
//
// Wires the AtomProjectionSubscriber (T011) AND the AtomReuseStranding
// subscriber onto the NATS JetStream event bus. The projection subscriber
// keeps the cached atom_projections read-model fresh from Content Creation
// events so the ShareAtom R1 (author-of-record) gate never does a cross-DB
// read of chora_creation:
//
//	chora.creation.atom.published.v1                → Upsert (cache/refresh)
//	chora.creation.atom.archived.v1                 → Invalidate (archive flag)
//	chora.creation.atom.reuse_visibility_changed.v1 → SetReuseVisibility
//
// The ADR-229 A1 stranding detector COMPOSES onto the archived +
// reuse_visibility deliveries (running AFTER the projection handler, under
// its own idempotency claims — a crash between the two legs replays only the
// unfinished one), and a fourth consumer consumes the mint answer:
//
//	chora.creation.atom.orphan_created.v1 → store edition + repoint stranded
//	  grants (consumer chora-sharing-atom-reuse-orphan-created)
//
// CONSUMER-OWNED DURABLE CONSUMERS: the bus self-provisions a durable
// JetStream consumer per subject at Subscribe time, so a fresh/rebuilt stack
// self-heals WITHOUT any infrastructure change. Each subject gets its OWN
// durable name so two legs of the same subscriber cannot collide on a shared
// consumer.
//
// Hand-written decode (DecodeAtom*WithAttrs) is intentionally pinned to today's
// STABLE fields, so additive upstream contract evolution (e.g. CHO-1967
// "answerability" on atom.published.v1) is safely ignored.
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

// atomProjectionSubscriptions maps each consumed Creation topic to its
// consumer-owned durable consumer name (chora-{service}-{purpose} convention).
var atomProjectionSubscriptions = map[string]string{
	subscribers.TopicAtomPublished: "chora-sharing-atom-projection-published",
	subscribers.TopicAtomArchived:  "chora-sharing-atom-projection-archived",
	// ADR-229 WS-1 (CHO-2127) — author reuse-consent audience changes map
	// onto atom_projections.reuse_visibility.
	subscribers.TopicAtomReuseVisibilityChanged: "chora-sharing-atom-projection-reuse-visibility",
}

// atomReuseOrphanCreatedSubscription is the ADR-229 A1 (CHO-2132) repoint
// leg's consumer-owned durable consumer name.
const atomReuseOrphanCreatedSubscription = "chora-sharing-atom-reuse-orphan-created"

// registerAtomProjectionSubscriber spawns one durable-consumer goroutine per
// consumed topic. `bus` may be nil in dev (NATS_URL unset) — the function
// no-ops + returns nil (the in-memory fallback has no cross-service surface).
// `sub` MUST be non-nil. `stranding` MAY be nil (dev without a DB pool) — the
// ADR-229 A1 legs are then skipped LOUDLY (grants cannot strand without pg
// anyway).
func registerAtomProjectionSubscriber(
	ctx context.Context,
	bus eventbus.Bus,
	sub *subscribers.AtomProjectionSubscriber,
	stranding *subscribers.AtomReuseStrandingSubscriber,
) error {
	if sub == nil {
		return errors.New("registerAtomProjectionSubscriber: nil AtomProjectionSubscriber")
	}
	if bus == nil {
		log.Printf("sharing: atom-projection subscriber SKIPPED (NATS_URL unset; in-memory fallback has no cross-service surface)")
		return nil
	}
	if stranding == nil {
		log.Printf("sharing: ADR-229 A1 stranding detector NOT wired (nil subscriber — pg repos unavailable); projection handlers run alone")
	}

	for topic, subscription := range atomProjectionSubscriptions {
		handler := buildAtomProjectionHandler(sub, stranding, topic)
		go func(topic, subscription string) {
			log.Printf("sharing: atom-projection subscriber started (topic=%s consumer=%s)", topic, subscription)
			if err := bus.Subscribe(ctx, consumerConfig(subscription, topic), handler); err != nil &&
				!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				log.Printf("sharing: atom-projection subscriber exited (topic=%s): %v", topic, err)
			}
		}(topic, subscription)
	}
	log.Printf("sharing: AtomProjectionSubscriber bound to %d topics (CHO-1972)", len(atomProjectionSubscriptions))

	// ADR-229 A1 repoint leg — its own consumer on the mint answer.
	if stranding != nil {
		handler := buildOrphanCreatedHandler(stranding)
		go func() {
			log.Printf("sharing: atom-reuse orphan-created subscriber started (topic=%s consumer=%s, ADR-229 A1)",
				subscribers.TopicAtomOrphanCreated, atomReuseOrphanCreatedSubscription)
			if err := bus.Subscribe(ctx, consumerConfig(atomReuseOrphanCreatedSubscription, subscribers.TopicAtomOrphanCreated), handler); err != nil &&
				!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				log.Printf("sharing: atom-reuse orphan-created subscriber exited: %v", err)
			}
		}()
	}
	return nil
}

// buildAtomProjectionHandler decodes one message (hand-written, attrs-aware
// decode pinned to today's fields) and forwards to the typed Handle* method,
// propagating tenant_id from the event envelope onto ctx so the pg
// AtomProjectionStore's rls.ApplySession can SET LOCAL chora.tenant_id.
//
// The ADR-229 A1 stranding detector runs AFTER the projection handler on the
// archived + reuse_visibility deliveries. Each leg holds its own idempotency
// claim: if the projection leg succeeded and the stranding leg failed, the
// NACK replays the message and only the stranding leg re-runs. Decode or
// handler errors Nack → broker retry (→ DLQ).
func buildAtomProjectionHandler(
	sub *subscribers.AtomProjectionSubscriber,
	stranding *subscribers.AtomReuseStrandingSubscriber,
	topic string,
) eventbus.Handler {
	return func(ctx context.Context, msg eventbus.Message) error {
		attrs := attrsFromBusEnvelope(msg) // defined in main.go (same package)
		switch topic {
		case subscribers.TopicAtomPublished:
			env, err := subscribers.DecodeAtomPublishedWithAttrs(msg.Payload, attrs)
			if err != nil {
				return err
			}
			return sub.HandleAtomPublished(tracing.WithTenantID(ctx, env.TenantID), env)
		case subscribers.TopicAtomArchived:
			env, err := subscribers.DecodeAtomArchivedWithAttrs(msg.Payload, attrs)
			if err != nil {
				return err
			}
			tctx := tracing.WithTenantID(ctx, env.TenantID)
			if err := sub.HandleAtomArchived(tctx, env); err != nil {
				return err
			}
			if stranding != nil {
				return stranding.HandleAtomArchivedStranding(tctx, env)
			}
			return nil
		case subscribers.TopicAtomReuseVisibilityChanged:
			env, err := subscribers.DecodeAtomReuseVisibilityChangedWithAttrs(msg.Payload, attrs)
			if err != nil {
				return err
			}
			tctx := tracing.WithTenantID(ctx, env.TenantID)
			if err := sub.HandleAtomReuseVisibilityChanged(tctx, env); err != nil {
				return err
			}
			if stranding != nil {
				return stranding.HandleReuseVisibilityChanged(tctx, env)
			}
			return nil
		default:
			return fmt.Errorf("atom-projection handler: unknown topic %q", topic)
		}
	}
}

// buildOrphanCreatedHandler decodes + dispatches the ADR-229 A1 repoint leg.
func buildOrphanCreatedHandler(stranding *subscribers.AtomReuseStrandingSubscriber) eventbus.Handler {
	return func(ctx context.Context, msg eventbus.Message) error {
		attrs := attrsFromBusEnvelope(msg)
		env, err := subscribers.DecodeAtomOrphanCreatedWithAttrs(msg.Payload, attrs)
		if err != nil {
			return err
		}
		return stranding.HandleAtomOrphanCreated(tracing.WithTenantID(ctx, env.TenantID), env)
	}
}
