// Package events_test holds RED-phase TDD specs for the in-memory
// envelope-conforming Pub/Sub bus that chora-sharing emits to.
package events_test

import (
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/events"
)

const (
	tenantA = "01970000-0000-7000-8000-000000000001"
	gcidA   = "01970000-0000-7000-9000-000000000001"
)

// Envelope mandatory fields per CLAUDE.md §6:
//
//	event_id, idempotency_key, tenant_id, gcid, occurred_at,
//	published_at, traceparent, tracestate, source_project,
//	source_service, schema_version
func TestEnvelope_MandatoryFieldsPopulated(t *testing.T) {
	t.Parallel()
	env := events.NewEnvelope("00-deadbeefdeadbeefdeadbeefdeadbeef-deadbeefdeadbeef-01", "", tenantA, gcidA)
	if env.EventID == "" {
		t.Fatalf("event_id required")
	}
	if env.IdempotencyKey == "" {
		t.Fatalf("idempotency_key required")
	}
	if env.TenantID != tenantA {
		t.Fatalf("tenant_id mismatch")
	}
	if env.GCID != gcidA {
		t.Fatalf("gcid mismatch")
	}
	if env.OccurredAt.IsZero() {
		t.Fatalf("occurred_at required")
	}
	if env.PublishedAt.IsZero() {
		t.Fatalf("published_at required")
	}
	if env.Traceparent == "" {
		t.Fatalf("traceparent required")
	}
	if env.SourceProject == "" {
		t.Fatalf("source_project required")
	}
	if env.SourceService != "chora-sharing" {
		t.Fatalf("source_service must be chora-sharing, got %q", env.SourceService)
	}
	if env.SchemaVersion < 1 {
		t.Fatalf("schema_version must be ≥1, got %d", env.SchemaVersion)
	}
}

// IdempotencyKey defaults to event_id when caller passes empty string.
func TestEnvelope_IdempotencyKeyDefaultsToEventID(t *testing.T) {
	t.Parallel()
	env := events.NewEnvelope("", "", tenantA, gcidA)
	if env.IdempotencyKey != env.EventID {
		t.Fatalf("idempotency_key should default to event_id when not specified")
	}
}

// Bus.Publish stores a copy + Bus.Drain returns ordered drained events.
func TestBus_PublishAndDrain(t *testing.T) {
	t.Parallel()
	bus := events.NewBus()
	bus.Publish("chora.sharing.follow.created.v1", events.NewEnvelope("", "", tenantA, gcidA), map[string]interface{}{
		"follower_gcid": gcidA,
		"followee_gcid": "01970000-0000-7000-9000-000000000002",
	})
	bus.Publish("chora.sharing.post.created.v1", events.NewEnvelope("", "", tenantA, gcidA), map[string]interface{}{
		"post_id": "p-1",
	})
	out := bus.Drain()
	if len(out) != 2 {
		t.Fatalf("expected 2 events drained, got %d", len(out))
	}
	if out[0].Topic != "chora.sharing.follow.created.v1" {
		t.Fatalf("unexpected topic order: %q", out[0].Topic)
	}
	// Drain a second time → empty.
	if len(bus.Drain()) != 0 {
		t.Fatalf("second Drain must return empty")
	}
}

func TestBus_TopicTaxonomyAccepted(t *testing.T) {
	t.Parallel()
	bus := events.NewBus()
	cases := []string{
		"chora.sharing.follow.created.v1",
		"chora.sharing.follow.removed.v1",
		"chora.sharing.post.created.v1",
		"chora.sharing.reaction.added.v1",
		"chora.sharing.duel.completed.v1",
		"chora.sharing.leaderboard.snapshot.v1",
	}
	for _, topic := range cases {
		topic := topic
		t.Run(topic, func(t *testing.T) {
			t.Parallel()
			env := events.NewEnvelope("", "", tenantA, gcidA)
			bus.Publish(topic, env, map[string]interface{}{"x": 1})
		})
	}
}

// Topic must match the chora.{domain}.{aggregate}.{event_type}.v{N} taxonomy.
func TestBus_RejectsMalformedTopic(t *testing.T) {
	t.Parallel()
	bus := events.NewBus()
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic on malformed topic")
		}
	}()
	bus.Publish("notatopic", events.NewEnvelope("", "", tenantA, gcidA), nil)
}

// Subscribe returns a channel on which published events are delivered.
func TestBus_Subscribe(t *testing.T) {
	t.Parallel()
	bus := events.NewBus()
	ch := bus.Subscribe()
	bus.Publish("chora.sharing.post.created.v1", events.NewEnvelope("", "", tenantA, gcidA), nil)
	select {
	case e := <-ch:
		if e.Topic != "chora.sharing.post.created.v1" {
			t.Fatalf("unexpected topic %q", e.Topic)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("Subscribe did not deliver event")
	}
}
