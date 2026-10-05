// Package outbox_test — Publisher adapter tests.
//
// Publisher satisfies the events.Bus interface used by
// `internal/adapter/events.Publisher` by writing the event row to the
// sharing_outbox_events table (via the Store port) instead of publishing
// directly to Pub/Sub. A separate Dispatcher drains the outbox to Cloud
// Pub/Sub. This decouples emission from Pub/Sub availability: a crash
// between domain state-write and Bus.Publish no longer loses events
// because the row is durably committed to chora_sharing before the HTTP
// request returns.
//
// Per `feedback_d6_resilience_first_class` B.6.2.a — producer-side durable
// emission for chora-sharing's chora.sharing.* streams. Composes with the
// LangGraph PostgresSaver pattern used by the closure saga + AI Kernel
// orchestrator.
//
// The Publisher PRESERVES the topic-validation logic that previously lived
// in the alt-pattern OutboxBus via the new exported ValidateTopicName
// function (used both at construction and at every Publish call).
package outbox_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"

	"github.com/apollo-chora/chora-sharing/internal/adapter/outbox"
)

const (
	pubTenantA  = "01970000-0000-7000-8000-000000000001"
	pubGCIDA    = "01970000-0000-7000-9000-000000000001"
	pubTracePar = "00-0af7651916cd43dd8448eb211c80319c-b9c7c989f97918e1-01"
)

func newPublishEnvelope(now time.Time) cgcenvelope.Envelope {
	return cgcenvelope.Envelope{
		EventID:        "01970000-0000-7000-8000-0000000000a1",
		IdempotencyKey: "01970000-0000-7000-8000-0000000000a1",
		TenantID:       pubTenantA,
		GCID:           pubGCIDA,
		OccurredAt:     now,
		PublishedAt:    now,
		Traceparent:    pubTracePar,
		SourceProject:  "chora-489812",
		SourceService:  "chora-sharing",
		SchemaVersion:  1,
	}
}

func TestPublisher_Publish_WritesRowToStore(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{
		Store:         store,
		SourceProject: "chora-489812",
		SourceService: "chora-sharing",
	})

	now := time.Now().UTC()
	env := newPublishEnvelope(now)
	payload := []byte(`{"edge_id":"edge-1"}`)
	if err := pub.Publish(context.Background(), "chora.sharing.follow.created.v1", env, payload); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 1 {
		t.Fatalf("store rows = %d; want 1", len(rows))
	}
	row := rows[0]
	if row.Topic != "chora.sharing.follow.created.v1" {
		t.Errorf("row.Topic = %q; want chora.sharing.follow.created.v1", row.Topic)
	}
	if row.TenantID != env.TenantID {
		t.Errorf("row.TenantID = %q; want %q", row.TenantID, env.TenantID)
	}
	if row.GCID != env.GCID {
		t.Errorf("row.GCID = %q; want %q", row.GCID, env.GCID)
	}
	if row.IdempotencyKey != env.IdempotencyKey {
		t.Errorf("row.IdempotencyKey = %q; want %q", row.IdempotencyKey, env.IdempotencyKey)
	}
	if string(row.Payload) != string(payload) {
		t.Errorf("row.Payload = %s; want %s", string(row.Payload), string(payload))
	}
	if row.EventType == "" {
		t.Errorf("row.EventType empty (should be derived from topic)")
	}
}

func TestPublisher_Publish_StampsEnvelopeFields(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	now := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)
	pub := outbox.NewPublisher(outbox.PublisherConfig{
		Store:         store,
		SourceProject: "chora-489812",
		SourceService: "chora-sharing",
	})
	env := newPublishEnvelope(now)
	env.ChoraImdaDimension = "accountability"
	env.ImdaLifecycleStage = "runtime"

	if err := pub.Publish(context.Background(), "chora.sharing.follow.created.v1", env, []byte(`{}`)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 1)
	if len(rows) != 1 {
		t.Fatalf("rows = %d; want 1", len(rows))
	}
	got := rows[0].Envelope

	for _, key := range []string{
		"event_id", "idempotency_key", "tenant_id",
		"occurred_at", "published_at",
		"traceparent", "source_project", "source_service", "schema_version",
	} {
		if got[key] == "" {
			t.Errorf("envelope.%s empty; want non-empty (mandatory per CLAUDE.md §6)", key)
		}
	}
	if got["source_project"] != "chora-489812" {
		t.Errorf("envelope.source_project = %q; want chora-489812", got["source_project"])
	}
	if got["source_service"] != "chora-sharing" {
		t.Errorf("envelope.source_service = %q; want chora-sharing", got["source_service"])
	}
	if got["schema_version"] != "1" {
		t.Errorf("envelope.schema_version = %q; want 1", got["schema_version"])
	}
	if got["chora_imda_dimension"] != "accountability" {
		t.Errorf("envelope.chora_imda_dimension = %q; want accountability", got["chora_imda_dimension"])
	}
	if got["imda_lifecycle_stage"] != "runtime" {
		t.Errorf("envelope.imda_lifecycle_stage = %q; want runtime", got["imda_lifecycle_stage"])
	}
	if got["tenant_id"] != env.TenantID {
		t.Errorf("envelope.tenant_id = %q; want %q", got["tenant_id"], env.TenantID)
	}
}

func TestPublisher_Publish_RejectsBadTopic(t *testing.T) {
	t.Parallel()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: outbox.NewInMemoryStore()})
	env := newPublishEnvelope(time.Now().UTC())
	err := pub.Publish(context.Background(), "not.a.canonical.topic", env, []byte(`{}`))
	if err == nil {
		t.Errorf("Publish(bad topic) err = nil; want validation error")
	}
}

func TestPublisher_Publish_RejectsEmptyTopic(t *testing.T) {
	t.Parallel()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: outbox.NewInMemoryStore()})
	env := newPublishEnvelope(time.Now().UTC())
	err := pub.Publish(context.Background(), "", env, []byte(`{}`))
	if err == nil {
		t.Errorf("Publish(empty topic) err = nil; want validation error")
	}
}

func TestPublisher_Publish_RejectsMissingStore(t *testing.T) {
	t.Parallel()
	pub := outbox.NewPublisher(outbox.PublisherConfig{})
	env := newPublishEnvelope(time.Now().UTC())
	err := pub.Publish(context.Background(), "chora.sharing.follow.created.v1", env, []byte(`{}`))
	if err == nil {
		t.Errorf("Publish without store = nil err; want error")
	}
}

func TestPublisher_Publish_RejectsInvalidEnvelope(t *testing.T) {
	t.Parallel()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: outbox.NewInMemoryStore()})
	env := cgcenvelope.Envelope{} // empty -> mandatory fields missing
	err := pub.Publish(context.Background(), "chora.sharing.follow.created.v1", env, []byte(`{}`))
	if err == nil {
		t.Errorf("Publish(empty envelope) err = nil; want validation error")
	}
}

func TestPublisher_Publish_PreservesIdempotencyOnDuplicateInsert(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store})
	now := time.Now().UTC()
	env := newPublishEnvelope(now)
	if err := pub.Publish(context.Background(), "chora.sharing.follow.created.v1", env, []byte(`{}`)); err != nil {
		t.Fatalf("Publish 1: %v", err)
	}
	err := pub.Publish(context.Background(), "chora.sharing.follow.created.v1", env, []byte(`{}`))
	if err == nil {
		t.Errorf("expected duplicate idempotency_key rejection on second Publish")
	}
	if !errors.Is(err, outbox.ErrDuplicateIdempotencyKey) {
		t.Errorf("err = %v; want ErrDuplicateIdempotencyKey", err)
	}
}

// -----------------------------------------------------------------------------
// ValidateTopicName — ported from the legacy OutboxBus.validateTopic.
// -----------------------------------------------------------------------------

func TestValidateTopicName_AcceptsCanonical(t *testing.T) {
	t.Parallel()
	good := []string{
		"chora.sharing.follow.created.v1",
		"chora.sharing.post.created.v1",
		"chora.sharing.reaction.added.v1",
		"chora.governance.policy.violation_detected.v1",
		"chora.closure.pii.pseudonymise.requested.v1",
	}
	for _, topic := range good {
		if err := outbox.ValidateTopicName(topic); err != nil {
			t.Errorf("ValidateTopicName(%q) = %v; want nil", topic, err)
		}
	}
}

func TestValidateTopicName_RejectsInvalid(t *testing.T) {
	t.Parallel()
	bad := []struct {
		topic string
		want  string
	}{
		{"", "topic required"},
		{"   ", "topic required"},
		{"chora.sharing.follow", "5 segments"},
		{"chora.sharing.follow.created", "5 segments"},
		{"shorea.sharing.follow.created.v1", "chora."},
		{"chora.unknown.follow.created.v1", "domain"},
		{"chora.sharing.follow.created.x1", "version"},
		{"chora.sharing.follow.created.v", "version"},
		{"chora.sharing.follow.created.vA", "numeric"},
	}
	for _, tc := range bad {
		err := outbox.ValidateTopicName(tc.topic)
		if err == nil {
			t.Errorf("ValidateTopicName(%q) = nil; want error", tc.topic)
		}
	}
}

func TestValidateTopicName_AllowsSharingGovernanceClosure(t *testing.T) {
	t.Parallel()
	allowed := []string{
		"chora.sharing.foo.bar.v1",
		"chora.governance.foo.bar.v2",
		"chora.closure.foo.bar.v9",
	}
	for _, topic := range allowed {
		if err := outbox.ValidateTopicName(topic); err != nil {
			t.Errorf("ValidateTopicName(%q) = %v; want nil for allowlist domain", topic, err)
		}
	}
}

// -----------------------------------------------------------------------------
// Compile-time check: the Publisher exposes the same shape as the legacy
// OutboxBus + as the eventbus.OutboxCompatible interface used by the
// existing events.Publisher.
// -----------------------------------------------------------------------------

func TestPublisher_SatisfiesBusContract(t *testing.T) {
	t.Parallel()
	// Compile-time assertion: outbox.Publisher implements the same
	// Publish(ctx, topic, env, payload) error signature as events.Bus.
	var _ interface {
		Publish(ctx context.Context, topic string, env cgcenvelope.Envelope, payload []byte) error
	} = (*outbox.Publisher)(nil)
}

// Guard against accidental misconfiguration — Publisher should mint the
// EventType from the topic when EventType is unset on the envelope.
func TestPublisher_Publish_DerivesEventTypeFromTopic(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store})
	env := newPublishEnvelope(time.Now().UTC())
	if err := pub.Publish(context.Background(), "chora.sharing.post.created.v1", env, []byte(`{"x":1}`)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 1)
	if !strings.HasPrefix(rows[0].EventType, "sharing.") {
		t.Errorf("row.EventType = %q; want prefix sharing.", rows[0].EventType)
	}
}
