// Package events_test holds RED-phase TDD specs for the chora-sharing
// Pub/Sub publisher adapter (S4.4 — A-Content-Sharing).
//
// The adapter wraps eventbus.Publisher (from chora-go-common) so
// production wiring uses Cloud Pub/Sub while unit tests inject the
// in-memory bus.
//
// Topics emitted by this adapter:
//   - chora.sharing.post.created.v1       (D2 transparency for public posts)
//   - chora.sharing.reaction.added.v1     (D1 accountability)
//   - chora.sharing.reaction.removed.v1   (D1 accountability)
//
// The follow.{created,removed}.v1 publish methods are RETIRED (ADR-230 D4);
// relationship.*.v1 events are enqueued transactionally by the pg
// RelationshipRepo, not published here.
package events_test

import (
	"context"
	"sync"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"
	sharingv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/sharing/v1"

	"github.com/apollo-chora/chora-sharing/internal/adapter/events"
)

const (
	tenantA = "01970000-0000-7000-8000-000000000001"
	gcidA   = "01970000-0000-7000-9000-000000000001"
	gcidB   = "01970000-0000-7000-9000-000000000002"
)

// fakeBus is an OutboxCompatible recorder used to assert publisher behaviour
// without spinning up the full in-memory bus.
type fakeBus struct {
	mu       sync.Mutex
	calls    []fakeCall
	failOnce error
}

type fakeCall struct {
	topic   string
	env     cgcenvelope.Envelope
	payload []byte
}

func (b *fakeBus) Publish(_ context.Context, topic string, env cgcenvelope.Envelope, payload []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failOnce != nil {
		err := b.failOnce
		b.failOnce = nil
		return err
	}
	b.calls = append(b.calls, fakeCall{topic: topic, env: env, payload: payload})
	return nil
}

func (b *fakeBus) recorded() []fakeCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]fakeCall, len(b.calls))
	copy(out, b.calls)
	return out
}

var _ eventbus.Publisher = (*fakeBus)(nil)

// -----------------------------------------------------------------------------
// Envelope + IMDA discipline (exercised via ReactionAdded — the follow.*
// publish methods are RETIRED per ADR-230 D4; relationship events flow
// through the pg RelationshipRepo's transactional outbox enqueue instead).
// -----------------------------------------------------------------------------

func TestPublishReactionAdded_EnvelopeMandatoryFields(t *testing.T) {
	bus := &fakeBus{}
	pub := events.NewPublisher(events.Config{Bus: bus})
	if err := pub.PublishReactionAdded(context.Background(), events.ReactionEvent{
		ReactionID: "reaction-1",
		TenantID:   tenantA,
		GCID:       gcidA,
		TargetType: "post",
		TargetID:   "post-1",
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	got := bus.recorded()[0]
	if err := cgcenvelope.Validate(got.env); err != nil {
		t.Fatalf("envelope validation failed: %v", err)
	}
	if got.env.TenantID != tenantA {
		t.Fatalf("tenant_id mismatch: %q", got.env.TenantID)
	}
	if got.env.SourceService != "chora-sharing" {
		t.Fatalf("source_service must be chora-sharing, got %q", got.env.SourceService)
	}
	if got.env.SchemaVersion != 1 {
		t.Fatalf("schema_version must be 1, got %d", got.env.SchemaVersion)
	}
}

// IMDA evidence per ADR-141: reaction events tag accountability (D1).
func TestPublishReactionAdded_IMDADimensionAccountability(t *testing.T) {
	bus := &fakeBus{}
	pub := events.NewPublisher(events.Config{Bus: bus})
	_ = pub.PublishReactionAdded(context.Background(), events.ReactionEvent{
		ReactionID: "reaction-1",
		TenantID:   tenantA,
		GCID:       gcidA,
		TargetType: "post",
		TargetID:   "post-1",
	})
	got := bus.recorded()[0]
	if got.env.ChoraImdaDimension != "accountability" {
		t.Fatalf("expected D1 accountability, got %q", got.env.ChoraImdaDimension)
	}
	if got.env.ImdaLifecycleStage != "runtime" {
		t.Fatalf("expected runtime lifecycle, got %q", got.env.ImdaLifecycleStage)
	}
}

// -----------------------------------------------------------------------------
// PublishPostCreated
// -----------------------------------------------------------------------------

func TestPublishPostCreated_EmitsCanonicalTopic(t *testing.T) {
	bus := &fakeBus{}
	pub := events.NewPublisher(events.Config{Bus: bus})
	if err := pub.PublishPostCreated(context.Background(), events.PostEvent{
		PostID:     "post-1",
		TenantID:   tenantA,
		AuthorGCID: gcidA,
		Visibility: "public",
		Body:       "hello chora",
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	calls := bus.recorded()
	if len(calls) != 1 || calls[0].topic != "chora.sharing.post.created.v1" {
		t.Fatalf("expected post.created.v1, got %#v", calls)
	}
}

// Public posts emit D2 transparency (audit trail of public-facing content).
func TestPublishPostCreated_PublicVisibilityTagsTransparency(t *testing.T) {
	bus := &fakeBus{}
	pub := events.NewPublisher(events.Config{Bus: bus})
	_ = pub.PublishPostCreated(context.Background(), events.PostEvent{
		PostID:     "p-1",
		TenantID:   tenantA,
		AuthorGCID: gcidA,
		Visibility: "public",
	})
	got := bus.recorded()[0]
	if got.env.ChoraImdaDimension != "transparency" {
		t.Fatalf("expected D2 transparency for public post, got %q", got.env.ChoraImdaDimension)
	}
}

// Tenant- or private-scoped posts are not platform-public; tag D1 accountability.
func TestPublishPostCreated_NonPublicTagsAccountability(t *testing.T) {
	bus := &fakeBus{}
	pub := events.NewPublisher(events.Config{Bus: bus})
	_ = pub.PublishPostCreated(context.Background(), events.PostEvent{
		PostID:     "p-1",
		TenantID:   tenantA,
		AuthorGCID: gcidA,
		Visibility: "tenant",
	})
	got := bus.recorded()[0]
	if got.env.ChoraImdaDimension != "accountability" {
		t.Fatalf("expected D1 accountability for non-public post, got %q", got.env.ChoraImdaDimension)
	}
}

// -----------------------------------------------------------------------------
// PublishReactionAdded / Removed
// -----------------------------------------------------------------------------

func TestPublishReactionAdded_EmitsCanonicalTopic(t *testing.T) {
	bus := &fakeBus{}
	pub := events.NewPublisher(events.Config{Bus: bus})
	if err := pub.PublishReactionAdded(context.Background(), events.ReactionEvent{
		ReactionID:   "rx-1",
		TenantID:     tenantA,
		GCID:         gcidA,
		TargetType:   "post",
		TargetID:     "p-1",
		ReactionType: "like",
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	calls := bus.recorded()
	if len(calls) != 1 || calls[0].topic != "chora.sharing.reaction.added.v1" {
		t.Fatalf("expected reaction.added.v1, got %#v", calls)
	}
}

func TestPublishReactionRemoved_EmitsCanonicalTopic(t *testing.T) {
	bus := &fakeBus{}
	pub := events.NewPublisher(events.Config{Bus: bus})
	if err := pub.PublishReactionRemoved(context.Background(), events.ReactionEvent{
		TenantID:     tenantA,
		GCID:         gcidA,
		TargetType:   "post",
		TargetID:     "p-1",
		ReactionType: "like",
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	calls := bus.recorded()
	if len(calls) != 1 || calls[0].topic != "chora.sharing.reaction.removed.v1" {
		t.Fatalf("expected reaction.removed.v1, got %#v", calls)
	}
}

// Reactions on atoms (target_type=atom) — same topic. The events-flat
// schema has only a single target_post_id slot (no atom/post discriminator);
// the atom-id flows into target_post_id and subscribers infer the target
// type via DB lookup. Asserting wire-level decode into the generated type.
func TestPublishReactionAdded_AtomTarget(t *testing.T) {
	bus := &fakeBus{}
	pub := events.NewPublisher(events.Config{Bus: bus})
	_ = pub.PublishReactionAdded(context.Background(), events.ReactionEvent{
		ReactionID:   "rx-2",
		TenantID:     tenantA,
		GCID:         gcidA,
		TargetType:   "atom",
		TargetID:     "atom-1",
		ReactionType: "insightful",
	})
	got := bus.recorded()[0]

	var msg sharingv1.ReactionAdded
	if err := proto.Unmarshal(got.payload, &msg); err != nil {
		t.Fatalf("payload not binary proto: %v", err)
	}
	if msg.GetTargetPostId() != "atom-1" {
		t.Fatalf("target_post_id (atom slot): got %q want atom-1", msg.GetTargetPostId())
	}
	if msg.GetReactionType() != sharingv1.ReactionType_REACTION_TYPE_INSIGHTFUL {
		t.Fatalf("reaction_type: got %v want INSIGHTFUL", msg.GetReactionType())
	}
	// Sanity: bytes start with envelope tag (field 1, length-delimited).
	num, typ, n := protowire.ConsumeTag(got.payload)
	if n < 0 || num != 1 || typ != protowire.BytesType {
		t.Fatalf("payload does not lead with envelope tag: num=%d typ=%d", num, typ)
	}
}

// -----------------------------------------------------------------------------
// Bus failure surfaces an error.
// -----------------------------------------------------------------------------

func TestPublish_BusErrorPropagates(t *testing.T) {
	bus := &fakeBus{}
	bus.failOnce = errBus
	pub := events.NewPublisher(events.Config{Bus: bus})
	err := pub.PublishReactionAdded(context.Background(), events.ReactionEvent{
		ReactionID: "reaction-1",
		TenantID:   tenantA,
		GCID:       gcidA,
		TargetType: "post",
		TargetID:   "post-1",
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

// -----------------------------------------------------------------------------
// End-to-end: in-memory bus + subscriber receives the publish.
// -----------------------------------------------------------------------------

func TestEndToEnd_InMemoryBusFlows(t *testing.T) {
	bus := eventbus.NewInMemoryBus(eventbus.WithSynchronousDelivery())
	defer bus.Close()
	pub := events.NewPublisher(events.Config{Bus: bus})

	var got []eventbus.Message
	var mu sync.Mutex
	cancel, err := bus.Subscribe(context.Background(), "chora.sharing.reaction.added.v1", func(_ context.Context, msg eventbus.Message) error {
		mu.Lock()
		got = append(got, msg)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()

	if err := pub.PublishReactionAdded(context.Background(), events.ReactionEvent{
		ReactionID: "reaction-1",
		TenantID:   tenantA,
		GCID:       gcidA,
		TargetType: "post",
		TargetID:   "post-1",
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("subscriber did not receive event, got=%d", len(got))
	}
	if got[0].Envelope.TenantID != tenantA {
		t.Fatalf("envelope tenant mismatch: %q", got[0].Envelope.TenantID)
	}
}

// errBus is a sentinel for fakeBus failure injection.
type busError string

func (e busError) Error() string { return string(e) }

var errBus = busError("simulated bus failure")
