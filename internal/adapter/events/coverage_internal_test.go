// coverage_internal_test.go — white-box coverage for the publisher helpers
// that are only reachable from inside the package (encodePayload / publish
// envelope-validation). External tests in coverage_extra_test.go cover the
// exported surface.
package events

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
)

// intBus is a package-internal OutboxCompatible double.
type intBus struct{}

func (intBus) Publish(_ context.Context, _ string, _ cgcenvelope.Envelope, _ []byte) error {
	return nil
}

func TestEncodePayload_UnsupportedTopic_FallsBackToJSON(t *testing.T) {
	payload := map[string]any{"note": "no encoder"}
	bz, err := encodePayload("chora.sharing.unknown.event.v9", validTestEnvelope(), payload)
	if err != nil {
		t.Fatalf("encodePayload: %v", err)
	}
	if !json.Valid(bz) {
		t.Fatalf("fallback payload not JSON: %q", bz)
	}
	// Second call exercises the once-only warning map guard.
	if _, err := encodePayload("chora.sharing.unknown.event.v9", validTestEnvelope(), payload); err != nil {
		t.Fatalf("encodePayload second call: %v", err)
	}
}

func TestEncodePayload_EncoderError_Propagates(t *testing.T) {
	// post.created has a string post_id slot — a wrong-typed value must be
	// rejected by the binary encoder (not silently JSON-fallen back).
	_, err := encodePayload(TopicPostCreated, validTestEnvelope(), map[string]any{"post_id": 123})
	if err == nil {
		t.Fatal("expected marshal error for wrong-typed post_id")
	}
	if strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("wrong-type error must NOT be reported as unsupported-topic; got %v", err)
	}
}

func TestPublish_EnvelopeValidation_Direct(t *testing.T) {
	p := NewPublisher(Config{Bus: intBus{}})
	err := p.publish(context.Background(), TopicPostCreated, cgcenvelope.Envelope{}, map[string]any{})
	if err == nil || !strings.Contains(err.Error(), "envelope validation") {
		t.Fatalf("expected envelope validation error, got %v", err)
	}
}

func TestPublish_MarshalError_Direct(t *testing.T) {
	// A well-formed envelope with a wrong-typed payload field makes the
	// binary encoder fail — the marshal error must wrap the topic.
	p := NewPublisher(Config{Bus: intBus{}, Now: func() time.Time { return time.Now().UTC() }})
	err := p.publish(context.Background(), TopicPostCreated, validTestEnvelope(), map[string]any{"post_id": 123})
	if err == nil || !strings.Contains(err.Error(), "marshal") || !strings.Contains(err.Error(), TopicPostCreated) {
		t.Fatalf("expected marshal error mentioning topic, got %v", err)
	}
}

func validTestEnvelope() cgcenvelope.Envelope {
	now := time.Now().UTC()
	return cgcenvelope.Envelope{
		EventID:        "01970000-0000-7000-8000-00000000evt1",
		IdempotencyKey: "01970000-0000-7000-8000-00000000evt1",
		TenantID:       "01970000-0000-7000-8000-000000000001",
		GCID:           "01970000-0000-7000-9000-000000000001",
		OccurredAt:     now,
		PublishedAt:    now,
		Traceparent:    "00-0af7651916cd43dd8448eb211c80319c-b9c7c989f97918e1-01",
		SourceProject:  "chora-489812",
		SourceService:  "chora-sharing",
		SchemaVersion:  1,
	}
}