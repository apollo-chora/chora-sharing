// coverage_internal_test.go — white-box coverage for the outbox_bus and
// publisher helpers that are only reachable from inside the package
// (validateTopic / deriveAggregate / deriveEventType / encodePayload /
// publish envelope-validation). External tests in coverage_extra_test.go
// cover the exported surface.
package events

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	cgcoutbox "github.com/apollo-chora/chora-common/outbox"
)

func TestValidateTopic_Branches(t *testing.T) {
	cases := []struct {
		name  string
		topic string
		want  string // substring of the error; "" means no error
	}{
		{"valid sharing", "chora.sharing.post.created.v1", ""},
		{"valid governance", "chora.governance.behavior.reported.v1", ""},
		{"valid closure", "chora.closure.pii.pseudonymised.v1", ""},
		{"empty", "  ", "topic required"},
		{"too few segments", "chora.sharing.post", "must follow"},
		{"wrong root", "com.sharing.post.created.v1", "must start with 'chora.'"},
		{"wrong domain", "chora.cms.post.created.v1", "domain must be"},
		{"no version suffix", "chora.sharing.post.created", "must follow"},
		{"bare v suffix", "chora.sharing.post.created.x", "must end with v{N}"},
		{"non-numeric version", "chora.sharing.post.created.v1x", "version suffix must be numeric"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateTopic(tc.topic)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("validateTopic(%q) = %v, want nil", tc.topic, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validateTopic(%q) = %v, want error containing %q", tc.topic, err, tc.want)
			}
		})
	}
}

func TestDeriveAggregate_Branches(t *testing.T) {
	env := cgcenvelope.Envelope{
		EventID: "evt-1",
		GCID:    "",
	}
	if typ, id := deriveAggregate("chora.governance.behavior.reported.v1", env); typ != "behavior" || id != "evt-1" {
		t.Fatalf("governance/short: got (%q, %q), want (behavior, evt-1)", typ, id)
	}
	if typ, id := deriveAggregate("ab.cd", env); typ != "post" || id != "evt-1" {
		t.Fatalf("short topic: got (%q, %q), want (post, evt-1)", typ, id)
	}
	env.GCID = "gcid-9"
	if _, id := deriveAggregate("chora.sharing.post.created.v1", env); id != "gcid-9" {
		t.Fatalf("gcid present: aggregate id = %q, want gcid-9", id)
	}
}

func TestDeriveEventType_Branches(t *testing.T) {
	// SplitN(topic, ".", 3) → parts[2] carries the whole trailing suffix.
	if got := deriveEventType("chora.sharing.post.created.v1"); got != "post.created.v1" {
		t.Fatalf("deriveEventType = %q, want post.created.v1", got)
	}
	if got := deriveEventType("short"); got != "short" {
		t.Fatalf("deriveEventType(short) = %q, want short", got)
	}
}

func TestNewRowID_NonEmpty(t *testing.T) {
	if got := newRowID(); got == "" {
		t.Fatal("newRowID returned empty string")
	}
}

// intRecorder is a package-internal recorder double (the external test
// package has its own stubRecorder, which is invisible here).
type intRecorder struct {
	rows  []*cgcoutbox.Row
	failR bool
}

func (r *intRecorder) Record(_ context.Context, _ cgcoutbox.Tx, row *cgcoutbox.Row) error {
	if r.failR {
		return context.DeadlineExceeded
	}
	r.rows = append(r.rows, row)
	return nil
}
func (r *intRecorder) Claim(_ context.Context, _ int) ([]*cgcoutbox.Row, error) { return nil, nil }
func (r *intRecorder) MarkPublished(_ context.Context, _ []string) error        { return nil }
func (r *intRecorder) MarkFailed(_ context.Context, _ string, _ string, _ bool) error {
	return nil
}

// intBus is a package-internal OutboxCompatible double.
type intBus struct{}

func (intBus) Publish(_ context.Context, _ string, _ cgcenvelope.Envelope, _ []byte) error {
	return nil
}

func TestOutboxBus_Publish_EnvelopeInvalid(t *testing.T) {
	bus := NewOutboxBus(&intRecorder{})
	// Empty envelope fails cgcenvelope.Validate (event_id required).
	if err := bus.Publish(context.Background(), TopicPostCreated, cgcenvelope.Envelope{}, []byte(`{}`)); err == nil {
		t.Fatal("expected envelope validation error")
	}
}

func TestOutboxBus_Publish_RecordError(t *testing.T) {
	bus := NewOutboxBus(&intRecorder{failR: true})
	if err := bus.Publish(context.Background(), TopicPostCreated, validTestEnvelope(), []byte(`{}`)); err == nil {
		t.Fatal("expected record error to propagate")
	}
}

func TestOutboxBus_Publish_GovernanceAggregateFallback(t *testing.T) {
	rec := &intRecorder{}
	bus := NewOutboxBus(rec)
	env := cgcenvelope.Envelope{
		EventID:        "evt-fallback",
		IdempotencyKey: "ik-fallback",
		TenantID:       "tenant-1",
		OccurredAt:     time.Now().UTC(),
		PublishedAt:    time.Now().UTC(),
		Traceparent:    "00-0af7651916cd43dd8448eb211c80319c-b9c7c989f97918e1-01",
		SourceProject:  "chora-489812",
		SourceService:  "chora-sharing",
		SchemaVersion:  1,
	}
	if err := bus.Publish(context.Background(), "chora.governance.behavior.reported.v1", env, []byte(`{}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if len(rec.rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rec.rows))
	}
	row := rec.rows[0]
	if row.AggregateType != "behavior" {
		t.Errorf("aggregate_type = %q, want behavior", row.AggregateType)
	}
	if row.AggregateID != "evt-fallback" {
		t.Errorf("aggregate_id = %q, want evt-fallback (GCID empty)", row.AggregateID)
	}
	if row.EventType != "behavior.reported.v1" {
		t.Errorf("event_type = %q, want behavior.reported.v1", row.EventType)
	}
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