// Tests for the closure pull-loop adapter (CHO-1719 gap 4).
//
// ClosurePullHandler bridges the eventbus.CloudSubscriber pull loop to
// ClosureSubscriber.Handle:
//
//	chora.sharing.pii.pseudonymise.requested.v1 (pull subscription)
//	  → ClosurePullHandler (JSON decode + envelope trace fallback)
//	    → ClosureSubscriber.Handle
//	      → ack on chora.sharing.account.pseudonymised.v1
package events_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-sharing/internal/adapter/events"
)

// newPullFixture builds a fully-wired ClosureSubscriber on the same
// in-memory doubles the closure_subscriber tests use.
func newPullFixture(t *testing.T) (*events.ClosureSubscriber, *events.InMemoryClosurePublisher, *events.InMemoryClosureRepo) {
	t.Helper()
	pub := events.NewInMemoryClosurePublisher()
	repo := events.NewInMemoryClosureRepo()
	repo.SeedSubject(testTenantID, testGCID)
	return events.NewClosureSubscriber(repo, pub, newPIIMap("chora_sharing"), nil), pub, repo
}

func TestClosurePullHandler_DispatchesAndAcksOnNewTopic(t *testing.T) {
	t.Parallel()
	sub, pub, repo := newPullFixture(t)
	handler := events.ClosurePullHandler(sub)

	body, err := json.Marshal(map[string]string{
		"saga_id":     testSagaID,
		"gcid":        testGCID,
		"tenant_id":   testTenantID,
		"traceparent": testTrace,
		"tracestate":  testTracestate,
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	msg := eventbus.Message{
		Subject:    events.TopicPseudonymiseRequested,
		Envelope: envelope.Envelope{Traceparent: "00-x-y-01", TenantID: testTenantID},
		Payload:  body,
	}
	if err := handler(context.Background(), msg); err != nil {
		t.Fatalf("handler: %v", err)
	}

	// The ack MUST land on the taxonomy-correct NEW topic.
	emitted := pub.ClosureRecordedByTopic("chora.sharing.account.pseudonymised.v1")
	if len(emitted) != 1 {
		t.Fatalf("expected 1 ack on chora.sharing.account.pseudonymised.v1; got %d (all: %+v)", len(emitted), pub.ClosureRecorded())
	}
	if emitted[0].Payload["saga_id"] != testSagaID {
		t.Fatalf("saga_id: %v", emitted[0].Payload["saga_id"])
	}
	// Payload-carried traceparent wins over the envelope's.
	if emitted[0].Traceparent != testTrace {
		t.Fatalf("traceparent: %q want %q", emitted[0].Traceparent, testTrace)
	}
	pseudonymised, err := repo.IsPseudonymised(context.Background(), testTenantID, testGCID)
	if err != nil {
		t.Fatalf("IsPseudonymised: %v", err)
	}
	if !pseudonymised {
		t.Fatalf("subject not pseudonymised via pull handler")
	}
}

func TestClosurePullHandler_MalformedJSONReturnsError(t *testing.T) {
	t.Parallel()
	sub, pub, _ := newPullFixture(t)
	handler := events.ClosurePullHandler(sub)

	msg := eventbus.Message{
		Subject:    events.TopicPseudonymiseRequested,
		Envelope: envelope.Envelope{TenantID: testTenantID},
		Payload:  []byte("{not-json"),
	}
	err := handler(context.Background(), msg)
	if err == nil {
		t.Fatalf("expected decode error for malformed payload")
	}
	if !strings.Contains(err.Error(), "decode") {
		t.Fatalf("expected decode error; got %v", err)
	}
	if got := len(pub.ClosureRecorded()); got != 0 {
		t.Fatalf("no ack should publish on decode failure; got %d", got)
	}
}

func TestClosurePullHandler_TraceparentFallsBackToEnvelope(t *testing.T) {
	t.Parallel()
	sub, pub, _ := newPullFixture(t)
	handler := events.ClosurePullHandler(sub)

	// Payload omits traceparent/tracestate — the envelope's values apply.
	body, err := json.Marshal(map[string]string{
		"saga_id":   testSagaID,
		"gcid":      testGCID,
		"tenant_id": testTenantID,
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	msg := eventbus.Message{
		Subject:    events.TopicPseudonymiseRequested,
		Envelope: envelope.Envelope{Traceparent: "00-x-y-01", Tracestate: "vendor=env", TenantID: testTenantID},
		Payload:  body,
	}
	if err := handler(context.Background(), msg); err != nil {
		t.Fatalf("handler: %v", err)
	}
	emitted := pub.ClosureRecordedByTopic(events.TopicPseudonymiseCompleted)
	if len(emitted) != 1 {
		t.Fatalf("expected 1 completed event; got %d", len(emitted))
	}
	if emitted[0].Traceparent != "00-x-y-01" {
		t.Fatalf("traceparent fallback: %q want %q", emitted[0].Traceparent, "00-x-y-01")
	}
}

func TestClosurePullHandler_NilSubscriberGuard(t *testing.T) {
	t.Parallel()
	if err := events.ClosurePullHandler(nil)(context.Background(), eventbus.Message{Payload: []byte("{}")}); err == nil {
		t.Fatalf("nil subscriber should error")
	}
}
