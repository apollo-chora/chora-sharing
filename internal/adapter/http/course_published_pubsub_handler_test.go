package httpadapter_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apollo-chora/chora-sharing/internal/adapter/eventpush"
	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	deliveryv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/delivery/v1"

	httpapi "github.com/apollo-chora/chora-sharing/internal/adapter/http"
	"github.com/apollo-chora/chora-sharing/internal/domain/discovery"
)

// binaryPushBody wraps raw BINARY proto wire bytes in a Pub/Sub push envelope.
// The existing pushBody helper marshals JSON — useless here: chora-delivery
// publishes this topic as binary against a Schema Registry schema, and a JSON
// fixture would pass against a handler that cannot read the real wire.
func binaryPushBody(t *testing.T, topic string, data []byte) []byte {
	t.Helper()
	env := map[string]any{
		"message": map[string]any{
			"data":        base64.StdEncoding.EncodeToString(data),
			"messageId":   "m-course-pub-1",
			"publishTime": time.Now().UTC().Format(time.RFC3339),
			"attributes":  map[string]string{"topic": topic},
		},
		"subscription": "projects/chora-489812/subscriptions/chora-sharing.delivery-course-published",
	}
	body, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("binaryPushBody: %v", err)
	}
	return body
}

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }

func coursePublishedWire(t *testing.T) []byte {
	t.Helper()
	bz, err := proto.Marshal(&deliveryv1.CoursePublished{
		Envelope: &commonv1.EventEnvelope{
			EventId:  "01980000-0000-7000-8000-0000000000aa",
			TenantId: "tnt-1",
			Gcid:     "gcid-instructor-1",
		},
		CourseId:       "course-42",
		Title:          "Fractions in the Wild",
		InstructorGcid: "gcid-instructor-1",
		PublishedAt:    timestamppb.New(time.Date(2026, 7, 14, 9, 0, 0, 0, time.UTC)),
	})
	if err != nil {
		t.Fatalf("marshal CoursePublished: %v", err)
	}
	return bz
}

// The happy path: a real binary course.published.v1 push lands the course on
// the C+ public discovery feed and acks 200.
func TestCoursePublishedPushHandler_ProjectsOntoPublicFeed(t *testing.T) {
	reg := discovery.NewRegistry()
	h := httpapi.NewCoursePublishedPushHandler(httpapi.CoursePublishedPushDeps{
		Registry: reg,
		Verifier: eventpush.NewVerifier(eventpush.VerifierConfig{}),
	})

	req := httptest.NewRequest(http.MethodPost, "/api/internal/pubsub/course-published",
		bytesReader(binaryPushBody(t, discovery.TopicCoursePublished, coursePublishedWire(t))))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}

	pub := reg.PublicCourses()
	if len(pub) != 1 {
		t.Fatalf("PublicCourses() = %d, want 1 — a 200 ack with an empty feed is the silent "+
			"failure this lane is prone to", len(pub))
	}
	if pub[0].CourseID != "course-42" {
		t.Errorf("CourseID = %q, want course-42", pub[0].CourseID)
	}
	if pub[0].Title != "Fractions in the Wild" {
		t.Errorf("Title = %q", pub[0].Title)
	}
}

// A payload that cannot be decoded must return 5xx so Pub/Sub retries and
// eventually dead-letters. Acking a corrupt message would erase the evidence.
func TestCoursePublishedPushHandler_UndecodablePayload_Returns5xx(t *testing.T) {
	reg := discovery.NewRegistry()
	h := httpapi.NewCoursePublishedPushHandler(httpapi.CoursePublishedPushDeps{
		Registry: reg,
		Verifier: eventpush.NewVerifier(eventpush.VerifierConfig{}),
	})

	req := httptest.NewRequest(http.MethodPost, "/api/internal/pubsub/course-published",
		bytesReader(binaryPushBody(t, discovery.TopicCoursePublished, []byte{0xff, 0xfe, 0xfd})))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code < 500 {
		t.Fatalf("status = %d, want 5xx so Pub/Sub retries -> DLQ", rec.Code)
	}
	if n := len(reg.PublicCourses()); n != 0 {
		t.Errorf("PublicCourses() = %d, want 0", n)
	}
}

// An unrelated topic on the same endpoint must ACK (200), not spin. A fan-out
// misconfiguration should not become an infinite retry storm.
func TestCoursePublishedPushHandler_UnknownTopic_Acks(t *testing.T) {
	reg := discovery.NewRegistry()
	h := httpapi.NewCoursePublishedPushHandler(httpapi.CoursePublishedPushDeps{
		Registry: reg,
		Verifier: eventpush.NewVerifier(eventpush.VerifierConfig{}),
	})

	req := httptest.NewRequest(http.MethodPost, "/api/internal/pubsub/course-published",
		bytesReader(binaryPushBody(t, "chora.delivery.course.cancelled.v1", coursePublishedWire(t))))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (ack) on an unrelated topic", rec.Code)
	}
	if n := len(reg.PublicCourses()); n != 0 {
		t.Errorf("PublicCourses() = %d, want 0 — an unrelated topic must not project", n)
	}
}
