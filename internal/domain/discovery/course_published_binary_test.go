// course_published_binary_test — CHO-2155 follow-up.
//
// End-to-end at the domain seam: the BINARY wire bytes chora-delivery actually
// publishes on chora.delivery.course.published.v1 must reach the C+ public
// discovery feed. This is the assertion that would have caught the whole lane
// being dark — every intermediate layer can report success while the course
// never surfaces.
package discovery_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"
	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	deliveryv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/delivery/v1"

	"github.com/apollo-chora/chora-sharing/internal/domain/discovery"
)

// TestHandleCoursePublished_BinaryWire_ReachesPublicFeed drives the handler
// with the real binary payload, not a hand-written JSON stand-in. A JSON
// fixture would pass against a decoder that cannot read the wire — which is
// exactly the shape of the bug.
func TestHandleCoursePublished_BinaryWire_ReachesPublicFeed(t *testing.T) {
	publishedAt := time.Date(2026, 7, 14, 8, 15, 0, 0, time.UTC)
	ev := &deliveryv1.CoursePublished{
		Envelope: &commonv1.EventEnvelope{
			EventId:  "01980000-0000-7000-8000-00000000feed",
			TenantId: "tenant-alpha",
			Gcid:     "gcid-instructor-1",
		},
		CourseId:       "course-7",
		Title:          "Intro to Place Value",
		InstructorGcid: "gcid-instructor-1",
		PublishedAt:    timestamppb.New(publishedAt),
	}
	payload, err := proto.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	reg := discovery.NewRegistry()
	handler := discovery.HandleCoursePublished(reg)

	msg := eventbus.Message{
		Subject:   discovery.TopicCoursePublished,
		Payload: payload,
		Envelope: envelope.Envelope{
			EventID:  "01980000-0000-7000-8000-00000000feed",
			TenantID: "tenant-alpha",
			GCID:     "gcid-instructor-1",
		},
	}
	if err := handler(context.Background(), msg); err != nil {
		t.Fatalf("HandleCoursePublished on BINARY wire bytes: %v", err)
	}

	// The whole point: it must be VISIBLE, not merely recorded. PublicCourses()
	// filters Visibility != "public", so a course recorded with an empty
	// visibility is dropped here — silently, with every layer reporting OK.
	pub := reg.PublicCourses()
	if len(pub) != 1 {
		t.Fatalf("PublicCourses() = %d courses, want 1 — the course was recorded but "+
			"filtered out of the feed (visibility never set from a proto that has no "+
			"visibility field)", len(pub))
	}
	got := pub[0]
	if got.CourseID != "course-7" {
		t.Errorf("CourseID = %q, want course-7", got.CourseID)
	}
	if got.Title != "Intro to Place Value" {
		t.Errorf("Title = %q", got.Title)
	}
	if got.TenantID != "tenant-alpha" {
		t.Errorf("TenantID = %q", got.TenantID)
	}
	if got.InstructorGCID != "gcid-instructor-1" {
		t.Errorf("InstructorGCID = %q", got.InstructorGCID)
	}
	if !got.PublishedAt.Equal(publishedAt) {
		t.Errorf("PublishedAt = %v, want %v", got.PublishedAt, publishedAt)
	}
}

// A malformed payload must FAIL LOUD (error -> Nack -> retry -> DLQ), never be
// silently swallowed into an empty feed.
func TestHandleCoursePublished_GarbagePayload_Errors(t *testing.T) {
	reg := discovery.NewRegistry()
	handler := discovery.HandleCoursePublished(reg)

	msg := eventbus.Message{
		Subject:   discovery.TopicCoursePublished,
		Payload: []byte{0xff, 0xfe, 0xfd, 0xfc},
	}
	if err := handler(context.Background(), msg); err == nil {
		t.Fatal("a payload that is neither valid proto nor valid JSON must return an error " +
			"so Pub/Sub Nacks and eventually dead-letters — silence would hide the break")
	}
	if n := len(reg.PublicCourses()); n != 0 {
		t.Errorf("PublicCourses() = %d, want 0 after a failed decode", n)
	}
}
