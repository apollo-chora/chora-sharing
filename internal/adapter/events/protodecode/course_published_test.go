// course_published_test — CHO-2155 follow-up.
//
// chora.delivery.course.published.v1 is a BINARY Schema-Registry topic. Until
// this decoder existed, chora-sharing could not consume it at all: the binary
// bytes fell through to the JSON fallback, failed json.Unmarshal, and the
// subscriber errored -> Pub/Sub retried -> dead-letter.
//
// The subtler half is `visibility`. The CoursePublished proto carries NO
// visibility field — by design, because the event's whole definition is "this
// course's visibility just flipped INTO public" (see the contract comment on
// events/delivery/course.proto: it does not fire on private->tenant_only).
// But discovery.Registry.PublicCourses() filters `Visibility != "public"`.
// So a decoder that leaves visibility empty produces a course that is
// recorded, returns 200, and is then SILENTLY filtered out of the feed —
// no error, no dead-letter, no trace. The projection must therefore assert
// the semantic the event carries.
package protodecode_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	deliveryv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/delivery/v1"

	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protodecode"
)

func TestDecode_CoursePublished_Binary(t *testing.T) {
	publishedAt := time.Date(2026, 7, 14, 8, 15, 0, 0, time.UTC)
	msg := &deliveryv1.CoursePublished{
		Envelope: &commonv1.EventEnvelope{
			EventId:  "01980000-0000-7000-8000-00000000c0de",
			TenantId: "tenant-alpha",
			Gcid:     "gcid-instructor-1",
		},
		CourseId:       "course-7",
		Title:          "Intro to Place Value",
		InstructorGcid: "gcid-instructor-1",
		PublishedAt:    timestamppb.New(publishedAt),
		ImdaDimensions: []string{"accountability", "transparency"},
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	got, err := protodecode.DecodePayloadMap("chora.delivery.course.published.v1", payload)
	if err != nil {
		t.Fatalf("DecodePayloadMap: %v (a binary payload with no registered decoder "+
			"falls through to the JSON fallback and fails -> Pub/Sub retry -> dead-letter)", err)
	}

	if got["course_id"] != "course-7" {
		t.Errorf("course_id = %v, want course-7", got["course_id"])
	}
	if got["title"] != "Intro to Place Value" {
		t.Errorf("title = %v, want %q", got["title"], "Intro to Place Value")
	}
	if got["instructor_gcid"] != "gcid-instructor-1" {
		t.Errorf("instructor_gcid = %v", got["instructor_gcid"])
	}
	if got["tenant_id"] != "tenant-alpha" {
		t.Errorf("tenant_id = %v (envelope must merge)", got["tenant_id"])
	}
	// discovery.parseRFC3339 expects a STRING — an emitted time.Time would
	// silently fail the type assertion and zero the timestamp.
	pub, ok := got["published_at"].(string)
	if !ok {
		t.Fatalf("published_at = %T, want an RFC3339 string", got["published_at"])
	}
	if ts, perr := time.Parse(time.RFC3339Nano, pub); perr != nil || !ts.Equal(publishedAt) {
		t.Errorf("published_at = %q, want %s", pub, publishedAt.Format(time.RFC3339Nano))
	}

	// THE SILENT ONE. The proto has no visibility field; the event IS the
	// flip into public. Without this, PublicCourses() drops the course and
	// the discovery feed stays empty while every layer reports success.
	if got["visibility"] != "public" {
		t.Errorf("visibility = %v, want \"public\" — course.published.v1 fires ONLY on the "+
			"flip into public, and discovery.PublicCourses() filters Visibility != \"public\", "+
			"so an empty visibility is SILENTLY dropped from the feed", got["visibility"])
	}
}
