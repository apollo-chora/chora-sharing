// course_published.go — binary decoder for chora.delivery.course.published.v1
// (CHO-2155 follow-up).
//
// chora-delivery publishes this topic as BINARY protobuf against a Pub/Sub
// Schema Registry schema. Without an entry in binaryDecoders the bytes fall
// through to the JSON fallback, fail json.Unmarshal, and the subscriber errors
// -> Pub/Sub retries -> dead-letter. The topic was previously unconsumable.
package protodecode

import (
	"time"

	"google.golang.org/protobuf/proto"

	deliveryv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/delivery/v1"
)

// decodeCoursePublished unmarshals the binary CoursePublished wire bytes.
func decodeCoursePublished(payload []byte) (proto.Message, error) {
	var m deliveryv1.CoursePublished
	if err := proto.Unmarshal(payload, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// projectCoursePublished maps CoursePublished onto the snake_case shape
// discovery.HandleCoursePublished reads.
func projectCoursePublished(msg proto.Message, out map[string]any) {
	m, ok := msg.(*deliveryv1.CoursePublished)
	if !ok || m == nil {
		return
	}
	mergeEnvelope(m.GetEnvelope(), out)

	// visibility is NOT a field on CoursePublished, and that is deliberate: the
	// event fires ONLY when a Course's visibility flips INTO public (it does not
	// fire on private->tenant_only — see events/delivery/course.proto and
	// chora-delivery's v1_handlers_test). The visibility IS the event.
	//
	// It has to be stated explicitly, because discovery.Registry.PublicCourses()
	// filters `Visibility != "public"`. Leaving it empty means the course is
	// decoded cleanly, recorded, ack'd 200 — and then silently dropped from the
	// feed, with no error and nothing to dead-letter. Encoding the semantic here
	// is what makes the lane observable instead of quietly empty.
	out["visibility"] = "public"

	if v := m.GetCourseId(); v != "" {
		out["course_id"] = v
	}
	if v := m.GetTitle(); v != "" {
		out["title"] = v
	}
	if v := m.GetInstructorGcid(); v != "" {
		out["instructor_gcid"] = v
	}
	if t := m.GetPublishedAt(); t != nil {
		out["published_at"] = t.AsTime().UTC().Format(time.RFC3339Nano)
	}
	if v := m.GetImdaDimensions(); len(v) > 0 {
		dims := make([]any, 0, len(v))
		for _, d := range v {
			dims = append(dims, d)
		}
		out["imda_dimensions"] = dims
	}
}
