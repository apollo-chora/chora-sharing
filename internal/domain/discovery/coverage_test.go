// Package discovery_test holds the remaining coverage specs for the C+
// discovery feed: deterministic sort tie-breaks, envelope-attribute
// projection, and defensive timestamp handling.
package discovery_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"

	"github.com/apollo-chora/chora-sharing/internal/domain/discovery"
)

// PublicCourses tie-breaks equal enrolment counts by CourseID ascending so the
// feed order is deterministic.
func TestRegistry_PublicCourses_TiebreakByCourseID(t *testing.T) {
	r := discovery.NewRegistry()
	r.RecordCourse(discovery.Course{CourseID: "b", Visibility: "public", EnrolmentCount: 5, PublishedAt: time.Now().UTC()})
	r.RecordCourse(discovery.Course{CourseID: "a", Visibility: "public", EnrolmentCount: 5, PublishedAt: time.Now().UTC()})
	got := r.PublicCourses()
	if len(got) != 2 || got[0].CourseID != "a" || got[1].CourseID != "b" {
		t.Fatalf("equal enrolment counts must tie-break by CourseID asc, got %+v", got)
	}
}

// TrendingAtoms tie-breaks equal activity counts by AtomID ascending.
func TestRegistry_TrendingAtoms_TiebreakByAtomID(t *testing.T) {
	r := discovery.NewRegistry()
	r.RecordAtomActivity(discovery.AtomActivity{AtomID: "z", CompletedAt: time.Now().UTC()})
	r.RecordAtomActivity(discovery.AtomActivity{AtomID: "a", CompletedAt: time.Now().UTC()})
	got := r.TrendingAtoms()
	if len(got) != 2 || got[0].AtomID != "a" || got[1].AtomID != "z" {
		t.Fatalf("equal activity counts must tie-break by AtomID asc, got %+v", got)
	}
}

// A fully-populated bus envelope projects every field back into the attribute
// map, and a missing instructor_gcid on the payload falls back to the
// envelope's gcid.
func TestHandleCoursePublished_EnvelopeAttributesAndGCIDFallback(t *testing.T) {
	r := discovery.NewRegistry()
	h := discovery.HandleCoursePublished(r)

	now := time.Now().UTC().Truncate(time.Millisecond)
	env := cgcenvelope.Build(context.Background(), cgcenvelope.BuildOpts{
		EventType:     discovery.TopicCoursePublished,
		SchemaVersion: 1,
		SourceProject: "chora-489812",
		SourceService: "chora-delivery",
	})
	env.EventID = "evt-1024"
	env.TenantID = tenantA
	env.GCID = "instructor-gcid-fallback"
	env.Traceparent = "00-abc-def-01"
	env.Tracestate = "vendor=chora"
	env.OccurredAt = now
	env.PublishedAt = now
	env.ChoraImdaDimension = "edu"
	env.ImdaLifecycleStage = "learn"

	body := map[string]interface{}{
		"course_id":       "csm-prep",
		"tenant_id":       tenantA,
		"gcid":            "instructor-gcid-fallback",
		"title":           "CSM-Prep",
		"visibility":      "public",
		"enrolment_count": 300,
		"published_at":    "2026-05-08T12:00:00Z",
	}
	payload, _ := json.Marshal(body)
	msg := eventbus.Message{Subject: discovery.TopicCoursePublished, Envelope: env, Payload: payload}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("handler: %v", err)
	}
	courses := r.PublicCourses()
	if len(courses) != 1 {
		t.Fatalf("expected 1 course, got %d", len(courses))
	}
	if courses[0].InstructorGCID != "instructor-gcid-fallback" {
		t.Errorf("expected instructor fallback from payload gcid, got %q", courses[0].InstructorGCID)
	}
	if courses[0].EnrolmentCount != 300 {
		t.Errorf("enrolment count = %d, want 300", courses[0].EnrolmentCount)
	}
}

// A non-string published_at is tolerated: parseRFC3339 fails, the error is
// ignored by the handler (timestamp optional), and the course is still
// recorded.
func TestHandleCoursePublished_NonStringPublishedAt(t *testing.T) {
	r := discovery.NewRegistry()
	h := discovery.HandleCoursePublished(r)
	env := cgcenvelope.Build(context.Background(), cgcenvelope.BuildOpts{
		EventType:     discovery.TopicCoursePublished,
		SchemaVersion: 1,
		SourceProject: "chora-489812",
		SourceService: "chora-delivery",
	})
	env.EventID = "evt-103"
	env.TenantID = tenantA
	env.OccurredAt = time.Time{}
	env.PublishedAt = time.Time{}
	body := map[string]interface{}{
		"course_id":    "course-7",
		"visibility":   "public",
		"published_at": 12345, // number, not RFC3339 string
	}
	payload, _ := json.Marshal(body)
	msg := eventbus.Message{Subject: discovery.TopicCoursePublished, Envelope: env, Payload: payload}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("handler: %v", err)
	}
	courses := r.PublicCourses()
	if len(courses) != 1 || courses[0].CourseID != "course-7" {
		t.Fatalf("course must be recorded despite malformed published_at, got %+v", courses)
	}
	if !courses[0].PublishedAt.IsZero() {
		t.Errorf("expected zero PublishedAt for a malformed timestamp, got %v", courses[0].PublishedAt)
	}
}

// When the payload carries owner_gcid instead of author_gcid, the session
// handler uses it as the author fallback.
func TestHandleAtomSessionCompleted_OwnerGCIDFallback(t *testing.T) {
	r := discovery.NewRegistry()
	h := discovery.HandleAtomSessionCompleted(r)
	env := cgcenvelope.Build(context.Background(), cgcenvelope.BuildOpts{
		EventType:     discovery.TopicAtomSessionCompleted,
		SchemaVersion: 1,
		SourceProject: "chora-489812",
		SourceService: "chora-consumption",
	})
	env.TenantID = tenantA
	body := map[string]interface{}{
		"session_id":   "sess-2",
		"atom_id":      "atom-11",
		"tenant_id":    tenantA,
		"owner_gcid":   "owner-gcid-1",
		"completed_at": "2026-05-08T13:00:00Z",
	}
	payload, _ := json.Marshal(body)
	msg := eventbus.Message{Subject: discovery.TopicAtomSessionCompleted, Envelope: env, Payload: payload}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("handler: %v", err)
	}
	got := r.TrendingAtoms()
	if len(got) != 1 || got[0].AuthorGCID != "owner-gcid-1" {
		t.Fatalf("expected owner_gcid fallback for author, got %+v", got)
	}
}

// When neither author_gcid nor owner_gcid is present anywhere (payload or
// envelope), the final envelope-gcid fallback still executes (canonical
// owner opaqueness) and the activity is recorded with an empty author.
func TestHandleAtomSessionCompleted_NoGCIDFallbackRuns(t *testing.T) {
	r := discovery.NewRegistry()
	h := discovery.HandleAtomSessionCompleted(r)
	env := cgcenvelope.Build(context.Background(), cgcenvelope.BuildOpts{
		EventType:     discovery.TopicAtomSessionCompleted,
		SchemaVersion: 1,
		SourceProject: "chora-489812",
		SourceService: "chora-consumption",
	})
	env.TenantID = tenantA
	body := map[string]interface{}{
		"session_id":   "sess-3",
		"atom_id":      "atom-12",
		"tenant_id":    tenantA,
		"completed_at": "2026-05-08T13:00:00Z",
	}
	payload, _ := json.Marshal(body)
	msg := eventbus.Message{Subject: discovery.TopicAtomSessionCompleted, Envelope: env, Payload: payload}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("handler: %v", err)
	}
	got := r.TrendingAtoms()
	if len(got) != 1 || got[0].AtomID != "atom-12" {
		t.Fatalf("expected atom-12 recorded, got %+v", got)
	}
	if got[0].AuthorGCID != "" {
		t.Errorf("expected empty author (no gcid anywhere), got %q", got[0].AuthorGCID)
	}
}