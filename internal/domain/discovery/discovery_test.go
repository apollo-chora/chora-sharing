// Package discovery_test exercises the in-memory discovery registry
// + event subscriber wiring (S4.4 — A-Content-Sharing — Phyllis MVP §5
// Step 5).
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

const (
	tenantA = "01970000-0000-7000-8000-000000000001"
	gcidA   = "01970000-0000-7000-9000-000000000001"
)

func TestRegistry_RecordCourse_AddsToList(t *testing.T) {
	r := discovery.NewRegistry()
	r.RecordCourse(discovery.Course{
		CourseID:       "csm-prep",
		TenantID:       tenantA,
		Visibility:     "public",
		EnrolmentCount: 300,
		PublishedAt:    time.Now().UTC(),
	})
	got := r.PublicCourses()
	if len(got) != 1 {
		t.Fatalf("expected 1 course, got %d", len(got))
	}
	if got[0].CourseID != "csm-prep" {
		t.Fatalf("unexpected: %v", got[0])
	}
}

// Idempotency on (course_id, tenant_id): re-record updates fields, does not
// duplicate.
func TestRegistry_RecordCourse_Idempotent(t *testing.T) {
	r := discovery.NewRegistry()
	r.RecordCourse(discovery.Course{CourseID: "x", TenantID: tenantA, Visibility: "public", EnrolmentCount: 1, PublishedAt: time.Now().UTC()})
	r.RecordCourse(discovery.Course{CourseID: "x", TenantID: tenantA, Visibility: "public", EnrolmentCount: 5, PublishedAt: time.Now().UTC()})
	got := r.PublicCourses()
	if len(got) != 1 {
		t.Fatalf("expected idempotent — 1 row, got %d", len(got))
	}
	if got[0].EnrolmentCount != 5 {
		t.Fatalf("expected updated EnrolmentCount=5, got %d", got[0].EnrolmentCount)
	}
}

// PublicCourses sorted by EnrolmentCount desc.
func TestRegistry_PublicCourses_SortedByPopularity(t *testing.T) {
	r := discovery.NewRegistry()
	r.RecordCourse(discovery.Course{CourseID: "a", TenantID: tenantA, Visibility: "public", EnrolmentCount: 5, PublishedAt: time.Now().UTC()})
	r.RecordCourse(discovery.Course{CourseID: "b", TenantID: tenantA, Visibility: "public", EnrolmentCount: 50, PublishedAt: time.Now().UTC()})
	got := r.PublicCourses()
	if got[0].CourseID != "b" {
		t.Fatalf("expected b first (50 enrolments), got %s", got[0].CourseID)
	}
}

// PublicCourses excludes private courses.
func TestRegistry_PublicCourses_ExcludesPrivate(t *testing.T) {
	r := discovery.NewRegistry()
	r.RecordCourse(discovery.Course{CourseID: "pub", Visibility: "public", PublishedAt: time.Now().UTC()})
	r.RecordCourse(discovery.Course{CourseID: "priv", Visibility: "tenant", PublishedAt: time.Now().UTC()})
	r.RecordCourse(discovery.Course{CourseID: "priv2", Visibility: "private", PublishedAt: time.Now().UTC()})
	got := r.PublicCourses()
	if len(got) != 1 || got[0].CourseID != "pub" {
		t.Fatalf("expected only pub: got %v", got)
	}
}

// AtomActivity recording: count per atom; TrendingAtoms returns desc by count.
func TestRegistry_RecordAtomActivity_Counts(t *testing.T) {
	r := discovery.NewRegistry()
	now := time.Now().UTC()
	r.RecordAtomActivity(discovery.AtomActivity{AtomID: "atom-1", TenantID: tenantA, CompletedAt: now})
	r.RecordAtomActivity(discovery.AtomActivity{AtomID: "atom-1", TenantID: tenantA, CompletedAt: now})
	r.RecordAtomActivity(discovery.AtomActivity{AtomID: "atom-1", TenantID: tenantA, CompletedAt: now})
	r.RecordAtomActivity(discovery.AtomActivity{AtomID: "atom-2", TenantID: tenantA, CompletedAt: now})

	got := r.TrendingAtoms()
	if len(got) != 2 {
		t.Fatalf("expected 2 atoms, got %d", len(got))
	}
	if got[0].AtomID != "atom-1" || got[0].ActivityCount != 3 {
		t.Fatalf("expected atom-1 (3), got %#v", got[0])
	}
	if got[1].AtomID != "atom-2" || got[1].ActivityCount != 1 {
		t.Fatalf("expected atom-2 (1), got %#v", got[1])
	}
}

// -----------------------------------------------------------------------------
// Subscriber: HandleCoursePublished decodes payload and writes to registry.
// -----------------------------------------------------------------------------

func TestHandleCoursePublished_DecodesAndRecords(t *testing.T) {
	r := discovery.NewRegistry()
	h := discovery.HandleCoursePublished(r)

	body := map[string]interface{}{
		"course_id":       "csm-prep",
		"tenant_id":       tenantA,
		"instructor_gcid": "mr-chen",
		"title":           "CSM-Prep",
		"visibility":      "public",
		"enrolment_count": 300,
		"published_at":    "2026-05-08T12:00:00Z",
	}
	payload, _ := json.Marshal(body)

	env := cgcenvelope.Build(context.Background(), cgcenvelope.BuildOpts{
		EventType:     discovery.TopicCoursePublished,
		SchemaVersion: 1,
		SourceProject: "chora-489812",
		SourceService: "chora-delivery",
	})
	env.TenantID = tenantA

	msg := eventbus.Message{
		Subject:    discovery.TopicCoursePublished,
		Envelope: env,
		Payload:  payload,
	}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("handler: %v", err)
	}

	got := r.PublicCourses()
	if len(got) != 1 || got[0].CourseID != "csm-prep" {
		t.Fatalf("expected csm-prep recorded, got %v", got)
	}
	if got[0].EnrolmentCount != 300 {
		t.Fatalf("expected 300 enrolments, got %d", got[0].EnrolmentCount)
	}
}

// Subscriber rejects malformed JSON payload.
func TestHandleCoursePublished_RejectsMalformed(t *testing.T) {
	r := discovery.NewRegistry()
	h := discovery.HandleCoursePublished(r)
	env := cgcenvelope.Build(context.Background(), cgcenvelope.BuildOpts{
		EventType:     discovery.TopicCoursePublished,
		SchemaVersion: 1,
		SourceProject: "chora-489812",
		SourceService: "chora-delivery",
	})
	env.TenantID = tenantA
	msg := eventbus.Message{
		Subject:    discovery.TopicCoursePublished,
		Envelope: env,
		Payload:  []byte("{not-json"),
	}
	if err := h(context.Background(), msg); err == nil {
		t.Fatalf("expected error on malformed payload")
	}
}

// -----------------------------------------------------------------------------
// HandleAtomSessionCompleted
// -----------------------------------------------------------------------------

// Records with empty CourseID are skipped (silently — defensive guard).
func TestRegistry_RecordCourse_SkipsEmpty(t *testing.T) {
	r := discovery.NewRegistry()
	r.RecordCourse(discovery.Course{CourseID: "", Visibility: "public"})
	if len(r.PublicCourses()) != 0 {
		t.Fatalf("expected empty after recording with empty CourseID")
	}
}

// Records with empty AtomID are skipped.
func TestRegistry_RecordAtomActivity_SkipsEmpty(t *testing.T) {
	r := discovery.NewRegistry()
	r.RecordAtomActivity(discovery.AtomActivity{AtomID: ""})
	if len(r.TrendingAtoms()) != 0 {
		t.Fatalf("expected empty after recording with empty AtomID")
	}
}

// HandleCoursePublished rejects payload missing course_id.
func TestHandleCoursePublished_RejectsMissingCourseID(t *testing.T) {
	r := discovery.NewRegistry()
	h := discovery.HandleCoursePublished(r)
	env := cgcenvelope.Build(context.Background(), cgcenvelope.BuildOpts{
		EventType:     discovery.TopicCoursePublished,
		SchemaVersion: 1,
		SourceProject: "chora-489812",
		SourceService: "chora-delivery",
	})
	env.TenantID = tenantA
	payload, _ := json.Marshal(map[string]interface{}{
		"tenant_id":  tenantA,
		"visibility": "public",
	})
	msg := eventbus.Message{
		Subject:    discovery.TopicCoursePublished,
		Envelope: env,
		Payload:  payload,
	}
	if err := h(context.Background(), msg); err == nil {
		t.Fatalf("expected error on missing course_id")
	}
}

// HandleAtomSessionCompleted rejects malformed payload.
func TestHandleAtomSessionCompleted_RejectsMalformed(t *testing.T) {
	r := discovery.NewRegistry()
	h := discovery.HandleAtomSessionCompleted(r)
	env := cgcenvelope.Build(context.Background(), cgcenvelope.BuildOpts{
		EventType:     discovery.TopicAtomSessionCompleted,
		SchemaVersion: 1,
		SourceProject: "chora-489812",
		SourceService: "chora-consumption",
	})
	env.TenantID = tenantA
	msg := eventbus.Message{
		Subject:    discovery.TopicAtomSessionCompleted,
		Envelope: env,
		Payload:  []byte("{not-json"),
	}
	if err := h(context.Background(), msg); err == nil {
		t.Fatalf("expected error on malformed payload")
	}
}

// HandleAtomSessionCompleted rejects payload missing atom_id.
func TestHandleAtomSessionCompleted_RejectsMissingAtomID(t *testing.T) {
	r := discovery.NewRegistry()
	h := discovery.HandleAtomSessionCompleted(r)
	env := cgcenvelope.Build(context.Background(), cgcenvelope.BuildOpts{
		EventType:     discovery.TopicAtomSessionCompleted,
		SchemaVersion: 1,
		SourceProject: "chora-489812",
		SourceService: "chora-consumption",
	})
	env.TenantID = tenantA
	payload, _ := json.Marshal(map[string]interface{}{
		"session_id": "sess-1",
		"tenant_id":  tenantA,
		"gcid":       gcidA,
	})
	msg := eventbus.Message{
		Subject:    discovery.TopicAtomSessionCompleted,
		Envelope: env,
		Payload:  payload,
	}
	if err := h(context.Background(), msg); err == nil {
		t.Fatalf("expected error on missing atom_id")
	}
}

func TestHandleAtomSessionCompleted_DecodesAndRecords(t *testing.T) {
	r := discovery.NewRegistry()
	h := discovery.HandleAtomSessionCompleted(r)

	body := map[string]interface{}{
		"session_id":   "sess-1",
		"atom_id":      "atom-9",
		"tenant_id":    tenantA,
		"gcid":         gcidA,
		"completed_at": "2026-05-08T13:00:00Z",
	}
	payload, _ := json.Marshal(body)

	env := cgcenvelope.Build(context.Background(), cgcenvelope.BuildOpts{
		EventType:     discovery.TopicAtomSessionCompleted,
		SchemaVersion: 1,
		SourceProject: "chora-489812",
		SourceService: "chora-consumption",
	})
	env.TenantID = tenantA
	env.GCID = gcidA

	msg := eventbus.Message{
		Subject:    discovery.TopicAtomSessionCompleted,
		Envelope: env,
		Payload:  payload,
	}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("handler: %v", err)
	}

	got := r.TrendingAtoms()
	if len(got) != 1 || got[0].AtomID != "atom-9" {
		t.Fatalf("expected atom-9 in trending, got %v", got)
	}
}
