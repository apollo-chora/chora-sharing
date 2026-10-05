// Package discovery is the pure-domain core for the C+ discovery feed
// (S4.4 — A-Content-Sharing — Phyllis MVP §5 Step 5).
//
// The feed is populated entirely by Pub/Sub event subscribers from sister
// services — chora-delivery (course.published) and chora-consumption
// (atom_session.completed) — per .claude/rules/ddd-enforcement.md
// HARD RULE: NO cross-database joins; events are the only inter-domain
// channel.
//
// Two event-source-projection registers maintained here:
//
//	Course        — populated by chora.delivery.course.published.v1
//	AtomActivity  — populated by chora.consumption.atom_session.completed.v1
//
// Production wiring (M12+) will swap the in-memory Registry for a Postgres
// adapter against chora_sharing.discovery_courses + discovery_atom_activity
// materialised views; the handler signatures remain stable.
package discovery

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/apollo-chora/chora-common/eventbus"

	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protodecode"
)

// Canonical event topics consumed by chora-sharing for discovery feed
// population. These topics live in the chora.delivery.* and
// chora.consumption.* taxonomy — chora-sharing is a *subscriber* (pure
// read-side projection); cross-DB read is forbidden.
//
// Topic naming conventions (chora-infra/topics/topics.yaml + sister-agent
// emit signatures as of 2026-05-09):
//
//   - chora-delivery currently emits course.created.v1; the catalogue
//     reserves course.published.v1 for the future "go-live" event. We
//     subscribe to BOTH so the discovery feed populates regardless of
//     sister-agent rollout sequencing (the projection is idempotent on
//     course_id).
//   - chora-consumption emits atom_session.completed.v1.
const (
	// TopicCoursePublished — preferred future name when chora-delivery
	// adds an explicit go-live event (a published course is the public
	// surface event Phyllis MVP §5 Step 5 calls out). Subscribe to it
	// for forward compatibility.
	TopicCoursePublished = "chora.delivery.course.published.v1"

	// TopicCourseCreated — current emit signature on chora-delivery as of
	// the S4.3 sister-agent state. Subscribe to it for the demo today.
	TopicCourseCreated = "chora.delivery.course.created.v1"

	// TopicAtomSessionCompleted — emitted by chora-consumption when an
	// AtomAttempt completes. Activity count drives the trending-atoms
	// view ("atoms peers are working through right now").
	TopicAtomSessionCompleted = "chora.consumption.atom_session.completed.v1"
)

// Course is the registry projection of a published course.
//
// Fields are sourced from the course.published.v1 payload; we hold them
// here as opaque cross-domain references (no FK constraints — per
// ddd-enforcement.md, references between domains are UUIDs only).
type Course struct {
	CourseID       string
	TenantID       string
	InstructorGCID string
	Title          string
	Visibility     string // "public" | "tenant" | "private"
	EnrolmentCount int
	PublishedAt    time.Time
}

// AtomActivity records a single atom-completion signal sourced from
// chora.consumption.atom_session.completed.v1. The trending view
// surfaces atoms with the highest aggregate ActivityCount.
type AtomActivity struct {
	AtomID        string
	TenantID      string
	AuthorGCID    string
	CompletedAt   time.Time
	ActivityCount int // populated when read via TrendingAtoms()
}

// Registry is an in-memory event-source projection of public courses +
// atom activity, scoped per process. Production swap-in lands at M12.
type Registry struct {
	mu          sync.RWMutex
	courses     map[string]Course // key = course_id
	atomActCnt  map[string]int    // atom_id → completion count
	atomLatest  map[string]AtomActivity
}

// NewRegistry returns an empty Registry ready to subscribe to events.
func NewRegistry() *Registry {
	return &Registry{
		courses:    make(map[string]Course),
		atomActCnt: make(map[string]int),
		atomLatest: make(map[string]AtomActivity),
	}
}

// RecordCourse upserts a Course projection. Idempotent on (course_id):
// a re-record updates fields in place (the chora-delivery emit is the
// source-of-truth for visibility/enrolment changes).
func (r *Registry) RecordCourse(c Course) {
	if c.CourseID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.courses[c.CourseID] = c
}

// RecordAtomActivity logs a single atom_session.completed signal. The
// trending view aggregates these into ActivityCount per atom.
func (r *Registry) RecordAtomActivity(a AtomActivity) {
	if a.AtomID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.atomActCnt[a.AtomID]++
	r.atomLatest[a.AtomID] = a
}

// PublicCourses returns the public-visibility courses sorted by
// EnrolmentCount descending (popularity proxy per the brief). Tenant +
// private courses are excluded.
func (r *Registry) PublicCourses() []Course {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Course, 0, len(r.courses))
	for _, c := range r.courses {
		if c.Visibility != "public" {
			continue
		}
		out = append(out, c)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].EnrolmentCount != out[j].EnrolmentCount {
			return out[i].EnrolmentCount > out[j].EnrolmentCount
		}
		// Stable tiebreak by CourseID for deterministic ordering in tests.
		return out[i].CourseID < out[j].CourseID
	})
	return out
}

// TrendingAtoms returns atoms sorted by ActivityCount descending. Each
// returned AtomActivity carries the ActivityCount populated.
func (r *Registry) TrendingAtoms() []AtomActivity {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]AtomActivity, 0, len(r.atomActCnt))
	for atomID, count := range r.atomActCnt {
		latest := r.atomLatest[atomID]
		latest.ActivityCount = count
		out = append(out, latest)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].ActivityCount != out[j].ActivityCount {
			return out[i].ActivityCount > out[j].ActivityCount
		}
		return out[i].AtomID < out[j].AtomID
	})
	return out
}

// -----------------------------------------------------------------------------
// Subscriber adapters
// -----------------------------------------------------------------------------

// coursePublishedPayload is the JSON shape expected on
// chora.delivery.course.published.v1. The payload aligns with the
// chora-contracts Protobuf spec; we accept JSON in the interim per the
// Phyllis MVP "JSON acceptable until codegen wired" mode.
type coursePublishedPayload struct {
	CourseID       string `json:"course_id"`
	TenantID       string `json:"tenant_id"`
	InstructorGCID string `json:"instructor_gcid"`
	Title          string `json:"title"`
	Visibility     string `json:"visibility"`
	EnrolmentCount int    `json:"enrolment_count"`
	PublishedAt    string `json:"published_at"`
}

// atomSessionCompletedPayload is the JSON shape for
// chora.consumption.atom_session.completed.v1.
type atomSessionCompletedPayload struct {
	SessionID   string `json:"session_id"`
	AtomID      string `json:"atom_id"`
	TenantID    string `json:"tenant_id"`
	GCID        string `json:"gcid"`
	AuthorGCID  string `json:"author_gcid,omitempty"`
	CompletedAt string `json:"completed_at"`
}

// HandleCoursePublished returns a eventbus.Handler that projects the
// course.published.v1 payload into the discovery registry.
//
// Wire-format-tolerant via protodecode (proto.Unmarshal first, json.Unmarshal
// fallback). Pub/Sub envelope attributes are projected back from the bus's
// reconstructed Envelope so envelope fields (event_id / tenant_id / gcid)
// populate the map even when the producer emits JSON-only payloads. On
// malformed payload the handler returns an error so the bus Nacks; persistent
// malformed messages deadletter via the broker's dead_letter_policy.
func HandleCoursePublished(r *Registry) eventbus.Handler {
	return func(_ context.Context, msg eventbus.Message) error {
		raw, err := protodecode.DecodePayloadMapWithAttrs(TopicCoursePublished, msg.Payload, attrsFromBusEnvelope(msg))
		if err != nil {
			return fmt.Errorf("discovery: course.published payload decode: %w", err)
		}
		courseID, _ := raw["course_id"].(string)
		if courseID == "" {
			return fmt.Errorf("discovery: course.published missing course_id")
		}
		ts, _ := parseRFC3339(raw["published_at"])
		title, _ := raw["title"].(string)
		visibility, _ := raw["visibility"].(string)
		tenantID, _ := raw["tenant_id"].(string)
		instructorGCID, _ := raw["instructor_gcid"].(string)
		// Fallback for instructor_gcid: legacy publishers stamped only
		// gcid on the envelope (no separate instructor field on the
		// payload); accept that as the instructor under hand-off.
		if instructorGCID == "" {
			instructorGCID, _ = raw["gcid"].(string)
		}
		enrolmentCount := 0
		if v, ok := raw["enrolment_count"].(float64); ok {
			enrolmentCount = int(v)
		} else if v, ok := raw["enrolment_count"].(int); ok {
			enrolmentCount = v
		}
		r.RecordCourse(Course{
			CourseID:       courseID,
			TenantID:       tenantID,
			InstructorGCID: instructorGCID,
			Title:          title,
			Visibility:     visibility,
			EnrolmentCount: enrolmentCount,
			PublishedAt:    ts,
		})
		return nil
	}
}

// HandleAtomSessionCompleted returns a eventbus.Handler that projects
// atom_session.completed.v1 into the trending-atoms register.
//
// Envelope-aware: bus envelope fields (event_id / tenant_id / gcid) are
// merged into the decoded map so the projection succeeds even when the
// producer is on JSON-only.
func HandleAtomSessionCompleted(r *Registry) eventbus.Handler {
	return func(_ context.Context, msg eventbus.Message) error {
		raw, err := protodecode.DecodePayloadMapWithAttrs(TopicAtomSessionCompleted, msg.Payload, attrsFromBusEnvelope(msg))
		if err != nil {
			return fmt.Errorf("discovery: atom_session.completed payload decode: %w", err)
		}
		atomID, _ := raw["atom_id"].(string)
		if atomID == "" {
			return fmt.Errorf("discovery: atom_session.completed missing atom_id")
		}
		ts, _ := parseRFC3339(raw["completed_at"])
		tenantID, _ := raw["tenant_id"].(string)
		authorGCID, _ := raw["author_gcid"].(string)
		// author_gcid often isn't on the JSON body — the envelope's GCID
		// (recorded as the session owner) is the canonical fallback.
		if authorGCID == "" {
			authorGCID, _ = raw["owner_gcid"].(string)
		}
		if authorGCID == "" {
			authorGCID, _ = raw["gcid"].(string)
		}
		r.RecordAtomActivity(AtomActivity{
			AtomID:      atomID,
			TenantID:    tenantID,
			AuthorGCID:  authorGCID,
			CompletedAt: ts,
		})
		return nil
	}
}

// attrsFromBusEnvelope projects an eventbus.Message's parsed envelope back
// into the attribute shape expected by the protodecode helpers. The bus
// reconstructs Envelope from the wire headers on receive; this is the
// inverse so DecodePayloadMapWithAttrs can merge envelope fields into the
// JSON fallback path.
func attrsFromBusEnvelope(msg eventbus.Message) map[string]string {
	env := msg.Envelope
	attrs := map[string]string{
		"topic":     msg.Subject,
		"event_id":  env.EventID,
		"tenant_id": env.TenantID,
		"gcid":      env.GCID,
	}
	if env.Traceparent != "" {
		attrs["traceparent"] = env.Traceparent
	}
	if env.Tracestate != "" {
		attrs["tracestate"] = env.Tracestate
	}
	if !env.OccurredAt.IsZero() {
		attrs["occurred_at"] = env.OccurredAt.UTC().Format(time.RFC3339Nano)
	}
	if !env.PublishedAt.IsZero() {
		attrs["published_at"] = env.PublishedAt.UTC().Format(time.RFC3339Nano)
	}
	if env.ChoraImdaDimension != "" {
		attrs["chora_imda_dimension"] = env.ChoraImdaDimension
	}
	if env.ImdaLifecycleStage != "" {
		attrs["imda_lifecycle_stage"] = env.ImdaLifecycleStage
	}
	return attrs
}

// parseRFC3339 returns a parsed time from a map value. Accepts the JSON
// fallback shape (string) and the binary projector shape (also string).
func parseRFC3339(v any) (time.Time, error) {
	s, ok := v.(string)
	if !ok || s == "" {
		return time.Time{}, fmt.Errorf("discovery: timestamp not a string")
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}
