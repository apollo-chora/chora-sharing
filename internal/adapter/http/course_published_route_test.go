package httpadapter_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/eventpush"

	httpapi "github.com/apollo-chora/chora-sharing/internal/adapter/http"
	"github.com/apollo-chora/chora-sharing/internal/domain/discovery"
)

// The route must actually be MOUNTED on the service mux.
//
// This is not a formality. chora-sharing's NewWeaknessGrownPushHandler has
// existed for some time and is wired into the gateway's SharingInboxes
// allowlist — but it is never mounted on any mux, so the gateway forwards
// /api/internal/pubsub/weakness-grown to chora-sharing and gets a 404, which
// Pub/Sub retries 5x and then dead-letters. A constructed-but-unmounted handler
// is indistinguishable from no handler at all.
//
// The gateway forwards the FULL path (resolveDownstream only picks the base
// URL; it does not rewrite), so the mounted path must be the complete
// /api/internal/pubsub/course-published.
func TestRouter_MountsCoursePublishedPushRoute(t *testing.T) {
	reg := discovery.NewRegistry()
	push := httpapi.NewCoursePublishedPushHandler(httpapi.CoursePublishedPushDeps{
		Registry: reg,
		Verifier: eventpush.NewVerifier(eventpush.VerifierConfig{}),
	})
	h := httpapi.NewHandler(httpapi.Deps{CoursePublishedPushHandler: push})

	req := httptest.NewRequest(http.MethodPost, "/api/internal/pubsub/course-published",
		bytesReader(binaryPushBody(t, discovery.TopicCoursePublished, coursePublishedWire(t))))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code == http.StatusNotFound {
		t.Fatal("/api/internal/pubsub/course-published is NOT mounted — the gateway would " +
			"forward here and get a 404, which Pub/Sub retries then dead-letters")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	if n := len(reg.PublicCourses()); n != 1 {
		t.Errorf("PublicCourses() = %d, want 1 — the mounted route must reach the projection", n)
	}
}

// Fail loud, not fake-success: with no push handler injected the route must NOT
// silently 200. Per the Deps contract a nil dependency leaves the route
// unregistered (404) rather than acking a message it never processed — acking
// would tell Pub/Sub the event was handled and discard it forever.
func TestRouter_NilCoursePublishedHandler_DoesNotFakeSuccess(t *testing.T) {
	h := httpapi.NewHandler(httpapi.Deps{})

	req := httptest.NewRequest(http.MethodPost, "/api/internal/pubsub/course-published", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code == http.StatusOK {
		t.Fatal("an unwired push route must never return 200 — that acks the message and " +
			"discards the event permanently")
	}
}
