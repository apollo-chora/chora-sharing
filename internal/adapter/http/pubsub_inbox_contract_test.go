// pubsub_inbox_contract_test.go — chora-sharing must MOUNT every inbox the
// canonical registry assigns to it.
//
// This is the test whose absence let CHO-2195 live for two weeks.
//
// chora-gateway routes `/api/internal/pubsub/{inbox}` by owner and forwards the
// FULL path unchanged. So an inbox the gateway sends here and this service does
// not mount is a 404 — which Pub/Sub retries five times and then dead-letters.
// On 2026-07-15 that was true of BOTH inboxes the gateway routed to us
// (weakness-grown, live-quiz-scores), while the one inbox we did mount
// (course-published) was one the gateway did not route. Perfectly inverted;
// every lane dead. Nothing failed.
//
// It hid because `NewWeaknessGrownPushHandler` exists and its unit test is
// GREEN — the test calls the handler DIRECTLY, bypassing the mux. A
// constructed-but-unmounted handler is indistinguishable from no handler at all.
// The only test that can tell them apart is one that goes THROUGH the mux, which
// is what this file does.
package httpadapter_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/http"
	"github.com/apollo-chora/chora-sharing/internal/adapter/eventpush"

	"github.com/apollo-chora/chora-sharing/internal/adapter/subscribers"
	"github.com/apollo-chora/chora-sharing/internal/domain/discovery"
	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
)

// Every registry-assigned inbox must be reachable through the service mux.
//
// The assertion is deliberately `!= 404` rather than `== 200`: a garbage body
// may legitimately produce a 400/500 from the dispatcher. What must never
// happen is 404 — "this route does not exist" — because that is the code Pub/Sub
// dead-letters on, and the code the gateway will get for an inbox it faithfully
// forwards here.
func TestMux_MountsEveryInboxTheRegistryAssignsToSharing(t *testing.T) {
	h := httpadapter.NewHandler(fullyWiredDeps(t))

	for _, inbox := range httpadapter.SharingInboxes {
		t.Run(inbox, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, httpadapter.InboxPath(inbox), nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code == http.StatusNotFound {
				t.Fatalf("%s is NOT MOUNTED on the service mux.\n"+
					"chora-gateway routes this inbox here and forwards %s verbatim. "+
					"An unmounted route 404s, Pub/Sub retries it 5x, and every message "+
					"dead-letters. A handler that exists but is never mounted is "+
					"indistinguishable from no handler.",
					inbox, httpadapter.InboxPath(inbox))
			}
		})
	}
}

// fullyWiredDeps builds Deps with every chora-sharing push handler injected.
// When a new inbox is added to the registry, this is the seam that forces
// someone to build its handler — and the test above forces them to mount it.
func fullyWiredDeps(t *testing.T) httpadapter.Deps {
	t.Helper()
	ranker := leaderboard.NewRanker()
	idem := subscribers.NewInMemoryIdempotencyStore()
	devVerifier := func() *eventpush.Verifier {
		return eventpush.NewVerifier(eventpush.VerifierConfig{})
	}

	return httpadapter.Deps{
		CoursePublishedPushHandler: httpadapter.NewCoursePublishedPushHandler(httpadapter.CoursePublishedPushDeps{
			Registry: discovery.NewRegistry(),
			Verifier: devVerifier(),
		}),
		WeaknessGrownPushHandler: httpadapter.NewWeaknessGrownPushHandler(httpadapter.WeaknessGrownPushDeps{
			Subscriber: subscribers.NewWeaknessGrownSubscriber(subscribers.WeaknessGrownConfig{
				Ranker:      ranker,
				Idempotency: idem,
			}),
			Verifier: devVerifier(),
		}),
		LiveQuizScorePushHandler: httpadapter.NewLiveQuizScorePushHandler(httpadapter.LiveQuizScorePushDeps{
			Subscriber: subscribers.NewLiveQuizScoreSubscriber(subscribers.LiveQuizScoreConfig{
				Ranker:      ranker,
				Idempotency: idem,
			}),
			Verifier: devVerifier(),
		}),
	}
}

// A Handler built with a missing push handler must REPORT the gap, not hide it.
// main() turns this into a boot failure: chora-sharing refuses to start rather
// than run with a lane that silently dead-letters every message.
func TestMissingInboxes_ReportsUnmountedInboxes(t *testing.T) {
	partial := httpadapter.NewHandler(httpadapter.Deps{
		CoursePublishedPushHandler: httpadapter.NewCoursePublishedPushHandler(httpadapter.CoursePublishedPushDeps{
			Registry: discovery.NewRegistry(),
			Verifier: eventpush.NewVerifier(eventpush.VerifierConfig{}),
		}),
	})

	missing := partial.MissingInboxes()
	if len(missing) != 2 {
		t.Fatalf("MissingInboxes() = %v, want the 2 unwired inboxes — a Handler that cannot "+
			"serve an inbox the gateway routes to it MUST say so at boot", missing)
	}

	full := httpadapter.NewHandler(fullyWiredDeps(t))
	if got := full.MissingInboxes(); len(got) != 0 {
		t.Fatalf("MissingInboxes() = %v on a fully-wired Handler, want none", got)
	}
}
