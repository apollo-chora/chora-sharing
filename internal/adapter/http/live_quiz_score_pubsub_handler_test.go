// live_quiz_score_pubsub_handler_test.go — ADR-168 Task #8 (chora-sharing half).
//
// These tests drive the handler directly, which is fine for BEHAVIOUR — but on
// its own that is exactly what let CHO-2195 ship: a green handler test proves the
// handler works and proves NOTHING about whether Pub/Sub can reach it. The mount
// is asserted separately, through the service mux, in pubsub_inbox_contract_test.go.
package httpadapter_test

import (
	"net/http"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/apollo-chora/chora-sharing/internal/adapter/eventpush"

	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	deliveryv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/delivery/v1"

	httpapi "github.com/apollo-chora/chora-sharing/internal/adapter/http"
	"github.com/apollo-chora/chora-sharing/internal/adapter/subscribers"
	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
)

func newLiveQuizScorePushHandler() (http.Handler, *leaderboard.Ranker) {
	ranker := leaderboard.NewRanker()
	sub := subscribers.NewLiveQuizScoreSubscriber(subscribers.LiveQuizScoreConfig{
		Ranker:      ranker,
		Idempotency: subscribers.NewInMemoryIdempotencyStore(),
	})
	h := httpapi.NewLiveQuizScorePushHandler(httpapi.LiveQuizScorePushDeps{
		Subscriber: sub,
		Verifier:   eventpush.NewVerifier(eventpush.VerifierConfig{}),
	})
	return h, ranker
}

func scoreAwardedPayload(eventID string, points int) map[string]any {
	return map[string]any{
		"event_id":         eventID,
		"tenant_id":        "tnt-1",
		"gcid":             "gcid-1",
		"session_id":       "sess-1",
		"live_quiz_id":     "lq-1",
		"question_id":      "q-1",
		"awarded_points":   points,
		"cumulative_score": points,
		"correct":          true,
		"answer_millis":    1200,
	}
}

// The happy path: a score_awarded push credits the tenant leaderboard.
func TestLiveQuizScorePushHandler_CreditsLeaderboard(t *testing.T) {
	h, ranker := newLiveQuizScorePushHandler()

	body := pushBody(t, subscribers.TopicLiveQuizScoreAwarded, scoreAwardedPayload("evt-lq-1", 40))
	rec := dispatchSharing(t, h, "/api/internal/pubsub/live-quiz-scores", body)

	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
	tenantScope := leaderboard.Scope{Kind: leaderboard.ScopeTenant}
	top := ranker.TopByPeriod(tenantScope, leaderboard.PeriodWeekly, "tnt-1", 10)
	if len(top) != 1 || top[0].GCID != "gcid-1" || top[0].Score != 40 {
		t.Fatalf("weekly top = %+v, want one entry gcid-1 score 40", top)
	}
}

// Pub/Sub is at-least-once. A redelivered event must not double-credit.
func TestLiveQuizScorePushHandler_DuplicateEvent_AcksWithoutDoubleCredit(t *testing.T) {
	h, ranker := newLiveQuizScorePushHandler()
	body := pushBody(t, subscribers.TopicLiveQuizScoreAwarded, scoreAwardedPayload("evt-lq-dup", 25))

	for i := 0; i < 2; i++ {
		if rec := dispatchSharing(t, h, "/api/internal/pubsub/live-quiz-scores", body); rec.Code != http.StatusOK {
			t.Fatalf("delivery %d: code=%d body=%s", i+1, rec.Code, rec.Body.String())
		}
	}

	tenantScope := leaderboard.Scope{Kind: leaderboard.ScopeTenant}
	top := ranker.TopByPeriod(tenantScope, leaderboard.PeriodWeekly, "tnt-1", 10)
	if len(top) != 1 || top[0].Score != 25 {
		t.Fatalf("weekly top = %+v, want score 25 exactly once — a redelivery double-credited", top)
	}
}

// A message this handler cannot process must NACK, never ACK.
//
// A 200 tells Pub/Sub the event was handled and DISCARDS IT PERMANENTLY. A 5xx
// costs five retries and then parks the message on
// chora.dlq.delivery.live_quiz_session.score_awarded.v1, where an operator can see
// it and replay it. Recoverable and visible beats silent and gone — the sibling
// weakness-grown handler shipped with an ACK here, and its test asserted the ACK.
func TestLiveQuizScorePushHandler_UnexpectedTopic_Nacks(t *testing.T) {
	h, ranker := newLiveQuizScorePushHandler()

	body := pushBody(t, "chora.unknown.thing.happened.v1", scoreAwardedPayload("evt-lq-x", 99))
	rec := dispatchSharing(t, h, "/api/internal/pubsub/live-quiz-scores", body)

	if rec.Code == http.StatusOK {
		t.Fatalf("an unexpected topic was ACKed (200) — the event is now destroyed. "+
			"It must NACK so Pub/Sub retries and dead-letters it. body=%s", rec.Body.String())
	}
	if rec.Code < 500 {
		t.Errorf("code = %d, want 5xx (a NACK)", rec.Code)
	}
	tenantScope := leaderboard.Scope{Kind: leaderboard.ScopeTenant}
	if top := ranker.TopByPeriod(tenantScope, leaderboard.PeriodWeekly, "tnt-1", 10); len(top) != 0 {
		t.Fatalf("expected zero dispatch, got %+v", top)
	}
}

// THE ONE THAT MATTERS: the route must decode the bytes chora-delivery ACTUALLY
// PUBLISHES.
//
// chora-delivery marshals this topic as BINARY PROTOBUF (its protomarshal switch,
// for the Pub/Sub Schema Registry's BINARY encoding mode) — not JSON. Every other
// test in this file feeds hand-built JSON, which travels the protodecode JSON
// FALLBACK path. A green JSON test therefore proves nothing about production: if
// chora-sharing had no binary decoder registered for this topic, json.Unmarshal
// would choke on proto bytes and every real message would 500 into the DLQ — and
// mounting the route would have converted a 404 into a 500 while looking fixed.
//
// So drive the handler with real proto.Marshal output.
func TestLiveQuizScorePushHandler_DecodesTheBinaryProtoDeliveryActuallyPublishes(t *testing.T) {
	h, ranker := newLiveQuizScorePushHandler()

	wire, err := proto.Marshal(&deliveryv1.LiveQuizSessionScoreAwarded{
		Envelope: &commonv1.EventEnvelope{
			EventId:  "evt-lq-proto-1",
			TenantId: "tnt-1",
			Gcid:     "gcid-1",
		},
		SessionId:       "sess-1",
		LiveQuizId:      "lq-1",
		QuestionId:      "q-1",
		AwardedPoints:   40,
		CumulativeScore: 40,
		Correct:         true,
	})
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}

	rec := dispatchSharing(t, h, "/api/internal/pubsub/live-quiz-scores",
		pushBodyRaw(t, subscribers.TopicLiveQuizScoreAwarded, wire))

	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s — chora-sharing could not decode the binary proto "+
			"chora-delivery publishes. The route is mounted but the lane is still dead.",
			rec.Code, rec.Body.String())
	}
	tenantScope := leaderboard.Scope{Kind: leaderboard.ScopeTenant}
	top := ranker.TopByPeriod(tenantScope, leaderboard.PeriodWeekly, "tnt-1", 10)
	if len(top) != 1 || top[0].GCID != "gcid-1" || top[0].Score != 40 {
		t.Fatalf("weekly top = %+v, want gcid-1 score 40 credited from the PROTO payload", top)
	}
}

// A nil Subscriber is a wiring error, and a push route with nothing behind it
// would 200 and discard every event. Fail at construction, loudly.
func TestNewLiveQuizScorePushHandler_NilSubscriber_Panics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("a nil Subscriber was accepted — the route would ack and destroy every event")
		}
	}()
	httpapi.NewLiveQuizScorePushHandler(httpapi.LiveQuizScorePushDeps{})
}
