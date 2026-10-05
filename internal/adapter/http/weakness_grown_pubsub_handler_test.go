// weakness_grown_pubsub_handler_test.go — ADR-196 B3 (chora-sharing half).
package httpadapter_test

import (
	"net/http"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/apollo-chora/chora-sharing/internal/adapter/eventpush"

	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	consumptionv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/consumption/v1"

	httpapi "github.com/apollo-chora/chora-sharing/internal/adapter/http"
	"github.com/apollo-chora/chora-sharing/internal/adapter/subscribers"
	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
)

func newWeaknessGrownPushSub() (*subscribers.WeaknessGrownSubscriber, *leaderboard.Ranker) {
	ranker := leaderboard.NewRanker()
	sub := subscribers.NewWeaknessGrownSubscriber(subscribers.WeaknessGrownConfig{
		Ranker:      ranker,
		Idempotency: subscribers.NewInMemoryIdempotencyStore(),
	})
	return sub, ranker
}

func TestWeaknessGrownPushHandler_CreditsXP(t *testing.T) {
	sub, ranker := newWeaknessGrownPushSub()
	h := httpapi.NewWeaknessGrownPushHandler(httpapi.WeaknessGrownPushDeps{
		Subscriber: sub,
		Verifier:   eventpush.NewVerifier(eventpush.VerifierConfig{}),
	})
	body := pushBody(t, subscribers.TopicWeaknessGrown, map[string]any{
		"event_id":        "evt-wg-1",
		"tenant_id":       "tnt-1",
		"learner_gcid":    "gcid-1",
		"growth_edge_id":  "edge-1",
		"concept_label":   "Recursion",
		"recovery_source": "drill_atom",
		"grown_at":        "2026-06-28T10:00:00Z",
	})
	rec := dispatchSharing(t, h, "/api/internal/pubsub/weakness-grown", body)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}

	tenantScope := leaderboard.Scope{Kind: leaderboard.ScopeTenant}
	top := ranker.TopByPeriod(tenantScope, leaderboard.PeriodWeekly, "tnt-1", 10)
	if len(top) != 1 || top[0].GCID != "gcid-1" || top[0].Score != subscribers.DefaultWeaknessGrownXP {
		t.Fatalf("weekly top = %+v, want one entry gcid-1 score %d", top, subscribers.DefaultWeaknessGrownXP)
	}
}

// An unexpected topic must NACK — never ack.
//
// This test used to be named `..._AcksWithoutDispatch` and asserted a 200. The
// handler's default arm did `return nil` "so Pub/Sub doesn't infinitely retry on
// fan-out misconfiguration". Both the code and the test were wrong, and the test
// being green is what kept the code that way:
//
//   - A 200 ACKS the message. Pub/Sub then considers it handled and DISCARDS IT
//     PERMANENTLY. CHO-2195 states the principle plainly — "an ack from the wrong
//     place is WORSE than a dead-letter: it tells Pub/Sub the event was handled
//     and discards it forever."
//   - The "infinite retry" it was defending against does not exist: this
//     subscription has maxDeliveryAttempts=5 and a dead-letter topic
//     (chora.dlq.consumption.weakness.grown.v1). A NACK costs five retries and
//     then parks the message somewhere an operator can SEE it and replay it.
//
// Recoverable and visible beats silent and gone.
func TestWeaknessGrownPushHandler_UnknownTopic_NacksAndDoesNotDispatch(t *testing.T) {
	sub, ranker := newWeaknessGrownPushSub()
	h := httpapi.NewWeaknessGrownPushHandler(httpapi.WeaknessGrownPushDeps{
		Subscriber: sub,
		Verifier:   eventpush.NewVerifier(eventpush.VerifierConfig{}),
	})
	body := pushBody(t, "chora.unknown.thing.happened.v1", map[string]any{
		"event_id": "e", "tenant_id": "t", "learner_gcid": "g", "growth_edge_id": "x",
	})

	rec := dispatchSharing(t, h, "/", body)

	if rec.Code == http.StatusOK {
		t.Fatalf("an unexpected topic was ACKed (200) — Pub/Sub will treat the event as "+
			"handled and destroy it permanently. It must NACK so the message retries and "+
			"dead-letters, where it is recoverable. body=%s", rec.Body.String())
	}
	if rec.Code < 500 {
		t.Errorf("code = %d, want 5xx (a NACK that Pub/Sub retries)", rec.Code)
	}
	tenantScope := leaderboard.Scope{Kind: leaderboard.ScopeTenant}
	if top := ranker.TopByPeriod(tenantScope, leaderboard.PeriodWeekly, "t", 10); len(top) != 0 {
		t.Fatalf("expected zero dispatch, got %+v", top)
	}
}

func TestWeaknessGrownPushHandler_DuplicateGrow_Acks(t *testing.T) {
	sub, ranker := newWeaknessGrownPushSub()
	h := httpapi.NewWeaknessGrownPushHandler(httpapi.WeaknessGrownPushDeps{
		Subscriber: sub,
		Verifier:   eventpush.NewVerifier(eventpush.VerifierConfig{}),
	})
	payload := map[string]any{
		"event_id":       "evt-dup",
		"tenant_id":      "tnt-1",
		"learner_gcid":   "gcid-1",
		"growth_edge_id": "edge-1",
		"grown_at":       "2026-06-28T10:00:00Z",
	}
	first := dispatchSharing(t, h, "/", pushBody(t, subscribers.TopicWeaknessGrown, payload))
	second := dispatchSharing(t, h, "/", pushBody(t, subscribers.TopicWeaknessGrown, payload))
	if first.Code != http.StatusOK || second.Code != http.StatusOK {
		t.Fatalf("codes=%d/%d", first.Code, second.Code)
	}
	tenantScope := leaderboard.Scope{Kind: leaderboard.ScopeTenant}
	top := ranker.TopByPeriod(tenantScope, leaderboard.PeriodWeekly, "tnt-1", 10)
	if len(top) != 1 || top[0].Score != subscribers.DefaultWeaknessGrownXP {
		t.Fatalf("after duplicate weekly top = %+v, want single award %d", top, subscribers.DefaultWeaknessGrownXP)
	}
}

func TestWeaknessGrownPushHandler_NilSubscriber_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on nil subscriber")
		}
	}()
	httpapi.NewWeaknessGrownPushHandler(httpapi.WeaknessGrownPushDeps{})
}

// The route must decode the bytes chora-consumption ACTUALLY PUBLISHES.
//
// chora-consumption marshals chora.consumption.weakness.grown.v1 as BINARY
// PROTOBUF. The JSON-driven tests above only exercise protodecode's fallback
// path, so they prove nothing about the production wire. If no binary decoder
// were registered for this topic in chora-sharing, mounting the route would have
// turned a 404 into a 500 and every message would still dead-letter — a fix that
// looks like a fix and changes nothing.
func TestWeaknessGrownPushHandler_DecodesTheBinaryProtoConsumptionActuallyPublishes(t *testing.T) {
	sub, ranker := newWeaknessGrownPushSub()
	h := httpapi.NewWeaknessGrownPushHandler(httpapi.WeaknessGrownPushDeps{
		Subscriber: sub,
		Verifier:   eventpush.NewVerifier(eventpush.VerifierConfig{}),
	})

	wire, err := proto.Marshal(&consumptionv1.WeaknessGrown{
		Envelope: &commonv1.EventEnvelope{
			EventId:  "evt-wg-proto-1",
			TenantId: "tnt-1",
			Gcid:     "gcid-1",
		},
		GrowthEdgeId: "edge-1",
		ConceptLabel: "Recursion",
	})
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}

	rec := dispatchSharing(t, h, "/api/internal/pubsub/weakness-grown",
		pushBodyRaw(t, subscribers.TopicWeaknessGrown, wire))

	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s — chora-sharing could not decode the binary proto "+
			"chora-consumption publishes. The route is mounted but the lane is still dead.",
			rec.Code, rec.Body.String())
	}
	tenantScope := leaderboard.Scope{Kind: leaderboard.ScopeTenant}
	top := ranker.TopByPeriod(tenantScope, leaderboard.PeriodWeekly, "tnt-1", 10)
	if len(top) != 1 || top[0].GCID != "gcid-1" || top[0].Score != subscribers.DefaultWeaknessGrownXP {
		t.Fatalf("weekly top = %+v, want gcid-1 score %d credited from the PROTO payload",
			top, subscribers.DefaultWeaknessGrownXP)
	}
}
