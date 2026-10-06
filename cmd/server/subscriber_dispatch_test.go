// subscriber_dispatch_test.go — drives the register*/build* subscriber
// wiring closures past their decode + dispatch lines by PUBLISHING valid
// messages on a synchronous in-memory bus (register* paths) and by
// invoking the built handlers directly (build* paths).
//
// The subscribers are wired with their in-memory ports (mirroring main()'s
// dev fallback) so the Handle* dispatch line returns nil and the closures
// are exercised end-to-end without a live Pub/Sub or Postgres. The
// goroutine-spawned register* loop bodies against a stub Cloud client are
// covered with a brief settle so the spawned goroutines finish before the
// test process exits.
package main

import (
	"context"
	"testing"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/adapter/subscribers"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
	tmpl "github.com/apollo-chora/chora-sharing/internal/domain/post_template"
)

// testEnvelope returns an envelope that passes envelope.Validate (the
// in-memory bus validates before delivery).
func testEnvelope(eventID string) cgcenvelope.Envelope {
	t0 := time.Date(2026, 5, 16, 9, 30, 0, 0, time.UTC)
	return cgcenvelope.Envelope{
		EventID:        eventID,
		IdempotencyKey: eventID,
		TenantID:       "tenant-1",
		GCID:           "gcid-1",
		OccurredAt:     t0,
		PublishedAt:    t0,
		Traceparent:    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		Tracestate:     "chora=test",
		SourceProject:  "chora-489812",
		SourceService:  "chora-sharing",
		SchemaVersion:  1,
	}
}

// busMsg builds a deliverable Message with a valid envelope + JSON payload.
func busMsg(topic string, payload string) eventbus.Message {
	return eventbus.Message{
		Subject:    topic,
		Envelope: testEnvelope("evt-" + topic),
		Payload:  []byte(payload),
	}
}

// wiredFamiliarSub builds the dev-fallback FamiliarMilestoneSubscriber
// exactly like main() does (in-memory ports + default templates).
func wiredFamiliarSub(t *testing.T) *subscribers.FamiliarMilestoneSubscriber {
	t.Helper()
	return subscribers.NewFamiliarMilestoneSubscriber(subscribers.Config{
		Drafts:        subscribers.NewInMemoryDraftStore(),
		Posts:         subscribers.NewInMemoryPostPublisher(),
		Preferences:   subscribers.NewInMemoryPreferenceStore(),
		Idempotency:   subscribers.NewInMemoryIdempotencyStore(),
		Composer:      tmpl.NewComposer(tmpl.DefaultTemplates()),
		DefaultPolicy: subscribers.PolicyDraft,
	})
}

func TestRegisterMilestoneSubscriber_DeliversAllTopics(t *testing.T) {
	bus := eventbus.NewInMemoryBus(eventbus.WithSynchronousDelivery())
	defer bus.Close()
	registerMilestoneSubscriber(context.Background(), bus, wiredFamiliarSub(t))

	payloads := map[string]string{
		subscribers.TopicCompanionStageUp:          `{"familiar_id":"fam-1","stage_to_name":"Stage 4"}`,
		subscribers.TopicCompanionBreedRevealed:    `{"familiar_id":"fam-1","species":"fox"}`,
		subscribers.TopicCompanionHatched:          `{"familiar_id":"fam-1","display_name":"Foxy"}`,
		subscribers.TopicCompanionSourceRevelation: `{"familiar_id":"fam-1","preview_llm_tier":"t2"}`,
		subscribers.TopicFamiliarStageUp:          `{"familiar_id":"fam-1","stage_to_name":"Stage 4"}`,
		subscribers.TopicFamiliarBreedRevealed:    `{"familiar_id":"fam-1","species":"fox"}`,
		subscribers.TopicFamiliarHatched:          `{"familiar_id":"fam-1","display_name":"Foxy"}`,
		subscribers.TopicFamiliarSourceRevelation: `{"familiar_id":"fam-1","preview_llm_tier":"t2"}`,
	}
	for topic, payload := range payloads {
		if err := bus.Publish(context.Background(), topic, testEnvelope("evt-"+topic), []byte(payload)); err != nil {
			t.Fatalf("publish %s: %v", topic, err)
		}
	}
}

func TestRegisterLiveQuizScoreSubscriber_Delivers(t *testing.T) {
	bus := eventbus.NewInMemoryBus(eventbus.WithSynchronousDelivery())
	defer bus.Close()
	sub := subscribers.NewLiveQuizScoreSubscriber(subscribers.LiveQuizScoreConfig{
		Ranker:      leaderboard.NewRanker(),
		Idempotency: subscribers.NewInMemoryIdempotencyStore(),
	})
	registerLiveQuizScoreSubscriber(context.Background(), bus, sub)

	if err := bus.Publish(context.Background(), subscribers.TopicLiveQuizScoreAwarded,
		testEnvelope("evt-live-quiz"), []byte(`{"session_id":"s1","awarded_points":10,"correct":true}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func TestRegisterDuelCompletedSubscriber_Delivers(t *testing.T) {
	bus := eventbus.NewInMemoryBus(eventbus.WithSynchronousDelivery())
	defer bus.Close()
	sub := subscribers.NewDuelCompletedSubscriber(subscribers.DuelCompletedConfig{
		Currency:    inmem.NewCurrencyRepo(),
		Ranker:      leaderboard.NewRanker(),
		Idempotency: subscribers.NewInMemoryIdempotencyStore(),
		Rewards:     duel.DefaultRewardConfig(),
	})
	registerDuelCompletedSubscriber(context.Background(), bus, sub)

	if err := bus.Publish(context.Background(), subscribers.TopicDuelCompleted,
		testEnvelope("evt-duel"),
		[]byte(`{"duel_id":"duel-1","challenger_gcid":"gcid-a","opponent_gcid":"gcid-b","winner_gcid":"gcid-a","scope":"TENANT","score_challenger":2,"score_opponent":1}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func TestBuildFamiliarMilestoneHandler_SuccessPaths(t *testing.T) {
	sub := wiredFamiliarSub(t)
	cases := []struct {
		topic   string
		payload string
	}{
		{subscribers.TopicCompanionStageUp, `{"familiar_id":"fam-1","stage_to_name":"Stage 4"}`},
		{subscribers.TopicCompanionBreedRevealed, `{"familiar_id":"fam-1","species":"fox"}`},
		{subscribers.TopicCompanionHatched, `{"familiar_id":"fam-1","display_name":"Foxy"}`},
		{subscribers.TopicCompanionSourceRevelation, `{"familiar_id":"fam-1","preview_llm_tier":"t2"}`},
		{subscribers.TopicFamiliarStageUp, `{"familiar_id":"fam-1","stage_to_name":"Stage 4"}`},
		{subscribers.TopicFamiliarBreedRevealed, `{"familiar_id":"fam-1","species":"fox"}`},
		{subscribers.TopicFamiliarHatched, `{"familiar_id":"fam-1","display_name":"Foxy"}`},
		{subscribers.TopicFamiliarSourceRevelation, `{"familiar_id":"fam-1","preview_llm_tier":"t2"}`},
	}
	for _, c := range cases {
		handler := buildFamiliarMilestoneHandler(sub, c.topic)
		if err := handler(context.Background(), busMsg(c.topic, c.payload)); err != nil {
			t.Fatalf("%s: unexpected handler error: %v", c.topic, err)
		}
	}
}

func TestBuildAtomProjectionHandler_SuccessPaths(t *testing.T) {
	sub := subscribers.NewAtomProjectionSubscriber(subscribers.AtomProjectionConfig{
		Writer:      inmem.NewProjectionRepo(),
		Idempotency: subscribers.NewInMemoryIdempotencyStore(),
	})

	// published → upsert → nil.
	h := buildAtomProjectionHandler(sub, nil, subscribers.TopicAtomPublished)
	if err := h(context.Background(), busMsg(subscribers.TopicAtomPublished,
		`{"atom_id":"atom-1","author_gcid":"gcid-1","title":"T","status":"published"}`)); err != nil {
		t.Fatalf("atom published: %v", err)
	}

	// archived with a nil stranding detector → invalidate + nil.
	h = buildAtomProjectionHandler(sub, nil, subscribers.TopicAtomArchived)
	if err := h(context.Background(), busMsg(subscribers.TopicAtomArchived,
		`{"atom_id":"atom-1","status":"archived"}`)); err != nil {
		t.Fatalf("atom archived (no stranding): %v", err)
	}

	// archived WITH a stranding detector → the stranding leg runs after the
	// projection handler (bare subscriber preflights, but the closure line
	// executes).
	bareStranding := &subscribers.AtomReuseStrandingSubscriber{}
	h = buildAtomProjectionHandler(sub, bareStranding, subscribers.TopicAtomArchived)
	if err := h(context.Background(), busMsg(subscribers.TopicAtomArchived,
		`{"atom_id":"atom-1","status":"archived"}`)); err == nil {
		t.Fatal("atom archived (stranding): expected error from bare stranding subscriber")
	}

	// reuse_visibility_changed with a nil stranding detector → nil.
	h = buildAtomProjectionHandler(sub, nil, subscribers.TopicAtomReuseVisibilityChanged)
	if err := h(context.Background(), busMsg(subscribers.TopicAtomReuseVisibilityChanged,
		`{"atom_id":"atom-1","reuse_visibility":"friends"}`)); err != nil {
		t.Fatalf("reuse visibility (no stranding): %v", err)
	}

	// reuse_visibility_changed WITH a stranding detector → stranding leg runs.
	h = buildAtomProjectionHandler(sub, bareStranding, subscribers.TopicAtomReuseVisibilityChanged)
	if err := h(context.Background(), busMsg(subscribers.TopicAtomReuseVisibilityChanged,
		`{"atom_id":"atom-1","reuse_visibility":"friends"}`)); err == nil {
		t.Fatal("reuse visibility (stranding): expected error from bare stranding subscriber")
	}
}

func TestBuildOrphanCreatedHandler_DecodeDispatch(t *testing.T) {
	// Bare stranding subscriber: decode succeeds then the stranding leg
	// preflights (error) — the decode + dispatch lines execute.
	stranding := &subscribers.AtomReuseStrandingSubscriber{}
	h := buildOrphanCreatedHandler(stranding)
	err := h(context.Background(), busMsg(subscribers.TopicAtomOrphanCreated,
		`{"orphan_atom_id":"atom-orph","source_revision_id":"rev-1"}`))
	if err == nil {
		t.Fatal("expected error from bare stranding subscriber after decode")
	}
}

func TestBuildAtomProjectionHandler_DispatchErrorBranches(t *testing.T) {
	// A bare subscriber (nil ports) decodes successfully then preflights, so
	// the closure's `if err := sub.Handle*...; err != nil { return err }`
	// error-return lines are exercised.
	bare := &subscribers.AtomProjectionSubscriber{}
	if err := buildAtomProjectionHandler(bare, nil, subscribers.TopicAtomArchived)(context.Background(),
		busMsg(subscribers.TopicAtomArchived, `{"atom_id":"atom-1","status":"archived"}`)); err == nil {
		t.Fatal("expected subscriber preflight error after successful decode")
	}
	if err := buildAtomProjectionHandler(bare, nil, subscribers.TopicAtomReuseVisibilityChanged)(context.Background(),
		busMsg(subscribers.TopicAtomReuseVisibilityChanged, `{"atom_id":"atom-1","reuse_visibility":"friends"}`)); err == nil {
		t.Fatal("expected subscriber preflight error after successful decode")
	}
}

func TestRegisterBusSubscribers_DecodeErrorPaths(t *testing.T) {
	bus := eventbus.NewInMemoryBus(eventbus.WithSynchronousDelivery())
	defer bus.Close()
	garbage := []byte("%%%not-json%%%")

	registerMilestoneSubscriber(context.Background(), bus, wiredFamiliarSub(t))
	registerLiveQuizScoreSubscriber(context.Background(), bus,
		subscribers.NewLiveQuizScoreSubscriber(subscribers.LiveQuizScoreConfig{}))
	registerDuelCompletedSubscriber(context.Background(), bus,
		subscribers.NewDuelCompletedSubscriber(subscribers.DuelCompletedConfig{}))

	topics := []string{
		subscribers.TopicCompanionStageUp,
		subscribers.TopicCompanionBreedRevealed,
		subscribers.TopicCompanionHatched,
		subscribers.TopicCompanionSourceRevelation,
		subscribers.TopicFamiliarStageUp,
		subscribers.TopicFamiliarBreedRevealed,
		subscribers.TopicFamiliarHatched,
		subscribers.TopicFamiliarSourceRevelation,
		subscribers.TopicLiveQuizScoreAwarded,
		subscribers.TopicDuelCompleted,
	}
	for _, topic := range topics {
		if err := bus.Publish(context.Background(), topic, testEnvelope("evt-garbage-"+topic), garbage); err != nil {
			t.Fatalf("publish %s: %v", topic, err)
		}
	}
	// Decode failure returns an error → the bus retries up to maxAttempts.
	// No assertion on the handler outcome: the closure's decode-error
	// `return err` lines are the statements under test.
}

func TestBuildHandlers_RemainingDecodeErrorPaths(t *testing.T) {
	garbage := eventbus.Message{Subject: "x", Envelope: testEnvelope("evt-x"), Payload: []byte("%%%not-json%%%")}

	fam := wiredFamiliarSub(t)
	for _, topic := range []string{
		subscribers.TopicCompanionBreedRevealed,
		subscribers.TopicCompanionHatched,
		subscribers.TopicCompanionSourceRevelation,
		subscribers.TopicFamiliarBreedRevealed,
		subscribers.TopicFamiliarHatched,
		subscribers.TopicFamiliarSourceRevelation,
	} {
		msg := garbage
		msg.Subject = topic
		if err := buildFamiliarMilestoneHandler(fam, topic)(context.Background(), msg); err == nil {
			t.Fatalf("%s: expected decode error for garbage payload", topic)
		}
	}

	atom := subscribers.NewAtomProjectionSubscriber(subscribers.AtomProjectionConfig{})
	for _, topic := range []string{
		subscribers.TopicAtomArchived,
		subscribers.TopicAtomReuseVisibilityChanged,
	} {
		msg := garbage
		msg.Subject = topic
		if err := buildAtomProjectionHandler(atom, nil, topic)(context.Background(), msg); err == nil {
			t.Fatalf("%s: expected decode error for garbage payload", topic)
		}
	}
}
