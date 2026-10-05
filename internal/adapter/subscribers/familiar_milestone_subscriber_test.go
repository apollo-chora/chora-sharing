// Package subscribers_test holds the RED-phase TDD specs for the
// FamiliarMilestoneSubscriber (PROD-G).
//
// Subscriber consumes 4 chora.consumption.* events and either:
//
//   - auto-posts (per-user preference = auto)     — composes a Post + emits
//   - queue-as-draft (preference = draft, default) — inserts post_drafts row
//   - suppress (preference = suppress)             — no-op, still logged
//
// Idempotency: composite (subscriber_handler, source_event_id) — replays
// are skipped with an OTLP span event "duplicate_event_skipped".
package subscribers_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/subscribers"
	tmpl "github.com/apollo-chora/chora-sharing/internal/domain/post_template"
)

const (
	tenantA = "01970000-0000-7000-8000-0000000000aa"
	gcidA   = "01970000-0000-7000-8000-0000000000bb"
	eventA  = "01970000-0000-7000-8000-0000000000cc"
	eventB  = "01970000-0000-7000-8000-0000000000dd"
	traceA  = "00-1234567890abcdef1234567890abcdef-1234567890abcdef-01"
)

func newSubscriber(t *testing.T) (*subscribers.FamiliarMilestoneSubscriber, *subscribers.InMemoryDraftStore, *subscribers.InMemoryPostPublisher, *subscribers.InMemoryPreferenceStore, *subscribers.InMemoryIdempotencyStore) {
	t.Helper()
	drafts := subscribers.NewInMemoryDraftStore()
	posts := subscribers.NewInMemoryPostPublisher()
	prefs := subscribers.NewInMemoryPreferenceStore()
	idem := subscribers.NewInMemoryIdempotencyStore()
	composer := tmpl.NewComposer(tmpl.DefaultTemplates())
	sub := subscribers.NewFamiliarMilestoneSubscriber(subscribers.Config{
		Drafts:        drafts,
		Posts:         posts,
		Preferences:   prefs,
		Idempotency:   idem,
		Composer:      composer,
		DefaultPolicy: subscribers.PolicyDraft,
	})
	return sub, drafts, posts, prefs, idem
}

// -----------------------------------------------------------------------------
// HandleStageUp
// -----------------------------------------------------------------------------

func TestStageUp_DraftPolicy_QueuesDraft(t *testing.T) {
	t.Parallel()
	sub, drafts, posts, _, _ := newSubscriber(t)
	in := subscribers.StageUpEnvelope{
		EventID:             eventA,
		TenantID:            tenantA,
		OwnerGCID:           gcidA,
		FamiliarID:          "01970000-0000-7000-8000-000000000111",
		FamiliarDisplayName: "Eira",
		StageFromName:       "Awakened",
		StageToName:         "Teen",
		NewlyUnlockedTools:  []string{"query_kg"},
		NewLLMTier:          "flash",
	}
	if err := sub.HandleStageUp(context.Background(), in); err != nil {
		t.Fatalf("HandleStageUp: %v", err)
	}
	if len(drafts.All()) != 1 {
		t.Fatalf("expected 1 draft, got %d", len(drafts.All()))
	}
	if len(posts.All()) != 0 {
		t.Fatalf("expected 0 auto-posts under draft policy, got %d", len(posts.All()))
	}
	if !strings.Contains(drafts.All()[0].Body, "Eira") {
		t.Fatalf("draft body must reference Familiar name; got %q", drafts.All()[0].Body)
	}
	// Was "Teen Dragon" — breed_adjective concatenated onto the stage name. The
	// adjective is on no proto message, so in production this rendered
	// "reached fledgling Companion!" from a fallback (CHO-2259).
	if !strings.Contains(drafts.All()[0].Body, "reached Teen") {
		t.Fatalf("draft body must name the stage the wire carried; got %q", drafts.All()[0].Body)
	}
}

func TestStageUp_AutoPolicy_ComposesPostDirectly(t *testing.T) {
	t.Parallel()
	sub, drafts, posts, prefs, _ := newSubscriber(t)
	prefs.Set(tenantA, gcidA, subscribers.PolicyAuto)

	in := subscribers.StageUpEnvelope{
		EventID:             eventA,
		TenantID:            tenantA,
		OwnerGCID:           gcidA,
		FamiliarID:          "01970000-0000-7000-8000-000000000111",
		FamiliarDisplayName: "Eira",
		StageFromName:       "Awakened",
		StageToName:         "Teen",
	}
	if err := sub.HandleStageUp(context.Background(), in); err != nil {
		t.Fatalf("HandleStageUp: %v", err)
	}
	if len(drafts.All()) != 0 {
		t.Fatalf("expected 0 drafts under auto policy, got %d", len(drafts.All()))
	}
	if len(posts.All()) != 1 {
		t.Fatalf("expected 1 auto-post, got %d", len(posts.All()))
	}
	if posts.All()[0].TenantID != tenantA {
		t.Fatalf("post tenant: %q", posts.All()[0].TenantID)
	}
}

func TestStageUp_SuppressPolicy_NoSideEffect(t *testing.T) {
	t.Parallel()
	sub, drafts, posts, prefs, _ := newSubscriber(t)
	prefs.Set(tenantA, gcidA, subscribers.PolicySuppress)

	if err := sub.HandleStageUp(context.Background(), subscribers.StageUpEnvelope{
		EventID:             eventA,
		TenantID:            tenantA,
		OwnerGCID:           gcidA,
		FamiliarID:          "01970000-0000-7000-8000-000000000111",
		FamiliarDisplayName: "Eira",
		StageToName:         "Teen",
	}); err != nil {
		t.Fatalf("HandleStageUp: %v", err)
	}
	if len(drafts.All()) != 0 || len(posts.All()) != 0 {
		t.Fatalf("suppress policy must produce no draft and no post")
	}
}

func TestStageUp_Idempotent_OnReplay(t *testing.T) {
	t.Parallel()
	sub, drafts, _, _, idem := newSubscriber(t)

	in := subscribers.StageUpEnvelope{
		EventID:             eventA,
		TenantID:            tenantA,
		OwnerGCID:           gcidA,
		FamiliarID:          "01970000-0000-7000-8000-000000000111",
		FamiliarDisplayName: "Eira",
		StageToName:         "Teen",
	}
	if err := sub.HandleStageUp(context.Background(), in); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if err := sub.HandleStageUp(context.Background(), in); err != nil {
		t.Fatalf("replay must not error: %v", err)
	}
	if len(drafts.All()) != 1 {
		t.Fatalf("expected exactly 1 draft across 2 calls, got %d", len(drafts.All()))
	}
	if !idem.Recorded("familiar_milestone:stage_up", eventA) {
		t.Fatalf("expected idempotency key recorded")
	}
}

func TestStageUp_RejectsMissingTenantID(t *testing.T) {
	t.Parallel()
	sub, _, _, _, _ := newSubscriber(t)
	err := sub.HandleStageUp(context.Background(), subscribers.StageUpEnvelope{
		EventID:             eventA,
		OwnerGCID:           gcidA,
		FamiliarDisplayName: "Eira",
		StageToName:         "Teen",
	})
	if err == nil {
		t.Fatalf("expected error on missing tenant_id")
	}
}

func TestStageUp_RejectsMissingEventID(t *testing.T) {
	t.Parallel()
	sub, _, _, _, _ := newSubscriber(t)
	err := sub.HandleStageUp(context.Background(), subscribers.StageUpEnvelope{
		TenantID:            tenantA,
		OwnerGCID:           gcidA,
		FamiliarDisplayName: "Eira",
		StageToName:         "Teen",
	})
	if err == nil {
		t.Fatalf("expected error on missing event_id")
	}
}

// -----------------------------------------------------------------------------
// HandleBreedRevealed
// -----------------------------------------------------------------------------

func TestBreedRevealed_DraftPolicy_QueuesDraft(t *testing.T) {
	t.Parallel()
	sub, drafts, _, _, _ := newSubscriber(t)
	in := subscribers.BreedRevealedEnvelope{
		EventID:             eventA,
		TenantID:            tenantA,
		OwnerGCID:           gcidA,
		FamiliarID:          "01970000-0000-7000-8000-000000000111",
		FamiliarDisplayName: "Eira",
		Species:             "Dragon",
		Rarity:              "rare",
		ShinyVariant:        true,
		EggSKU:              "egg.standard.v1",
	}
	if err := sub.HandleBreedRevealed(context.Background(), in); err != nil {
		t.Fatalf("HandleBreedRevealed: %v", err)
	}
	if len(drafts.All()) != 1 {
		t.Fatalf("expected 1 draft, got %d", len(drafts.All()))
	}
	if !strings.Contains(strings.ToLower(drafts.All()[0].Body), "shiny") &&
		!strings.Contains(strings.ToLower(drafts.All()[0].Body), "shimmer") {
		t.Fatalf("breed-reveal draft must adorn shiny variants; got %q", drafts.All()[0].Body)
	}
}

func TestBreedRevealed_DefaultsRarityToCommon(t *testing.T) {
	t.Parallel()
	sub, drafts, _, _, _ := newSubscriber(t)
	in := subscribers.BreedRevealedEnvelope{
		EventID:             eventA,
		TenantID:            tenantA,
		OwnerGCID:           gcidA,
		FamiliarID:          "01970000-0000-7000-8000-000000000111",
		FamiliarDisplayName: "Eira",
		Species:             "Dragon",
		// Rarity intentionally empty.
	}
	if err := sub.HandleBreedRevealed(context.Background(), in); err != nil {
		t.Fatalf("HandleBreedRevealed: %v", err)
	}
	if len(drafts.All()) != 1 {
		t.Fatalf("expected 1 draft, got %d", len(drafts.All()))
	}
}

// -----------------------------------------------------------------------------
// HandleHatched
// -----------------------------------------------------------------------------

func TestHatched_DraftPolicy_QueuesDraft(t *testing.T) {
	t.Parallel()
	sub, drafts, _, _, _ := newSubscriber(t)
	in := subscribers.HatchedEnvelope{
		EventID:        eventA,
		TenantID:       tenantA,
		OwnerGCID:      gcidA,
		FamiliarID:     "01970000-0000-7000-8000-000000000111",
		DisplayName:    "Eira",
		Species:        "Dragon",
		ShinyVariant:   false,
		ResonantAtomID: "01970000-0000-7000-8000-000000000222",
		Specialization: "math",
		Tone:           "socratic",
		LearnerPersona: "curious-explorer",
	}
	if err := sub.HandleHatched(context.Background(), in); err != nil {
		t.Fatalf("HandleHatched: %v", err)
	}
	if len(drafts.All()) != 1 {
		t.Fatalf("expected 1 draft, got %d", len(drafts.All()))
	}
}

// -----------------------------------------------------------------------------
// HandleSourceRevelation
// -----------------------------------------------------------------------------

func TestSourceRevelation_DraftPolicy_QueuesCeremonyDraft(t *testing.T) {
	t.Parallel()
	sub, drafts, _, _, _ := newSubscriber(t)
	in := subscribers.SourceRevelationEnvelope{
		EventID:             eventA,
		TenantID:            tenantA,
		OwnerGCID:           gcidA,
		FamiliarID:          "01970000-0000-7000-8000-000000000111",
		FamiliarDisplayName: "Eira",
		PreviewLLMTier:      "pro",
	}
	if err := sub.HandleSourceRevelation(context.Background(), in); err != nil {
		t.Fatalf("HandleSourceRevelation: %v", err)
	}
	if len(drafts.All()) != 1 {
		t.Fatalf("expected 1 draft, got %d", len(drafts.All()))
	}
	// The §4.3 breed-themed imagery is gone with the fields that never existed:
	// this handler could not render AT ALL in production (Stage6Adjective
	// required ⇒ 100% NACK). Spot-check what the wire can actually compose.
	body := drafts.All()[0].Body
	for _, phrase := range []string{"paused", "glimpsed"} {
		if !strings.Contains(body, phrase) {
			t.Fatalf("source_revelation draft must include the ceremony cue %q; got %q", phrase, body)
		}
	}
}

// -----------------------------------------------------------------------------
// Subscriber wiring
// -----------------------------------------------------------------------------

func TestSubscribedTopics_AllFour(t *testing.T) {
	t.Parallel()
	sub, _, _, _, _ := newSubscriber(t)
	topics := sub.SubscribedTopics()
	if len(topics) != 4 {
		t.Fatalf("expected 4 topics; got %d (%v)", len(topics), topics)
	}
	wantSet := map[string]bool{
		subscribers.TopicFamiliarStageUp:          true,
		subscribers.TopicFamiliarBreedRevealed:    true,
		subscribers.TopicFamiliarHatched:          true,
		subscribers.TopicFamiliarSourceRevelation: true,
	}
	for _, tp := range topics {
		if !wantSet[tp] {
			t.Fatalf("unexpected topic %q", tp)
		}
	}
}

func TestConfig_NilPiecesReturnsError(t *testing.T) {
	t.Parallel()
	composer := tmpl.NewComposer(tmpl.DefaultTemplates())
	sub := subscribers.NewFamiliarMilestoneSubscriber(subscribers.Config{
		Composer: composer,
	})
	err := sub.HandleStageUp(context.Background(), subscribers.StageUpEnvelope{
		EventID:             eventA,
		TenantID:            tenantA,
		OwnerGCID:           gcidA,
		FamiliarDisplayName: "Eira",
		StageToName:         "Teen",
	})
	if err == nil {
		t.Fatalf("subscriber missing dependencies must fail closed")
	}
}

func TestDecodeStageUpJSON_ParsesContract(t *testing.T) {
	t.Parallel()
	blob := []byte(`{
		"familiar_id": "01970000-0000-7000-8000-000000000111",
		"owner_gcid": "` + gcidA + `",
		"tenant_id": "` + tenantA + `",
		"event_id": "` + eventB + `",
		"stage_from": 5,
		"stage_to": 6,
		"stage_from_name": "Teen",
		"stage_to_name": "Matured",
		"newly_unlocked_tools": ["propose_kg_merge"],
		"newly_revealed_kg_neighbors": [],
		"new_llm_tier": "pro",
		"new_memory_mode": "full-procedural"
	}`)
	got, err := subscribers.DecodeStageUpWithAttrs(blob, nil)
	if err != nil {
		t.Fatalf("DecodeStageUpWithAttrs: %v", err)
	}
	if got.StageToName != "Matured" {
		t.Fatalf("stage_to_name: %q", got.StageToName)
	}
	if got.NewLLMTier != "pro" {
		t.Fatalf("new_llm_tier: %q", got.NewLLMTier)
	}
}

func TestPolicy_Validate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in string
		ok bool
	}{
		{"auto", true},
		{"draft", true},
		{"suppress", true},
		{"unknown", false},
		{"", false},
	}
	for _, c := range cases {
		_, err := subscribers.ParsePolicy(c.in)
		if c.ok && err != nil {
			t.Fatalf("ParsePolicy(%q) unexpected err: %v", c.in, err)
		}
		if !c.ok && err == nil {
			t.Fatalf("ParsePolicy(%q) expected err; got nil", c.in)
		}
	}
}

// -----------------------------------------------------------------------------
// Suppress + auto paths for all four handlers (coverage)
// -----------------------------------------------------------------------------

func TestBreedRevealed_AutoPolicy_ComposesPostDirectly(t *testing.T) {
	t.Parallel()
	sub, drafts, posts, prefs, _ := newSubscriber(t)
	prefs.Set(tenantA, gcidA, subscribers.PolicyAuto)
	in := subscribers.BreedRevealedEnvelope{
		EventID: eventA, TenantID: tenantA, OwnerGCID: gcidA,
		FamiliarID:          "01970000-0000-7000-8000-000000000111",
		FamiliarDisplayName: "Eira", Species: "Dragon", Rarity: "rare",
	}
	if err := sub.HandleBreedRevealed(context.Background(), in); err != nil {
		t.Fatalf("HandleBreedRevealed: %v", err)
	}
	if len(posts.All()) != 1 {
		t.Fatalf("expected 1 auto-post, got %d", len(posts.All()))
	}
	if len(drafts.All()) != 0 {
		t.Fatalf("expected 0 drafts, got %d", len(drafts.All()))
	}
}

func TestBreedRevealed_Suppress_NoSideEffect(t *testing.T) {
	t.Parallel()
	sub, drafts, posts, prefs, _ := newSubscriber(t)
	prefs.Set(tenantA, gcidA, subscribers.PolicySuppress)
	if err := sub.HandleBreedRevealed(context.Background(), subscribers.BreedRevealedEnvelope{
		EventID: eventA, TenantID: tenantA, OwnerGCID: gcidA,
		FamiliarID:          "01970000-0000-7000-8000-000000000111",
		FamiliarDisplayName: "Eira", Species: "Dragon",
	}); err != nil {
		t.Fatalf("HandleBreedRevealed: %v", err)
	}
	if len(posts.All()) != 0 || len(drafts.All()) != 0 {
		t.Fatalf("suppress must produce no side effect")
	}
}

func TestBreedRevealed_Idempotent_OnReplay(t *testing.T) {
	t.Parallel()
	sub, drafts, _, _, idem := newSubscriber(t)
	in := subscribers.BreedRevealedEnvelope{
		EventID: eventA, TenantID: tenantA, OwnerGCID: gcidA,
		FamiliarID:          "01970000-0000-7000-8000-000000000111",
		FamiliarDisplayName: "Eira", Species: "Dragon",
	}
	if err := sub.HandleBreedRevealed(context.Background(), in); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := sub.HandleBreedRevealed(context.Background(), in); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(drafts.All()) != 1 {
		t.Fatalf("expected 1 draft; got %d", len(drafts.All()))
	}
	if !idem.Recorded("familiar_milestone:breed_revealed", eventA) {
		t.Fatalf("idempotency not recorded")
	}
}

func TestHatched_Suppress_NoSideEffect(t *testing.T) {
	t.Parallel()
	sub, drafts, posts, prefs, _ := newSubscriber(t)
	prefs.Set(tenantA, gcidA, subscribers.PolicySuppress)
	if err := sub.HandleHatched(context.Background(), subscribers.HatchedEnvelope{
		EventID: eventA, TenantID: tenantA, OwnerGCID: gcidA,
		FamiliarID:  "01970000-0000-7000-8000-000000000111",
		DisplayName: "Eira", Species: "Dragon",
	}); err != nil {
		t.Fatalf("HandleHatched: %v", err)
	}
	if len(drafts.All()) != 0 || len(posts.All()) != 0 {
		t.Fatalf("suppress must produce no side effect")
	}
}

func TestHatched_Idempotent_OnReplay(t *testing.T) {
	t.Parallel()
	sub, drafts, _, _, idem := newSubscriber(t)
	in := subscribers.HatchedEnvelope{
		EventID: eventA, TenantID: tenantA, OwnerGCID: gcidA,
		FamiliarID:  "01970000-0000-7000-8000-000000000111",
		DisplayName: "Eira", Species: "Dragon",
	}
	if err := sub.HandleHatched(context.Background(), in); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := sub.HandleHatched(context.Background(), in); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(drafts.All()) != 1 {
		t.Fatalf("expected 1 draft on replay; got %d", len(drafts.All()))
	}
	if !idem.Recorded("familiar_milestone:hatched", eventA) {
		t.Fatalf("idempotency not recorded")
	}
}

func TestHatched_AutoPolicy_ComposesPostDirectly(t *testing.T) {
	t.Parallel()
	sub, _, posts, prefs, _ := newSubscriber(t)
	prefs.Set(tenantA, gcidA, subscribers.PolicyAuto)
	if err := sub.HandleHatched(context.Background(), subscribers.HatchedEnvelope{
		EventID: eventA, TenantID: tenantA, OwnerGCID: gcidA,
		FamiliarID:  "01970000-0000-7000-8000-000000000111",
		DisplayName: "Eira", Species: "Dragon",
	}); err != nil {
		t.Fatalf("HandleHatched: %v", err)
	}
	if len(posts.All()) != 1 {
		t.Fatalf("expected 1 auto-post; got %d", len(posts.All()))
	}
}

func TestSourceRevelation_AutoPolicy_ComposesPostDirectly(t *testing.T) {
	t.Parallel()
	sub, _, posts, prefs, _ := newSubscriber(t)
	prefs.Set(tenantA, gcidA, subscribers.PolicyAuto)
	in := subscribers.SourceRevelationEnvelope{
		EventID: eventA, TenantID: tenantA, OwnerGCID: gcidA,
		FamiliarID:          "01970000-0000-7000-8000-000000000111",
		FamiliarDisplayName: "Eira",
	}
	if err := sub.HandleSourceRevelation(context.Background(), in); err != nil {
		t.Fatalf("HandleSourceRevelation: %v", err)
	}
	if len(posts.All()) != 1 {
		t.Fatalf("expected 1 auto-post; got %d", len(posts.All()))
	}
}

func TestSourceRevelation_Suppress_NoSideEffect(t *testing.T) {
	t.Parallel()
	sub, drafts, posts, prefs, _ := newSubscriber(t)
	prefs.Set(tenantA, gcidA, subscribers.PolicySuppress)
	in := subscribers.SourceRevelationEnvelope{
		EventID: eventA, TenantID: tenantA, OwnerGCID: gcidA,
		FamiliarID:          "01970000-0000-7000-8000-000000000111",
		FamiliarDisplayName: "Eira",
	}
	if err := sub.HandleSourceRevelation(context.Background(), in); err != nil {
		t.Fatalf("HandleSourceRevelation: %v", err)
	}
	if len(drafts.All()) != 0 || len(posts.All()) != 0 {
		t.Fatalf("suppress must produce no side effect")
	}
}

func TestSourceRevelation_Idempotent_OnReplay(t *testing.T) {
	t.Parallel()
	sub, drafts, _, _, idem := newSubscriber(t)
	in := subscribers.SourceRevelationEnvelope{
		EventID: eventA, TenantID: tenantA, OwnerGCID: gcidA,
		FamiliarID:          "01970000-0000-7000-8000-000000000111",
		FamiliarDisplayName: "Eira",
	}
	if err := sub.HandleSourceRevelation(context.Background(), in); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := sub.HandleSourceRevelation(context.Background(), in); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(drafts.All()) != 1 {
		t.Fatalf("expected 1 draft on replay; got %d", len(drafts.All()))
	}
	if !idem.Recorded("familiar_milestone:source_revelation", eventA) {
		t.Fatalf("idempotency not recorded")
	}
}

func TestBreedRevealed_RejectsMissingTenant(t *testing.T) {
	t.Parallel()
	sub, _, _, _, _ := newSubscriber(t)
	if err := sub.HandleBreedRevealed(context.Background(), subscribers.BreedRevealedEnvelope{
		EventID: eventA, OwnerGCID: gcidA,
		FamiliarDisplayName: "Eira", Species: "Dragon",
	}); err == nil {
		t.Fatalf("expected error")
	}
}

func TestHatched_RejectsMissingFields(t *testing.T) {
	t.Parallel()
	sub, _, _, _, _ := newSubscriber(t)
	if err := sub.HandleHatched(context.Background(), subscribers.HatchedEnvelope{
		EventID: eventA, OwnerGCID: gcidA,
		Species: "Dragon",
	}); err == nil {
		t.Fatalf("expected missing tenant_id error")
	}
}

func TestSourceRevelation_RejectsMissingFields(t *testing.T) {
	t.Parallel()
	sub, _, _, _, _ := newSubscriber(t)
	if err := sub.HandleSourceRevelation(context.Background(), subscribers.SourceRevelationEnvelope{
		EventID: eventA, TenantID: tenantA,
	}); err == nil {
		t.Fatalf("expected missing owner_gcid error")
	}
}

// -----------------------------------------------------------------------------
// DraftStore in-memory port — ListPending / Get / MarkPublished / MarkDiscarded
// -----------------------------------------------------------------------------

func TestInMemoryDraftStore_ListPending(t *testing.T) {
	t.Parallel()
	sub, drafts, _, _, _ := newSubscriber(t)
	in := subscribers.StageUpEnvelope{
		EventID: eventA, TenantID: tenantA, OwnerGCID: gcidA,
		FamiliarID:          "01970000-0000-7000-8000-000000000111",
		FamiliarDisplayName: "Eira", StageToName: "Teen",
	}
	if err := sub.HandleStageUp(context.Background(), in); err != nil {
		t.Fatalf("HandleStageUp: %v", err)
	}
	got, err := drafts.ListPending(context.Background(), tenantA, gcidA)
	if err != nil {
		t.Fatalf("ListPending: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 pending draft; got %d", len(got))
	}

	// Also list for a different tenant — should be empty.
	other, err := drafts.ListPending(context.Background(), "01970000-0000-7000-8000-0000000099aa", gcidA)
	if err != nil {
		t.Fatalf("ListPending: %v", err)
	}
	if len(other) != 0 {
		t.Fatalf("ListPending must filter by tenant; got %d", len(other))
	}
}

func TestInMemoryDraftStore_Get_MarkPublished_MarkDiscarded(t *testing.T) {
	t.Parallel()
	sub, drafts, _, _, _ := newSubscriber(t)
	in := subscribers.StageUpEnvelope{
		EventID: eventA, TenantID: tenantA, OwnerGCID: gcidA,
		FamiliarID:          "01970000-0000-7000-8000-000000000111",
		FamiliarDisplayName: "Eira", StageToName: "Teen",
	}
	if err := sub.HandleStageUp(context.Background(), in); err != nil {
		t.Fatalf("HandleStageUp: %v", err)
	}
	pending, _ := drafts.ListPending(context.Background(), tenantA, gcidA)
	draftID := pending[0].DraftID

	got, ok, err := drafts.Get(context.Background(), tenantA, draftID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || got.DraftID != draftID {
		t.Fatalf("Get returned unexpected: ok=%v draft=%+v", ok, got)
	}

	// Get on wrong tenant returns false.
	_, ok2, _ := drafts.Get(context.Background(), "01970000-0000-7000-8000-0000000099aa", draftID)
	if ok2 {
		t.Fatalf("Get must filter by tenant")
	}

	if err := drafts.MarkPublished(context.Background(), tenantA, draftID); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}
	got2, _, _ := drafts.Get(context.Background(), tenantA, draftID)
	if got2.Status != "published" {
		t.Fatalf("expected status=published; got %q", got2.Status)
	}

	// MarkPublished on missing draft fails.
	if err := drafts.MarkPublished(context.Background(), tenantA, "missing"); err == nil {
		t.Fatalf("MarkPublished missing must error")
	}

	if err := drafts.MarkDiscarded(context.Background(), tenantA, draftID); err != nil {
		t.Fatalf("MarkDiscarded: %v", err)
	}
	got3, _, _ := drafts.Get(context.Background(), tenantA, draftID)
	if got3.Status != "discarded" {
		t.Fatalf("expected status=discarded; got %q", got3.Status)
	}
	// MarkDiscarded missing fails.
	if err := drafts.MarkDiscarded(context.Background(), tenantA, "missing"); err == nil {
		t.Fatalf("MarkDiscarded missing must error")
	}
}

func TestInMemoryPreferenceStore_UpsertPort(t *testing.T) {
	t.Parallel()
	prefs := subscribers.NewInMemoryPreferenceStore()
	if err := prefs.Upsert(context.Background(), tenantA, gcidA, subscribers.PolicyAuto); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	got, ok, err := prefs.Get(context.Background(), tenantA, gcidA)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || got != subscribers.PolicyAuto {
		t.Fatalf("expected PolicyAuto; got ok=%v pol=%q", ok, got)
	}
}

func TestDecodeBreedRevealed_ParsesContract(t *testing.T) {
	t.Parallel()
	blob := []byte(`{
		"event_id": "` + eventA + `",
		"tenant_id": "` + tenantA + `",
		"owner_gcid": "` + gcidA + `",
		"familiar_id": "01970000-0000-7000-8000-000000000111",
		"species": "Dragon",
		"shiny_variant": true,
		"rarity": "rare",
		"egg_sku": "egg.standard.v1",
		"rolled_probability": 25.0
	}`)
	got, err := subscribers.DecodeBreedRevealedWithAttrs(blob, nil)
	if err != nil {
		t.Fatalf("DecodeBreedRevealedWithAttrs: %v", err)
	}
	if got.Species != "Dragon" || got.Rarity != "rare" || !got.ShinyVariant {
		t.Fatalf("decode mismatch: %+v", got)
	}
}

func TestDecodeHatched_ParsesContract(t *testing.T) {
	t.Parallel()
	blob := []byte(`{
		"event_id": "` + eventA + `",
		"tenant_id": "` + tenantA + `",
		"owner_gcid": "` + gcidA + `",
		"familiar_id": "01970000-0000-7000-8000-000000000111",
		"display_name": "Eira",
		"species": "Dragon",
		"shiny_variant": false,
		"resonant_atom_id": "01970000-0000-7000-8000-000000000222",
		"specialization": "math",
		"tone": "socratic",
		"learner_persona": "curious-explorer"
	}`)
	got, err := subscribers.DecodeHatchedWithAttrs(blob, nil)
	if err != nil {
		t.Fatalf("DecodeHatchedWithAttrs: %v", err)
	}
	if got.DisplayName != "Eira" || got.Tone != "socratic" {
		t.Fatalf("decode mismatch: %+v", got)
	}
}

// TestDecodeSourceRevelation_ParsesContract — CHO-2259.
//
// This test used to feed a blob carrying "breed_adjective",
// "stage_6_adjective", "breed_themed_effect" and "stage_3_form", then assert
// they decoded. None of them is a field on FamiliarSourceRevelation, so the
// blob was fiction: it proved the decoder could read keys no producer has ever
// emitted, while the real lane NACKed 100% of the time on the missing
// Stage6Adjective. Assert the CONTRACT's fields instead.
func TestDecodeSourceRevelation_ParsesContract(t *testing.T) {
	t.Parallel()
	blob := []byte(`{
		"event_id": "` + eventA + `",
		"tenant_id": "` + tenantA + `",
		"owner_gcid": "` + gcidA + `",
		"familiar_id": "01970000-0000-7000-8000-000000000111",
		"preview_tools": ["query_kg", "score_atom_for_learner"],
		"preview_llm_tier": "pro"
	}`)
	got, err := subscribers.DecodeSourceRevelationWithAttrs(blob, nil)
	if err != nil {
		t.Fatalf("DecodeSourceRevelationWithAttrs: %v", err)
	}
	if got.PreviewLLMTier != "pro" {
		t.Errorf("preview_llm_tier = %q; want %q", got.PreviewLLMTier, "pro")
	}
	if len(got.PreviewTools) != 2 || got.PreviewTools[0] != "query_kg" {
		t.Errorf("preview_tools = %v; want the two wire-carried tools", got.PreviewTools)
	}
	if got.FamiliarID != "01970000-0000-7000-8000-000000000111" {
		t.Errorf("familiar_id = %q", got.FamiliarID)
	}
}

func TestDecode_RejectsMalformedJSON(t *testing.T) {
	t.Parallel()
	for name, fn := range map[string]func([]byte) error{
		"stage_up": func(b []byte) error {
			_, err := subscribers.DecodeStageUpWithAttrs(b, nil)
			return err
		},
		"breed_revealed": func(b []byte) error {
			_, err := subscribers.DecodeBreedRevealedWithAttrs(b, nil)
			return err
		},
		"hatched": func(b []byte) error {
			_, err := subscribers.DecodeHatchedWithAttrs(b, nil)
			return err
		},
		"source_revelation": func(b []byte) error {
			_, err := subscribers.DecodeSourceRevelationWithAttrs(b, nil)
			return err
		},
	} {
		if err := fn([]byte(`{not json`)); err == nil {
			t.Fatalf("%s: expected JSON parse error", name)
		}
	}
}

func TestStageUp_RejectsMissingOwnerGCID(t *testing.T) {
	t.Parallel()
	sub, _, _, _, _ := newSubscriber(t)
	err := sub.HandleStageUp(context.Background(), subscribers.StageUpEnvelope{
		EventID: eventA, TenantID: tenantA,
		FamiliarDisplayName: "Eira", StageToName: "Teen",
	})
	if err == nil {
		t.Fatalf("expected error on missing owner_gcid")
	}
}
