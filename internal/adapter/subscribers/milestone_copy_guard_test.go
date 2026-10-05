// milestone_copy_guard_test.go — the guard CHO-2259 exists for.
//
// These bodies are learner-facing and they reach the C+ social feed. They went
// broken and unnoticed for the whole life of the lane for one reason: the
// default policy is `draft`, and until CHO-2258 NOTHING could read a draft. The
// first person ever to read one found two defects immediately.
//
// The guard therefore composes through the REAL handler path (real Composer,
// real DefaultTemplates, real displayOrDefault fallbacks) driven by envelopes
// carrying ONLY what the wire actually delivers — because the root cause is that
// the templates were authored against fields the wire has never carried:
//
//   - post_template.go's own doc says StageUpInput comes from stage_up.v1 "+ the
//     FamiliarInstance snapshot (display name + breed adjective) joined
//     client-side". That join was never built, and it CANNOT lawfully be built:
//     FamiliarInstance lives in chora_consumption and cross-DB queries are
//     forbidden. The only lawful source is the event payload.
//   - familiar.proto carries familiar_display_name on stage_up (13) and
//     source_revelation (9) since CHO-2266 — denormalised from the FamiliarInstance
//     at publish time (the lawful carrier; cross-DB queries are forbidden). It
//     still carries NO owner_display_name, breed_adjective, stage_6_adjective,
//     breed_themed_effect or stage_3_form on ANY message, and NO name on
//     breed_revealed (which fires BEFORE the learner names the pod, so a name
//     field there would render its fallback 100% of the time). hatched carries
//     display_name (4). So stage_up, source_revelation and hatched name the
//     Familiar from the wire; breed_revealed cannot and must not try.
//
// A test that composed with hand-filled display names would pass while
// production renders "Their friend's Your Familiar". So every envelope below is
// wire-realistic: if protodecode cannot produce the field, the test must not
// supply it. That is the assertion.
package subscribers_test

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/subscribers"
	tmpl "github.com/apollo-chora/chora-sharing/internal/domain/post_template"
)

// rawEnumToken matches an internal SCREAMING_SNAKE identifier — a proto enum
// name that escaped into learner copy (FAMILIAR_SPECIES_FOX).
var rawEnumToken = regexp.MustCompile(`[A-Z][A-Z0-9]*_[A-Z0-9_]+`)

// emptySlotArtifact matches the punctuation scars a blank template field leaves
// mid-sentence: "a , wreathed in .", "still ,", "reached fledgling !", or the
// doubled space where a value should have been.
//
// Deliberately [ \t] and not \s: a blank line between paragraphs is intended
// copy, and \s{2,} flagged "memory.\n\nWalk with them." as a defect. (Caught
// by this guard failing on the CORRECT output — a scanner that cries on good
// copy gets disabled, and then it guards nothing.)
var emptySlotArtifact = regexp.MustCompile(`([ \t][,.!?])|(\ba[ \t]+,)|([ \t]{2,}\S)`)

// composeAll drives every milestone handler over the in-memory doubles and
// returns the composed draft bodies by handler name.
func composeAll(t *testing.T) map[string]string {
	t.Helper()
	const (
		tenant = "11111111-1111-7111-8111-111111111111"
		owner  = "019f65a7-ce2a-739b-b563-d54653d2b0b6"
	)
	drafts := subscribers.NewInMemoryDraftStore()
	sub := subscribers.NewFamiliarMilestoneSubscriber(subscribers.Config{
		Drafts:      drafts,
		Posts:       subscribers.NewInMemoryPostPublisher(),
		Preferences: subscribers.NewInMemoryPreferenceStore(),
		Idempotency: subscribers.NewInMemoryIdempotencyStore(),
		Composer:    tmpl.NewComposer(tmpl.DefaultTemplates()),
	})
	ctx := context.Background()

	// stage_up — the wire carries stage names + tools + tier, AND (since CHO-2266)
	// the denormalised familiar_display_name. NO breed adjective (still not a field).
	if err := sub.HandleStageUp(ctx, subscribers.StageUpEnvelope{
		EventID:             "019f7005-9698-7795-a3b3-708f79cc02bc",
		TenantID:            tenant,
		OwnerGCID:           owner,
		FamiliarID:          "019f6eba-0000-7000-a000-000000000001",
		FamiliarDisplayName: "Tempo",
		StageFromName:       "baby",
		StageToName:         "fledgling",
		NewlyUnlockedTools:  []string{"atom_search"},
		NewLLMTier:          "flash",
	}); err != nil {
		t.Fatalf("HandleStageUp: %v", err)
	}

	// hatched — the ONLY message carrying a name (display_name, field 4).
	if err := sub.HandleHatched(ctx, subscribers.HatchedEnvelope{
		EventID:      "019f6eba-a1e2-7000-a000-000000000002",
		TenantID:     tenant,
		OwnerGCID:    owner,
		FamiliarID:   "019f6eba-0000-7000-a000-000000000001",
		DisplayName:  "Tempo",
		Species:      "fox",
		ShinyVariant: true,
	}); err != nil {
		t.Fatalf("HandleHatched: %v", err)
	}

	// breed_revealed — species + rarity + shiny only. No name on the wire.
	if err := sub.HandleBreedRevealed(ctx, subscribers.BreedRevealedEnvelope{
		EventID:      "019f6eba-a1e2-7000-a000-000000000003",
		TenantID:     tenant,
		OwnerGCID:    owner,
		FamiliarID:   "019f6eba-0000-7000-a000-000000000001",
		Species:      "fox",
		Rarity:       "common",
		ShinyVariant: true,
	}); err != nil {
		t.Fatalf("HandleBreedRevealed: %v", err)
	}

	// source_revelation — preview tier + tools + window + (since CHO-2266) the
	// denormalised familiar_display_name, so the ceremony names the Familiar. The
	// breed-themed imagery (stage_6_adjective / breed_themed_effect / stage_3_form)
	// remains OFF the contract — no per-breed descriptor content exists to
	// denormalise (deferred; see the CHO-2266 report), so it renders nothing rather
	// than an empty slot. This milestone has never fired in production (it needs a
	// stage-3 ceremony).
	if err := sub.HandleSourceRevelation(ctx, subscribers.SourceRevelationEnvelope{
		EventID:             "019f6eba-a1e2-7000-a000-000000000004",
		TenantID:            tenant,
		OwnerGCID:           owner,
		FamiliarID:          "019f6eba-0000-7000-a000-000000000001",
		FamiliarDisplayName: "Tempo",
		PreviewLLMTier:      "pro",
	}); err != nil {
		t.Fatalf("HandleSourceRevelation: %v", err)
	}

	out := map[string]string{}
	for _, d := range drafts.All() {
		out[d.ComposedFromTopic] = d.Body
	}
	return out
}

// No internal identifier may reach a learner.
func TestComposedBodies_LeakNoRawEnumOrIdentifier(t *testing.T) {
	t.Parallel()
	for topic, body := range composeAll(t) {
		if tok := rawEnumToken.FindString(body); tok != "" {
			t.Errorf("%s: raw internal identifier %q leaked into learner copy:\n%s", topic, tok, body)
		}
	}
}

// A blank template field leaves punctuation scars mid-sentence. If a field the
// wire cannot carry is in a template, this is what the learner reads.
func TestComposedBodies_HaveNoEmptySlotArtifacts(t *testing.T) {
	t.Parallel()
	for topic, body := range composeAll(t) {
		if scar := emptySlotArtifact.FindString(body); scar != "" {
			t.Errorf("%s: empty-slot artifact %q — a template field the wire never carries:\n%s", topic, scar, body)
		}
	}
}

// The defaults are standalone phrases that read fine alone and collide when the
// template slots them into a possessive: "Their friend's Your Familiar grew".
func TestComposedBodies_NeverCollideTwoFallbackSubjects(t *testing.T) {
	t.Parallel()
	for topic, body := range composeAll(t) {
		for _, bad := range []string{"Their friend's Your", "Their friend", "'s Your Familiar"} {
			if strings.Contains(body, bad) {
				t.Errorf("%s: fallback-subject collision %q:\n%s", topic, bad, body)
			}
		}
	}
}

// Curiosity-first copy rules (post_template.go §9): the composed copy must keep
// using the sanctioned verbs and never the forbidden gaming register.
func TestComposedBodies_HonourCuriosityFirstVocabulary(t *testing.T) {
	t.Parallel()
	for topic, body := range composeAll(t) {
		for _, banned := range []string{"level up", "loot", "lootbox", "rare drop", "premium content"} {
			if strings.Contains(strings.ToLower(body), banned) {
				t.Errorf("%s: forbidden gaming register %q:\n%s", topic, banned, body)
			}
		}
	}
}

// The one name the wire DOES carry must actually be used.
func TestComposedBodies_HatchedNamesTheFamiliarFromTheWire(t *testing.T) {
	t.Parallel()
	body := composeAll(t)[subscribers.TopicFamiliarHatched]
	if !strings.Contains(body, "Tempo") {
		t.Errorf("hatched carries display_name on the wire and must name the familiar:\n%s", body)
	}
	if !strings.Contains(strings.ToLower(body), "fox") {
		t.Errorf("hatched must name the species in plain language:\n%s", body)
	}
}

// CHO-2266: stage_up now carries the name on the wire, so the copy names the
// Familiar ("Tempo grew") instead of the neutral "My Familiar" fallback.
func TestComposedBodies_StageUpNamesTheFamiliarFromTheWire(t *testing.T) {
	t.Parallel()
	body := composeAll(t)[subscribers.TopicFamiliarStageUp]
	if !strings.Contains(body, "Tempo") {
		t.Errorf("stage_up carries familiar_display_name (CHO-2266) and must name the familiar:\n%s", body)
	}
	if strings.Contains(body, "My Familiar") {
		t.Errorf("stage_up must not fall back to the neutral form when the wire carried a name:\n%s", body)
	}
}

// CHO-2266: source_revelation now carries the name on the wire, so the Stage-3
// ceremony names the Familiar ("Tempo paused...").
func TestComposedBodies_SourceRevelationNamesTheFamiliarFromTheWire(t *testing.T) {
	t.Parallel()
	body := composeAll(t)[subscribers.TopicFamiliarSourceRevelation]
	if !strings.Contains(body, "Tempo") {
		t.Errorf("source_revelation carries familiar_display_name (CHO-2266) and must name the familiar:\n%s", body)
	}
}
