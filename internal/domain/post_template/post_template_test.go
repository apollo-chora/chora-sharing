// Package post_template_test holds the RED-phase TDD specs for the
// chora-sharing Familiar milestone post-template composer (PROD-G).
//
// Composer renders curiosity-first copy per ADR-149 +
// docs/m13/HANDOFF_FE_FAMILIAR_GROWTH_2026-05-13.md §4. The composer is
// PURE — no I/O, no time-source — so the same input always renders the
// same output. Templates are loaded from YAML at boot (see
// services/chora-sharing/config/familiar_milestone_templates.yaml).
package post_template_test

import (
	"strings"
	"testing"

	tmpl "github.com/apollo-chora/chora-sharing/internal/domain/post_template"
)

// -----------------------------------------------------------------------------
// StageUpTemplate
// -----------------------------------------------------------------------------

func TestStageUpTemplate_RendersStageNameFromTheWire(t *testing.T) {
	t.Parallel()

	c := tmpl.NewComposer(tmpl.DefaultTemplates())
	out, err := c.StageUp(tmpl.StageUpInput{
		FamiliarDisplayName:     "Eira",
		StageToName:             "Teen",
		NewlyUnlockedTools:      []string{"query_kg", "score_atom_for_learner"},
		NewlyRevealedKgNeighbors: []string{"atom-1", "atom-2"},
	})
	if err != nil {
		t.Fatalf("StageUp: %v", err)
	}
	if !strings.Contains(out, "Eira") {
		t.Fatalf("StageUp must reference Familiar name; got %q", out)
	}
	if !strings.Contains(out, "reached Teen") {
		t.Fatalf("StageUp must name the stage the wire carried; got %q", out)
	}
	// BreedAdjective is gone: it was never a proto field, and appending it to the
	// stage name printed "reached fledgling Companion!" in production.
	if strings.Contains(out, "Dragon") || strings.Contains(out, "Companion") {
		t.Fatalf("StageUp must not invent a breed adjective; got %q", out)
	}
}

func TestStageUpTemplate_RefusesAvoidedCurseWords(t *testing.T) {
	t.Parallel()

	c := tmpl.NewComposer(tmpl.DefaultTemplates())
	out, err := c.StageUp(tmpl.StageUpInput{
		FamiliarDisplayName: "Eira",
		StageToName:         "Teen",
	})
	if err != nil {
		t.Fatalf("StageUp: %v", err)
	}
	for _, banned := range []string{"level up", "leveled up", "loot", "lootbox", "rare drop", "got lucky"} {
		if strings.Contains(strings.ToLower(out), banned) {
			t.Fatalf("StageUp copy must NOT contain %q per HANDOFF §4.2; got %q", banned, out)
		}
	}
}

func TestStageUpTemplate_UsesGrewOrMaturedVerb(t *testing.T) {
	t.Parallel()

	c := tmpl.NewComposer(tmpl.DefaultTemplates())
	out, err := c.StageUp(tmpl.StageUpInput{
		FamiliarDisplayName: "Eira",
		StageToName:         "Matured",
	})
	if err != nil {
		t.Fatalf("StageUp: %v", err)
	}
	lo := strings.ToLower(out)
	if !(strings.Contains(lo, "grew") ||
		strings.Contains(lo, "stage up") ||
		strings.Contains(lo, "stage-up") ||
		strings.Contains(lo, "matured") ||
		strings.Contains(lo, "reached")) {
		t.Fatalf("StageUp copy must use grew/stage-up/matured/reached verb; got %q", out)
	}
}

func TestStageUpTemplate_IncludesUnlockedToolsList(t *testing.T) {
	t.Parallel()

	c := tmpl.NewComposer(tmpl.DefaultTemplates())
	out, err := c.StageUp(tmpl.StageUpInput{
		FamiliarDisplayName: "Eira",
		StageToName:         "Teen",
		NewlyUnlockedTools:  []string{"query_kg", "score_atom_for_learner"},
	})
	if err != nil {
		t.Fatalf("StageUp: %v", err)
	}
	if !strings.Contains(out, "query_kg") {
		t.Fatalf("StageUp copy must mention newly_unlocked_tools when present; got %q", out)
	}
}


// -----------------------------------------------------------------------------
// BreedRevealTemplate
// -----------------------------------------------------------------------------

func TestBreedRevealTemplate_RendersSpeciesAndRarity(t *testing.T) {
	t.Parallel()

	c := tmpl.NewComposer(tmpl.DefaultTemplates())
	out, err := c.BreedReveal(tmpl.BreedRevealInput{
		FamiliarDisplayName: "Eira",
		Species:             "Dragon",
		Rarity:              "rare",
		ShinyVariant:        false,
	})
	if err != nil {
		t.Fatalf("BreedReveal: %v", err)
	}
	if !strings.Contains(out, "Eira") {
		t.Fatalf("BreedReveal must reference Familiar name; got %q", out)
	}
	if !strings.Contains(strings.ToLower(out), "dragon") {
		t.Fatalf("BreedReveal must reference species; got %q", out)
	}
}

func TestBreedRevealTemplate_AddsShinyAdorner(t *testing.T) {
	t.Parallel()

	c := tmpl.NewComposer(tmpl.DefaultTemplates())
	out, err := c.BreedReveal(tmpl.BreedRevealInput{
		FamiliarDisplayName: "Eira",
		Species:             "Dragon",
		Rarity:              "rare",
		ShinyVariant:        true,
	})
	if err != nil {
		t.Fatalf("BreedReveal: %v", err)
	}
	if !strings.Contains(strings.ToLower(out), "shiny") &&
		!strings.Contains(strings.ToLower(out), "shimmer") &&
		!strings.Contains(strings.ToLower(out), "alternate palette") {
		t.Fatalf("BreedReveal must adorn shiny variants; got %q", out)
	}
}

func TestBreedRevealTemplate_RefusesGamblingLanguage(t *testing.T) {
	t.Parallel()

	c := tmpl.NewComposer(tmpl.DefaultTemplates())
	out, err := c.BreedReveal(tmpl.BreedRevealInput{
		FamiliarDisplayName: "Eira",
		Species:             "Dragon",
		Rarity:              "legendary",
	})
	if err != nil {
		t.Fatalf("BreedReveal: %v", err)
	}
	for _, banned := range []string{"got lucky", "lootbox", "loot", "rare drop", "won"} {
		if strings.Contains(strings.ToLower(out), banned) {
			t.Fatalf("BreedReveal copy must NOT contain gambling phrasing %q per HANDOFF §4.2; got %q", banned, out)
		}
	}
}


// -----------------------------------------------------------------------------
// SourceRevelationTemplate
// -----------------------------------------------------------------------------

func TestSourceRevelationTemplate_RendersCeremonyFromWireFieldsOnly(t *testing.T) {
	t.Parallel()

	c := tmpl.NewComposer(tmpl.DefaultTemplates())
	out, err := c.SourceRevelation(tmpl.SourceRevelationInput{
		FamiliarDisplayName: "Eira",
	})
	if err != nil {
		t.Fatalf("SourceRevelation: %v", err)
	}
	// HANDOFF §4.3 mandates these phrases (paraphrase-safe but key cues).
	// The §4.3 breed-themed imagery is GONE, with the fields that never existed.
	// "Matured Angelic Dragon" / "starlit aurora" came from Stage6Adjective +
	// BreedThemedEffect, which are on no milestone message — so this test only
	// ever passed on hand-fed input while the real lane could not render at all.
	// Restoring the imagery needs those descriptors on the CONTRACT, not a
	// fallback invented here.
	for _, phrase := range []string{"paused", "glimpsed", "Eira"} {
		if !strings.Contains(out, phrase) {
			t.Fatalf("SourceRevelation copy must include the ceremony cue %q; got %q", phrase, out)
		}
	}
	for _, phantom := range []string{"Matured Angelic Dragon", "starlit aurora"} {
		if strings.Contains(out, phantom) {
			t.Fatalf("SourceRevelation must not render %q — no message carries it; got %q", phantom, out)
		}
	}
}

func TestSourceRevelationTemplate_RefusesBannedWords(t *testing.T) {
	t.Parallel()

	c := tmpl.NewComposer(tmpl.DefaultTemplates())
	out, err := c.SourceRevelation(tmpl.SourceRevelationInput{
		FamiliarDisplayName: "Eira",
	})
	if err != nil {
		t.Fatalf("SourceRevelation: %v", err)
	}
	for _, banned := range []string{"level up", "loot", "lootbox", "rare drop", "premium content"} {
		if strings.Contains(strings.ToLower(out), banned) {
			t.Fatalf("SourceRevelation must NOT contain %q; got %q", banned, out)
		}
	}
}

// -----------------------------------------------------------------------------
// LoadTemplates from YAML
// -----------------------------------------------------------------------------

func TestLoadFromBytes_ParsesYAML(t *testing.T) {
	t.Parallel()

	yamlBlob := []byte(`
stage_up: |
  {{.FamiliarDisplayName}} just reached {{.StageToName}} {{.BreedAdjective}}!
breed_reveal: |
  Meet {{.FamiliarDisplayName}}, a {{.Rarity}} {{.Species}}!
source_revelation: |
  {{.FamiliarDisplayName}} paused. Glimpsed a {{.Stage6Adjective}} wreathed in {{.BreedThemedEffect}}.
`)
	tpls, err := tmpl.LoadFromBytes(yamlBlob)
	if err != nil {
		t.Fatalf("LoadFromBytes: %v", err)
	}
	if tpls.StageUp == "" || tpls.BreedReveal == "" || tpls.SourceRevelation == "" {
		t.Fatalf("LoadFromBytes must populate all three templates; got %+v", tpls)
	}
}

func TestLoadFromBytes_RejectsMissingTemplates(t *testing.T) {
	t.Parallel()
	_, err := tmpl.LoadFromBytes([]byte(`stage_up: "x"`))
	if err == nil {
		t.Fatalf("LoadFromBytes must reject a YAML missing breed_reveal + source_revelation")
	}
}

// -----------------------------------------------------------------------------
// Composer guard rails
// -----------------------------------------------------------------------------

func TestComposer_NilTemplatesReturnsError(t *testing.T) {
	t.Parallel()
	c := tmpl.NewComposer(tmpl.Templates{})
	_, err := c.StageUp(tmpl.StageUpInput{
		FamiliarDisplayName: "Eira",
		StageToName:         "Teen",
	})
	if err == nil {
		t.Fatalf("StageUp must error when StageUp template is empty")
	}
	if _, err := c.BreedReveal(tmpl.BreedRevealInput{
		FamiliarDisplayName: "Eira",
		Species:             "Dragon",
	}); err == nil {
		t.Fatalf("BreedReveal must error when template is empty")
	}
	if _, err := c.SourceRevelation(tmpl.SourceRevelationInput{
		FamiliarDisplayName: "Eira",
	}); err == nil {
		t.Fatalf("SourceRevelation must error when template is empty")
	}
}

func TestStageUpTemplate_RejectsEmptyStageToName(t *testing.T) {
	t.Parallel()
	c := tmpl.NewComposer(tmpl.DefaultTemplates())
	if _, err := c.StageUp(tmpl.StageUpInput{
		FamiliarDisplayName: "Eira",
	}); err == nil {
		t.Fatalf("StageUp must reject empty StageToName")
	}
}



func TestBreedRevealTemplate_DefaultsRarity(t *testing.T) {
	t.Parallel()
	c := tmpl.NewComposer(tmpl.DefaultTemplates())
	out, err := c.BreedReveal(tmpl.BreedRevealInput{
		FamiliarDisplayName: "Eira",
		Species:             "Dragon",
		// Rarity intentionally empty — composer should default to "common".
	})
	if err != nil {
		t.Fatalf("BreedReveal: %v", err)
	}
	if !strings.Contains(strings.ToLower(out), "common") {
		t.Fatalf("BreedReveal must default rarity to 'common' when empty; got %q", out)
	}
}


func TestLoadFromBytes_RejectsMalformedYAML(t *testing.T) {
	t.Parallel()
	_, err := tmpl.LoadFromBytes([]byte(": not yaml\n\t \t  - garbage"))
	if err == nil {
		t.Fatalf("LoadFromBytes must reject malformed YAML")
	}
}

func TestComposer_BadTemplateParseFails(t *testing.T) {
	t.Parallel()
	// Bad template syntax — {{ without close.
	c := tmpl.NewComposer(tmpl.Templates{
		StageUp:          `{{`,
		BreedReveal:      `{{`,
		SourceRevelation: `{{`,
	})
	if _, err := c.StageUp(tmpl.StageUpInput{
		FamiliarDisplayName: "Eira", StageToName: "Teen",
	}); err == nil {
		t.Fatalf("expected parse error from malformed StageUp template")
	}
}
