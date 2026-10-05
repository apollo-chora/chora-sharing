// Package post_template renders curiosity-first share-post bodies for the
// Familiar growth milestones defined in ADR-149:
//
//   - StageUp        → chora.consumption.familiar.stage_up.v1
//   - BreedReveal    → chora.consumption.familiar.breed_revealed.v1
//   - chora.consumption.familiar.hatched.v1
//   - SourceRevelation → chora.consumption.familiar.source_revelation.v1
//
// Curiosity-first copy rules (HANDOFF_FE_FAMILIAR_GROWTH_2026-05-13.md §4):
//
//   - Use "grew" / "stage up" / "matured" / "reached"
//   - NEVER use "level up", "loot", "lootbox", "rare drop", "premium content"
//
// Pure-domain package: no I/O, no clock, no logger. Templates are
// text/template strings loaded from YAML (services/chora-sharing/config/
// familiar_milestone_templates.yaml) at boot per
// `feedback_no_inline_config` (the canonical defaults are still embedded
// here so the package is testable without the YAML).
package post_template

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"text/template"

	"gopkg.in/yaml.v3"
)

// Templates holds the three milestone copy templates, loaded from YAML.
//
// Each field is a `text/template` source string. The composer parses on
// every call; templates are tiny and parsing is cheap, but a future
// optimisation may pre-parse once at boot.
type Templates struct {
	StageUp          string `yaml:"stage_up"`
	BreedReveal      string `yaml:"breed_reveal"`
	SourceRevelation string `yaml:"source_revelation"`
}

// DefaultTemplates returns the canonical copy embedded in source for use
// when the YAML file is missing (tests + dev fallback).
//
// HANDOFF §4.3 ceremony copy is reproduced VERBATIM in SourceRevelation
// (with template placeholders).
// DefaultTemplates returns the canonical copy embedded in source, used when the
// YAML override is absent. CHORA_FAMILIAR_TEMPLATES_PATH is UNSET in prod, so
// THESE are what actually renders today.
//
// ⚠ Byte-identical to config/familiar_milestone_templates.yaml, enforced by
// templates_parity_test.go. Written as raw literals with real newlines rather
// than a "\n\n" + concat chain so the two copies can be diffed by eye — the
// concat form is how they silently drifted apart in the first place.
//
// ⚠ COMPOSE ONLY FROM FIELDS THE EVENT CARRIES (CHO-2259). See the input type
// docs: no milestone message carries an owner display name, a breed adjective,
// or the ceremony descriptors. A new placeholder here needs a proto field first.
func DefaultTemplates() Templates {
	return Templates{
		StageUp: `{{ familiarName .FamiliarDisplayName }} grew — they reached {{.StageToName}}!

{{- if .NewlyUnlockedTools }}
They learned: {{ joinList .NewlyUnlockedTools ", " }}.
{{- end }}
{{- if .NewlyRevealedKgNeighbors }}
New territory came into view: {{ countItems .NewlyRevealedKgNeighbors }} fresh atom(s) joined their reach.
{{- end }}

Walk with them.`,

		BreedReveal: `{{- if .FamiliarDisplayName }}Meet {{.FamiliarDisplayName}} — {{ speciesPhrase .Rarity .Species }}{{ if .ShinyVariant }} with a shiny shimmer{{ end }}!
{{- else }}A fated companion has come into the world — {{ speciesPhrase .Rarity .Species }}{{ if .ShinyVariant }} with a shiny shimmer{{ end }}!
{{- end }}

{{ if .FamiliarDisplayName }}{{.FamiliarDisplayName}} and I are{{ else }}We are{{ end }} walking together now.`,

		SourceRevelation: `{{ familiarName .FamiliarDisplayName }} paused. For a heartbeat, I glimpsed what they will one day become.

The vision faded. They returned as they were — but their eyes carry the memory.
{{- if .PreviewTools }}
*For a short while, they can draw on the wisdom of their future self: {{ joinList .PreviewTools ", " }}.*
{{- else }}
*For a short while, they can draw on the wisdom of their future self.*
{{- end }}`,
	}
}

// LoadFromBytes parses a YAML document into a Templates value and
// validates that all three required template strings are present.
func LoadFromBytes(blob []byte) (Templates, error) {
	var t Templates
	if err := yaml.Unmarshal(blob, &t); err != nil {
		return Templates{}, fmt.Errorf("post_template: parse yaml: %w", err)
	}
	if strings.TrimSpace(t.StageUp) == "" ||
		strings.TrimSpace(t.BreedReveal) == "" ||
		strings.TrimSpace(t.SourceRevelation) == "" {
		return Templates{}, errors.New("post_template: YAML must define stage_up, breed_reveal, and source_revelation")
	}
	return t, nil
}

// -----------------------------------------------------------------------------
// Input shapes
// -----------------------------------------------------------------------------

// StageUpInput is the template payload for a Familiar stage-up share.
//
// Source: chora.consumption.familiar.stage_up.v1 — and NOTHING ELSE.
//
// ⚠ This doc used to promise "+ the FamiliarInstance snapshot (display name +
// breed adjective) joined client-side". That join was never built and it CANNOT
// be: FamiliarInstance lives in chora_consumption and cross-DB queries are
// forbidden (ddd-enforcement #1). So OwnerDisplayName and BreedAdjective were
// fields nothing could ever fill, and the subscriber papered over them with the
// standalone fallbacks "Their friend" and "Companion" — which the template then
// slotted into a possessive, printing "Their friend's Your Familiar grew — they
// reached fledgling Companion!" to real learners (CHO-2259).
//
// Every field below is carried by the event. FamiliarDisplayName is the one
// exception and is OPTIONAL: FamiliarStageUp has no name field today, so it
// arrives empty and the template renders the neutral form. If the contract ever
// gains one (see the CHO-2259 split story), it starts naming the Familiar here
// with no code change.
type StageUpInput struct {
	FamiliarDisplayName      string // OPTIONAL — absent on the stage_up wire today
	StageToName              string // e.g. "fledgling", "matured"
	NewlyUnlockedTools       []string
	NewlyRevealedKgNeighbors []string
	NewLLMTier               string // optional — "pro" | "flash" | …
}

// BreedRevealInput is the template payload for a hatching reveal share.
//
// Source: chora.consumption.familiar.breed_revealed.v1 OR .hatched.v1.
//
// ⚠ Only FamiliarHatched carries display_name (field 4). breed_revealed has no
// name on the wire, so the old copy rendered "Meet Your Familiar, a common
// FAMILIAR_SPECIES_FOX…" — you cannot "meet" a fallback. The template now
// branches on the name instead of demanding one.
type BreedRevealInput struct {
	FamiliarDisplayName string // OPTIONAL — only hatched carries a name
	Species             string // OPTIONAL — plain-language label ("fox"); absent when off-contract
	Rarity              string // "common" | "uncommon" | "rare" | "legendary"
	ShinyVariant        bool
	EggSKU              string // optional; included in metadata, not body
}

// SourceRevelationInput is the template payload for the Stage-3 Aha ceremony.
//
// Source: chora.consumption.familiar.source_revelation.v1 — and NOTHING ELSE.
//
// ⚠⚠ This template was the worst of the CHO-2259 set, and the only one that
// never reached a learner — because it could not render AT ALL. It demanded
// Stage6Adjective + BreedThemedEffect + Stage3Form as REQUIRED, none of which
// exist on FamiliarSourceRevelation (or any milestone message), and unlike the
// other two the subscriber supplied no fallback for them. So Compose returned
// "Stage6Adjective required", HandleSourceRevelation returned that error, and
// EVERY source_revelation message NACKed — 100%, by construction, on a lane with
// no DLQ. It went unnoticed because the ceremony needs a stage-3 Familiar.
//
// The breed-themed imagery is gone with the fields that never existed. Restoring
// it needs those descriptors on the contract (the CHO-2259 split story), not a
// fallback string invented here. What remains is composed only from what the
// event carries.
type SourceRevelationInput struct {
	FamiliarDisplayName string   // OPTIONAL — absent on this wire today
	PreviewTools        []string // capabilities the future self lends back
	PreviewLLMTier      string   // "pro" | "flash"
}

// -----------------------------------------------------------------------------
// Composer
// -----------------------------------------------------------------------------

// Composer renders milestone post bodies. Stateless — safe for shared use.
type Composer struct {
	templates Templates
	funcs     template.FuncMap
}

// NewComposer constructs a Composer around the supplied template strings.
func NewComposer(templates Templates) *Composer {
	return &Composer{
		templates: templates,
		funcs: template.FuncMap{
			"joinList":   func(in []string, sep string) string { return strings.Join(in, sep) },
			"countItems": func(in []string) int { return len(in) },
			// familiarName and speciesPhrase keep every fallback in ONE place.
			// The CHO-2259 collision happened because two fallbacks were chosen
			// independently ("Their friend", "Your Familiar"), each fine alone,
			// and a template then slotted them into one possessive.
			"familiarName":  familiarName,
			"speciesPhrase": speciesPhrase,
		},
	}
}

// StageUp renders the stage-up share body.
func (c *Composer) StageUp(in StageUpInput) (string, error) {
	if strings.TrimSpace(c.templates.StageUp) == "" {
		return "", errors.New("post_template: stage_up template not configured")
	}
	// StageToName is the ONLY required field: it is on the wire, and the copy is
	// meaningless without it. FamiliarDisplayName is optional by design — see the
	// type doc. Requiring a field the wire cannot carry is what turned
	// source_revelation into a 100%-NACK lane.
	if strings.TrimSpace(in.StageToName) == "" {
		return "", errors.New("post_template: StageToName required")
	}
	return c.render("stage_up", c.templates.StageUp, in)
}

// BreedReveal renders the hatching breed-reveal share body.
func (c *Composer) BreedReveal(in BreedRevealInput) (string, error) {
	if strings.TrimSpace(c.templates.BreedReveal) == "" {
		return "", errors.New("post_template: breed_reveal template not configured")
	}
	// Neither the name nor the species is required: breed_revealed carries no
	// name, and an off-contract species decodes to absent rather than leaking its
	// proto identifier (CHO-2259). The template branches on both.
	if strings.TrimSpace(in.Rarity) == "" {
		in.Rarity = "common"
	}
	return c.render("breed_reveal", c.templates.BreedReveal, in)
}

// SourceRevelation renders the Stage-3 Aha-moment ceremony share body.
func (c *Composer) SourceRevelation(in SourceRevelationInput) (string, error) {
	if strings.TrimSpace(c.templates.SourceRevelation) == "" {
		return "", errors.New("post_template: source_revelation template not configured")
	}
	// Nothing here is required. The three fields this used to demand were never
	// on the contract, so this branch failed 100% of the time — see the type doc.
	return c.render("source_revelation", c.templates.SourceRevelation, in)
}

// familiarName renders the Familiar's name, or the owner-voiced fallback when
// the wire carried none.
//
// "My Familiar" is deliberate. The draft is composed FOR THE OWNER and, once
// published, the C+ feed renders it beneath the owner's own name — so the voice
// is first-person in both places. The old fallback "Your Familiar" addressed the
// reader (wrong in the feed), and the separate "Their friend's" prefix narrated
// the owner in the third person on their OWN post (wrong everywhere, and
// unobtainable: no message carries an owner display name).
func familiarName(name string) string {
	if n := strings.TrimSpace(name); n != "" {
		return n
	}
	return "My Familiar"
}

// speciesPhrase renders "a common fox", degrading to "a common companion" when
// the species is absent — never "a common " with a hole in it.
//
// Species can legitimately be absent: protodecode omits it rather than leaking
// FAMILIAR_SPECIES_FOX or the raw digits of an off-contract value (CHO-2259).
func speciesPhrase(rarity, species string) string {
	r := strings.TrimSpace(rarity)
	if r == "" {
		r = "common"
	}
	s := strings.TrimSpace(species)
	if s == "" {
		s = "companion"
	}
	return "a " + r + " " + s
}

func (c *Composer) render(name, src string, data interface{}) (string, error) {
	t, err := template.New(name).Funcs(c.funcs).Parse(src)
	if err != nil {
		return "", fmt.Errorf("post_template: parse %s: %w", name, err)
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("post_template: render %s: %w", name, err)
	}
	return buf.String(), nil
}
