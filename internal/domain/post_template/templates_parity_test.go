// templates_parity_test.go — CHO-2259: the YAML and the embedded defaults are
// two copies of one thing, and they must not drift.
//
// config/familiar_milestone_templates.yaml is the ops override channel
// (CHORA_FAMILIAR_TEMPLATES_PATH). That env var is UNSET in prod, so the
// EMBEDDED DefaultTemplates() are what actually renders and the YAML is inert
// today — which is precisely how it rots unnoticed, and then resurrects the
// CHO-2259 copy defects the day someone mounts it. An inert second copy is the
// most dangerous kind: nothing exercises it, so nothing reports it.
//
// This reads the file FROM DISK rather than restating its content, so the test
// cannot pass by agreeing with a stale copy of itself.
package post_template_test

import (
	"os"
	"path/filepath"
	"testing"

	tmpl "github.com/apollo-chora/chora-sharing/internal/domain/post_template"
)

// templatesYAMLPath is the checked-in override file, relative to this package.
const templatesYAMLPath = "../../../config/familiar_milestone_templates.yaml"

func TestYAMLTemplates_AreIdenticalToTheEmbeddedDefaults(t *testing.T) {
	t.Parallel()

	blob, err := os.ReadFile(filepath.Clean(templatesYAMLPath))
	if err != nil {
		t.Fatalf("read %s: %v — the override file is part of the contract, not optional", templatesYAMLPath, err)
	}
	fromYAML, err := tmpl.LoadFromBytes(blob)
	if err != nil {
		t.Fatalf("the checked-in YAML does not even parse: %v", err)
	}
	embedded := tmpl.DefaultTemplates()

	for _, tc := range []struct {
		name           string
		yaml, embedded string
	}{
		{"stage_up", fromYAML.StageUp, embedded.StageUp},
		{"breed_reveal", fromYAML.BreedReveal, embedded.BreedReveal},
		{"source_revelation", fromYAML.SourceRevelation, embedded.SourceRevelation},
	} {
		if tc.yaml != tc.embedded {
			t.Errorf("%s has DRIFTED between the YAML and DefaultTemplates().\n"+
				"Mounting CHORA_FAMILIAR_TEMPLATES_PATH would silently change learner-facing copy.\n"+
				"--- yaml ---\n%s\n--- embedded ---\n%s", tc.name, tc.yaml, tc.embedded)
		}
	}
}

// Both copies must compose — a YAML that parses but references a field the input
// no longer has renders an error at runtime, on a lane with no DLQ.
func TestYAMLTemplates_ComposeEveryMilestoneFromWireOnlyFields(t *testing.T) {
	t.Parallel()

	blob, err := os.ReadFile(filepath.Clean(templatesYAMLPath))
	if err != nil {
		t.Fatalf("read %s: %v", templatesYAMLPath, err)
	}
	loaded, err := tmpl.LoadFromBytes(blob)
	if err != nil {
		t.Fatalf("LoadFromBytes: %v", err)
	}
	c := tmpl.NewComposer(loaded)

	// Every input below carries ONLY what the event actually delivers: no name
	// on stage_up or source_revelation, no name on breed_revealed. A test that
	// hand-filled them is exactly what let the defect ship.
	if _, err := c.StageUp(tmpl.StageUpInput{StageToName: "fledgling", NewlyUnlockedTools: []string{"atom_search"}}); err != nil {
		t.Errorf("stage_up must compose with no name on the wire: %v", err)
	}
	if _, err := c.BreedReveal(tmpl.BreedRevealInput{Species: "fox", Rarity: "common"}); err != nil {
		t.Errorf("breed_reveal must compose with no name on the wire: %v", err)
	}
	if _, err := c.BreedReveal(tmpl.BreedRevealInput{FamiliarDisplayName: "Tempo", Species: "fox", Rarity: "common"}); err != nil {
		t.Errorf("breed_reveal must compose with the hatched name: %v", err)
	}
	if _, err := c.SourceRevelation(tmpl.SourceRevelationInput{PreviewLLMTier: "pro"}); err != nil {
		t.Errorf("source_revelation must compose from wire fields alone: %v", err)
	}
}
