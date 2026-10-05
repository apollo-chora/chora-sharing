// Package post_template_test holds the remaining coverage specs for the
// milestone template composer (empty-species degradation).
package post_template_test

import (
	"strings"
	"testing"

	tmpl "github.com/apollo-chora/chora-sharing/internal/domain/post_template"
)

// speciesPhrase must never render "a rare " with a hole: when the species is
// absent on the wire (off-contract decode), it degrades to "companion".
func TestBreedRevealTemplate_EmptySpeciesDegradesToCompanion(t *testing.T) {
	t.Parallel()
	c := tmpl.NewComposer(tmpl.DefaultTemplates())
	out, err := c.BreedReveal(tmpl.BreedRevealInput{
		FamiliarDisplayName: "Foxy",
		Species:             "   ",
		Rarity:              "rare",
	})
	if err != nil {
		t.Fatalf("BreedReveal: %v", err)
	}
	if !strings.Contains(out, "a rare companion") {
		t.Fatalf("expected degradation to \"a rare companion\", got %q", out)
	}
}

// render surfaces template EXECUTION failures (as opposed to parse failures):
// a template that parses but references a template that does not exist errors
// at Execute time.
func TestComposer_TemplateExecuteFailureSurfacesError(t *testing.T) {
	t.Parallel()
	c := tmpl.NewComposer(tmpl.Templates{
		StageUp:          `{{ template "missing" . }}`,
		BreedReveal:      `{{ template "missing" . }}`,
		SourceRevelation: `{{ template "missing" . }}`,
	})
	if _, err := c.StageUp(tmpl.StageUpInput{StageToName: "fledgling"}); err == nil {
		t.Fatal("expected an error when the template references a missing sub-template at Execute time")
	}
}