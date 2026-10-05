package profiler

import "testing"

// TestIsValidTag_FreeFormTagString is the regression test for the
// taxonomy-loosening change: IsValidTag must accept ANY non-empty tag
// string within a valid category, not just the closed vocabulary in
// ValidCategories. The original closed vocabulary rejected "golang",
// "rust", "system_design" — a bio about those topics yielded zero tags.
func TestIsValidTag_FreeFormTagString(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		category TagCategory
		tag      string
		want     bool
	}{
		// Closed-vocabulary tags still pass (backwards compatible).
		{"known tag in programming", CategoryProgramming, "recursion", true},
		{"known tag in science", CategoryScience, "astronomy", true},

		// Free-form tags now pass — the regression case. "golang" was
		// the motivating example: a bio "i love golang" used to yield
		// zero tags because "golang" wasn't in the programming list.
		{"golang in programming", CategoryProgramming, "golang", true},
		{"rust in programming", CategoryProgramming, "rust", true},
		{"system_design in programming", CategoryProgramming, "system_design", true},
		{"cooking in arts", CategoryArts, "cooking", true},
		{"basketball in humanities", CategoryHumanities, "basketball", true},

		// Empty/whitespace tags are rejected (not a meaningful interest).
		{"empty tag", CategoryProgramming, "", false},
		{"whitespace-only tag", CategoryProgramming, "   ", false},

		// Unknown categories are rejected — the 6-category structure is
		// the matchmaking signal + must stay enforced. "sports" isn't a
		// category, so even a non-empty tag is dropped.
		{"unknown category", TagCategory("sports"), "basketball", false},
		{"unknown category empty tag", TagCategory("fictional"), "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := IsValidTag(tc.category, tc.tag)
			if got != tc.want {
				t.Errorf("IsValidTag(%q, %q) = %v; want %v", tc.category, tc.tag, got, tc.want)
			}
		})
	}
}

// TestAddTag_AcceptsFreeFormTag asserts the Profile.AddTag domain method
// accepts free-form tags (it delegates to IsValidTag). A user adding
// "golang" to the programming category via the FE picker must succeed.
func TestAddTag_AcceptsFreeFormTag(t *testing.T) {
	t.Parallel()
	p, err := NewProfile("gcid-1", "tenant-1", "bio", nil)
	if err != nil {
		t.Fatalf("NewProfile: %v", err)
	}
	if !p.AddTag(CategoryProgramming, "golang") {
		t.Error("AddTag(programming, golang) = false; want true (free-form tag must be accepted)")
	}
	if !p.AddTag(CategoryProgramming, "rust") {
		t.Error("AddTag(programming, rust) = false; want true")
	}
	// Duplicate (category, tag) is still rejected — idempotency invariant.
	if p.AddTag(CategoryProgramming, "golang") {
		t.Error("AddTag(programming, golang) twice = true; want false (duplicate)")
	}
	if len(p.Tags) != 2 {
		t.Errorf("len(Tags) = %d; want 2 (golang + rust)", len(p.Tags))
	}
}

// TestAddTag_RejectsUnknownCategory asserts the category gate still
// holds — free-form tags are allowed, but the 6-category structure is
// the matchmaking signal + must stay enforced.
func TestAddTag_RejectsUnknownCategory(t *testing.T) {
	t.Parallel()
	p, err := NewProfile("gcid-1", "tenant-1", "bio", nil)
	if err != nil {
		t.Fatalf("NewProfile: %v", err)
	}
	if p.AddTag(TagCategory("sports"), "basketball") {
		t.Error("AddTag(sports, basketball) = true; want false (unknown category)")
	}
	if len(p.Tags) != 0 {
		t.Errorf("len(Tags) = %d; want 0 (unknown category must not add)", len(p.Tags))
	}
}
