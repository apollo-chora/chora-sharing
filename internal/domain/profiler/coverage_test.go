package profiler

import (
	"reflect"
	"testing"
	"time"
)

// TestValidProficiencyLevel asserts the band validator accepts the three
// canonical bands and rejects anything else (used by the conjurer client to
// drop unknown per-category levels).
func TestValidProficiencyLevel(t *testing.T) {
	cases := []struct {
		name  string
		level string
		want  bool
	}{
		{"beginner", string(ProficiencyBeginner), true},
		{"intermediate", string(ProficiencyIntermediate), true},
		{"advanced", string(ProficiencyAdvanced), true},
		{"unknown band", "expert", false},
		{"empty", "", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := ValidProficiencyLevel(tc.level); got != tc.want {
				t.Errorf("ValidProficiencyLevel(%q) = %v; want %v", tc.level, got, tc.want)
			}
		})
	}
}

func TestNewProfile_RejectsEmptyGCID(t *testing.T) {
	if _, err := NewProfile("   ", "tenant-1", "bio", nil); err == nil {
		t.Fatal("expected error for blank gcid")
	}
}

func TestNewProfile_RejectsEmptyTenant(t *testing.T) {
	if _, err := NewProfile("gcid-1", "", "bio", nil); err == nil {
		t.Fatal("expected error for blank tenantID")
	}
}

func TestNewProfile_CourseTitlesCopied(t *testing.T) {
	titles := []string{"math-101", "physics-101"}
	p, err := NewProfile("gcid-1", "tenant-1", "bio", titles)
	if err != nil {
		t.Fatalf("NewProfile: %v", err)
	}
	titles[0] = "mutated"
	if len(p.CourseTitles) != 2 || p.CourseTitles[0] != "math-101" {
		t.Error("NewProfile must copy courseTitles, not alias them")
	}
	if len(p.Tags) != 0 {
		t.Error("fresh profile must start with an empty tag list")
	}
	if p.CreatedAt.IsZero() || p.UpdatedAt.IsZero() {
		t.Error("timestamps must be set on a fresh profile")
	}
}

func TestProfile_SetTags_CopiesInputAndBumpsUpdatedAt(t *testing.T) {
	tags := []InterestTag{{Category: CategoryProgramming, Tag: "golang"}}
	p, err := NewProfile("gcid-1", "tenant-1", "bio", nil)
	if err != nil {
		t.Fatalf("NewProfile: %v", err)
	}
	before := p.UpdatedAt
	time.Sleep(time.Millisecond)
	p.SetTags(tags)
	if !p.UpdatedAt.After(before) {
		t.Error("SetTags must bump UpdatedAt")
	}
	// Mutating the caller's slice must not affect the profile (copied).
	tags[0] = InterestTag{Category: CategoryMathematics, Tag: "calculus"}
	if len(p.Tags) != 1 || p.Tags[0].Tag != "golang" {
		t.Errorf("SetTags must copy the input slice, got %v", p.Tags)
	}
}

func TestProfile_SetProficiency_BumpsUpdatedAt(t *testing.T) {
	p, err := NewProfile("gcid-1", "tenant-1", "bio", nil)
	if err != nil {
		t.Fatalf("NewProfile: %v", err)
	}
	before := p.UpdatedAt
	time.Sleep(time.Millisecond)
	pr := Proficiency{PerCategory: map[string]ProficiencyLevel{"programming": ProficiencyAdvanced}}
	p.SetProficiency(pr)
	if p.Proficiency.PerCategory["programming"] != ProficiencyAdvanced {
		t.Error("SetProficiency must record the inferred band")
	}
	if !p.UpdatedAt.After(before) {
		t.Error("SetProficiency must bump UpdatedAt")
	}
}

func TestProfile_RemoveTag(t *testing.T) {
	p, err := NewProfile("gcid-1", "tenant-1", "bio", nil)
	if err != nil {
		t.Fatalf("NewProfile: %v", err)
	}
	p.AddTag(CategoryProgramming, "golang")
	p.AddTag(CategoryMathematics, "calculus")

	if !p.RemoveTag(CategoryProgramming, "golang") {
		t.Fatal("RemoveTag of an existing (category, tag) must return true")
	}
	if len(p.Tags) != 1 || p.Tags[0].Tag != "calculus" {
		t.Errorf("expected only calculus to remain, got %v", p.Tags)
	}
	if p.RemoveTag(CategoryArts, "absent") {
		t.Error("RemoveTag of a missing (category, tag) must return false")
	}
	if len(p.Tags) != 1 {
		t.Error("failed RemoveTag must not mutate the tag list")
	}
}

func TestProfile_FlatTags(t *testing.T) {
	p, err := NewProfile("gcid-1", "tenant-1", "bio", nil)
	if err != nil {
		t.Fatalf("NewProfile: %v", err)
	}
	p.AddTag(CategoryProgramming, "golang")
	p.AddTag(CategoryScience, "physics")
	p.AddTag(CategoryProgramming, "rust")
	got := p.FlatTags()
	want := []string{"golang", "physics", "rust"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("FlatTags = %v; want %v", got, want)
	}
}

func TestProfile_TagCategories(t *testing.T) {
	p, err := NewProfile("gcid-1", "tenant-1", "bio", nil)
	if err != nil {
		t.Fatalf("NewProfile: %v", err)
	}
	p.AddTag(CategoryProgramming, "golang")
	p.AddTag(CategoryProgramming, "rust")
	p.AddTag(CategoryScience, "physics")
	got := p.TagCategories()
	want := map[string][]string{
		"programming": {"golang", "rust"},
		"science":     {"physics"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("TagCategories = %v; want %v", got, want)
	}
}