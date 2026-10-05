// Package profiler is the domain for the user interest profiler.
//
// A user writes a short "tell me about yourself" free-text bio. The LLM
// extracts structured interest tags from it, constrained to the taxonomy
// defined in docs/design/duel-system.md. The user can then manually edit
// the AI-generated tags. The profile (tags + bio) feeds the Meilisearch
// suggestion engine for duel matchmaking.
package profiler

import (
	"context"
	"errors"
	"strings"
	"time"
)

var ErrInvalidArgument = errors.New("profiler: invalid argument")

type TagCategory string

const (
	CategoryProgramming TagCategory = "programming"
	CategoryMathematics TagCategory = "mathematics"
	CategoryScience     TagCategory = "science"
	CategoryHumanities  TagCategory = "humanities"
	CategoryArts        TagCategory = "arts"
	CategoryLanguages   TagCategory = "languages"
)

var ValidCategories = map[TagCategory][]string{
	CategoryProgramming: {"inheritance", "polymorphism", "recursion", "async", "pointers", "functional_programming", "design_patterns"},
	CategoryMathematics: {"calculus", "linear_algebra", "statistics", "probability", "discrete_math", "number_theory", "topology"},
	CategoryScience:     {"physics", "chemistry", "biology", "astronomy", "earth_science", "neuroscience"},
	CategoryHumanities:  {"history", "philosophy", "literature", "linguistics", "sociology", "economics"},
	CategoryArts:        {"music_theory", "visual_arts", "digital_design", "film_studies", "creative_writing"},
	CategoryLanguages:   {"english", "mandarin", "spanish", "french", "german", "japanese"},
}

type InterestTag struct {
	Category TagCategory `json:"category"`
	Tag      string      `json:"tag"`
}

// ProficiencyLevel is the coarse learner-skill band the profiler infers
// from completed-course count + topic depth per category. It feeds the
// duel_atom_smith agent's difficulty selection (so a learner strong in
// mathematics gets harder math atoms) + the C+ profiler page.
type ProficiencyLevel string

const (
	ProficiencyBeginner     ProficiencyLevel = "beginner"
	ProficiencyIntermediate  ProficiencyLevel = "intermediate"
	ProficiencyAdvanced     ProficiencyLevel = "advanced"
)

// ValidProficiencyLevel reports whether s is one of the three bands.
// Used by the conjurer client to drop unknown per-category levels rather
// than fail the whole profile.
func ValidProficiencyLevel(s string) bool {
	switch ProficiencyLevel(s) {
	case ProficiencyBeginner, ProficiencyIntermediate, ProficiencyAdvanced:
		return true
	}
	return false
}

// Proficiency is the agent-inferred skill band, scoped per category.
// PerCategory maps each interest category (programming, mathematics,
// science, humanities, arts, languages) to its inferred level. An empty
// map is valid — a new user with no completed courses + a bio that
// yields no tags gets proficiency: {} (the smith agent treats empty as
// "unknown difficulty, default to beginner-level atoms").
//
// The Overall field was removed: "beginner in what?" was meaningless
// without category context. Per-category is the only meaningful signal
// (a learner can be advanced in mathematics + beginner in arts).
type Proficiency struct {
	PerCategory map[string]ProficiencyLevel `json:"per_category,omitempty"`
}

// ConjuredProfile is the agent-call result: the interest tags extracted
// from the bio plus the proficiency inferred from completed courses.
// "Conjured" because the tags + proficiency are LLM-generated, not
// user-asserted — the user can still edit tags afterwards.
type ConjuredProfile struct {
	Tags        []InterestTag
	Proficiency Proficiency
}

type Profile struct {
	GCID         string
	TenantID     string
	Bio          string
	DisplayName  string
	Tags         []InterestTag
	Proficiency  Proficiency
	CourseTitles []string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

func NewProfile(gcid, tenantID, bio string, courseTitles []string) (*Profile, error) {
	if strings.TrimSpace(gcid) == "" {
		return nil, ErrInvalidArgument
	}
	if strings.TrimSpace(tenantID) == "" {
		return nil, ErrInvalidArgument
	}
	now := time.Now().UTC()
	return &Profile{
		GCID:         gcid,
		TenantID:     tenantID,
		Bio:          bio,
		Tags:         []InterestTag{},
		CourseTitles: append([]string(nil), courseTitles...),
		CreatedAt:    now,
		UpdatedAt:    now,
	}, nil
}

func (p *Profile) SetTags(tags []InterestTag) {
	p.Tags = append([]InterestTag(nil), tags...)
	p.UpdatedAt = time.Now().UTC()
}

// SetProficiency records the agent-inferred proficiency band and bumps
// UpdatedAt, mirroring SetTags. Called from the conjurer client after a
// successful agent Conjure call.
func (p *Profile) SetProficiency(pr Proficiency) {
	p.Proficiency = pr
	p.UpdatedAt = time.Now().UTC()
}

func (p *Profile) AddTag(category TagCategory, tag string) bool {
	if !IsValidTag(category, tag) {
		return false
	}
	for _, existing := range p.Tags {
		if existing.Category == category && existing.Tag == tag {
			return false
		}
	}
	p.Tags = append(p.Tags, InterestTag{Category: category, Tag: tag})
	p.UpdatedAt = time.Now().UTC()
	return true
}

func (p *Profile) RemoveTag(category TagCategory, tag string) bool {
	for i, existing := range p.Tags {
		if existing.Category == category && existing.Tag == tag {
			p.Tags = append(p.Tags[:i], p.Tags[i+1:]...)
			p.UpdatedAt = time.Now().UTC()
			return true
		}
	}
	return false
}

func (p *Profile) FlatTags() []string {
	out := make([]string, 0, len(p.Tags))
	for _, t := range p.Tags {
		out = append(out, t.Tag)
	}
	return out
}

func (p *Profile) TagCategories() map[string][]string {
	out := make(map[string][]string)
	for _, t := range p.Tags {
		cat := string(t.Category)
		out[cat] = append(out[cat], t.Tag)
	}
	return out
}
// IsValidTag reports whether (category, tag) is admissible. The category
// MUST be one of the 6 taxonomy categories (programming/mathematics/
// science/humanities/arts/languages). The tag string is FREE-FORM — the
// agent (or the user, via the FE picker) may emit any non-empty slug
// (e.g. "golang", "rust", "system_design", "cooking") + it will be
// accepted. The closed per-category tag lists in ValidCategories are
// advisory examples, NOT an enforced vocabulary.
//
// Rationale: the original closed vocabulary (7 tags per category) was
// too narrow — a bio about "golang" yielded zero tags because "golang"
// wasn't in the programming list. Free-form tags within a category keep
// the category-scoped matchmaking signal (two "programming" learners
// still match) without blocking legitimate interests the taxonomy
// author didn't foresee. Canonicalisation (golang vs go vs Go) is a
// future Meilisearch-normalisation concern, not a validation concern.
func IsValidTag(category TagCategory, tag string) bool {
	if _, ok := ValidCategories[category]; !ok {
		return false
	}
	return strings.TrimSpace(tag) != ""
}
type ProfileRepo interface {
	SaveProfile(ctx context.Context, p *Profile) error
	GetProfile(ctx context.Context, gcid string) (*Profile, error)
	// ResolveDisplayNames batch-resolves display_name for a set of gcids.
	// Returns a map gcid→display_name; gcids with no profile or empty
	// display_name are absent. Used by the leaderboard handler to enrich
	// entries with human-readable names.
	ResolveDisplayNames(ctx context.Context, gcids []string) (map[string]string, error)
}
