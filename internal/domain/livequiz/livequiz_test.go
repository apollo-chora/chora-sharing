package livequiz

import (
	"context"
	"errors"
	"testing"
)

// --- stubQuizGenerator: in-memory QuizGenerator double ---

type stubQuizGenerator struct {
	questions []GeneratedQuestion
	err       error
}

func (s *stubQuizGenerator) GenerateQuizQuestions(_ context.Context, instructorGCID, tenantID, topic string, topicTags []string, questionCount int) ([]GeneratedQuestion, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.questions, nil
}

// --- IsLiveQuizAtomUsable tests (pure arm-time gate logic) ---
// ADR-229 WS-3: the disjunct is own ∪ tenant-visible ∪ granted (the middle
// leg is the projection's reuse_visibility='tenant' audience consent, NOT a
// feed-share existence — mirrors the WS-2 picker AuthorizeAtomUse).

func TestIsLiveQuizAtomUsable_OwnAtomAlwaysUsable(t *testing.T) {
	// own-atom usable regardless of tenant-visibility/grant state.
	if !IsLiveQuizAtomUsable(true, false, false) {
		t.Error("own atom should be usable even without tenant-visibility or grant")
	}
	if !IsLiveQuizAtomUsable(true, true, true) {
		t.Error("own atom should be usable")
	}
}

func TestIsLiveQuizAtomUsable_TenantVisible(t *testing.T) {
	if !IsLiveQuizAtomUsable(false, true, false) {
		t.Error("tenant-visible atom should be usable (audience consent)")
	}
}

func TestIsLiveQuizAtomUsable_NonOwnerWithGrant(t *testing.T) {
	if !IsLiveQuizAtomUsable(false, false, true) {
		t.Error("non-owner atom with active grant should be usable")
	}
}

func TestIsLiveQuizAtomUsable_TenantVisibleAndGrant(t *testing.T) {
	if !IsLiveQuizAtomUsable(false, true, true) {
		t.Error("tenant-visible atom with grant should be usable")
	}
}

func TestIsLiveQuizAtomUsable_NonOwnerWithoutTenantVisibleOrGrant(t *testing.T) {
	if IsLiveQuizAtomUsable(false, false, false) {
		t.Error("non-owner atom without tenant-visibility or grant should NOT be usable")
	}
}

func TestIsLiveQuizAtomUsable_TruthTable(t *testing.T) {
	// Full truth table: (isOwn, tenantVisible, hasGrant) -> usable.
	cases := []struct {
		isOwn         bool
		tenantVisible bool
		hasGrant      bool
		want          bool
	}{
		{true, false, false, true},   // own
		{true, true, false, true},    // own + tenant-visible
		{true, false, true, true},    // own + grant
		{true, true, true, true},     // own + both
		{false, true, false, true},   // tenant-visible only
		{false, false, true, true},   // grant only
		{false, true, true, true},    // both
		{false, false, false, false}, // neither
	}
	for _, tc := range cases {
		got := IsLiveQuizAtomUsable(tc.isOwn, tc.tenantVisible, tc.hasGrant)
		if got != tc.want {
			t.Errorf("IsLiveQuizAtomUsable(%v, %v, %v) = %v, want %v", tc.isOwn, tc.tenantVisible, tc.hasGrant, got, tc.want)
		}
	}
}

// --- GeneratedQuestion.Validate tests ---

func validQuestion() GeneratedQuestion {
	return GeneratedQuestion{
		AtomID:          "atom-1",
		RevisionID:      "rev-1",
		Stem:            "What is 2+2?",
		Options:         []string{"3", "4", "5"},
		CorrectOptionID: "4",
		TimerSeconds:    30,
		Points:          10,
	}
}

func TestGeneratedQuestion_Validate_AcceptsValid(t *testing.T) {
	if err := validQuestion().Validate(); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestGeneratedQuestion_Validate_RejectsMissingFields(t *testing.T) {
	cases := []struct {
		name string
		mut  func(GeneratedQuestion) GeneratedQuestion
	}{
		{"missing atom_id", func(q GeneratedQuestion) GeneratedQuestion { q.AtomID = ""; return q }},
		{"missing revision_id", func(q GeneratedQuestion) GeneratedQuestion { q.RevisionID = ""; return q }},
		{"missing stem", func(q GeneratedQuestion) GeneratedQuestion { q.Stem = ""; return q }},
		{"empty options", func(q GeneratedQuestion) GeneratedQuestion { q.Options = nil; return q }},
		{"correct_option not in options", func(q GeneratedQuestion) GeneratedQuestion { q.CorrectOptionID = "99"; return q }},
		{"zero timer", func(q GeneratedQuestion) GeneratedQuestion { q.TimerSeconds = 0; return q }},
		{"negative timer", func(q GeneratedQuestion) GeneratedQuestion { q.TimerSeconds = -1; return q }},
		{"zero points", func(q GeneratedQuestion) GeneratedQuestion { q.Points = 0; return q }},
		{"negative points", func(q GeneratedQuestion) GeneratedQuestion { q.Points = -1; return q }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.mut(validQuestion()).Validate()
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("%s: expected ErrInvalidArgument, got %v", tc.name, err)
			}
		})
	}
}

func TestGeneratedQuestion_Validate_AcceptsSingleOption(t *testing.T) {
	q := validQuestion()
	q.Options = []string{"4"}
	q.CorrectOptionID = "4"
	if err := q.Validate(); err != nil {
		t.Fatalf("single option should be valid, got %v", err)
	}
}

// --- QuizGenerator port tests ---

func TestStubQuizGenerator_ReturnsQuestions(t *testing.T) {
	gen := &stubQuizGenerator{
		questions: []GeneratedQuestion{validQuestion(), validQuestion()},
	}
	questions, err := gen.GenerateQuizQuestions(context.Background(), "instructor-1", "tenant-1", "math", []string{"algebra"}, 2)
	if err != nil {
		t.Fatalf("GenerateQuizQuestions: %v", err)
	}
	if len(questions) != 2 {
		t.Fatalf("expected 2 questions, got %d", len(questions))
	}
}

func TestStubQuizGenerator_PropagatesError(t *testing.T) {
	gen := &stubQuizGenerator{err: ErrQuizGeneratorNotConfigured}
	_, err := gen.GenerateQuizQuestions(context.Background(), "i", "t", "topic", nil, 1)
	if !errors.Is(err, ErrQuizGeneratorNotConfigured) {
		t.Errorf("expected ErrQuizGeneratorNotConfigured, got %v", err)
	}
}
