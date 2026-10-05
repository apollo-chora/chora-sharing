package atom_projection

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// stubReader is an in-memory AtomProjectionReader for testing the read-port
// contract (ErrNotFound propagation, R1 attribution lookup).
type stubReader struct {
	byAtom map[string]Projection
}

func (s *stubReader) Get(_ context.Context, atomID string) (Projection, error) {
	p, ok := s.byAtom[atomID]
	if !ok {
		return Projection{}, ErrNotFound
	}
	return p, nil
}

func validProjection() Projection {
	return Projection{
		AtomID:            "00000000-0000-7000-8000-0000000000a1",
		RevisionID:       "00000000-0000-7000-8000-0000000000r1",
		OwnerGCID:        "00000000-0000-7000-8000-0000000000o1",
		AuthorDisplayName: "Ada Lovelace",
		Stem:             "What is the derivative of x^2?",
		QuestionType:      QuestionTypeMCQ,
	}
}

func TestProjection_Validate_AcceptsValid(t *testing.T) {
	if err := validProjection().Validate(); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

// Only the CONSENT facts are mandatory (CHO-2174b). revision_id was REMOVED from
// this list: requiring it made a consent-valid but question-less atom
// unprojectable, so the ADR-229 gate refused it forever. Its optionality is
// pinned in projection_consent_first_test.go.
func TestProjection_Validate_RejectsMissingR1Attribution(t *testing.T) {
	cases := []struct {
		name string
		mut  func(Projection) Projection
	}{
		{"missing atom_id", func(p Projection) Projection { p.AtomID = ""; return p }},
		{"missing owner_gcid (R1)", func(p Projection) Projection { p.OwnerGCID = ""; return p }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.mut(validProjection()).Validate()
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("expected ErrInvalidArgument, got %v", err)
			}
		})
	}
}

func TestProjection_StemPreview_TruncatesOnRuneBoundary(t *testing.T) {
	// Multi-byte stem (CJK) must not split a rune.
	cjk := strings.Repeat("数", 10) // 10 runes
	p := Projection{Stem: cjk}
	got := p.StemPreview(5)
	if got != strings.Repeat("数", 5) {
		t.Fatalf("expected 5 runes, got %q", got)
	}
	// Short stem returns whole.
	p.Stem = "short"
	if got := p.StemPreview(140); got != "short" {
		t.Fatalf("expected short stem unchanged, got %q", got)
	}
}

func TestAtomProjectionReader_Get_NotFoundIsHardRefusal(t *testing.T) {
	r := &stubReader{byAtom: map[string]Projection{
		"known": validProjection(),
	}}
	if _, err := r.Get(context.Background(), "known"); err != nil {
		t.Fatalf("known atom: expected nil, got %v", err)
	}
	_, err := r.Get(context.Background(), "unknown")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown atom: expected ErrNotFound, got %v", err)
	}
}

// TestValidReuseVisibility — the ADR-229 audience-label allowlist guards the
// event-fed copy before it reaches the CHECK-constrained column.
func TestValidReuseVisibility(t *testing.T) {
	for _, ok := range []string{"private", "friends", "tenant"} {
		if !ValidReuseVisibility(ok) {
			t.Errorf("ValidReuseVisibility(%q) = false; want true", ok)
		}
	}
	for _, bad := range []string{"", "everyone", "PRIVATE", "tenant "} {
		if ValidReuseVisibility(bad) {
			t.Errorf("ValidReuseVisibility(%q) = true; want false", bad)
		}
	}
}
