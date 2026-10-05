// projection_consent_first_test.go — CHO-2174b.
//
// atom_projections welded two unrelated concerns together: the CONSENT facts
// (owner_gcid + reuse_visibility + published-ness), which every published atom
// has, and the QUESTION-DISPLAY facts (revision_id + stem + question_type),
// which only a question-bearing atom has. Validate() required RevisionID, so an
// atom whose consent is perfectly well-defined could not be projected at all
// when it carried no question revision — and the ADR-229 gate then refused it
// (AuthorizeAtomUse → ErrNotFound → FailedPrecondition 412).
//
// These tests pin the consent-first model: the projection REQUIRES the consent
// facts and treats the question facts as OPTIONAL enrichment.
package atom_projection_test

import (
	"errors"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
)

const (
	cfAtom  = "01970000-0000-7000-c000-00000000d001"
	cfOwner = "01970000-0000-7000-c000-00000000d002"
	cfRev   = "01970000-0000-7000-c000-00000000d003"
)

// A question-less atom (no revision, no stem, no question_type) has fully
// well-defined CONSENT — it MUST validate, so the projector can cache it and
// the gate can see it. This is the defect: it used to fail on revision_id.
func TestValidate_QuestionLessProjection_IsValid(t *testing.T) {
	p := atom_projection.Projection{
		AtomID:          cfAtom,
		TenantID:        "01970000-0000-7000-c000-00000000d0ff",
		OwnerGCID:       cfOwner,
		ReuseVisibility: "private",
		// RevisionID / Stem / QuestionType deliberately absent.
	}
	if err := p.Validate(); err != nil {
		t.Fatalf("Validate() on a question-less but consent-complete projection = %v; want nil "+
			"(consent facts stand alone — question fields are optional enrichment)", err)
	}
}

// The CONSENT facts remain mandatory. owner_gcid is the R1 attribution source
// and the `own` leg of the ADR-229 disjunct — without it the gate cannot
// decide anything, so it must still fail loud.
func TestValidate_ConsentFactsStillRequired(t *testing.T) {
	tests := []struct {
		name string
		p    atom_projection.Projection
	}{
		{"missing atom_id", atom_projection.Projection{OwnerGCID: cfOwner}},
		{"missing owner_gcid", atom_projection.Projection{AtomID: cfAtom}},
		{"blank owner_gcid", atom_projection.Projection{AtomID: cfAtom, OwnerGCID: "   "}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.p.Validate()
			if err == nil {
				t.Fatalf("Validate() = nil; want ErrInvalidArgument (consent facts are mandatory)")
			}
			if !errors.Is(err, atom_projection.ErrInvalidArgument) {
				t.Fatalf("Validate() error = %v; want ErrInvalidArgument", err)
			}
		})
	}
}

// HasQuestion is the explicit predicate every question-needing consumer
// (ShareAtom feed card, grant revision-pinning, LiveQuiz arm) must gate on, so a
// question-less projection is refused DELIBERATELY rather than by accident.
//
// The discriminator is the REVISION, never the stem. Verified against the live
// DB (2026-07-14): stem is EMPTY on 100% of projected rows (97/97), so a
// stem-based predicate would refuse every real atom. revision_id resolves in
// chora_creation.question_revisions — its presence IS "this atom has a question".
func TestHasQuestion(t *testing.T) {
	tests := []struct {
		name string
		p    atom_projection.Projection
		want bool
	}{
		{
			name: "revision present (a real live projection: revision set, stem EMPTY)",
			p:    atom_projection.Projection{AtomID: cfAtom, OwnerGCID: cfOwner, RevisionID: cfRev},
			want: true,
		},
		{
			name: "revision + stem present",
			p:    atom_projection.Projection{AtomID: cfAtom, OwnerGCID: cfOwner, RevisionID: cfRev, Stem: "What is 2+2?"},
			want: true,
		},
		{"no revision (the CHO-2174b consent-only class)", atom_projection.Projection{AtomID: cfAtom, OwnerGCID: cfOwner}, false},
		{
			name: "stem but no revision — still question-less (stem is not the signal)",
			p:    atom_projection.Projection{AtomID: cfAtom, OwnerGCID: cfOwner, Stem: "What is 2+2?"},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.p.HasQuestion(); got != tc.want {
				t.Fatalf("HasQuestion() = %v; want %v", got, tc.want)
			}
		})
	}
}

// HasPinnedRevision gates the grant snapshot specifically: atom_usage_grants.
// atom_revision_id is NOT NULL, so a grant physically cannot be minted without
// a revision. A stem is irrelevant to pinning — keep the two predicates apart.
func TestHasPinnedRevision(t *testing.T) {
	withRev := atom_projection.Projection{AtomID: cfAtom, OwnerGCID: cfOwner, RevisionID: cfRev}
	if !withRev.HasPinnedRevision() {
		t.Fatalf("HasPinnedRevision() = false on a projection carrying a revision")
	}
	none := atom_projection.Projection{AtomID: cfAtom, OwnerGCID: cfOwner}
	if none.HasPinnedRevision() {
		t.Fatalf("HasPinnedRevision() = true on a revision-less projection")
	}
}
