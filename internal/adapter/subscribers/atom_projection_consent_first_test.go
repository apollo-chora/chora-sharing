// atom_projection_consent_first_test.go — CHO-2174b.
//
// The projector must cache the CONSENT facts for EVERY published atom, and fill
// the question fields only when the event carries them. Before the fix, an
// atom.published.v1 without a current_revision_id was rejected by
// Projection.Validate() → the handler NACK'd → the atom was never projected →
// the ADR-229 gate 412'd on it forever (AuthorizeAtomUse → ErrNotFound →
// "atom not published or withdrawn").
package subscribers

import (
	"context"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
)

// questionLessPublishedEnv is a published.v1 carrying ONLY consent facts — the
// CHO-2174b class (19 live atoms: revisioned on the atom, no question revision).
func questionLessPublishedEnv() AtomPublishedEnvelope {
	return AtomPublishedEnvelope{
		EventID:         "01970000-0000-7000-a000-00000000f001",
		TenantID:        "01970000-0000-7000-a000-00000000f002",
		GCID:            "01970000-0000-7000-a000-00000000f003",
		AtomID:          "01970000-0000-7000-a000-00000000f004",
		AuthorGCID:      "01970000-0000-7000-a000-00000000f003",
		ReuseVisibility: "private",
		// RevisionID / Stem / QuestionType deliberately absent.
	}
}

// A published atom with NO question revision must still be PROJECTED — its
// consent facts (owner + reuse_visibility) are perfectly well-defined.
func TestHandleAtomPublished_QuestionLessAtom_IsProjected(t *testing.T) {
	sub, w, _ := newProjSub(t)
	env := questionLessPublishedEnv()

	if err := sub.HandleAtomPublished(context.Background(), env); err != nil {
		t.Fatalf("HandleAtomPublished() on a question-less atom = %v; want nil. The consent facts are "+
			"well-defined — refusing to project them makes the ADR-229 gate refuse a consent-valid "+
			"atom forever (CHO-2174b)", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.upserts) != 1 {
		t.Fatalf("upserts = %d; want 1 — the consent read-model must carry EVERY published atom, "+
			"question or not", len(w.upserts))
	}
	p := w.upserts[0]
	if p.OwnerGCID != env.AuthorGCID {
		t.Fatalf("projected owner_gcid = %q; want %q (the R1/consent attribution source)", p.OwnerGCID, env.AuthorGCID)
	}
	if p.ReuseVisibility != "private" {
		t.Fatalf("projected reuse_visibility = %q; want \"private\" (consent rides the event verbatim)", p.ReuseVisibility)
	}
	if p.RevisionID != "" {
		t.Fatalf("projected revision_id = %q; want empty (the event carried none — never fabricate one)", p.RevisionID)
	}
	if p.HasQuestion() {
		t.Fatalf("HasQuestion() = true on an event that carried no question fields")
	}
}

// When the event DOES carry the question fields they are filled — question data
// is optional ENRICHMENT, not something we drop.
func TestHandleAtomPublished_QuestionBearingAtom_EnrichmentPreserved(t *testing.T) {
	sub, w, _ := newProjSub(t)
	env := questionLessPublishedEnv()
	env.RevisionID = "01970000-0000-7000-a000-00000000f0aa"
	env.Stem = "What is 2+2?"
	env.QuestionType = "mcq"
	env.ReuseVisibility = "tenant"

	if err := sub.HandleAtomPublished(context.Background(), env); err != nil {
		t.Fatalf("HandleAtomPublished() = %v; want nil", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.upserts) != 1 {
		t.Fatalf("upserts = %d; want 1", len(w.upserts))
	}
	p := w.upserts[0]
	if !p.HasQuestion() {
		t.Fatalf("HasQuestion() = false though the event carried revision + stem")
	}
	if p.Stem != "What is 2+2?" || p.QuestionType != atom_projection.QuestionType("mcq") {
		t.Fatalf("question enrichment lost: stem=%q type=%q", p.Stem, p.QuestionType)
	}
	if p.RevisionID != env.RevisionID {
		t.Fatalf("revision_id = %q; want %q", p.RevisionID, env.RevisionID)
	}
}

// The CONSENT facts stay mandatory: an event with no author_gcid AND no envelope
// gcid cannot establish R1 ownership, so it must still NACK (fail loud) rather
// than project a consent-less row.
func TestHandleAtomPublished_NoOwner_StillNacks(t *testing.T) {
	sub, w, _ := newProjSub(t)
	env := questionLessPublishedEnv()
	env.GCID = ""
	env.AuthorGCID = ""

	if err := sub.HandleAtomPublished(context.Background(), env); err == nil {
		t.Fatalf("HandleAtomPublished() = nil with NO owner_gcid; want a loud error (R1 attribution " +
			"is a CONSENT fact and stays mandatory)")
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.upserts) != 0 {
		t.Fatalf("an owner-less atom was projected — consent-less rows must never reach the read-model")
	}
}
