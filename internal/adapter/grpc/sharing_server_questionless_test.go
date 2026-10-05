// sharing_server_questionless_test.go — CHO-2174b.
//
// atom_projections becomes a CONSENT read-model first: every published atom is
// projected, and the question-display fields (revision_id / stem / question_type)
// are OPTIONAL enrichment. Two consequences must be pinned at the gRPC surface:
//
//  1. Consumers that genuinely need a QUESTION must keep their own precondition
//     and refuse EXPLICITLY — never run on a question-less projection by
//     accident, and never blame the caller's input for a projection's shape.
//     - ShareAtom renders a feed card → needs a stem.
//     - AuthorizeAtomUse mints a grant → atom_usage_grants.atom_revision_id is
//     NOT NULL, so it needs a pinned revision.
//     - AuthorizeLiveQuizAtoms arms a quiz → needs a question.
//
//  2. Consent MUST NOT WIDEN. A newly-projected PRIVATE atom is still refused
//     to a non-owner; it merely becomes visible to the gate.
package grpcadapter

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sharingv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/sharing/v1"

	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
)

const (
	qlOwner  = "01970000-0000-7000-9000-00000000ee01"
	qlOther  = "01970000-0000-7000-9000-00000000ee02" // non-owner
	qlAtom   = "01970000-0000-7000-8000-00000000ee03"
	qlTenant = "01970000-0000-7000-8000-00000000ee04"
	qlRev    = "01970000-0000-7000-8000-00000000ee05"
)

// questionLessServer seeds a projection carrying ONLY the consent facts — the
// CHO-2174b class: consent well-defined, no question revision.
func questionLessServer(visibility string) *SharingServer {
	projections := newFakeProjectionRepo()
	projections.seed(atom_projection.Projection{
		AtomID:          qlAtom,
		OwnerGCID:       qlOwner,
		PublishedAt:     time.Now().UTC(),
		ReuseVisibility: visibility,
		// RevisionID / Stem / QuestionType deliberately absent.
	})
	return New(Deps{
		Shares:      newFakeShareRepo(),
		Grants:      newFakeGrantRepo(),
		Projections: projections,
		Rules:       testRules(),
	})
}

// ── 1. Explicit, distinct refusals for question-needing consumers ────────────

// ShareAtom must refuse a question-less projection CLEARLY — it cannot render a
// feed card without a stem. The refusal must be FailedPrecondition (a fact about
// the ATOM), never InvalidArgument (which blames the caller's request), and it
// must name the cause.
func TestShareAtom_QuestionLessProjection_RefusedExplicitly(t *testing.T) {
	t.Parallel()
	srv := questionLessServer("private")

	_, err := srv.ShareAtom(context.Background(), &sharingv1.ShareAtomRequest{
		AuthorGcid:     qlOwner, // the OWNER — R1 passes; the refusal is about the question
		TenantId:       qlTenant,
		AtomId:         qlAtom,
		LicenseTerms:   sharingv1.LicenseTerms_LICENSE_TERMS_FREE,
		IdempotencyKey: "idem-ql-share",
	})
	if err == nil {
		t.Fatalf("ShareAtom() = nil error on a question-less projection; want an explicit refusal " +
			"(a feed card cannot be rendered without a stem)")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("ShareAtom() code = %v (%q); want FailedPrecondition — a question-less projection is a "+
			"fact about the ATOM, not a malformed request", st.Code(), st.Message())
	}
	if !strings.Contains(strings.ToLower(st.Message()), "question") {
		t.Fatalf("ShareAtom() message = %q; want it to NAME the cause (no question / stem) so the "+
			"refusal is deliberate, not accidental", st.Message())
	}
}

// AuthorizeAtomUse mints an AtomUsageGrant, and atom_usage_grants.atom_revision_id
// is NOT NULL — a grant physically cannot pin a revision that does not exist.
// The OWNER passes the consent gate, so this exercises the revision-pinning
// precondition in isolation: it must refuse loudly and name revision-pinning,
// never fabricate a revision and never surface as a caller InvalidArgument.
func TestAuthorizeAtomUse_QuestionLessProjection_OwnerRefusedOnRevisionPinning(t *testing.T) {
	t.Parallel()
	srv := questionLessServer("private")

	_, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:    qlOwner, // OWNER → consent leg passes
		TenantId:       qlTenant,
		AtomId:         qlAtom,
		Scope:          sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		IdempotencyKey: "idem-ql-auth",
	})
	if err == nil {
		t.Fatalf("AuthorizeAtomUse() = nil on a revision-less projection; want an explicit refusal " +
			"(atom_usage_grants.atom_revision_id is NOT NULL — nothing to pin)")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("AuthorizeAtomUse() code = %v (%q); want FailedPrecondition, NOT InvalidArgument "+
			"(the caller's request is fine — the projection has no revision)", st.Code(), st.Message())
	}
	if !strings.Contains(strings.ToLower(st.Message()), "revision") {
		t.Fatalf("AuthorizeAtomUse() message = %q; want it to NAME revision-pinning", st.Message())
	}
}

// A LiveQuiz arm needs a question to ask. A question-less atom must come back
// unusable with a NAMED reason (SC-009), not silently armed.
func TestAuthorizeLiveQuizAtoms_QuestionLessProjection_Unusable(t *testing.T) {
	t.Parallel()
	srv := questionLessServer("tenant") // tenant-visible: consent passes, question does not

	resp, err := srv.AuthorizeLiveQuizAtoms(context.Background(), &sharingv1.AuthorizeLiveQuizAtomsRequest{
		InstructorGcid: qlOther,
		TenantId:       qlTenant,
		AtomIds:        []string{qlAtom},
		IdempotencyKey: "idem-ql-lq",
	})
	if err != nil {
		// A hard error is an acceptable loud refusal too, but the shipped shape
		// reports per-atom usability — assert that shape when it returns.
		return
	}
	if len(resp.GetAtoms()) != 1 {
		t.Fatalf("AuthorizeLiveQuizAtoms() returned %d atoms; want 1", len(resp.GetAtoms()))
	}
	a := resp.GetAtoms()[0]
	if a.GetUsable() {
		t.Fatalf("AuthorizeLiveQuizAtoms() marked a QUESTION-LESS atom usable — a live quiz cannot " +
			"ask a question that does not exist")
	}
	if !strings.Contains(strings.ToLower(a.GetReason()), "question") {
		t.Fatalf("reason = %q; want it to NAME the missing question (SC-009)", a.GetReason())
	}
}

// ── 2. CONSENT NON-WIDENING at the gRPC gate ─────────────────────────────────

// THE PROOF, at the AuthorizeAtomUse surface: a newly-projected PRIVATE atom is
// still refused to a NON-OWNER. Before the fix the refusal was
// "atom not published or withdrawn (412)" (projection missing). After the fix
// the projection resolves — and the D2 audience gate must STILL refuse.
//
// Note the ordering requirement: the CONSENT refusal must fire BEFORE the
// revision-pinning refusal, so a non-owner never learns anything about an atom
// they have no consent to reuse.
func TestAuthorizeAtomUse_NewlyProjectedPrivateAtom_StillRefusedToNonOwner(t *testing.T) {
	t.Parallel()
	srv := questionLessServer("private")

	_, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:    qlOther, // NON-owner, no source share, no grant
		TenantId:       qlTenant,
		AtomId:         qlAtom,
		Scope:          sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		IdempotencyKey: "idem-ql-widen",
	})
	if err == nil {
		t.Fatalf("CONSENT WIDENED: AuthorizeAtomUse() minted a grant on a PRIVATE atom for a " +
			"non-owner. Projecting more atoms must never confer reuse — STOP.")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("AuthorizeAtomUse() code = %v (%q); want FailedPrecondition (audience gate refusal)",
			st.Code(), st.Message())
	}
	// The refusal must be the AUDIENCE refusal (ADR-229 D2), i.e. it must cite
	// the visibility — not a revision-pinning refusal, which would mean the
	// consent check was skipped or reordered behind it.
	msg := strings.ToLower(st.Message())
	if !strings.Contains(msg, "visib") && !strings.Contains(msg, "audience") {
		t.Fatalf("AuthorizeAtomUse() refused a non-owner with %q; want the ADR-229 D2 AUDIENCE "+
			"refusal. If this is the revision-pinning refusal, the consent gate is being "+
			"evaluated AFTER it — a non-owner must be refused on CONSENT first.", st.Message())
	}
}

// The 'friends' audience is not un-hidden (Amendment A1.4) — still closed.
func TestAuthorizeAtomUse_NewlyProjectedFriendsAtom_StillRefusedToNonOwner(t *testing.T) {
	t.Parallel()
	srv := questionLessServer("friends")

	_, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:    qlOther,
		TenantId:       qlTenant,
		AtomId:         qlAtom,
		Scope:          sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		IdempotencyKey: "idem-ql-friends",
	})
	if err == nil {
		t.Fatalf("CONSENT WIDENED: 'friends' audience must fail closed until A1.4 un-hides it")
	}
	statusIs(t, err, codes.FailedPrecondition)
}
