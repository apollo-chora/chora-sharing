// converters_test.go — statement coverage for the pure proto<->domain
// converter helpers + the AuthorizeLiveQuizAtoms guard branches that the
// dedicated WS-2/WS-3 files don't reach (unwired deps, request validation,
// dedup, projection failures).
package grpcadapter

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	sharingv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/sharing/v1"

	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
	"github.com/apollo-chora/chora-sharing/internal/domain/grant"
	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
	"github.com/apollo-chora/chora-sharing/internal/domain/post"
	"github.com/apollo-chora/chora-sharing/internal/domain/reaction"
	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

// timestamppbNow is the timing helper used by the RPC tests.
func timestamppbNow() *timestamppb.Timestamp { return timestamppb.Now() }

// -----------------------------------------------------------------------------
// Visibility converters
// -----------------------------------------------------------------------------

func TestProtoVisibilityToDomain(t *testing.T) {
	t.Parallel()
	cases := map[sharingv1.PostVisibility]post.Visibility{
		sharingv1.PostVisibility_POST_VISIBILITY_FOLLOWERS:  post.VisibilityTenant,
		sharingv1.PostVisibility_POST_VISIBILITY_CLASS:      post.VisibilityTenant,
		sharingv1.PostVisibility_POST_VISIBILITY_PRIVATE:    post.VisibilityPrivate,
		sharingv1.PostVisibility_POST_VISIBILITY_PUBLIC:     post.VisibilityPublic,
		sharingv1.PostVisibility_POST_VISIBILITY_UNSPECIFIED: post.VisibilityPublic,
		sharingv1.PostVisibility(99):                        post.VisibilityPublic,
	}
	for in, want := range cases {
		if got := protoVisibilityToDomain(in); got != want {
			t.Errorf("protoVisibilityToDomain(%v)=%q want %q", in, got, want)
		}
	}
}

func TestDomainVisibilityToProto(t *testing.T) {
	t.Parallel()
	cases := map[post.Visibility]sharingv1.PostVisibility{
		post.VisibilityTenant:  sharingv1.PostVisibility_POST_VISIBILITY_FOLLOWERS,
		post.VisibilityPrivate: sharingv1.PostVisibility_POST_VISIBILITY_PRIVATE,
		post.VisibilityPublic:  sharingv1.PostVisibility_POST_VISIBILITY_PUBLIC,
		post.Visibility("x"):   sharingv1.PostVisibility_POST_VISIBILITY_UNSPECIFIED,
	}
	for in, want := range cases {
		if got := domainVisibilityToProto(in); got != want {
			t.Errorf("domainVisibilityToProto(%q)=%v want %v", in, got, want)
		}
	}
}

// -----------------------------------------------------------------------------
// Reaction kind converters
// -----------------------------------------------------------------------------

func TestProtoReactionKindToDomain(t *testing.T) {
	t.Parallel()
	cases := map[sharingv1.ReactionKind]reaction.Type{
		sharingv1.ReactionKind_REACTION_KIND_LIKE:        reaction.TypeLike,
		sharingv1.ReactionKind_REACTION_KIND_CLAP:        reaction.TypeInspired,
		sharingv1.ReactionKind_REACTION_KIND_STAR:        reaction.TypeInspired,
		sharingv1.ReactionKind_REACTION_KIND_INSIGHTFUL:  reaction.TypeInsightful,
		sharingv1.ReactionKind_REACTION_KIND_UNSPECIFIED: "",
	}
	for in, want := range cases {
		if got := protoReactionKindToDomain(in); got != want {
			t.Errorf("protoReactionKindToDomain(%v)=%q want %q", in, got, want)
		}
	}
}

func TestDomainReactionKindToProto(t *testing.T) {
	t.Parallel()
	cases := map[reaction.Type]sharingv1.ReactionKind{
		reaction.TypeLike:       sharingv1.ReactionKind_REACTION_KIND_LIKE,
		reaction.TypeInsightful: sharingv1.ReactionKind_REACTION_KIND_INSIGHTFUL,
		reaction.TypeInspired:   sharingv1.ReactionKind_REACTION_KIND_CLAP,
		reaction.TypeCurious:    sharingv1.ReactionKind_REACTION_KIND_STAR,
		reaction.Type("odd"):    sharingv1.ReactionKind_REACTION_KIND_UNSPECIFIED,
	}
	for in, want := range cases {
		if got := domainReactionKindToProto(in); got != want {
			t.Errorf("domainReactionKindToProto(%q)=%v want %v", in, got, want)
		}
	}
}

// -----------------------------------------------------------------------------
// License + royalty converters
// -----------------------------------------------------------------------------

func TestProtoLicenseToDomain(t *testing.T) {
	t.Parallel()
	cases := map[sharingv1.LicenseTerms]atom_share.LicenseTerms{
		sharingv1.LicenseTerms_LICENSE_TERMS_FREE:       atom_share.LicenseFree,
		sharingv1.LicenseTerms_LICENSE_TERMS_ROYALTY_PCT: atom_share.LicenseRoyaltyPct,
		sharingv1.LicenseTerms_LICENSE_TERMS_ROYALTY_FIXED: atom_share.LicenseRoyaltyFixed,
		sharingv1.LicenseTerms_LICENSE_TERMS_CC_BY_SA:   atom_share.LicenseCCBySA,
		sharingv1.LicenseTerms_LICENSE_TERMS_CC_ND:      atom_share.LicenseCCND,
		sharingv1.LicenseTerms_LICENSE_TERMS_UNSPECIFIED: "",
	}
	for in, want := range cases {
		if got := protoLicenseToDomain(in); got != want {
			t.Errorf("protoLicenseToDomain(%v)=%q want %q", in, got, want)
		}
	}
}

func TestDomainLicenseToProto(t *testing.T) {
	t.Parallel()
	cases := map[atom_share.LicenseTerms]sharingv1.LicenseTerms{
		atom_share.LicenseFree:        sharingv1.LicenseTerms_LICENSE_TERMS_FREE,
		atom_share.LicenseRoyaltyPct:  sharingv1.LicenseTerms_LICENSE_TERMS_ROYALTY_PCT,
		atom_share.LicenseRoyaltyFixed: sharingv1.LicenseTerms_LICENSE_TERMS_ROYALTY_FIXED,
		atom_share.LicenseCCBySA:      sharingv1.LicenseTerms_LICENSE_TERMS_CC_BY_SA,
		atom_share.LicenseCCND:        sharingv1.LicenseTerms_LICENSE_TERMS_CC_ND,
		atom_share.LicenseTerms("x"):  sharingv1.LicenseTerms_LICENSE_TERMS_UNSPECIFIED,
	}
	for in, want := range cases {
		if got := domainLicenseToProto(in); got != want {
			t.Errorf("domainLicenseToProto(%q)=%v want %v", in, got, want)
		}
	}
}

func TestProtoRoyaltyRateToDomain(t *testing.T) {
	t.Parallel()
	if got := protoRoyaltyRateToDomain(nil); got != (atom_share.RoyaltyRate{}) {
		t.Errorf("nil rate → %+v, want zero value", got)
	}
	got := protoRoyaltyRateToDomain(&sharingv1.RoyaltyRate{Kind: "pct", Value: 0.1})
	if got != (atom_share.RoyaltyRate{Kind: "pct", Value: 0.1}) {
		t.Errorf("rate → %+v, want {pct 0.1}", got)
	}
}

func TestDomainRoyaltyRateToProto(t *testing.T) {
	t.Parallel()
	got := domainRoyaltyRateToProto(atom_share.RoyaltyRate{Kind: "fixed_per_use", Value: 5})
	if got.GetKind() != "fixed_per_use" || got.GetValue() != 5 {
		t.Errorf("rate → %+v, want {fixed_per_use 5}", got)
	}
}

// -----------------------------------------------------------------------------
// Grant scope + status converters
// -----------------------------------------------------------------------------

func TestProtoGrantScopeToDomain(t *testing.T) {
	t.Parallel()
	cases := map[sharingv1.GrantScope]grant.Scope{
		sharingv1.GrantScope_GRANT_SCOPE_TEST_SET:   grant.ScopeTestSet,
		sharingv1.GrantScope_GRANT_SCOPE_DUEL:       grant.ScopeDuel,
		sharingv1.GrantScope_GRANT_SCOPE_LIVE_QUIZ:  grant.ScopeLiveQuiz,
		sharingv1.GrantScope_GRANT_SCOPE_COLLECTION: grant.ScopeCollection,
		sharingv1.GrantScope_GRANT_SCOPE_UNLIMITED:  grant.ScopeUnlimited,
		sharingv1.GrantScope_GRANT_SCOPE_UNSPECIFIED: "",
	}
	for in, want := range cases {
		if got := protoGrantScopeToDomain(in); got != want {
			t.Errorf("protoGrantScopeToDomain(%v)=%q want %q", in, got, want)
		}
	}
}

func TestDomainGrantStatusToProto(t *testing.T) {
	t.Parallel()
	cases := map[grant.Status]sharingv1.GrantStatus{
		grant.StatusActive:  sharingv1.GrantStatus_GRANT_STATUS_ACTIVE,
		grant.StatusRevoked: sharingv1.GrantStatus_GRANT_STATUS_REVOKED,
		grant.StatusExpired: sharingv1.GrantStatus_GRANT_STATUS_EXPIRED,
		grant.Status("zz"):  sharingv1.GrantStatus_GRANT_STATUS_UNSPECIFIED,
	}
	for in, want := range cases {
		if got := domainGrantStatusToProto(in); got != want {
			t.Errorf("domainGrantStatusToProto(%q)=%v want %v", in, got, want)
		}
	}
}

// -----------------------------------------------------------------------------
// Leaderboard helpers
// -----------------------------------------------------------------------------

func TestProtoLeaderboardScope(t *testing.T) {
	t.Parallel()
	kind, id, err := protoLeaderboardScope(sharingv1.LeaderboardScope_LEADERBOARD_SCOPE_GLOBAL, "")
	if err != nil || kind != leaderboard.ScopeGlobal || id != "" {
		t.Errorf("GLOBAL → (%q,%q,%v), want (global,\"\",nil)", kind, id, err)
	}
	kind, id, err = protoLeaderboardScope(sharingv1.LeaderboardScope_LEADERBOARD_SCOPE_TENANT, "")
	if err != nil || kind != leaderboard.ScopeTenant || id != "" {
		t.Errorf("TENANT → (%q,%q,%v), want (tenant,\"\",nil)", kind, id, err)
	}
	kind, id, err = protoLeaderboardScope(sharingv1.LeaderboardScope_LEADERBOARD_SCOPE_COURSE, "cohort-1")
	if err != nil || kind != leaderboard.ScopeCohort || id != "cohort-1" {
		t.Errorf("COURSE+target → (%q,%q,%v), want (cohort,cohort-1,nil)", kind, id, err)
	}
	kind, id, err = protoLeaderboardScope(sharingv1.LeaderboardScope_LEADERBOARD_SCOPE_CLASS, "class-1")
	if err != nil || kind != leaderboard.ScopeCohort || id != "class-1" {
		t.Errorf("CLASS+target → (%q,%q,%v), want (cohort,class-1,nil)", kind, id, err)
	}
	for name, in := range map[string]sharingv1.LeaderboardScope{
		"course no target": sharingv1.LeaderboardScope_LEADERBOARD_SCOPE_COURSE,
		"class no target":  sharingv1.LeaderboardScope_LEADERBOARD_SCOPE_CLASS,
		"unspecified":      sharingv1.LeaderboardScope_LEADERBOARD_SCOPE_UNSPECIFIED,
	} {
		if _, _, err := protoLeaderboardScope(in, "  "); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: expected InvalidArgument, got %v", name, err)
		}
	}
}

func TestProtoLeaderboardPeriod(t *testing.T) {
	t.Parallel()
	if got := protoLeaderboardPeriod(nil, nil, testRules()); got != leaderboard.PeriodAllTime {
		t.Errorf("nil window → %q, want all-time", got)
	}
	if got := protoLeaderboardPeriod(timestamppbNow(), nil, testRules()); got != leaderboard.PeriodMonthly {
		t.Errorf("start-only → %q, want monthly", got)
	}
	if got := protoLeaderboardPeriod(nil, timestamppbNow(), testRules()); got != leaderboard.PeriodMonthly {
		t.Errorf("end-only → %q, want monthly", got)
	}
	if got := protoLeaderboardPeriod(timestamppbNow(), timestamppbNow(), testRules()); got != leaderboard.PeriodMonthly {
		t.Errorf("full window → %q, want monthly", got)
	}
}

// -----------------------------------------------------------------------------
// Struct helpers
// -----------------------------------------------------------------------------

func TestEdgeToProto(t *testing.T) {
	t.Parallel()
	if got := edgeToProto(nil); got != nil {
		t.Errorf("nil edge → %+v, want nil", got)
	}
	now := time.Now().UTC()
	got := edgeToProto(&social.Edge{FollowerGCID: "a", FolloweeGCID: "b", TenantID: "t", CreatedAt: now})
	if got.GetFollowerGcid() != "a" || got.GetFolloweeGcid() != "b" || got.GetTenantId() != "t" {
		t.Errorf("edge mismatch: %+v", got)
	}
	if !got.GetFollowedAt().AsTime().Equal(now) {
		t.Errorf("followed_at mismatch: %v vs %v", got.GetFollowedAt(), now)
	}
}

func TestPostToProto(t *testing.T) {
	t.Parallel()
	if got := postToProto(nil, 3); got != nil {
		t.Errorf("nil post → %+v, want nil", got)
	}
	deletedAt := time.Now().UTC()
	got := postToProto(&post.Post{
		ID: "p1", TenantID: "t", AuthorGCID: "a", Body: "b", AtomID: "atom-1",
		Visibility: post.VisibilityPrivate, PostedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(), DeletedAt: &deletedAt,
	}, 7)
	if got.GetPostId() != "p1" || got.GetReactionCount() != 7 || !got.GetIsRemoved() {
		t.Errorf("post mapping mismatch: %+v", got)
	}
	if got.GetVisibility() != sharingv1.PostVisibility_POST_VISIBILITY_PRIVATE {
		t.Errorf("visibility=%v want PRIVATE", got.GetVisibility())
	}
}

func TestReactionToProto(t *testing.T) {
	t.Parallel()
	if got := reactionToProto(nil); got != nil {
		t.Errorf("nil reaction → %+v, want nil", got)
	}
	got := reactionToProto(&reaction.Reaction{
		ID: "rx", PostID: "p", GCID: "g", TenantID: "t",
		Type: reaction.TypeCurious, CreatedAt: time.Now().UTC(),
	})
	if got.GetReactionId() != "rx" || got.GetKind() != sharingv1.ReactionKind_REACTION_KIND_STAR {
		t.Errorf("reaction mapping mismatch: %+v", got)
	}
}

func TestShareToCard(t *testing.T) {
	t.Parallel()
	royal := shareToCard(atom_share.Share{
		FeedEntryID: "fe", OwnerGCID: "o", AuthorDisplayName: "Ada", AtomID: "a",
		RevisionID: "r", StemPreview: "Stem", QuestionType: "mcq",
		Options: []string{"x"}, Caption: "c",
		License: atom_share.LicenseRoyaltyPct,
		Rate:    atom_share.RoyaltyRate{Kind: "pct", Value: 0.1},
		CreatedAt: time.Now().UTC(),
	})
	if royal.GetRoyaltyRate() == nil {
		t.Error("royalty card missing rate snapshot")
	}
	if royal.GetAuthorGcid() != "o" || royal.GetAtomStemPreview() != "Stem" {
		t.Errorf("card mapping mismatch: %+v", royal)
	}
	free := shareToCard(atom_share.Share{
		FeedEntryID: "fe2", License: atom_share.LicenseFree, CreatedAt: time.Now().UTC(),
	})
	if free.GetRoyaltyRate() != nil {
		t.Error("free card must not carry a rate snapshot")
	}
}

func TestDedupAtoms(t *testing.T) {
	t.Parallel()
	got := dedupAtoms([]string{"a", "", " b ", "a", "c", " b ", "a", ""})
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("dedupAtoms → %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("dedupAtoms → %v, want %v", got, want)
		}
	}
	if got := dedupAtoms(nil); len(got) != 0 {
		t.Errorf("dedupAtoms(nil) → %v, want empty", got)
	}
}

// -----------------------------------------------------------------------------
// AuthorizeLiveQuizAtoms guard branches
// -----------------------------------------------------------------------------

func TestAuthorizeLiveQuizAtoms_NotWired_Unimplemented(t *testing.T) {
	t.Parallel()
	t.Run("nilProjections", func(t *testing.T) {
		_, err := New(Deps{Grants: newFakeGrantRepo()}).AuthorizeLiveQuizAtoms(context.Background(), &sharingv1.AuthorizeLiveQuizAtomsRequest{
			InstructorGcid: "i", TenantId: "t", AtomIds: []string{"a"},
			IdempotencyKey: "k",
		})
		statusIs(t, err, codes.Unimplemented)
	})
	t.Run("nilGrants", func(t *testing.T) {
		_, err := New(Deps{Projections: newFakeProjectionRepo()}).AuthorizeLiveQuizAtoms(context.Background(), &sharingv1.AuthorizeLiveQuizAtomsRequest{
			InstructorGcid: "i", TenantId: "t", AtomIds: []string{"a"},
			IdempotencyKey: "k",
		})
		statusIs(t, err, codes.Unimplemented)
	})
}

func TestAuthorizeLiveQuizAtoms_InvalidInput(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Projections: newFakeProjectionRepo(), Grants: newFakeGrantRepo()})
	for name, req := range map[string]*sharingv1.AuthorizeLiveQuizAtomsRequest{
		"nil request":        nil,
		"all missing":        {},
		"no instructor":      {TenantId: "t", AtomIds: []string{"a"}, IdempotencyKey: "k"},
		"no tenant":          {InstructorGcid: "i", AtomIds: []string{"a"}, IdempotencyKey: "k"},
		"no idempotency":     {InstructorGcid: "i", TenantId: "t", AtomIds: []string{"a"}},
		"no atom ids":        {InstructorGcid: "i", TenantId: "t", IdempotencyKey: "k"},
		"whitespace atom ids": {InstructorGcid: "i", TenantId: "t", AtomIds: []string{"  ", ""}, IdempotencyKey: "k"},
	} {
		if _, err := srv.AuthorizeLiveQuizAtoms(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: expected InvalidArgument, got %v", name, err)
		}
	}
}

func TestAuthorizeLiveQuizAtoms_DedupAtomIDs(t *testing.T) {
	t.Parallel()
	projections := newFakeProjectionRepo()
	projections.seed(atom_projectionSeed())
	srv := New(Deps{Projections: projections, Grants: newFakeGrantRepo()})
	resp, err := srv.AuthorizeLiveQuizAtoms(context.Background(), &sharingv1.AuthorizeLiveQuizAtomsRequest{
		InstructorGcid: ws3Owner,
		TenantId:       ws3Tenant,
		AtomIds:        []string{ws3Atom, ws3Atom, "", "  ", ws3Atom},
		IdempotencyKey: "arm-dedup",
	})
	if err != nil {
		t.Fatalf("AuthorizeLiveQuizAtoms: %v", err)
	}
	if len(resp.GetAtoms()) != 1 {
		t.Errorf("atoms=%d want 1 (duplicates + blanks deduped)", len(resp.GetAtoms()))
	}
	if !resp.GetAllAuthorized() {
		t.Errorf("all_authorized=false, want true (owner arms own atom)")
	}
}

func TestAuthorizeLiveQuizAtoms_ProjectionError_Unusable(t *testing.T) {
	t.Parallel()
	srv := New(Deps{
		Projections: &errProjectionRepo{fakeProjectionRepo: newFakeProjectionRepo(), err: errors.New("db down")},
		Grants:      newFakeGrantRepo(),
	})
	resp, err := srv.AuthorizeLiveQuizAtoms(context.Background(), &sharingv1.AuthorizeLiveQuizAtomsRequest{
		InstructorGcid: ws3Instructor,
		TenantId:       ws3Tenant,
		AtomIds:        []string{ws3Atom},
		IdempotencyKey: "arm-proj-err",
	})
	if err != nil {
		t.Fatalf("AuthorizeLiveQuizAtoms: %v", err)
	}
	if resp.GetAllAuthorized() {
		t.Error("all_authorized=true, want false")
	}
	a := resp.GetAtoms()[0]
	if a.GetUsable() {
		t.Error("atom usable=true for an unresolvable projection")
	}
	if a.GetReason() == "" {
		t.Error("unusable atom must name a reason (SC-009)")
	}
}

func TestAuthorizeLiveQuizAtoms_MixedAtoms_AllAuthorizedFalse(t *testing.T) {
	t.Parallel()
	projections := newFakeProjectionRepo()
	projections.seed(atom_projectionSeed())
	// Second atom has no projection row → the arm must report a mixed verdict.
	srv := New(Deps{Projections: projections, Grants: newFakeGrantRepo()})
	resp, err := srv.AuthorizeLiveQuizAtoms(context.Background(), &sharingv1.AuthorizeLiveQuizAtomsRequest{
		InstructorGcid: ws3Owner,
		TenantId:       ws3Tenant,
		AtomIds:        []string{ws3Atom, "missing-atom"},
		IdempotencyKey: "arm-mixed",
	})
	if err != nil {
		t.Fatalf("AuthorizeLiveQuizAtoms: %v", err)
	}
	if len(resp.GetAtoms()) != 2 {
		t.Fatalf("atoms=%d want 2", len(resp.GetAtoms()))
	}
	if resp.GetAllAuthorized() {
		t.Error("all_authorized=true, want false (one atom unusable)")
	}
	if !resp.GetAtoms()[0].GetUsable() || resp.GetAtoms()[1].GetUsable() {
		t.Errorf("per-atom verdicts wrong: %+v", resp.GetAtoms())
	}
}

// atom_projectionSeed reseeds the ws3 atom as the ws3 owner's own atom so the
// dedup/mixed tests exercise the own-leg without a grant.
func atom_projectionSeed() atom_projection.Projection {
	return atom_projection.Projection{
		AtomID:          ws3Atom,
		RevisionID:      ws3Rev,
		OwnerGCID:       ws3Owner,
		PublishedAt:     time.Now().UTC(),
		ReuseVisibility: "tenant",
	}
}