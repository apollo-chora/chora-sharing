// listing_rpc_test.go — statement coverage for the listing RPCs:
// ListEntitledAtoms (§7.4), ListSavedAtomIDs (C+ picker), GenerateQuizFromTopic
// (§7.9), ListSharedAtoms (feed read), plus the RevokeAtomUse error branches.
// Hand-written fakes per the package convention.
package grpcadapter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sharingv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/sharing/v1"

	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
	"github.com/apollo-chora/chora-sharing/internal/domain/bookmark"
	"github.com/apollo-chora/chora-sharing/internal/domain/grant"
	"github.com/apollo-chora/chora-sharing/internal/domain/livequiz"
)

// -----------------------------------------------------------------------------
// Fakes
// -----------------------------------------------------------------------------

// listingGrantRepo wraps fakeGrantRepo and makes ListEntitled observable +
// failure-injectable (the shared fake returns nil,nil unconditionally).
type listingGrantRepo struct {
	*fakeGrantRepo
	entitled  []grant.EntitledAtom
	err       error
	gotLimits []int
	gotScopes []grant.Scope
	gotTags   [][]string
}

func (f *listingGrantRepo) ListEntitled(ctx context.Context, gcid string, scope grant.Scope, topicTags []string, limit int) ([]grant.EntitledAtom, error) {
	f.gotLimits = append(f.gotLimits, limit)
	f.gotScopes = append(f.gotScopes, scope)
	f.gotTags = append(f.gotTags, topicTags)
	if f.err != nil {
		return nil, f.err
	}
	return f.entitled, nil
}

// listingShareRepo wraps fakeShareRepo so ListSharedAtoms records the limit
// (the shared fake ignores pagination) and can inject a failure.
type listingShareRepo struct {
	*fakeShareRepo
	gotLimits []int
	err       error
	next      string
}

func (f *listingShareRepo) ListSharedAtoms(ctx context.Context, tenantID string, cursor string, limit int, topicFilter, questionTypeFilter string, scope string, followingGCIDs []string, blockedGCIDs []string) ([]atom_share.Share, string, error) {
	f.gotLimits = append(f.gotLimits, limit)
	if f.err != nil {
		return nil, "", f.err
	}
	shares, _, err := f.fakeShareRepo.ListSharedAtoms(ctx, tenantID, cursor, limit, topicFilter, questionTypeFilter, scope, followingGCIDs, blockedGCIDs)
	return shares, f.next, err
}

// fakeBookmarkRepo simulates keyset pagination: pages of itemsPerPage, a
// non-empty next cursor whenever a page comes back full and hasMore is set,
// and a dedicated knob for the final cap probe (limit == 1).
type fakeBookmarkRepo struct {
	mu           sync.Mutex
	itemsPerPage int
	hasMore      bool
	probeNext    bool
	probeErr     error
	err          error
	callLimits   []int
}

func (f *fakeBookmarkRepo) Save(ctx context.Context, b *bookmark.Bookmark) error { return nil }

func (f *fakeBookmarkRepo) Delete(ctx context.Context, tenantID, gcid, atomID string) error {
	return nil
}

func (f *fakeBookmarkRepo) ListByOwner(ctx context.Context, tenantID, gcid, cursor string, limit int) ([]bookmark.Bookmark, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, "", f.err
	}
	f.callLimits = append(f.callLimits, limit)
	if limit == 1 && f.probeErr != nil {
		return nil, "", f.probeErr
	}
	n := limit
	if f.itemsPerPage > 0 && n > f.itemsPerPage {
		n = f.itemsPerPage
	}
	page := make([]bookmark.Bookmark, 0, n)
	for i := 0; i < n; i++ {
		page = append(page, bookmark.Bookmark{
			ID:       fmt.Sprintf("bm-%s-%d", cursor, i),
			TenantID: tenantID,
			GCID:     gcid,
			AtomID:   fmt.Sprintf("atom-%s-%d", cursor, i),
		})
	}
	next := ""
	full := f.itemsPerPage > 0 && n == f.itemsPerPage
	if full && f.hasMore {
		next = fmt.Sprintf("c-%d", len(f.callLimits))
	}
	if limit == 1 && f.probeNext {
		next = "probe-next"
	}
	return page, next, nil
}

// failingRevokeGrantRepo wraps fakeGrantRepo so Revoke can return the
// permission-denied + internal error classes the shared fake cannot produce
// (its real Revoke only yields ErrGrantNotFound / ErrInvalidArgument).
type failingRevokeGrantRepo struct {
	*fakeGrantRepo
	revokeErr error
}

func (f *failingRevokeGrantRepo) Revoke(ctx context.Context, grantID, revokerGCID, reason string) error {
	if f.revokeErr != nil {
		return f.revokeErr
	}
	return f.fakeGrantRepo.Revoke(ctx, grantID, revokerGCID, reason)
}

// fakeQuizGen is a configurable livequiz.QuizGenerator.
type fakeQuizGen struct {
	questions    []livequiz.GeneratedQuestion
	err          error
	gotInstructor string
	gotTenant     string
	gotTopic      string
	gotTags       []string
	gotCount      int
}

func (f *fakeQuizGen) GenerateQuizQuestions(ctx context.Context, instructorGCID, tenantID, topic string, topicTags []string, questionCount int) ([]livequiz.GeneratedQuestion, error) {
	f.gotInstructor, f.gotTenant, f.gotTopic, f.gotTags, f.gotCount = instructorGCID, tenantID, topic, topicTags, questionCount
	if f.err != nil {
		return nil, f.err
	}
	return f.questions, nil
}

// -----------------------------------------------------------------------------
// ListEntitledAtoms
// -----------------------------------------------------------------------------

func TestListEntitledAtoms_NotWired_Unimplemented(t *testing.T) {
	t.Parallel()
	_, err := New(Deps{}).ListEntitledAtoms(context.Background(), &sharingv1.ListEntitledAtomsRequest{
		Gcid: "g", TenantId: "t",
		Scope: sharingv1.GrantScope_GRANT_SCOPE_DUEL,
	})
	statusIs(t, err, codes.Unimplemented)
}

func TestListEntitledAtoms_InvalidInput(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Grants: newFakeGrantRepo()})
	for name, req := range map[string]*sharingv1.ListEntitledAtomsRequest{
		"nil request":      nil,
		"all missing":      {},
		"no gcid":          {TenantId: "t", Scope: sharingv1.GrantScope_GRANT_SCOPE_DUEL},
		"no tenant":        {Gcid: "g", Scope: sharingv1.GrantScope_GRANT_SCOPE_DUEL},
		"scope unspecified": {Gcid: "g", TenantId: "t"},
	} {
		if _, err := srv.ListEntitledAtoms(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: expected InvalidArgument, got %v", name, err)
		}
	}
}

func TestListEntitledAtoms_HappyPath_LimitDefault(t *testing.T) {
	t.Parallel()
	grants := &listingGrantRepo{fakeGrantRepo: newFakeGrantRepo()}
	grants.entitled = []grant.EntitledAtom{
		{
			AtomID: "atom-1", RevisionID: "rev-1", AuthorGCID: "g",
			AuthorDisplayName: "Ada", StemPreview: "What?",
			License: atom_share.LicenseCCBySA, IsOwn: true, HasGrant: false,
		},
		{
			AtomID: "atom-2", RevisionID: "rev-2", AuthorGCID: "o",
			AuthorDisplayName: "Bob", StemPreview: "Who?",
			License: atom_share.LicenseFree, IsOwn: false, HasGrant: true,
		},
	}
	srv := New(Deps{Grants: grants})
	resp, err := srv.ListEntitledAtoms(context.Background(), &sharingv1.ListEntitledAtomsRequest{
		Gcid:     "g",
		TenantId: "t",
		Scope:    sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		// limit 0 → default 50.
	})
	if err != nil {
		t.Fatalf("ListEntitledAtoms: %v", err)
	}
	if len(resp.GetAtoms()) != 2 {
		t.Fatalf("atoms=%d want 2", len(resp.GetAtoms()))
	}
	a := resp.GetAtoms()[0]
	if a.GetAtomId() != "atom-1" || a.GetAuthorGcid() != "g" || a.GetLicenseTerms() != sharingv1.LicenseTerms_LICENSE_TERMS_CC_BY_SA {
		t.Errorf("atom[0] mismatch: %+v", a)
	}
	if !a.GetIsOwn() || a.GetHasGrant() {
		t.Errorf("atom[0] flags wrong: is_own=%v has_grant=%v", a.GetIsOwn(), a.GetHasGrant())
	}
	if len(grants.gotLimits) != 1 || grants.gotLimits[0] != 50 {
		t.Errorf("limit=%v want [50]", grants.gotLimits)
	}
	if len(grants.gotScopes) != 1 || grants.gotScopes[0] != grant.ScopeDuel {
		t.Errorf("scope=%v want [duel]", grants.gotScopes)
	}
}

func TestListEntitledAtoms_LimitClamp(t *testing.T) {
	t.Parallel()
	grants := &listingGrantRepo{fakeGrantRepo: newFakeGrantRepo()}
	srv := New(Deps{Grants: grants})
	_, err := srv.ListEntitledAtoms(context.Background(), &sharingv1.ListEntitledAtomsRequest{
		Gcid: "g", TenantId: "t", Scope: sharingv1.GrantScope_GRANT_SCOPE_UNLIMITED,
		Limit: 100000, // clamped to 200
	})
	if err != nil {
		t.Fatalf("ListEntitledAtoms: %v", err)
	}
	if len(grants.gotLimits) != 1 || grants.gotLimits[0] != 200 {
		t.Errorf("limit=%v want [200]", grants.gotLimits)
	}
	if len(grants.gotScopes) != 1 || grants.gotScopes[0] != grant.ScopeUnlimited {
		t.Errorf("scope=%v want [unlimited]", grants.gotScopes)
	}
}

func TestListEntitledAtoms_ReadError_Internal(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Grants: &listingGrantRepo{
		fakeGrantRepo: newFakeGrantRepo(),
		err:           errors.New("db down"),
	}})
	_, err := srv.ListEntitledAtoms(context.Background(), &sharingv1.ListEntitledAtomsRequest{
		Gcid: "g", TenantId: "t", Scope: sharingv1.GrantScope_GRANT_SCOPE_DUEL,
	})
	statusIs(t, err, codes.Internal)
}

// -----------------------------------------------------------------------------
// ListSavedAtomIDs
// -----------------------------------------------------------------------------

func TestListSavedAtomIDs_NotWired_Unimplemented(t *testing.T) {
	t.Parallel()
	_, err := New(Deps{}).ListSavedAtomIDs(context.Background(), &sharingv1.ListSavedAtomIDsRequest{
		Gcid: "g", TenantId: "t",
	})
	statusIs(t, err, codes.Unimplemented)
}

func TestListSavedAtomIDs_InvalidInput(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Bookmarks: &fakeBookmarkRepo{}})
	for name, req := range map[string]*sharingv1.ListSavedAtomIDsRequest{
		"nil request": nil,
		"all missing": {},
		"no gcid":     {TenantId: "t"},
		"no tenant":   {Gcid: "g"},
	} {
		if _, err := srv.ListSavedAtomIDs(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: expected InvalidArgument, got %v", name, err)
		}
	}
}

func TestListSavedAtomIDs_HappyPath_Truncated(t *testing.T) {
	t.Parallel()
	// Default limit 500 = 5 full pages of 100, then the cap probe reports
	// more → truncated=true.
	bm := &fakeBookmarkRepo{itemsPerPage: 100, hasMore: true, probeNext: true}
	srv := New(Deps{Bookmarks: bm})
	resp, err := srv.ListSavedAtomIDs(context.Background(), &sharingv1.ListSavedAtomIDsRequest{
		Gcid: "g", TenantId: "t",
	})
	if err != nil {
		t.Fatalf("ListSavedAtomIDs: %v", err)
	}
	if len(resp.GetAtomIds()) != 500 {
		t.Errorf("atom_ids=%d want 500", len(resp.GetAtomIds()))
	}
	if !resp.GetTruncated() {
		t.Error("truncated=false, want true (cap probe found more)")
	}
	if len(bm.callLimits) != 6 || bm.callLimits[5] != 1 {
		t.Errorf("call limits=%v; want 5 page calls + 1 probe", bm.callLimits)
	}
}

func TestListSavedAtomIDs_PartialLastPage_NotTruncated(t *testing.T) {
	t.Parallel()
	// limit 250 → 100 + 100 + 50(partial, next="") → break; probe finds
	// nothing → truncated=false.
	bm := &fakeBookmarkRepo{itemsPerPage: 100, hasMore: true}
	srv := New(Deps{Bookmarks: bm})
	resp, err := srv.ListSavedAtomIDs(context.Background(), &sharingv1.ListSavedAtomIDsRequest{
		Gcid: "g", TenantId: "t", Limit: 250,
	})
	if err != nil {
		t.Fatalf("ListSavedAtomIDs: %v", err)
	}
	if len(resp.GetAtomIds()) != 250 {
		t.Errorf("atom_ids=%d want 250", len(resp.GetAtomIds()))
	}
	if resp.GetTruncated() {
		t.Error("truncated=true, want false (last page was partial)")
	}
}

func TestListSavedAtomIDs_ProbeError_NotTruncated(t *testing.T) {
	t.Parallel()
	// The cap probe failing is tolerated — the RPC still succeeds with
	// truncated=false (never fails the picker on a probing hiccup).
	bm := &fakeBookmarkRepo{itemsPerPage: 100, hasMore: true, probeErr: errors.New("probe down")}
	srv := New(Deps{Bookmarks: bm})
	resp, err := srv.ListSavedAtomIDs(context.Background(), &sharingv1.ListSavedAtomIDsRequest{
		Gcid: "g", TenantId: "t", Limit: 250,
	})
	if err != nil {
		t.Fatalf("ListSavedAtomIDs: %v", err)
	}
	if len(resp.GetAtomIds()) != 250 {
		t.Errorf("atom_ids=%d want 250", len(resp.GetAtomIds()))
	}
	if resp.GetTruncated() {
		t.Error("truncated=true, want false on probe error")
	}
}

func TestListSavedAtomIDs_ReadError_Internal(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Bookmarks: &fakeBookmarkRepo{err: errors.New("db down")}})
	_, err := srv.ListSavedAtomIDs(context.Background(), &sharingv1.ListSavedAtomIDsRequest{
		Gcid: "g", TenantId: "t",
	})
	statusIs(t, err, codes.Internal)
}

// -----------------------------------------------------------------------------
// ListSharedAtoms
// -----------------------------------------------------------------------------

func TestListSharedAtoms_HappyPath(t *testing.T) {
	t.Parallel()
	shares := &listingShareRepo{fakeShareRepo: newFakeShareRepo(), next: "cur-nex"}
	ctx := context.Background()
	_ = shares.SaveShare(ctx, &atom_share.Share{
		FeedEntryID: "fe-roy", TenantID: "t", AtomID: "atom-1", RevisionID: "rev-1",
		OwnerGCID: "o", AuthorDisplayName: "Ada", StemPreview: "What is 2+2?",
		QuestionType: "mcq", Options: []string{"a", "b"}, Caption: "hi",
		License: atom_share.LicenseRoyaltyPct,
		Rate:    atom_share.RoyaltyRate{Kind: "pct", Value: 0.1},
		CreatedAt: time.Now().UTC(),
	})
	_ = shares.SaveShare(ctx, &atom_share.Share{
		FeedEntryID: "fe-free", TenantID: "t", AtomID: "atom-2", RevisionID: "rev-2",
		OwnerGCID: "o", AuthorDisplayName: "Bob", StemPreview: "Who?",
		License: atom_share.LicenseFree,
		CreatedAt: time.Now().UTC(),
	})
	srv := New(Deps{Shares: shares})
	resp, err := srv.ListSharedAtoms(context.Background(), &sharingv1.ListSharedAtomsRequest{
		TenantId: "t", // limit 0 → default 20
	})
	if err != nil {
		t.Fatalf("ListSharedAtoms: %v", err)
	}
	if len(resp.GetCards()) != 2 {
		t.Fatalf("cards=%d want 2", len(resp.GetCards()))
	}
	if resp.GetNextCursor() != "cur-nex" {
		t.Errorf("next_cursor=%q want cur-nex", resp.GetNextCursor())
	}
	if len(shares.gotLimits) != 1 || shares.gotLimits[0] != 20 {
		t.Errorf("limit=%v want [20]", shares.gotLimits)
	}
	// Royalty share → card carries the frozen rate; free share → no rate.
	var royalCard, freeCard *sharingv1.SharedAtomCard
	for _, c := range resp.GetCards() {
		if c.GetShareEntryId() == "fe-roy" {
			royalCard = c
		} else {
			freeCard = c
		}
	}
	if royalCard == nil || freeCard == nil {
		t.Fatal("expected both cards")
	}
	if royalCard.GetRoyaltyRate() == nil {
		t.Error("royalty card missing rate snapshot")
	}
	if freeCard.GetRoyaltyRate() != nil {
		t.Error("free card must not carry a royalty rate")
	}
}

func TestListSharedAtoms_LimitClamp(t *testing.T) {
	t.Parallel()
	shares := &listingShareRepo{fakeShareRepo: newFakeShareRepo()}
	srv := New(Deps{Shares: shares})
	_, err := srv.ListSharedAtoms(context.Background(), &sharingv1.ListSharedAtomsRequest{
		TenantId: "t", Limit: 100000, // clamped to 100
	})
	if err != nil {
		t.Fatalf("ListSharedAtoms: %v", err)
	}
	if len(shares.gotLimits) != 1 || shares.gotLimits[0] != 100 {
		t.Errorf("limit=%v want [100]", shares.gotLimits)
	}
}

func TestListSharedAtoms_InvalidInput(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Shares: &listingShareRepo{fakeShareRepo: newFakeShareRepo()}})
	for name, req := range map[string]*sharingv1.ListSharedAtomsRequest{
		"nil request": nil,
		"no tenant":   {},
	} {
		if _, err := srv.ListSharedAtoms(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: expected InvalidArgument, got %v", name, err)
		}
	}
}

func TestListSharedAtoms_ReadError_Internal(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Shares: &listingShareRepo{
		fakeShareRepo: newFakeShareRepo(),
		err:           errors.New("db down"),
	}})
	_, err := srv.ListSharedAtoms(context.Background(), &sharingv1.ListSharedAtomsRequest{
		TenantId: "t",
	})
	statusIs(t, err, codes.Internal)
}

// -----------------------------------------------------------------------------
// GenerateQuizFromTopic
// -----------------------------------------------------------------------------

func TestGenerateQuizFromTopic_InvalidInput(t *testing.T) {
	t.Parallel()
	srv := New(Deps{QuizGen: &fakeQuizGen{}})
	for name, req := range map[string]*sharingv1.GenerateQuizFromTopicRequest{
		"nil request":  nil,
		"all missing":  {},
		"no instructor": {TenantId: "t", Topic: "math"},
		"no tenant":     {InstructorGcid: "i", Topic: "math"},
		"no topic":      {InstructorGcid: "i", TenantId: "t"},
	} {
		if _, err := srv.GenerateQuizFromTopic(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: expected InvalidArgument, got %v", name, err)
		}
	}
}

func TestGenerateQuizFromTopic_HappyPath_DefaultCount(t *testing.T) {
	t.Parallel()
	qg := &fakeQuizGen{questions: []livequiz.GeneratedQuestion{
		{AtomID: "a1", RevisionID: "r1", Stem: "Q1", Options: []string{"x", "y"},
			CorrectOptionID: "x", TimerSeconds: 30, Points: 10},
		{AtomID: "a2", RevisionID: "r2", Stem: "Q2", Options: []string{"p", "q"},
			CorrectOptionID: "q", TimerSeconds: 20, Points: 5},
	}}
	srv := New(Deps{QuizGen: qg})
	resp, err := srv.GenerateQuizFromTopic(context.Background(), &sharingv1.GenerateQuizFromTopicRequest{
		InstructorGcid: "i",
		TenantId:       "t",
		Topic:          "math",
		TopicTags:      []string{"algebra"},
		// question_count 0 → server default 5.
	})
	if err != nil {
		t.Fatalf("GenerateQuizFromTopic: %v", err)
	}
	if resp.GetDraftTemplateId() == "" || resp.GetQgenRunId() == "" {
		t.Error("draft_template_id / qgen_run_id empty")
	}
	if resp.GetReviewRequired() != "true" {
		t.Errorf("review_required=%q want \"true\" (HITL before ARM)", resp.GetReviewRequired())
	}
	if len(resp.GetQuestions()) != 2 {
		t.Fatalf("questions=%d want 2", len(resp.GetQuestions()))
	}
	q := resp.GetQuestions()[0]
	if q.GetAtomId() != "a1" || q.GetStem() != "Q1" || q.GetCorrectOptionId() != "x" ||
		q.GetTimerSeconds() != 30 || q.GetPoints() != 10 {
		t.Errorf("question[0] mismatch: %+v", q)
	}
	if qg.gotCount != 5 {
		t.Errorf("question_count=%d want 5 (default)", qg.gotCount)
	}
	if qg.gotInstructor != "i" || qg.gotTenant != "t" || qg.gotTopic != "math" {
		t.Errorf("quiz gen args mismatch: %+v", qg)
	}
}

func TestGenerateQuizFromTopic_GenError_Internal(t *testing.T) {
	t.Parallel()
	srv := New(Deps{QuizGen: &fakeQuizGen{err: errors.New("gateway down")}})
	_, err := srv.GenerateQuizFromTopic(context.Background(), &sharingv1.GenerateQuizFromTopicRequest{
		InstructorGcid: "i", TenantId: "t", Topic: "math",
	})
	statusIs(t, err, codes.Internal)
}

// -----------------------------------------------------------------------------
// RevokeAtomUse error branches
// -----------------------------------------------------------------------------

func TestRevokeAtomUse_InvalidInput(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Grants: newFakeGrantRepo()})
	for name, req := range map[string]*sharingv1.RevokeAtomUseRequest{
		"nil request":   nil,
		"all missing":   {},
		"no grant_id":   {RevokerGcid: "o"},
		"no revoker":    {GrantId: "g"},
	} {
		if _, err := srv.RevokeAtomUse(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: expected InvalidArgument, got %v", name, err)
		}
	}
}

func TestRevokeAtomUse_Forbidden_PermissionDenied(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Grants: &failingRevokeGrantRepo{
		fakeGrantRepo: newFakeGrantRepo(),
		revokeErr:     grant.ErrForbidden,
	}})
	_, err := srv.RevokeAtomUse(context.Background(), &sharingv1.RevokeAtomUseRequest{
		GrantId: "g", RevokerGcid: "non-owner",
	})
	statusIs(t, err, codes.PermissionDenied)
}

func TestRevokeAtomUse_ExpiredGrant_InvalidArgument(t *testing.T) {
	t.Parallel()
	grants := newFakeGrantRepo()
	g, err := grant.NewGrant("owner", "grantee", "atom", "rev",
		grant.ScopeDuel, atom_share.LicenseFree, atom_share.RoyaltyRate{}, "")
	if err != nil {
		t.Fatalf("NewGrant: %v", err)
	}
	g.Status = grant.StatusExpired
	grants.byID[g.ID] = g
	srv := New(Deps{Grants: grants})
	// g.Revoke refuses an expired grant with ErrInvalidArgument → InvalidArgument.
	_, err = srv.RevokeAtomUse(context.Background(), &sharingv1.RevokeAtomUseRequest{
		GrantId: g.ID, RevokerGcid: "owner",
	})
	statusIs(t, err, codes.InvalidArgument)
}

func TestRevokeAtomUse_RepoError_Internal(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Grants: &failingRevokeGrantRepo{
		fakeGrantRepo: newFakeGrantRepo(),
		revokeErr:     errors.New("db down"),
	}})
	_, err := srv.RevokeAtomUse(context.Background(), &sharingv1.RevokeAtomUseRequest{
		GrantId: "g", RevokerGcid: "owner",
	})
	statusIs(t, err, codes.Internal)
}