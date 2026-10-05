// social_handlers_test.go — the §6 social surface beyond the reaction toggle
// already pinned in share_reaction_counts_test.go:
//
//	DELETE /v1/posts/{post_id}/reactions/{reaction_id}
//	GET  /v1/posts/{post_id}/comments
//	POST /v1/posts/{post_id}/comments
//	PATCH /v1/posts/{post_id}/comments/{comment_id}
//	DELETE /v1/posts/{post_id}/comments/{comment_id}
//	GET  /v1/leaderboard
//
// Plus the reactToPost error/odd branches not reached by the toggle tests.
package httpadapter

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
	"github.com/apollo-chora/chora-sharing/internal/domain/comment"
	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
	"github.com/apollo-chora/chora-sharing/internal/domain/post"
	"github.com/apollo-chora/chora-sharing/internal/domain/reaction"
)

const (
	socTenant = "01970000-0000-7000-8000-0000000000d1"
	socGCID   = "01970000-0000-7000-9000-0000000000d1"
	socOther  = "01970000-0000-7000-9000-0000000000d2"
	socPost   = "01970000-0000-7000-a000-0000000000d1"
)

func socReq(method, path, body string) *http.Request {
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	r.Header.Set("gcid", socGCID)
	r.Header.Set("X-Tenant-Id", socTenant)
	return r
}

// faultyReactions is a ReactionRepo with error injection for the paths the
// in-memory Registry cannot reach (unreact failure, react failure).
type faultyReactions struct {
	reactErr    error
	unreactErr  error
	unreactByID func(string, string) (bool, error)
	listErr     error
	// listCalls / failListOnCall fail ListByPost only on the Nth call
	// (e.g. the recount after a successful react).
	listCalls     int
	failListOnCall int
	// existing, when set, is what ListByPost returns — used to simulate a
	// user's already-active reaction so the toggle/switch branches run.
	existing []*reaction.Reaction
}

func (f *faultyReactions) React(_ context.Context, _, _, _ string, _ reaction.Type) (*reaction.Reaction, bool, error) {
	if f.reactErr != nil {
		return nil, false, f.reactErr
	}
	return &reaction.Reaction{ID: "rx-1", Type: reaction.TypeLike}, true, nil
}
func (f *faultyReactions) Unreact(_ context.Context, _, _ string, _ reaction.Type) (bool, error) {
	return false, f.unreactErr
}
func (f *faultyReactions) UnreactByID(_ context.Context, reactionID, gcid string) (bool, error) {
	if f.unreactByID != nil {
		return f.unreactByID(reactionID, gcid)
	}
	return false, nil
}
func (f *faultyReactions) ListByPost(_ context.Context, _ string) ([]*reaction.Reaction, error) {
	f.listCalls++
	// failing after N calls: first call passes the pre-react scan, later
	// calls fail (the post-react recount).
	if f.listErr != nil && (f.failListOnCall == 0 || f.listCalls > f.failListOnCall) {
		return nil, f.listErr
	}
	if f.existing != nil && f.listCalls == 1 {
		return append([]*reaction.Reaction(nil), f.existing...), nil
	}
	return nil, nil
}

// faultyPosts is a PostRepo with error injection.
type faultyPosts struct {
	getErr   error
	found    bool
	tenantID string
}

func (f *faultyPosts) Save(_ context.Context, _ *post.Post) error { return nil }
func (f *faultyPosts) Get(_ context.Context, _ string) (*post.Post, bool, error) {
	if f.getErr != nil {
		return nil, false, f.getErr
	}
	if !f.found {
		return nil, false, nil
	}
	return &post.Post{ID: socPost, TenantID: f.tenantID, AuthorGCID: socGCID}, true, nil
}
func (f *faultyPosts) ListByTenant(_ context.Context, _ string, _, _ int) ([]*post.Post, int, error) {
	return nil, 0, nil
}

// faultyComments is a CommentStore with error injection.
type faultyComments struct {
	createErr error
	listErr   error
	updateErr error
	deleteErr error
}

func (f *faultyComments) Create(_ context.Context, _ *comment.Comment) error { return f.createErr }
func (f *faultyComments) ListByPost(_ context.Context, _ string, _ int, _ string) ([]comment.Comment, string, error) {
	if f.listErr != nil {
		return nil, "", f.listErr
	}
	return nil, "", nil
}
func (f *faultyComments) Update(_ context.Context, _, _, _ string) error { return f.updateErr }
func (f *faultyComments) Delete(_ context.Context, _, _ string) error    { return f.deleteErr }

// fakeLeaderboards is a leaderboard.LeaderboardReader recording its inputs.
type fakeLeaderboards struct {
	entries []leaderboard.Entry
	err     error
}

func (f *fakeLeaderboards) ReadTop(_ context.Context, scopeKind leaderboard.ScopeKind, scopeID, tenantID string, period leaderboard.Period, limit int) ([]leaderboard.Entry, error) {
	return f.entries, f.err
}

// ---------------------------------------------------------------------------
// DELETE /v1/posts/{post_id}/reactions/{reaction_id}
// ---------------------------------------------------------------------------

func TestRemoveReaction_Success(t *testing.T) {
	t.Parallel()
	// Seed a real reaction through the registry, then remove it by id.
	reg := reaction.NewRegistry()
	_, _, _ = reg.React(context.Background(), socTenant, socGCID, socPost, reaction.TypeLike)

	rxs, _ := reg.ListByPost(context.Background(), socPost)
	if len(rxs) == 0 {
		t.Fatal("seed reaction missing")
	}
	h := NewHandler(Deps{Reactions: reg})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, socReq(http.MethodDelete, "/v1/posts/"+socPost+"/reactions/"+rxs[0].ID, ""))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status=%d body=%s, want 204", rr.Code, rr.Body.String())
	}
	// The registry must be empty afterwards.
	left, _ := reg.ListByPost(context.Background(), socPost)
	if len(left) != 0 {
		t.Errorf("reaction not removed: %d remain", len(left))
	}
}

func TestRemoveReaction_NotOwnedOrMissing_404(t *testing.T) {
	t.Parallel()
	h := NewHandler(Deps{Reactions: &faultyReactions{
		unreactByID: func(_, _ string) (bool, error) { return false, reaction.ErrInvalidArgument },
	}})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, socReq(http.MethodDelete, "/v1/posts/"+socPost+"/reactions/whatever", ""))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status=%d, want 404", rr.Code)
	}
}

func TestRemoveReaction_Branches(t *testing.T) {
	t.Parallel()
	// 501 nil port.
	h501 := NewHandler(Deps{})
	rr := httptest.NewRecorder()
	h501.ServeHTTP(rr, socReq(http.MethodDelete, "/v1/posts/"+socPost+"/reactions/x", ""))
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("nil port: status=%d, want 501", rr.Code)
	}

	// 400 empty reaction_id (direct — the mux wildcard cannot be empty).
	h := NewHandler(Deps{Reactions: reaction.NewRegistry()})
	req := httptest.NewRequest(http.MethodDelete, "/v1/posts/x/reactions/", nil)
	req.SetPathValue("reaction_id", "  ")
	req.Header.Set("gcid", socGCID)
	req.Header.Set("X-Tenant-Id", socTenant)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	// Through the mux this route won't match; call the handler directly.
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodDelete, "/v1/posts/x/reactions/y", nil)
	req2.SetPathValue("reaction_id", "  ")
	h.removeReaction(rr2, req2)
	if rr2.Code != http.StatusBadRequest {
		t.Errorf("empty reaction_id: status=%d, want 400", rr2.Code)
	}

	// 500 generic repo fault.
	h500 := NewHandler(Deps{Reactions: &faultyReactions{
		unreactByID: func(_, _ string) (bool, error) { return false, errors.New("db down") },
	}})
	rr3 := httptest.NewRecorder()
	h500.ServeHTTP(rr3, socReq(http.MethodDelete, "/v1/posts/"+socPost+"/reactions/x", ""))
	if rr3.Code != http.StatusInternalServerError {
		t.Errorf("fault: status=%d, want 500", rr3.Code)
	}
}

// ---------------------------------------------------------------------------
// ReactToPost — branches beyond the toggle happy path
// ---------------------------------------------------------------------------

func TestReactToPost_ErrorAndGuardBranches(t *testing.T) {
	t.Parallel()

	// 501 nil Reactions.
	h501 := NewHandler(Deps{})
	rr := httptest.NewRecorder()
	h501.ServeHTTP(rr, socReq(http.MethodPost, "/v1/posts/"+socPost+"/reactions", `{"kind":"like"}`))
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("nil port: status=%d, want 501", rr.Code)
	}

	// Post lookup fault → 500 (fail loud, not a silent absence).
	h500 := NewHandler(Deps{Reactions: reaction.NewRegistry(), Posts: &faultyPosts{getErr: errors.New("db down")}})
	rr = httptest.NewRecorder()
	h500.ServeHTTP(rr, socReq(http.MethodPost, "/v1/posts/"+socPost+"/reactions", `{"kind":"like"}`))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("post lookup fault: status=%d, want 500", rr.Code)
	}

	// Post absent + Shares nil → 404.
	h404a := NewHandler(Deps{Reactions: reaction.NewRegistry(), Posts: &faultyPosts{found: false}})
	rr = httptest.NewRecorder()
	h404a.ServeHTTP(rr, socReq(http.MethodPost, "/v1/posts/"+socPost+"/reactions", `{"kind":"like"}`))
	if rr.Code != http.StatusNotFound {
		t.Errorf("absent post, nil shares: status=%d, want 404", rr.Code)
	}

	// Post absent + Shares present but no share → 404.
	h404b := NewHandler(Deps{Reactions: reaction.NewRegistry(), Posts: &faultyPosts{found: false}, Shares: inmem.NewShareRepo()})
	rr = httptest.NewRecorder()
	h404b.ServeHTTP(rr, socReq(http.MethodPost, "/v1/posts/"+socPost+"/reactions", `{"kind":"like"}`))
	if rr.Code != http.StatusNotFound {
		t.Errorf("absent post + absent share: status=%d, want 404", rr.Code)
	}

	// Posts nil + Shares present with the feed entry → the existence check
	// falls back to GetShare; a found share proceeds.
	shares := inmem.NewShareRepo()
	_ = shares.SaveShare(context.Background(), &atom_share.Share{
		FeedEntryID: socPost, TenantID: socTenant, OwnerGCID: socGCID,
		AtomID: "atom-1", RevisionID: "rev-1", License: atom_share.LicenseFree,
	})
	hFound := NewHandler(Deps{Reactions: reaction.NewRegistry(), Shares: shares})
	rr = httptest.NewRecorder()
	hFound.ServeHTTP(rr, socReq(http.MethodPost, "/v1/posts/"+socPost+"/reactions", `{"kind":"like"}`))
	if rr.Code != http.StatusCreated {
		t.Errorf("share fallback: status=%d, want 201 (body %s)", rr.Code, rr.Body.String())
	}

	// Cross-tenant post → 404.
	hXTenant := NewHandler(Deps{Reactions: reaction.NewRegistry(), Posts: &faultyPosts{found: true, tenantID: "other-tenant"}})
	rr = httptest.NewRecorder()
	hXTenant.ServeHTTP(rr, socReq(http.MethodPost, "/v1/posts/"+socPost+"/reactions", `{"kind":"like"}`))
	if rr.Code != http.StatusNotFound {
		t.Errorf("cross-tenant: status=%d, want 404", rr.Code)
	}

	// Malformed body → 400.
	hBad := NewHandler(Deps{Reactions: reaction.NewRegistry(), Posts: &faultyPosts{found: true, tenantID: socTenant}})
	rr = httptest.NewRecorder()
	hBad.ServeHTTP(rr, socReq(http.MethodPost, "/v1/posts/"+socPost+"/reactions", `{oops`))
	if rr.Code != http.StatusBadRequest {
		t.Errorf("malformed body: status=%d, want 400", rr.Code)
	}

	// Unknown kind → 400.
	hKind := NewHandler(Deps{Reactions: reaction.NewRegistry(), Posts: &faultyPosts{found: true, tenantID: socTenant}})
	rr = httptest.NewRecorder()
	hKind.ServeHTTP(rr, socReq(http.MethodPost, "/v1/posts/"+socPost+"/reactions", `{"kind":"angry"}`))
	if rr.Code != http.StatusBadRequest {
		t.Errorf("unknown kind: status=%d, want 400", rr.Code)
	}

	// ListByPost fault on the existing-reaction scan → 500.
	hListErr := NewHandler(Deps{Reactions: &faultyReactions{listErr: errors.New("db down")}, Posts: &faultyPosts{found: true, tenantID: socTenant}})
	rr = httptest.NewRecorder()
	hListErr.ServeHTTP(rr, socReq(http.MethodPost, "/v1/posts/"+socPost+"/reactions", `{"kind":"like"}`))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("list fault: status=%d, want 500", rr.Code)
	}

	// React fault (not invalid-argument) → 500; ErrInvalidArgument → 400.
	hReactErr := NewHandler(Deps{Reactions: &faultyReactions{reactErr: errors.New("boom")}, Posts: &faultyPosts{found: true, tenantID: socTenant}})
	rr = httptest.NewRecorder()
	hReactErr.ServeHTTP(rr, socReq(http.MethodPost, "/v1/posts/"+socPost+"/reactions", `{"kind":"like"}`))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("react fault: status=%d, want 500", rr.Code)
	}

	hReactArg := NewHandler(Deps{Reactions: &faultyReactions{reactErr: reaction.ErrInvalidArgument}, Posts: &faultyPosts{found: true, tenantID: socTenant}})
	rr = httptest.NewRecorder()
	hReactArg.ServeHTTP(rr, socReq(http.MethodPost, "/v1/posts/"+socPost+"/reactions", `{"kind":"like"}`))
	if rr.Code != http.StatusBadRequest {
		t.Errorf("react invalid: status=%d, want 400", rr.Code)
	}

	// Unreact fault on the same-kind toggle-off → 500.
	faulty := &faultyReactions{
		unreactErr: errors.New("boom"),
		existing: []*reaction.Reaction{{
			ID: "rx-1", GCID: socGCID, PostID: socPost, Type: reaction.TypeLike,
		}},
	}
	hToggleOff := NewHandler(Deps{Reactions: faulty, Posts: &faultyPosts{found: true, tenantID: socTenant}})
	rr = httptest.NewRecorder()
	hToggleOff.ServeHTTP(rr, socReq(http.MethodPost, "/v1/posts/"+socPost+"/reactions", `{"kind":"like"}`))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("toggle-off fault: status=%d, want 500", rr.Code)
	}
}

// The switch path (different-kind reaction active) with an unreact fault
// → 500; with the unreact succeeding → the switch completes.
func TestReactToPost_SwitchPath(t *testing.T) {
	t.Parallel()
	reg := reaction.NewRegistry()
	posts := &faultyPosts{found: true, tenantID: socTenant}
	_, _, _ = reg.React(context.Background(), socTenant, socGCID, socPost, reaction.TypeLike)
	h := NewHandler(Deps{Reactions: reg, Posts: posts})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, socReq(http.MethodPost, "/v1/posts/"+socPost+"/reactions", `{"kind":"curious"}`))
	if rr.Code != http.StatusCreated {
		t.Fatalf("switch: status=%d body=%s, want 201", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"curious":1`) {
		t.Errorf("switch must re-count curious: %s", rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), `"like":1`) {
		t.Errorf("switch must drop the old like: %s", rr.Body.String())
	}
}

// ---------------------------------------------------------------------------
// GET /v1/posts/{post_id}/comments
// ---------------------------------------------------------------------------

func TestListComments_Success(t *testing.T) {
	t.Parallel()
	repo := inmem.NewCommentRepo()
	c, err := comment.NewComment(socTenant, socPost, socGCID, "Great post", "")
	if err != nil {
		t.Fatalf("NewComment: %v", err)
	}
	if err := repo.Create(context.Background(), c); err != nil {
		t.Fatalf("seed: %v", err)
	}
	h := NewHandler(Deps{Comments: repo, Posts: inmem.NewPostRepo()})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, socReq(http.MethodGet, "/v1/posts/"+socPost+"/comments?limit=10", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "Great post") {
		t.Errorf("comment not projected: %s", rr.Body.String())
	}
}

func TestListComments_Branches(t *testing.T) {
	t.Parallel()
	// 501 nil port.
	h501 := NewHandler(Deps{})
	rr := httptest.NewRecorder()
	h501.ServeHTTP(rr, socReq(http.MethodGet, "/v1/posts/"+socPost+"/comments", ""))
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("nil port: status=%d, want 501", rr.Code)
	}

	// 400 empty post_id (direct).
	h := NewHandler(Deps{Comments: inmem.NewCommentRepo()})
	req := httptest.NewRequest(http.MethodGet, "/v1/posts//comments", nil)
	req.SetPathValue("post_id", "")
	rr = httptest.NewRecorder()
	h.listComments(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("empty post_id: status=%d, want 400", rr.Code)
	}

	// 500 store fault.
	h500 := NewHandler(Deps{Comments: &faultyComments{listErr: errors.New("pg down")}})
	rr = httptest.NewRecorder()
	h500.ServeHTTP(rr, socReq(http.MethodGet, "/v1/posts/"+socPost+"/comments", ""))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("list fault: status=%d, want 500", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// POST /v1/posts/{post_id}/comments
// ---------------------------------------------------------------------------

func TestCreateComment_Success(t *testing.T) {
	t.Parallel()
	repo := inmem.NewCommentRepo()
	posts := &faultyPosts{found: true, tenantID: socTenant}
	h := NewHandler(Deps{Comments: repo, Posts: posts})

	req := socReq(http.MethodPost, "/v1/posts/"+socPost+"/comments", `{"body":"Nice insight"}`)
	req.Header.Set("Idempotency-Key", "idem-c1")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s, want 201", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("Location") == "" {
		t.Error("201 must carry a Location header")
	}
	got, _, _ := repo.ListByPost(context.Background(), socPost, 10, "")
	if len(got) != 1 || got[0].Body != "Nice insight" {
		t.Errorf("comment not persisted: %+v", got)
	}
}

func TestCreateComment_Branches(t *testing.T) {
	t.Parallel()

	// 501 for each missing port.
	for _, deps := range []Deps{{}, {Comments: inmem.NewCommentRepo()}, {Posts: inmem.NewPostRepo()}} {
		h := NewHandler(deps)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, socReq(http.MethodPost, "/v1/posts/"+socPost+"/comments", `{"body":"x"}`))
		if rr.Code != http.StatusNotImplemented {
			t.Errorf("deps %+v: status=%d, want 501", deps, rr.Code)
		}
	}

	h := NewHandler(Deps{Comments: inmem.NewCommentRepo(), Posts: &faultyPosts{found: true, tenantID: socTenant}})

	// 400 missing Idempotency-Key.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, socReq(http.MethodPost, "/v1/posts/"+socPost+"/comments", `{"body":"x"}`))
	if rr.Code != http.StatusBadRequest {
		t.Errorf("missing idem: status=%d, want 400", rr.Code)
	}

	// 400 empty post_id (direct).
	req := httptest.NewRequest(http.MethodPost, "/v1/posts//comments", strings.NewReader(`{"body":"x"}`))
	req.SetPathValue("post_id", "")
	rr = httptest.NewRecorder()
	h.createComment(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("empty post_id: status=%d, want 400", rr.Code)
	}

	// 500 post lookup fault.
	h500 := NewHandler(Deps{Comments: inmem.NewCommentRepo(), Posts: &faultyPosts{getErr: errors.New("db down")}})
	req2 := socReq(http.MethodPost, "/v1/posts/"+socPost+"/comments", `{"body":"x"}`)
	req2.Header.Set("Idempotency-Key", "k")
	rr = httptest.NewRecorder()
	h500.ServeHTTP(rr, req2)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("lookup fault: status=%d, want 500", rr.Code)
	}

	// 404 absent post.
	h404 := NewHandler(Deps{Comments: inmem.NewCommentRepo(), Posts: &faultyPosts{found: false}})
	req3 := socReq(http.MethodPost, "/v1/posts/"+socPost+"/comments", `{"body":"x"}`)
	req3.Header.Set("Idempotency-Key", "k")
	rr = httptest.NewRecorder()
	h404.ServeHTTP(rr, req3)
	if rr.Code != http.StatusNotFound {
		t.Errorf("absent post: status=%d, want 404", rr.Code)
	}

	// 404 cross-tenant post.
	hXT := NewHandler(Deps{Comments: inmem.NewCommentRepo(), Posts: &faultyPosts{found: true, tenantID: "other"}})
	req4 := socReq(http.MethodPost, "/v1/posts/"+socPost+"/comments", `{"body":"x"}`)
	req4.Header.Set("Idempotency-Key", "k")
	rr = httptest.NewRecorder()
	hXT.ServeHTTP(rr, req4)
	if rr.Code != http.StatusNotFound {
		t.Errorf("cross-tenant: status=%d, want 404", rr.Code)
	}

	// 400 malformed body.
	req5 := socReq(http.MethodPost, "/v1/posts/"+socPost+"/comments", `{nope`)
	req5.Header.Set("Idempotency-Key", "k")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req5)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("malformed body: status=%d, want 400", rr.Code)
	}

	// 400 invalid comment (empty body).
	req6 := socReq(http.MethodPost, "/v1/posts/"+socPost+"/comments", `{"body":"   "}`)
	req6.Header.Set("Idempotency-Key", "k")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req6)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("empty body: status=%d, want 400", rr.Code)
	}

	// 400 invalid parent (non-UUID).
	req7 := socReq(http.MethodPost, "/v1/posts/"+socPost+"/comments", `{"body":"ok","parent_comment_id":"not-a-uuid"}`)
	req7.Header.Set("Idempotency-Key", "k")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req7)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("bad parent: status=%d, want 400", rr.Code)
	}

	// 500 create fault.
	h500b := NewHandler(Deps{Comments: &faultyComments{createErr: errors.New("pg down")}, Posts: &faultyPosts{found: true, tenantID: socTenant}})
	req8 := socReq(http.MethodPost, "/v1/posts/"+socPost+"/comments", `{"body":"ok"}`)
	req8.Header.Set("Idempotency-Key", "k")
	rr = httptest.NewRecorder()
	h500b.ServeHTTP(rr, req8)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("create fault: status=%d, want 500", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// PATCH /v1/posts/{post_id}/comments/{comment_id}
// ---------------------------------------------------------------------------

func TestUpdateComment_SuccessAndBranches(t *testing.T) {
	t.Parallel()
	repo := inmem.NewCommentRepo()
	c, _ := comment.NewComment(socTenant, socPost, socGCID, "original", "")
	_ = repo.Create(context.Background(), c)

	// 204 success.
	h := NewHandler(Deps{Comments: repo})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, socReq(http.MethodPatch, "/v1/posts/"+socPost+"/comments/"+c.ID, `{"body":"edited"}`))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("success: status=%d, want 204", rr.Code)
	}
	got, _, _ := repo.ListByPost(context.Background(), socPost, 10, "")
	if len(got) != 1 || got[0].Body != "edited" {
		t.Errorf("comment not updated: %+v", got)
	}

	// 501 nil port.
	h501 := NewHandler(Deps{})
	rr = httptest.NewRecorder()
	h501.ServeHTTP(rr, socReq(http.MethodPatch, "/v1/posts/x/comments/y", `{"body":"e"}`))
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("nil port: status=%d, want 501", rr.Code)
	}

	// 400 empty comment_id (direct).
	req := httptest.NewRequest(http.MethodPatch, "/v1/posts/x/comments/y", strings.NewReader(`{"body":"e"}`))
	req.SetPathValue("comment_id", " ")
	rr = httptest.NewRecorder()
	h.updateComment(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("empty comment_id: status=%d, want 400", rr.Code)
	}

	// 400 malformed body.
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, socReq(http.MethodPatch, "/v1/posts/x/comments/y", `{bad`))
	if rr.Code != http.StatusBadRequest {
		t.Errorf("malformed: status=%d, want 400", rr.Code)
	}

	// 404 not found/not owned.
	h404 := NewHandler(Deps{Comments: &faultyComments{updateErr: comment.ErrNotFound}})
	rr = httptest.NewRecorder()
	h404.ServeHTTP(rr, socReq(http.MethodPatch, "/v1/posts/x/comments/y", `{"body":"e"}`))
	if rr.Code != http.StatusNotFound {
		t.Errorf("not found: status=%d, want 404", rr.Code)
	}

	// 500 generic.
	h500 := NewHandler(Deps{Comments: &faultyComments{updateErr: errors.New("pg down")}})
	rr = httptest.NewRecorder()
	h500.ServeHTTP(rr, socReq(http.MethodPatch, "/v1/posts/x/comments/y", `{"body":"e"}`))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("fault: status=%d, want 500", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// DELETE /v1/posts/{post_id}/comments/{comment_id}
// ---------------------------------------------------------------------------

func TestDeleteComment_SuccessAndBranches(t *testing.T) {
	t.Parallel()
	repo := inmem.NewCommentRepo()
	c, _ := comment.NewComment(socTenant, socPost, socGCID, "doomed", "")
	_ = repo.Create(context.Background(), c)

	h := NewHandler(Deps{Comments: repo})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, socReq(http.MethodDelete, "/v1/posts/"+socPost+"/comments/"+c.ID, ""))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("success: status=%d, want 204", rr.Code)
	}
	// Repeat delete on an absent comment is also 204 (idempotent via ErrNotFound).
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, socReq(http.MethodDelete, "/v1/posts/"+socPost+"/comments/"+c.ID, ""))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("idempotent repeat: status=%d, want 204", rr.Code)
	}

	// 501 nil port.
	h501 := NewHandler(Deps{})
	rr = httptest.NewRecorder()
	h501.ServeHTTP(rr, socReq(http.MethodDelete, "/v1/posts/x/comments/y", ""))
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("nil port: status=%d, want 501", rr.Code)
	}

	// 400 empty comment_id (direct).
	req := httptest.NewRequest(http.MethodDelete, "/v1/posts/x/comments/y", nil)
	req.SetPathValue("comment_id", " ")
	rr = httptest.NewRecorder()
	h.deleteComment(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("empty comment_id: status=%d, want 400", rr.Code)
	}

	// 500 generic fault.
	h500 := NewHandler(Deps{Comments: &faultyComments{deleteErr: errors.New("pg down")}})
	rr = httptest.NewRecorder()
	h500.ServeHTTP(rr, socReq(http.MethodDelete, "/v1/posts/x/comments/y", ""))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("fault: status=%d, want 500", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// GET /v1/leaderboard
// ---------------------------------------------------------------------------

func TestGetLeaderboard_Success(t *testing.T) {
	t.Parallel()
	lb := &fakeLeaderboards{entries: []leaderboard.Entry{
		{GCID: socGCID, Score: 100, Rank: 1},
		{GCID: socOther, Score: 50, Rank: 2},
	}}
	h := NewHandler(Deps{Leaderboards: lb})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, socReq(http.MethodGet, "/v1/leaderboard?scope=tenant&period=weekly&metric=xp&limit=5", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), socGCID) || !strings.Contains(rr.Body.String(), `"rank":1`) {
		t.Errorf("leaderboard rows missing: %s", rr.Body.String())
	}
}

func TestGetLeaderboard_Branches(t *testing.T) {
	t.Parallel()

	// 501 nil port.
	h501 := NewHandler(Deps{})
	rr := httptest.NewRecorder()
	h501.ServeHTTP(rr, socReq(http.MethodGet, "/v1/leaderboard", ""))
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("nil port: status=%d, want 501", rr.Code)
	}

	// 400 cohort scope without scope_target_id.
	h := NewHandler(Deps{Leaderboards: &fakeLeaderboards{}})
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, socReq(http.MethodGet, "/v1/leaderboard?scope=class", ""))
	if rr.Code != http.StatusBadRequest {
		t.Errorf("cohort missing id: status=%d, want 400", rr.Code)
	}

	// 500 read fault.
	h500 := NewHandler(Deps{Leaderboards: &fakeLeaderboards{err: errors.New("pg down")}})
	rr = httptest.NewRecorder()
	h500.ServeHTTP(rr, socReq(http.MethodGet, "/v1/leaderboard", ""))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("read fault: status=%d, want 500", rr.Code)
	}

	// Success with cohort + bogus scope/metric/period values (drives the
	// leaderboardScopeFromQuery / leaderboardMetricFromQuery /
	// leaderboardPeriodFromQuery default branches).
	lb := &fakeLeaderboards{}
	hOK := NewHandler(Deps{Leaderboards: lb})
	rr = httptest.NewRecorder()
	hOK.ServeHTTP(rr, socReq(http.MethodGet, "/v1/leaderboard?scope=bogus&metric=magic&period=nonsense", ""))
	if rr.Code != http.StatusOK {
		t.Errorf("bogus query values: status=%d, want 200", rr.Code)
	}
}