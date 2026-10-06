// connection_handlers_test.go — the full §6 connections surface:
//
//	GET    /v1/connections?type=following|followers|blocked
//	POST   /v1/connections/follows            {gcid}
//	DELETE /v1/connections/follows/{gcid}
//	POST   /v1/connections/blocks             {gcid}
//	DELETE /v1/connections/blocks/{gcid}
//	GET    /v1/connections/suggestions
//
// plus the shared helpers relationshipErrToStatus / targetFromBody /
// targetFromPath.
package httpadapter

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

const (
	connTenant  = "01970000-0000-7000-8000-0000000000c1"
	connGCID    = "01970000-0000-7000-9000-0000000000c1"
	connOther   = "01970000-0000-7000-9000-0000000000c2"
	connThird   = "01970000-0000-7000-9000-0000000000c3"
)

func connDeps(graph SocialGraph) Deps {
	return Deps{Graph: graph}
}

func connReq(method, path, body string) *http.Request {
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	r.Header.Set("gcid", connGCID)
	r.Header.Set("X-Tenant-Id", connTenant)
	return r
}

// errGraph is a SocialGraph that fails every callable with the injected
// error (or nil for success) — drives the fail-loud 500 paths.
type errGraph struct {
	err error
}

func (g *errGraph) Follow(_ context.Context, _, _, _ string) (*social.Edge, bool, error) {
	if g.err != nil {
		return nil, false, g.err
	}
	return &social.Edge{FolloweeGCID: "x"}, true, nil
}
func (g *errGraph) Unfollow(_ context.Context, _, _, _ string) (bool, error) {
	return false, g.err
}
func (g *errGraph) FollowingGCIDs(_ context.Context, _, _ string) ([]string, error) {
	if g.err != nil {
		return nil, g.err
	}
	return []string{connOther}, nil
}
func (g *errGraph) FollowersGCIDs(_ context.Context, _, _ string) ([]string, error) {
	if g.err != nil {
		return nil, g.err
	}
	return []string{connOther}, nil
}
func (g *errGraph) Block(_ context.Context, _, _, _ string) error { return g.err }
func (g *errGraph) Unblock(_ context.Context, _, _, _ string) (bool, error) {
	return true, g.err
}
func (g *errGraph) BlockedBy(_ context.Context, _, _ string) ([]string, error) {
	if g.err != nil {
		return nil, g.err
	}
	return []string{connOther}, nil
}

// errSuggestions is a SuggestionQueries that fails or returns fixed rows.
type errSuggestions struct {
	err   error
	items []social.FollowSuggestion
}

func (s *errSuggestions) FollowSuggestions(_ context.Context, _, _ string, _ int) ([]social.FollowSuggestion, error) {
	return s.items, s.err
}

// ---------------------------------------------------------------------------
// relationshipErrToStatus — the whole status map
// ---------------------------------------------------------------------------

func TestRelationshipErrToStatus(t *testing.T) {
	t.Parallel()

	code, msg := relationshipErrToStatus(social.ErrConnectionNotPermitted)
	if code != http.StatusConflict {
		t.Errorf("ErrConnectionNotPermitted -> %d, want 409", code)
	}
	if msg != social.ErrConnectionNotPermitted.Error() {
		t.Errorf("blocked-pair message must pass through verbatim, got %q", msg)
	}

	if code, _ := relationshipErrToStatus(social.ErrSelfFollow); code != http.StatusBadRequest {
		t.Errorf("ErrSelfFollow -> %d, want 400", code)
	}
	if code, _ := relationshipErrToStatus(social.ErrInvalidArgument); code != http.StatusBadRequest {
		t.Errorf("ErrInvalidArgument -> %d, want 400", code)
	}

	code, msg = relationshipErrToStatus(errors.New("db down"))
	if code != http.StatusInternalServerError {
		t.Errorf("generic -> %d, want 500", code)
	}
	if !strings.Contains(msg, "relationship:") {
		t.Errorf("generic message = %q, want 'relationship:' prefix", msg)
	}
}

// ---------------------------------------------------------------------------
// GET /v1/connections
// ---------------------------------------------------------------------------

func TestListConnections_Following(t *testing.T) {
	t.Parallel()
	graph := inmem.NewSocialGraph()
	if _, _, err := graph.Follow(context.Background(), connTenant, connGCID, connOther); err != nil {
		t.Fatalf("seed follow: %v", err)
	}
	h := NewHandler(connDeps(graph))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, connReq(http.MethodGet, "/v1/connections", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), connOther) {
		t.Errorf("following list missing %s: %s", connOther, rr.Body.String())
	}
}

func TestListConnections_TypeDefaultAndVariants(t *testing.T) {
	t.Parallel()
	graph := inmem.NewSocialGraph()
	_, _, _ = graph.Follow(context.Background(), connTenant, connGCID, connOther) // following
	_, _, _ = graph.Follow(context.Background(), connTenant, connThird, connGCID) // follower
	_ = graph.Block(context.Background(), connTenant, connGCID, connThird)        // blocked
	h := NewHandler(connDeps(graph))

	for _, tc := range []struct{ query, want string }{
		{"", connOther},        // default type=following
		{"?type=following", connOther},
		{"?type=followers", connThird},
		{"?type=blocked", connThird},
	} {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, connReq(http.MethodGet, "/v1/connections"+tc.query, ""))
		if rr.Code != http.StatusOK {
			t.Fatalf("type %q: status=%d body=%s", tc.query, rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), tc.want) {
			t.Errorf("type %q: missing %s in %s", tc.query, tc.want, rr.Body.String())
		}
	}
}

func TestListConnections_InvalidType_400(t *testing.T) {
	t.Parallel()
	h := NewHandler(connDeps(inmem.NewSocialGraph()))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, connReq(http.MethodGet, "/v1/connections?type=mutual", ""))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400", rr.Code)
	}
}

func TestListConnections_NilGraph_501(t *testing.T) {
	t.Parallel()
	h := NewHandler(Deps{})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, connReq(http.MethodGet, "/v1/connections", ""))
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("status=%d, want 501", rr.Code)
	}
}

func TestListConnections_GraphFault_500(t *testing.T) {
	t.Parallel()
	h := NewHandler(connDeps(&errGraph{err: errors.New("pg down")}))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, connReq(http.MethodGet, "/v1/connections?type=followers", ""))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d, want 500", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// POST /v1/connections/follows
// ---------------------------------------------------------------------------

func TestFollowMember_Created(t *testing.T) {
	t.Parallel()
	graph := inmem.NewSocialGraph()
	h := NewHandler(connDeps(graph))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, connReq(http.MethodPost, "/v1/connections/follows", `{"gcid":"`+connOther+`"}`))
	if rr.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"created":true`) {
		t.Errorf("expected created=true: %s", rr.Body.String())
	}
}

func TestFollowMember_Duplicate_200CreatedFalse(t *testing.T) {
	t.Parallel()
	graph := inmem.NewSocialGraph()
	_, _, _ = graph.Follow(context.Background(), connTenant, connGCID, connOther)
	h := NewHandler(connDeps(graph))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, connReq(http.MethodPost, "/v1/connections/follows", `{"gcid":"`+connOther+`"}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200 (duplicate)", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), `"created":false`) {
		t.Errorf("expected created=false: %s", rr.Body.String())
	}
}

func TestFollowMember_EmptyGCID_400(t *testing.T) {
	t.Parallel()
	h := NewHandler(connDeps(inmem.NewSocialGraph()))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, connReq(http.MethodPost, "/v1/connections/follows", `{"gcid":"  "}`))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400 (empty body gcid)", rr.Code)
	}
}

func TestFollowMember_MalformedJSON_400(t *testing.T) {
	t.Parallel()
	h := NewHandler(connDeps(inmem.NewSocialGraph()))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, connReq(http.MethodPost, "/v1/connections/follows", `{not json`))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400", rr.Code)
	}
}

func TestFollowMember_NilGraph_501(t *testing.T) {
	t.Parallel()
	h := NewHandler(Deps{})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, connReq(http.MethodPost, "/v1/connections/follows", `{"gcid":"`+connOther+`"}`))
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("status=%d, want 501", rr.Code)
	}
}

func TestFollowMember_SelfFollow_400(t *testing.T) {
	t.Parallel()
	h := NewHandler(connDeps(inmem.NewSocialGraph()))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, connReq(http.MethodPost, "/v1/connections/follows", `{"gcid":"`+connGCID+`"}`))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400 (self-follow)", rr.Code)
	}
}

func TestFollowMember_GraphFault_500(t *testing.T) {
	t.Parallel()
	h := NewHandler(connDeps(&errGraph{err: errors.New("db down")}))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, connReq(http.MethodPost, "/v1/connections/follows", `{"gcid":"`+connOther+`"}`))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d, want 500", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// DELETE /v1/connections/follows/{gcid}
// ---------------------------------------------------------------------------

func TestUnfollowMember_204(t *testing.T) {
	t.Parallel()
	graph := inmem.NewSocialGraph()
	_, _, _ = graph.Follow(context.Background(), connTenant, connGCID, connOther)
	h := NewHandler(connDeps(graph))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, connReq(http.MethodDelete, "/v1/connections/follows/"+connOther, ""))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status=%d, want 204", rr.Code)
	}
}

func TestUnfollowMember_Fault_500And501(t *testing.T) {
	t.Parallel()
	// Store fault → 500.
	h500 := NewHandler(connDeps(&errGraph{err: errors.New("db down")}))
	rr := httptest.NewRecorder()
	h500.ServeHTTP(rr, connReq(http.MethodDelete, "/v1/connections/follows/"+connOther, ""))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("fault: status=%d, want 500", rr.Code)
	}

	// Nil graph → 501.
	h501 := NewHandler(Deps{})
	rr = httptest.NewRecorder()
	h501.ServeHTTP(rr, connReq(http.MethodDelete, "/v1/connections/follows/"+connOther, ""))
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("nil graph: status=%d, want 501", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// POST /v1/connections/blocks
// ---------------------------------------------------------------------------

func TestBlockMember_204(t *testing.T) {
	t.Parallel()
	graph := inmem.NewSocialGraph()
	h := NewHandler(connDeps(graph))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, connReq(http.MethodPost, "/v1/connections/blocks", `{"gcid":"`+connOther+`"}`))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status=%d body=%s, want 204", rr.Code, rr.Body.String())
	}
}

func TestBlockMember_SelfBlock_400AndErrors(t *testing.T) {
	t.Parallel()
	h400 := NewHandler(connDeps(inmem.NewSocialGraph()))
	rr := httptest.NewRecorder()
	h400.ServeHTTP(rr, connReq(http.MethodPost, "/v1/connections/blocks", `{"gcid":"`+connGCID+`"}`))
	if rr.Code != http.StatusBadRequest {
		t.Errorf("self-block: status=%d, want 400", rr.Code)
	}

	h500 := NewHandler(connDeps(&errGraph{err: errors.New("db down")}))
	rr = httptest.NewRecorder()
	h500.ServeHTTP(rr, connReq(http.MethodPost, "/v1/connections/blocks", `{"gcid":"`+connOther+`"}`))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("fault: status=%d, want 500", rr.Code)
	}

	hNil := NewHandler(Deps{})
	rr = httptest.NewRecorder()
	hNil.ServeHTTP(rr, connReq(http.MethodPost, "/v1/connections/blocks", `{"gcid":"`+connOther+`"}`))
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("nil graph: status=%d, want 501", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// DELETE /v1/connections/blocks/{gcid}
// ---------------------------------------------------------------------------

func TestUnblockMember_204AndErrors(t *testing.T) {
	t.Parallel()
	graph := inmem.NewSocialGraph()
	_ = graph.Block(context.Background(), connTenant, connGCID, connOther)
	h := NewHandler(connDeps(graph))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, connReq(http.MethodDelete, "/v1/connections/blocks/"+connOther, ""))
	if rr.Code != http.StatusNoContent {
		t.Errorf("happy: status=%d, want 204", rr.Code)
	}

	h500 := NewHandler(connDeps(&errGraph{err: errors.New("db down")}))
	rr = httptest.NewRecorder()
	h500.ServeHTTP(rr, connReq(http.MethodDelete, "/v1/connections/blocks/"+connOther, ""))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("fault: status=%d, want 500", rr.Code)
	}

	hNil := NewHandler(Deps{})
	rr = httptest.NewRecorder()
	hNil.ServeHTTP(rr, connReq(http.MethodDelete, "/v1/connections/blocks/"+connOther, ""))
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("nil graph: status=%d, want 501", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// Legacy per-target routes are not mounted
// ---------------------------------------------------------------------------

func identityReq(method, path string) *http.Request {
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("gcid", connGCID)
	req.Header.Set("X-Tenant-Id", connTenant)
	ctx := tracing.WithTenantID(req.Context(), connTenant)
	ctx = tracing.WithGCID(ctx, connGCID)
	return req.WithContext(ctx)
}

func TestLegacyHandlers_NoLongerMounted(t *testing.T) {
	t.Parallel()
	// These handlers belong to routes that are NOT registered on the mux
	// (only /v1/connections/follows + /blocks exist). A GET on the legacy
	// path must 405/404 — pinning that the mux contract did not change.
	h := NewHandler(connDeps(inmem.NewSocialGraph()))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, identityReq(http.MethodPost, "/v1/connections/"+connOther+"/follow"))
	if rr.Code == http.StatusOK || rr.Code == http.StatusCreated {
		t.Fatalf("legacy follow route answered %d — it must not be mounted", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// GET /v1/connections/suggestions
// ---------------------------------------------------------------------------

func TestListFriendSuggestions_Success(t *testing.T) {
	t.Parallel()
	sug := &errSuggestions{items: []social.FollowSuggestion{{
		GCID:          connOther,
		DisplayName:   "Phyllis",
		SharedTags:    []string{"inheritance"},
		MutualFollows: 2,
	}}}
	h := NewHandler(Deps{Suggestions: sug})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, connReq(http.MethodGet, "/v1/connections/suggestions?limit=5", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), connOther) || !strings.Contains(rr.Body.String(), `"shared_tags":["inheritance"]`) {
		t.Errorf("suggestion not projected: %s", rr.Body.String())
	}
}

func TestListFriendSuggestions_NilTagsRenderedEmpty(t *testing.T) {
	t.Parallel()
	sug := &errSuggestions{items: []social.FollowSuggestion{{GCID: connOther, SharedTags: nil}}}
	h := NewHandler(Deps{Suggestions: sug})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, connReq(http.MethodGet, "/v1/connections/suggestions", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), `"shared_tags":[]`) {
		t.Errorf("nil shared_tags must render []: %s", rr.Body.String())
	}
}

func TestListFriendSuggestions_Errors(t *testing.T) {
	t.Parallel()
	h501 := NewHandler(Deps{})
	rr := httptest.NewRecorder()
	h501.ServeHTTP(rr, connReq(http.MethodGet, "/v1/connections/suggestions", ""))
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("nil port: status=%d, want 501", rr.Code)
	}

	h500 := NewHandler(Deps{Suggestions: &errSuggestions{err: errors.New("pg down")}})
	rr = httptest.NewRecorder()
	h500.ServeHTTP(rr, connReq(http.MethodGet, "/v1/connections/suggestions", ""))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("fault: status=%d, want 500", rr.Code)
	}

	h409 := NewHandler(Deps{Suggestions: &errSuggestions{err: social.ErrConnectionNotPermitted}})
	rr = httptest.NewRecorder()
	h409.ServeHTTP(rr, connReq(http.MethodGet, "/v1/connections/suggestions", ""))
	if rr.Code != http.StatusConflict {
		t.Errorf("domain forbidden: status=%d, want 409", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// targetFromBody / targetFromPath / resolveDisplayName helpers
// ---------------------------------------------------------------------------

func TestTargetFromBody(t *testing.T) {
	t.Parallel()

	// Valid target.
	req := connReq(http.MethodPost, "/v1/connections/follows", `{"gcid":"`+connOther+`"}`)
	rr := httptest.NewRecorder()
	target, ok := targetFromBody(rr, req)
	if !ok || target != connOther {
		t.Fatalf("ok=%v target=%q, want %q", ok, target, connOther)
	}

	// Missing gcid.
	rr2 := httptest.NewRecorder()
	_, ok2 := targetFromBody(rr2, connReq(http.MethodPost, "/v1/connections/follows", `{"gcid":""}`))
	if ok2 || rr2.Code != http.StatusBadRequest {
		t.Errorf("empty gcid: ok=%v code=%d, want 400", ok2, rr2.Code)
	}
}

func TestTargetFromPath(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodDelete, "/v1/connections/follows/"+connOther, nil)
	req.SetPathValue("gcid", connOther)
	rr := httptest.NewRecorder()
	target, ok := targetFromPath(rr, req)
	if !ok || target != connOther {
		t.Fatalf("ok=%v target=%q", ok, target)
	}

	req2 := httptest.NewRequest(http.MethodDelete, "/v1/connections/follows/x", nil)
	req2.SetPathValue("gcid", " ")
	rr2 := httptest.NewRecorder()
	_, ok2 := targetFromPath(rr2, req2)
	if ok2 || rr2.Code != http.StatusBadRequest {
		t.Errorf("empty path: ok=%v code=%d, want 400", ok2, rr2.Code)
	}
}
