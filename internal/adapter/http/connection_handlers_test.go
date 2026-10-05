// connection_handlers_test.go — the full §6 connections surface:
//
//	GET    /v1/connections?type=following|followers|blocked
//	POST   /v1/connections/follows            {gcid}
//	DELETE /v1/connections/follows/{gcid}
//	POST   /v1/connections/blocks             {gcid}
//	DELETE /v1/connections/blocks/{gcid}
//	GET    /v1/connections/suggestions
//
// plus the legacy per-target handlers (followUser/unfollowUser/blockUser/
// unblockUser — no longer mounted on the mux, exercised directly) and the
// shared helpers relationshipErrToStatus / targetFromBody / targetFromPath /
// resolveDisplayName.
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
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
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
// Legacy per-target handlers (no longer mounted — called directly)
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

func TestFollowUser_Direct(t *testing.T) {
	t.Parallel()
	graph := inmem.NewSocialGraph()
	conns := &recConnections{}
	h2 := NewHandler(Deps{Graph: graph, Connections: conns})

	// Created path.
	req := identityReq(http.MethodPost, "/v1/connections/x/follow")
	req.SetPathValue("target_gcid", connOther)
	rr := httptest.NewRecorder()
	h2.followUser(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("created: status=%d body=%s, want 201", rr.Code, rr.Body.String())
	}
	if len(conns.saved) != 1 || conns.saved[0] != connOther {
		t.Errorf("SaveFollow not called with target: %v", conns.saved)
	}

	// Duplicate → 200.
	req2 := identityReq(http.MethodPost, "/v1/connections/x/follow")
	req2.SetPathValue("target_gcid", connOther)
	rr2 := httptest.NewRecorder()
	h2.followUser(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("duplicate: status=%d, want 200", rr2.Code)
	}

	// Empty target → 400.
	req3 := identityReq(http.MethodPost, "/v1/connections/x/follow")
	req3.SetPathValue("target_gcid", "   ")
	rr3 := httptest.NewRecorder()
	h2.followUser(rr3, req3)
	if rr3.Code != http.StatusBadRequest {
		t.Fatalf("empty target: status=%d, want 400", rr3.Code)
	}

	// Self-follow via graph → 400.
	req4 := identityReq(http.MethodPost, "/v1/connections/x/follow")
	req4.SetPathValue("target_gcid", connGCID)
	rr4 := httptest.NewRecorder()
	h2.followUser(rr4, req4)
	if rr4.Code != http.StatusBadRequest {
		t.Fatalf("self-follow: status=%d, want 400", rr4.Code)
	}

	// Graph fault → 500.
	h5 := NewHandler(Deps{Graph: &errGraph{err: errors.New("db down")}})
	req5 := identityReq(http.MethodPost, "/v1/connections/x/follow")
	req5.SetPathValue("target_gcid", connOther)
	rr5 := httptest.NewRecorder()
	h5.followUser(rr5, req5)
	if rr5.Code != http.StatusInternalServerError {
		t.Fatalf("graph fault: status=%d, want 500", rr5.Code)
	}

	// Persist fault → 500.
	h6 := NewHandler(Deps{Graph: graph, Connections: &recConnections{saveErr: errors.New("pg down")}})
	req6 := identityReq(http.MethodPost, "/v1/connections/x/follow")
	req6.SetPathValue("target_gcid", connOther)
	rr6 := httptest.NewRecorder()
	h6.followUser(rr6, req6)
	if rr6.Code != http.StatusInternalServerError {
		t.Fatalf("persist fault: status=%d, want 500", rr6.Code)
	}

	// Nil graph → 501.
	h7 := NewHandler(Deps{})
	req7 := identityReq(http.MethodPost, "/v1/connections/x/follow")
	req7.SetPathValue("target_gcid", connOther)
	rr7 := httptest.NewRecorder()
	h7.followUser(rr7, req7)
	if rr7.Code != http.StatusNotImplemented {
		t.Fatalf("nil graph: status=%d, want 501", rr7.Code)
	}
}

func TestUnfollowUser_Direct(t *testing.T) {
	t.Parallel()
	graph := inmem.NewSocialGraph()
	_, _, _ = graph.Follow(context.Background(), connTenant, connGCID, connOther)
	conns := &recConnections{}
	h := NewHandler(Deps{Graph: graph, Connections: conns})

	req := identityReq(http.MethodDelete, "/v1/connections/x/follow")
	req.SetPathValue("target_gcid", connOther)
	rr := httptest.NewRecorder()
	h.unfollowUser(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status=%d, want 204", rr.Code)
	}
	if len(conns.deleted) != 1 || conns.deleted[0] != connOther {
		t.Errorf("DeleteFollow not called: %v", conns.deleted)
	}

	// Empty target → 400; nil graph → 501.
	h400 := NewHandler(Deps{Graph: graph})
	req2 := identityReq(http.MethodDelete, "/v1/connections/x/follow")
	req2.SetPathValue("target_gcid", " ")
	rr2 := httptest.NewRecorder()
	h400.unfollowUser(rr2, req2)
	if rr2.Code != http.StatusBadRequest {
		t.Errorf("empty target: status=%d, want 400", rr2.Code)
	}

	h501 := NewHandler(Deps{})
	req3 := identityReq(http.MethodDelete, "/v1/connections/x/follow")
	req3.SetPathValue("target_gcid", connOther)
	rr3 := httptest.NewRecorder()
	h501.unfollowUser(rr3, req3)
	if rr3.Code != http.StatusNotImplemented {
		t.Errorf("nil graph: status=%d, want 501", rr3.Code)
	}
}

func TestBlockUser_Direct(t *testing.T) {
	t.Parallel()
	graph := inmem.NewSocialGraph()
	conns := &recConnections{}
	h := NewHandler(Deps{Graph: graph, Connections: conns})

	req := identityReq(http.MethodPost, "/v1/connections/x/block")
	req.SetPathValue("target_gcid", connOther)
	rr := httptest.NewRecorder()
	h.blockUser(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s, want 201", rr.Code, rr.Body.String())
	}
	if len(conns.blocks) != 1 || conns.blocks[0] != connOther {
		t.Errorf("SaveBlock not called: %v", conns.blocks)
	}

	// Self block → 400 (mapped from ErrSelfFollow). The inmem graph returns
	// ErrSelfBlock, which the handler maps to 500 — the pg adapter surfaces
	// ErrSelfFollow, so drive that with a fake.
	h400 := NewHandler(Deps{Graph: &errGraph{err: social.ErrSelfFollow}})
	req2 := identityReq(http.MethodPost, "/v1/connections/x/block")
	req2.SetPathValue("target_gcid", connGCID)
	rr2 := httptest.NewRecorder()
	h400.blockUser(rr2, req2)
	if rr2.Code != http.StatusBadRequest {
		t.Errorf("self-block: status=%d, want 400", rr2.Code)
	}

	// Graph fault (non-self) → 500; empty target → 400; nil graph → 501.
	h500 := NewHandler(Deps{Graph: &errGraph{err: errors.New("boom")}})
	req3 := identityReq(http.MethodPost, "/v1/connections/x/block")
	req3.SetPathValue("target_gcid", connOther)
	rr3 := httptest.NewRecorder()
	h500.blockUser(rr3, req3)
	if rr3.Code != http.StatusInternalServerError {
		t.Errorf("graph fault: status=%d, want 500", rr3.Code)
	}

	req4 := identityReq(http.MethodPost, "/v1/connections/x/block")
	req4.SetPathValue("target_gcid", " ")
	rr4 := httptest.NewRecorder()
	h400.blockUser(rr4, req4)
	if rr4.Code != http.StatusBadRequest {
		t.Errorf("empty target: status=%d, want 400", rr4.Code)
	}

	h501 := NewHandler(Deps{})
	req5 := identityReq(http.MethodPost, "/v1/connections/x/block")
	req5.SetPathValue("target_gcid", connOther)
	rr5 := httptest.NewRecorder()
	h501.blockUser(rr5, req5)
	if rr5.Code != http.StatusNotImplemented {
		t.Errorf("nil graph: status=%d, want 501", rr5.Code)
	}
}

func TestUnblockUser_Direct(t *testing.T) {
	t.Parallel()
	graph := inmem.NewSocialGraph()
	_ = graph.Block(context.Background(), connTenant, connGCID, connOther)
	conns := &recConnections{}
	h := NewHandler(Deps{Graph: graph, Connections: conns})

	req := identityReq(http.MethodDelete, "/v1/connections/x/block")
	req.SetPathValue("target_gcid", connOther)
	rr := httptest.NewRecorder()
	h.unblockUser(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status=%d, want 204", rr.Code)
	}
	if len(conns.unblocks) != 1 || conns.unblocks[0] != connOther {
		t.Errorf("DeleteBlock not called: %v", conns.unblocks)
	}

	h400 := NewHandler(Deps{Graph: graph})
	req2 := identityReq(http.MethodDelete, "/v1/connections/x/block")
	req2.SetPathValue("target_gcid", " ")
	rr2 := httptest.NewRecorder()
	h400.unblockUser(rr2, req2)
	if rr2.Code != http.StatusBadRequest {
		t.Errorf("empty target: status=%d, want 400", rr2.Code)
	}

	h501 := NewHandler(Deps{})
	req3 := identityReq(http.MethodDelete, "/v1/connections/x/block")
	req3.SetPathValue("target_gcid", connOther)
	rr3 := httptest.NewRecorder()
	h501.unblockUser(rr3, req3)
	if rr3.Code != http.StatusNotImplemented {
		t.Errorf("nil graph: status=%d, want 501", rr3.Code)
	}
}

// recConnections is a recording ConnectionStore for the legacy handlers.
type recConnections struct {
	saved    []string
	deleted  []string
	blocks   []string
	unblocks []string
	saveErr  error
}

func (c *recConnections) SaveFollow(_ context.Context, _, follower, followee string) error {
	c.saved = append(c.saved, followee)
	return c.saveErr
}
func (c *recConnections) DeleteFollow(_ context.Context, follower, followee string) error {
	c.deleted = append(c.deleted, followee)
	return nil
}
func (c *recConnections) SaveBlock(_ context.Context, _, blocker, blocked string) error {
	c.blocks = append(c.blocks, blocked)
	return c.saveErr
}
func (c *recConnections) DeleteBlock(_ context.Context, blocker, blocked string) error {
	c.unblocks = append(c.unblocks, blocked)
	return nil
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

// nameResolvingShares is a ShareRepo whose extra method satisfies
// resolveDisplayName's inline `nameResolver` interface.
type nameResolvingShares struct{}

func (s *nameResolvingShares) SaveShare(_ context.Context, _ *atom_share.Share) error { return nil }
func (s *nameResolvingShares) GetShare(_ context.Context, _ string) (*atom_share.Share, error) {
	return nil, atom_share.ErrNotFound
}
func (s *nameResolvingShares) GetShareByAtom(_ context.Context, _, _ string) (*atom_share.Share, error) {
	return nil, atom_share.ErrNotFound
}
func (s *nameResolvingShares) ListSharedAtoms(_ context.Context, _ string, _ string, _ int, _, _ string, _ string, _, _ []string) ([]atom_share.Share, string, error) {
	return nil, "", nil
}
func (s *nameResolvingShares) AppendEvent(_ context.Context, _ *atom_share.ShareEvent) error { return nil }
func (s *nameResolvingShares) ListEvents(_ context.Context, _ string) ([]atom_share.ShareEvent, error) {
	return nil, nil
}
func (s *nameResolvingShares) ResolveDisplayName(_ context.Context, gcid string) (string, error) {
	return "name-of-" + gcid, nil
}

func TestResolveDisplayName(t *testing.T) {
	t.Parallel()
	h := NewHandler(Deps{Shares: &nameResolvingShares{}})
	req := connReq(http.MethodGet, "/v1/connections", "")
	got := h.resolveDisplayName(req, connOther)
	if got != "name-of-"+connOther {
		t.Errorf("resolveDisplayName = %q, want name-of-"+connOther, got)
	}

	// Nil Shares → "".
	hNil := NewHandler(Deps{})
	if got := hNil.resolveDisplayName(req, connOther); got != "" {
		t.Errorf("nil Shares: got %q, want empty", got)
	}

	// A ShareRepo WITHOUT the extra method → "".
	hPlain := NewHandler(Deps{Shares: inmem.NewShareRepo()})
	if got := hPlain.resolveDisplayName(req, connOther); got != "" {
		t.Errorf("plain repo: got %q, want empty", got)
	}
}