// milestone_drafts_handlers_test.go — the C+ Familiar-milestone drafts + share
// preference REST surface (CHO-2258).
//
// Why these routes exist: CHO-2203 made the milestone lane durable, but the
// default policy is `draft` and NOTHING could read a draft back — every
// milestone since PROD-G landed in post_drafts for an audience of nobody, and
// no learner could opt into `auto` because PreferenceStore.Upsert had no caller.
// These are the five operations that close that loop.
package httpadapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/adapter/subscribers"
)

// testTenant / testGCID are the package-wide identities (atom_id_boundary_test.go).
const testDraft = "01970000-0000-7000-a000-0000000000d1"

// fakeMilestoneDrafts implements MilestoneDrafts, recording the scope it was
// called with so tests can prove the caller identity reached the store.
type fakeMilestoneDrafts struct {
	pending []subscribers.Draft
	postID  string

	listErr    error
	publishErr error
	discardErr error

	gotTenant  string
	gotGCID    string
	gotDraftID string
}

func (f *fakeMilestoneDrafts) ListPending(_ context.Context, tenantID, gcid string) ([]subscribers.Draft, error) {
	f.gotTenant, f.gotGCID = tenantID, gcid
	return f.pending, f.listErr
}

func (f *fakeMilestoneDrafts) PublishDraft(_ context.Context, tenantID, gcid, draftID string) (string, error) {
	f.gotTenant, f.gotGCID, f.gotDraftID = tenantID, gcid, draftID
	return f.postID, f.publishErr
}

func (f *fakeMilestoneDrafts) Discard(_ context.Context, tenantID, gcid, draftID string) error {
	f.gotTenant, f.gotGCID, f.gotDraftID = tenantID, gcid, draftID
	return f.discardErr
}

// fakeMilestonePrefs implements MilestonePrefs.
type fakeMilestonePrefs struct {
	policy    subscribers.Policy
	found     bool
	getErr    error
	upsertErr error

	gotTenant string
	gotGCID   string
	gotPolicy subscribers.Policy
}

func (f *fakeMilestonePrefs) Get(_ context.Context, tenantID, gcid string) (subscribers.Policy, bool, error) {
	f.gotTenant, f.gotGCID = tenantID, gcid
	return f.policy, f.found, f.getErr
}

func (f *fakeMilestonePrefs) Upsert(_ context.Context, tenantID, gcid string, p subscribers.Policy) error {
	f.gotTenant, f.gotGCID, f.gotPolicy = tenantID, gcid, p
	return f.upsertErr
}

func milestoneDeps(d *fakeMilestoneDrafts, p *fakeMilestonePrefs) Deps {
	return Deps{MilestoneDrafts: d, MilestonePrefs: p}
}

// req builds an identity-stamped request the way the gateway does.
func milestoneReq(method, path, body string) *http.Request {
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	r.Header.Set("gcid", testGCID)
	r.Header.Set("X-Tenant-Id", testTenant)
	return r
}

// -----------------------------------------------------------------------------
// Route registration — these paths are ALREADY LIVE in the mesh
// -----------------------------------------------------------------------------

// The Istio AuthorizationPolicy on ns/sharing already allowlists exactly
// /v1/me/post-drafts, /v1/me/post-drafts/* and
// /v1/me/preferences/familiar-milestone-share (verified live 2026-07-17). The
// allowlist is a strict per-path match with no wildcard allow-all, so a route
// served at any OTHER path is mesh-403'd at the edge while every unit test here
// still passes. This test pins the served paths to the deployed contract.
func TestMilestoneRoutes_MatchTheLiveIstioAllowlist(t *testing.T) {
	t.Parallel()
	h := NewHandler(milestoneDeps(&fakeMilestoneDrafts{}, &fakeMilestonePrefs{}))

	for _, tc := range []struct{ method, path string }{
		{"GET", "/v1/me/post-drafts"},
		{"POST", "/v1/me/post-drafts/" + testDraft + "/publish"},
		{"POST", "/v1/me/post-drafts/" + testDraft + "/discard"},
		{"GET", "/v1/me/preferences/familiar-milestone-share"},
		{"POST", "/v1/me/preferences/familiar-milestone-share"},
	} {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, milestoneReq(tc.method, tc.path, `{"policy":"auto"}`))
		if rr.Code == http.StatusNotFound || rr.Code == http.StatusMethodNotAllowed {
			t.Errorf("%s %s is not routed (%d) — the mesh allowlists this exact path", tc.method, tc.path, rr.Code)
		}
	}
}

// -----------------------------------------------------------------------------
// GET /v1/me/post-drafts
// -----------------------------------------------------------------------------

func TestListPostDrafts_ReturnsOwnersPendingDrafts(t *testing.T) {
	t.Parallel()
	drafts := &fakeMilestoneDrafts{pending: []subscribers.Draft{{
		DraftID:           testDraft,
		TenantID:          testTenant,
		AuthorGCID:        testGCID,
		ComposedFromTopic: subscribers.TopicFamiliarHatched,
		Body:              "Ember hatched!",
		Metadata:          map[string]interface{}{"familiar_id": "fam-1"},
		Status:            "pending",
		CreatedAt:         time.Now().UTC(),
	}}}
	h := NewHandler(milestoneDeps(drafts, &fakeMilestonePrefs{}))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, milestoneReq("GET", "/v1/me/post-drafts", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	var resp listPostDraftsResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Drafts) != 1 {
		t.Fatalf("expected 1 draft; got %d", len(resp.Drafts))
	}
	if resp.Drafts[0].DraftID != testDraft || resp.Drafts[0].Body != "Ember hatched!" {
		t.Errorf("draft not projected: %+v", resp.Drafts[0])
	}
}

// The scope must come from the gateway-stamped identity, never from a param a
// caller could set — that is the whole reason RLS reads the tenant from ctx.
func TestListPostDrafts_ScopesToTheCallerIdentityNotAParam(t *testing.T) {
	t.Parallel()
	drafts := &fakeMilestoneDrafts{}
	h := NewHandler(milestoneDeps(drafts, &fakeMilestonePrefs{}))

	rr := httptest.NewRecorder()
	// A caller trying to widen scope via query params must be ignored.
	h.ServeHTTP(rr, milestoneReq("GET", "/v1/me/post-drafts?gcid=someone-else&tenant_id=other-tenant", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if drafts.gotGCID != testGCID {
		t.Errorf("store scoped to gcid %q; want the header identity %q", drafts.gotGCID, testGCID)
	}
	if drafts.gotTenant != testTenant {
		t.Errorf("store scoped to tenant %q; want the header identity %q", drafts.gotTenant, testTenant)
	}
}

// An empty list must serialise as [] — a nil slice renders `null`, which the FE
// cannot iterate.
func TestListPostDrafts_EmptyRendersEmptyArrayNotNull(t *testing.T) {
	t.Parallel()
	h := NewHandler(milestoneDeps(&fakeMilestoneDrafts{pending: nil}, &fakeMilestonePrefs{}))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, milestoneReq("GET", "/v1/me/post-drafts", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), `"drafts":[]`) {
		t.Errorf(`expected "drafts":[]; got %s`, rr.Body.String())
	}
}

func TestListPostDrafts_StoreError_500NotEmptyList(t *testing.T) {
	t.Parallel()
	h := NewHandler(milestoneDeps(&fakeMilestoneDrafts{listErr: errors.New("boom")}, &fakeMilestonePrefs{}))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, milestoneReq("GET", "/v1/me/post-drafts", ""))
	// A failed read reported as an empty list tells the learner they have no
	// milestones waiting. Fail loud.
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestListPostDrafts_NilPort_501(t *testing.T) {
	t.Parallel()
	h := NewHandler(Deps{})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, milestoneReq("GET", "/v1/me/post-drafts", ""))
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d", rr.Code)
	}
}

func TestListPostDrafts_MissingIdentity_400(t *testing.T) {
	t.Parallel()
	h := NewHandler(milestoneDeps(&fakeMilestoneDrafts{}, &fakeMilestonePrefs{}))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/v1/me/post-drafts", nil))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

// -----------------------------------------------------------------------------
// POST /v1/me/post-drafts/{draft_id}/publish
// -----------------------------------------------------------------------------

func TestPublishPostDraft_201_ReturnsPostIDAndPassesCallerScope(t *testing.T) {
	t.Parallel()
	drafts := &fakeMilestoneDrafts{postID: "01970000-0000-7000-a000-0000000000p1"}
	h := NewHandler(milestoneDeps(drafts, &fakeMilestonePrefs{}))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, milestoneReq("POST", "/v1/me/post-drafts/"+testDraft+"/publish", ""))
	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rr.Code, rr.Body.String())
	}
	var resp publishPostDraftResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.PostID != drafts.postID {
		t.Errorf("post_id = %q; want %q", resp.PostID, drafts.postID)
	}
	if drafts.gotDraftID != testDraft {
		t.Errorf("draft_id = %q; want %q", drafts.gotDraftID, testDraft)
	}
	if drafts.gotGCID != testGCID {
		t.Errorf("publish must be scoped to the caller gcid; got %q", drafts.gotGCID)
	}
}

// ErrDraftNotPending covers "no such draft" / "not yours" / "already published"
// with ONE 404 — distinguishing them would let a learner probe for the
// existence of another learner's drafts.
func TestPublishPostDraft_NotPending_404(t *testing.T) {
	t.Parallel()
	h := NewHandler(milestoneDeps(&fakeMilestoneDrafts{publishErr: subscribers.ErrDraftNotPending}, &fakeMilestonePrefs{}))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, milestoneReq("POST", "/v1/me/post-drafts/"+testDraft+"/publish", ""))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rr.Code, rr.Body.String())
	}
}

// A backing-store fault is OUR fault. Reporting it as 404 would tell the learner
// their milestone never existed.
func TestPublishPostDraft_StoreFault_500Not404(t *testing.T) {
	t.Parallel()
	h := NewHandler(milestoneDeps(&fakeMilestoneDrafts{publishErr: errors.New("connection reset")}, &fakeMilestonePrefs{}))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, milestoneReq("POST", "/v1/me/post-drafts/"+testDraft+"/publish", ""))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestPublishPostDraft_NilPort_501(t *testing.T) {
	t.Parallel()
	h := NewHandler(Deps{})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, milestoneReq("POST", "/v1/me/post-drafts/"+testDraft+"/publish", ""))
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d", rr.Code)
	}
}

// -----------------------------------------------------------------------------
// POST /v1/me/post-drafts/{draft_id}/discard
// -----------------------------------------------------------------------------

func TestDiscardPostDraft_204_PassesCallerScope(t *testing.T) {
	t.Parallel()
	drafts := &fakeMilestoneDrafts{}
	h := NewHandler(milestoneDeps(drafts, &fakeMilestonePrefs{}))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, milestoneReq("POST", "/v1/me/post-drafts/"+testDraft+"/discard", ""))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rr.Code, rr.Body.String())
	}
	if drafts.gotDraftID != testDraft || drafts.gotGCID != testGCID {
		t.Errorf("discard scope = (%q,%q); want (%q,%q)", drafts.gotDraftID, drafts.gotGCID, testDraft, testGCID)
	}
}

func TestDiscardPostDraft_NotPending_404(t *testing.T) {
	t.Parallel()
	h := NewHandler(milestoneDeps(&fakeMilestoneDrafts{discardErr: subscribers.ErrDraftNotPending}, &fakeMilestonePrefs{}))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, milestoneReq("POST", "/v1/me/post-drafts/"+testDraft+"/discard", ""))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func TestDiscardPostDraft_StoreFault_500Not404(t *testing.T) {
	t.Parallel()
	h := NewHandler(milestoneDeps(&fakeMilestoneDrafts{discardErr: errors.New("connection reset")}, &fakeMilestonePrefs{}))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, milestoneReq("POST", "/v1/me/post-drafts/"+testDraft+"/discard", ""))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr.Code)
	}
}

// -----------------------------------------------------------------------------
// GET/POST /v1/me/preferences/familiar-milestone-share
// -----------------------------------------------------------------------------

func TestGetSharePref_ReturnsStoredPolicy(t *testing.T) {
	t.Parallel()
	prefs := &fakeMilestonePrefs{policy: subscribers.PolicyAuto, found: true}
	h := NewHandler(milestoneDeps(&fakeMilestoneDrafts{}, prefs))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, milestoneReq("GET", "/v1/me/preferences/familiar-milestone-share", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	var resp sharePrefResponse
	_ = json.NewDecoder(rr.Body).Decode(&resp)
	if resp.Policy != string(subscribers.PolicyAuto) {
		t.Errorf("policy = %q; want auto", resp.Policy)
	}
}

// No row is the NORMAL state — user_preferences starts empty and the subscriber
// falls back to its DefaultPolicy. The API must report the SAME effective
// policy, or the UI shows the learner a setting that is not the one in force.
func TestGetSharePref_NoRow_ReportsTheSubscribersEffectiveDefault(t *testing.T) {
	t.Parallel()
	prefs := &fakeMilestonePrefs{found: false}
	h := NewHandler(milestoneDeps(&fakeMilestoneDrafts{}, prefs))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, milestoneReq("GET", "/v1/me/preferences/familiar-milestone-share", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var resp sharePrefResponse
	_ = json.NewDecoder(rr.Body).Decode(&resp)
	if resp.Policy != string(subscribers.PolicyDraft) {
		t.Errorf("policy = %q; want the subscriber's default %q", resp.Policy, subscribers.PolicyDraft)
	}
	if !resp.IsDefault {
		t.Error("response must mark the value as the unset default, not a stored choice")
	}
}

func TestGetSharePref_StoreFault_500(t *testing.T) {
	t.Parallel()
	h := NewHandler(milestoneDeps(&fakeMilestoneDrafts{}, &fakeMilestonePrefs{getErr: errors.New("boom")}))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, milestoneReq("GET", "/v1/me/preferences/familiar-milestone-share", ""))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr.Code)
	}
}

func TestSetSharePref_UpsertsPolicyForTheCaller(t *testing.T) {
	t.Parallel()
	prefs := &fakeMilestonePrefs{}
	h := NewHandler(milestoneDeps(&fakeMilestoneDrafts{}, prefs))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, milestoneReq("POST", "/v1/me/preferences/familiar-milestone-share", `{"policy":"auto"}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if prefs.gotPolicy != subscribers.PolicyAuto {
		t.Errorf("upserted policy = %q; want auto", prefs.gotPolicy)
	}
	if prefs.gotGCID != testGCID || prefs.gotTenant != testTenant {
		t.Errorf("upsert scope = (%q,%q); want the header identity", prefs.gotTenant, prefs.gotGCID)
	}
}

// The three policies are the enum on user_preferences. Anything else must be
// refused at the boundary, not passed to Postgres to reject as a 22P02/22023.
func TestSetSharePref_AcceptsExactlyTheCanonicalPolicySet(t *testing.T) {
	t.Parallel()
	for _, ok := range []string{"auto", "draft", "suppress"} {
		h := NewHandler(milestoneDeps(&fakeMilestoneDrafts{}, &fakeMilestonePrefs{}))
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, milestoneReq("POST", "/v1/me/preferences/familiar-milestone-share", `{"policy":"`+ok+`"}`))
		if rr.Code != http.StatusOK {
			t.Errorf("policy %q must be accepted; got %d", ok, rr.Code)
		}
	}
	for _, bad := range []string{"", "AUTO_POST", "yes", "null", "drafts"} {
		h := NewHandler(milestoneDeps(&fakeMilestoneDrafts{}, &fakeMilestonePrefs{}))
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, milestoneReq("POST", "/v1/me/preferences/familiar-milestone-share", `{"policy":"`+bad+`"}`))
		if rr.Code != http.StatusBadRequest {
			t.Errorf("policy %q must be refused with 400; got %d", bad, rr.Code)
		}
	}
}

func TestSetSharePref_StoreFault_500(t *testing.T) {
	t.Parallel()
	h := NewHandler(milestoneDeps(&fakeMilestoneDrafts{}, &fakeMilestonePrefs{upsertErr: errors.New("boom")}))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, milestoneReq("POST", "/v1/me/preferences/familiar-milestone-share", `{"policy":"auto"}`))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr.Code)
	}
}

func TestSetSharePref_NilPort_501(t *testing.T) {
	t.Parallel()
	h := NewHandler(Deps{})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, milestoneReq("POST", "/v1/me/preferences/familiar-milestone-share", `{"policy":"auto"}`))
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d", rr.Code)
	}
}
