// share_handlers_extra_test.go — the share/bookmark lifecycle branches the
// atom_id boundary tests do not touch (they pin 400/500/412/204 around the
// pathUUID/absent cases):
//
//	POST   /v1/atoms/{atom_id}/share   — guardrail, R1, license validation,
//	                                     royalty, revision pinning, success
//	DELETE /v1/atoms/{atom_id}/share   — R1 mismatch, append-fault, success
//	GET    /v1/feed/shared-atoms       — scopes, reaction counts, enrichment
//	POST   /v1/atoms/{atom_id}/bookmark — malformed body, success w/ revision
//	GET    /v1/me/bookmarks            — paginated success with cursor
package httpadapter

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/adapter/modelarmor"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
	"github.com/apollo-chora/chora-sharing/internal/domain/bookmark"
	"github.com/apollo-chora/chora-sharing/internal/domain/reaction"
)

const (
	shTenant  = "01970000-0000-7000-8000-0000000000e1"
	shGCID    = "01970000-0000-7000-9000-0000000000e1"
	shOther   = "01970000-0000-7000-9000-0000000000e2"
	shAtomID  = "01890a5d-ac96-774b-bcce-b302099a8057" // well-formed UUIDv7
	shFeedID  = "019700aa-0000-7000-8000-0000000000e1"
)

func shReq(method, path, body string) *http.Request {
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	r.Header.Set("gcid", shGCID)
	r.Header.Set("X-Tenant-Id", shTenant)
	return r
}

// guardrailFake is a modelarmor.GuardrailPort returning a fixed verdict.
type guardrailFake struct {
	verdict string
	err     error
}

func (g *guardrailFake) Screen(_ context.Context, _ modelarmor.ScreenRequest) (modelarmor.ScreenVerdict, error) {
	if g.err != nil {
		return modelarmor.ScreenVerdict{}, g.err
	}
	return modelarmor.ScreenVerdict{Verdict: g.verdict}, nil
}

// projRepo is an AtomProjectionReader returning a fixed projection (or error).
type projRepo struct {
	proj atom_projection.Projection
	err  error
}

func (p *projRepo) Get(_ context.Context, _ string) (atom_projection.Projection, error) {
	return p.proj, p.err
}
func (p *projRepo) ListRandom(_ context.Context, _ string, _ []string, _ int) ([]atom_projection.Projection, error) {
	return nil, nil
}

// ownedProjection is the R1-correct projection for shGCID's atom.
func ownedProjection() atom_projection.Projection {
	return atom_projection.Projection{
		AtomID:            shAtomID,
		TenantID:          shTenant,
		RevisionID:        "rev-current",
		OwnerGCID:         shGCID,
		AuthorDisplayName: "Phyllis",
		Stem:              "What is 2 + 2?",
		QuestionType:      atom_projection.QuestionTypeMCQ,
		Options:           []string{"4", "5"},
	}
}

// ---------------------------------------------------------------------------
// POST /v1/atoms/{atom_id}/share
// ---------------------------------------------------------------------------

func TestShareAtom_Success(t *testing.T) {
	t.Parallel()
	shares := inmem.NewShareRepo()
	h := NewHandler(Deps{
		Shares:      shares,
		Projections: &projRepo{proj: ownedProjection()},
	})

	req := shReq(http.MethodPost, "/v1/atoms/"+shAtomID+"/share", `{"license_terms":"free","caption":"Check this out"}`)
	req.Header.Set("Idempotency-Key", "idem-share-1")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s, want 201", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("Location") == "" {
		t.Error("201 must carry a Location header")
	}
	// The share must be persisted with the R1 snapshot + explicit... default
	// revision (body omitted revision → projection's current).
	got, err := shares.GetShareByAtom(context.Background(), shAtomID, shGCID)
	if err != nil {
		t.Fatalf("share not persisted: %v", err)
	}
	if got.RevisionID != "rev-current" {
		t.Errorf("revision = %q, want the projection's current", got.RevisionID)
	}
	if got.OwnerGCID != shGCID || got.AuthorDisplayName != "Phyllis" {
		t.Errorf("R1 snapshot not denormalised: %+v", got)
	}
}

func TestShareAtom_RoyaltyLicenseRequiresRate(t *testing.T) {
	t.Parallel()
	// R2: a royalty license without a rate must be rejected at the boundary.
	h := NewHandler(Deps{Shares: inmem.NewShareRepo(), Projections: &projRepo{proj: ownedProjection()}})
	req := shReq(http.MethodPost, "/v1/atoms/"+shAtomID+"/share", `{"license_terms":"royalty_pct"}`)
	req.Header.Set("Idempotency-Key", "idem-share-roy")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s, want 400", rr.Code, rr.Body.String())
	}
}

func TestShareAtom_RoyaltyLicenseHappyPath(t *testing.T) {
	t.Parallel()
	shares := inmem.NewShareRepo()
	h := NewHandler(Deps{Shares: shares, Projections: &projRepo{proj: ownedProjection()}})
	body := `{"license_terms":"royalty_pct","royalty_rate":{"kind":"pct","value":10},"atom_revision_id":"rev-pinned"}`
	req := shReq(http.MethodPost, "/v1/atoms/"+shAtomID+"/share", body)
	req.Header.Set("Idempotency-Key", "idem-share-roy2")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s, want 201", rr.Code, rr.Body.String())
	}
	got, _ := shares.GetShareByAtom(context.Background(), shAtomID, shGCID)
	if got.RevisionID != "rev-pinned" {
		t.Errorf("pinned revision = %q, want rev-pinned", got.RevisionID)
	}
	if got.License != atom_share.LicenseRoyaltyPct || got.Rate.Value != 10 {
		t.Errorf("royalty not frozen: license=%s rate=%+v", got.License, got.Rate)
	}
}

func TestShareAtom_GuardrailPaths(t *testing.T) {
	t.Parallel()
	base := func(guard modelarmor.GuardrailPort) Deps {
		return Deps{Shares: inmem.NewShareRepo(), Projections: &projRepo{proj: ownedProjection()}, Guardrail: guard}
	}

	// Screen fault → 500.
	h500 := NewHandler(base(&guardrailFake{err: errors.New("guardrail down")}))
	req := shReq(http.MethodPost, "/v1/atoms/"+shAtomID+"/share", `{"license_terms":"free","caption":"hello"}`)
	req.Header.Set("Idempotency-Key", "k")
	rr := httptest.NewRecorder()
	h500.ServeHTTP(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("screen fault: status=%d, want 500", rr.Code)
	}

	// Blocked → 422 (never persisted).
	h422 := NewHandler(base(&guardrailFake{verdict: modelarmor.VerdictBlocked}))
	req = shReq(http.MethodPost, "/v1/atoms/"+shAtomID+"/share", `{"license_terms":"free","caption":"bad caption"}`)
	req.Header.Set("Idempotency-Key", "k")
	rr = httptest.NewRecorder()
	h422.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnprocessableEntity {
		t.Errorf("blocked: status=%d, want 422", rr.Code)
	}
	if _, err := h422.deps.Shares.GetShareByAtom(context.Background(), shAtomID, shGCID); err == nil {
		t.Error("a blocked caption must never reach the store")
	}

	// Flagged → proceeds (audit-only).
	shares := inmem.NewShareRepo()
	hFlagged := NewHandler(Deps{Shares: shares, Projections: &projRepo{proj: ownedProjection()}, Guardrail: &guardrailFake{verdict: modelarmor.VerdictFlagged}})
	req = shReq(http.MethodPost, "/v1/atoms/"+shAtomID+"/share", `{"license_terms":"free","caption":"edge case"}`)
	req.Header.Set("Idempotency-Key", "k")
	rr = httptest.NewRecorder()
	hFlagged.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Errorf("flagged: status=%d, want 201", rr.Code)
	}

	// Empty caption with a wired guardrail must not be screened at all.
	hNoCaption := NewHandler(base(&guardrailFake{verdict: modelarmor.VerdictBlocked}))
	req = shReq(http.MethodPost, "/v1/atoms/"+shAtomID+"/share", `{"license_terms":"free"}`)
	req.Header.Set("Idempotency-Key", "k")
	rr = httptest.NewRecorder()
	hNoCaption.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Errorf("empty caption must skip the guardrail: status=%d, want 201", rr.Code)
	}
}

func TestShareAtom_ValidationBranches(t *testing.T) {
	t.Parallel()
	base := func() Deps {
		return Deps{Shares: inmem.NewShareRepo(), Projections: &projRepo{proj: ownedProjection()}}
	}

	// 501s.
	for _, deps := range []Deps{{}, {Projections: &projRepo{proj: ownedProjection()}}, {Shares: inmem.NewShareRepo()}} {
		h := NewHandler(deps)
		req := shReq(http.MethodPost, "/v1/atoms/"+shAtomID+"/share", `{"license_terms":"free"}`)
		req.Header.Set("Idempotency-Key", "k")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusNotImplemented {
			t.Errorf("deps %+v: status=%d, want 501", deps, rr.Code)
		}
	}

	h := NewHandler(base())

	// Missing Idempotency-Key → 400.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, shReq(http.MethodPost, "/v1/atoms/"+shAtomID+"/share", `{"license_terms":"free"}`))
	if rr.Code != http.StatusBadRequest {
		t.Errorf("missing idem: status=%d, want 400", rr.Code)
	}

	// Malformed body → 400.
	req := shReq(http.MethodPost, "/v1/atoms/"+shAtomID+"/share", `{bad`)
	req.Header.Set("Idempotency-Key", "k")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("malformed body: status=%d, want 400", rr.Code)
	}

	// Unknown license → 400.
	req = shReq(http.MethodPost, "/v1/atoms/"+shAtomID+"/share", `{"license_terms":"pirate"}`)
	req.Header.Set("Idempotency-Key", "k")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("unknown license: status=%d, want 400", rr.Code)
	}

	// Projection ErrNotFound → 412.
	h412 := NewHandler(Deps{Shares: inmem.NewShareRepo(), Projections: &projRepo{err: atom_projection.ErrNotFound}})
	req = shReq(http.MethodPost, "/v1/atoms/"+shAtomID+"/share", `{"license_terms":"free"}`)
	req.Header.Set("Idempotency-Key", "k")
	rr = httptest.NewRecorder()
	h412.ServeHTTP(rr, req)
	if rr.Code != http.StatusPreconditionFailed {
		t.Errorf("absent atom: status=%d, want 412", rr.Code)
	}

	// Projection lookup fault → 500.
	h500 := NewHandler(Deps{Shares: inmem.NewShareRepo(), Projections: &projRepo{err: errors.New("pg down")}})
	req = shReq(http.MethodPost, "/v1/atoms/"+shAtomID+"/share", `{"license_terms":"free"}`)
	req.Header.Set("Idempotency-Key", "k")
	rr = httptest.NewRecorder()
	h500.ServeHTTP(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("proj fault: status=%d, want 500", rr.Code)
	}

	// R1 mismatch → 403.
	foreign := ownedProjection()
	foreign.OwnerGCID = shOther
	h403 := NewHandler(Deps{Shares: inmem.NewShareRepo(), Projections: &projRepo{proj: foreign}})
	req = shReq(http.MethodPost, "/v1/atoms/"+shAtomID+"/share", `{"license_terms":"free"}`)
	req.Header.Set("Idempotency-Key", "k")
	rr = httptest.NewRecorder()
	h403.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Errorf("R1 mismatch: status=%d, want 403", rr.Code)
	}

	// SaveShare fault → 500.
	h500b := NewHandler(Deps{Shares: &failingShares{saveErr: errors.New("pg down")}, Projections: &projRepo{proj: ownedProjection()}})
	req = shReq(http.MethodPost, "/v1/atoms/"+shAtomID+"/share", `{"license_terms":"free"}`)
	req.Header.Set("Idempotency-Key", "k")
	rr = httptest.NewRecorder()
	h500b.ServeHTTP(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("save fault: status=%d, want 500", rr.Code)
	}
}

// failingShares is a ShareRepo that fails SaveShare/AppendEvent/GetShareByAtom
// on demand, delegating everything else to a real inmem repo.
type failingShares struct {
	*inmem.ShareRepo
	saveErr  error
	appendErr error

	// shareOverride, when set, replaces the GetShareByAtom result — used to
	// force the R1 defence-in-depth mismatch in revokeShare.
	shareOverride *atom_share.Share
	byAtomErr     error
}

func (f *failingShares) SaveShare(ctx context.Context, s *atom_share.Share) error {
	if f.saveErr != nil {
		return f.saveErr
	}
	return f.ShareRepo.SaveShare(ctx, s)
}
func (f *failingShares) AppendEvent(ctx context.Context, e *atom_share.ShareEvent) error {
	if f.appendErr != nil {
		return f.appendErr
	}
	return f.ShareRepo.AppendEvent(ctx, e)
}
func (f *failingShares) GetShareByAtom(ctx context.Context, atomID, owner string) (*atom_share.Share, error) {
	if f.byAtomErr != nil {
		return nil, f.byAtomErr
	}
	if f.shareOverride != nil {
		return f.shareOverride, nil
	}
	return f.ShareRepo.GetShareByAtom(ctx, atomID, owner)
}

// ---------------------------------------------------------------------------
// DELETE /v1/atoms/{atom_id}/share
// ---------------------------------------------------------------------------

func TestRevokeShare_Success(t *testing.T) {
	t.Parallel()
	shares := inmem.NewShareRepo()
	_ = shares.SaveShare(context.Background(), &atom_share.Share{
		FeedEntryID: shFeedID, TenantID: shTenant, AtomID: shAtomID,
		OwnerGCID: shGCID, RevisionID: "rev-1", License: atom_share.LicenseFree,
	})
	h := NewHandler(Deps{Shares: shares})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, shReq(http.MethodDelete, "/v1/atoms/"+shAtomID+"/share", ""))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status=%d, want 204", rr.Code)
	}
	// The entry must now be revoked (not visible).
	if _, err := shares.GetShareByAtom(context.Background(), shAtomID, shGCID); err == nil {
		t.Error("share must be revoked after DELETE")
	}
}

func TestRevokeShare_R1MismatchAndFaults(t *testing.T) {
	t.Parallel()
	// R1 defence-in-depth: share resolves to ANOTHER owner → 403.
	h403 := NewHandler(Deps{Shares: &failingShares{ShareRepo: inmem.NewShareRepo(), shareOverride: &atom_share.Share{OwnerGCID: shOther}}})
	rr := httptest.NewRecorder()
	h403.ServeHTTP(rr, shReq(http.MethodDelete, "/v1/atoms/"+shAtomID+"/share", ""))
	if rr.Code != http.StatusForbidden {
		t.Errorf("R1 mismatch: status=%d, want 403", rr.Code)
	}

	// Read fault → 500.
	h500 := NewHandler(Deps{Shares: &failingShares{ShareRepo: inmem.NewShareRepo(), byAtomErr: errors.New("pg down")}})
	rr = httptest.NewRecorder()
	h500.ServeHTTP(rr, shReq(http.MethodDelete, "/v1/atoms/"+shAtomID+"/share", ""))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("read fault: status=%d, want 500", rr.Code)
	}

	// Append fault → 500.
	shares := inmem.NewShareRepo()
	_ = shares.SaveShare(context.Background(), &atom_share.Share{
		FeedEntryID: shFeedID, TenantID: shTenant, AtomID: shAtomID,
		OwnerGCID: shGCID, RevisionID: "rev-1", License: atom_share.LicenseFree,
	})
	h500b := NewHandler(Deps{Shares: &failingShares{ShareRepo: shares, appendErr: errors.New("pg down")}})
	rr = httptest.NewRecorder()
	h500b.ServeHTTP(rr, shReq(http.MethodDelete, "/v1/atoms/"+shAtomID+"/share", ""))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("append fault: status=%d, want 500", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// GET /v1/feed/shared-atoms
// ---------------------------------------------------------------------------

func TestListSharedAtoms_SuccessWithReactionsAndEnrichment(t *testing.T) {
	t.Parallel()
	shares := inmem.NewShareRepo()
	_ = shares.SaveShare(context.Background(), &atom_share.Share{
		FeedEntryID:      shFeedID,
		TenantID:         shTenant,
		AtomID:           shAtomID,
		RevisionID:       "rev-1",
		OwnerGCID:        shGCID,
		AuthorDisplayName: "Old Name",
		StemPreview:      "Bayes",
		QuestionType:     "multiple_choice",
		Options:          []string{"A", "B"},
		License:          atom_share.LicenseFree,
		CreatedAt:        time.Now().UTC(),
	})
	reg := reaction.NewRegistry()
	_, _, _ = reg.React(context.Background(), shTenant, shOther, shFeedID, reaction.TypeLike)
	_, _, _ = reg.React(context.Background(), shTenant, shGCID, shFeedID, reaction.TypeCurious)

	profiles := inmem.NewProfilerRepo()
	h := NewHandler(Deps{
		Shares:    shares,
		Reactions: reg,
		Profiles:  profiles,
	})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, shReq(http.MethodGet, "/v1/feed/shared-atoms?scope=tenant", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, `"reaction_count":2`) {
		t.Errorf("reaction_count missing: %s", body)
	}
	if !strings.Contains(body, `"like":1`) || !strings.Contains(body, `"curious":1`) {
		t.Errorf("per-type counts missing: %s", body)
	}
	if !strings.Contains(body, `"my_reaction":"curious"`) {
		t.Errorf("my_reaction missing: %s", body)
	}
}

func TestListSharedAtoms_RoyaltyCardAndFilters(t *testing.T) {
	t.Parallel()
	shares := inmem.NewShareRepo()
	_ = shares.SaveShare(context.Background(), &atom_share.Share{
		FeedEntryID:  shFeedID,
		TenantID:     shTenant,
		AtomID:       shAtomID,
		RevisionID:   "rev-1",
		OwnerGCID:    shGCID,
		QuestionType: "multiple_choice",
		License:      atom_share.LicenseRoyaltyPct,
		Rate:         atom_share.RoyaltyRate{Kind: "pct", Value: 15},
		CreatedAt:    time.Now().UTC(),
	})
	h := NewHandler(Deps{Shares: shares, Graph: inmem.NewSocialGraph()})

	// topic_filter + question_type_filter flavour the query; scope defaults to tenant.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, shReq(http.MethodGet, "/v1/feed/shared-atoms?limit=5&cursor=&topic_filter=multiple_choice&question_type_filter=multiple_choice", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"royalty_rate":{"kind":"pct","value":15}`) {
		t.Errorf("royalty_rate missing from the card: %s", rr.Body.String())
	}
}

func TestListSharedAtoms_Branches(t *testing.T) {
	t.Parallel()

	// 501 nil Shares.
	h501 := NewHandler(Deps{})
	rr := httptest.NewRecorder()
	h501.ServeHTTP(rr, shReq(http.MethodGet, "/v1/feed/shared-atoms", ""))
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("nil shares: status=%d, want 501", rr.Code)
	}

	// scope=following with nil Graph → 501.
	h501b := NewHandler(Deps{Shares: inmem.NewShareRepo()})
	rr = httptest.NewRecorder()
	h501b.ServeHTTP(rr, shReq(http.MethodGet, "/v1/feed/shared-atoms?scope=following", ""))
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("following+nil graph: status=%d, want 501", rr.Code)
	}

	// following scope + graph error → 500.
	h500g := NewHandler(Deps{Shares: inmem.NewShareRepo(), Graph: &errGraph{err: errors.New("pg down")}})
	rr = httptest.NewRecorder()
	h500g.ServeHTTP(rr, shReq(http.MethodGet, "/v1/feed/shared-atoms?scope=following", ""))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("following graph fault: status=%d, want 500", rr.Code)
	}

	// following scope with zero followees → 200 empty (no store call).
	hEmpty := NewHandler(Deps{Shares: inmem.NewShareRepo(), Graph: inmem.NewSocialGraph()})
	rr = httptest.NewRecorder()
	hEmpty.ServeHTTP(rr, shReq(http.MethodGet, "/v1/feed/shared-atoms?scope=following", ""))
	if rr.Code != http.StatusOK {
		t.Errorf("empty following: status=%d, want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), `"cards":[]`) {
		t.Errorf("empty following must render cards:[]: %s", rr.Body.String())
	}

	// ListSharedAtoms fault → 500.
	h500 := NewHandler(Deps{Shares: &failingShares{ShareRepo: inmem.NewShareRepo(), saveErr: nil}})
	// Save a share so the read path is reached... the failing fake only fails
	// SaveShare; for a read fault use a repo stubbed to fail ListSharedAtoms.
	h500 = NewHandler(Deps{Shares: &failingSharesList{err: errors.New("pg down")}})
	rr = httptest.NewRecorder()
	h500.ServeHTTP(rr, shReq(http.MethodGet, "/v1/feed/shared-atoms", ""))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("list fault: status=%d, want 500", rr.Code)
	}

	// Reaction read fault on an existing share → 500.
	shares := inmem.NewShareRepo()
	_ = shares.SaveShare(context.Background(), &atom_share.Share{
		FeedEntryID: shFeedID, TenantID: shTenant, AtomID: shAtomID,
		OwnerGCID: shGCID, License: atom_share.LicenseFree,
	})
	hReactErr := NewHandler(Deps{Shares: shares, Reactions: &faultyReactions{listErr: errors.New("db down")}})
	rr = httptest.NewRecorder()
	hReactErr.ServeHTTP(rr, shReq(http.MethodGet, "/v1/feed/shared-atoms", ""))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("reaction fault: status=%d, want 500", rr.Code)
	}
}

// failingSharesList stubs ListSharedAtoms to fail.
type failingSharesList struct {
	err error
}

func (f *failingSharesList) SaveShare(_ context.Context, _ *atom_share.Share) error { return nil }
func (f *failingSharesList) GetShare(_ context.Context, _ string) (*atom_share.Share, error) {
	return nil, atom_share.ErrNotFound
}
func (f *failingSharesList) GetShareByAtom(_ context.Context, _, _ string) (*atom_share.Share, error) {
	return nil, atom_share.ErrNotFound
}
func (f *failingSharesList) ListSharedAtoms(_ context.Context, _ string, _ string, _ int, _, _ string, _ string, _, _ []string) ([]atom_share.Share, string, error) {
	return nil, "", f.err
}
func (f *failingSharesList) AppendEvent(_ context.Context, _ *atom_share.ShareEvent) error { return nil }
func (f *failingSharesList) ListEvents(_ context.Context, _ string) ([]atom_share.ShareEvent, error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// POST /v1/atoms/{atom_id}/bookmark + GET /v1/me/bookmarks
// ---------------------------------------------------------------------------

func TestBookmarkAtom_MalformedBodyAndSuccess(t *testing.T) {
	t.Parallel()
	repo := inmem.NewBookmarkRepo()

	// Malformed body → 400.
	h := NewHandler(Deps{Bookmarks: repo})
	req := shReq(http.MethodPost, "/v1/atoms/"+shAtomID+"/bookmark", `{nope`)
	req.Header.Set("Idempotency-Key", "k")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("malformed body: status=%d, want 400", rr.Code)
	}

	// Success with a pinned revision.
	req = shReq(http.MethodPost, "/v1/atoms/"+shAtomID+"/bookmark", `{"atom_revision_id":"rev-9"}`)
	req.Header.Set("Idempotency-Key", "k")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s, want 201", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"atom_revision_id":"rev-9"`) {
		t.Errorf("revision not in response: %s", rr.Body.String())
	}

	// Success without a body at all (nil body path).
	req = shReq(http.MethodPost, "/v1/atoms/"+shAtomID+"/bookmark", "")
	req.Header.Set("Idempotency-Key", "k2")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Errorf("no-body: status=%d, want 201", rr.Code)
	}
}

func TestListBookmarks_SuccessWithCursor(t *testing.T) {
	t.Parallel()
	repo := inmem.NewBookmarkRepo()
	bm1, _ := bookmark.NewBookmark(shTenant, shGCID, "atom-one", "")
	bm2, _ := bookmark.NewBookmark(shTenant, shGCID, "atom-two", "rev-2")
	_ = repo.Save(context.Background(), bm1)
	_ = repo.Save(context.Background(), bm2)
	h := NewHandler(Deps{Bookmarks: repo})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, shReq(http.MethodGet, "/v1/me/bookmarks?limit=10&cursor=", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "atom-one") || !strings.Contains(rr.Body.String(), "rev-2") {
		t.Errorf("bookmark cards missing: %s", rr.Body.String())
	}
}