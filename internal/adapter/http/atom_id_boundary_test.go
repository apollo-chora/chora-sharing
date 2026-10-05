package httpadapter

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
	"github.com/apollo-chora/chora-sharing/internal/domain/bookmark"
	"github.com/google/uuid"
)

// =============================================================================
// atom_id boundary validation — "whose fault is it?" (4xx/5xx + pg-as-validator)
//
// A malformed atom_id ("banana") is not a UUID, so when the handler passes it
// straight into a query it dies in Postgres on `invalid input syntax for type
// uuid (SQLSTATE 22P02)` and surfaces as HTTP 500. That pages the on-call for a
// client typo: Postgres has become the input validator, and we take the blame.
//
// The fix parses every caller-supplied atom_id AT THE BOUNDARY, before any
// query is built from it: malformed → 400 (THEIR fault, they can fix it by
// sending a real id). A valid-but-absent id keeps its domain status (412/204,
// never 400 — a 404-class answer claims we looked, which for a well-formed id
// we did). A genuine DB fault on a VALID id still returns 500 — outages MUST
// keep alerting; the fix touches only the parse-failure path.
//
// These fakes reproduce Postgres's TYPE constraint. An in-memory map has no
// type system — a non-UUID is a perfectly good map key — so ONLY a fake that
// rejects a non-UUID the way pg does can expose this class of bug.
// =============================================================================

// pgUUIDErr mimics the error the real pg adapter returns when a non-UUID string
// is bound to a `uuid`-typed column: SQLSTATE 22P02. It is an ordinary error
// (NOT a domain sentinel), exactly as a raw driver error would reach the
// handler — so the handler cannot tell it apart from any other repository
// failure. That indistinguishability is the whole disease.
func pgUUIDErr(v string) error {
	return fmt.Errorf(`pq: invalid input syntax for type uuid: %q (SQLSTATE 22P02)`, v)
}

// pgLikeProjections is an AtomProjectionReader that behaves like the pg adapter:
// a non-UUID atom_id fails with 22P02; a well-formed but unknown atom_id returns
// ErrNotFound. When outage is set, EVERY call fails with it (an infra fault on a
// perfectly valid id — the negative control that must stay 500).
type pgLikeProjections struct{ outage error }

func (p *pgLikeProjections) Get(_ context.Context, atomID string) (atom_projection.Projection, error) {
	if p.outage != nil {
		return atom_projection.Projection{}, p.outage
	}
	if _, err := uuid.Parse(atomID); err != nil {
		return atom_projection.Projection{}, pgUUIDErr(atomID)
	}
	return atom_projection.Projection{}, atom_projection.ErrNotFound
}

func (p *pgLikeProjections) ListRandom(_ context.Context, _ string, _ []string, _ int) ([]atom_projection.Projection, error) {
	return nil, nil
}

// pgLikeShares is a ShareRepo with the same pg-faithful behaviour on the
// atom_id-keyed read (GetShareByAtom). Every other method is an inert stub —
// the atom_id routes under test never reach them before the id is validated.
type pgLikeShares struct{ outage error }

func (s *pgLikeShares) GetShareByAtom(_ context.Context, atomID, _ string) (*atom_share.Share, error) {
	if s.outage != nil {
		return nil, s.outage
	}
	if _, err := uuid.Parse(atomID); err != nil {
		return nil, pgUUIDErr(atomID)
	}
	return nil, atom_share.ErrNotFound
}

func (s *pgLikeShares) SaveShare(_ context.Context, _ *atom_share.Share) error { return nil }
func (s *pgLikeShares) GetShare(_ context.Context, _ string) (*atom_share.Share, error) {
	return nil, atom_share.ErrNotFound
}
func (s *pgLikeShares) ListSharedAtoms(_ context.Context, _ string, _ string, _ int, _, _ string, _ string, _, _ []string) ([]atom_share.Share, string, error) {
	return nil, "", nil
}
func (s *pgLikeShares) AppendEvent(_ context.Context, _ *atom_share.ShareEvent) error { return nil }
func (s *pgLikeShares) ListEvents(_ context.Context, _ string) ([]atom_share.ShareEvent, error) {
	return nil, nil
}

// pgLikeBookmarks is a BookmarkStore that rejects a non-UUID atom_id with 22P02
// (as pg would) on both Save and Delete; a valid id is a no-op. outage set ⇒
// every call fails (infra fault on a valid id).
type pgLikeBookmarks struct{ outage error }

func (b *pgLikeBookmarks) Save(_ context.Context, bm *bookmark.Bookmark) error {
	if b.outage != nil {
		return b.outage
	}
	if _, err := uuid.Parse(bm.AtomID); err != nil {
		return pgUUIDErr(bm.AtomID)
	}
	return nil
}
func (b *pgLikeBookmarks) Delete(_ context.Context, _, _, atomID string) error {
	if b.outage != nil {
		return b.outage
	}
	if _, err := uuid.Parse(atomID); err != nil {
		return pgUUIDErr(atomID)
	}
	return nil
}
func (b *pgLikeBookmarks) ListByOwner(_ context.Context, _, _, _ string, _ int) ([]bookmark.Bookmark, string, error) {
	return nil, "", nil
}

const (
	testTenant  = "11111111-1111-7111-8111-111111111111"
	testGCID    = "22222222-2222-7222-8222-222222222222"
	validAtomID = "01890a5d-ac96-774b-bcce-b302099a8057" // well-formed UUIDv7
	malformedID = "banana"                               // a client typo — not a UUID
)

// withIdentity stamps the gateway identity + optional idempotency key that the
// /v1 routes require, so the request reaches the handler body under test rather
// than being turned away by requireIdentity.
func withIdentity(req *http.Request, idemKey string) *http.Request {
	req.Header.Set("gcid", testGCID)
	req.Header.Set("X-Tenant-Id", testTenant)
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	return req
}

// ---------------------------------------------------------------------------
// pathUUID contract — the boundary helper itself, all branches. An empty path
// segment cannot occur through the mux (a {wildcard} matches only a non-empty
// segment), so its branch is exercised here directly rather than via a route.
// ---------------------------------------------------------------------------

func TestPathUUID_Contract(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		raw        string
		wantOK     bool
		wantStatus int
		wantMsgSub string
	}{
		{"empty", "", false, http.StatusBadRequest, "path segment required"},
		{"whitespace_only", "   ", false, http.StatusBadRequest, "path segment required"},
		{"malformed", malformedID, false, http.StatusBadRequest, "must be a valid UUID"},
		{"valid", validAtomID, true, http.StatusOK, ""},
		{"valid_padded", "  " + validAtomID + "  ", true, http.StatusOK, ""},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.SetPathValue("atom_id", tc.raw)
			rr := httptest.NewRecorder()

			got, ok := pathUUID(rr, req, "atom_id")

			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (body: %s)", ok, tc.wantOK, rr.Body.String())
			}
			if ok {
				// A valid id is returned trimmed and the writer is left untouched.
				if got != strings.TrimSpace(tc.raw) {
					t.Errorf("returned id = %q, want trimmed %q", got, strings.TrimSpace(tc.raw))
				}
				return
			}
			if rr.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d", rr.Code, tc.wantStatus)
			}
			if !strings.Contains(rr.Body.String(), tc.wantMsgSub) {
				t.Errorf("body %q missing %q", rr.Body.String(), tc.wantMsgSub)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// RED: a malformed atom_id must be 400 (a client typo), not 500 (our outage).
// Each request is otherwise fully valid, so the ONLY thing wrong is the id —
// which pins the status to the id, not to a missing header/body/key.
// ---------------------------------------------------------------------------

func TestMalformedAtomID_Is400_NotOurFault(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		method  string
		path    string
		body    string
		idemKey string
	}{
		{"shareAtom", http.MethodPost, "/v1/atoms/" + malformedID + "/share", `{"license_terms":"free"}`, "idem-share"},
		{"revokeShare", http.MethodDelete, "/v1/atoms/" + malformedID + "/share", "", ""},
		{"bookmarkAtom", http.MethodPost, "/v1/atoms/" + malformedID + "/bookmark", "", "idem-bm"},
		{"unbookmarkAtom", http.MethodDelete, "/v1/atoms/" + malformedID + "/bookmark", "", ""},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := NewHandler(Deps{
				Shares:      &pgLikeShares{},
				Projections: &pgLikeProjections{},
				Bookmarks:   &pgLikeBookmarks{},
			})
			var reqBody *strings.Reader
			if tc.body != "" {
				reqBody = strings.NewReader(tc.body)
			} else {
				reqBody = strings.NewReader("")
			}
			req := withIdentity(httptest.NewRequest(tc.method, tc.path, reqBody), tc.idemKey)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("malformed atom_id: status = %d, want 400 (body: %s)", rr.Code, rr.Body.String())
			}
			// The message must point the caller at the id, not paste a raw
			// SQLSTATE. "valid UUID" is actionable; "SQLSTATE 22P02" is a leak.
			if !strings.Contains(rr.Body.String(), "valid UUID") {
				t.Errorf("400 body should name the id problem, got: %s", rr.Body.String())
			}
			if strings.Contains(rr.Body.String(), "22P02") {
				t.Errorf("400 body leaked the raw pg error: %s", rr.Body.String())
			}
		})
	}
}

// ---------------------------------------------------------------------------
// NEGATIVE CONTROL 1: a genuine DB fault on a VALID atom_id stays 500.
// This is the guard against over-correction — the fix must NOT wrap the handler
// in a 500->400 catch, or a real outage would go silent (4xx never alerts).
// ---------------------------------------------------------------------------

func TestGenuineDBError_OnValidAtomID_Stays500(t *testing.T) {
	t.Parallel()

	dbErr := pgUUIDErr("(simulated)") // stand-in for any raw driver failure
	// A distinct undefined-table fault, the CHO-2177 shape, for the bookmark leg.
	tableErr := fmt.Errorf(`ERROR: relation "atom_bookmarks" does not exist (SQLSTATE 42P01)`)

	cases := []struct {
		name    string
		method  string
		path    string
		body    string
		idemKey string
		deps    Deps
	}{
		{
			"shareAtom", http.MethodPost, "/v1/atoms/" + validAtomID + "/share", `{"license_terms":"free"}`, "idem-share",
			Deps{Shares: &pgLikeShares{}, Projections: &pgLikeProjections{outage: dbErr}},
		},
		{
			"revokeShare", http.MethodDelete, "/v1/atoms/" + validAtomID + "/share", "", "",
			Deps{Shares: &pgLikeShares{outage: dbErr}},
		},
		{
			"bookmarkAtom", http.MethodPost, "/v1/atoms/" + validAtomID + "/bookmark", "", "idem-bm",
			Deps{Bookmarks: &failingBookmarks{err: tableErr}},
		},
		{
			"unbookmarkAtom", http.MethodDelete, "/v1/atoms/" + validAtomID + "/bookmark", "", "",
			Deps{Bookmarks: &failingBookmarks{err: tableErr}},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := NewHandler(tc.deps)
			req := withIdentity(httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)), tc.idemKey)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)

			if rr.Code != http.StatusInternalServerError {
				t.Fatalf("genuine DB fault on valid id: status = %d, want 500 (body: %s)", rr.Code, rr.Body.String())
			}
		})
	}
}

// ---------------------------------------------------------------------------
// NEGATIVE CONTROL 2: a VALID-but-absent atom_id keeps its domain status, never
// 400. A well-formed id we could not find is not a malformed request — turning
// it into 400 would be the mirror error (claiming the caller is wrong when we
// simply have no such row).
// ---------------------------------------------------------------------------

func TestValidButAbsentAtomID_KeepsDomainStatus(t *testing.T) {
	t.Parallel()

	t.Run("shareAtom_absent_is_412", func(t *testing.T) {
		t.Parallel()
		h := NewHandler(Deps{Shares: &pgLikeShares{}, Projections: &pgLikeProjections{}})
		req := withIdentity(
			httptest.NewRequest(http.MethodPost, "/v1/atoms/"+validAtomID+"/share", strings.NewReader(`{"license_terms":"free"}`)),
			"idem-share-absent",
		)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusPreconditionFailed {
			t.Fatalf("absent atom (share): status = %d, want 412 (body: %s)", rr.Code, rr.Body.String())
		}
	})

	t.Run("revokeShare_absent_is_204", func(t *testing.T) {
		t.Parallel()
		h := NewHandler(Deps{Shares: &pgLikeShares{}})
		req := withIdentity(
			httptest.NewRequest(http.MethodDelete, "/v1/atoms/"+validAtomID+"/share", nil),
			"",
		)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusNoContent {
			t.Fatalf("absent share (revoke): status = %d, want 204 (body: %s)", rr.Code, rr.Body.String())
		}
	})
}
