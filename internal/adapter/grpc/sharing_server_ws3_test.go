// sharing_server_ws3_test.go — ADR-229 WS-3 (CHO-2141): arm-time usable gate
// for LiveQuiz.
//
// The shipped arm gate (spec-001 §7.8) resolved usability as
// own ∪ share ∪ grant, but the `share` leg was a hardcoded no-op
// (hasShare := false) — so a tenant-visible published atom that the
// instructor had no explicit grant for was wrongly REFUSED at ARM, diverging
// from the WS-2 picker (AuthorizeAtomUse), whose disjunct is
// own ∪ tenant-visible ∪ granted. WS-3 folds the tenant-visibility leg into
// the pure gate so AuthorizeLiveQuizAtoms mirrors WS-2 leg-for-leg:
// reuse_visibility='tenant' (published — the projection only resolves when the
// atom is published + not archived) is the consent, the grant leg still
// confers permission + freezes the license snapshot, and everything else stays
// unusable (offender named, SC-009).
package grpcadapter

import (
	"context"
	"testing"
	"time"

	sharingv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/sharing/v1"

	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
	"github.com/apollo-chora/chora-sharing/internal/domain/grant"
)

const (
	ws3Owner      = "01970000-0000-7000-9000-0000000ca001"
	ws3Instructor = "01970000-0000-7000-9000-0000000ca002" // NOT the owner
	ws3Atom       = "01970000-0000-7000-8000-0000000ca003"
	ws3Rev        = "01970000-0000-7000-8000-0000000ca0ab"
	ws3Tenant     = "01970000-0000-7000-8000-0000000ca004"
)

// ws3Server wires a SharingServer with a single seeded projection at the given
// reuse_visibility + owner, plus an empty grant repo (seed grants via the
// returned handle).
func ws3Server(t *testing.T, ownerGCID, reuseVisibility string) (*SharingServer, *fakeGrantRepo) {
	t.Helper()
	projections := newFakeProjectionRepo()
	projections.seed(atom_projection.Projection{
		AtomID:          ws3Atom,
		RevisionID:      ws3Rev,
		OwnerGCID:       ownerGCID,
		PublishedAt:     time.Now().UTC(),
		ReuseVisibility: reuseVisibility,
	})
	grants := newFakeGrantRepo()
	return New(Deps{
		Grants:      grants,
		Projections: projections,
		Rules:       testRules(),
	}), grants
}

func armLiveQuiz(t *testing.T, srv *SharingServer) *sharingv1.AuthorizeLiveQuizAtomsResponse {
	t.Helper()
	resp, err := srv.AuthorizeLiveQuizAtoms(context.Background(), &sharingv1.AuthorizeLiveQuizAtomsRequest{
		InstructorGcid: ws3Instructor,
		TenantId:       ws3Tenant,
		AtomIds:        []string{ws3Atom},
		IdempotencyKey: "arm-ws3-1",
	})
	if err != nil {
		t.Fatalf("AuthorizeLiveQuizAtoms: %v", err)
	}
	if len(resp.GetAtoms()) != 1 {
		t.Fatalf("expected 1 atom result, got %d", len(resp.GetAtoms()))
	}
	return resp
}

// The core WS-3 regression: a tenant-visible published atom the instructor
// does NOT own and has NO grant for must be AUTHORIZED (the audience is the
// consent — mirrors WS-2 AuthorizeAtomUse).
func TestAuthorizeLiveQuizAtoms_TenantVisible_NoGrant_Authorized(t *testing.T) {
	t.Parallel()
	srv, _ := ws3Server(t, ws3Owner, "tenant")

	resp := armLiveQuiz(t, srv)
	if !resp.GetAllAuthorized() {
		t.Errorf("all_authorized=false; want true (tenant-visible published atom is usable without an explicit grant)")
	}
	if !resp.GetAtoms()[0].GetUsable() {
		t.Errorf("atom usable=false (reason %q); want true (reuse_visibility='tenant' is the consent)", resp.GetAtoms()[0].GetReason())
	}
}

// A private atom the instructor does not own + no grant stays UNUSABLE — the
// ARM is blocked loud + the offender is named (SC-009).
func TestAuthorizeLiveQuizAtoms_Private_NoGrant_Refused(t *testing.T) {
	t.Parallel()
	srv, _ := ws3Server(t, ws3Owner, "private")

	resp := armLiveQuiz(t, srv)
	if resp.GetAllAuthorized() {
		t.Errorf("all_authorized=true; want false (private atom, no grant)")
	}
	atom := resp.GetAtoms()[0]
	if atom.GetUsable() {
		t.Errorf("atom usable=true; want false (private, not own, no grant)")
	}
	if atom.GetReason() == "" {
		t.Errorf("unusable atom must name a reason (SC-009)")
	}
}

// An empty reuse_visibility (pre-ADR-229 producer, no backfill) is treated as
// private — fail closed, exactly like WS-2.
func TestAuthorizeLiveQuizAtoms_AudienceEmpty_Refused(t *testing.T) {
	t.Parallel()
	srv, _ := ws3Server(t, ws3Owner, "")

	resp := armLiveQuiz(t, srv)
	if resp.GetAtoms()[0].GetUsable() {
		t.Errorf("empty reuse_visibility must fail closed (treated as private)")
	}
}

// Own-atom is always usable regardless of visibility (author reuses own atom).
func TestAuthorizeLiveQuizAtoms_OwnAtom_Authorized(t *testing.T) {
	t.Parallel()
	// owner == instructor, and the atom is private — still usable.
	srv, _ := ws3Server(t, ws3Instructor, "private")

	resp := armLiveQuiz(t, srv)
	if !resp.GetAtoms()[0].GetUsable() {
		t.Errorf("own atom must be usable regardless of reuse_visibility")
	}
}

// A private atom the instructor holds an active live_quiz grant for is usable,
// and the grant snapshot is returned (R-20 — existing behavior preserved).
func TestAuthorizeLiveQuizAtoms_Granted_Authorized(t *testing.T) {
	t.Parallel()
	srv, grants := ws3Server(t, ws3Owner, "private")

	g, err := grant.NewGrant(ws3Owner, ws3Instructor, ws3Atom, ws3Rev,
		grant.ScopeLiveQuiz, atom_share.LicenseFree, atom_share.RoyaltyRate{}, "")
	if err != nil {
		t.Fatalf("NewGrant: %v", err)
	}
	g.TenantID = ws3Tenant
	if _, err := grants.Authorize(context.Background(), g); err != nil {
		t.Fatalf("seed grant: %v", err)
	}

	resp := armLiveQuiz(t, srv)
	atom := resp.GetAtoms()[0]
	if !atom.GetUsable() {
		t.Errorf("granted atom must be usable")
	}
	if atom.GetGrantId() != g.ID {
		t.Errorf("grant_id=%q; want %q (snapshot frozen at ARM)", atom.GetGrantId(), g.ID)
	}
}

// ws3CaptureProjections records the ctx tenant seen by Projections.Get.
type ws3CaptureProjections struct {
	atom_projection.AtomProjectionReader
	tenants []string
}

func (c *ws3CaptureProjections) Get(ctx context.Context, atomID string) (atom_projection.Projection, error) {
	c.tenants = append(c.tenants, tracing.TenantIDFromContext(ctx))
	return c.AtomProjectionReader.Get(ctx, atomID)
}

// ws3CaptureGrants records the ctx tenant seen by Grants.GetActive.
type ws3CaptureGrants struct {
	*fakeGrantRepo
	tenants []string
}

func (c *ws3CaptureGrants) GetActive(ctx context.Context, grantee, atomID string, scope grant.Scope) (*grant.AtomUsageGrant, error) {
	c.tenants = append(c.tenants, tracing.TenantIDFromContext(ctx))
	return c.fakeGrantRepo.GetActive(ctx, grantee, atomID, scope)
}

// The arm gate runs its projection + grant reads on the RAW request ctx unless
// the handler stamps tracing.WithTenantID — the pg stores' rls.ApplySession
// fails loud ("tenant_id missing on context") without it (the WS-2 live catch,
// 2026-07-11). Pin the tenant-stamped ctx contract for BOTH reads.
func TestAuthorizeLiveQuizAtoms_StampsTenantOnContext(t *testing.T) {
	t.Parallel()
	projections := newFakeProjectionRepo()
	projections.seed(atom_projection.Projection{
		AtomID:          ws3Atom,
		RevisionID:      ws3Rev,
		OwnerGCID:       ws3Owner, // NOT the instructor → grant read fires too
		PublishedAt:     time.Now().UTC(),
		ReuseVisibility: "tenant",
	})
	capProj := &ws3CaptureProjections{AtomProjectionReader: projections}
	capGrants := &ws3CaptureGrants{fakeGrantRepo: newFakeGrantRepo()}
	srv := New(Deps{
		Grants:      capGrants,
		Projections: capProj,
		Rules:       testRules(),
	})

	_, err := srv.AuthorizeLiveQuizAtoms(context.Background(), &sharingv1.AuthorizeLiveQuizAtomsRequest{
		InstructorGcid: ws3Instructor,
		TenantId:       ws3Tenant,
		AtomIds:        []string{ws3Atom},
		IdempotencyKey: "arm-ws3-ctx",
	})
	if err != nil {
		t.Fatalf("AuthorizeLiveQuizAtoms: %v", err)
	}
	if len(capProj.tenants) == 0 || capProj.tenants[0] != ws3Tenant {
		t.Errorf("Projections.Get ctx tenant = %v; want [%q] (rls.ApplySession needs it)", capProj.tenants, ws3Tenant)
	}
	if len(capGrants.tenants) == 0 || capGrants.tenants[0] != ws3Tenant {
		t.Errorf("Grants.GetActive ctx tenant = %v; want [%q]", capGrants.tenants, ws3Tenant)
	}
}
