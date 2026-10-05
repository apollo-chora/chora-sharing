// sharing_server_ws2_test.go — ADR-229 WS-2 (CHO-2133): audience-based
// AuthorizeAtomUse.
//
// The shipped RPC (spec-001 §7.3) supported exactly two license resolutions:
// own-atom (free) and feed-share-sourced (source_share_entry required — 412
// otherwise). ADR-229 D2 retains AtomUsageGrant as the AUDIT RECORD of
// reuse, written idempotently at snapshot time — and a tenant-visible atom
// need never have been promoted to the feed (D2: ShareAtom stays a separate
// act). WS-2 therefore adds the third resolution: non-owner + NO source
// share is allowed IFF the projection's reuse_visibility='tenant' (the
// audience IS the consent), minting the v1 free license. Anything else
// stays FAILED_PRECONDITION — AuthorizeAtomUse must never become an open
// grant-minting endpoint.
package grpcadapter

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	sharingv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/sharing/v1"

	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
	"github.com/apollo-chora/chora-sharing/internal/domain/grant"
)

const (
	ws2Owner  = "01970000-0000-7000-9000-00000000aaa1"
	ws2Caller = "01970000-0000-7000-9000-00000000bbb2"
	ws2Atom   = "01970000-0000-7000-8000-00000000cc03"
	ws2Tenant = "01970000-0000-7000-8000-00000000dd04"
)

func ws2Server(reuseVisibility string) (*SharingServer, *fakeGrantRepo) {
	projections := newFakeProjectionRepo()
	projections.seed(atom_projection.Projection{
		AtomID:          ws2Atom,
		RevisionID:      "rev-ws2-1",
		OwnerGCID:       ws2Owner,
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

func TestAuthorizeAtomUse_AudienceTenant_NoSourceShare_MintsFreeGrant(t *testing.T) {
	t.Parallel()
	srv, _ := ws2Server("tenant")

	resp, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:    ws2Caller,
		TenantId:       ws2Tenant,
		AtomId:         ws2Atom,
		Scope:          sharingv1.GrantScope_GRANT_SCOPE_TEST_SET,
		IdempotencyKey: "adr229-snapshot:t:g:a",
		// NO source_share_entry — audience-based (ADR-229 D2).
	})
	if err != nil {
		t.Fatalf("AuthorizeAtomUse (audience tenant): %v", err)
	}
	if resp.GetLicenseTermsSnapshot() != sharingv1.LicenseTerms_LICENSE_TERMS_FREE {
		t.Errorf("license = %v; want FREE (v1 reuse is free-license only)", resp.GetLicenseTermsSnapshot())
	}
	if resp.GetOwnerGcid() != ws2Owner {
		t.Errorf("owner = %q; want %q (R1 attribution)", resp.GetOwnerGcid(), ws2Owner)
	}
	if resp.GetStatus() != sharingv1.GrantStatus_GRANT_STATUS_ACTIVE {
		t.Errorf("status = %v; want ACTIVE", resp.GetStatus())
	}
}

func TestAuthorizeAtomUse_AudienceTenant_Idempotent(t *testing.T) {
	t.Parallel()
	srv, _ := ws2Server("tenant")

	req := &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:    ws2Caller,
		TenantId:       ws2Tenant,
		AtomId:         ws2Atom,
		Scope:          sharingv1.GrantScope_GRANT_SCOPE_TEST_SET,
		IdempotencyKey: "adr229-snapshot:t:g:a",
	}
	first, err := srv.AuthorizeAtomUse(context.Background(), req)
	if err != nil {
		t.Fatalf("first AuthorizeAtomUse: %v", err)
	}
	second, err := srv.AuthorizeAtomUse(context.Background(), req)
	if err != nil {
		t.Fatalf("second AuthorizeAtomUse: %v", err)
	}
	if first.GetGrantId() != second.GetGrantId() {
		t.Errorf("re-authorize minted a new grant (%q then %q); want the same active grant", first.GetGrantId(), second.GetGrantId())
	}
}

func TestAuthorizeAtomUse_AudiencePrivate_NoSourceShare_412(t *testing.T) {
	t.Parallel()
	srv, _ := ws2Server("private")

	_, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:    ws2Caller,
		TenantId:       ws2Tenant,
		AtomId:         ws2Atom,
		Scope:          sharingv1.GrantScope_GRANT_SCOPE_TEST_SET,
		IdempotencyKey: "adr229-snapshot:t:g:a",
	})
	statusIs(t, err, codes.FailedPrecondition)
}

func TestAuthorizeAtomUse_AudienceEmpty_NoSourceShare_412(t *testing.T) {
	t.Parallel()
	// A projection row whose reuse_visibility never arrived (pre-ADR-229
	// producer + no backfill) must be treated as private — fail closed.
	srv, _ := ws2Server("")

	_, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:    ws2Caller,
		TenantId:       ws2Tenant,
		AtomId:         ws2Atom,
		Scope:          sharingv1.GrantScope_GRANT_SCOPE_TEST_SET,
		IdempotencyKey: "adr229-snapshot:t:g:a",
	})
	statusIs(t, err, codes.FailedPrecondition)
}

func TestAuthorizeAtomUse_SourceShare_PathUnchanged(t *testing.T) {
	t.Parallel()
	// Regression guard: a private atom WITH a source share entry still
	// resolves via the shipped share-license path (Shares dep required).
	srv, _ := ws2Server("private")

	_, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:      ws2Caller,
		TenantId:         ws2Tenant,
		AtomId:           ws2Atom,
		Scope:            sharingv1.GrantScope_GRANT_SCOPE_TEST_SET,
		IdempotencyKey:   "idem-share-1",
		SourceShareEntry: "share-entry-1",
	})
	// Shares dep is not wired in this fixture — the shipped path must still
	// be taken (Unimplemented notWired), NOT the audience path.
	statusIs(t, err, codes.Unimplemented)
}

// -----------------------------------------------------------------------------
// Live-walk regression (2026-07-11 deploy catch): AuthorizeAtomUse ran its
// projection lookup + grant persist on the RAW request ctx — no
// tracing.WithTenantID — so the pg stores' rls.ApplySession failed loud
// ("rls: tenant_id missing on context") the FIRST time the RPC ever fired
// (zero callers until the WS-2 snapshot gate). The in-mem fakes never noticed.
// These asserts pin the tenant-stamped ctx contract for BOTH stores.
// -----------------------------------------------------------------------------

type tenantCaptureProjections struct {
	atom_projection.AtomProjectionReader
	tenants []string
}

func (c *tenantCaptureProjections) Get(ctx context.Context, atomID string) (atom_projection.Projection, error) {
	c.tenants = append(c.tenants, tracing.TenantIDFromContext(ctx))
	return c.AtomProjectionReader.Get(ctx, atomID)
}

type tenantCaptureGrants struct {
	*fakeGrantRepo
	tenants      []string
	grantTenants []string
}

func (c *tenantCaptureGrants) Authorize(ctx context.Context, g *grant.AtomUsageGrant) (*grant.AtomUsageGrant, error) {
	c.tenants = append(c.tenants, tracing.TenantIDFromContext(ctx))
	c.grantTenants = append(c.grantTenants, g.TenantID)
	return c.fakeGrantRepo.Authorize(ctx, g)
}

func TestAuthorizeAtomUse_StampsTenantOnContext(t *testing.T) {
	t.Parallel()
	projections := newFakeProjectionRepo()
	projections.seed(atom_projection.Projection{
		AtomID:          ws2Atom,
		RevisionID:      "rev-ws2-1",
		OwnerGCID:       ws2Owner,
		PublishedAt:     time.Now().UTC(),
		ReuseVisibility: "tenant",
	})
	capProj := &tenantCaptureProjections{AtomProjectionReader: projections}
	capGrants := &tenantCaptureGrants{fakeGrantRepo: newFakeGrantRepo()}
	srv := New(Deps{
		Grants:      capGrants,
		Projections: capProj,
		Rules:       testRules(),
	})

	_, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:    ws2Caller,
		TenantId:       ws2Tenant,
		AtomId:         ws2Atom,
		Scope:          sharingv1.GrantScope_GRANT_SCOPE_TEST_SET,
		IdempotencyKey: "adr229-snapshot:t:g:a-ctx",
	})
	if err != nil {
		t.Fatalf("AuthorizeAtomUse: %v", err)
	}
	if len(capProj.tenants) == 0 || capProj.tenants[0] != ws2Tenant {
		t.Errorf("Projections.Get ctx tenant = %v; want [%q] (rls.ApplySession needs it — live 502 without)", capProj.tenants, ws2Tenant)
	}
	if len(capGrants.tenants) == 0 || capGrants.tenants[0] != ws2Tenant {
		t.Errorf("Grants.Authorize ctx tenant = %v; want [%q]", capGrants.tenants, ws2Tenant)
	}
	// The grant ROW must carry the tenant too — tenant_id is a NOT NULL uuid
	// column; an unset "" binds as invalid uuid (live 22P02, walk catch #3).
	if len(capGrants.grantTenants) == 0 || capGrants.grantTenants[0] != ws2Tenant {
		t.Errorf("grant.TenantID = %v; want [%q] (NewGrant carries no tenant; the handler must stamp it)", capGrants.grantTenants, ws2Tenant)
	}
}
