// grant_extra_test.go — residual GrantRepo surface not covered by
// inmem_idempotency_test.go: Revoke (with terminal-idempotency + missing),
// GetActive not-found, the §7.4 ListEntitled union (own ∪ free ∪ grant),
// and the ADR-229 ActiveGrantAtomIDs reuse-context surface.
package inmem_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
	"github.com/apollo-chora/chora-sharing/internal/domain/grant"
)

func TestGrantRepo_Revoke(t *testing.T) {
	t.Parallel()
	store := newTestStore()
	ctx := context.Background()

	g := mustNewGrant(t, grantOwner, grantGrantee, grantAtom, grantRevis,
		grant.ScopeDuel, atom_share.LicenseFree, atom_share.RoyaltyRate{}, "share-1")
	if _, err := store.Grants.Authorize(ctx, g); err != nil {
		t.Fatalf("Authorize: %v", err)
	}

	// Revoke → active grant removed.
	if err := store.Grants.Revoke(ctx, g.ID, grantOwner, "manual revoke"); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if _, err := store.Grants.GetActive(ctx, grantGrantee, grantAtom, grant.ScopeDuel); !errors.Is(err, grant.ErrGrantNotFound) {
		t.Fatalf("after revoke: expected ErrGrantNotFound, got %v", err)
	}

	// Idempotent: revoking an already-revoked grant is a no-op.
	if err := store.Grants.Revoke(ctx, g.ID, grantOwner, ""); err != nil {
		t.Fatalf("re-Revoke: %v", err)
	}

	// Unknown grant ID → ErrGrantNotFound.
	if err := store.Grants.Revoke(ctx, "no-such-grant", grantOwner, ""); !errors.Is(err, grant.ErrGrantNotFound) {
		t.Fatalf("Revoke missing: expected ErrGrantNotFound, got %v", err)
	}

	// Terminal-by-expiry grants are also no-op revokes.
	expired := mustNewGrant(t, grantOwner, grantGrantee, grantAtom+"2", grantRevis,
		grant.ScopeDuel, atom_share.LicenseFree, atom_share.RoyaltyRate{}, "")
	if _, err := store.Grants.Authorize(ctx, expired); err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if err := expired.Expire(time.Now().UTC()); err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if err := store.Grants.Revoke(ctx, expired.ID, grantOwner, ""); err != nil {
		t.Fatalf("expired-grant revoke must be a no-op; got %v", err)
	}
}

func TestGrantRepo_GetActive_NotFound(t *testing.T) {
	t.Parallel()
	store := newTestStore()
	ctx := context.Background()

	if _, err := store.Grants.GetActive(ctx, grantGrantee, grantAtom, grant.ScopeDuel); !errors.Is(err, grant.ErrGrantNotFound) {
		t.Fatalf("GetActive empty: expected ErrGrantNotFound, got %v", err)
	}
}

func TestGrantRepo_ListEntitled_Union(t *testing.T) {
	t.Parallel()
	store := newTestStore()
	ctx := context.Background()

	// --- Own leg (projection.owner_gcid == grantee) ---
	own := mustProjection("a-own", grantGrantee)
	own.QuestionType = atom_projection.QuestionTypeMCQ
	_ = store.Projections.Upsert(ctx, own)

	ownOE := mustProjection("a-own-oe", grantGrantee)
	ownOE.QuestionType = atom_projection.QuestionTypeOE
	_ = store.Projections.Upsert(ctx, ownOE)

	_ = store.Projections.Upsert(ctx, mustProjection("a-other", grantOwner))

	archived := mustProjection("a-archived", grantGrantee)
	archived.Archived = true
	_ = store.Projections.Upsert(ctx, archived)

	// --- Free leg (visible shares with non-royalty licenses) ---
	freeShare, err := atom_share.NewShare(grantTenant, "a-free", "r-free", "shared-owner",
		"Shared", "stem", "mcq", nil, "", atom_share.LicenseFree, atom_share.RoyaltyRate{})
	if err != nil {
		t.Fatalf("NewShare: %v", err)
	}
	freeShare.FeedEntryID = "f-free"
	_ = store.Shares.SaveShare(ctx, freeShare)

	// A second free share with a different question type (tag-filtered leg).
	freeShareOE, err := atom_share.NewShare(grantTenant, "a-free-oe", "r-free-oe", "shared-owner",
		"Shared", "stem", "oe", nil, "", atom_share.LicenseCCBySA, atom_share.RoyaltyRate{})
	if err != nil {
		t.Fatalf("NewShare: %v", err)
	}
	freeShareOE.FeedEntryID = "f-free-oe"
	_ = store.Shares.SaveShare(ctx, freeShareOE)

	royaltyShare, err := atom_share.NewShare(grantTenant, "a-royalty", "r-royalty", "shared-owner",
		"Shared", "stem", "mcq", nil, "", atom_share.LicenseRoyaltyFixed, atom_share.RoyaltyRate{Kind: "fixed_per_use", Value: 1})
	if err != nil {
		t.Fatalf("NewShare: %v", err)
	}
	royaltyShare.FeedEntryID = "f-royalty"
	_ = store.Shares.SaveShare(ctx, royaltyShare)

	revokedShare, err := atom_share.NewShare(grantTenant, "a-revoked", "r-revoked", "shared-owner",
		"Shared", "stem", "mcq", nil, "", atom_share.LicenseFree, atom_share.RoyaltyRate{})
	if err != nil {
		t.Fatalf("NewShare: %v", err)
	}
	revokedShare.FeedEntryID = "f-revoked"
	_ = store.Shares.SaveShare(ctx, revokedShare)
	_ = store.Shares.AppendEvent(ctx, &atom_share.ShareEvent{
		FeedEntryID: "f-revoked", ActorGCID: "shared-owner", Type: atom_share.EventRevoked, CreatedAt: time.Now().UTC(),
	})

	// --- Grant leg ---
	gGrant := mustNewGrant(t, grantOwner, grantGrantee, "a-grant", "r-grant",
		grant.ScopeDuel, atom_share.LicenseFree, atom_share.RoyaltyRate{}, "")
	if _, err := store.Grants.Authorize(ctx, gGrant); err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	// Wrong grantee → not in the union.
	otherGrant := mustNewGrant(t, grantOwner, "other-grantee", "a-other-grant", "r-og",
		grant.ScopeDuel, atom_share.LicenseFree, atom_share.RoyaltyRate{}, "")
	if _, err := store.Grants.Authorize(ctx, otherGrant); err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	// Scope not covering the requested scope → not in the union.
	tsGrant := mustNewGrant(t, grantOwner, grantGrantee, "a-testset", "r-ts",
		grant.ScopeTestSet, atom_share.LicenseFree, atom_share.RoyaltyRate{}, "")
	if _, err := store.Grants.Authorize(ctx, tsGrant); err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	// Own + grant overlap on one atom → both flags set.
	_ = store.Projections.Upsert(ctx, mustProjection("a-dual", grantGrantee))
	dualGrant := mustNewGrant(t, grantOwner, grantGrantee, "a-dual", "r-dual",
		grant.ScopeDuel, atom_share.LicenseFree, atom_share.RoyaltyRate{}, "")
	if _, err := store.Grants.Authorize(ctx, dualGrant); err != nil {
		t.Fatalf("Authorize: %v", err)
	}

	out, err := store.Grants.ListEntitled(ctx, grantGrantee, grant.ScopeDuel, nil, 0)
	if err != nil {
		t.Fatalf("ListEntitled: %v", err)
	}
	// Expected: a-own, a-own-oe, a-free, a-free-oe, a-grant, a-dual.
	if len(out) != 6 {
		t.Fatalf("union: want 6 atoms, got %d: %+v", len(out), out)
	}
	byAtom := make(map[string]grant.EntitledAtom, len(out))
	for _, e := range out {
		byAtom[e.AtomID] = e
	}
	if !byAtom["a-own"].IsOwn || byAtom["a-own"].HasGrant {
		t.Fatalf("a-own flags: %+v", byAtom["a-own"])
	}
	if byAtom["a-free"].IsOwn || byAtom["a-free"].AuthorGCID != "shared-owner" {
		t.Fatalf("a-free flags: %+v", byAtom["a-free"])
	}
	if byAtom["a-grant"].IsOwn || !byAtom["a-grant"].HasGrant {
		t.Fatalf("a-grant flags: %+v", byAtom["a-grant"])
	}
	if !byAtom["a-dual"].IsOwn || !byAtom["a-dual"].HasGrant {
		t.Fatalf("a-dual must set both flags: %+v", byAtom["a-dual"])
	}
	if _, ok := byAtom["a-royalty"]; ok {
		t.Fatal("royalty share must not appear in the free leg")
	}
	if _, ok := byAtom["a-other"]; ok {
		t.Fatal("foreign projection leaked into the union")
	}
	if _, ok := byAtom["a-archived"]; ok {
		t.Fatal("archived projection leaked into the union")
	}
	if _, ok := byAtom["a-testset"]; ok {
		t.Fatal("non-covering grant leaked into the union")
	}

	// Tag filter (case-insensitive): own leg + free leg only keep mcq rows;
	// grant-leg atoms are not tag-filtered.
	outFiltered, err := store.Grants.ListEntitled(ctx, grantGrantee, grant.ScopeDuel, []string{"MCQ"}, 100)
	if err != nil {
		t.Fatalf("ListEntitled tagged: %v", err)
	}
	kept := make(map[string]bool, len(outFiltered))
	for _, e := range outFiltered {
		kept[e.AtomID] = true
	}
	// a-own (mcq) kept; a-own-oe (oe) and a-free-oe (oe) dropped;
	// a-free kept; a-grant + a-dual kept.
	for _, want := range []string{"a-own", "a-free", "a-grant", "a-dual"} {
		if !kept[want] {
			t.Fatalf("tagged union: %s must be kept", want)
		}
	}
	if kept["a-own-oe"] {
		t.Fatal("tagged union: a-own-oe must be dropped by the oe tag")
	}
	if kept["a-free-oe"] {
		t.Fatal("tagged union: a-free-oe must be dropped by the oe tag")
	}

	// Limit truncates; limit > 500 clamps without error.
	outLimited, err := store.Grants.ListEntitled(ctx, grantGrantee, grant.ScopeDuel, nil, 1)
	if err != nil {
		t.Fatalf("ListEntitled limit: %v", err)
	}
	if len(outLimited) != 1 {
		t.Fatalf("limit 1: want 1, got %d", len(outLimited))
	}
	outHuge, err := store.Grants.ListEntitled(ctx, grantGrantee, grant.ScopeDuel, nil, 1000)
	if err != nil || len(outHuge) != 6 {
		t.Fatalf("limit 1000: want 6, got %d err=%v", len(outHuge), err)
	}
}

func TestGrantRepo_ActiveGrantAtomIDs(t *testing.T) {
	t.Parallel()
	store := newTestStore()
	ctx := context.Background()

	active := mustNewGrant(t, grantOwner, grantGrantee, "a-live", "r-live",
		grant.ScopeDuel, atom_share.LicenseFree, atom_share.RoyaltyRate{}, "")
	if _, err := store.Grants.Authorize(ctx, active); err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	// Same atom under a second scope → distinct grant IDs, deduped atoms.
	dup := mustNewGrant(t, grantOwner, grantGrantee, "a-live", "r-live",
		grant.ScopeLiveQuiz, atom_share.LicenseFree, atom_share.RoyaltyRate{}, "")
	if _, err := store.Grants.Authorize(ctx, dup); err != nil {
		t.Fatalf("Authorize: %v", err)
	}

	past := time.Now().UTC().Add(-24 * time.Hour)
	expired := mustNewGrant(t, grantOwner, grantGrantee, "a-expired", "r-exp",
		grant.ScopeDuel, atom_share.LicenseFree, atom_share.RoyaltyRate{}, "")
	expired.ExpiresAt = &past
	if _, err := store.Grants.Authorize(ctx, expired); err != nil {
		t.Fatalf("Authorize: %v", err)
	}

	otherGrantee := mustNewGrant(t, grantOwner, "other-grantee", "a-other", "r-other",
		grant.ScopeDuel, atom_share.LicenseFree, atom_share.RoyaltyRate{}, "")
	if _, err := store.Grants.Authorize(ctx, otherGrantee); err != nil {
		t.Fatalf("Authorize: %v", err)
	}

	ids, err := store.Grants.ActiveGrantAtomIDs(ctx, grantGrantee)
	if err != nil {
		t.Fatalf("ActiveGrantAtomIDs: %v", err)
	}
	if len(ids) != 1 || ids[0] != "a-live" {
		t.Fatalf("want [a-live], got %v", ids)
	}

	// A grantee with no grants → empty.
	none, err := store.Grants.ActiveGrantAtomIDs(ctx, "no-grants-grantee")
	if err != nil {
		t.Fatalf("ActiveGrantAtomIDs empty: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("empty: want 0, got %d", len(none))
	}
}

// TestStore_NewStore_Wiring verifies the composite NewStore() wires every
// domain port and that GrantRepo.ListEntitled cross-repo wiring (projection
// + share + grant) works through the composed store.
func TestStore_NewStore_Wiring(t *testing.T) {
	t.Parallel()
	store := inmem.NewStore()
	if store.Posts == nil || store.Shares == nil || store.Grants == nil ||
		store.Royalties == nil || store.Currency == nil || store.Projections == nil ||
		store.Leaderboards == nil || store.Comments == nil || store.Reactions == nil ||
		store.Graph == nil {
		t.Fatal("NewStore must wire every field")
	}

	// The cross-wire: projection (own) + share (free) both feed ListEntitled.
	ctx := context.Background()
	_ = store.Projections.Upsert(ctx, mustProjection("wired-own", grantGrantee))
	sh, err := atom_share.NewShare(grantTenant, "wired-free", "r-free", "wire-owner",
		"Wire", strings.Repeat("x", 200), "mcq", nil, "", atom_share.LicenseFree, atom_share.RoyaltyRate{})
	if err != nil {
		t.Fatalf("NewShare: %v", err)
	}
	sh.FeedEntryID = "f-wired"
	_ = store.Shares.SaveShare(ctx, sh)

	out, err := store.Grants.ListEntitled(ctx, grantGrantee, grant.ScopeDuel, nil, 50)
	if err != nil {
		t.Fatalf("ListEntitled via store: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("store wiring: want 2 entitled atoms, got %d", len(out))
	}
}