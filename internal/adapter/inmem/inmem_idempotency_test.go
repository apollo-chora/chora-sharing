// Package inmem_test exercises the in-memory adapter doubles. These tests
// focus on the idempotency contracts that the spec marks as load-bearing:
//   - GrantRepo.Authorize returns the existing active grant on re-call.
//   - RoyaltyRepo.Record dedups on source_event_id (no double-credit).
//
// The PostRepo tests (tenant filtering, soft-delete, pagination) live in
// inmem_test.go; this file covers the new domain ports.
package inmem_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
	"github.com/apollo-chora/chora-sharing/internal/domain/grant"
)

// shared fixtures for the idempotency tests.
const (
	grantTenant  = "01970000-0000-7000-8000-0000000000aa"
	grantOwner   = "01970000-0000-7000-9000-0000000000a1"
	grantGrantee = "01970000-0000-7000-9000-0000000000a2"
	grantAtom    = "01970000-0000-7000-a000-0000000000a3"
	grantRevis   = "01970000-0000-7000-b000-0000000000a4"
)

func newTestStore() *inmem.Store { return inmem.NewStore() }

func mustNewGrant(t *testing.T, owner, grantee, atom, rev string, scope grant.Scope, license atom_share.LicenseTerms, rate atom_share.RoyaltyRate, srcShare string) *grant.AtomUsageGrant {
	t.Helper()
	g, err := grant.NewGrant(owner, grantee, atom, rev, scope, license, rate, srcShare)
	if err != nil {
		t.Fatalf("grant.NewGrant: %v", err)
	}
	return g
}

// TestGrantRepo_AuthorizeIdempotent_ReturnsExisting verifies the
// Authorize idempotency contract: a re-call for the same (grantee, atom,
// scope) WHERE active returns the existing frozen grant, NOT a duplicate.
// The frozen snapshot from the first call wins (R2 — dispute-proof).
func TestGrantRepo_AuthorizeIdempotent_ReturnsExisting(t *testing.T) {
	t.Parallel()
	store := newTestStore()
	ctx := context.Background()

	rate := atom_share.RoyaltyRate{Kind: "fixed_per_use", Value: 5}
	original := mustNewGrant(t, grantOwner, grantGrantee, grantAtom, grantRevis,
		grant.ScopeDuel, atom_share.LicenseRoyaltyFixed, rate, "share-1")

	got1, err := store.Grants.Authorize(ctx, original)
	if err != nil {
		t.Fatalf("Authorize #1: %v", err)
	}
	if got1.ID != original.ID {
		t.Fatalf("Authorize #1: expected grant %s, got %s", original.ID, got1.ID)
	}

	// Second call with a NEWLY-constructed grant (fresh ID) for the same
	// (grantee, atom, scope). The repo MUST return the original — the frozen
	// snapshot from the first call wins (R2).
	dup := mustNewGrant(t, grantOwner, grantGrantee, grantAtom, grantRevis,
		grant.ScopeDuel, atom_share.LicenseRoyaltyFixed, rate, "share-1")
	if dup.ID == original.ID {
		t.Fatalf("setup: dup grant should have a fresh ID")
	}

	got2, err := store.Grants.Authorize(ctx, dup)
	if err != nil {
		t.Fatalf("Authorize #2: %v", err)
	}
	if got2.ID != original.ID {
		t.Fatalf("Authorize idempotency: expected original grant %s returned, got %s", original.ID, got2.ID)
	}
	if got2.ID == dup.ID {
		t.Fatalf("Authorize idempotency: must NOT persist the duplicate grant %s", dup.ID)
	}

	// The frozen snapshot is the original's, not the dup's.
	got1Snap := got1.LicenseTermsSnapshot
	got2Snap := got2.LicenseTermsSnapshot
	if got1Snap != got2Snap {
		t.Fatalf("Authorize idempotency: frozen license snapshot drifted: %q vs %q", got1Snap, got2Snap)
	}

	// GetActive confirms only one active grant exists for the triple.
	active, err := store.Grants.GetActive(ctx, grantGrantee, grantAtom, grant.ScopeDuel)
	if err != nil {
		t.Fatalf("GetActive: %v", err)
	}
	if active.ID != original.ID {
		t.Fatalf("GetActive: expected the original grant %s, got %s", original.ID, active.ID)
	}
}

// TestGrantRepo_Authorize_DifferentScopesAreIndependent verifies that
// idempotency is scoped to the grant's Scope — authorizing the same atom
// under a different scope creates a distinct active grant.
func TestGrantRepo_Authorize_DifferentScopesAreIndependent(t *testing.T) {
	t.Parallel()
	store := newTestStore()
	ctx := context.Background()

	g1 := mustNewGrant(t, grantOwner, grantGrantee, grantAtom, grantRevis,
		grant.ScopeDuel, atom_share.LicenseFree, atom_share.RoyaltyRate{}, "")
	g2 := mustNewGrant(t, grantOwner, grantGrantee, grantAtom, grantRevis,
		grant.ScopeTestSet, atom_share.LicenseFree, atom_share.RoyaltyRate{}, "")

	if _, err := store.Grants.Authorize(ctx, g1); err != nil {
		t.Fatalf("Authorize duel: %v", err)
	}
	if _, err := store.Grants.Authorize(ctx, g2); err != nil {
		t.Fatalf("Authorize test_set: %v", err)
	}

	if got, err := store.Grants.GetActive(ctx, grantGrantee, grantAtom, grant.ScopeDuel); err != nil || got.ID != g1.ID {
		t.Fatalf("duel scope: expected g1, got %v err=%v", got, err)
	}
	if got, err := store.Grants.GetActive(ctx, grantGrantee, grantAtom, grant.ScopeTestSet); err != nil || got.ID != g2.ID {
		t.Fatalf("test_set scope: expected g2, got %v err=%v", got, err)
	}
}

// TestGrantRepo_GetActive_UnlimitedCoversAnyScope verifies FR-013: an active
// UNLIMITED grant satisfies a reuse request for any target scope.
func TestGrantRepo_GetActive_UnlimitedCoversAnyScope(t *testing.T) {
	t.Parallel()
	store := newTestStore()
	ctx := context.Background()

	ul := mustNewGrant(t, grantOwner, grantGrantee, grantAtom, grantRevis,
		grant.ScopeUnlimited, atom_share.LicenseFree, atom_share.RoyaltyRate{}, "")
	if _, err := store.Grants.Authorize(ctx, ul); err != nil {
		t.Fatalf("Authorize unlimited: %v", err)
	}

	for _, target := range []grant.Scope{grant.ScopeDuel, grant.ScopeTestSet, grant.ScopeLiveQuiz, grant.ScopeCollection} {
		got, err := store.Grants.GetActive(ctx, grantGrantee, grantAtom, target)
		if err != nil {
			t.Fatalf("GetActive(%s) under UNLIMITED: %v", target, err)
		}
		if got.ID != ul.ID {
			t.Fatalf("GetActive(%s): expected unlimited grant, got %s", target, got.ID)
		}
	}
}

// TestRoyaltyRepo_RecordDedup verifies the royalty dedup contract: recording
// a settlement with the same source_event_id twice does NOT double-credit.
// The second Record returns ErrRoyaltyAlreadySettled (caller treats as
// idempotent success); the settlement count stays at one.
func TestRoyaltyRepo_RecordDedup(t *testing.T) {
	t.Parallel()
	store := newTestStore()
	ctx := context.Background()

	const srcEvt = "evt-royalty-001"
	s1 := &grant.RoyaltySettlement{
		ID:              "settle-1",
		GrantID:         "grant-1",
		OwnerGCID:       grantOwner,
		GranteeTenantID: grantTenant,
		AtomID:          grantAtom,
		Amount:          10.0,
		Currency:        "mana",
		UsageContext:     "duel",
		SourceEventID:   srcEvt,
		CreatedAt:       time.Now().UTC(),
	}

	if err := store.Royalties.Record(ctx, s1); err != nil {
		t.Fatalf("Record #1: %v", err)
	}

	// A second settlement with the SAME source_event_id is rejected.
	s2 := &grant.RoyaltySettlement{
		ID:              "settle-2", // different row id, same source_event_id
		GrantID:         "grant-1",
		OwnerGCID:       grantOwner,
		GranteeTenantID: grantTenant,
		AtomID:          grantAtom,
		Amount:          999.0, // would double-credit if not deduped
		Currency:        "mana",
		UsageContext:     "duel",
		SourceEventID:   srcEvt,
		CreatedAt:       time.Now().UTC(),
	}
	err := store.Royalties.Record(ctx, s2)
	if !errors.Is(err, grant.ErrRoyaltyAlreadySettled) {
		t.Fatalf("Record #2: expected ErrRoyaltyAlreadySettled, got %v", err)
	}

	// Exists confirms the dedup key is present exactly once.
	exists, err := store.Royalties.Exists(ctx, srcEvt)
	if err != nil || !exists {
		t.Fatalf("Exists: expected true for deduped source_event_id, got exists=%v err=%v", exists, err)
	}
	exists2, _ := store.Royalties.Exists(ctx, "never-recorded")
	if exists2 {
		t.Fatalf("Exists: expected false for unknown source_event_id")
	}

	// A settlement with a DIFFERENT source_event_id is recorded normally.
	s3 := &grant.RoyaltySettlement{
		ID:              "settle-3",
		GrantID:         "grant-1",
		OwnerGCID:       grantOwner,
		GranteeTenantID: grantTenant,
		AtomID:          grantAtom,
		Amount:          5.0,
		Currency:        "mana",
		UsageContext:     "duel",
		SourceEventID:   "evt-royalty-002",
		CreatedAt:       time.Now().UTC(),
	}
	if err := store.Royalties.Record(ctx, s3); err != nil {
		t.Fatalf("Record #3 (different source_event_id): %v", err)
	}
}

// TestShareRepo_AppendEventIdempotent verifies the ShareEvent append-only
// idempotency: a replay with the same SourceEventID + payload is a no-op,
// and a replay with a different payload returns ErrConflict.
func TestShareRepo_AppendEventIdempotent(t *testing.T) {
	t.Parallel()
	store := newTestStore()
	ctx := context.Background()

	const feedEntry = "feed-entry-001"
	const srcEvt = "src-evt-001"
	evt := &atom_share.ShareEvent{
		FeedEntryID:   feedEntry,
		ActorGCID:     grantOwner,
		Type:          atom_share.EventHidden,
		SourceEventID: srcEvt,
		CreatedAt:     time.Now().UTC(),
	}
	if err := store.Shares.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent #1: %v", err)
	}

	// Replay with identical payload → no-op (no error, no duplicate).
	if err := store.Shares.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent replay: %v", err)
	}
	evts, err := store.Shares.ListEvents(ctx, feedEntry)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(evts) != 1 {
		t.Fatalf("idempotency: expected 1 event after replay, got %d", len(evts))
	}

	// Replay with DIFFERENT payload → ErrConflict.
	conflict := &atom_share.ShareEvent{
		FeedEntryID:   feedEntry,
		ActorGCID:     grantOwner,
		Type:          atom_share.EventRevoked, // different type
		SourceEventID: srcEvt,
		CreatedAt:     time.Now().UTC(),
	}
	err = store.Shares.AppendEvent(ctx, conflict)
	if !errors.Is(err, atom_share.ErrConflict) {
		t.Fatalf("AppendEvent conflict: expected ErrConflict, got %v", err)
	}
}
