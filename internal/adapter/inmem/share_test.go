// share_test.go — exercises the in-memory ShareRepo adapter: immutable
// save/get by feed entry id, newest-visible lookup by atom+owner, the
// keyset-paginated shared-atom feed with all filters (tenant / following /
// blocked / topic / question-type), and derived-status read-side semantics.
package inmem_test

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
)

func mustShare(t *testing.T, tenant, atom, owner, feedEntryID, questionType string, createdAt time.Time, license atom_share.LicenseTerms) *atom_share.Share {
	t.Helper()
	s, err := atom_share.NewShare(tenant, atom, "rev-"+atom, owner, "display-"+owner, "stem-preview", questionType, nil, "", license, atom_share.RoyaltyRate{Kind: "fixed_per_use", Value: 1})
	if err != nil {
		t.Fatalf("NewShare: %v", err)
	}
	s.FeedEntryID = feedEntryID
	s.CreatedAt = createdAt
	return s
}

func TestShareRepo_SaveGet(t *testing.T) {
	t.Parallel()
	repo := inmem.NewShareRepo()
	ctx := context.Background()

	s := mustShare(t, tenantA, "atom-1", gcidA, "feed-1", "mcq", time.Now().UTC(), atom_share.LicenseFree)
	if err := repo.SaveShare(ctx, s); err != nil {
		t.Fatalf("SaveShare: %v", err)
	}
	// Idempotent on FeedEntryID — save again (upsert, no error).
	if err := repo.SaveShare(ctx, s); err != nil {
		t.Fatalf("SaveShare replay: %v", err)
	}
	got, err := repo.GetShare(ctx, "feed-1")
	if err != nil {
		t.Fatalf("GetShare: %v", err)
	}
	if got.AtomID != "atom-1" {
		t.Fatalf("GetShare: atom mismatch")
	}
	if _, err := repo.GetShare(ctx, "missing"); !errors.Is(err, atom_share.ErrNotFound) {
		t.Fatalf("GetShare missing: expected ErrNotFound, got %v", err)
	}
}

func TestShareRepo_GetShare_DerivedStatus(t *testing.T) {
	t.Parallel()
	repo := inmem.NewShareRepo()
	ctx := context.Background()
	now := time.Now().UTC()

	hidden := mustShare(t, tenantA, "atom-h", gcidA, "feed-h", "mcq", now, atom_share.LicenseFree)
	revoked := mustShare(t, tenantA, "atom-r", gcidA, "feed-r", "mcq", now, atom_share.LicenseFree)
	_ = repo.SaveShare(ctx, hidden)
	_ = repo.SaveShare(ctx, revoked)
	_ = repo.AppendEvent(ctx, &atom_share.ShareEvent{FeedEntryID: "feed-h", ActorGCID: gcidA, Type: atom_share.EventHidden, CreatedAt: now})
	_ = repo.AppendEvent(ctx, &atom_share.ShareEvent{FeedEntryID: "feed-r", ActorGCID: gcidA, Type: atom_share.EventRevoked, CreatedAt: now})

	// Hidden-by-author is still readable (only revoked is excluded).
	if _, err := repo.GetShare(ctx, "feed-h"); err != nil {
		t.Fatalf("hidden share must be readable; got %v", err)
	}
	// Revoked is not readable.
	if _, err := repo.GetShare(ctx, "feed-r"); !errors.Is(err, atom_share.ErrNotFound) {
		t.Fatalf("revoked share: expected ErrNotFound, got %v", err)
	}
}

func TestShareRepo_GetShareByAtom(t *testing.T) {
	t.Parallel()
	repo := inmem.NewShareRepo()
	ctx := context.Background()
	now := time.Now().UTC()

	// Never shared → not found.
	if _, err := repo.GetShareByAtom(ctx, "atom-x", gcidA); !errors.Is(err, atom_share.ErrNotFound) {
		t.Fatalf("never shared: expected ErrNotFound, got %v", err)
	}

	old := mustShare(t, tenantA, "atom-x", gcidA, "feed-old", "mcq", now.Add(-time.Hour), atom_share.LicenseFree)
	new := mustShare(t, tenantA, "atom-x", gcidA, "feed-new", "mcq", now, atom_share.LicenseFree)
	wrongAtom := mustShare(t, tenantA, "atom-y", gcidA, "feed-other", "mcq", now, atom_share.LicenseFree)
	wrongOwner := mustShare(t, tenantA, "atom-x", "someone-else", "feed-owner", "mcq", now, atom_share.LicenseFree)
	revoked := mustShare(t, tenantA, "atom-x", gcidA, "feed-rev", "mcq", now.Add(2*time.Hour), atom_share.LicenseFree)
	_ = repo.SaveShare(ctx, old)
	_ = repo.SaveShare(ctx, new)
	_ = repo.SaveShare(ctx, wrongAtom)
	_ = repo.SaveShare(ctx, wrongOwner)
	_ = repo.SaveShare(ctx, revoked)
	_ = repo.AppendEvent(ctx, &atom_share.ShareEvent{FeedEntryID: "feed-rev", ActorGCID: gcidA, Type: atom_share.EventRevoked, CreatedAt: now})

	// Newest visible share wins; wrong atom / wrong owner / revoked are skipped.
	best, err := repo.GetShareByAtom(ctx, "atom-x", gcidA)
	if err != nil {
		t.Fatalf("GetShareByAtom: %v", err)
	}
	if best.FeedEntryID != "feed-new" {
		t.Fatalf("expected newest visible share feed-new, got %q", best.FeedEntryID)
	}

	// Only revoked for this atom+owner → not found.
	if _, err := repo.GetShareByAtom(ctx, "atom-z", gcidA); !errors.Is(err, atom_share.ErrNotFound) {
		t.Fatalf("atom-z never shared: expected ErrNotFound, got %v", err)
	}
}

func TestShareRepo_ListSharedAtoms_Filters(t *testing.T) {
	t.Parallel()
	repo := inmem.NewShareRepo()
	ctx := context.Background()

	base := time.Now().UTC().Add(-time.Hour)
	s1 := mustShare(t, tenantA, "atom-1", "u-1", "f-1", "mcq", base.Add(3*time.Second), atom_share.LicenseFree)
	s2 := mustShare(t, tenantA, "atom-2", "u-2", "f-2", "oe", base.Add(2*time.Second), atom_share.LicenseFree)
	s3 := mustShare(t, tenantA, "atom-3", "u-1", "f-3", "mcq", base.Add(1*time.Second), atom_share.LicenseFree)
	s4 := mustShare(t, tenantB, "atom-4", "u-1", "f-4", "mcq", base, atom_share.LicenseFree)
	sHidden := mustShare(t, tenantA, "atom-5", "u-1", "f-5", "mcq", base.Add(4*time.Second), atom_share.LicenseFree)
	sRevoked := mustShare(t, tenantA, "atom-6", "u-2", "f-6", "mcq", base.Add(-time.Second), atom_share.LicenseFree)
	sTied := mustShare(t, tenantA, "atom-7", "u-3", "f-7", "mcq", base.Add(3*time.Second), atom_share.LicenseFree) // same ts as s1 → tie-break by FeedEntryID

	for _, s := range []*atom_share.Share{s1, s2, s3, s4, sHidden, sRevoked, sTied} {
		if err := repo.SaveShare(ctx, s); err != nil {
			t.Fatalf("SaveShare: %v", err)
		}
	}
	_ = repo.AppendEvent(ctx, &atom_share.ShareEvent{FeedEntryID: "f-5", ActorGCID: "u-1", Type: atom_share.EventHidden, CreatedAt: base})
	_ = repo.AppendEvent(ctx, &atom_share.ShareEvent{FeedEntryID: "f-6", ActorGCID: "u-2", Type: atom_share.EventRevoked, CreatedAt: base})

	// Tenant scope (explicit) + default limit 20: newest-first with
	// tie-break on FeedEntryID desc → f-7, f-1, f-2, f-3 (hidden/revoked
	// and tenant B excluded).
	page, next, err := repo.ListSharedAtoms(ctx, tenantA, "", 0, "", "", "tenant", nil, nil)
	if err != nil {
		t.Fatalf("ListSharedAtoms: %v", err)
	}
	if len(page) != 4 {
		t.Fatalf("tenant feed: want 4 shares, got %d", len(page))
	}
	wantOrder := []string{"f-7", "f-1", "f-2", "f-3"}
	for i, want := range wantOrder {
		if page[i].FeedEntryID != want {
			t.Fatalf("tenant feed order[%d]: want %s, got %s", i, want, page[i].FeedEntryID)
		}
	}
	if next != "" {
		t.Fatalf("4 shares fit on one default-20 page; expected empty next, got %q", next)
	}

	// Default scope ("") behaves like "tenant".
	pageDefault, _, err := repo.ListSharedAtoms(ctx, tenantA, "", 100, "", "", "", nil, nil)
	if err != nil {
		t.Fatalf("ListSharedAtoms default scope: %v", err)
	}
	if len(pageDefault) != 4 {
		t.Fatalf("default scope: want 4, got %d", len(pageDefault))
	}

	// limit > 100 → clamped to 100.
	if _, _, err := repo.ListSharedAtoms(ctx, tenantA, "", 1000, "", "", "tenant", nil, nil); err != nil {
		t.Fatalf("clamped limit: %v", err)
	}

	// Topic filter (case-insensitive "MCQ") → mcq shares only (f-7, f-1, f-3).
	page, _, err = repo.ListSharedAtoms(ctx, tenantA, "", 100, "MCQ", "", "tenant", nil, nil)
	if err != nil {
		t.Fatalf("topic filter: %v", err)
	}
	if len(page) != 3 {
		t.Fatalf("topic mcq: want 3, got %d", len(page))
	}

	// Question-type filter → oe shares only (f-2).
	page, _, err = repo.ListSharedAtoms(ctx, tenantA, "", 100, "", "oe", "tenant", nil, nil)
	if err != nil {
		t.Fatalf("qtype filter: %v", err)
	}
	if len(page) != 1 || page[0].FeedEntryID != "f-2" {
		t.Fatalf("qtype oe: want [f-2], got %+v", page)
	}

	// Following scope → owner in followingGCIDs only (u-2 → f-2).
	page, _, err = repo.ListSharedAtoms(ctx, tenantA, "", 100, "", "", "following", []string{"u-2"}, nil)
	if err != nil {
		t.Fatalf("following scope: %v", err)
	}
	if len(page) != 1 || page[0].FeedEntryID != "f-2" {
		t.Fatalf("following u-2: want [f-2], got %d", len(page))
	}

	// Following scope with no matches.
	page, _, err = repo.ListSharedAtoms(ctx, tenantA, "", 100, "", "", "following", []string{"nobody"}, nil)
	if err != nil {
		t.Fatalf("following nobody: %v", err)
	}
	if len(page) != 0 {
		t.Fatalf("following nobody: want 0, got %d", len(page))
	}

	// Blocked owner → excluded (u-1 blocked → f-7, f-2 remain).
	page, _, err = repo.ListSharedAtoms(ctx, tenantA, "", 100, "", "", "tenant", nil, []string{"u-1"})
	if err != nil {
		t.Fatalf("blocked filter: %v", err)
	}
	if len(page) != 2 {
		t.Fatalf("blocked u-1: want 2, got %d", len(page))
	}
}

func TestShareRepo_ListSharedAtoms_Cursor(t *testing.T) {
	t.Parallel()
	repo := inmem.NewShareRepo()
	ctx := context.Background()

	base := time.Now().UTC().Add(-time.Hour)
	s1 := mustShare(t, tenantA, "atom-1", "u-1", "f-1", "mcq", base.Add(3*time.Second), atom_share.LicenseFree)
	s2 := mustShare(t, tenantA, "atom-2", "u-2", "f-2", "mcq", base.Add(2*time.Second), atom_share.LicenseFree)
	s3 := mustShare(t, tenantA, "atom-3", "u-3", "f-3", "mcq", base.Add(1*time.Second), atom_share.LicenseFree)
	for _, s := range []*atom_share.Share{s1, s2, s3} {
		if err := repo.SaveShare(ctx, s); err != nil {
			t.Fatalf("SaveShare: %v", err)
		}
	}

	// Page 1 (limit 2): [f-1, f-2], cursor = last row (f-2).
	page1, next1, err := repo.ListSharedAtoms(ctx, tenantA, "", 2, "", "", "tenant", nil, nil)
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if len(page1) != 2 || page1[0].FeedEntryID != "f-1" || page1[1].FeedEntryID != "f-2" {
		t.Fatalf("page1: got %+v", page1)
	}
	if next1 == "" {
		t.Fatal("page1: expected next cursor")
	}

	// Page 2: the remaining row, no cursor.
	page2, next2, err := repo.ListSharedAtoms(ctx, tenantA, next1, 2, "", "", "tenant", nil, nil)
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if len(page2) != 1 || page2[0].FeedEntryID != "f-3" {
		t.Fatalf("page2: got %+v", page2)
	}
	if next2 != "" {
		t.Fatalf("page2: expected empty cursor, got %q", next2)
	}

	// Cursor pointing at the last row (f-3) → start >= len → empty page.
	// The cursor format is opaque: base64url("RFC3339Nano|feedEntryID").
	packed := s3.CreatedAt.UTC().Format(time.RFC3339Nano) + "|f-3"
	lastCursor := base64.RawURLEncoding.EncodeToString([]byte(packed))
	page3, next3, err := repo.ListSharedAtoms(ctx, tenantA, lastCursor, 10, "", "", "tenant", nil, nil)
	if err != nil {
		t.Fatalf("post-end page: %v", err)
	}
	if len(page3) != 0 || next3 != "" {
		t.Fatalf("post-end page: got %d rows next=%q", len(page3), next3)
	}

	// Cursor that decodes but matches nothing → treated as first page.
	// "stale-cursor" is valid base64url but contains no "|" separator.
	if _, _, err := repo.ListSharedAtoms(ctx, tenantA, "stale-cursor", 10, "", "", "tenant", nil, nil); err != nil {
		t.Fatalf("stale cursor: %v", err)
	}
	// Cursor that decodes to a REAL timestamp but no matching row → the
	// found-scan comes up empty and the page resets to the first page.
	ghostTS := base.Add(9999 * time.Second)
	ghost := base64.RawURLEncoding.EncodeToString([]byte(ghostTS.UTC().Format(time.RFC3339Nano) + "|ghost"))
	page, _, err := repo.ListSharedAtoms(ctx, tenantA, ghost, 10, "", "", "tenant", nil, nil)
	if err != nil {
		t.Fatalf("ghost cursor: %v", err)
	}
	if len(page) != 3 {
		t.Fatalf("ghost cursor: want first page of 3, got %d", len(page))
	}
	// Malformed cursor string (not base64) → first page.
	page, _, err = repo.ListSharedAtoms(ctx, tenantA, "!!!not-base64!!!", 10, "", "", "tenant", nil, nil)
	if err != nil {
		t.Fatalf("malformed cursor: %v", err)
	}
	if len(page) != 3 {
		t.Fatalf("malformed cursor: want first page of 3, got %d", len(page))
	}

	// Empty tenant → no shares.
	page, _, err = repo.ListSharedAtoms(ctx, "no-such-tenant", "", 10, "", "", "tenant", nil, nil)
	if err != nil {
		t.Fatalf("empty tenant: %v", err)
	}
	if len(page) != 0 {
		t.Fatalf("empty tenant: want 0, got %d", len(page))
	}
}

func TestShareRepo_AppendEvent_ListEvents(t *testing.T) {
	t.Parallel()
	repo := inmem.NewShareRepo()
	ctx := context.Background()
	now := time.Now().UTC()

	// No events → empty.
	evs, err := repo.ListEvents(ctx, "feed-1")
	if err != nil {
		t.Fatalf("ListEvents empty: %v", err)
	}
	if len(evs) != 0 {
		t.Fatalf("empty: want 0, got %d", len(evs))
	}

	// Newest-first ordering; equal timestamps keep a stable tie-break.
	first := &atom_share.ShareEvent{FeedEntryID: "feed-1", ActorGCID: gcidA, Type: atom_share.EventHidden, SourceEventID: "src-1", CreatedAt: now}
	second := &atom_share.ShareEvent{FeedEntryID: "feed-1", ActorGCID: gcidA, Type: atom_share.EventRevoked, SourceEventID: "src-2", CreatedAt: now.Add(-time.Minute)}
	equal := &atom_share.ShareEvent{FeedEntryID: "feed-1", ActorGCID: gcidA, Type: atom_share.EventModerationHidden, CreatedAt: now.Add(-time.Minute)} // same ts as second
	if err := repo.AppendEvent(ctx, first); err != nil {
		t.Fatalf("AppendEvent #1: %v", err)
	}
	if err := repo.AppendEvent(ctx, second); err != nil {
		t.Fatalf("AppendEvent #2: %v", err)
	}
	// Event without SourceEventID → appended without dedup.
	if err := repo.AppendEvent(ctx, equal); err != nil {
		t.Fatalf("AppendEvent #3: %v", err)
	}
	evs, err = repo.ListEvents(ctx, "feed-1")
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(evs) != 3 {
		t.Fatalf("want 3 events, got %d", len(evs))
	}
	if evs[0].Type != atom_share.EventHidden {
		t.Fatalf("newest event must sort first; got %q", evs[0].Type)
	}
}