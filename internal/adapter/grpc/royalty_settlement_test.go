// royalty_settlement_test.go — tests for the §3.3 royalty settlement wired
// into AuthorizeAtomUse. The settlement fires ONLY for royalty-bearing
// licenses (royalty_pct / royalty_fixed) on the shared-atom path (the
// source_share_entry path). Own-atom + audience-based grants are always free.
//
// Three contracts:
//  1. royalty_pct license settles the correct amount + records + debits + credits + publishes.
//  2. free license (own-atom) skips settlement entirely — no record, no debit, no publish.
//  3. idempotent replay (same idempotency_key) returns ErrRoyaltyAlreadySettled → treated as success, no double-debit.
package grpcadapter

import (
	"context"
	"sync"
	"testing"
	"time"

	sharingv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/sharing/v1"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
	"github.com/apollo-chora/chora-sharing/internal/domain/currency"
)
// --- Fakes ---

// fakeManaDebiter records mana debits for assertion.
type fakeManaDebiter struct {
	mu     sync.Mutex
	debits []debitCall
}

type debitCall struct {
	gcid     string
	tenantID string
	amount   float64
	code     string
}

func (f *fakeManaDebiter) DebitReuser(_ context.Context, gcid, tenantID string, amount float64, code string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.debits = append(f.debits, debitCall{gcid, tenantID, amount, code})
	return nil
}

// fakeRoyaltyPublisher records published royalty.settled.v1 events.
type fakeRoyaltyPublisher struct {
	mu    sync.Mutex
	pubs  []royaltyPub
}
type royaltyPub struct {
	settlementID  string
	grantID       string
	ownerGCID     string
	granteeTenant string
	atomID        string
	amount        float64
	currency      string
	usageContext  string
	sourceEventID string
	tenantID      string
}

func (f *fakeRoyaltyPublisher) PublishRoyaltySettled(_ context.Context,
	settlementID, grantID, ownerGCID, granteeTenantID, atomID string,
	amount float64,
	curr, usageContext, sourceEventID, tenantID string,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pubs = append(f.pubs, royaltyPub{
		settlementID, grantID, ownerGCID, granteeTenantID, atomID,
		amount, curr, usageContext, sourceEventID, tenantID,
	})
	return nil
}

// fakeCurrencyStore records author credits.
type fakeCurrencyStore struct {
	mu      sync.Mutex
	credits []creditCall
}

type creditCall struct {
	tenantID string
	gcid     string
	currency currency.Currency
	amount   float64
}

func (f *fakeCurrencyStore) CreditAuthor(_ context.Context, tenantID, gcid string, c currency.Currency, amount float64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.credits = append(f.credits, creditCall{tenantID, gcid, c, amount})
	return nil
}

func (f *fakeCurrencyStore) GetBalance(_ context.Context, _, _ string, _ string) (float64, error) {
	return 0, nil
}

// --- Helpers ---

func royaltyTestDeps(t *testing.T) (Deps, *inmem.RoyaltyRepo, *fakeManaDebiter, *fakeCurrencyStore, *fakeRoyaltyPublisher) {
	t.Helper()
	const (
		owner    = "01970000-0000-7000-9000-0000000000aa"
		atomID   = "01970000-0000-7000-9000-atom-royalty1"
		tenantID = "01970000-0000-7000-9000-tenant-roya1"
		revID    = "01970000-0000-7000-9000-rev-royalty1"
	)
	_ = owner
	_ = atomID
	_ = tenantID
	_ = revID
	royalties := inmem.NewRoyaltyRepo()
	mana := &fakeManaDebiter{}
	curr := &fakeCurrencyStore{}
	pub := &fakeRoyaltyPublisher{}
	return Deps{
		Grants:       newFakeGrantRepo(),
		Projections:  newFakeProjectionRepo(),
		Shares:       newFakeShareRepo(),
		Royalties:    royalties,
		Currency:     curr,
		Rules:        testRules(),
		Mana:         mana,
		RoyaltyEvents: pub,
	}, royalties, mana, curr, pub
}

// seedRoyaltyShare sets up a shared atom with a royalty_pct license at 10%,
// owned by `owner`, visible to a non-owner grantee via source_share_entry.
func seedRoyaltyShare(deps Deps, license atom_share.LicenseTerms, rate atom_share.RoyaltyRate) (owner, grantee, atomID, tenantID, feedEntryID string) {
	owner = "01970000-0000-7000-9000-0000000000aa"
	grantee = "01970000-0000-7000-9000-0000000000bb"
	atomID = "01970000-0000-7000-9000-atom-royalty1"
	tenantID = "01970000-0000-7000-9000-tenant-roya1"
	feedEntryID = "01970000-0000-7000-9000-share-roya1"
	revID := "01970000-0000-7000-9000-rev-royalty1"

	deps.Projections.(*fakeProjectionRepo).seed(atom_projection.Projection{
		AtomID:      atomID,
		RevisionID:  revID,
		OwnerGCID:   owner,
		PublishedAt: time.Now().UTC(),
	})
	sh := &atom_share.Share{
		FeedEntryID: feedEntryID,
		TenantID:    tenantID,
		AtomID:      atomID,
		RevisionID:  revID,
		OwnerGCID:   owner,
		License:     license,
		Rate:        rate,
	}
	_ = deps.Shares.SaveShare(context.Background(), sh)
	return
}

// --- Tests ---

// TestAuthorizeAtomUse_RoyaltyPct_Settles tests the full settlement pipeline
// for a royalty_pct license: the settlement record lands, the mana debit fires,
// the author is credited in the config currency (reputation), and the
// royalty.settled.v1 event is published.
func TestAuthorizeAtomUse_RoyaltyPct_Settles(t *testing.T) {
	t.Parallel()
	deps, royalties, mana, curr, pub := royaltyTestDeps(t)

	// royalty_pct at 10% with base_usage_value=1 → amount = 1 × 0.1 = 0.1.
	// RoyaltyBase=10 (testRules) floors it → amount = 10.
	owner, grantee, atomID, tenantID, feedEntry := seedRoyaltyShare(deps,
		atom_share.LicenseRoyaltyPct, atom_share.RoyaltyRate{Kind: "pct", Value: 0.1})

	srv := New(deps)
	resp, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:      grantee,
		TenantId:         tenantID,
		AtomId:           atomID,
		Scope:            sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		IdempotencyKey:   "royalty-idem-001",
		SourceShareEntry: feedEntry,
	})
	if err != nil {
		t.Fatalf("AuthorizeAtomUse: %v", err)
	}
	if resp.GetLicenseTermsSnapshot() != sharingv1.LicenseTerms_LICENSE_TERMS_ROYALTY_PCT {
		t.Fatalf("license=%v want ROYALTY_PCT", resp.GetLicenseTermsSnapshot())
	}

	// Settlement record persisted.
	exists, err := royalties.Exists(context.Background(), "royalty-idem-001")
	if err != nil {
		t.Fatalf("royalties.Exists: %v", err)
	}
	if !exists {
		t.Fatal("expected royalty settlement record to exist")
	}

	// Mana debited from the grantee.
	mana.mu.Lock()
	defer mana.mu.Unlock()
	if len(mana.debits) != 1 {
		t.Fatalf("expected 1 mana debit, got %d", len(mana.debits))
	}
	d := mana.debits[0]
	if d.gcid != grantee {
		t.Errorf("debit gcid=%q want %q", d.gcid, grantee)
	}
	if d.amount != 10 {
		t.Errorf("debit amount=%v want 10 (floored at RoyaltyBase)", d.amount)
	}
	if d.code != "atom_royalty" {
		t.Errorf("debit code=%q want atom_royalty", d.code)
	}

	// Author credited in the config currency (reputation).
	curr.mu.Lock()
	defer curr.mu.Unlock()
	if len(curr.credits) != 1 {
		t.Fatalf("expected 1 author credit, got %d", len(curr.credits))
	}
	c := curr.credits[0]
	if c.gcid != owner {
		t.Errorf("credit gcid=%q want %q (the author)", c.gcid, owner)
	}
	if c.currency != currency.CurrencyReputation {
		t.Errorf("credit currency=%q want reputation", c.currency)
	}
	if c.amount != 10 {
		t.Errorf("credit amount=%v want 10", c.amount)
	}

	// royalty.settled.v1 published.
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.pubs) != 1 {
		t.Fatalf("expected 1 royalty.settled.v1 publish, got %d", len(pub.pubs))
	}
	p := pub.pubs[0]
	if p.ownerGCID != owner {
		t.Errorf("publish owner=%q want %q", p.ownerGCID, owner)
	}
	if p.amount != 10 {
		t.Errorf("publish amount=%v want 10", p.amount)
	}
	if p.currency != "reputation" {
		t.Errorf("publish currency=%q want reputation", p.currency)
	}
	if p.sourceEventID != "royalty-idem-001" {
		t.Errorf("publish sourceEventID=%q want royalty-idem-001", p.sourceEventID)
	}
}

// TestAuthorizeAtomUse_FreeLicense_NoSettlement tests that a free-license
// own-atom grant skips settlement entirely — no record, no debit, no credit,
// no publish.
func TestAuthorizeAtomUse_FreeLicense_NoSettlement(t *testing.T) {
	t.Parallel()
	deps, royalties, mana, curr, pub := royaltyTestDeps(t)

	const (
		owner    = "01970000-0000-7000-9000-0000000000cc"
		atomID   = "01970000-0000-7000-9000-atom-free1"
		tenantID = "01970000-0000-7000-9000-tenant-free1"
	)
	deps.Projections.(*fakeProjectionRepo).seed(atom_projection.Projection{
		AtomID:      atomID,
		RevisionID:  "rev-free1",
		OwnerGCID:   owner,
		PublishedAt: time.Now().UTC(),
	})

	srv := New(deps)
	_, err := srv.AuthorizeAtomUse(context.Background(), &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:    owner, // own-atom → free
		TenantId:       tenantID,
		AtomId:         atomID,
		Scope:          sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		IdempotencyKey: "free-idem-001",
	})
	if err != nil {
		t.Fatalf("AuthorizeAtomUse: %v", err)
	}

	// No settlement record.
	exists, _ := royalties.Exists(context.Background(), "free-idem-001")
	if exists {
		t.Fatal("expected NO royalty settlement for free license")
	}

	// No mana debit.
	mana.mu.Lock()
	defer mana.mu.Unlock()
	if len(mana.debits) != 0 {
		t.Errorf("expected 0 mana debits, got %d", len(mana.debits))
	}

	// No author credit.
	curr.mu.Lock()
	defer curr.mu.Unlock()
	if len(curr.credits) != 0 {
		t.Errorf("expected 0 author credits, got %d", len(curr.credits))
	}

	// No event publish.
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.pubs) != 0 {
		t.Errorf("expected 0 royalty.settled.v1 publishes, got %d", len(pub.pubs))
	}
}
// TestAuthorizeAtomUse_Royalty_IdempotentReplay tests that re-authorizing the
// same grant (same idempotency_key) does NOT double-debit or double-credit —
// the RoyaltyRepo returns ErrRoyaltyAlreadySettled, which settleRoyalty
// treats as success (idempotent replay).
func TestAuthorizeAtomUse_Royalty_IdempotentReplay(t *testing.T) {
	t.Parallel()
	deps, _, mana, curr, pub := royaltyTestDeps(t)

	_, grantee, atomID, tenantID, feedEntry := seedRoyaltyShare(deps,
		atom_share.LicenseRoyaltyFixed, atom_share.RoyaltyRate{Kind: "fixed_per_use", Value: 5})

	srv := New(deps)
	req := &sharingv1.AuthorizeAtomUseRequest{
		GranteeGcid:      grantee,
		TenantId:         tenantID,
		AtomId:           atomID,
		Scope:            sharingv1.GrantScope_GRANT_SCOPE_DUEL,
		IdempotencyKey:   "idem-replay-001",
		SourceShareEntry: feedEntry,
	}

	// First call — settles.
	if _, err := srv.AuthorizeAtomUse(context.Background(), req); err != nil {
		t.Fatalf("first AuthorizeAtomUse: %v", err)
	}

	// Second call — same idempotency_key. The GrantRepo.Authorize returns
	// the existing active grant (idempotent on (grantee, atom, scope)).
	// settleRoyalty fires again with the same source_event_id, but
	// RoyaltyRepo.Record returns ErrRoyaltyAlreadySettled → settleRoyalty
	// returns early — no double-debit, no double-credit, no double-publish.
	if _, err := srv.AuthorizeAtomUse(context.Background(), req); err != nil {
		t.Fatalf("second AuthorizeAtomUse (idempotent replay): %v", err)
	}

	// Exactly 1 mana debit (not 2).
	mana.mu.Lock()
	defer mana.mu.Unlock()
	if len(mana.debits) != 1 {
		t.Errorf("expected 1 mana debit after replay, got %d", len(mana.debits))
	}

	// Exactly 1 author credit (not 2).
	curr.mu.Lock()
	defer curr.mu.Unlock()
	if len(curr.credits) != 1 {
		t.Errorf("expected 1 author credit after replay, got %d", len(curr.credits))
	}

	// Exactly 1 event publish (not 2).
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.pubs) != 1 {
		t.Errorf("expected 1 royalty.settled.v1 publish after replay, got %d", len(pub.pubs))
	}
}
