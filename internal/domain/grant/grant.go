// Package grant is the pure-domain core for the AtomUsageGrant aggregate —
// the only genuinely-new aggregate root in the chora-sharing greenfield
// rebuild.
//
// An AtomUsageGrant is a licensing record created lazily on real reuse: when a
// learner wants to reuse an atom in a duel / test-set / LiveQuiz / collection,
// the system materialises a grant freezing the license snapshot at that
// moment (dispute-proof — an author price-change does NOT alter existing
// grants, R2). Royalty settles at the point of reuse via the double-entry
// RoyaltySettlement (debit reuser tenant mana, credit author non-cash
// currency).
//
// Aggregate root : AtomUsageGrant.
// Owning domain : Content Sharing (chora_sharing DB).
// Owning team   : Team 1 (Content).
//
// Hard rules (per .claude/rules/ddd-enforcement.md + §3.3 + §3.4):
//   - atom_id / revision_id are opaque UUID refs to chora_creation — NO FK.
//   - owner_gcid is the AUTHOR of record (R1); grantee_gcid is the reuser.
//   - The license snapshot (license_terms_snapshot + royalty_rate_snapshot) is
//     FROZEN at grant time and never mutates — R2. Re-issuing the same grant
//     (idempotent on (grantee, atom, scope) WHERE active) returns the existing
//     frozen snapshot.
package grant

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
)

// ErrInvalidArgument is returned for guard-clause failures inside NewGrant.
var ErrInvalidArgument = errors.New("invalid argument")

// Scope is the real reuse target an AtomUsageGrant covers (§3.4). Maps to the
// atom_grant_scope PG enum.
//
// UNLIMITED covers every scope (Scope.Covers(target) returns true for any
// target). Otherwise exact match. There is NO question_set / course scope —
// no real aggregate backs them.
type Scope string

const (
	// ScopeTestSet covers delivery test_sets.
	ScopeTestSet Scope = "test_set"
	// ScopeDuel covers sharing duels.
	ScopeDuel Scope = "duel"
	// ScopeLiveQuiz covers delivery LiveQuiz.
	ScopeLiveQuiz Scope = "live_quiz"
	// ScopeCollection covers creation collections.
	ScopeCollection Scope = "collection"
	// ScopeUnlimited covers all of the above — FR-013.
	ScopeUnlimited Scope = "unlimited"
)

// IsValid reports whether the scope is one of the five canonical scopes.
func (s Scope) IsValid() bool {
	switch s {
	case ScopeTestSet, ScopeDuel, ScopeLiveQuiz, ScopeCollection, ScopeUnlimited:
		return true
	}
	return false
}

// Covers reports whether this grant's scope covers the requested target scope.
// UNLIMITED covers all targets; otherwise exact match. Per FR-013: a grant
// with ScopeUnlimited satisfies a reuse request for any target scope (duel,
// test_set, live_quiz, collection).
func (s Scope) Covers(target Scope) bool {
	if s == ScopeUnlimited {
		return true
	}
	return s == target
}

// Status is the grant lifecycle state (maps to atom_grant_status PG enum).
type Status string

const (
	// StatusActive — the grant permits reuse.
	StatusActive Status = "active"
	// StatusRevoked — the owner revoked it (terminal).
	StatusRevoked Status = "revoked"
	// StatusExpired — the grant passed its expiry (terminal).
	StatusExpired Status = "expired"
)

// IsTerminal reports whether the status is a terminal state (no further
// transitions possible). active is non-terminal; revoked + expired are
// terminal.
func (s Status) IsTerminal() bool {
	return s == StatusRevoked || s == StatusExpired
}

// AtomUsageGrant is the aggregate root. Created lazily on real reuse; freezes
// the license snapshot at grant time (R2 — dispute-proof against later author
// price-changes).
//
// Cross-domain references (all opaque UUID, no FK):
//   - OwnerGCID   → chora_identity.Account (the AUTHOR — R1)
//   - GranteeGCID → chora_identity.Account (the reuser)
//   - AtomID      → chora_creation.LearningAtom
//   - RevisionID  → chora_creation.AtomRevision (pinned at grant time)
type AtomUsageGrant struct {
	ID                   string                  `json:"id"`
	TenantID             string                  `json:"tenant_id"`                    // the REUSER's tenant (pays royalty)
	SourceShareEntry     string                  `json:"source_share_entry,omitempty"` // provenance; empty for own-atom
	OwnerGCID            string                  `json:"owner_gcid"`                   // the AUTHOR (R1)
	GranteeGCID          string                  `json:"grantee_gcid"`                 // the reuser
	AtomID               string                  `json:"atom_id"`
	RevisionID           string                  `json:"atom_revision_id"` // pinned at grant time
	Scope                Scope                   `json:"scope"`
	LicenseTermsSnapshot atom_share.LicenseTerms `json:"license_terms_snapshot"` // FROZEN
	RoyaltyRateSnapshot  atom_share.RoyaltyRate  `json:"royalty_rate_snapshot"`  // FROZEN
	Status               Status                  `json:"status"`
	GrantedAt            time.Time               `json:"granted_at"`
	RevokedAt            *time.Time              `json:"revoked_at,omitempty"`
	ExpiresAt            *time.Time              `json:"expires_at,omitempty"` // nil = no expiry
}

// NewGrant constructs an AtomUsageGrant, freezing the license snapshot. The
// caller (the AuthorizeAtomUse handler §7.3) resolves the license:
//   - own-atom (grantee == owner) → license `free`, no rate
//   - shared-atom → resolve the share's LicenseTerms + RoyaltyRate
//
// Validation guards rejected with ErrInvalidArgument:
//   - tenantID, ownerGCID, granteeGCID, atomID, revisionID required
//   - scope must be valid (non-UNSPECIFIED)
//   - license must be valid; if royalty license, rate must be valid (R2)
//   - sourceShareEntry optional (empty for own-atom grants)
//
// The frozen snapshot is stored verbatim — never re-derived after grant.
func NewGrant(ownerGCID, granteeGCID, atomID, revisionID string, scope Scope, license atom_share.LicenseTerms, rate atom_share.RoyaltyRate, sourceShareEntry string) (*AtomUsageGrant, error) {
	if ownerGCID == "" {
		return nil, fmt.Errorf("%w: owner_gcid (R1 author) required", ErrInvalidArgument)
	}
	if granteeGCID == "" {
		return nil, fmt.Errorf("%w: grantee_gcid required", ErrInvalidArgument)
	}
	if atomID == "" {
		return nil, fmt.Errorf("%w: atom_id required", ErrInvalidArgument)
	}
	if revisionID == "" {
		return nil, fmt.Errorf("%w: atom_revision_id required", ErrInvalidArgument)
	}
	if !scope.IsValid() {
		return nil, fmt.Errorf("%w: invalid scope %q", ErrInvalidArgument, scope)
	}
	if !license.IsValid() {
		return nil, fmt.Errorf("%w: invalid license_terms %q", ErrInvalidArgument, license)
	}
	if license.IsRoyalty() {
		if err := rate.Validate(); err != nil {
			return nil, fmt.Errorf("%w: royalty license %q requires a valid royalty_rate snapshot", ErrInvalidArgument, license)
		}
	}
	return &AtomUsageGrant{
		ID:                   NewUUIDv7(),
		OwnerGCID:            ownerGCID,
		GranteeGCID:          granteeGCID,
		AtomID:               atomID,
		RevisionID:           revisionID,
		Scope:                scope,
		LicenseTermsSnapshot: license,
		RoyaltyRateSnapshot:  rate,
		Status:               StatusActive,
		GrantedAt:            time.Now().UTC(),
		SourceShareEntry:     sourceShareEntry,
	}, nil
}

// IsUsable reports whether the grant currently permits the grantee to reuse
// the atom for the requested target scope. The grant must be active, owned
// by grantee, and its scope must cover the target.
//
// Expiry is checked lazily: if ExpiresAt is non-nil and in the past, the grant
// is not usable (the caller should call Expire to flip status to expired, or
// the expiry sweeper does it).
func (g *AtomUsageGrant) IsUsable(grantee string, target Scope) bool {
	if g == nil {
		return false
	}
	if g.Status != StatusActive {
		return false
	}
	if g.GranteeGCID != grantee {
		return false
	}
	if !g.Scope.Covers(target) {
		return false
	}
	if g.ExpiresAt != nil && !g.ExpiresAt.After(time.Now().UTC()) {
		return false
	}
	return true
}

// Revoke transitions the grant to StatusRevoked (terminal). Idempotent —
// revoking an already-revoked grant is a no-op. Revoking an expired grant is
// rejected (expired is also terminal; don't mask the cause).
func (g *AtomUsageGrant) Revoke(at time.Time) error {
	if g.Status == StatusExpired {
		return fmt.Errorf("%w: cannot revoke an expired grant", ErrInvalidArgument)
	}
	if g.Status == StatusRevoked {
		return nil // idempotent
	}
	g.Status = StatusRevoked
	t := at.UTC()
	g.RevokedAt = &t
	return nil
}

// Expire transitions the grant to StatusExpired (terminal). Idempotent —
// expiring an already-expired grant is a no-op. Expiring a revoked grant is
// rejected (revoked is terminal; don't mask the cause).
func (g *AtomUsageGrant) Expire(at time.Time) error {
	if g.Status == StatusRevoked {
		return fmt.Errorf("%w: cannot expire a revoked grant", ErrInvalidArgument)
	}
	if g.Status == StatusExpired {
		return nil // idempotent
	}
	g.Status = StatusExpired
	t := at.UTC()
	g.ExpiresAt = &t
	return nil
}

// SnapshotLicense returns the frozen license snapshot taken at grant time.
// This is the dispute-proof record (R2): an author price-change does NOT
// alter existing grants — the snapshot is immutable.
func (g *AtomUsageGrant) SnapshotLicense() (atom_share.LicenseTerms, atom_share.RoyaltyRate) {
	return g.LicenseTermsSnapshot, g.RoyaltyRateSnapshot
}

// RoyaltySettlement is the double-entry accrual record (§3.3). Debit the
// reuser tenant's mana (via chora-identity ManaService); credit the author's
// non-cash currency (Reputation/Coins) in currency_balances. Idempotent on
// SourceEventID — a replay never double-credits.
type RoyaltySettlement struct {
	ID              string    `json:"id"`
	GrantID         string    `json:"grant_id"`
	OwnerGCID       string    `json:"owner_gcid"`        // credit (the author)
	GranteeTenantID string    `json:"grantee_tenant_id"` // debit
	AtomID          string    `json:"atom_id"`
	Amount          float64   `json:"amount"`
	Currency        string    `json:"currency"`        // mana / coins / reputation / usd
	UsageContext    string    `json:"usage_context"`   // duel / live_quiz / question_set
	SourceEventID   string    `json:"source_event_id"` // idempotency key
	CreatedAt       time.Time `json:"created_at"`
}

// SettleInput carries the inputs to SettleRoyalty. The caller (the reuse
// site — SubmitDuelAnswer, AuthorizeLiveQuizAtoms) supplies the base usage
// value + the frozen snapshot + the config caps.
type SettleInput struct {
	BaseUsageValue  float64                 // the per-use value the rate is applied to
	License         atom_share.LicenseTerms // from the grant's frozen snapshot
	Rate            atom_share.RoyaltyRate  // from the grant's frozen snapshot
	RoyaltyBase     int                     // config floor (CHORA_SHARING_ROYALTY_BASE)
	RoyaltyCap      int                     // config ceiling (0 = uncapped)
	SourceEventID   string                  // idempotency key (the triggering event)
	UsageContext    string                  // duel / live_quiz / question_set
	OwnerGCID       string                  // the author (credit)
	GranteeTenantID string                  // the reuser tenant (debit)
	AtomID          string
	GrantID         string
	Currency        string // config-driven credit currency (CHORA_SHARING_ROYALTY_CURRENCY, default "reputation")
}

// SettleRoyalty computes the RoyaltySettlement per §3.3 math:
//
//	royalty_pct   → amount = base_usage_value × rate.value
//	royalty_fixed → amount = rate.value
//	free / cc_*   → amount = 0 (no settlement)
//
// The amount is clamped to RoyaltyCap when cap > 0. When RoyaltyBase > 0 and
// the computed amount is non-zero, the amount is floored at RoyaltyBase
// (the per-use royalty floor) before clamping — so a royalty_pct with a tiny
// base_usage_value still accrues at least the floor.
//
// Free licenses incur no settlement (amount=0); the returned settlement still
// carries the IDs for audit + idempotency tracking (the caller MAY persist it
// to record "no royalty accrued for this reuse" — or skip when amount == 0).
func SettleRoyalty(in SettleInput) RoyaltySettlement {
	amount := computeRoyaltyAmount(in)

	return RoyaltySettlement{
		ID:              NewUUIDv7(),
		GrantID:         in.GrantID,
		OwnerGCID:       in.OwnerGCID,
		GranteeTenantID: in.GranteeTenantID,
		AtomID:          in.AtomID,
		Amount:          amount,
		Currency:        in.Currency,
		UsageContext:    in.UsageContext,
		SourceEventID:   in.SourceEventID,
		CreatedAt:       time.Now().UTC(),
	}
}

// computeRoyaltyAmount implements the §3.3 math: pct = base × rate; fixed =
// rate; free = 0; floored at RoyaltyBase when > 0; clamped to RoyaltyCap
// when > 0.
func computeRoyaltyAmount(in SettleInput) float64 {
	if !in.License.IsRoyalty() {
		return 0
	}
	var amount float64
	switch in.License {
	case atom_share.LicenseRoyaltyPct:
		amount = in.BaseUsageValue * in.Rate.Value
	case atom_share.LicenseRoyaltyFixed:
		amount = in.Rate.Value
	}
	// Floor at RoyaltyBase when a floor is configured (only when amount > 0).
	if in.RoyaltyBase > 0 && amount > 0 && amount < float64(in.RoyaltyBase) {
		amount = float64(in.RoyaltyBase)
	}
	// Clamp to RoyaltyCap when a cap is configured (> 0 means capped).
	if in.RoyaltyCap > 0 && amount > float64(in.RoyaltyCap) {
		amount = float64(in.RoyaltyCap)
	}
	if amount < 0 {
		amount = 0
	}
	return amount
}

// EntitledAtom is a single row in the ListEntitledAtoms result (§7.4). The
// "atoms usable by me" union: own ∪ free ∪ active-grant. IsOwn is true when
// the projection's owner_gcid == gcid; HasGrant is true when an active grant
// covers (gcid, atom) for the scope.
type EntitledAtom struct {
	AtomID            string                  `json:"atom_id"`
	RevisionID        string                  `json:"atom_revision_id"`
	AuthorGCID        string                  `json:"author_gcid"`
	AuthorDisplayName string                  `json:"author_display_name"`
	StemPreview       string                  `json:"stem_preview"`
	License           atom_share.LicenseTerms `json:"license_terms"`
	IsOwn             bool                    `json:"is_own"`
	HasGrant          bool                    `json:"has_grant"`
}

// RoyaltyRepo is the hexagonal port for persisting + dedup-checking royalty
// settlements. Idempotent on SourceEventID — a replay never double-credits.
// The pg adapter implements it with RLS-aware pgx; the inmem double for tests.
type RoyaltyRepo interface {
	Record(ctx context.Context, s *RoyaltySettlement) error
	Exists(ctx context.Context, sourceEventID string) (bool, error)
}

// ErrRoyaltyAlreadySettled signals an idempotent replay — the settlement was
// already recorded for this SourceEventID. The caller treats this as success
// (idempotent), NOT an error to surface.
var ErrRoyaltyAlreadySettled = errors.New("royalty already settled for source_event_id")

// GrantRepo is the hexagonal port the AuthorizeAtomUse + RevokeAtomUse +
// ListEntitledAtoms handlers use. Idempotency:
//   - Authorize: ON CONFLICT DO NOTHING on (grantee_gcid, atom_id, scope) WHERE
//     active; SELECT returns the existing active grant when the conflict hits.
//   - Revoke: flips status to revoked; idempotent on grant_id.
//   - GetActive: returns the single active grant for (grantee, atom, scope),
//     or ErrNotFound.
//   - ListEntitled: the union query (own ∪ free ∪ active-grant) per §7.4.
//   - ActiveGrantAtomIDs: distinct atom ids under an ACTIVE, unexpired grant
//     for the grantee (any scope) — the reuse-context surface behind
//     GetReuseContext (ADR-229 WS-0). RLS-scoped to the ctx tenant.
type GrantRepo interface {
	Authorize(ctx context.Context, g *AtomUsageGrant) (*AtomUsageGrant, error)
	Revoke(ctx context.Context, grantID, revokerGCID, reason string) error
	GetActive(ctx context.Context, grantee, atomID string, scope Scope) (*AtomUsageGrant, error)
	ListEntitled(ctx context.Context, gcid string, scope Scope, topicTags []string, limit int) ([]EntitledAtom, error)
	ActiveGrantAtomIDs(ctx context.Context, granteeGCID string) ([]string, error)
}

// ErrGrantNotFound is returned by GrantRepo.GetActive when no active grant
// covers (grantee, atom, scope).
var ErrGrantNotFound = errors.New("no active grant found")

// ErrForbidden is returned when the caller is not authorised to perform
// the operation (e.g. a non-owner attempting to revoke a grant).
var ErrForbidden = errors.New("forbidden: caller is not the owner")

// NewUUIDv7 — RFC 9562 §5.7 UUIDv7. Generated in-domain to keep the aggregate
// dependency-free (matches the post/reaction/duel package pattern).
func NewUUIDv7() string {
	const buflen = 16
	var b [buflen]byte
	now := uint64(time.Now().UnixMilli())
	b[0] = byte(now >> 40)
	b[1] = byte(now >> 32)
	b[2] = byte(now >> 24)
	b[3] = byte(now >> 16)
	b[4] = byte(now >> 8)
	b[5] = byte(now)
	if _, err := rand.Read(b[6:]); err != nil {
		for i := 6; i < buflen; i++ {
			b[i] = byte(now >> uint(8*(i-6)))
		}
	}
	b[6] = (b[6] & 0x0F) | 0x70 // version 7
	b[8] = (b[8] & 0x3F) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
