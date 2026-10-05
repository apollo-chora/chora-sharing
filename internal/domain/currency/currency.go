// Package currency is the pure-domain core for the non-cash author credit
// ledger of the Content Sharing domain.
//
// This package holds:
//   - CurrencyBalance value object (the author's non-cash running balance).
//   - ManaLedger port (debit reuser tenant mana via chora-identity).
//   - CurrencyRepo port (credit author non-cash currency).
//
// Per §3.3: the mana DEBIT to the reuser tenant goes through chora-identity
// ManaService (gRPC); the non-cash CREDIT to the author (Reputation/Coins) is
// recorded in chora_sharing.currency_balances. XP is NOT stored here —
// chora-consumption owns canonical XP. This package holds ONLY the non-cash
// author credit + the mana-debit seam.
//
// Owning domain : Content Sharing (chora_sharing DB).
// Owning team   : Team 1 (Content).
//
// Hard rules (per .claude/rules/ddd-enforcement.md):
//   - This file holds the domain only — NO HTTP, NO persistence.
//   - holder_gcid is the AUTHOR (credit side); grantee_tenant_id is the
//     reuser's tenant (debit side). Both are opaque cross-domain UUIDs.
package currency

import (
	"context"
	"errors"
	"fmt"
)

// Currency is the non-cash credit currency recorded in currency_balances.
// Mirrors the currency_code PG enum. XP is deliberately excluded —
// chora-consumption owns canonical XP.
type Currency string

const (
	// CurrencyCoins — spendable social currency.
	CurrencyCoins Currency = "coins"
	// CurrencyReputation — standing currency (the §3.3 default non-cash credit).
	CurrencyReputation Currency = "reputation"
)

// IsValid reports whether the currency is one of the non-cash codes. XP is
// NOT valid here — chora-consumption owns it.
func (c Currency) IsValid() bool {
	switch c {
	case CurrencyCoins, CurrencyReputation:
		return true
	}
	return false
}

// ErrInvalidArgument is returned for guard-clause failures.
var ErrInvalidArgument = errors.New("invalid argument")

// ErrInsufficientMana is returned by ManaLedger.DebitReuser when the reuser
// tenant's mana balance cannot cover the debit. The caller (the royalty
// settlement site) MUST nack — never double-spend. Per §9: "no double-spend".
var ErrInsufficientMana = errors.New("insufficient mana for royalty debit")

// ErrManaClientNotConfigured is returned when ManaLedger is nil at the call
// site — fail-loud on misconfiguration, never a fake success (§1.1).
var ErrManaClientNotConfigured = errors.New("mana ledger client not configured")

// CurrencyBalance is the value object for an author's non-cash running
// balance. Upsert key: (tenant_id, holder_gcid, currency). Balance ≥ 0
// enforced at the DB layer (CHECK constraint) + by CreditAuthor's accumulation.
//
// Cross-domain references (all opaque UUID, no FK):
//   - HolderGCID → chora_identity.Account (the AUTHOR)
type CurrencyBalance struct {
	TenantID   string   `json:"tenant_id"`
	HolderGCID string   `json:"holder_gcid"` // the author (credit side)
	Currency   Currency `json:"currency"`
	Balance    float64  `json:"balance"` // ≥ 0
}

// Validate enforces the balance invariants: tenantID + holderGCID required,
// currency valid, balance ≥ 0.
func (b CurrencyBalance) Validate() error {
	if b.TenantID == "" {
		return fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if b.HolderGCID == "" {
		return fmt.Errorf("%w: holder_gcid required", ErrInvalidArgument)
	}
	if !b.Currency.IsValid() {
		return fmt.Errorf("%w: invalid currency %q (XP is owned by chora-consumption)", ErrInvalidArgument, b.Currency)
	}
	if b.Balance < 0 {
		return fmt.Errorf("%w: balance %v must be >= 0", ErrInvalidArgument, b.Balance)
	}
	return nil
}

// ManaLedger is the hexagonal port for the mana DEBIT to the reuser.
// Implemented by adapter/clients/mana_client.go → chora-identity
// ManaService.DeductMana (gRPC). The debit is the "spend" side of the
// double-entry royalty settlement (§3.3).
//
// Contract:
//   - DebitReuser debits `amount` mana from the grantee's wallet (gcid)
//     under the given action_code (price-plan rule key, e.g. "atom_royalty").
//     The granteeTenantID biases subsidy-FIFO selection when the user holds
//     allocations from multiple tenants (per ManaService.DeductMana.tenant_id).
//   - Returns ErrInsufficientMana when the wallet cannot cover the debit — the
//     caller MUST nack (no double-spend).
//   - Returns ErrManaClientNotConfigured when the client is nil (fail-loud).
//
// The wire proto (chora-identity ManaService.DeductMana) debits a PER-USER
// wallet keyed by gcid (field 1, required); tenant_id (field 6) is the
// subsidy-FIFO bias only. The port therefore carries granteeGCID as the
// primary debit target — settlement is a per-user wallet debit, not per-tenant
// (the proto wins over the §3.3 "reuser tenant" prose).
type ManaLedger interface {
	DebitReuser(ctx context.Context, granteeGCID, granteeTenantID string, amount float64, actionCode string) error
}

// CurrencyRepo is the hexagonal port for the non-cash CREDIT to the author.
// Implemented by adapter/pg with RLS-aware pgx; the inmem double for tests.
// The credit is the "accrue" side of the double-entry royalty settlement.
//
// Contract:
//   - CreditAuthor upserts (tenant_id, holder_gcid, currency) accumulating
//     the balance. Idempotent on the caller's idempotency token (the royalty
//     settlement's SourceEventID) — the adapter dedups via the settlement
//     record, not via this call.
//   - GetBalance returns the current non-cash balance for
//     (tenant, holder, currency); 0.0 when no row (a fresh author).
type CurrencyRepo interface {
	CreditAuthor(ctx context.Context, tenantID, holderGCID string, currency Currency, amount float64) error
	GetBalance(ctx context.Context, tenantID, holderGCID, currency string) (float64, error)
}
