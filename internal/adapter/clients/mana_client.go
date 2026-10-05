// Package clients — ManaService client (chora-identity).
//
// ManaClient implements currency.ManaLedger by delegating the reuser-tenant
// mana debit to chora-identity ManaService.DeductMana (gRPC). Used by the
// royalty-settlement site (§3.3): the debit is the "spend" side of the
// double-entry accrual; the author credit lives in chora_sharing.
//
// Per §9 + §1.1: fail-loud on misconfiguration (nil client →
// ErrManaClientNotConfigured). Per §9 "no double-spend": an insufficient
// balance returns ErrInsufficientMana and the caller MUST nack.
package clients

import (
	"context"
	"fmt"
	"strings"

	"github.com/apollo-chora/chora-sharing/internal/domain/currency"
	identityv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/identity/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Compile-time check: *ManaClient satisfies currency.ManaLedger.
var _ currency.ManaLedger = (*ManaClient)(nil)

// ManaClient is the production implementation of currency.ManaLedger. It
// debits the reuser's per-user mana wallet via chora-identity ManaService.
//
// A nil ManaServiceClient (deps.Mana == nil at boot) makes every call return
// ErrManaClientNotConfigured — the handler surfaces 501/Unimplemented (§1.1).
type ManaClient struct {
	mana identityv1.ManaServiceClient
}

// NewManaClient wraps a bound ManaServiceClient. Pass nil when the env is
// unconfigured; the resulting client fails every call loud (never a fake
// success) so handlers emit 501 rather than silently skipping the debit.
func NewManaClient(mana identityv1.ManaServiceClient) *ManaClient {
	return &ManaClient{mana: mana}
}

// DebitReuser debits `amount` mana from the grantee's wallet under actionCode.
//
// Mapping to ManaService.DeductMana (chora-contracts/proto/services/identity/
// v1/mana_service.proto):
//   - gcid            = granteeGCID            (the reuser learner — REQUIRED, the wallet key)
//   - tenant_id       = granteeTenantID        (subsidy-FIFO bias only)
//   - units           = int64(amount)          (mana is integer-denominated)
//   - action_code     = actionCode             (price-plan rule key, e.g. "atom_royalty")
//   - idempotency_key = idempotencyKey         (caller-supplied — a retry with the
//                                               same key is a no-op, never a double-debit)
//
// The idempotency_key is the no-double-spend seam: the royalty settlement's
// SourceEventID SHOULD be threaded here so a ledger replay collapses to a
// single debit (§9). When idempotencyKey is empty the client mints a UUIDv7
// so a one-shot debit still dedups on retry within the caller's window.
//
// ErrInsufficientMana is returned when the wallet cannot cover the debit —
// the proto signals this via FAILED_PRECONDITION (gRPC code 9; mapped to
// HTTP 402). The caller MUST nack (never double-spend).
func (c *ManaClient) DebitReuser(ctx context.Context, granteeGCID, granteeTenantID string, amount float64, actionCode string) error {
	if c == nil || c.mana == nil {
		return currency.ErrManaClientNotConfigured
	}
	if strings.TrimSpace(granteeGCID) == "" {
		return fmt.Errorf("%w: grantee_gcid required", currency.ErrInvalidArgument)
	}
	if amount <= 0 {
		return fmt.Errorf("%w: amount must be > 0", currency.ErrInvalidArgument)
	}
	if strings.TrimSpace(actionCode) == "" {
		return fmt.Errorf("%w: action_code required", currency.ErrInvalidArgument)
	}

	idemKey := newIdempotencyKey(ctx)
	resp, err := c.mana.DeductMana(ctx, &identityv1.DeductManaRequest{
		Gcid:           granteeGCID,
		ActionCode:     actionCode,
		Units:          int64(amount),
		IdempotencyKey: idemKey,
		TenantId:       granteeTenantID,
	})
	if err != nil {
		// FAILED_PRECONDITION is the insufficient-balance signal (proto comment).
		if status.Code(err) == codes.FailedPrecondition {
			return fmt.Errorf("%w: %v", currency.ErrInsufficientMana, err)
		}
		return fmt.Errorf("mana debit: %w", err)
	}
	if !resp.GetSuccess() {
		// Dry-run-style shortfall (success=false carries required/current).
		return fmt.Errorf("%w: required=%d available=%d",
			currency.ErrInsufficientMana, resp.GetRequiredUnits(), resp.GetCurrentBalanceUnits())
	}
	return nil
}

// manaIdempotencyKeyKey is the context.Value key carrying the caller-supplied
// idempotency token (the royalty settlement's SourceEventID). When absent the
// client mints a UUIDv7 so a single debit still dedups on in-process retry.
type manaIdempotencyKey struct{}

// WithManaIdempotencyKey returns a ctx carrying idempotencyKey so a royalty
// settlement replay reuses the SAME DeductMana idempotency_key (the no-double-
// spend seam). Callers SHOULD pass RoyaltySettlement.SourceEventID.
func WithManaIdempotencyKey(ctx context.Context, idempotencyKey string) context.Context {
	if idempotencyKey == "" {
		return ctx
	}
	return context.WithValue(ctx, manaIdempotencyKey{}, idempotencyKey)
}

// newIdempotencyKey resolves the caller-supplied idempotency key from the
// context, minting a fresh UUIDv7 when none is present. Determinism across
// retries is the caller's responsibility (thread SourceEventID via
// WithManaIdempotencyKey); a minted key is a per-call fallback for one-shot
// debits that still dedup within the caller's retry window.
func newIdempotencyKey(ctx context.Context) string {
	if v, ok := ctx.Value(manaIdempotencyKey{}).(string); ok && v != "" {
		return v
	}
	return uuid.Must(uuid.NewV7()).String()
}
