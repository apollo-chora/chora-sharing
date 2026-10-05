// currency_repo.go — pgx-backed currency.CurrencyRepo.
//
// Credits the author's non-cash running balance (Reputation/Coins) via an
// upsert on currency_balances keyed by (tenant_id, holder_gcid, currency).
// RLS applied on every method via qToExecer(q) before user queries.
//
// Schema: migrations/0005_currency_leaderboard.up.sql (currency_balances).
package pg

import (
	"context"
	"fmt"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-sharing/internal/domain/currency"
)

// sqlBalanceUpsert accumulates the balance via ON CONFLICT … DO UPDATE. The
// CHECK (balance >= 0) constraint is enforced at the DB layer; CreditAuthor
// only ever ADDS (the debit side goes to chora-identity mana, not here).
const sqlBalanceUpsert = `
INSERT INTO currency_balances (tenant_id, holder_gcid, currency, balance)
VALUES ($1, $2, $3, $4)
ON CONFLICT (tenant_id, holder_gcid, currency)
DO UPDATE SET balance = currency_balances.balance + EXCLUDED.balance`

const sqlBalanceSelect = `
SELECT balance FROM currency_balances
WHERE tenant_id = $1 AND holder_gcid = $2 AND currency = $3`

// CurrencyRepo implements currency.CurrencyRepo against Postgres.
type CurrencyRepo struct {
	tx TxRunner
}

// NewCurrencyRepo constructs a CurrencyRepo bound to a TxRunner.
func NewCurrencyRepo(tx TxRunner) *CurrencyRepo { return &CurrencyRepo{tx: tx} }

// Compile-time port assertion.
var _ currency.CurrencyRepo = (*CurrencyRepo)(nil)

// CreditAuthor upserts (tenant_id, holder_gcid, currency) accumulating the
// balance. The caller's idempotency token (the royalty settlement's
// SourceEventID) is deduped at the RoyaltyRepo layer, not here.
func (r *CurrencyRepo) CreditAuthor(ctx context.Context, tenantID, holderGCID string, c currency.Currency, amount float64) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if amount < 0 {
		return fmt.Errorf("%w: credit amount %v must be >= 0", currency.ErrInvalidArgument, amount)
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		if _, err := q.Exec(ctx, sqlBalanceUpsert,
			tenantID, holderGCID, string(c), amount,
		); err != nil {
			return fmt.Errorf("pg: upsert currency balance: %w", err)
		}
		return nil
	})
}

// GetBalance returns the current non-cash balance for (tenant, holder,
// currency); 0.0 when no row (a fresh author).
func (r *CurrencyRepo) GetBalance(ctx context.Context, tenantID, holderGCID, currencyCode string) (float64, error) {
	if r == nil || r.tx == nil {
		return 0, ErrNotImplemented
	}
	var balance float64
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		if err := q.QueryRow(ctx, sqlBalanceSelect, tenantID, holderGCID, currencyCode).Scan(&balance); err != nil {
			// No row → fresh author; balance stays 0.0.
			return nil
		}
		return nil
	})
	return balance, err
}
