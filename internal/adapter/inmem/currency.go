package inmem

import (
	"context"
	"sync"

	"github.com/apollo-chora/chora-sharing/internal/domain/currency"
)

// CurrencyRepo is an in-memory adapter implementing currency.CurrencyRepo.
// Balances are keyed by (tenant_id, holder_gcid, currency) and accumulated
// via upsert.
type CurrencyRepo struct {
	mu       sync.RWMutex
	balances map[string]float64 // key = tenant|holder|currency
}

// NewCurrencyRepo returns an empty CurrencyRepo.
func NewCurrencyRepo() *CurrencyRepo {
	return &CurrencyRepo{balances: make(map[string]float64)}
}

func balanceKey(tenantID, holderGCID, currencyCode string) string {
	return tenantID + "|" + holderGCID + "|" + currencyCode
}

// CreditAuthor upserts (tenant_id, holder_gcid, currency) accumulating
// the balance.
func (r *CurrencyRepo) CreditAuthor(_ context.Context, tenantID, holderGCID string, c currency.Currency, amount float64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := balanceKey(tenantID, holderGCID, string(c))
	r.balances[key] += amount
	return nil
}

// GetBalance returns the current non-cash balance for (tenant, holder,
// currency); 0.0 when no row (a fresh author).
func (r *CurrencyRepo) GetBalance(_ context.Context, tenantID, holderGCID, currencyCode string) (float64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.balances[balanceKey(tenantID, holderGCID, currencyCode)], nil
}
