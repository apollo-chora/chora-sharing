// currency_test.go — exercises the in-memory CurrencyRepo: accumulating
// upserts keyed by (tenant, holder, currency) and balance reads including
// the fresh-author 0.0 default.
package inmem_test

import (
	"context"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/currency"
)

func TestCurrencyRepo_CreditAndBalance(t *testing.T) {
	t.Parallel()
	repo := inmem.NewCurrencyRepo()
	ctx := context.Background()

	// Fresh author → 0.0.
	if bal, err := repo.GetBalance(ctx, "t-1", "g-1", string(currency.CurrencyCoins)); err != nil || bal != 0 {
		t.Fatalf("fresh balance: want 0, got %v err=%v", bal, err)
	}

	// Credits accumulate on (tenant, holder, currency).
	if err := repo.CreditAuthor(ctx, "t-1", "g-1", currency.CurrencyCoins, 10.5); err != nil {
		t.Fatalf("CreditAuthor: %v", err)
	}
	if err := repo.CreditAuthor(ctx, "t-1", "g-1", currency.CurrencyCoins, 2.5); err != nil {
		t.Fatalf("CreditAuthor: %v", err)
	}
	bal, err := repo.GetBalance(ctx, "t-1", "g-1", string(currency.CurrencyCoins))
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal != 13.0 {
		t.Fatalf("accumulated balance: want 13, got %v", bal)
	}

	// Currency, holder and tenant isolation.
	if err := repo.CreditAuthor(ctx, "t-1", "g-2", currency.CurrencyReputation, 5.0); err != nil {
		t.Fatalf("CreditAuthor: %v", err)
	}
	if bal, _ := repo.GetBalance(ctx, "t-1", "g-1", string(currency.CurrencyReputation)); bal != 0 {
		t.Fatalf("different currency: want 0, got %v", bal)
	}
	if bal, _ := repo.GetBalance(ctx, "t-2", "g-1", string(currency.CurrencyCoins)); bal != 0 {
		t.Fatalf("different tenant: want 0, got %v", bal)
	}
	if bal, _ := repo.GetBalance(ctx, "t-1", "g-2", string(currency.CurrencyReputation)); bal != 5.0 {
		t.Fatalf("g-2 reputation: want 5, got %v", bal)
	}
}