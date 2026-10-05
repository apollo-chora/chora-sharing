package currency

import (
	"context"
	"errors"
	"testing"
)

// --- stubCurrencyRepo + stubManaLedger: in-memory doubles ---

type stubCurrencyRepo struct {
	balances map[string]float64 // key: tenant|holder|currency
}

func newStubCurrencyRepo() *stubCurrencyRepo {
	return &stubCurrencyRepo{balances: make(map[string]float64)}
}

func key(tenantID, holderGCID, currency string) string {
	return tenantID + "|" + holderGCID + "|" + currency
}

func (s *stubCurrencyRepo) CreditAuthor(_ context.Context, tenantID, holderGCID string, currency Currency, amount float64) error {
	k := key(tenantID, holderGCID, string(currency))
	s.balances[k] += amount
	return nil
}

func (s *stubCurrencyRepo) GetBalance(_ context.Context, tenantID, holderGCID, currency string) (float64, error) {
	return s.balances[key(tenantID, holderGCID, currency)], nil
}

type stubManaLedger struct {
	insufficient bool
	debits       map[string]float64 // granteeGCID -> total debited
}

func newStubManaLedger() *stubManaLedger {
	return &stubManaLedger{debits: make(map[string]float64)}
}

func (s *stubManaLedger) DebitReuser(_ context.Context, granteeGCID, _ string, amount float64, _ string) error {
	if s.insufficient {
		return ErrInsufficientMana
	}
	s.debits[granteeGCID] += amount
	return nil
}

// --- Currency tests ---

func TestCurrency_IsValid(t *testing.T) {
	valid := []Currency{CurrencyCoins, CurrencyReputation}
	for _, c := range valid {
		if !c.IsValid() {
			t.Errorf("expected %q to be valid", c)
		}
	}
	if Currency("xp").IsValid() {
		t.Error("XP should NOT be valid here — chora-consumption owns it")
	}
	if Currency("bogus").IsValid() {
		t.Error("expected bogus currency to be invalid")
	}
}

// --- CurrencyBalance.Validate tests ---

func TestCurrencyBalance_Validate_AcceptsValid(t *testing.T) {
	b := CurrencyBalance{
		TenantID:   "t",
		HolderGCID: "h",
		Currency:   CurrencyReputation,
		Balance:    100,
	}
	if err := b.Validate(); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestCurrencyBalance_Validate_AcceptsZeroBalance(t *testing.T) {
	b := CurrencyBalance{
		TenantID:   "t",
		HolderGCID: "h",
		Currency:   CurrencyCoins,
		Balance:    0,
	}
	if err := b.Validate(); err != nil {
		t.Fatalf("zero balance should be valid, got %v", err)
	}
}

func TestCurrencyBalance_Validate_RejectsMissingFields(t *testing.T) {
	cases := []struct {
		name   string
		tenant string
		holder string
		cur    Currency
	}{
		{"missing tenant", "", "h", CurrencyCoins},
		{"missing holder", "t", "", CurrencyCoins},
		{"invalid currency", "t", "h", Currency("xp")},
		{"bogus currency", "t", "h", Currency("bogus")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := CurrencyBalance{TenantID: tc.tenant, HolderGCID: tc.holder, Currency: tc.cur, Balance: 10}
			if !errors.Is(b.Validate(), ErrInvalidArgument) {
				t.Fatalf("%s: expected ErrInvalidArgument", tc.name)
			}
		})
	}
}

func TestCurrencyBalance_Validate_RejectsNegativeBalance(t *testing.T) {
	b := CurrencyBalance{
		TenantID:   "t",
		HolderGCID: "h",
		Currency:   CurrencyReputation,
		Balance:    -1,
	}
	if !errors.Is(b.Validate(), ErrInvalidArgument) {
		t.Errorf("negative balance: expected ErrInvalidArgument, got %v", b.Validate())
	}
}

// --- CurrencyRepo port tests ---

func TestCurrencyRepo_CreditAuthorAccumulates(t *testing.T) {
	repo := newStubCurrencyRepo()
	_ = repo.CreditAuthor(context.Background(), "t", "h", CurrencyReputation, 50)
	_ = repo.CreditAuthor(context.Background(), "t", "h", CurrencyReputation, 30)
	bal, err := repo.GetBalance(context.Background(), "t", "h", "reputation")
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal != 80 {
		t.Errorf("expected accumulated balance 80, got %v", bal)
	}
}

func TestCurrencyRepo_GetBalanceZeroForNewHolder(t *testing.T) {
	repo := newStubCurrencyRepo()
	bal, err := repo.GetBalance(context.Background(), "t", "new-holder", "coins")
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal != 0 {
		t.Errorf("expected 0 for new holder, got %v", bal)
	}
}

func TestCurrencyRepo_SeparatesCurrencies(t *testing.T) {
	repo := newStubCurrencyRepo()
	_ = repo.CreditAuthor(context.Background(), "t", "h", CurrencyCoins, 100)
	_ = repo.CreditAuthor(context.Background(), "t", "h", CurrencyReputation, 50)
	coins, _ := repo.GetBalance(context.Background(), "t", "h", "coins")
	rep, _ := repo.GetBalance(context.Background(), "t", "h", "reputation")
	if coins != 100 {
		t.Errorf("expected coins 100, got %v", coins)
	}
	if rep != 50 {
		t.Errorf("expected reputation 50, got %v", rep)
	}
}

// --- ManaLedger port tests ---

func TestManaLedger_DebitReuserSuccess(t *testing.T) {
	ledger := newStubManaLedger()
	if err := ledger.DebitReuser(context.Background(), "gcid-1", "tenant-1", 10, "atom_royalty"); err != nil {
		t.Fatalf("DebitReuser: %v", err)
	}
	if ledger.debits["gcid-1"] != 10 {
		t.Errorf("expected debited 10, got %v", ledger.debits["gcid-1"])
	}
}

func TestManaLedger_DebitReuserInsufficientMana(t *testing.T) {
	ledger := newStubManaLedger()
	ledger.insufficient = true
	err := ledger.DebitReuser(context.Background(), "gcid-1", "tenant-1", 10, "atom_royalty")
	if !errors.Is(err, ErrInsufficientMana) {
		t.Errorf("expected ErrInsufficientMana, got %v", err)
	}
}

func TestManaLedger_AccumulatesAcrossCalls(t *testing.T) {
	ledger := newStubManaLedger()
	_ = ledger.DebitReuser(context.Background(), "gcid-1", "tenant-1", 5, "atom_royalty")
	_ = ledger.DebitReuser(context.Background(), "gcid-1", "tenant-1", 7, "atom_royalty")
	if ledger.debits["gcid-1"] != 12 {
		t.Errorf("expected accumulated debit 12, got %v", ledger.debits["gcid-1"])
	}
}

