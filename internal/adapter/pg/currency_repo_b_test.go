// currency_repo_b_test.go — coverage tests for CurrencyRepo (second coverage
// agent): CreditAuthor upsert + guard clauses, GetBalance no-row → 0.0.
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/domain/currency"
)

func TestCurrencyRepo_NilTxRunner_ReturnsErrNotImplemented(t *testing.T) {
	t.Parallel()
	r := pg.NewCurrencyRepo(nil)
	if err := r.CreditAuthor(context.Background(), tenantID, authorGCID, currency.CurrencyReputation, 5); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("CreditAuthor: expected ErrNotImplemented; got %v", err)
	}
	if _, err := r.GetBalance(context.Background(), tenantID, authorGCID, "reputation"); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("GetBalance: expected ErrNotImplemented; got %v", err)
	}
}

func TestCurrencyRepo_CreditAuthor_NegativeAmountRejected(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewCurrencyRepo(&stubTxRunner{q: q})
	err := r.CreditAuthor(context.Background(), tenantID, authorGCID, currency.CurrencyCoins, -1)
	if !errors.Is(err, currency.ErrInvalidArgument) {
		t.Fatalf("expected currency.ErrInvalidArgument; got %v", err)
	}
	if len(q.sqls) != 0 {
		t.Fatalf("negative credit must issue NO SQL; got %v", q.sqls)
	}
}

func TestCurrencyRepo_CreditAuthor_UpsertsBalance(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewCurrencyRepo(&stubTxRunner{q: q})
	if err := r.CreditAuthor(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, currency.CurrencyReputation, 12.5); err != nil {
		t.Fatalf("CreditAuthor: %v", err)
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL") {
		t.Fatalf("first SQL must be SET LOCAL; got %q", q.sqls[0])
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "INSERT INTO currency_balances") || !strings.Contains(last, "ON CONFLICT (tenant_id, holder_gcid, currency)") {
		t.Fatalf("expected balance upsert; got %q", last)
	}
	args := q.args[len(q.args)-1]
	if len(args) != 4 {
		t.Fatalf("upsert binds tenant, holder, currency, amount; got %v", args)
	}
	if c, _ := args[2].(string); c != "reputation" {
		t.Fatalf("currency arg wrong; got %v", args[2])
	}
	if amt, _ := args[3].(float64); amt != 12.5 {
		t.Fatalf("amount arg wrong; got %v", args[3])
	}
}

func TestCurrencyRepo_CreditAuthor_ExecErrorPropagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerierB{
		execErrFn: func(sql string) error {
			if strings.Contains(sql, "SET LOCAL") {
				return nil
			}
			return errors.New("boom")
		},
	}
	r := pg.NewCurrencyRepo(&stubTxRunnerB{q: q})
	err := r.CreditAuthor(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, currency.CurrencyCoins, 1)
	if err == nil || !strings.Contains(err.Error(), "upsert currency balance") {
		t.Fatalf("expected wrapped upsert error; got %v", err)
	}
}

func TestCurrencyRepo_GetBalance_ScansAndDefaultsFreshPlayer(t *testing.T) {
	t.Parallel()

	t.Run("row present", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			rowFn: func(sql string, args ...any) pg.Row {
				return stubRow{scanFn: func(dest ...any) error {
					bSetInto(dest, 0, 42.0)
					return nil
				}}
			},
		}
		r := pg.NewCurrencyRepo(&stubTxRunner{q: q})
		bal, err := r.GetBalance(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "coins")
		if err != nil {
			t.Fatalf("GetBalance: %v", err)
		}
		if bal != 42.0 {
			t.Fatalf("expected balance 42.0; got %v", bal)
		}
		if !strings.Contains(q.sqls[len(q.sqls)-1], "SELECT balance FROM currency_balances") {
			t.Fatalf("expected SELECT balance; got %q", q.sqls[len(q.sqls)-1])
		}
	})

	t.Run("no row → 0.0", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			rowFn: func(sql string, args ...any) pg.Row {
				return stubRow{scanFn: func(dest ...any) error { return errors.New("no rows") }}
			},
		}
		r := pg.NewCurrencyRepo(&stubTxRunner{q: q})
		bal, err := r.GetBalance(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "coins")
		if err != nil || bal != 0.0 {
			t.Fatalf("expected 0.0 balance with nil err; got %v, %v", bal, err)
		}
	})
}
