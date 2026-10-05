// pgx_txrunner_test.go - the nil-guard contracts of the pgx-backed
// TxRunner. The full RunInTx / Querier surface needs a live pgxpool, which
// is out of scope for unit tests; these pin the fail-soft paths callers
// rely on when the pool is absent.
package main

import (
	"context"
	"testing"

	sharingpg "github.com/apollo-chora/chora-sharing/internal/adapter/pg"
)

func TestNewPgxTxRunner_NilPool(t *testing.T) {
	if r := newPgxTxRunner(nil); r != nil {
		t.Fatal("expected nil runner for nil pool")
	}
}

func TestPgxTxRunner_RunInTx_NilReceiver(t *testing.T) {
	var r *pgxTxRunner
	err := r.RunInTx(context.Background(), func(context.Context, sharingpg.Querier) error {
		return nil
	})
	if err != sharingpg.ErrNotImplemented {
		t.Fatalf("expected ErrNotImplemented, got %v", err)
	}
}