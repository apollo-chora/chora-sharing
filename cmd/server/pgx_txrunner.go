// pgx_txrunner.go — production pgxpool-backed adapter for pg.TxRunner.
//
// The internal/adapter/pg package deliberately does NOT import pgx
// (see runtime.go — it defines a minimal Querier / Row / Rows / TxRunner
// shape so the SQL surface stays reviewable without dragging pgx into the
// adapter's import graph). The concrete pgx wiring lives HERE, in cmd/server,
// where bootstrap.go already imports pgxpool.
//
// Mirrors services/chora-delivery/cmd/server/pgx_txrunner.go exactly —
// the only delta is the pg import path (sharingpg vs deliverypg).
//
// Resilience-priority (`feedback_resilience_priority`):
//   - RunInTx opens a real pgx transaction, runs fn, commits on success and
//     rolls back on any error — so SET LOCAL chora.tenant_id (emitted by
//     rls.ApplySession inside the repo) is transaction-scoped and never
//     leaks across PgBouncer connection-pool siblings.
package main

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/apollo-chora/chora-common/rls"
	sharingpg "github.com/apollo-chora/chora-sharing/internal/adapter/pg"
)

// pgxTxRunner adapts a *pgxpool.Pool to sharingpg.TxRunner.
type pgxTxRunner struct {
	pool *pgxpool.Pool
}

// newPgxTxRunner constructs a pgxTxRunner around a connection pool. Returns
// nil when pool is nil so callers can fall back to in-memory adapters.
func newPgxTxRunner(pool *pgxpool.Pool) *pgxTxRunner {
	if pool == nil {
		return nil
	}
	return &pgxTxRunner{pool: pool}
}

// RunInTx opens a pgx transaction, invokes fn with a Querier wrapping the
// tx, and commits on success / rolls back on error.
func (t *pgxTxRunner) RunInTx(ctx context.Context, fn func(ctx context.Context, q sharingpg.Querier) error) (err error) {
	if t == nil || t.pool == nil {
		return sharingpg.ErrNotImplemented
	}
	tx, err := t.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
			return
		}
		err = tx.Commit(ctx)
	}()
	return fn(ctx, &pgxQuerier{tx: tx})
}

// pgxQuerier wraps a pgx.Tx so it satisfies sharingpg.Querier.
type pgxQuerier struct {
	tx pgx.Tx
}

func (q *pgxQuerier) Exec(ctx context.Context, sql string, args ...any) (rls.CommandTag, error) {
	tag, err := q.tx.Exec(ctx, sql, args...)
	return rls.CommandTag{RowsAffected: tag.RowsAffected()}, err
}

func (q *pgxQuerier) QueryRow(ctx context.Context, sql string, args ...any) sharingpg.Row {
	return &pgxRow{r: q.tx.QueryRow(ctx, sql, args...)}
}

func (q *pgxQuerier) Query(ctx context.Context, sql string, args ...any) (sharingpg.Rows, error) {
	rows, err := q.tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &pgxRows{r: rows}, nil
}

type pgxRow struct{ r pgx.Row }

func (r *pgxRow) Scan(dest ...any) error { return r.r.Scan(dest...) }

type pgxRows struct{ r pgx.Rows }

func (r *pgxRows) Next() bool             { return r.r.Next() }
func (r *pgxRows) Scan(dest ...any) error { return r.r.Scan(dest...) }
func (r *pgxRows) Close() error           { r.r.Close(); return nil }
func (r *pgxRows) Err() error             { return r.r.Err() }

// compile-time conformance.
var _ sharingpg.TxRunner = (*pgxTxRunner)(nil)
