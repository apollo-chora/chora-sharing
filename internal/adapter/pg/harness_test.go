// harness_test.go — the shared stub Querier / TxRunner harness for the
// chora-sharing pg adapter unit tests (pg_test package).
//
// atom_projection_test.go has referenced this harness since T011 ("defined in
// post_test.go") but that file never landed — the pg test package did NOT
// compile on main (`undefined: authorGCID`) until this file restored it
// (found during ADR-229 WS-0 / CHO-2102).
//
// The stubs record every SQL + arg list so tests can assert (a) the
// rls.ApplySession SET LOCAL ordering, (b) the exact SQL template used,
// (c) bound args — without a live Postgres. Live RLS behaviour is verified
// against the deployed DB via the PREPARE-smoke lane
// (feedback_pg_prepare_smoke_over_exec_stubs).
package pg_test

import (
	"context"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
)

const (
	tenantID   = "01970000-0000-7000-a000-000000000001"
	authorGCID = "01970000-0000-7000-a000-0000000000aa"
)

// stubQuerier records SQLs + args. Exec returns RowsAffected=1 unless
// execTagFn overrides; QueryRow delegates to rowFn (or a zero-scan row);
// Query delegates to queryFn (or empty rows).
type stubQuerier struct {
	sqls []string
	args [][]any

	execTagFn func(sql string) rls.CommandTag
	rowFn     func(sql string, args ...any) pg.Row
	queryFn   func(sql string, args ...any) (pg.Rows, error)
}

func (q *stubQuerier) Exec(_ context.Context, sql string, args ...any) (rls.CommandTag, error) {
	q.sqls = append(q.sqls, sql)
	q.args = append(q.args, args)
	if q.execTagFn != nil {
		return q.execTagFn(sql), nil
	}
	return rls.CommandTag{RowsAffected: 1}, nil
}

func (q *stubQuerier) QueryRow(_ context.Context, sql string, args ...any) pg.Row {
	q.sqls = append(q.sqls, sql)
	q.args = append(q.args, args)
	if q.rowFn != nil {
		return q.rowFn(sql, args...)
	}
	return stubRow{}
}

func (q *stubQuerier) Query(_ context.Context, sql string, args ...any) (pg.Rows, error) {
	q.sqls = append(q.sqls, sql)
	q.args = append(q.args, args)
	if q.queryFn != nil {
		return q.queryFn(sql, args...)
	}
	return &stubRows{}, nil
}

// stubRow scans via scanFn (nil scanFn = leave dests zero, return nil).
type stubRow struct{ scanFn func(dest ...any) error }

func (r stubRow) Scan(dest ...any) error {
	if r.scanFn != nil {
		return r.scanFn(dest...)
	}
	return nil
}

// stubRows iterates rows of pre-baked scan functions.
type stubRows struct {
	scans []func(dest ...any) error
	i     int
	err   error
}

func (r *stubRows) Next() bool { return r.i < len(r.scans) }

func (r *stubRows) Scan(dest ...any) error {
	fn := r.scans[r.i]
	r.i++
	if fn != nil {
		return fn(dest...)
	}
	return nil
}

func (r *stubRows) Close() error { return nil }
func (r *stubRows) Err() error   { return r.err }

// stubTxRunner runs fn directly against the stub querier (no real tx).
type stubTxRunner struct{ q *stubQuerier }

func (r *stubTxRunner) RunInTx(ctx context.Context, fn func(ctx context.Context, q pg.Querier) error) error {
	return fn(ctx, r.q)
}
