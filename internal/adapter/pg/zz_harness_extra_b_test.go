// zz_harness_extra_b_test.go — B-side extras on top of the shared harness
// (harness_test.go), used by the second coverage agent's tests only.
//
// stubQuerierB is a copy of stubQuerier with ONE addition: execErrFn lets
// tests inject an Exec error (the shared stub never fails Exec), so Exec
// error branches in the repos can be exercised. Everything else mirrors the
// shared stub (sqls/args recording, execTagFn / rowFn / queryFn).
//
// bSetInto is a small scan-writer that sets dest[idx] from a typed value,
// so stub scan functions can feed controlled rows tersely. bPtr creates a
// pointer to v (for nullable-column dests).
package pg_test

import (
	"context"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/jackc/pgx/v5"
)

// noRowsB returns pgx.ErrNoRows so repos can exercise their ErrNoRows
// mapping (fresh-player / not-found paths) without a real driver.
func noRowsB() error { return pgx.ErrNoRows }

// newRelRepoB is the B-side analog of the shared newRelRepo helper: builds a
// RelationshipRepo over whichever stub querier variant the caller supplies
// (stubQuerier or stubQuerierB).
func newRelRepoB(q any) *pg.RelationshipRepo {
	opts := pg.RelationshipRepoOptions{
		SourceProject: "chora-489812",
		SourceService: "chora-sharing",
		Now:           fixedNow,
	}
	switch t := q.(type) {
	case *stubQuerier:
		return pg.NewRelationshipRepo(&stubTxRunner{q: t}, opts)
	case *stubQuerierB:
		return pg.NewRelationshipRepo(&stubTxRunnerB{q: t}, opts)
	}
	return nil
}

// stubQuerierB behaves like stubQuerier except it can fail Exec via
// execErrFn (checked BEFORE execTagFn). A nil execErrFn never fails.
type stubQuerierB struct {
	sqls []string
	args [][]any

	execErrFn func(sql string) error
	execTagFn func(sql string) rls.CommandTag
	rowFn     func(sql string, args ...any) pg.Row
	queryFn   func(sql string, args ...any) (pg.Rows, error)
}

func (q *stubQuerierB) Exec(_ context.Context, sql string, args ...any) (rls.CommandTag, error) {
	q.sqls = append(q.sqls, sql)
	q.args = append(q.args, args)
	if q.execErrFn != nil {
		if err := q.execErrFn(sql); err != nil {
			return rls.CommandTag{}, err
		}
	}
	if q.execTagFn != nil {
		return q.execTagFn(sql), nil
	}
	return rls.CommandTag{RowsAffected: 1}, nil
}

func (q *stubQuerierB) QueryRow(_ context.Context, sql string, args ...any) pg.Row {
	q.sqls = append(q.sqls, sql)
	q.args = append(q.args, args)
	if q.rowFn != nil {
		return q.rowFn(sql, args...)
	}
	return stubRow{}
}

func (q *stubQuerierB) Query(_ context.Context, sql string, args ...any) (pg.Rows, error) {
	q.sqls = append(q.sqls, sql)
	q.args = append(q.args, args)
	if q.queryFn != nil {
		return q.queryFn(sql, args...)
	}
	return &stubRows{}, nil
}

// stubTxRunnerB is the stubTxRunner analog for stubQuerierB: runs fn directly
// against the underlying querier (no real transaction).
type stubTxRunnerB struct{ q *stubQuerierB }

func (r *stubTxRunnerB) RunInTx(ctx context.Context, fn func(ctx context.Context, q pg.Querier) error) error {
	return fn(ctx, r.q)
}

// bPtr returns a pointer to v (for nullable destination cells).
func bPtr[T any](v T) *T { return &v }

// bSetInto writes v into dest[idx] when the destination pointer type matches
// the value's dynamic type. Unknown/mismatched combos are ignored so the same
// helper is safe across differently-shaped SELECT lists.
func bSetInto(dest []any, idx int, v any) {
	if idx < 0 || idx >= len(dest) {
		return
	}
	switch val := v.(type) {
	case string:
		if p, ok := dest[idx].(*string); ok {
			*p = val
		}
	case *string:
		if p, ok := dest[idx].(**string); ok {
			*p = val
		}
	case int:
		if p, ok := dest[idx].(*int); ok {
			*p = val
		}
	case *int:
		if p, ok := dest[idx].(**int); ok {
			*p = val
		}
	case int64:
		if p, ok := dest[idx].(*int64); ok {
			*p = val
		}
	case *int64:
		if p, ok := dest[idx].(**int64); ok {
			*p = val
		}
	case bool:
		if p, ok := dest[idx].(*bool); ok {
			*p = val
		}
	case *bool:
		if p, ok := dest[idx].(**bool); ok {
			*p = val
		}
	case time.Time:
		if p, ok := dest[idx].(*time.Time); ok {
			*p = val
		}
	case *time.Time:
		if p, ok := dest[idx].(**time.Time); ok {
			*p = val
		}
	case float64:
		if p, ok := dest[idx].(*float64); ok {
			*p = val
		}
	case []string:
		if p, ok := dest[idx].(*[]string); ok {
			*p = val
		}
	case []byte:
		if p, ok := dest[idx].(*[]byte); ok {
			*p = val
		}
	}
}
