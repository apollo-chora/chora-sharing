// zz_harness_extra_a_test.go — supplementary stub helpers for the pg_test
// suite, owned by the "A" test lane (feed_cursor / grant / matchmaking /
// projection / atom_reuse / milestone / outbox coverage).
//
// All names are prefixed "a" / "zq" so they cannot collide with the shared
// harness (harness_test.go) or the sibling "B" lane's helpers.
//
// The key addition over harness_test.go is aExecFailQuerier: the shared
// stubQuerier's Exec always returns nil error, so repo-level Exec error
// branches (and the rls.ApplySession SET LOCAL failure branches) are
// unreachable with it. aExecFailQuerier fails Exec statements containing a
// configured substring, letting a single stub drive both error and happy
// paths.
package pg_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-common/tracing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
)

// aCtx returns a tenant-stamped context (mirrors reuseCtx in the reuse lane
// without depending on that file's internals).
func aCtx() context.Context {
	return tracing.WithTenantID(context.Background(), tenantID)
}

// aLastSQL returns the most recent SQL the querier recorded (RLS SET LOCAL
// lands first, so the repo's own statement is always the last entry).
func aLastSQL(q *stubQuerier) string {
	if len(q.sqls) == 0 {
		return ""
	}
	return q.sqls[len(q.sqls)-1]
}

// aFailedQueryErr is the sentinel returned by stub query/exec error paths.
const aFailedQueryErr = "pg: db down"

// aExecFailQuerier embeds stubQuerier but fails every Exec whose SQL
// contains failSubstr with execErr. Query/QueryRow behaviour (promoted from
// stubQuerier via rowFn/queryFn/execTagFn) is untouched, so a single stub
// can drive e.g. "SET LOCAL" failure AND a happy user query separately.
type aExecFailQuerier struct {
	*stubQuerier
	failSubstr string
	execErr    error
}

func (q *aExecFailQuerier) Exec(_ context.Context, sql string, args ...any) (rls.CommandTag, error) {
	q.sqls = append(q.sqls, sql)
	q.args = append(q.args, args)
	if strings.Contains(sql, q.failSubstr) {
		return rls.CommandTag{}, q.execErr
	}
	if q.execTagFn != nil {
		return q.execTagFn(sql), nil
	}
	return rls.CommandTag{RowsAffected: 1}, nil
}

// aTxRunner runs fn directly against ANY pg.Querier — used when the stub is
// an aExecFailQuerier (the shared stubTxRunner insists on *stubQuerier).
type aTxRunner struct{ q pg.Querier }

func (r *aTxRunner) RunInTx(ctx context.Context, fn func(ctx context.Context, q pg.Querier) error) error {
	return fn(ctx, r.q)
}

// aErrExec is the canned error used by aExecFailQuerier-driven tests.
var aErrExec = newStdErr(aFailedQueryErr)

// newStdErr sidesteps errors.New collisions across lanes.
func newStdErr(msg string) error {
	return &stdErrShim{msg: msg}
}

type stdErrShim struct{ msg string }

func (e *stdErrShim) Error() string { return e.msg }

// zqSetString writes a string value into dest[i] when that slot is a *string.
func zqSetString(dest []any, i int, v string) {
	if p, ok := dest[i].(*string); ok {
		*p = v
	}
}

// zqSetBytes writes raw bytes into dest[i] when that slot is a *[]byte.
func zqSetBytes(dest []any, i int, v []byte) {
	if p, ok := dest[i].(*[]byte); ok {
		*p = v
	}
}

// zqSetTime writes a time value into dest[i] when that slot is a *time.Time.
func zqSetTime(dest []any, i int, v time.Time) {
	if p, ok := dest[i].(*time.Time); ok {
		*p = v
	}
}

// zqSetTimePtr writes a *time.Time value into a **time.Time dest slot —
// nullable timestamps are scanned through a *time.Time variable, so the Scan
// target is &var of type **time.Time.
func zqSetTimePtr(dest []any, i int, v time.Time) {
	if p, ok := dest[i].(**time.Time); ok {
		t := v
		*p = &t
	}
}

// zqAssertErr verifies wantErr != "" implies err != nil.
func zqAssertErr(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an error, got nil")
	}
}