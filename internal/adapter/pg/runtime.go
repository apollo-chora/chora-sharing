// runtime.go — minimal pgx-shaped contracts for the chora-sharing pg
// adapter package.
//
// Mirrors chora-delivery/internal/adapter/repo/pg/application.go's design:
// the production code stays pgx-import-free; the cmd/server bootstrap
// wires a thin pgx-bound TxRunner (under the integration build tag for
// tests, and via a small bridge in cmd/server for production).
//
// Per multi-tenant-rls + feedback_resilience_priority: every repository
// method MUST call rls.ApplySession on the supplied Querier BEFORE any
// user query. The TxRunner contract guarantees that ApplySession's
// SET LOCAL chora.tenant_id lands in transaction scope (so PgBouncer
// transaction-pooling does not leak the value across sibling requests).
package pg

import (
	"context"
	"errors"

	"github.com/apollo-chora/chora-common/rls"
)

// Querier is the minimal Exec + QueryRow + Query contract this package
// needs. Production wires a pgx.Tx bridge; unit tests inject a stub.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (rls.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
}

// Row is the minimal Scan surface used by the repos.
type Row interface {
	Scan(dest ...any) error
}

// Rows is the minimal multi-row iteration surface used by the repos.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
	Err() error
}

// TxRunner abstracts pool.BeginTx so chora-sharing's domain-facing pg
// code never imports pgx directly.
type TxRunner interface {
	RunInTx(ctx context.Context, fn func(ctx context.Context, q Querier) error) error
}

// ErrNotImplemented signals that the repo was constructed without a
// TxRunner (i.e. dev-fallback wired the in-memory adapter). The HTTP
// handler swap in cmd/server prevents this in practice but Save / Get
// guard against accidental misuse.
var ErrNotImplemented = errors.New("pg: pgx adapter not yet wired (M12)")

// qExecer adapts Querier to rls.Execer so rls.ApplySession can issue
// SET LOCAL chora.tenant_id on the same tx the repo writes through.
type qExecer struct{ q Querier }

func (q qExecer) Exec(ctx context.Context, sql string, args ...any) (rls.CommandTag, error) {
	return q.q.Exec(ctx, sql, args...)
}

// qToExecer is the named helper invoked by every repo method ahead of a
// user query.
func qToExecer(q Querier) rls.Execer { return qExecer{q: q} }
