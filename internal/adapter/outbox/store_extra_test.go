// Package outbox_test — Store branch-coverage tests.
//
// Drives the residual Store branches left uncovered by store_test.go:
// not-found errors on the InMemoryStore mutators, PostgresStore
// ExecContext/QueryContext/Scan/Err failure branches, the nullableUUID
// empty-GCID branch and the truncate(long) branch — via fake SQLDB surfaces
// (seqDB + the rows fakes below). No production code touched.
package outbox_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/adapter/outbox"
)

// -----------------------------------------------------------------------------
// InMemoryStore not-found error branches
// -----------------------------------------------------------------------------

func TestInMemoryStore_MarkPublished_MissingRow_ReturnsError(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	if err := store.MarkPublished(context.Background(), "no-such-row"); err == nil {
		t.Errorf("MarkPublished(missing) err = nil; want not-found error")
	}
}

func TestInMemoryStore_MarkFailed_MissingRow_ReturnsError(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	if err := store.MarkFailed(context.Background(), "no-such-row", "boom"); err == nil {
		t.Errorf("MarkFailed(missing) err = nil; want not-found error")
	}
}

func TestInMemoryStore_Deadletter_MissingRow_ReturnsError(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	if err := store.Deadletter(context.Background(), "no-such-row", "fatal", 1); err == nil {
		t.Errorf("Deadletter(missing) err = nil; want not-found error")
	}
}

// -----------------------------------------------------------------------------
// PostgresStore error branches — scripted fake SQLDB surface
// -----------------------------------------------------------------------------

// seqDB is a scripted database/sql surface: every ExecContext is recorded,
// the failAtExec-th (1-based) call returns execErr (or none), and
// QueryContext returns queryErr or the configured rows.
type seqDB struct {
	execStatements []string
	execArgs       [][]any
	failAtExec     int    // 0 = never fail
	execErr        error  // error returned on the failAtExec-th ExecContext

	queryStatement string
	queryErr       error
	rows           outbox.SQLRows
}

func (d *seqDB) ExecContext(_ context.Context, q string, args ...any) (sql.Result, error) {
	d.execStatements = append(d.execStatements, q)
	cp := append([]any(nil), args...)
	d.execArgs = append(d.execArgs, cp)
	if d.failAtExec == len(d.execStatements) {
		return nil, d.execErr
	}
	return nil, nil
}

func (d *seqDB) QueryContext(_ context.Context, q string, args ...any) (outbox.SQLRows, error) {
	d.queryStatement = q
	if d.queryErr != nil {
		return nil, d.queryErr
	}
	if d.rows != nil {
		return d.rows, nil
	}
	return noRows{}, nil
}

// noRows is an empty SQLRows result.
type noRows struct{}

func (noRows) Next() bool          { return false }
func (noRows) Scan(_ ...any) error { return nil }
func (noRows) Close() error        { return nil }
func (noRows) Err() error          { return nil }

// scanFailRows yields one row whose Scan fails.
type scanFailRows struct{}

func (scanFailRows) Next() bool          { return true }
func (scanFailRows) Scan(_ ...any) error { return errors.New("scan boom") }
func (scanFailRows) Close() error        { return nil }
func (scanFailRows) Err() error          { return nil }

// envFailRows yields one row with an envelope column that is not JSON.
type envFailRows struct{ once bool }

func (r *envFailRows) Next() bool {
	if !r.once {
		r.once = true
		return true
	}
	return false
}
func (r *envFailRows) Scan(dest ...any) error {
	*dest[0].(*string) = "id-1"
	*dest[1].(*string) = "tenant"
	*dest[2].(*string) = ""
	*dest[3].(*string) = "post"
	*dest[4].(*string) = "id-1"
	*dest[5].(*string) = "sharing.post.created"
	*dest[6].(*string) = "chora.sharing.post.created.v1"
	*dest[7].(*[]byte) = []byte(`{}`)
	*dest[8].(*string) = "{not-json}!"
	*dest[9].(*string) = "idem-1"
	*dest[10].(*int) = 0
	*dest[11].(*time.Time) = time.Now().UTC()
	return nil
}
func (r *envFailRows) Close() error { return nil }
func (r *envFailRows) Err() error   { return nil }

// iterFailRows yields one well-formed row then fails rows.Err().
type iterFailRows struct{ once bool }

func (r *iterFailRows) Next() bool {
	if !r.once {
		r.once = true
		return true
	}
	return false
}
func (r *iterFailRows) Scan(dest ...any) error {
	*dest[0].(*string) = "id-1"
	*dest[1].(*string) = "tenant"
	*dest[2].(*string) = ""
	*dest[3].(*string) = "post"
	*dest[4].(*string) = "id-1"
	*dest[5].(*string) = "sharing.post.created"
	*dest[6].(*string) = "chora.sharing.post.created.v1"
	*dest[7].(*[]byte) = []byte(`{}`)
	*dest[8].(*string) = `{}`
	*dest[9].(*string) = "idem-1"
	*dest[10].(*int) = 0
	*dest[11].(*time.Time) = time.Now().UTC()
	return nil
}
func (r *iterFailRows) Close() error { return nil }
func (r *iterFailRows) Err() error   { return errors.New("iteration boom") }

func TestPostgresStore_Insert_GenericExecError(t *testing.T) {
	t.Parallel()
	db := &seqDB{execErr: errors.New("connection refused"), failAtExec: 1}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	r := newRow("X", "00000000-0000-0000-0000-000000000aaa", time.Now().UTC())
	// Empty GCID exercises the nullableUUID empty branch (NULL for system events).
	r.GCID = ""
	err := store.Insert(context.Background(), r)
	if err == nil {
		t.Fatalf("Insert(exec err) = nil; want wrapped error")
	}
	if !strings.Contains(err.Error(), "outbox: Insert") {
		t.Errorf("Insert err = %v; want 'outbox: Insert' wrapper", err)
	}
	if errors.Is(err, outbox.ErrDuplicateIdempotencyKey) {
		t.Errorf("Insert err = %v; must NOT be ErrDuplicateIdempotencyKey", err)
	}
}

func TestPostgresStore_FetchPending_QueryError(t *testing.T) {
	t.Parallel()
	db := &seqDB{queryErr: errors.New("db down")}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	_, err := store.FetchPending(context.Background(), 10)
	if err == nil || !strings.Contains(err.Error(), "FetchPending") {
		t.Errorf("FetchPending(query err) = %v; want wrapped 'FetchPending' error", err)
	}
}

func TestPostgresStore_FetchPending_ScanError(t *testing.T) {
	t.Parallel()
	db := &seqDB{rows: scanFailRows{}}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	_, err := store.FetchPending(context.Background(), 10)
	if err == nil || !strings.Contains(err.Error(), "scan") {
		t.Errorf("FetchPending(scan err) = %v; want 'scan' error", err)
	}
}

func TestPostgresStore_FetchPending_EnvelopeUnmarshalError(t *testing.T) {
	t.Parallel()
	db := &seqDB{rows: &envFailRows{}}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	_, err := store.FetchPending(context.Background(), 10)
	if err == nil || !strings.Contains(err.Error(), "envelope") {
		t.Errorf("FetchPending(envelope err) = %v; want 'envelope' error", err)
	}
}

func TestPostgresStore_FetchPending_IterationError(t *testing.T) {
	t.Parallel()
	db := &seqDB{rows: &iterFailRows{}}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	_, err := store.FetchPending(context.Background(), 10)
	if err == nil || !strings.Contains(err.Error(), "iter") {
		t.Errorf("FetchPending(iter err) = %v; want 'iter' error", err)
	}
}

func TestPostgresStore_MarkPublished_ExecError(t *testing.T) {
	t.Parallel()
	db := &seqDB{execErr: errors.New("update boom"), failAtExec: 1}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	err := store.MarkPublished(context.Background(), "row-X")
	if err == nil || !strings.Contains(err.Error(), "MarkPublished") {
		t.Errorf("MarkPublished(exec err) = %v; want 'MarkPublished' error", err)
	}
}

func TestPostgresStore_MarkFailed_ExecError_AndTruncatesLongMessage(t *testing.T) {
	t.Parallel()
	db := &seqDB{execErr: errors.New("update boom"), failAtExec: 1}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	longMsg := strings.Repeat("x", 1500)
	err := store.MarkFailed(context.Background(), "row-Y", longMsg)
	if err == nil || !strings.Contains(err.Error(), "MarkFailed") {
		t.Errorf("MarkFailed(exec err) = %v; want 'MarkFailed' error", err)
	}
	if len(db.execArgs) != 1 {
		t.Fatalf("Exec calls = %d; want 1", len(db.execArgs))
	}
	got, ok := db.execArgs[0][1].(string)
	if !ok || len(got) != 1000 {
		t.Errorf("MarkFailed last_error truncated to %d chars; want 1000", len(got))
	}
}

func TestPostgresStore_Deadletter_InsertError(t *testing.T) {
	t.Parallel()
	db := &seqDB{execErr: errors.New("insert boom"), failAtExec: 1}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	err := store.Deadletter(context.Background(), "row-Z", "fatal", 5)
	if err == nil || !strings.Contains(err.Error(), "Deadletter insert") {
		t.Errorf("Deadletter(insert err) = %v; want 'Deadletter insert' error", err)
	}
}

func TestPostgresStore_Deadletter_UpdateError(t *testing.T) {
	t.Parallel()
	db := &seqDB{execErr: errors.New("update boom"), failAtExec: 2}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	err := store.Deadletter(context.Background(), "row-Z", "fatal", 5)
	if err == nil || !strings.Contains(err.Error(), "Deadletter update") {
		t.Errorf("Deadletter(update err) = %v; want 'Deadletter update' error", err)
	}
	if len(db.execStatements) != 2 {
		t.Errorf("Exec calls = %d; want 2 (insert succeeded, update failed)", len(db.execStatements))
	}
}