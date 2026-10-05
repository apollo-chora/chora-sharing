// pgx_querier_fakes_test.go — white-box coverage of the pgx-backed
// adapter surface (pgxQuerier / pgxRow / pgxRows) and the RunInTx
// BeginTx-error branch.
//
// pgx.Tx, pgx.Row and pgx.Rows are all interfaces, so the querier surface
// is exercisable with embedding fakes that override only the methods the
// adapter calls — no live Postgres needed. RunInTx's BeginTx error branch
// is reachable through a lazily-created *pgxpool.Pool pointed at an
// unreachable DSN (pgxpool.New does not dial); the fn-success/commit path
// needs a live connection and stays a documented plateau.
package main

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	sharingpg "github.com/apollo-chora/chora-sharing/internal/adapter/pg"
)

// fakePGXTx satisfies pgx.Tx by embedding the interface; only the three
// methods pgxQuerier touches are overridden.
type fakePGXTx struct {
	pgx.Tx
	execTag pgconn.CommandTag
	execErr error
	row     pgx.Row
	rows    pgx.Rows
	rowsErr error
}

func (f *fakePGXTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return f.execTag, f.execErr
}

func (f *fakePGXTx) QueryRow(context.Context, string, ...any) pgx.Row {
	return f.row
}

func (f *fakePGXTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return f.rows, f.rowsErr
}

// fakePGXRow satisfies pgx.Row (the Scan-only interface).
type fakePGXRow struct {
	scanErr error
	value   string
}

func (f *fakePGXRow) Scan(dest ...any) error {
	if f.scanErr != nil {
		return f.scanErr
	}
	if len(dest) > 0 {
		if s, ok := dest[0].(*string); ok {
			*s = f.value
		}
	}
	return nil
}

// fakePGXRows satisfies pgx.Rows by embedding the interface; the
// navigation methods pgxRows wraps are overridden. Close() has no return
// value in pgx v5 — the adapter's Close wraps it and always returns nil.
type fakePGXRows struct {
	pgx.Rows
	next    bool
	scanErr error
	err     error
	closed  bool
}

func (f *fakePGXRows) Next() bool { return f.next }

func (f *fakePGXRows) Scan(dest ...any) error { return f.scanErr }

func (f *fakePGXRows) Close() { f.closed = true }

func (f *fakePGXRows) Err() error { return f.err }

func TestPgxQuerier_Exec(t *testing.T) {
	tx := &fakePGXTx{execTag: pgconn.NewCommandTag("UPDATE 1")}
	q := &pgxQuerier{tx: tx}

	tag, err := q.Exec(context.Background(), "UPDATE posts SET body=$1", "x")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if tag.RowsAffected != 1 {
		t.Fatalf("RowsAffected = %d, want 1", tag.RowsAffected)
	}

	// Error path still returns the mirrored tag + the underlying error.
	tx.execErr = errors.New("exec boom")
	if _, err := q.Exec(context.Background(), "UPDATE posts SET body=$1", "x"); err == nil {
		t.Fatal("expected Exec error")
	}
}

func TestPgxQuerier_QueryRow(t *testing.T) {
	tx := &fakePGXTx{row: &fakePGXRow{value: "gcid-1"}}
	q := &pgxQuerier{tx: tx}

	var got string
	if err := q.QueryRow(context.Background(), "SELECT gcid").Scan(&got); err != nil {
		t.Fatalf("QueryRow.Scan: %v", err)
	}
	if got != "gcid-1" {
		t.Fatalf("scanned = %q, want gcid-1", got)
	}

	// Scan error propagates through pgxRow.
	tx.row = &fakePGXRow{scanErr: errors.New("scan boom")}
	var other string
	if err := q.QueryRow(context.Background(), "SELECT gcid").Scan(&other); err == nil {
		t.Fatal("expected Scan error")
	}
}

func TestPgxQuerier_Query(t *testing.T) {
	rows := &fakePGXRows{next: true}
	tx := &fakePGXTx{rows: rows}
	q := &pgxQuerier{tx: tx}

	got, err := q.Query(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	pr, ok := got.(*pgxRows)
	if !ok {
		t.Fatalf("Query returned %T, want *pgxRows", got)
	}
	if !pr.Next() {
		t.Fatal("Next returned false")
	}

	// Query error returns nil rows + err.
	tx.rowsErr = errors.New("query boom")
	if rows, err := q.Query(context.Background(), "SELECT 1"); err == nil || rows != nil {
		t.Fatalf("expected (nil, error), got (%v, %v)", rows, err)
	}
}

func TestPgxRows_Navigation(t *testing.T) {
	var scanned string
	inner := &fakePGXRows{next: true}
	r := &pgxRows{r: inner}

	if !r.Next() {
		t.Fatal("Next() = false, want true")
	}
	if err := r.Scan(&scanned); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if err := r.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !inner.closed {
		t.Fatal("inner Rows.Close not forwarded")
	}

	if (&pgxRows{r: &fakePGXRows{next: false}}).Next() {
		t.Fatal("Next() = true, want false")
	}
	if err := (&pgxRows{r: &fakePGXRows{err: errors.New("rows boom")}}).Err(); err == nil {
		t.Fatal("expected rows Err")
	}
	if err := (&pgxRows{r: &fakePGXRows{scanErr: errors.New("scan boom")}}).Scan(&scanned); err == nil {
		t.Fatal("expected rows Scan error")
	}
}

func TestPgxTxRunner_RunInTx_BeginTxError(t *testing.T) {
	// pgxpool.New performs no dial — the pool is created lazily against an
	// unreachable endpoint, so BeginTx fails fast with a connection error.
	pool, err := pgxpool.New(context.Background(),
		"postgres://chora:chora@127.0.0.1:1/chora_sharing?sslmode=disable&connect_timeout=2")
	if err != nil {
		t.Fatalf("pgxpool.New should be lazy, got %v", err)
	}
	defer pool.Close()

	r := newPgxTxRunner(pool)
	if r == nil {
		t.Fatal("newPgxTxRunner returned nil for a non-nil pool")
	}

	err = r.RunInTx(context.Background(), func(context.Context, sharingpg.Querier) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected BeginTx error against an unreachable database")
	}
}