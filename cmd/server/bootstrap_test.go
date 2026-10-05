// bootstrap_test.go — white-box coverage of the env-gated bootstrap
// helpers in bootstrap.go.
//
// The fail-loud FATAL branches (secret-manager init, pgx pool ping,
// outbox db.Ping) call log.Fatalf → os.Exit, which would terminate the
// whole test binary, so only the env-unset early returns are reachable
// deterministically; the deeper branches are documented plateaus.
package main

import (
	"context"
	"database/sql"
	"testing"
)

func TestBootstrapDBPool_EnvUnsetReturnsNil(t *testing.T) {
	t.Setenv("CHORA_DB_DSN", "")
	t.Setenv("CHORA_DB_DSN_SECRET_ID", "")
	t.Setenv("CHORA_DB_PROJECT", "")
	t.Setenv("CHORA_DB_REWRITE_FROM_PORT", "")
	t.Setenv("CHORA_DB_REWRITE_TO_PORT", "")

	pool, shutdown := bootstrapDBPool(context.Background())
	if pool != nil || shutdown != nil {
		t.Fatalf("expected (nil, nil) when CHORA_DB_DSN unset, got pool=%v closer-set=%v", pool, shutdown != nil)
	}
}

func TestBootstrapBus_EnvUnsetReturnsNil(t *testing.T) {
	t.Setenv("NATS_URL", "")
	bus, shutdown := bootstrapBus(context.Background())
	if bus != nil || shutdown != nil {
		t.Fatalf("expected (nil, nil) when NATS_URL unset, got bus=%v closer-set=%v", bus, shutdown != nil)
	}
}

func TestBootstrapBus_UnreachableURLReturnsNil(t *testing.T) {
	// An unreachable broker URL makes NewJetStream fail; the helper must
	// degrade to (nil, nil) rather than crash boot.
	t.Setenv("NATS_URL", "nats://127.0.0.1:1")
	bus, shutdown := bootstrapBus(context.Background())
	if bus != nil || shutdown != nil {
		t.Fatalf("expected (nil, nil) when the broker is unreachable, got bus=%v closer-set=%v", bus, shutdown != nil)
	}
}

func TestBootstrapOutboxDB_EnvUnsetReturnsNil(t *testing.T) {
	t.Setenv("CHORA_OUTBOX_DSN", "")
	t.Setenv("CHORA_OUTBOX_DSN_SECRET_ID", "")
	t.Setenv("CHORA_DB_PROJECT", "")

	db, shutdown := bootstrapOutboxDB(context.Background())
	if db != nil || shutdown != nil {
		t.Fatalf("expected (nil, nil) when CHORA_OUTBOX_DSN unset, got db=%v closer-set=%v", db, shutdown != nil)
	}
}

func TestOutboxWorkerID(t *testing.T) {
	t.Setenv("CHORA_OUTBOX_WORKER_ID", "")
	t.Setenv("HOSTNAME", "")
	if got := outboxWorkerID(); got != "chora-sharing-local" {
		t.Fatalf("no env: got %q, want chora-sharing-local", got)
	}

	t.Setenv("CHORA_OUTBOX_WORKER_ID", "worker-7")
	if got := outboxWorkerID(); got != "worker-7" {
		t.Fatalf("worker env: got %q, want worker-7", got)
	}

	t.Setenv("CHORA_OUTBOX_WORKER_ID", "")
	t.Setenv("HOSTNAME", "sharing-pod-ab12")
	if got := outboxWorkerID(); got != "sharing-pod-ab12" {
		t.Fatalf("hostname fallback: got %q, want sharing-pod-ab12", got)
	}
}

func TestSQLDBAdapter_ClosedDBExecAndQueryError(t *testing.T) {
	// sql.Open is lazy; a closed *sql.DB makes ExecContext/QueryContext
	// error deterministically without any network.
	db, err := sql.Open("pgx", "postgres://chora:chora@127.0.0.1:1/chora_sharing?sslmode=disable")
	if err != nil {
		t.Fatalf("sql.Open should be lazy, got %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}
	a := sqlDBAdapter{db: db}

	if _, err := a.ExecContext(context.Background(), "INSERT INTO t(c) VALUES (1)"); err == nil {
		t.Fatal("expected ExecContext error against a closed DB")
	}
	if _, err := a.QueryContext(context.Background(), "SELECT 1"); err == nil {
		t.Fatal("expected QueryContext error against a closed DB")
	}
}