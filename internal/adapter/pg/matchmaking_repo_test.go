//go:build integration

// matchmaking_repo_test.go — integration tests for the pgx-backed
// MatchmakingQueueRepo against a real Postgres instance.
//
// Run with:   go test -tags=integration ./internal/adapter/pg/...
// Requires:   CHORA_TEST_DSN env var pointing at a chora_sharing DB
//
//	with migrations 0038 applied.
//
// Per spec §8.1: tests Enqueue, Cancel (UpdateStatus), UpdateHeartbeat,
// FindByStatus, FindCandidates, ClaimMatch, RevertToFinding, GetByGCID and
// ExpireStaleForTenant against real Postgres + RLS.
package pg_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-common/tracing"

	sharingpg "github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	domainmm "github.com/apollo-chora/chora-sharing/internal/domain/matchmaking"
)

const (
	testTenantID = "01970000-0000-7000-a000-000000000001"
	testGCIDA    = "01970000-0000-7000-a000-0000000000aa"
	testGCIDB    = "01970000-0000-7000-a000-0000000000bb"
	testGCIDC    = "01970000-0000-7000-a000-0000000000cc"
	testDuelID   = "01970000-0000-7000-a000-0000000000dd"
)

func newTestSearcher(gcid string, now time.Time) domainmm.Searcher {
	return domainmm.Searcher{
		GCID:          gcid,
		TenantID:      testTenantID,
		Proficiency:   1200,
		InterestTags:  []string{"recursion", "inheritance"},
		EnteredAt:     now,
		ExpiresAt:     now.Add(10 * time.Minute),
		LastHeartbeat: now,
	}
}

func setupTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("CHORA_TEST_DSN")
	if dsn == "" {
		t.Skip("CHORA_TEST_DSN not set — skipping integration test")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func cleanupQueue(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	// The RLS policy evaluates current_setting('chora.tenant_id') — the
	// GUC must be set on the SAME connection that issues the DELETE.
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Logf("cleanup acquire warning: %v", err)
		return
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT set_config('chora.tenant_id', $1, false)`, testTenantID); err != nil {
		t.Logf("cleanup set_config warning: %v", err)
		return
	}
	if _, err := conn.Exec(ctx,
		`DELETE FROM matchmaking_queue WHERE tenant_id = $1`, testTenantID); err != nil {
		t.Logf("cleanup warning: %v", err)
	}
}

// poolTxRunner wraps a pgxpool.Pool to satisfy sharingpg.TxRunner.
type poolTxRunner struct{ pool *pgxpool.Pool }

func (t *poolTxRunner) RunInTx(ctx context.Context, fn func(ctx context.Context, q sharingpg.Querier) error) (err error) {
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

type pgxQuerier struct{ tx pgx.Tx }

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

var _ sharingpg.TxRunner = (*poolTxRunner)(nil)

func TestMatchmakingRepo_Integration_EnqueueAndFindByStatus(t *testing.T) {
	pool := setupTestPool(t)
	cleanupQueue(t, pool)
	defer cleanupQueue(t, pool)

	repo := sharingpg.NewMatchmakingQueueRepo(&poolTxRunner{pool: pool})
	ctx := tracing.WithTenantID(context.Background(), testTenantID)
	now := time.Now().UTC()

	s := newTestSearcher(testGCIDA, now)
	if err := repo.Enqueue(ctx, s); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	found, err := repo.FindByStatus(ctx, testTenantID, domainmm.StatusFinding)
	if err != nil {
		t.Fatalf("FindByStatus: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("expected 1 finding entry; got %d", len(found))
	}
	if found[0].GCID != testGCIDA {
		t.Errorf("GCID=%s want %s", found[0].GCID, testGCIDA)
	}
}

func TestMatchmakingRepo_Integration_UpdateStatus(t *testing.T) {
	pool := setupTestPool(t)
	cleanupQueue(t, pool)
	defer cleanupQueue(t, pool)

	repo := sharingpg.NewMatchmakingQueueRepo(&poolTxRunner{pool: pool})
	ctx := tracing.WithTenantID(context.Background(), testTenantID)
	now := time.Now().UTC()

	s := newTestSearcher(testGCIDA, now)
	if err := repo.Enqueue(ctx, s); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := repo.UpdateStatus(ctx, testTenantID, testGCIDA, domainmm.StatusCancelled, ""); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}

	found, _ := repo.FindByStatus(ctx, testTenantID, domainmm.StatusFinding)
	if len(found) != 0 {
		t.Errorf("expected 0 finding entries after cancel; got %d", len(found))
	}
}

func TestMatchmakingRepo_Integration_UpdateHeartbeat(t *testing.T) {
	pool := setupTestPool(t)
	cleanupQueue(t, pool)
	defer cleanupQueue(t, pool)

	repo := sharingpg.NewMatchmakingQueueRepo(&poolTxRunner{pool: pool})
	ctx := tracing.WithTenantID(context.Background(), testTenantID)
	now := time.Now().UTC()

	s := newTestSearcher(testGCIDA, now)
	_ = repo.Enqueue(ctx, s)

	if err := repo.UpdateHeartbeat(ctx, testTenantID, testGCIDA); err != nil {
		t.Fatalf("UpdateHeartbeat: %v", err)
	}

	found, _ := repo.FindByStatus(ctx, testTenantID, domainmm.StatusFinding)
	if len(found) != 1 {
		t.Fatalf("expected 1 finding entry; got %d", len(found))
	}
	if !found[0].LastHeartbeat.After(now) {
		t.Errorf("heartbeat not updated: last=%v now=%v", found[0].LastHeartbeat, now)
	}
}

func TestMatchmakingRepo_Integration_ExpireStaleForTenant(t *testing.T) {
	pool := setupTestPool(t)
	cleanupQueue(t, pool)
	defer cleanupQueue(t, pool)

	repo := sharingpg.NewMatchmakingQueueRepo(&poolTxRunner{pool: pool})
	ctx := tracing.WithTenantID(context.Background(), testTenantID)
	now := time.Now().UTC()

	stale := newTestSearcher(testGCIDA, now)
	stale.LastHeartbeat = now.Add(-2 * time.Minute)
	stale.ExpiresAt = now.Add(-1 * time.Minute)
	_ = repo.Enqueue(ctx, stale)

	fresh := newTestSearcher(testGCIDB, now)
	_ = repo.Enqueue(ctx, fresh)

	if err := repo.ExpireStaleForTenant(ctx, testTenantID); err != nil {
		t.Fatalf("ExpireStaleForTenant: %v", err)
	}

	finding, _ := repo.FindByStatus(ctx, testTenantID, domainmm.StatusFinding)
	if len(finding) != 1 || finding[0].GCID != testGCIDB {
		t.Errorf("expected only fresh searcher %s remaining; got %d entries", testGCIDB, len(finding))
	}
}

func TestMatchmakingRepo_Integration_FindCandidates(t *testing.T) {
	pool := setupTestPool(t)
	cleanupQueue(t, pool)
	defer cleanupQueue(t, pool)

	repo := sharingpg.NewMatchmakingQueueRepo(&poolTxRunner{pool: pool})
	ctx := tracing.WithTenantID(context.Background(), testTenantID)
	now := time.Now().UTC()

	fresh := newTestSearcher(testGCIDA, now)
	_ = repo.Enqueue(ctx, fresh)

	stale := newTestSearcher(testGCIDB, now)
	stale.LastHeartbeat = now.Add(-2 * time.Minute)
	_ = repo.Enqueue(ctx, stale)

	candidates, err := repo.FindCandidates(ctx, testTenantID)
	if err != nil {
		t.Fatalf("FindCandidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].GCID != testGCIDA {
		t.Fatalf("expected only fresh candidate %s; got %d entries", testGCIDA, len(candidates))
	}
	if candidates[0].Status != domainmm.StatusFinding {
		t.Errorf("candidate status=%s want finding", candidates[0].Status)
	}
}

func TestMatchmakingRepo_Integration_ClaimMatch(t *testing.T) {
	pool := setupTestPool(t)
	cleanupQueue(t, pool)
	defer cleanupQueue(t, pool)

	repo := sharingpg.NewMatchmakingQueueRepo(&poolTxRunner{pool: pool})
	ctx := tracing.WithTenantID(context.Background(), testTenantID)
	now := time.Now().UTC()

	_ = repo.Enqueue(ctx, newTestSearcher(testGCIDA, now))
	_ = repo.Enqueue(ctx, newTestSearcher(testGCIDB, now))

	// First claim wins.
	if err := repo.ClaimMatch(ctx, testTenantID, testGCIDA, testGCIDB, testDuelID); err != nil {
		t.Fatalf("ClaimMatch: %v", err)
	}

	// Second claim (another pod racing) loses — rows are no longer finding.
	if err := repo.ClaimMatch(ctx, testTenantID, testGCIDA, testGCIDB, testDuelID); !errors.Is(err, sharingpg.ErrNoMatchFound) {
		t.Fatalf("re-claim: got %v, want ErrNoMatchFound", err)
	}

	matched, err := repo.FindByStatus(ctx, testTenantID, domainmm.StatusMatched)
	if err != nil {
		t.Fatalf("FindByStatus(matched): %v", err)
	}
	if len(matched) != 2 {
		t.Fatalf("expected 2 matched entries; got %d", len(matched))
	}
	for _, m := range matched {
		if m.MatchedDuelID != testDuelID {
			t.Errorf("matched entry %s duel=%s want %s", m.GCID, m.MatchedDuelID, testDuelID)
		}
	}
}

func TestMatchmakingRepo_Integration_GetByGCID(t *testing.T) {
	pool := setupTestPool(t)
	cleanupQueue(t, pool)
	defer cleanupQueue(t, pool)

	repo := sharingpg.NewMatchmakingQueueRepo(&poolTxRunner{pool: pool})
	ctx := tracing.WithTenantID(context.Background(), testTenantID)
	now := time.Now().UTC()

	// Absent user → nil, nil.
	s, err := repo.GetByGCID(ctx, testTenantID, testGCIDA)
	if err != nil {
		t.Fatalf("GetByGCID absent: %v", err)
	}
	if s != nil {
		t.Fatalf("GetByGCID absent: got %+v, want nil", s)
	}

	// Finding entry.
	_ = repo.Enqueue(ctx, newTestSearcher(testGCIDA, now))
	s, err = repo.GetByGCID(ctx, testTenantID, testGCIDA)
	if err != nil || s == nil {
		t.Fatalf("GetByGCID finding: s=%v err=%v", s, err)
	}
	if s.Status != domainmm.StatusFinding {
		t.Errorf("status=%s want finding", s.Status)
	}

	// After claim: matched with the duel ID (the multi-pod pickup signal).
	_ = repo.Enqueue(ctx, newTestSearcher(testGCIDB, now))
	if err := repo.ClaimMatch(ctx, testTenantID, testGCIDA, testGCIDB, testDuelID); err != nil {
		t.Fatalf("ClaimMatch: %v", err)
	}
	s, err = repo.GetByGCID(ctx, testTenantID, testGCIDA)
	if err != nil || s == nil {
		t.Fatalf("GetByGCID matched: s=%v err=%v", s, err)
	}
	if s.Status != domainmm.StatusMatched {
		t.Errorf("status=%s want matched", s.Status)
	}
	if s.MatchedDuelID != testDuelID {
		t.Errorf("matched_duel_id=%s want %s", s.MatchedDuelID, testDuelID)
	}
}

func TestMatchmakingRepo_Integration_RevertToFinding(t *testing.T) {
	pool := setupTestPool(t)
	cleanupQueue(t, pool)
	defer cleanupQueue(t, pool)

	repo := sharingpg.NewMatchmakingQueueRepo(&poolTxRunner{pool: pool})
	ctx := tracing.WithTenantID(context.Background(), testTenantID)
	now := time.Now().UTC()

	_ = repo.Enqueue(ctx, newTestSearcher(testGCIDA, now))
	_ = repo.Enqueue(ctx, newTestSearcher(testGCIDB, now))
	if err := repo.ClaimMatch(ctx, testTenantID, testGCIDA, testGCIDB, testDuelID); err != nil {
		t.Fatalf("ClaimMatch: %v", err)
	}

	// Revert one side (duel creation failed) — row returns to finding.
	if err := repo.RevertToFinding(ctx, testTenantID, testGCIDA, testDuelID); err != nil {
		t.Fatalf("RevertToFinding: %v", err)
	}
	s, err := repo.GetByGCID(ctx, testTenantID, testGCIDA)
	if err != nil || s == nil {
		t.Fatalf("GetByGCID after revert: s=%v err=%v", s, err)
	}
	if s.Status != domainmm.StatusFinding || s.MatchedDuelID != "" {
		t.Errorf("after revert: status=%s duel=%q, want finding with no duel", s.Status, s.MatchedDuelID)
	}

	// Reverting again is a guarded no-op — the row already moved on.
	if err := repo.RevertToFinding(ctx, testTenantID, testGCIDA, testDuelID); !errors.Is(err, sharingpg.ErrNoMatchFound) {
		t.Fatalf("re-revert: got %v, want ErrNoMatchFound", err)
	}
}
