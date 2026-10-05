// matchmaking_repo.go — pgx-backed MatchmakingQueueRepo.
//
// Persists matchmaking queue entries to the matchmaking_queue table AND drives
// multi-pod-safe matching via optimistic concurrency: FindCandidates is a
// plain SELECT (no row locks held), and ClaimMatch's atomic
// UPDATE ... WHERE status='finding' is the concurrency guard. If two pods try
// the same pair, only one succeeds; the loser gets ErrNoMatchFound and moves
// to the next pair.
//
// The in-memory pool has been retired; this repo is the sole source of truth
// for finding searchers and claiming matches atomically. The partial unique
// index on (tenant_id, gcid) WHERE status='finding' guarantees one active
// search per user.
//
// RLS constraint: each transaction sets ONE chora.tenant_id via SET LOCAL, so
// every method is strictly per-tenant — the caller MUST supply a context
// carrying the tenant (tracing.WithTenantID); rls.ApplySession fails loud
// with ErrNoTenantContext otherwise. Cross-tenant fan-out (e.g. expiring
// stale rows for every tenant) is the matchmaker's job: it iterates its
// active-tenant hint set and calls the per-tenant ExpireStaleForTenant.
//
// Schema: migrations/0038_matchmaking_queue.up.sql.
package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/apollo-chora/chora-common/rls"
	domainmm "github.com/apollo-chora/chora-sharing/internal/domain/matchmaking"
)

// ErrNoMatchFound signals that ClaimMatch / RevertToFinding could not find
// the expected rows in the guarded state — they were already claimed by
// another pod, cancelled, or expired. This is a soft error: the matchmaker
// should skip and retry on the next tick.
var ErrNoMatchFound = errors.New("pg: matchmaking row no longer in expected state")

// queueColumns is the canonical column list every row-scanning query shares.
// Keep scanQueueRow in sync.
const queueColumns = `id, tenant_id, gcid, status, proficiency, interest_tags,
       entered_at, expires_at, last_heartbeat, matched_duel_id, question_count, category,
       mode, blitz_variant`

const (
	sqlMatchmakingQueueInsert = `
INSERT INTO matchmaking_queue
    (tenant_id, gcid, status, proficiency, interest_tags, entered_at, expires_at, last_heartbeat, question_count, category, mode, blitz_variant)
VALUES ($1, $2, 'finding', $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (tenant_id, gcid) WHERE status = 'finding'
DO UPDATE SET
    proficiency = EXCLUDED.proficiency,
    interest_tags = EXCLUDED.interest_tags,
    expires_at = EXCLUDED.expires_at,
    last_heartbeat = EXCLUDED.last_heartbeat,
    question_count = EXCLUDED.question_count,
    category = EXCLUDED.category,
    mode = EXCLUDED.mode,
    blitz_variant = EXCLUDED.blitz_variant,
    updated_at = now()`

	sqlMatchmakingQueueUpdateStatus = `
UPDATE matchmaking_queue
SET status = $3, matched_duel_id = $4, updated_at = now()
WHERE tenant_id = $1 AND gcid = $2 AND status = 'finding'`

	sqlMatchmakingQueueUpdateHeartbeat = `
UPDATE matchmaking_queue
SET last_heartbeat = now(), updated_at = now()
WHERE tenant_id = $1 AND gcid = $2 AND status = 'finding'`

	sqlMatchmakingQueueFindByStatus = `
SELECT ` + queueColumns + `
FROM matchmaking_queue
WHERE tenant_id = $1 AND status = $2
ORDER BY entered_at ASC`

	// ExpireStaleForTenant marks one tenant's finding entries with stale
	// heartbeats or past expiry as abandoned. Per-tenant by design: RLS
	// forbids cross-tenant writes, so the matchmaker's sweeper calls this
	// once per active tenant.
	sqlMatchmakingQueueExpireStaleForTenant = `
UPDATE matchmaking_queue
SET status = 'abandoned', updated_at = now()
WHERE tenant_id = $1
  AND status = 'finding'
  AND (last_heartbeat < now() - INTERVAL '30 seconds'
       OR expires_at < now())`

	// FindCandidates returns valid (non-expired, non-stale) finding
	// searchers for a tenant. Plain SELECT — no FOR UPDATE locks are
	// held; ClaimMatch's atomic UPDATE ... WHERE status='finding' is the
	// concurrency guard (optimistic: two pods may try the same pair,
	// only one succeeds; the loser gets ErrNoMatchFound and moves on).
	sqlMatchmakingQueueFindCandidates = `
SELECT ` + queueColumns + `
FROM matchmaking_queue
WHERE tenant_id = $1 AND status = 'finding'
  AND last_heartbeat >= now() - INTERVAL '30 seconds'
  AND expires_at > now()
ORDER BY entered_at ASC`

	// ClaimMatch atomically transitions two finding rows to matched.
	// The WHERE status='finding' guards against races: if another pod
	// already claimed one of the rows, rows_affected < 2 and the caller
	// MUST treat the claim as lost (ErrNoMatchFound).
	sqlMatchmakingQueueClaimMatch = `
UPDATE matchmaking_queue
SET status = 'matched', matched_duel_id = $4, updated_at = now()
WHERE tenant_id = $1 AND gcid IN ($2, $3) AND status = 'finding'`

	// RevertToFinding is the compensating action for a failed match: it
	// returns ONE claimed row to finding, but ONLY while the row is still
	// in the exact state the claim left it (status='matched' with this
	// duel ID). The guard prevents clobbering a row that has since been
	// cancelled, re-claimed by another pod, or otherwise moved on.
	sqlMatchmakingQueueRevertToFinding = `
UPDATE matchmaking_queue
SET status = 'finding', matched_duel_id = NULL, updated_at = now()
WHERE tenant_id = $1 AND gcid = $2 AND status = 'matched' AND matched_duel_id = $3`

	// GetByGCID fetches the most recent queue entry for a (tenant, gcid)
	// pair regardless of status. The status endpoint and heartbeat path
	// use it to report finding/matched/cancelled state — including the
	// matched_duel_id written by ClaimMatch, which is the durable
	// multi-pod match signal.
	sqlMatchmakingQueueGetByGCID = `
SELECT ` + queueColumns + `
FROM matchmaking_queue
WHERE tenant_id = $1 AND gcid = $2
ORDER BY updated_at DESC
LIMIT 1`

	// CountFinding returns the number of active finding entries for a tenant.
	sqlMatchmakingQueueCountFinding = `
SELECT COUNT(*) FROM matchmaking_queue
WHERE tenant_id = $1 AND status = 'finding'
  AND last_heartbeat >= now() - INTERVAL '30 seconds'
  AND expires_at > now()`
)

// MatchmakingQueueRepo persists matchmaking queue entries.
type MatchmakingQueueRepo struct {
	tx TxRunner
}

// NewMatchmakingQueueRepo constructs a MatchmakingQueueRepo bound to a TxRunner.
func NewMatchmakingQueueRepo(tx TxRunner) *MatchmakingQueueRepo {
	return &MatchmakingQueueRepo{tx: tx}
}

// scanQueueRow scans one row of the shared queueColumns projection into a
// domain Searcher. Works for both Row and Rows (same Scan signature).
func scanQueueRow(row interface{ Scan(dest ...any) error }) (domainmm.Searcher, error) {
	var (
		s            domainmm.Searcher
		id           string // row UUID — carried for column alignment, not surfaced
		statusStr    string
		matched      *string
		mode         string
		blitzVariant *string
	)
	if err := row.Scan(
		&id, &s.TenantID, &s.GCID, &statusStr, &s.Proficiency, &s.InterestTags,
		&s.EnteredAt, &s.ExpiresAt, &s.LastHeartbeat, &matched, &s.QuestionCount, &s.Category,
		&mode, &blitzVariant,
	); err != nil {
		return domainmm.Searcher{}, err
	}
	s.Status = domainmm.QueueStatus(statusStr)
	if matched != nil {
		s.MatchedDuelID = *matched
	}
	if mode != "" {
		s.Mode = mode
	} else {
		s.Mode = "classic"
	}
	if blitzVariant != nil {
		s.BlitzVariant = *blitzVariant
	}
	return s, nil
}

// Enqueue inserts a new finding entry, or updates the existing one if the
// user re-enters the pool (idempotent on tenant_id + gcid WHERE finding).
func (r *MatchmakingQueueRepo) Enqueue(ctx context.Context, s domainmm.Searcher) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		mode := s.Mode
		if mode == "" {
			mode = "classic"
		}
		var blitzVariant any
		if s.BlitzVariant != "" {
			blitzVariant = s.BlitzVariant
		}
		if _, err := q.Exec(ctx, sqlMatchmakingQueueInsert,
			s.TenantID, s.GCID, s.Proficiency, nonNilTags(s.InterestTags),
			s.EnteredAt, s.ExpiresAt, s.LastHeartbeat, s.QuestionCount, s.Category,
			mode, blitzVariant,
		); err != nil {
			return fmt.Errorf("pg: enqueue matchmaking: %w", err)
		}
		return nil
	})
}

// UpdateStatus sets the status for a finding entry. matchedDuelID is set
// when status is 'matched'. No-op (0 rows) when the entry is not currently
// finding — e.g. cancelling an entry that was just claimed.
func (r *MatchmakingQueueRepo) UpdateStatus(ctx context.Context, tenantID, gcid string, status domainmm.QueueStatus, matchedDuelID string) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	var duelID any
	if matchedDuelID != "" {
		duelID = matchedDuelID
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		if _, err := q.Exec(ctx, sqlMatchmakingQueueUpdateStatus,
			tenantID, gcid, string(status), duelID,
		); err != nil {
			return fmt.Errorf("pg: update matchmaking status: %w", err)
		}
		return nil
	})
}

// UpdateHeartbeat refreshes the last_heartbeat timestamp for a finding entry.
func (r *MatchmakingQueueRepo) UpdateHeartbeat(ctx context.Context, tenantID, gcid string) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		if _, err := q.Exec(ctx, sqlMatchmakingQueueUpdateHeartbeat,
			tenantID, gcid,
		); err != nil {
			return fmt.Errorf("pg: update heartbeat: %w", err)
		}
		return nil
	})
}

// FindByStatus returns all entries with the given status for a tenant.
func (r *MatchmakingQueueRepo) FindByStatus(ctx context.Context, tenantID string, status domainmm.QueueStatus) ([]domainmm.Searcher, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	out := []domainmm.Searcher{}
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		rows, err := q.Query(ctx, sqlMatchmakingQueueFindByStatus, tenantID, string(status))
		if err != nil {
			return fmt.Errorf("pg: find matchmaking by status: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			s, err := scanQueueRow(rows)
			if err != nil {
				return fmt.Errorf("pg: scan matchmaking row: %w", err)
			}
			out = append(out, s)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ExpireStaleForTenant marks one tenant's finding entries with stale
// heartbeats or past expiry as abandoned. The matchmaker's sweeper loop
// calls this once per active tenant; RLS forbids a cross-tenant sweep.
func (r *MatchmakingQueueRepo) ExpireStaleForTenant(ctx context.Context, tenantID string) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		if _, err := q.Exec(ctx, sqlMatchmakingQueueExpireStaleForTenant, tenantID); err != nil {
			return fmt.Errorf("pg: expire stale matchmaking: %w", err)
		}
		return nil
	})
}

// FindCandidates returns all valid (non-expired, non-stale) finding
// searchers for a tenant, oldest first. Plain SELECT — no locks are held;
// ClaimMatch's atomic guard resolves any race between pods.
func (r *MatchmakingQueueRepo) FindCandidates(ctx context.Context, tenantID string) ([]domainmm.Searcher, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	out := []domainmm.Searcher{}
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		rows, err := q.Query(ctx, sqlMatchmakingQueueFindCandidates, tenantID)
		if err != nil {
			return fmt.Errorf("pg: find matchmaking candidates: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			s, err := scanQueueRow(rows)
			if err != nil {
				return fmt.Errorf("pg: scan matchmaking candidate: %w", err)
			}
			out = append(out, s)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ClaimMatch atomically transitions two finding rows to 'matched' with
// the given duel ID. Returns ErrNoMatchFound if fewer than 2 rows were
// affected (another pod claimed one first, or a row expired between
// FindCandidates and ClaimMatch). The caller should skip to the next
// pair on this error.
func (r *MatchmakingQueueRepo) ClaimMatch(
	ctx context.Context,
	tenantID, gcidA, gcidB, duelID string,
) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		tag, err := q.Exec(ctx, sqlMatchmakingQueueClaimMatch, tenantID, gcidA, gcidB, duelID)
		if err != nil {
			return fmt.Errorf("pg: claim match: %w", err)
		}
		if tag.RowsAffected < 2 {
			return ErrNoMatchFound
		}
		return nil
	})
}

// RevertToFinding returns one claimed row to 'finding' so the matchmaker
// can re-pair the user after the duel-creation pipeline failed. Guarded
// by status='matched' AND matched_duel_id=$3: if the row has moved on
// (cancelled, re-claimed, expired) the revert is a no-op and returns
// ErrNoMatchFound so the caller can log it loud.
func (r *MatchmakingQueueRepo) RevertToFinding(ctx context.Context, tenantID, gcid, duelID string) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		tag, err := q.Exec(ctx, sqlMatchmakingQueueRevertToFinding, tenantID, gcid, duelID)
		if err != nil {
			return fmt.Errorf("pg: revert to finding: %w", err)
		}
		if tag.RowsAffected == 0 {
			return ErrNoMatchFound
		}
		return nil
	})
}

// GetByGCID returns the most recent queue entry for a (tenant, gcid) pair,
// whatever its status. Returns nil, nil when the user has no queue entry at
// all. Only the driver's empty-result signal maps to (nil, nil) — every
// other error is surfaced.
func (r *MatchmakingQueueRepo) GetByGCID(ctx context.Context, tenantID, gcid string) (*domainmm.Searcher, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	var out *domainmm.Searcher
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		s, err := scanQueueRow(q.QueryRow(ctx, sqlMatchmakingQueueGetByGCID, tenantID, gcid))
		if err != nil {
			if isNoRows(err) {
				return nil
			}
			return fmt.Errorf("pg: get matchmaking entry: %w", err)
		}
		out = &s
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CountFinding returns the number of active finding entries for a tenant.
func (r *MatchmakingQueueRepo) CountFinding(ctx context.Context, tenantID string) (int, error) {
	if r == nil || r.tx == nil {
		return 0, ErrNotImplemented
	}
	var count int
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		return q.QueryRow(ctx, sqlMatchmakingQueueCountFinding, tenantID).Scan(&count)
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}
