// atom_reuse_repo.go — pgx-backed adapter for the ADR-229 Amendment A1
// orphan-edition saga ports (CHO-2132). One repo implements:
//
//   - atom_reuse.GrantReads      — active-grant refs on an atom
//   - atom_reuse.EditionStore    — the event-fed (atom, revision) → orphan map
//     (atom_orphan_editions, migration 0034)
//   - atom_reuse.GrantRepointer  — the stranded-grant repoint sweep with the
//     append-only grant_events 'repointed' trail; duplicate-coverage grants
//     (grantee already holds an active orphan grant for the scope) are
//     soft-deleted as a dedupe-merge — NEVER revoked, NEVER hard-deleted
//   - atom_reuse.OrphanRequirer  — chora.sharing.atom_reuse.orphan_required.v1
//     via the same-transaction outbox (BINARY protobuf payload; mirrors the
//     relationship spine's Enqueue idiom)
//   - atom_reuse.ProjectionReads — the projection state INCLUDING archived
//     rows (the stranding recompute needs the archived flag + pinned revision)
//
// RLS: every method runs rls.ApplySession (SET LOCAL chora.tenant_id from
// ctx) before its SQL inside one TxRunner.RunInTx.
package pg

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protomarshal"
	"github.com/apollo-chora/chora-sharing/internal/adapter/outbox"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_reuse"
)

// TopicAtomReuseOrphanRequired is the saga-request topic sharing publishes.
const TopicAtomReuseOrphanRequired = "chora.sharing.atom_reuse.orphan_required.v1"

// -----------------------------------------------------------------------------
// SQL templates (exported for the PREPARE-smoke lane)
// -----------------------------------------------------------------------------

// SQLAtomReuseActiveGrantRefs selects the stranding-relevant slice of every
// ACTIVE, unexpired grant on the atom (RLS scopes the tenant).
const SQLAtomReuseActiveGrantRefs = `
SELECT id, grantee_gcid, owner_gcid, scope::text
FROM atom_usage_grants
WHERE atom_id = $1
  AND status = 'active'
  AND deleted_at IS NULL
  AND (expires_at IS NULL OR expires_at > now())`

// SQLAtomReuseLatestEdition resolves the most recent orphan edition for an
// atom (a later published revision withdrawn again gets its own edition).
const SQLAtomReuseLatestEdition = `
SELECT atom_id, source_revision_id, orphan_atom_id, orphaned_at
FROM atom_orphan_editions
WHERE atom_id = $1
ORDER BY orphaned_at DESC, created_at DESC
LIMIT 1`

// SQLAtomReusePutEdition upserts the mapping — idempotent against the
// singleton unique (atom_id, source_revision_id).
const SQLAtomReusePutEdition = `
INSERT INTO atom_orphan_editions (tenant_id, atom_id, source_revision_id, orphan_atom_id, orphaned_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (atom_id, source_revision_id) DO NOTHING`

// SQLAtomReuseRepointGrant moves ONE stranded grant original→orphan. The
// NOT EXISTS guard keeps the partial unique
// atom_usage_grants_active_unique_idx (grantee, atom, scope) WHERE active
// satisfied: if the grantee already holds an active grant on the orphan for
// the same scope, the UPDATE matches nothing and the merge below handles it.
const SQLAtomReuseRepointGrant = `
UPDATE atom_usage_grants g
SET atom_id = $2
WHERE g.id = $1
  AND g.atom_id = $3
  AND g.status = 'active'
  AND g.deleted_at IS NULL
  AND NOT EXISTS (
      SELECT 1 FROM atom_usage_grants o
      WHERE o.grantee_gcid = g.grantee_gcid
        AND o.atom_id = $2
        AND o.scope = g.scope
        AND o.status = 'active'
        AND o.deleted_at IS NULL
  )`

// SQLAtomReuseMergeGrant soft-deletes a duplicate-coverage grant (the
// grantee's continuity is already carried by their existing active grant on
// the orphan). A dedupe-merge — the row is NEVER revoked (A1.1) and NEVER
// hard-deleted; deleted_at excludes it from every default query.
const SQLAtomReuseMergeGrant = `
UPDATE atom_usage_grants
SET deleted_at = now()
WHERE id = $1
  AND atom_id = $2
  AND status = 'active'
  AND deleted_at IS NULL`

// SQLAtomReuseInsertGrantEvent appends the 'repointed' trail row.
const SQLAtomReuseInsertGrantEvent = `
INSERT INTO grant_events
    (tenant_id, grant_id, actor_gcid, event_type, reason)
VALUES ($1, $2, $3, $4, $5)`

// SQLAtomReuseSelectProjectionAnyState reads the stranding-relevant
// projection slice INCLUDING archived rows (closure-tombstoned rows stay
// excluded).
//
// revision_id is COALESCEd because it is NULLABLE since CHO-2174b (mig 0036):
// a consent-only projection carries no question revision, and a NULL scanned
// into a plain string is a hard driver error. The stranding detector treats an
// EMPTY revision as "nothing pinned to strand" — see GetAnyState.
const SQLAtomReuseSelectProjectionAnyState = `
SELECT COALESCE(revision_id::text, '') AS revision_id, owner_gcid,
       COALESCE(reuse_visibility, 'private'), archived
FROM atom_projections
WHERE atom_id = $1 AND deleted_at IS NULL`

// -----------------------------------------------------------------------------
// Repo
// -----------------------------------------------------------------------------

// AtomReuseRepoOptions configures envelope provenance for RequireOrphan.
type AtomReuseRepoOptions struct {
	SourceProject string
	SourceService string
	Now           func() time.Time
}

// AtomReuseRepo implements the atom_reuse ports against Postgres.
type AtomReuseRepo struct {
	tx   TxRunner
	opts AtomReuseRepoOptions
}

// NewAtomReuseRepo constructs the repo.
func NewAtomReuseRepo(tx TxRunner, opts AtomReuseRepoOptions) *AtomReuseRepo {
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Now().UTC() }
	}
	if opts.SourceService == "" {
		opts.SourceService = "chora-sharing"
	}
	return &AtomReuseRepo{tx: tx, opts: opts}
}

// Compile-time port assertions.
var (
	_ atom_reuse.GrantReads      = (*AtomReuseRepo)(nil)
	_ atom_reuse.EditionStore    = (*AtomReuseRepo)(nil)
	_ atom_reuse.GrantRepointer  = (*AtomReuseRepo)(nil)
	_ atom_reuse.OrphanRequirer  = (*AtomReuseRepo)(nil)
	_ atom_reuse.ProjectionReads = (*AtomReuseRepo)(nil)
)

// ActiveGrantRefsForAtom implements atom_reuse.GrantReads.
func (r *AtomReuseRepo) ActiveGrantRefsForAtom(ctx context.Context, atomID string) ([]atom_reuse.GrantRef, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if strings.TrimSpace(atomID) == "" {
		return nil, fmt.Errorf("pg: atom_reuse grant refs: atom_id required")
	}
	var out []atom_reuse.GrantRef
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		rows, err := q.Query(ctx, SQLAtomReuseActiveGrantRefs, atomID)
		if err != nil {
			return fmt.Errorf("pg: list active grant refs: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var g atom_reuse.GrantRef
			if err := rows.Scan(&g.GrantID, &g.GranteeGCID, &g.OwnerGCID, &g.Scope); err != nil {
				return fmt.Errorf("pg: scan grant ref: %w", err)
			}
			out = append(out, g)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// LatestEdition implements atom_reuse.EditionStore. A scan miss returns
// (zero, false, nil) per the package idiom — the detector then round-trips
// creation, the safe fallback.
func (r *AtomReuseRepo) LatestEdition(ctx context.Context, atomID string) (atom_reuse.OrphanEdition, bool, error) {
	if r == nil || r.tx == nil {
		return atom_reuse.OrphanEdition{}, false, ErrNotImplemented
	}
	var (
		ed    atom_reuse.OrphanEdition
		found bool
	)
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLAtomReuseLatestEdition, atomID)
		if scanErr := row.Scan(&ed.AtomID, &ed.SourceRevisionID, &ed.OrphanAtomID, &ed.OrphanedAt); scanErr != nil {
			return nil // not-found path — found stays false
		}
		found = true
		return nil
	})
	if err != nil {
		return atom_reuse.OrphanEdition{}, false, err
	}
	return ed, found, nil
}

// PutEdition implements atom_reuse.EditionStore (idempotent upsert).
func (r *AtomReuseRepo) PutEdition(ctx context.Context, e atom_reuse.OrphanEdition) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if err := e.Validate(); err != nil {
		return err
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		tenantID := tracing.TenantIDFromContext(ctx) // non-empty: ApplySession passed
		if _, err := q.Exec(ctx, SQLAtomReusePutEdition,
			tenantID, e.AtomID, e.SourceRevisionID, e.OrphanAtomID, e.OrphanedAt.UTC(),
		); err != nil {
			return fmt.Errorf("pg: put orphan edition: %w", err)
		}
		return nil
	})
}

// RepointStranded implements atom_reuse.GrantRepointer. One transaction
// sweeps every stranded grant:
//
//	repoint UPDATE hit  → grant now references the orphan  → trail 'repointed'
//	repoint miss + merge hit → duplicate coverage soft-deleted → trail 'repointed' (merge note)
//	repoint miss + merge miss → already handled by a previous delivery → skipped
func (r *AtomReuseRepo) RepointStranded(ctx context.Context, cmd atom_reuse.RepointCommand) (atom_reuse.RepointResult, error) {
	var res atom_reuse.RepointResult
	if r == nil || r.tx == nil {
		return res, ErrNotImplemented
	}
	if strings.TrimSpace(cmd.OriginalAtomID) == "" {
		return res, fmt.Errorf("pg: repoint stranded: original atom id required")
	}
	if strings.TrimSpace(cmd.OrphanAtomID) == "" {
		return res, fmt.Errorf("pg: repoint stranded: orphan atom id required")
	}
	if len(cmd.Stranded) == 0 {
		return res, nil // nothing to sweep
	}
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		tenantID := tracing.TenantIDFromContext(ctx)
		for _, g := range cmd.Stranded {
			tag, err := q.Exec(ctx, SQLAtomReuseRepointGrant, g.GrantID, cmd.OrphanAtomID, cmd.OriginalAtomID)
			if err != nil {
				return fmt.Errorf("pg: repoint grant %s: %w", g.GrantID, err)
			}
			if tag.RowsAffected > 0 {
				res.Repointed++
				reason := fmt.Sprintf("repointed %s -> %s (trigger=%s; %s)",
					cmd.OriginalAtomID, cmd.OrphanAtomID, cmd.Trigger, cmd.Note)
				if _, err := q.Exec(ctx, SQLAtomReuseInsertGrantEvent,
					tenantID, g.GrantID, g.OwnerGCID, "repointed", reason,
				); err != nil {
					return fmt.Errorf("pg: trail repointed grant %s: %w", g.GrantID, err)
				}
				continue
			}
			// Blocked by the active-unique (grantee already covered on the
			// orphan for this scope) OR already handled. The merge UPDATE
			// only matches a still-active-on-original row — distinguishing
			// the two.
			mtag, err := q.Exec(ctx, SQLAtomReuseMergeGrant, g.GrantID, cmd.OriginalAtomID)
			if err != nil {
				return fmt.Errorf("pg: merge grant %s: %w", g.GrantID, err)
			}
			if mtag.RowsAffected > 0 {
				res.Merged++
				reason := fmt.Sprintf("repointed %s -> %s (trigger=%s; duplicate coverage merged into the grantee's existing orphan grant; %s)",
					cmd.OriginalAtomID, cmd.OrphanAtomID, cmd.Trigger, cmd.Note)
				if _, err := q.Exec(ctx, SQLAtomReuseInsertGrantEvent,
					tenantID, g.GrantID, g.OwnerGCID, "repointed", reason,
				); err != nil {
					return fmt.Errorf("pg: trail merged grant %s: %w", g.GrantID, err)
				}
				continue
			}
			res.Skipped++
		}
		return nil
	})
	if err != nil {
		return atom_reuse.RepointResult{}, err
	}
	return res, nil
}

// RequireOrphan implements atom_reuse.OrphanRequirer — publishes
// orphan_required.v1 through the same-transaction outbox (BINARY protobuf,
// Schema Registry chora-sharing-atom_reuse-orphan_required-v1).
func (r *AtomReuseRepo) RequireOrphan(ctx context.Context, req atom_reuse.OrphanRequest) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if err := req.Validate(); err != nil {
		return err
	}
	detected := req.DetectedAt
	if detected.IsZero() {
		detected = r.opts.Now()
	}

	env := cgcenvelope.Build(ctx, cgcenvelope.BuildOpts{
		SchemaVersion:      1,
		SourceProject:      r.opts.SourceProject,
		SourceService:      r.opts.SourceService,
		ChoraImdaDimension: "accountability",
		ImdaLifecycleStage: "runtime",
		Now:                r.opts.Now,
	})
	env.GCID = req.ActorGCID
	env.OccurredAt = detected

	payload := map[string]any{
		"atom_id":              req.AtomID,
		"revision_id":          req.RevisionID,
		"trigger":              req.Trigger,
		"stranded_grant_count": int32(req.StrandedCount),
		"detected_at":          detected,
		"chora_imda_dimension": "accountability",
		"imda_lifecycle_stage": "runtime",
	}
	body, err := protomarshal.MarshalPayload(TopicAtomReuseOrphanRequired, protomarshal.Envelope{
		EventID:        env.EventID,
		IdempotencyKey: env.IdempotencyKey,
		TenantID:       env.TenantID,
		GCID:           env.GCID,
		OccurredAt:     env.OccurredAt,
		PublishedAt:    env.PublishedAt,
		Traceparent:    env.Traceparent,
		Tracestate:     env.Tracestate,
		SourceProject:  env.SourceProject,
		SourceService:  env.SourceService,
		SchemaVersion:  env.SchemaVersion,
	}, payload)
	if err != nil {
		return fmt.Errorf("pg: marshal orphan_required: %w", err)
	}

	row, err := outbox.BuildRow(TopicAtomReuseOrphanRequired, env, body)
	if err != nil {
		return fmt.Errorf("pg: build orphan_required outbox row: %w", err)
	}
	// Aggregate identity: the withdrawn atom (the saga subject).
	row.AggregateType = "atom_reuse"
	row.AggregateID = req.AtomID

	envJSON, err := json.Marshal(row.Envelope)
	if err != nil {
		return fmt.Errorf("pg: marshal orphan_required envelope: %w", err)
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		if _, err := q.Exec(ctx, SQLInsertOutboxEvent,
			row.ID, row.TenantID, nullableOutboxUUID(row.GCID), row.AggregateType, row.AggregateID,
			row.EventType, row.Topic, row.Payload, string(envJSON), row.IdempotencyKey, row.OccurredAt,
		); err != nil {
			return fmt.Errorf("pg: insert orphan_required outbox row: %w", err)
		}
		return nil
	})
}

// GetAnyState implements atom_reuse.ProjectionReads — the projection slice
// INCLUDING archived rows.
func (r *AtomReuseRepo) GetAnyState(ctx context.Context, atomID string) (atom_reuse.ProjectionState, error) {
	var st atom_reuse.ProjectionState
	if r == nil || r.tx == nil {
		return st, ErrNotImplemented
	}
	found := false
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLAtomReuseSelectProjectionAnyState, atomID)
		if scanErr := row.Scan(&st.RevisionID, &st.OwnerGCID, &st.ReuseVisibility, &st.Archived); scanErr != nil {
			return nil // not-found path
		}
		found = true
		return nil
	})
	if err != nil {
		return atom_reuse.ProjectionState{}, err
	}
	if !found {
		return atom_reuse.ProjectionState{}, atom_reuse.ErrProjectionNotFound
	}
	return st, nil
}
