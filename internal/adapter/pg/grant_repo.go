// grant_repo.go — pgx-backed grant.GrantRepo.
//
// The AtomUsageGrant aggregate root persisted to atom_usage_grants. RLS applied
// on every method via qToExecer(q) before user queries.
//
// Idempotency contract (per the port):
//   - Authorize: ON CONFLICT DO NOTHING on the partial unique index
//     atom_usage_grants_active_unique_idx (grantee_gcid, atom_id, scope)
//     WHERE status='active' AND deleted_at IS NULL; SELECT returns the
//     existing active grant when the conflict hits (R2 frozen snapshot wins).
//   - Revoke: UPDATE status='revoked'; idempotent on grant_id.
//   - GetActive: SELECT WHERE grantee+atom+scope AND status='active', with
//     UNLIMITED covers-any-scope fallback.
//   - ListEntitled: own ∪ free ∪ active-grant union per §7.4.
//
// Schema: migrations/0004_atom_sharing.up.sql (atom_usage_grants).
package pg

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
	"github.com/apollo-chora/chora-sharing/internal/domain/grant"
)

const (
	// sqlGrantInsertActive inserts an active grant. The partial unique index
	// atom_usage_grants_active_unique_idx ON CONFLICT DO NOTHING makes a
	// re-Authorize for the same (grantee, atom, scope) WHERE active a no-op;
	// the fall-through SELECT returns the existing frozen snapshot.
	sqlGrantInsertActive = `
INSERT INTO atom_usage_grants
    (id, tenant_id, source_share_entry, owner_gcid, atom_id, atom_revision_id,
     grantee_gcid, scope, license_terms_snapshot, royalty_rate_snapshot,
     status, granted_at, revoked_at, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'active', $11, $12, $13)
ON CONFLICT DO NOTHING`

	// sqlGrantSelectActiveExact loads the active grant for the exact triple.
	sqlGrantSelectActiveExact = `
SELECT id, tenant_id, source_share_entry, owner_gcid, atom_id, atom_revision_id,
       grantee_gcid, scope, license_terms_snapshot, royalty_rate_snapshot,
       status, granted_at, revoked_at, expires_at
FROM atom_usage_grants
WHERE grantee_gcid = $1 AND atom_id = $2 AND scope = $3
  AND status = 'active' AND deleted_at IS NULL`

	// sqlGrantSelectByID loads a grant by id (used by Revoke's existence +
	// ownership check). Loads owner_gcid so Revoke can verify the revoker
	// is the atom owner.
	sqlGrantSelectByID = `
SELECT id, tenant_id, owner_gcid FROM atom_usage_grants WHERE id = $1 AND deleted_at IS NULL`

	// sqlGrantRevoke flips status to revoked + stamps revoked_at. The
	// updated_at trigger bumps the row.
	sqlGrantRevoke = `
UPDATE atom_usage_grants
SET status = 'revoked', revoked_at = now()
WHERE id = $1 AND status = 'active' AND deleted_at IS NULL`

	// sqlGrantInsertEvent appends the grant_events lifecycle log row.
	sqlGrantInsertEvent = `
INSERT INTO grant_events
    (tenant_id, grant_id, actor_gcid, event_type, reason)
VALUES ($1, $2, $3, $4, $5)`

	// sqlListEntitledOwn selects atoms the gcid owns (from atom_projections) —
	// the `own` leg of the ADR-229 picker disjunct.
	//
	// COALESCE on the now-nullable question columns (CHO-2174b, mig 0036): a
	// consent-only projection (no question revision) must still appear in the
	// owner's picker; a NULL scanned into a plain string would instead fail the
	// whole listing with a driver error.
	sqlListEntitledOwn = `
SELECT atom_id, COALESCE(revision_id::text, '') AS revision_id, owner_gcid,
       author_display_name, COALESCE(stem, '') AS stem,
       COALESCE(question_type, '') AS question_type
FROM atom_projections
WHERE owner_gcid = $1 AND archived = FALSE`

	// sqlListEntitledFree selects visible free-license shares (usable by
	// anyone). A share is "visible" when no atom_share_event hides/revokes it.
	sqlListEntitledFree = `
SELECT e.content
FROM social_feed_entries e
WHERE e.entry_type = 'share'
  AND (e.content->>'license_terms') IN ('free', 'cc_by_sa', 'cc_nd')
  AND NOT EXISTS (
      SELECT 1 FROM atom_share_events ev
      WHERE ev.feed_entry_id = e.id
        AND ev.event_type IN ('revoked', 'hidden', 'moderation_hidden')
  )`

	// sqlListEntitledGrants selects active grants covering the scope for the
	// gcid. UNLIMITED covers any requested scope (handled in Go via Covers).
	sqlListEntitledGrants = `
SELECT atom_id, atom_revision_id, owner_gcid, license_terms_snapshot, scope
FROM atom_usage_grants
WHERE grantee_gcid = $1 AND status = 'active' AND deleted_at IS NULL`
)

// GrantRepo implements grant.GrantRepo against Postgres.
type GrantRepo struct {
	tx TxRunner
}

// NewGrantRepo constructs a GrantRepo bound to a TxRunner.
func NewGrantRepo(tx TxRunner) *GrantRepo { return &GrantRepo{tx: tx} }

// Compile-time port assertion.
var _ grant.GrantRepo = (*GrantRepo)(nil)

// Authorize persists a grant. Idempotent on (grantee, atom, scope) WHERE
// active: if an active grant already exists for that triple, it is returned
// unchanged (the frozen snapshot from the first call wins — R2).
func (r *GrantRepo) Authorize(ctx context.Context, g *grant.AtomUsageGrant) (*grant.AtomUsageGrant, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if g == nil {
		return nil, grant.ErrInvalidArgument
	}
	rateJSON, err := json.Marshal(g.RoyaltyRateSnapshot)
	if err != nil {
		return nil, fmt.Errorf("pg: marshal royalty snapshot: %w", err)
	}
	var inserted bool
	err = r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		tag, err := q.Exec(ctx, sqlGrantInsertActive,
			g.ID, g.TenantID, nullStr(g.SourceShareEntry),
			g.OwnerGCID, g.AtomID, g.RevisionID, g.GranteeGCID, string(g.Scope),
			string(g.LicenseTermsSnapshot), []byte(rateJSON),
			g.GrantedAt, nullTime(g.RevokedAt), nullTime(g.ExpiresAt),
		)
		if err != nil {
			return fmt.Errorf("pg: insert grant: %w", err)
		}
		inserted = tag.RowsAffected > 0
		return nil
	})
	if err != nil {
		return nil, err
	}
	if inserted {
		return g, nil
	}
	// ON CONFLICT path — the partial unique already has an active grant for
	// (grantee, atom, scope). Load + return the existing frozen snapshot.
	existing, gErr := r.GetActive(ctx, g.GranteeGCID, g.AtomID, g.Scope)
	if gErr != nil {
		// Edge case: the conflict was NOT on the active-grant unique index
		// (e.g. a soft-deleted row reactivated concurrently). Return the
		// input grant as the persisted record — the caller's snapshot wins.
		return g, nil
	}
	return existing, nil
}

// Revoke transitions a grant to revoked. Idempotent on grant_id — revoking an
// already-revoked/expired grant is a no-op. Appends a grant_events row.
func (r *GrantRepo) Revoke(ctx context.Context, grantID, revokerGCID, reason string) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		// Existence + ownership check (returns ErrGrantNotFound when missing).
		// The revoker MUST be the atom owner (OwnerGCID) — a non-owner
		// revoker gets ErrForbidden so the gRPC handler returns PermissionDenied.
		var id, tenantID, ownerGCID string
		if err := q.QueryRow(ctx, sqlGrantSelectByID, grantID).Scan(&id, &tenantID, &ownerGCID); err != nil {
			return grant.ErrGrantNotFound
		}
		if ownerGCID != revokerGCID {
			return grant.ErrForbidden
		}
		// Idempotent revoke — only active grants flip; already-terminal rows
		// are a no-op (UPDATE affects 0 rows, which is success here).
		if _, err := q.Exec(ctx, sqlGrantRevoke, grantID); err != nil {
			return fmt.Errorf("pg: revoke grant: %w", err)
		}
		if _, err := q.Exec(ctx, sqlGrantInsertEvent,
			tenantID, grantID, revokerGCID, "revoked", reason,
		); err != nil {
			return fmt.Errorf("pg: insert grant event: %w", err)
		}
		return nil
	})
}

// GetActive returns the active grant for (grantee, atom, scope), checking
// Covers semantics: an active UNLIMITED grant satisfies any requested scope.
// Returns ErrGrantNotFound when no active grant covers the triple.
func (r *GrantRepo) GetActive(ctx context.Context, grantee, atomID string, scope grant.Scope) (*grant.AtomUsageGrant, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	// Exact scope match first.
	g, err := r.loadActive(ctx, grantee, atomID, scope)
	if err == nil {
		return g, nil
	}
	if err != grant.ErrGrantNotFound {
		return nil, err
	}
	// Fall back to UNLIMITED (covers any scope) — but only when the request
	// is not itself for UNLIMITED (already tried above).
	if scope != grant.ScopeUnlimited {
		return r.loadActive(ctx, grantee, atomID, grant.ScopeUnlimited)
	}
	return nil, grant.ErrGrantNotFound
}

// loadActive is the single-active-grant SELECT for an exact scope.
func (r *GrantRepo) loadActive(ctx context.Context, grantee, atomID string, scope grant.Scope) (*grant.AtomUsageGrant, error) {
	var g *grant.AtomUsageGrant
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, sqlGrantSelectActiveExact, grantee, atomID, string(scope))
		gg, err := scanGrant(row.Scan)
		if err != nil {
			return grant.ErrGrantNotFound
		}
		g = gg
		return nil
	})
	if err != nil {
		return nil, err
	}
	return g, nil
}

// ListEntitled builds the "atoms usable by me" union per §7.4:
// own ∪ free ∪ active-grant. An atom may appear once with both IsOwn and
// HasGrant set if the grantee both owns the atom and has a grant for it.
//
// topicTags filters on question_type (case-insensitive); limit clamps the
// result (default 100, max 500) — mirrors the inmem behavior.
func (r *GrantRepo) ListEntitled(ctx context.Context, gcid string, scope grant.Scope, topicTags []string, limit int) ([]grant.EntitledAtom, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	tagSet := make(map[string]bool, len(topicTags))
	for _, t := range topicTags {
		tagSet[strings.ToLower(t)] = true
	}

	// union keyed by atom_id (own + free + active-grant merge).
	union := make(map[string]grant.EntitledAtom)

	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}

		// 1. Own atoms: projection.owner_gcid == gcid.
		ownRows, err := q.Query(ctx, sqlListEntitledOwn, gcid)
		if err != nil {
			return err
		}
		for ownRows.Next() {
			var atomID, revID, owner, displayName, stem, qType string
			if err := ownRows.Scan(&atomID, &revID, &owner, &displayName, &stem, &qType); err != nil {
				ownRows.Close()
				return err
			}
			if len(tagSet) > 0 && !tagSet[strings.ToLower(qType)] {
				continue
			}
			e := union[atomID]
			e.AtomID = atomID
			e.RevisionID = revID
			e.AuthorGCID = owner
			e.AuthorDisplayName = displayName
			e.StemPreview = stemPreviewFromString(stem)
			e.License = atom_share.LicenseFree
			e.IsOwn = true
			union[atomID] = e
		}
		if err := ownRows.Err(); err != nil {
			ownRows.Close()
			return err
		}
		ownRows.Close()

		// 2. Free shares: a visible share with a free/cc license.
		freeRows, err := q.Query(ctx, sqlListEntitledFree)
		if err != nil {
			return err
		}
		for freeRows.Next() {
			var raw []byte
			if err := freeRows.Scan(&raw); err != nil {
				freeRows.Close()
				return err
			}
			var c shareContent
			if err := json.Unmarshal(raw, &c); err != nil {
				continue
			}
			if len(tagSet) > 0 && !tagSet[strings.ToLower(c.QuestionType)] {
				continue
			}
			e := union[c.AtomID]
			e.AtomID = c.AtomID
			e.RevisionID = c.RevisionID
			e.AuthorGCID = c.OwnerGCID
			e.AuthorDisplayName = c.AuthorDisplayName
			e.StemPreview = c.StemPreview
			e.License = c.License
			union[c.AtomID] = e
		}
		if err := freeRows.Err(); err != nil {
			freeRows.Close()
			return err
		}
		freeRows.Close()

		// 3. Active grants: an active grant covers (gcid, atom) for the scope.
		grantRows, err := q.Query(ctx, sqlListEntitledGrants, gcid)
		if err != nil {
			return err
		}
		for grantRows.Next() {
			var atomID, revID, owner, licenseStr, scopeStr string
			if err := grantRows.Scan(&atomID, &revID, &owner, &licenseStr, &scopeStr); err != nil {
				grantRows.Close()
				return err
			}
			// Covers semantics: UNLIMITED covers any requested scope;
			// otherwise exact match.
			if !grant.Scope(scopeStr).Covers(scope) {
				continue
			}
			e := union[atomID]
			e.AtomID = atomID
			e.RevisionID = revID
			e.AuthorGCID = owner
			e.License = atom_share.LicenseTerms(licenseStr)
			e.HasGrant = true
			union[atomID] = e
		}
		return grantRows.Err()
	})
	if err != nil {
		return nil, err
	}

	out := make([]grant.EntitledAtom, 0, len(union))
	for _, e := range union {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AtomID < out[j].AtomID })
	if limit < len(out) {
		out = out[:limit]
	}
	return out, nil
}

// scanGrant hydrates an AtomUsageGrant from a row scanner.
func scanGrant(scan func(...any) error) (*grant.AtomUsageGrant, error) {
	var (
		g           grant.AtomUsageGrant
		sourceShare *string
		rateJSON    []byte
		revokedAt   *time.Time
		expiresAt   *time.Time
	)
	if err := scan(
		&g.ID, &g.TenantID, &sourceShare, &g.OwnerGCID, &g.AtomID, &g.RevisionID,
		&g.GranteeGCID, &g.Scope, &g.LicenseTermsSnapshot, &rateJSON,
		&g.Status, &g.GrantedAt, &revokedAt, &expiresAt,
	); err != nil {
		return nil, err
	}
	if sourceShare != nil {
		g.SourceShareEntry = *sourceShare
	}
	if revokedAt != nil {
		g.RevokedAt = revokedAt
	}
	if expiresAt != nil {
		g.ExpiresAt = expiresAt
	}
	if len(rateJSON) > 0 {
		_ = json.Unmarshal(rateJSON, &g.RoyaltyRateSnapshot)
	}
	return &g, nil
}

// stemPreviewFromString truncates a stem to 140 runes on a rune boundary
// (mirrors atom_projection.Projection.StemPreview).
func stemPreviewFromString(s string) string {
	const max = 140
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}
