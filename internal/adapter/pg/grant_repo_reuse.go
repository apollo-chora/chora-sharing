// grant_repo_reuse.go — the reuse-context surface of the pgx-backed
// GrantRepo (ADR-229 WS-0, CHO-2102): distinct atom ids under an ACTIVE,
// unexpired grant for a grantee, feeding GetReuseContext's granted disjunct.
package pg

import (
	"context"
	"fmt"
	"strings"

	"github.com/apollo-chora/chora-common/rls"
)

// SQLActiveGrantAtomIDs selects the grantee's active-grant atom ids (any
// scope). Revoked / soft-deleted / expired grants are excluded.
const SQLActiveGrantAtomIDs = `
SELECT DISTINCT atom_id
FROM atom_usage_grants
WHERE grantee_gcid = $1
  AND status = 'active'
  AND deleted_at IS NULL
  AND (expires_at IS NULL OR expires_at > now())
ORDER BY atom_id`

// ActiveGrantAtomIDs implements grant.GrantRepo (reuse-context surface).
// RLS-scoped to the ctx tenant (rls.ApplySession before the user query).
func (r *GrantRepo) ActiveGrantAtomIDs(ctx context.Context, granteeGCID string) ([]string, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	granteeGCID = strings.TrimSpace(granteeGCID)
	if granteeGCID == "" {
		return nil, fmt.Errorf("pg: grantee gcid required")
	}
	out := make([]string, 0, 8)
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		rows, err := q.Query(ctx, SQLActiveGrantAtomIDs, granteeGCID)
		if err != nil {
			return fmt.Errorf("pg: active grant atom ids: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return fmt.Errorf("pg: scan grant atom id: %w", err)
			}
			out = append(out, id)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
