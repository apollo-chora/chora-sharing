// grant_reuse.go — in-memory ActiveGrantAtomIDs (reuse-context surface of
// grant.GrantRepo; ADR-229 WS-0, CHO-2102). Mirrors the pg adapter: ACTIVE,
// unexpired grants only, distinct atom ids, sorted for stable output.
package inmem

import (
	"context"
	"sort"
	"time"
)

// ActiveGrantAtomIDs implements grant.GrantRepo (reuse-context surface).
func (r *GrantRepo) ActiveGrantAtomIDs(_ context.Context, granteeGCID string) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	seen := make(map[string]struct{}, len(r.active))
	for _, g := range r.active {
		if g.GranteeGCID != granteeGCID {
			continue
		}
		if g.ExpiresAt != nil && !g.ExpiresAt.After(now) {
			continue
		}
		seen[g.AtomID] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}
