// social_repo.go — pgx-backed SocialGraphRepo (ADR-229 WS-0, CHO-2102).
//
// The durable social graph: follow edges persist to social_follows (0002) and
// block edges to social_blocks (0031). This replaces the in-memory-only Graph
// whose runtime edges were lost on pod restart. Reads are served from Postgres
// under RLS (tenant-correct — the old in-memory reads mixed a dual-membership
// user's edges across tenants), so the repo is BOTH transports' SocialGraph
// port AND the social.GraphQueries doorway behind GetReuseContext (ADR-230
// D3; friend set = explicit friendships per ADR-230 D2, schema 0032).
//
// RLS: every method stamps the explicit tenantID onto ctx
// (tracing.WithTenantID) then calls rls.ApplySession BEFORE any user query —
// an empty tenant fails loud with rls.ErrNoTenantContext, never a silent
// zero-row read.
//
// Schema: migrations/0002_social.up.sql (social_follows) +
// 0031_social_blocks_and_projection_reconcile.up.sql (social_blocks).
package pg

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

const (
	// SQLInsertSocialFollow inserts a follow edge; the UNIQUE
	// (follower_gcid, followee_gcid) constraint + ON CONFLICT DO NOTHING make
	// a re-Follow a no-op (append-only: the ORIGINAL edge wins).
	SQLInsertSocialFollow = `
INSERT INTO social_follows (edge_id, tenant_id, follower_gcid, followee_gcid, created_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (follower_gcid, followee_gcid) DO NOTHING`

	// SQLSelectSocialFollow loads the existing edge after a conflict so
	// re-Follow returns the ORIGINAL edge unchanged (original CreatedAt
	// preserved for audit).
	SQLSelectSocialFollow = `
SELECT edge_id, tenant_id, created_at
FROM social_follows
WHERE follower_gcid = $1 AND followee_gcid = $2`

	// SQLDeleteSocialFollow removes the edge (presence record — the shipped
	// Unfollow semantic; social_follows carries no deleted_at).
	SQLDeleteSocialFollow = `
DELETE FROM social_follows
WHERE follower_gcid = $1 AND followee_gcid = $2`

	// SQLListFollowing lists the followee GCIDs for a follower.
	SQLListFollowing = `
SELECT followee_gcid
FROM social_follows
WHERE follower_gcid = $1
ORDER BY created_at DESC`

	// SQLListFollowers lists the follower GCIDs for a followee.
	SQLListFollowers = `
SELECT follower_gcid
FROM social_follows
WHERE followee_gcid = $1
ORDER BY created_at DESC`

	// SQLInsertSocialBlock inserts a block edge idempotently.
	SQLInsertSocialBlock = `
INSERT INTO social_blocks (block_id, tenant_id, blocker_gcid, blocked_gcid, created_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (blocker_gcid, blocked_gcid) DO NOTHING`

	// SQLDeleteSocialBlock removes a block edge.
	SQLDeleteSocialBlock = `
DELETE FROM social_blocks
WHERE blocker_gcid = $1 AND blocked_gcid = $2`

	// SQLListBlockedBy lists the GCIDs a blocker has blocked.
	SQLListBlockedBy = `
SELECT blocked_gcid
FROM social_blocks
WHERE blocker_gcid = $1
ORDER BY created_at DESC`

	// SQLFollowSuggestions resolves hybrid "who to follow" candidates:
	// interest-based ranking (shared profiler tags) PRIMARY, follow-graph
	// proximity (mutual follows) SECONDARY.
	//
	// Two signals are UNIONed + deduped:
	//   1. INTEREST: candidates sharing >=1 tag with the caller, ranked by
	//      shared-tag count. Traverses profiler_profiles.tags jsonb.
	//   2. FOLLOW-GRAPH: 2-hop candidates (followees-of-followees),
	//      ranked by mutual-follow count.
	//
	// Exclusions applied IN-QUERY before LIMIT (not in the handler):
	//   - self ($1)
	//   - already-followed (social_follows WHERE follower = $1)
	//   - blocked either direction (social_blocks)
	//
	// RLS scopes every table to the caller's tenant. Sorted by
	// shared_tag_count DESC, mutual_follows DESC, candidate ASC.
	SQLFollowSuggestions = `
WITH my_tags AS (
    SELECT (tag->>'tag')::text AS tag
    FROM profiler_profiles,
         jsonb_array_elements(tags) AS tag
    WHERE gcid = $1
),
my_followees AS (
    SELECT followee_gcid
    FROM social_follows
    WHERE follower_gcid = $1
),
interest_candidates AS (
    SELECT DISTINCT pp.gcid AS candidate
    FROM profiler_profiles pp, my_tags mt
    WHERE EXISTS (
        SELECT 1 FROM jsonb_array_elements(pp.tags) AS t
        WHERE t->>'tag' = mt.tag
    )
    AND pp.gcid <> $1
),
fof_candidates AS (
    SELECT sf2.followee_gcid AS candidate
    FROM social_follows sf1
    JOIN social_follows sf2 ON sf2.follower_gcid = sf1.followee_gcid
    WHERE sf1.follower_gcid = $1
      AND sf2.followee_gcid <> $1
),
all_candidates AS (
    SELECT candidate FROM interest_candidates
    UNION
    SELECT candidate FROM fof_candidates
)
SELECT * FROM (
    SELECT
        ac.candidate,
        COALESCE(NULLIF(pp.display_name, ''), sfe.display_name, '') AS display_name,
        COALESCE((
            SELECT array_agg(DISTINCT t->>'tag')
            FROM profiler_profiles pp2, jsonb_array_elements(pp2.tags) AS t
            WHERE pp2.gcid = ac.candidate
              AND t->>'tag' IN (SELECT tag FROM my_tags)
        ), ARRAY[]::text[]) AS shared_tags,
        COALESCE((
            SELECT count(*)::int
            FROM social_follows sf
            JOIN my_followees mf ON sf.follower_gcid = mf.followee_gcid
            WHERE sf.followee_gcid = ac.candidate
        ), 0) AS mutual_follows
    FROM all_candidates ac
    LEFT JOIN profiler_profiles pp ON pp.gcid = ac.candidate
    LEFT JOIN LATERAL (
        SELECT e.content->>'author_display_name' AS display_name
        FROM social_feed_entries e
        WHERE e.entry_type = 'share'
          AND e.actor_gcid = ac.candidate
          AND e.tenant_id = current_setting('chora.tenant_id', true)::uuid
          AND e.content->>'author_display_name' != ''
        ORDER BY e.created_at DESC
        LIMIT 1
    ) sfe ON true
    WHERE ac.candidate <> $1
      AND NOT EXISTS (
          SELECT 1 FROM my_followees mf WHERE mf.followee_gcid = ac.candidate
      )
      AND NOT EXISTS (
          SELECT 1 FROM social_blocks b
          WHERE (b.blocker_gcid = $1 AND b.blocked_gcid = ac.candidate)
             OR (b.blocker_gcid = ac.candidate AND b.blocked_gcid = $1)
      )
) AS ranked
ORDER BY array_length(shared_tags, 1) DESC NULLS LAST, mutual_follows DESC, candidate ASC
LIMIT $2`
)

// SocialGraphRepo is the pgx-backed social graph. It satisfies the http +
// grpc SocialGraph ports and the social.GraphQueries domain port.
type SocialGraphRepo struct {
	tx TxRunner
}

// NewSocialGraphRepo constructs the repo around a TxRunner. A nil runner
// (dev-fallback / misuse) makes every method return ErrNotImplemented.
func NewSocialGraphRepo(tx TxRunner) *SocialGraphRepo {
	return &SocialGraphRepo{tx: tx}
}

// Follow inserts the (follower → followee) edge if absent; a duplicate
// returns the ORIGINAL edge unchanged with created=false (append-only).
func (r *SocialGraphRepo) Follow(ctx context.Context, tenantID, follower, followee string) (*social.Edge, bool, error) {
	if r == nil || r.tx == nil {
		return nil, false, ErrNotImplemented
	}
	edge, err := social.NewEdge(tenantID, follower, followee)
	if err != nil {
		return nil, false, err
	}
	var (
		out     *social.Edge
		created bool
	)
	err = r.run(ctx, tenantID, func(ctx context.Context, q Querier) error {
		tag, err := q.Exec(ctx, SQLInsertSocialFollow,
			edge.ID, edge.TenantID, edge.FollowerGCID, edge.FolloweeGCID, edge.CreatedAt)
		if err != nil {
			return fmt.Errorf("pg: insert social follow: %w", err)
		}
		if tag.RowsAffected == 1 {
			out, created = edge, true
			return nil
		}
		// Conflict — load + return the original edge unchanged.
		existing := &social.Edge{FollowerGCID: follower, FolloweeGCID: followee}
		var createdAt time.Time
		if err := q.QueryRow(ctx, SQLSelectSocialFollow, follower, followee).
			Scan(&existing.ID, &existing.TenantID, &createdAt); err != nil {
			return fmt.Errorf("pg: select social follow after conflict: %w", err)
		}
		existing.CreatedAt = createdAt
		out, created = existing, false
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return out, created, nil
}

// Unfollow removes the edge; returns (true, nil) when a row was deleted.
func (r *SocialGraphRepo) Unfollow(ctx context.Context, tenantID, follower, followee string) (bool, error) {
	if r == nil || r.tx == nil {
		return false, ErrNotImplemented
	}
	if err := requireGCIDs(follower, followee); err != nil {
		return false, err
	}
	var removed bool
	err := r.run(ctx, tenantID, func(ctx context.Context, q Querier) error {
		tag, err := q.Exec(ctx, SQLDeleteSocialFollow, follower, followee)
		if err != nil {
			return fmt.Errorf("pg: delete social follow: %w", err)
		}
		removed = tag.RowsAffected > 0
		return nil
	})
	if err != nil {
		return false, err
	}
	return removed, nil
}

// Block records a (blocker → blocked) edge idempotently.
func (r *SocialGraphRepo) Block(ctx context.Context, tenantID, blocker, blocked string) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	b, err := social.NewBlock(tenantID, blocker, blocked)
	if err != nil {
		return err
	}
	return r.run(ctx, tenantID, func(ctx context.Context, q Querier) error {
		if _, err := q.Exec(ctx, SQLInsertSocialBlock,
			b.ID, b.TenantID, b.BlockerGCID, b.BlockedGCID, b.CreatedAt); err != nil {
			return fmt.Errorf("pg: insert social block: %w", err)
		}
		return nil
	})
}

// Unblock removes a block edge; returns (true, nil) when a row was deleted.
func (r *SocialGraphRepo) Unblock(ctx context.Context, tenantID, blocker, blocked string) (bool, error) {
	if r == nil || r.tx == nil {
		return false, ErrNotImplemented
	}
	if err := requireGCIDs(blocker, blocked); err != nil {
		return false, err
	}
	var removed bool
	err := r.run(ctx, tenantID, func(ctx context.Context, q Querier) error {
		tag, err := q.Exec(ctx, SQLDeleteSocialBlock, blocker, blocked)
		if err != nil {
			return fmt.Errorf("pg: delete social block: %w", err)
		}
		removed = tag.RowsAffected > 0
		return nil
	})
	if err != nil {
		return false, err
	}
	return removed, nil
}

// FollowingGCIDs lists the followee GCIDs for gcid within the tenant.
func (r *SocialGraphRepo) FollowingGCIDs(ctx context.Context, tenantID, gcid string) ([]string, error) {
	return r.listGCIDs(ctx, tenantID, gcid, SQLListFollowing)
}

// FollowersGCIDs lists the follower GCIDs for gcid within the tenant.
func (r *SocialGraphRepo) FollowersGCIDs(ctx context.Context, tenantID, gcid string) ([]string, error) {
	return r.listGCIDs(ctx, tenantID, gcid, SQLListFollowers)
}

// BlockedBy lists the GCIDs gcid has blocked within the tenant.
func (r *SocialGraphRepo) BlockedBy(ctx context.Context, tenantID, gcid string) ([]string, error) {
	return r.listGCIDs(ctx, tenantID, gcid, SQLListBlockedBy)
}

// FollowSuggestions resolves hybrid "who to follow" candidates
// (social.SuggestionQueries): interest-based ranking (shared profiler tags)
// PRIMARY, follow-graph proximity (mutual follows) SECONDARY. Exclusion-
// filtered in-query (self / already-followed / blocked-either-direction)
// before the LIMIT. RLS-scoped to the caller's tenant.
func (r *SocialGraphRepo) FollowSuggestions(ctx context.Context, tenantID, gcid string, limit int) ([]social.FollowSuggestion, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if strings.TrimSpace(gcid) == "" {
		return nil, fmt.Errorf("%w: gcid required", social.ErrInvalidArgument)
	}
	if limit <= 0 {
		return nil, fmt.Errorf("%w: limit must be positive", social.ErrInvalidArgument)
	}
	out := make([]social.FollowSuggestion, 0, limit)
	err := r.run(ctx, tenantID, func(ctx context.Context, q Querier) error {
		rows, err := q.Query(ctx, SQLFollowSuggestions, gcid, limit)
		if err != nil {
			return fmt.Errorf("pg: follow suggestions query: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var s social.FollowSuggestion
			if err := rows.Scan(&s.GCID, &s.DisplayName, &s.SharedTags, &s.MutualFollows); err != nil {
				return fmt.Errorf("pg: follow suggestions scan: %w", err)
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

// FriendSet resolves the friend set (social.GraphQueries, ADR-230 D2):
// accepted friendships minus blocks in either direction. Replaces the
// WS-0 derived mutual-follow set with zero contract change upstream.
func (r *SocialGraphRepo) FriendSet(ctx context.Context, tenantID, gcid string) ([]string, error) {
	return r.listGCIDs(ctx, tenantID, gcid, SQLFriendPartners)
}

// FriendSuggestions resolves bounded FoF candidates (social.GraphQueries).
// The friendship write path is gone, so the friend-of-friend graph is always
// empty — returns an empty slice without hitting the DB. Kept for
// GraphQueries interface parity.
func (r *SocialGraphRepo) FriendSuggestions(_ context.Context, _, _ string, _ int) ([]social.FriendSuggestion, error) {
	return []social.FriendSuggestion{}, nil
}

// listGCIDs runs a single-arg single-column GCID list query under RLS.
func (r *SocialGraphRepo) listGCIDs(ctx context.Context, tenantID, gcid, sql string) ([]string, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if strings.TrimSpace(gcid) == "" {
		return nil, fmt.Errorf("%w: gcid required", social.ErrInvalidArgument)
	}
	out := make([]string, 0, 8)
	err := r.run(ctx, tenantID, func(ctx context.Context, q Querier) error {
		rows, err := q.Query(ctx, sql, gcid)
		if err != nil {
			return fmt.Errorf("pg: social list query: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var g string
			if err := rows.Scan(&g); err != nil {
				return fmt.Errorf("pg: social list scan: %w", err)
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

// run stamps the explicit tenant onto ctx then applies the RLS session before
// the user queries (fail-loud on empty tenant — rls.ErrNoTenantContext).
func (r *SocialGraphRepo) run(ctx context.Context, tenantID string, fn func(ctx context.Context, q Querier) error) error {
	ctx = tracing.WithTenantID(ctx, strings.TrimSpace(tenantID))
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		return fn(ctx, q)
	})
}

func requireGCIDs(a, b string) error {
	if strings.TrimSpace(a) == "" || strings.TrimSpace(b) == "" {
		return fmt.Errorf("%w: gcid required", social.ErrInvalidArgument)
	}
	return nil
}
