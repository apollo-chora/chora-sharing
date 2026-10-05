// profiler_repo.go — pgx-backed profiler.ProfileRepo.
//
// Maps the Profile aggregate to profiler_profiles. RLS is applied on every
// method via qToExecer(q) before any user query, per multi-tenant-rls + the
// runtime.go contract.
//
// Schema: migrations/0037_profiler_profiles.up.sql.
package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-sharing/internal/domain/profiler"
)

const (
	sqlProfilerUpsert = `
INSERT INTO profiler_profiles
    (gcid, tenant_id, bio, tags, course_titles, proficiency, display_name, created_at, updated_at)
VALUES ($1, $2, $3, $4::jsonb, $5, $6::jsonb, $7, $8, $9)
ON CONFLICT (tenant_id, gcid) DO UPDATE SET
    bio           = EXCLUDED.bio,
    tags          = EXCLUDED.tags,
    course_titles = EXCLUDED.course_titles,
    proficiency   = EXCLUDED.proficiency,
    display_name  = EXCLUDED.display_name,
    updated_at    = EXCLUDED.updated_at`

	sqlProfilerSelectByGCID = `
SELECT gcid, tenant_id, bio, tags, course_titles, proficiency, display_name, created_at, updated_at
FROM profiler_profiles
WHERE gcid = $1`
)

// ProfilerRepo implements profiler.ProfileRepo against Postgres.
type ProfilerRepo struct {
	tx TxRunner
}

// NewProfilerRepo constructs a ProfilerRepo bound to a TxRunner.
func NewProfilerRepo(tx TxRunner) *ProfilerRepo { return &ProfilerRepo{tx: tx} }

// Compile-time port assertion.
var _ profiler.ProfileRepo = (*ProfilerRepo)(nil)

// SaveProfile upserts a profile for (tenant_id, gcid).
func (r *ProfilerRepo) SaveProfile(ctx context.Context, p *profiler.Profile) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if p == nil {
		return profiler.ErrInvalidArgument
	}

	tagsJSON, err := json.Marshal(p.Tags)
	if err != nil {
		return fmt.Errorf("pg: marshal profiler tags: %w", err)
	}

	profJSON, err := json.Marshal(p.Proficiency)
	if err != nil {
		return fmt.Errorf("pg: marshal profiler proficiency: %w", err)
	}

	created := p.CreatedAt
	if created.IsZero() {
		created = time.Now().UTC()
	}
	updated := p.UpdatedAt
	if updated.IsZero() {
		updated = time.Now().UTC()
	}

	courses := p.CourseTitles
	if courses == nil {
		courses = []string{}
	}
	// display_name is populated by the generateProfile handler via an
	// identity GetMe gRPC call (stored in Profile.DisplayName). On
	// conflict, update display_name so a re-generation picks up name
	// changes. Pass "" when no identity client is wired (dev/tests).
	displayName := p.DisplayName
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		if _, err := q.Exec(ctx, sqlProfilerUpsert,
			p.GCID, p.TenantID, p.Bio, string(tagsJSON), courses,
			string(profJSON), displayName, created, updated,
		); err != nil {
			return fmt.Errorf("pg: upsert profiler profile: %w", err)
		}
		return nil
	})
}

// GetProfile loads a profile by gcid under the RLS tenant. Missing → (nil, nil).
func (r *ProfilerRepo) GetProfile(ctx context.Context, gcid string) (*profiler.Profile, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if gcid == "" {
		return nil, nil
	}

	var out *profiler.Profile
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		var (
			p            profiler.Profile
			tagsRaw      []byte
			profRaw      []byte
			courses      []string
			displayName  string
			createdAt    time.Time
			updatedAt    time.Time
		)
		scanErr := q.QueryRow(ctx, sqlProfilerSelectByGCID, gcid).Scan(
			&p.GCID, &p.TenantID, &p.Bio, &tagsRaw, &courses,
			&profRaw, &displayName, &createdAt, &updatedAt,
		)
		if scanErr != nil {
			if errors.Is(scanErr, pgx.ErrNoRows) {
				return nil
			}
			return fmt.Errorf("pg: select profiler profile: %w", scanErr)
		}
		if len(tagsRaw) > 0 {
			if err := json.Unmarshal(tagsRaw, &p.Tags); err != nil {
				return fmt.Errorf("pg: unmarshal profiler tags: %w", err)
			}
		}
		if p.Tags == nil {
			p.Tags = []profiler.InterestTag{}
		}
		if len(profRaw) > 0 {
			if err := json.Unmarshal(profRaw, &p.Proficiency); err != nil {
				return fmt.Errorf("pg: unmarshal profiler proficiency: %w", err)
			}
		}
		if courses == nil {
			courses = []string{}
		}
		p.CourseTitles = courses
		p.CreatedAt = createdAt.UTC()
		p.UpdatedAt = updatedAt.UTC()
	p.DisplayName = displayName // preserve display_name so the async conjurer re-save doesn't wipe it
		out = &p
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ResolveDisplayNames batch-resolves display_name for a set of gcids from
// profiler_profiles. Returns a map gcid→display_name; gcids with no profile
// or empty display_name are absent from the map. Used by the leaderboard
// handler to enrich entries with human-readable names (mirrors the duels
// TopRatings LEFT JOIN profiler_profiles pattern). RLS-scoped to the
// tenant in ctx.
func (r *ProfilerRepo) ResolveDisplayNames(ctx context.Context, gcids []string) (map[string]string, error) {
	out := make(map[string]string, len(gcids))
	if r == nil || r.tx == nil || len(gcids) == 0 {
		return out, nil
	}
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		// unnest the gcid slice into rows + join to profiler_profiles.
		// Cast to uuid[] — profiler_profiles.gcid is UUID (migration 0037),
		// so `pp.gcid = g.gcid` needs matching types (uuid = uuid). pgx
		// encodes []string to uuid[] fine. Explicit tenant_id filter
		// mirrors the TopRatings precedent (duel_repo.go:116) for safety
		// on ro/superuser paths (RLS covers app_rw already).
		rows, err := q.Query(ctx, `
SELECT g.gcid,
       COALESCE(NULLIF(pp.display_name, ''), sfe.display_name, '') AS display_name
FROM unnest($1::uuid[]) AS g(gcid)
LEFT JOIN profiler_profiles pp
  ON pp.gcid = g.gcid
 AND pp.tenant_id = current_setting('chora.tenant_id', true)::uuid
LEFT JOIN LATERAL (
    SELECT e.content->>'author_display_name' AS display_name
    FROM social_feed_entries e
    WHERE e.entry_type = 'share'
      AND e.actor_gcid = g.gcid
      AND e.tenant_id = current_setting('chora.tenant_id', true)::uuid
      AND e.content->>'author_display_name' != ''
    ORDER BY e.created_at DESC
    LIMIT 1
) sfe ON true
`, gcids)
		if err != nil {
			return fmt.Errorf("pg: resolve display names: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var gcid, name string
			if err := rows.Scan(&gcid, &name); err != nil {
				return fmt.Errorf("pg: scan display name: %w", err)
			}
			if name != "" {
				out[gcid] = name
			}
		}
		return rows.Err()
	})
	if err != nil {
		return out, err
	}
	return out, nil
}
