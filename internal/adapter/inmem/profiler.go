package inmem

import (
	"context"
	"sync"

	"github.com/apollo-chora/chora-sharing/internal/domain/profiler"
)

type ProfilerRepo struct {
	mu      sync.RWMutex
	byGCID  map[string]*profiler.Profile
}

func NewProfilerRepo() *ProfilerRepo {
	return &ProfilerRepo{byGCID: make(map[string]*profiler.Profile)}
}

func (r *ProfilerRepo) SaveProfile(_ context.Context, p *profiler.Profile) error {
	if p == nil {
		return ErrProfileNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byGCID[p.GCID] = cloneProfile(p)
	return nil
}

func (r *ProfilerRepo) GetProfile(_ context.Context, gcid string) (*profiler.Profile, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.byGCID[gcid]
	if !ok {
		return nil, ErrProfileNotFound
	}
	return cloneProfile(p), nil
}

// ResolveDisplayNames returns an empty map for the in-memory repo —
// display_name is not stored on profiler.Profile (it's a JOIN column
// from profiler_profiles in the pg adapter). The no-DB path has no
// display names; the leaderboard handler falls back to shortGcid.
func (r *ProfilerRepo) ResolveDisplayNames(_ context.Context, _ []string) (map[string]string, error) {
	return map[string]string{}, nil
}
func cloneProfile(p *profiler.Profile) *profiler.Profile {
	if p == nil {
		return nil
	}
	cp := *p
	cp.Tags = append([]profiler.InterestTag(nil), p.Tags...)
	cp.CourseTitles = append([]string(nil), p.CourseTitles...)
	return &cp
}
