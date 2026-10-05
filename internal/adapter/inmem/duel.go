package inmem

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
)

var ErrDuelNotFound = errors.New("duel not found")

type DuelRepo struct {
	mu         sync.RWMutex
	byID       map[string]*duel.Duel
	ratings    map[string]int
	wins       map[string]int
	losses     map[string]int
	draws      map[string]int
	peakELO    map[string]int
	eloApplied map[string]bool
}

func NewDuelRepo() *DuelRepo {
	return &DuelRepo{
		byID:       make(map[string]*duel.Duel),
		ratings:    make(map[string]int),
		wins:       make(map[string]int),
		losses:     make(map[string]int),
		draws:      make(map[string]int),
		peakELO:    make(map[string]int),
		eloApplied: make(map[string]bool),
	}
}

func (r *DuelRepo) SaveDuel(_ context.Context, d *duel.Duel) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byID[d.ID] = d
	return nil
}

func (r *DuelRepo) GetDuel(_ context.Context, duelID string) (*duel.Duel, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.byID[duelID]
	if !ok {
		return nil, ErrDuelNotFound
	}
	return d, nil
}

func (r *DuelRepo) GetDuelForUpdate(_ context.Context, duelID string) (*duel.Duel, error) {
	return r.GetDuel(nil, duelID)
}

// ListDuelsWithExpiredRounds returns in-progress duels for the given tenant
// that have at least one unresolved round whose DeadlineAt < now. WS3
// round-timer sweep. Per-tenant to mirror the pg adapter's RLS-scoped query.
func (r *DuelRepo) ListDuelsWithExpiredRounds(_ context.Context, tenantID string, now time.Time) ([]*duel.Duel, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*duel.Duel
	for _, d := range r.byID {
		if d.TenantID != tenantID {
			continue
		}
		if d.Status != duel.StatusInProgress {
			continue
		}
		for _, rd := range d.Rounds {
			if rd.ResolvedAt == nil && rd.DeadlineAt != nil && now.After(*rd.DeadlineAt) {
				out = append(out, d)
				break
			}
		}
	}
	return out, nil
}

func (r *DuelRepo) ListDuels(_ context.Context, tenantID, gcid, status string, limit int, cursor string) ([]*duel.Duel, string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if limit <= 0 {
		limit = 20
	}
	var duels []*duel.Duel
	for _, d := range r.byID {
		if d.TenantID != tenantID {
			continue
		}
		if d.ChallengerGCID != gcid && d.OpponentGCID != gcid {
			continue
		}
		if status != "" && string(d.Status) != status {
			continue
		}
		duels = append(duels, d)
	}
	sort.Slice(duels, func(i, j int) bool {
		return duels[i].CreatedAt.After(duels[j].CreatedAt)
	})
	start := 0
	if cursor != "" {
		for i, d := range duels {
			if d.ID == cursor {
				start = i + 1
				break
			}
		}
	}
	var nextCursor string
	if start >= len(duels) {
		duels = nil
	} else {
		end := start + limit
		if end > len(duels) {
			end = len(duels)
		}
		page := duels[start:end]
		if end < len(duels) {
			nextCursor = page[len(page)-1].ID
		}
		duels = page
	}
	return duels, nextCursor, nil
}

func (r *DuelRepo) ResolveRound(_ context.Context, d *duel.Duel, _ int, _ string, _ duel.RoundResolution) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d == nil {
		return duel.ErrInvalidArgument
	}
	if _, ok := r.byID[d.ID]; !ok {
		return ErrDuelNotFound
	}
	// Store the mutated duel so subsequent GetDuel calls reflect the
	// domain mutations (scores, status, winner). The pg adapter persists
	// via SQL; inmem persists by reference swap.
	r.byID[d.ID] = d
	return nil
}

func (r *DuelRepo) StampRoundDeadline(_ context.Context, duelID string, roundNo int, deadline time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.byID[duelID]
	if !ok {
		return ErrDuelNotFound
	}
	if roundNo < 1 || roundNo > len(d.Rounds) {
		return duel.ErrRoundOutOfRange
	}
	rd := &d.Rounds[roundNo-1]
	// Idempotent: never reset an already-stamped timer (second player to
	// connect must not restart the countdown).
	if rd.ResolvedAt == nil && rd.DeadlineAt == nil {
		rd.DeadlineAt = &deadline
	}
	return nil
}

func (r *DuelRepo) ApplyELO(_ context.Context, d *duel.Duel, kFactor int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.eloApplied[d.ID] {
		return nil
	}
	r.eloApplied[d.ID] = true
	if d.Scope != duel.ScopeRanked {
		return nil
	}
	// WS1: ratings are scoped per-category. The duel's Category determines
	// which bucket the ELO writes to. "overall" is the default.
	cat := d.Category
	if cat == "" {
		cat = "overall"
	}
	if d.WinnerGCID == "" {
		// Draw: both players move toward midpoint.
		rA := r.ratingOrDefault(catKey(d.ChallengerGCID, cat))
		rB := r.ratingOrDefault(catKey(d.OpponentGCID, cat))
		expA := 1.0 / (1.0 + duel.ExpApprox(float64(rB-rA)/400.0))
		drawDelta := float64(kFactor) * (0.5 - expA)
		rANew := rA + int(drawDelta+0.5)
		rBNew := rB - int(drawDelta+0.5)
		r.setRating(catKey(d.ChallengerGCID, cat), rANew)
		r.setRating(catKey(d.OpponentGCID, cat), rBNew)
		r.draws[catKey(d.ChallengerGCID, cat)]++
		r.draws[catKey(d.OpponentGCID, cat)]++
		return nil
	}
	loserGCID := d.OpponentGCID
	if d.WinnerGCID == d.OpponentGCID {
		loserGCID = d.ChallengerGCID
	}
	winnerRating := r.ratingOrDefault(catKey(d.WinnerGCID, cat))
	loserRating := r.ratingOrDefault(catKey(loserGCID, cat))
	winnerNew, loserNew := duel.ComputeELO(winnerRating, loserRating, kFactor)
	r.setRating(catKey(d.WinnerGCID, cat), winnerNew)
	r.setRating(catKey(loserGCID, cat), loserNew)
	r.wins[catKey(d.WinnerGCID, cat)]++
	r.losses[catKey(loserGCID, cat)]++
	return nil
}

// catKey builds the composite map key for per-category ratings (WS1).
// The "overall" category is the default bucket (backward-compatible).
func catKey(gcid, category string) string {
	return gcid + "|" + category
}

// gcidFromCatKey extracts the GCID from a composite key.
func gcidFromCatKey(key string) string {
	for i := 0; i < len(key); i++ {
		if key[i] == '|' {
			return key[:i]
		}
	}
	return key
}

func (r *DuelRepo) ratingOrDefault(key string) int {
	v := r.ratings[key]
	if v == 0 {
		return 1200
	}
	return v
}

func (r *DuelRepo) setRating(key string, rating int) {
	r.ratings[key] = rating
	if rating > r.peakELO[key] {
		r.peakELO[key] = rating
	}
}

func (r *DuelRepo) GetRating(_ context.Context, _ string, gcid string) (int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ratingOrDefault(catKey(gcid, "overall")), nil
}

func (r *DuelRepo) GetRatingStats(_ context.Context, _ string, gcid string) (duel.RatingStats, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key := catKey(gcid, "overall")
	return duel.RatingStats{
		GCID:    gcid,
		Rating:  r.ratingOrDefault(key),
		Wins:    r.wins[key],
		Losses:  r.losses[key],
		Draws:   r.draws[key],
		PeakELO: r.peakOrDefault(key),
	}, nil
}

// GetRatingForCategory returns the caller's ELO for a specific category (WS1).
func (r *DuelRepo) GetRatingForCategory(_ context.Context, _ string, gcid, category string) (int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if category == "" {
		category = "overall"
	}
	return r.ratingOrDefault(catKey(gcid, category)), nil
}

// GetRatingStatsForCategory returns the caller's W/L/D record for a specific
// category (WS1).
func (r *DuelRepo) GetRatingStatsForCategory(_ context.Context, _ string, gcid, category string) (duel.RatingStats, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if category == "" {
		category = "overall"
	}
	key := catKey(gcid, category)
	return duel.RatingStats{
		GCID:    gcid,
		Rating:  r.ratingOrDefault(key),
		Wins:    r.wins[key],
		Losses:  r.losses[key],
		Draws:   r.draws[key],
		PeakELO: r.peakOrDefault(key),
	}, nil
}

func (r *DuelRepo) TopRatings(_ context.Context, tenantID string, limit int) ([]duel.RatingStats, error) {
	return r.topRatingsForCategory(tenantID, "overall", limit)
}

// TopRatingsForCategory returns the top-N players for a specific category (WS1).
func (r *DuelRepo) TopRatingsForCategory(_ context.Context, tenantID string, category string, limit int) ([]duel.RatingStats, error) {
	if category == "" {
		category = "overall"
	}
	return r.topRatingsForCategory(tenantID, category, limit)
}

func (r *DuelRepo) topRatingsForCategory(_ string, category string, limit int) ([]duel.RatingStats, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	suffix := "|" + category
	var results []duel.RatingStats
	for key := range r.ratings {
		// Only keys matching this category.
		if len(key) <= len(suffix) || key[len(key)-len(suffix):] != suffix {
			continue
		}
		gcid := gcidFromCatKey(key)
		results = append(results, duel.RatingStats{
			GCID:    gcid,
			Rating:  r.ratingOrDefault(key),
			Wins:    r.wins[key],
			Losses:  r.losses[key],
			Draws:   r.draws[key],
			PeakELO: r.peakOrDefault(key),
		})
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Rating != results[j].Rating {
			return results[i].Rating > results[j].Rating
		}
		return results[i].GCID < results[j].GCID
	})
	if len(results) > limit {
		results = results[:limit]
	}
	return results, nil
}

func (r *DuelRepo) peakOrDefault(key string) int {
	v := r.peakELO[key]
	if v == 0 {
		return 1200
	}
	return v
}

func (r *DuelRepo) Rating(gcid string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ratings[catKey(gcid, "overall")]
}

func (r *DuelRepo) FindNearbyRatings(_ context.Context, _ string, callerGCID string, limit int) ([]duel.RatingStats, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if limit <= 0 || limit > 50 {
		limit = 10
	}
	callerELO := r.ratingOrDefault(catKey(callerGCID, "overall"))
	var results []duel.RatingStats
	for key := range r.ratings {
		// Only overall-bucket keys (FindNearbyRatings is not category-scoped).
		if len(key) <= len("|overall") || key[len(key)-len("|overall"):] != "|overall" {
			continue
		}
		gcid := gcidFromCatKey(key)
		if gcid == callerGCID {
			continue
		}
		results = append(results, duel.RatingStats{
			GCID:    gcid,
			Rating:  r.ratingOrDefault(key),
			Wins:    r.wins[key],
			Losses:  r.losses[key],
			Draws:   r.draws[key],
			PeakELO: r.peakOrDefault(key),
		})
	}
	sort.Slice(results, func(i, j int) bool {
		di := abs(results[i].Rating - callerELO)
		dj := abs(results[j].Rating - callerELO)
		if di != dj {
			return di < dj
		}
		return results[i].GCID < results[j].GCID
	})
	if len(results) > limit {
		results = results[:limit]
	}
	return results, nil
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

var _ = time.Now
