// Package matchmaking is the pure-domain core for the pool-based duel
// matchmaking model.
//
// A Searcher represents a user actively looking for a duel opponent. The
// matchmaking_queue table (chora_sharing DB) is the sole source of truth
// for the pool; the Matchmaker adapter polls it per tenant and pairs
// searchers using MatchScore — shared interest tags weighted above ELO
// proximity.
//
// Owning domain : Content Sharing (chora_sharing DB).
// Owning team   : Team 1 (Content).
package matchmaking

import (
	"sort"
	"time"
)

// Searcher represents a user actively searching for a duel opponent.
type Searcher struct {
	GCID          string
	TenantID      string
	Proficiency   int
	InterestTags  []string
	EnteredAt     time.Time
	ExpiresAt     time.Time
	LastHeartbeat time.Time

	// QuestionCount is the duel length preset both players must agree on
	// (WS4). Matchmaker pairs only matching counts. Presets: 5 (Quick),
	// 10 (Standard), 15 (Marathon). Default 5 when omitted at enqueue.
	QuestionCount int

	// Category is the per-category scope for matchmaking + ratings (WS1).
	// Players pick a category when queueing (category-scoped matchmaking);
	// the matchmaker pairs only same-category entries. Duel resolution writes
	// the rating per category. "overall" is the default (un-categorized).
	Category string

	// Mode is the gameplay model the searcher wants: "classic" (sequential
	// FCFS rounds) or "blitz" (all questions open, race or timed). The
	// matchmaker pairs only same-mode entries. Empty defaults to "classic"
	// (backward-compatible with pre-blitz queue rows).
	Mode string

	// BlitzVariant is the blitz win condition ("timed" or "race"). Only
	// meaningful when Mode=="blitz". The matchmaker pairs only matching
	// variants so a timed-blitz player never faces a race-blitz player.
	BlitzVariant string

	// Status + MatchedDuelID reflect the queue row's persisted state.
	// Status is empty on enqueue (the DB defaults to 'finding'); reads
	// populate it. MatchedDuelID is set once the row is claimed.
	Status        QueueStatus
	MatchedDuelID string
}

// QueueStatus is the state of a user in the matchmaking queue.
type QueueStatus string

const (
	StatusFinding   QueueStatus = "finding"
	StatusMatched   QueueStatus = "matched"
	StatusCancelled QueueStatus = "cancelled"
	StatusExpired   QueueStatus = "expired"
	StatusAbandoned QueueStatus = "abandoned"
)

// MatchResult is returned when two searchers are paired.
type MatchResult struct {
	SearcherA     Searcher
	SearcherB     Searcher
	SharedTags    []string
	MatchedAt     time.Time
	MatchedDuelID string // assigned by the repo during atomic claim
}

// MatchScore ranks a candidate against a searcher.
// Higher = better match. The formula rewards shared interest tags (each
// worth 100 points) and penalises proficiency delta (absolute ELO distance).
func MatchScore(searcher, candidate Searcher) int {
	shared := SharedTags(searcher.InterestTags, candidate.InterestTags)
	delta := abs(searcher.Proficiency - candidate.Proficiency)
	return len(shared)*100 - delta
}

// IsExpired reports whether the searcher's 10-minute search window has elapsed.
func (s Searcher) IsExpired(now time.Time) bool {
	return now.After(s.ExpiresAt)
}

// IsHeartbeatStale reports whether the searcher has missed the heartbeat
// threshold (> staleThreshold since last heartbeat).
func (s Searcher) IsHeartbeatStale(now time.Time, staleThreshold time.Duration) bool {
	return now.Sub(s.LastHeartbeat) > staleThreshold
}

// SharedTags returns the intersection of a and b, sorted alphabetically.
func SharedTags(a, b []string) []string {
	set := make(map[string]struct{}, len(a))
	for _, t := range a {
		set[t] = struct{}{}
	}
	var shared []string
	for _, t := range b {
		if _, ok := set[t]; ok {
			shared = append(shared, t)
		}
	}
	sort.Strings(shared)
	return shared
}

// IsCompatible reports whether two searchers can be paired. The matchmaker
// must pair only same-mode + same-blitz-variant entries so a classic player
// never faces a blitz player, and a timed-blitz player never faces a
// race-blitz player. Empty Mode defaults to "classic" (backward-compatible
// with pre-blitz queue rows).
func IsCompatible(a, b Searcher) bool {
	modeA := a.Mode
	if modeA == "" {
		modeA = "classic"
	}
	modeB := b.Mode
	if modeB == "" {
		modeB = "classic"
	}
	if modeA != modeB {
		return false
	}
	if modeA == "blitz" && a.BlitzVariant != b.BlitzVariant {
		return false
	}
	return true
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
