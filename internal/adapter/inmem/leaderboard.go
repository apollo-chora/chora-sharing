package inmem

import "github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"

// LeaderboardReader wraps the domain's in-memory *leaderboard.Ranker.
// The leaderboard domain does not define a LeaderboardReader interface —
// Ranker is itself the in-memory aggregator (keyed by scope/period/gcid).
// This wrapper exists so the composite Store exposes a uniform inmem type
// for every domain port.
type LeaderboardReader struct {
	*leaderboard.Ranker
}

// NewLeaderboardReader returns a LeaderboardReader wrapping a fresh Ranker.
func NewLeaderboardReader() *LeaderboardReader {
	return &LeaderboardReader{Ranker: leaderboard.NewRanker()}
}
