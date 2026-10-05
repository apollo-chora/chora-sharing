package inmem

import (
	"github.com/apollo-chora/chora-sharing/internal/domain/reaction"
	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

// Store is the composite in-memory store wiring every domain Repo port
// into a single struct. Tests construct a Store via NewStore and inject
// individual repos (or the whole Store) into handlers.
//
// GrantRepo holds back-references to ProjectionRepo + ShareRepo so that
// ListEntitled can build the own ∪ free ∪ active-grant union without a
// cross-repo query at the caller site.
type Store struct {
	Posts        *PostRepo
	Shares       *ShareRepo
	Grants       *GrantRepo
	Royalties    *RoyaltyRepo
	Currency     *CurrencyRepo
	Projections  *ProjectionRepo
	Leaderboards *LeaderboardReader
	Comments     *CommentRepo
	Reactions    *reaction.Registry
	Graph        *social.Graph
}

// NewStore returns a fully-wired Store with empty repos. GrantRepo is
// cross-wired to ProjectionRepo + ShareRepo for ListEntitled.
func NewStore() *Store {
	projections := NewProjectionRepo()
	shares := NewShareRepo()
	return &Store{
		Posts:        NewPostRepo(),
		Shares:       shares,
		Grants:       NewGrantRepo(projections, shares),
		Royalties:    NewRoyaltyRepo(),
		Currency:     NewCurrencyRepo(),
		Projections:  projections,
		Leaderboards: NewLeaderboardReader(),
		Comments:     NewCommentRepo(),
		Reactions:    reaction.NewRegistry(),
		Graph:        social.NewGraph(),
	}
}
