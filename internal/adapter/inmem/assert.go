package inmem

import (
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
	"github.com/apollo-chora/chora-sharing/internal/domain/comment"
	"github.com/apollo-chora/chora-sharing/internal/domain/currency"
	"github.com/apollo-chora/chora-sharing/internal/domain/grant"
)

// Compile-time interface-satisfaction assertions. If any adapter drifts from
// its domain port, the build breaks here at the var declaration rather than
// at a distant call-site.
var (
	_ atom_share.ShareRepo                 = (*ShareRepo)(nil)
	_ atom_projection.AtomProjectionReader = (*ProjectionRepo)(nil)
	_ atom_projection.AtomProjectionWriter = (*ProjectionRepo)(nil)
	_ grant.GrantRepo                      = (*GrantRepo)(nil)
	_ grant.RoyaltyRepo                    = (*RoyaltyRepo)(nil)
	_ currency.CurrencyRepo                = (*CurrencyRepo)(nil)
	_ comment.CommentRepo                  = (*CommentRepo)(nil)
)
