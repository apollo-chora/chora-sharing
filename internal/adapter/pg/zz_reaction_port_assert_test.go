package pg

import "github.com/apollo-chora/chora-sharing/internal/domain/reaction"

// Compile-time proof the pg adapter satisfies the domain port.
var _ reaction.ReactionRepo = (*ReactionRepo)(nil)
