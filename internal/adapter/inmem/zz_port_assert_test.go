package inmem

import "github.com/apollo-chora/chora-sharing/internal/domain/post"

// Compile-time proof that BOTH adapters satisfy the ONE domain port. Before
// CHO-2193/W0-F1 there was no port at all, so nothing could be asserted and
// nothing could be substituted.
var _ post.PostRepo = (*PostRepo)(nil)
