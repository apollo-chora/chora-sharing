// Package pg is the pgx-backed adapter for chora-sharing repository ports.
//
// This package ships the RLS-aware Postgres implementations of the sharing
// domain ports:
//   - atom_share.ShareRepo            (share_repo.go)
//   - grant.GrantRepo                 (grant_repo.go)
//   - grant.RoyaltyRepo               (royalty_repo.go)
//   - currency.CurrencyRepo          (currency_repo.go)
//   - atom_projection.Reader + Writer (projection_repo.go)
//   - comment.CommentRepo             (comment_repo.go)
//
// Every repository method follows the runtime.go contract: obtain a Querier
// from the TxRunner, call rls.ApplySession via qToExecer(q) to set
// chora.tenant_id (SET LOCAL, transaction-scoped) BEFORE any user query, then
// issue the SQL. A repo constructed without a TxRunner returns
// ErrNotImplemented — the cmd/server bootstrap wires either the pgx TxRunner
// (production) or the in-memory adapter (dev/tests) at the composition root.
//
// NOT YET MIGRATED to pg (still on the in-memory adapter under
// internal/adapter/inmem/):
//   - post.Post — no domain-level PostRepo port exists; the HTTP layer's
//     narrow PostStore interface is currently satisfied by inmem.PostRepo.
//   - reaction.Registry — a concrete in-process struct (not a port); the
//     HTTP layer's ReactionStore interface wraps it directly.
//   - leaderboard.LeaderboardReader — the RankerReader in-process double
//     (leaderboard.RankerReader) satisfies it; the pg view-backed reader is
//     a future bring-up.
//   - social.Graph — the in-process follow-edge struct; no pg port today.
//
// Cross-DB queries forbidden — chora-sharing reads only chora_sharing.
// Inter-domain side effects flow through Pub/Sub via the outbox-backed
// Bus (see ../events/outbox_bus.go).
package pg
