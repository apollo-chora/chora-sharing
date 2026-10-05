// relationship.go — the ADR-230 Relationship aggregate: entities, errors,
// lifecycle events, and the persistence + read ports (B-lite.1, CHO-2119).
//
// The aggregate owns person↔person connection state per tenant across two
// planes: directional follow edges (Edge) and directional blocks (Block).
// The friendship plane (friend requests + friendships) has been removed;
// FriendSet + FriendSuggestions stay on GraphQueries for the atom_reuse
// audience tier + GetReuseContext gRPC contract (both return empty).
//
// Hard rules (per .claude/rules/ddd-enforcement.md):
//   - This file holds the domain only — NO HTTP, NO persistence.
//   - UUIDv7 IDs.
//   - GCIDs travel as opaque cross-domain UUIDs.
package social

import (
	"context"
	"errors"
	"time"
)

// Relationship errors. ErrConnectionNotPermitted is DELIBERATELY unspecific:
// it covers every blocked-pair refusal without disclosing block existence or
// direction (ADR-230 D1 — a blockee must not be able to confirm being
// blocked). Transports map it to 409 / FailedPrecondition verbatim.
var (
	ErrConnectionNotPermitted = errors.New("connection not permitted between these members")
)


// -----------------------------------------------------------------------------
// Lifecycle events (ADR-230 D4 — the relationship.*.v1 outbox spine)
// -----------------------------------------------------------------------------

// RelationshipEventKind names one lifecycle transition. The four kinds map
// 1:1 onto the chora.sharing.relationship.{kind}.v1 topics (adapter concern).
type RelationshipEventKind string

// The complete spine. Blocked carries severance semantics BY CONTRACT: a
// consumer/projector applying blocked must also remove both follow edges —
// no synthetic unfollowed events accompany a block (ADR-230 D1).
const (
	RelFollowed   RelationshipEventKind = "followed"
	RelUnfollowed RelationshipEventKind = "unfollowed"
	RelBlocked    RelationshipEventKind = "blocked"
	RelUnblocked  RelationshipEventKind = "unblocked"
)

// RelationshipEvent is one committed transition, enqueued onto the outbox in
// the SAME transaction as the state write (envelope invariant). Actor is the
// member who acted; Subject is the other member of the pair.
type RelationshipEvent struct {
	Kind        RelationshipEventKind
	TenantID    string
	ActorGCID   string
	SubjectGCID string
	OccurredAt  time.Time
}

// -----------------------------------------------------------------------------
// Ports
// -----------------------------------------------------------------------------

// PairState is the current relationship state between the caller and another
// member, loaded inside the transaction so the state machine decides against
// committed rows. BlockedEither reports block existence only — never
// direction (block privacy, ADR-230 D1).
type PairState struct {
	BlockedEither bool
}

// RelationshipTx is the transactional persistence surface the service drives
// inside one RunRelationship callback. The adapter carries the tenant (RLS
// session applied before the callback); ops take GCIDs only.
type RelationshipTx interface {
	PairState(ctx context.Context, caller, other string) (PairState, error)
	InsertFollow(ctx context.Context, e *Edge) (created bool, existing *Edge, err error)
	DeleteFollow(ctx context.Context, follower, followee string) (bool, error)
	InsertBlock(ctx context.Context, b *Block) (created bool, err error)
	DeleteBlock(ctx context.Context, blocker, blocked string) (bool, error)

	// Enqueue writes the event's outbox row in the SAME transaction as the
	// state mutation — atomic state-write + event-publish (envelope
	// invariant); the sharing outbox Dispatcher drains it to Pub/Sub.
	Enqueue(ctx context.Context, ev RelationshipEvent) error
}

// RelationshipStore opens one tenant-scoped transaction around fn. An empty
// tenant fails loud (rls.ErrNoTenantContext in the pg adapter) — never a
// silent zero-row read.
type RelationshipStore interface {
	RunRelationship(ctx context.Context, tenantID string, fn func(ctx context.Context, tx RelationshipTx) error) error
}
// FollowSuggestion is one "who to follow" candidate, surfaced by the hybrid
// discovery engine: interest-based ranking (shared profiler tags) PRIMARY,
// follow-graph proximity (mutual follows) SECONDARY. Unlike the old FoF
type FollowSuggestion struct {
	GCID           string   // candidate's GCID
	DisplayName    string   // candidate's display_name from profiler_profiles
	SharedTags     []string // tag slugs both user + candidate have (interest signal)
	MutualFollows  int      // count of caller's followees who follow this candidate (path count)
}

// SuggestionQueries is the hybrid discovery port. Implementations return
// candidates ranked by shared-tag count DESC then mutual-follows DESC,
// with exclusion filtering (self / already-followed / blocked-either-direction)
// applied IN-QUERY before the LIMIT — not in the handler — so the result set
// fills to the limit instead of under-filling after post-filter culling.
type SuggestionQueries interface {
	// FollowSuggestions returns up to `limit` candidates for gcid, hybrid-ranked.
	// Excludes: self, already-followed, blocked-either-direction. The caller's
	// own tenant scoping (RLS) + display_name enrichment happen at the repo layer.
	FollowSuggestions(ctx context.Context, tenantID, gcid string, limit int) ([]FollowSuggestion, error)
}
// FriendSuggestion is one bounded friend-of-friend candidate. The friendship
// write path is gone, so the set is always empty — the type stays for
// GraphQueries.FriendSuggestions interface parity.
type FriendSuggestion struct {
	GCID          string
	MutualFriends int
}

// GraphQueries is the sole doorway for graph-shaped reads (ADR-230 D3,
// upgrade seam 1). Postgres/CTE adapter now; a future Neo4j adapter swaps in
// behind this port with zero caller changes. No handler embeds traversal SQL.
//
// FriendSet + FriendSuggestions remain on the port for the atom_reuse
// audience tier (AudienceFriends) + GetReuseContext gRPC contract. The
// friendship write path is gone, so the friend set is always empty —
// "friends" audience atoms effectively strand every non-author grant
// (treated like private). The 'friends' visibility enum stays valid.
type GraphQueries interface {
	// FriendSet resolves the caller's friend set. Always empty now that the
	// friendship write path is removed — kept for the atom_reuse audience
	// tier + GetReuseContext gRPC contract (returns empty slice).
	FriendSet(ctx context.Context, tenantID, gcid string) ([]string, error)
	FollowingGCIDs(ctx context.Context, tenantID, gcid string) ([]string, error)
	FollowersGCIDs(ctx context.Context, tenantID, gcid string) ([]string, error)
	BlockedBy(ctx context.Context, tenantID, gcid string) ([]string, error)
	// FriendSuggestions returns bounded FoF candidates. Always empty now
	// that friendships are gone — kept for interface parity.
	FriendSuggestions(ctx context.Context, tenantID, gcid string, limit int) ([]FriendSuggestion, error)
}
