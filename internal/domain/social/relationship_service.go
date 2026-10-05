// relationship_service.go — the Relationship aggregate's application service
// (ADR-230 D1, B-lite.1, CHO-2119).
//
// One method = one tenant-scoped transaction: load PairState, decide, mutate,
// and Enqueue the matching relationship.*.v1 event — all inside the same
// RunRelationship callback so state write + event publish are atomic.
//
// The service satisfies BOTH transports' SocialGraph ports (identical
// Follow/Unfollow/Block/Unblock/read signatures to the WS-0 repo), so the
// composition root swaps it in with no handler changes; reads delegate to the
// GraphQueries port (ADR-230 D3 — no traversal logic here).
package social

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// RelationshipServiceConfig wires the service. Store + Reads are mandatory —
// a nil port is a composition-root bug and fails loud at construction.
type RelationshipServiceConfig struct {
	Store RelationshipStore
	Reads GraphQueries
	Now   func() time.Time // test seam; defaults to time.Now UTC
}

// RelationshipService owns every relationship state transition.
type RelationshipService struct {
	store RelationshipStore
	reads GraphQueries
	now   func() time.Time
}

// NewRelationshipService constructs the service, failing loud on missing
// ports (never a half-wired aggregate).
func NewRelationshipService(cfg RelationshipServiceConfig) (*RelationshipService, error) {
	if cfg.Store == nil {
		return nil, errors.New("social: RelationshipService requires a RelationshipStore")
	}
	if cfg.Reads == nil {
		return nil, errors.New("social: RelationshipService requires GraphQueries")
	}
	now := cfg.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &RelationshipService{store: cfg.Store, reads: cfg.Reads, now: now}, nil
}

// -----------------------------------------------------------------------------
// follow plane
// -----------------------------------------------------------------------------

// Follow inserts the (follower → followee) edge unless a block exists in
// either direction. Duplicate follow returns the original edge unchanged with
// created=false and emits nothing (events fire only on actual change).
func (s *RelationshipService) Follow(ctx context.Context, tenantID, follower, followee string) (*Edge, bool, error) {
	edge, err := NewEdge(tenantID, follower, followee)
	if err != nil {
		return nil, false, err
	}
	var (
		out     *Edge
		created bool
	)
	err = s.store.RunRelationship(ctx, tenantID, func(ctx context.Context, tx RelationshipTx) error {
		state, err := tx.PairState(ctx, follower, followee)
		if err != nil {
			return err
		}
		if state.BlockedEither {
			return ErrConnectionNotPermitted
		}
		c, existing, err := tx.InsertFollow(ctx, edge)
		if err != nil {
			return err
		}
		out, created = existing, c
		if !created {
			return nil
		}
		return tx.Enqueue(ctx, s.event(RelFollowed, tenantID, follower, followee))
	})
	if err != nil {
		return nil, false, err
	}
	return out, created, nil
}

// Unfollow removes the edge; emits unfollowed only when a row was removed.
func (s *RelationshipService) Unfollow(ctx context.Context, tenantID, follower, followee string) (bool, error) {
	if err := requirePair(follower, followee); err != nil {
		return false, err
	}
	var removed bool
	err := s.store.RunRelationship(ctx, tenantID, func(ctx context.Context, tx RelationshipTx) error {
		r, err := tx.DeleteFollow(ctx, follower, followee)
		if err != nil {
			return err
		}
		removed = r
		if !removed {
			return nil
		}
		return tx.Enqueue(ctx, s.event(RelUnfollowed, tenantID, follower, followee))
	})
	if err != nil {
		return false, err
	}
	return removed, nil
}

// -----------------------------------------------------------------------------
// block plane — severance
// -----------------------------------------------------------------------------

// Block writes the (blocker → blocked) edge and SEVERS the pair in the same
// transaction: both follow directions are deleted. Emits ONLY blocked.v1 —
// severance is part of that event's contract; no synthetic unfollowed noise
// for actions nobody took (ADR-230 D1). Re-block of an existing block is a
// silent no-op.
func (s *RelationshipService) Block(ctx context.Context, tenantID, blocker, blocked string) error {
	b, err := NewBlock(tenantID, blocker, blocked)
	if err != nil {
		return err
	}
	return s.store.RunRelationship(ctx, tenantID, func(ctx context.Context, tx RelationshipTx) error {
		created, err := tx.InsertBlock(ctx, b)
		if err != nil {
			return err
		}
		if !created {
			return nil // already blocked — idempotent, nothing re-severed
		}
		if _, err := tx.DeleteFollow(ctx, blocker, blocked); err != nil {
			return err
		}
		if _, err := tx.DeleteFollow(ctx, blocked, blocker); err != nil {
			return err
		}
		return tx.Enqueue(ctx, s.event(RelBlocked, tenantID, blocker, blocked))
	})
}

// Unblock deletes the block edge and restores NOTHING — follows must be
// re-earned from scratch (ADR-230 D1).
func (s *RelationshipService) Unblock(ctx context.Context, tenantID, blocker, blocked string) (bool, error) {
	if err := requirePair(blocker, blocked); err != nil {
		return false, err
	}
	var removed bool
	err := s.store.RunRelationship(ctx, tenantID, func(ctx context.Context, tx RelationshipTx) error {
		r, err := tx.DeleteBlock(ctx, blocker, blocked)
		if err != nil {
			return err
		}
		removed = r
		if !removed {
			return nil
		}
		return tx.Enqueue(ctx, s.event(RelUnblocked, tenantID, blocker, blocked))
	})
	if err != nil {
		return false, err
	}
	return removed, nil
}

// -----------------------------------------------------------------------------
// reads — delegate to the GraphQueries port (ADR-230 D3)
// -----------------------------------------------------------------------------

// FollowingGCIDs lists the followee GCIDs for gcid within the tenant.
func (s *RelationshipService) FollowingGCIDs(ctx context.Context, tenantID, gcid string) ([]string, error) {
	return s.reads.FollowingGCIDs(ctx, tenantID, gcid)
}

// FollowersGCIDs lists the follower GCIDs for gcid within the tenant.
func (s *RelationshipService) FollowersGCIDs(ctx context.Context, tenantID, gcid string) ([]string, error) {
	return s.reads.FollowersGCIDs(ctx, tenantID, gcid)
}

// BlockedBy lists the GCIDs gcid has blocked within the tenant.
func (s *RelationshipService) BlockedBy(ctx context.Context, tenantID, gcid string) ([]string, error) {
	return s.reads.BlockedBy(ctx, tenantID, gcid)
}

// FriendSet resolves gcid's friend set within the tenant.
func (s *RelationshipService) FriendSet(ctx context.Context, tenantID, gcid string) ([]string, error) {
	return s.reads.FriendSet(ctx, tenantID, gcid)
}

// FriendSuggestions lists gcid's bounded FoF candidates (B-lite.3 —
// graph-shaped read, delegates to the D3 seam like FriendSet).
func (s *RelationshipService) FriendSuggestions(ctx context.Context, tenantID, gcid string, limit int) ([]FriendSuggestion, error) {
	return s.reads.FriendSuggestions(ctx, tenantID, gcid, limit)
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

func (s *RelationshipService) event(kind RelationshipEventKind, tenantID, actor, subject string) RelationshipEvent {
	return RelationshipEvent{
		Kind:        kind,
		TenantID:    tenantID,
		ActorGCID:   actor,
		SubjectGCID: subject,
		OccurredAt:  s.now(),
	}
}

func requirePair(a, b string) error {
	if strings.TrimSpace(a) == "" || strings.TrimSpace(b) == "" {
		return fmt.Errorf("%w: gcid required", ErrInvalidArgument)
	}
	return nil
}
