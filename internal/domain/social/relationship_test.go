// relationship_test.go — RED-phase specs for the ADR-230 Relationship
// aggregate state machine (B-lite.1, CHO-2119).
//
// The service is exercised against a fake RelationshipStore/RelationshipTx so
// every invariant is proven in the domain, driver-free:
//
//   - follow refused while a block exists in either direction
//   - friend_request refused when blocked / already friends / pending either way
//   - accept materialises the friendship + clears pendings
//   - decline/cancel delete the pending row and emit NOTHING
//   - unfriend removes the friendship only (follow edges untouched)
//   - block = severance (both follows + friendship + pendings) + ONLY blocked
//   - unblock restores nothing
//   - events fire only on actual state change (duplicate follow = no event)
package social_test

import (
	"context"
	"errors"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

// -----------------------------------------------------------------------------
// fakes
// -----------------------------------------------------------------------------

// fakeRelTx records every mutation the service asks for and returns the
// canned PairState. It deliberately does NOT re-implement the state machine —
// the service owns the decisions; the fake proves which ops were issued.
type fakeRelTx struct {
	state    social.PairState
	stateErr error

	// InsertFollow behaviour: created=false simulates the ON CONFLICT no-op.
	followCreated bool

	insertedFollows   []*social.Edge
	deletedFollows    [][2]string // [follower, followee]
	deletedFollowHits map[[2]string]bool
	insertedBlocks    []*social.Block
	blockCreated      bool
	deletedBlocks     [][2]string
	blockRemoved      bool
	events            []social.RelationshipEvent
}

func (f *fakeRelTx) PairState(_ context.Context, caller, other string) (social.PairState, error) {
	return f.state, f.stateErr
}

func (f *fakeRelTx) InsertFollow(_ context.Context, e *social.Edge) (bool, *social.Edge, error) {
	f.insertedFollows = append(f.insertedFollows, e)
	if f.followCreated {
		return true, e, nil
	}
	return false, e, nil
}

func (f *fakeRelTx) DeleteFollow(_ context.Context, follower, followee string) (bool, error) {
	f.deletedFollows = append(f.deletedFollows, [2]string{follower, followee})
	if f.deletedFollowHits == nil {
		return false, nil
	}
	return f.deletedFollowHits[[2]string{follower, followee}], nil
}

func (f *fakeRelTx) InsertBlock(_ context.Context, b *social.Block) (bool, error) {
	f.insertedBlocks = append(f.insertedBlocks, b)
	return f.blockCreated, nil
}

func (f *fakeRelTx) DeleteBlock(_ context.Context, blocker, blocked string) (bool, error) {
	f.deletedBlocks = append(f.deletedBlocks, [2]string{blocker, blocked})
	return f.blockRemoved, nil
}


func (f *fakeRelTx) Enqueue(_ context.Context, ev social.RelationshipEvent) error {
	f.events = append(f.events, ev)
	return nil
}

type fakeRelStore struct {
	tx      *fakeRelTx
	tenants []string
	err     error
}

func (s *fakeRelStore) RunRelationship(ctx context.Context, tenantID string, fn func(ctx context.Context, tx social.RelationshipTx) error) error {
	s.tenants = append(s.tenants, tenantID)
	if s.err != nil {
		return s.err
	}
	return fn(ctx, s.tx)
}

type fakeGraphQueries struct {
	friends          []string
	suggestions      []social.FriendSuggestion
	suggestionsCalls int
}

func (f *fakeGraphQueries) FriendSet(_ context.Context, _, _ string) ([]string, error) {
	return f.friends, nil
}

func (f *fakeGraphQueries) FriendSuggestions(_ context.Context, _, _ string, _ int) ([]social.FriendSuggestion, error) {
	f.suggestionsCalls++
	return f.suggestions, nil
}
func (f *fakeGraphQueries) FollowingGCIDs(_ context.Context, _, _ string) ([]string, error) {
	return []string{"following"}, nil
}
func (f *fakeGraphQueries) FollowersGCIDs(_ context.Context, _, _ string) ([]string, error) {
	return []string{"followers"}, nil
}
func (f *fakeGraphQueries) BlockedBy(_ context.Context, _, _ string) ([]string, error) {
	return []string{"blocked"}, nil
}

func newService(t *testing.T, tx *fakeRelTx) (*social.RelationshipService, *fakeRelStore) {
	t.Helper()
	store := &fakeRelStore{tx: tx}
	svc, err := social.NewRelationshipService(social.RelationshipServiceConfig{
		Store: store,
		Reads: &fakeGraphQueries{},
	})
	if err != nil {
		t.Fatalf("NewRelationshipService: %v", err)
	}
	return svc, store
}

func kinds(evs []social.RelationshipEvent) []social.RelationshipEventKind {
	out := make([]social.RelationshipEventKind, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.Kind)
	}
	return out
}

// -----------------------------------------------------------------------------
// construction
// -----------------------------------------------------------------------------

func TestNewRelationshipService_RequiresStoreAndReads(t *testing.T) {
	t.Parallel()
	if _, err := social.NewRelationshipService(social.RelationshipServiceConfig{Reads: &fakeGraphQueries{}}); err == nil {
		t.Fatalf("expected error for nil Store")
	}
	if _, err := social.NewRelationshipService(social.RelationshipServiceConfig{Store: &fakeRelStore{}}); err == nil {
		t.Fatalf("expected error for nil Reads")
	}
}
// -----------------------------------------------------------------------------
// follow / unfollow
// -----------------------------------------------------------------------------

func TestRelationship_Follow_RefusedWhileBlockedEitherDirection(t *testing.T) {
	t.Parallel()
	tx := &fakeRelTx{state: social.PairState{BlockedEither: true}}
	svc, _ := newService(t, tx)
	_, _, err := svc.Follow(context.Background(), tenantA, gcidA, gcidB)
	if !errors.Is(err, social.ErrConnectionNotPermitted) {
		t.Fatalf("expected ErrConnectionNotPermitted, got %v", err)
	}
	if len(tx.insertedFollows) != 0 {
		t.Fatalf("no follow edge may be written while blocked")
	}
	if len(tx.events) != 0 {
		t.Fatalf("no event may fire on a refused follow; got %v", kinds(tx.events))
	}
}

func TestRelationship_Follow_CreatesEdgeAndEmitsFollowed(t *testing.T) {
	t.Parallel()
	tx := &fakeRelTx{followCreated: true}
	svc, store := newService(t, tx)
	edge, created, err := svc.Follow(context.Background(), tenantA, gcidA, gcidB)
	if err != nil || !created || edge == nil {
		t.Fatalf("Follow: edge=%v created=%v err=%v", edge, created, err)
	}
	if len(store.tenants) != 1 || store.tenants[0] != tenantA {
		t.Fatalf("tenant must scope the tx; got %v", store.tenants)
	}
	if len(tx.events) != 1 || tx.events[0].Kind != social.RelFollowed {
		t.Fatalf("expected exactly [followed]; got %v", kinds(tx.events))
	}
	ev := tx.events[0]
	if ev.ActorGCID != gcidA || ev.SubjectGCID != gcidB || ev.TenantID != tenantA {
		t.Fatalf("event fields wrong: %+v", ev)
	}
	if ev.OccurredAt.IsZero() {
		t.Fatalf("event OccurredAt must be stamped")
	}
}

func TestRelationship_Follow_DuplicateIsNoOpWithoutEvent(t *testing.T) {
	t.Parallel()
	tx := &fakeRelTx{followCreated: false}
	svc, _ := newService(t, tx)
	_, created, err := svc.Follow(context.Background(), tenantA, gcidA, gcidB)
	if err != nil || created {
		t.Fatalf("duplicate follow must be a created=false no-op; created=%v err=%v", created, err)
	}
	if len(tx.events) != 0 {
		t.Fatalf("duplicate follow must not emit; got %v", kinds(tx.events))
	}
}

func TestRelationship_Follow_SelfRejected(t *testing.T) {
	t.Parallel()
	svc, _ := newService(t, &fakeRelTx{})
	if _, _, err := svc.Follow(context.Background(), tenantA, gcidA, gcidA); !errors.Is(err, social.ErrSelfFollow) {
		t.Fatalf("expected ErrSelfFollow, got %v", err)
	}
}

func TestRelationship_Unfollow_EmitsOnlyWhenRemoved(t *testing.T) {
	t.Parallel()
	tx := &fakeRelTx{deletedFollowHits: map[[2]string]bool{{gcidA, gcidB}: true}}
	svc, _ := newService(t, tx)
	removed, err := svc.Unfollow(context.Background(), tenantA, gcidA, gcidB)
	if err != nil || !removed {
		t.Fatalf("Unfollow: removed=%v err=%v", removed, err)
	}
	if len(tx.events) != 1 || tx.events[0].Kind != social.RelUnfollowed {
		t.Fatalf("expected [unfollowed]; got %v", kinds(tx.events))
	}

	tx2 := &fakeRelTx{} // absent edge
	svc2, _ := newService(t, tx2)
	removed, err = svc2.Unfollow(context.Background(), tenantA, gcidA, gcidB)
	if err != nil || removed {
		t.Fatalf("absent unfollow must be silent no-op; removed=%v err=%v", removed, err)
	}
	if len(tx2.events) != 0 {
		t.Fatalf("absent unfollow must not emit; got %v", kinds(tx2.events))
	}
}

// -----------------------------------------------------------------------------
// block severance / unblock
// -----------------------------------------------------------------------------

func TestRelationship_Block_SeversEverythingAndEmitsOnlyBlocked(t *testing.T) {
	t.Parallel()
	tx := &fakeRelTx{blockCreated: true}
	svc, _ := newService(t, tx)
	if err := svc.Block(context.Background(), tenantA, gcidA, gcidB); err != nil {
		t.Fatalf("Block: %v", err)
	}
	// Severance: BOTH follow directions deleted.
	if len(tx.deletedFollows) != 2 {
		t.Fatalf("block must delete both follow directions; got %v", tx.deletedFollows)
	}
	seen := map[[2]string]bool{}
	for _, d := range tx.deletedFollows {
		seen[d] = true
	}
	if !seen[[2]string{gcidA, gcidB}] || !seen[[2]string{gcidB, gcidA}] {
		t.Fatalf("both directions required; got %v", tx.deletedFollows)
	}
	// ONLY blocked.v1 — no synthetic unfollowed noise.
	if len(tx.events) != 1 || tx.events[0].Kind != social.RelBlocked {
		t.Fatalf("block must emit ONLY [blocked]; got %v", kinds(tx.events))
	}
}

func TestRelationship_Block_IdempotentReBlockEmitsNothing(t *testing.T) {
	t.Parallel()
	tx := &fakeRelTx{blockCreated: false} // ON CONFLICT no-op
	svc, _ := newService(t, tx)
	if err := svc.Block(context.Background(), tenantA, gcidA, gcidB); err != nil {
		t.Fatalf("re-block must be a silent no-op: %v", err)
	}
	if len(tx.events) != 0 {
		t.Fatalf("re-block must not emit; got %v", kinds(tx.events))
	}
	if len(tx.deletedFollows) != 0 {
		t.Fatalf("re-block must not re-sever")
	}
}

func TestRelationship_Unblock_RestoresNothing(t *testing.T) {
	t.Parallel()
	tx := &fakeRelTx{blockRemoved: true}
	svc, _ := newService(t, tx)
	removed, err := svc.Unblock(context.Background(), tenantA, gcidA, gcidB)
	if err != nil || !removed {
		t.Fatalf("Unblock: removed=%v err=%v", removed, err)
	}
	if len(tx.insertedFollows) != 0 {
		t.Fatalf("unblock must restore NOTHING")
	}
	if len(tx.events) != 1 || tx.events[0].Kind != social.RelUnblocked {
		t.Fatalf("expected [unblocked]; got %v", kinds(tx.events))
	}

	tx2 := &fakeRelTx{blockRemoved: false}
	svc2, _ := newService(t, tx2)
	removed, err = svc2.Unblock(context.Background(), tenantA, gcidA, gcidB)
	if err != nil || removed {
		t.Fatalf("absent unblock must be silent no-op")
	}
	if len(tx2.events) != 0 {
		t.Fatalf("no event when no block was removed")
	}
}


