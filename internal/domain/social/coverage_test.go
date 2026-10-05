// Package social_test holds the remaining coverage specs for the
// Relationship service: error propagation through every tx op, the
// read-delegation stubs, and the reverse-direction blocker lookup.
package social_test

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

// errTx is a RelationshipTx fake with injectable per-op errors. Unlike the
// fakeRelTx in relationship_test.go it never succeeds silently: every op can
// fail on demand, so each service error branch is reachable.
type errTx struct {
	pairStateErr    error
	insertFollowErr error
	deleteFollowErr error
	deleteFollowOn  int // fail DeleteFollow on the Nth call (0 = always)
	deleteFollowCnt int
	insertBlockErr  error
	deleteBlockErr  error
	enqueueErr      error
}

func (f *errTx) PairState(context.Context, string, string) (social.PairState, error) {
	return social.PairState{}, f.pairStateErr
}

func (f *errTx) InsertFollow(_ context.Context, e *social.Edge) (bool, *social.Edge, error) {
	if f.insertFollowErr != nil {
		return false, nil, f.insertFollowErr
	}
	return true, e, nil
}

func (f *errTx) DeleteFollow(context.Context, string, string) (bool, error) {
	f.deleteFollowCnt++
	if f.deleteFollowErr != nil && (f.deleteFollowOn == 0 || f.deleteFollowCnt == f.deleteFollowOn) {
		return false, f.deleteFollowErr
	}
	return true, nil
}

func (f *errTx) InsertBlock(_ context.Context, _ *social.Block) (bool, error) {
	if f.insertBlockErr != nil {
		return false, f.insertBlockErr
	}
	return true, nil
}

func (f *errTx) DeleteBlock(context.Context, string, string) (bool, error) {
	if f.deleteBlockErr != nil {
		return false, f.deleteBlockErr
	}
	return true, nil
}

func (f *errTx) Enqueue(context.Context, social.RelationshipEvent) error { return f.enqueueErr }

type errRelStore struct {
	tx  social.RelationshipTx
	err error
}

func (s *errRelStore) RunRelationship(_ context.Context, _ string, fn func(context.Context, social.RelationshipTx) error) error {
	if s.err != nil {
		return s.err
	}
	return fn(context.Background(), s.tx)
}

func newErrService(t *testing.T, tx *errTx) *social.RelationshipService {
	t.Helper()
	svc, err := social.NewRelationshipService(social.RelationshipServiceConfig{
		Store: &errRelStore{tx: tx},
		Reads: &fakeGraphQueries{},
	})
	if err != nil {
		t.Fatalf("NewRelationshipService: %v", err)
	}
	return svc
}

// -----------------------------------------------------------------------------
// read delegations (ADR-230 D3 — no traversal logic in the service)
// -----------------------------------------------------------------------------

func TestRelationshipService_ReadsDelegateToGraphQueries(t *testing.T) {
	reads := &fakeGraphQueries{friends: []string{"friend-1"}}
	svc, err := social.NewRelationshipService(social.RelationshipServiceConfig{
		Store: &fakeRelStore{tx: &fakeRelTx{}},
		Reads: reads,
	})
	if err != nil {
		t.Fatalf("NewRelationshipService: %v", err)
	}
	ctx := context.Background()

	got, err := svc.FollowingGCIDs(ctx, tenantA, gcidA)
	if err != nil || !reflect.DeepEqual(got, []string{"following"}) {
		t.Errorf("FollowingGCIDs = %v, %v; want [following]", got, err)
	}
	got, err = svc.FollowersGCIDs(ctx, tenantA, gcidA)
	if err != nil || !reflect.DeepEqual(got, []string{"followers"}) {
		t.Errorf("FollowersGCIDs = %v, %v; want [followers]", got, err)
	}
	got, err = svc.BlockedBy(ctx, tenantA, gcidA)
	if err != nil || !reflect.DeepEqual(got, []string{"blocked"}) {
		t.Errorf("BlockedBy = %v, %v; want [blocked]", got, err)
	}
	got, err = svc.FriendSet(ctx, tenantA, gcidA)
	if err != nil || !reflect.DeepEqual(got, []string{"friend-1"}) {
		t.Errorf("FriendSet = %v, %v; want [friend-1]", got, err)
	}
}

// -----------------------------------------------------------------------------
// Follow error propagation
// -----------------------------------------------------------------------------

func TestRelationship_Follow_PairStateErrorPropagates(t *testing.T) {
	svc := newErrService(t, &errTx{pairStateErr: errors.New("pair state failed")})
	if _, _, err := svc.Follow(context.Background(), tenantA, gcidA, gcidB); err == nil {
		t.Fatal("expected PairState error to propagate")
	}
}

func TestRelationship_Follow_InsertFollowErrorPropagates(t *testing.T) {
	svc := newErrService(t, &errTx{insertFollowErr: errors.New("insert failed")})
	_, created, err := svc.Follow(context.Background(), tenantA, gcidA, gcidB)
	if err == nil || created {
		t.Fatalf("expected InsertFollow error propagation, got created=%v err=%v", created, err)
	}
}

func TestRelationship_Follow_EnqueueErrorPropagates(t *testing.T) {
	svc := newErrService(t, &errTx{enqueueErr: errors.New("outbox failed")})
	if _, _, err := svc.Follow(context.Background(), tenantA, gcidA, gcidB); err == nil {
		t.Fatal("expected enqueue error to propagate")
	}
}

// -----------------------------------------------------------------------------
// Unfollow error propagation + guard
// -----------------------------------------------------------------------------

func TestRelationship_Unfollow_RejectsBlankFollowee(t *testing.T) {
	svc := newErrService(t, &errTx{})
	if removed, err := svc.Unfollow(context.Background(), tenantA, gcidA, "  "); err == nil || removed {
		t.Fatalf("expected blank-followee error, got removed=%v err=%v", removed, err)
	}
}

func TestRelationship_Unfollow_DeleteFollowErrorPropagates(t *testing.T) {
	svc := newErrService(t, &errTx{deleteFollowErr: errors.New("delete failed")})
	if _, err := svc.Unfollow(context.Background(), tenantA, gcidA, gcidB); err == nil {
		t.Fatal("expected DeleteFollow error to propagate")
	}
}

func TestRelationship_Unfollow_EnqueueErrorPropagates(t *testing.T) {
	svc := newErrService(t, &errTx{enqueueErr: errors.New("outbox failed")})
	if _, err := svc.Unfollow(context.Background(), tenantA, gcidA, gcidB); err == nil {
		t.Fatal("expected enqueue error to propagate")
	}
}

// -----------------------------------------------------------------------------
// Block error propagation + guard
// -----------------------------------------------------------------------------

func TestRelationship_Block_RejectsBlankBlocker(t *testing.T) {
	svc := newErrService(t, &errTx{})
	if err := svc.Block(context.Background(), tenantA, "  ", gcidB); err == nil {
		t.Fatal("expected blank-blocker error (NewBlock guard)")
	}
}

func TestRelationship_Block_InsertBlockErrorPropagates(t *testing.T) {
	svc := newErrService(t, &errTx{insertBlockErr: errors.New("insert failed")})
	if err := svc.Block(context.Background(), tenantA, gcidA, gcidB); err == nil {
		t.Fatal("expected InsertBlock error to propagate")
	}
}

func TestRelationship_Block_FirstDeleteFollowErrorPropagates(t *testing.T) {
	svc := newErrService(t, &errTx{deleteFollowErr: errors.New("delete failed")})
	if err := svc.Block(context.Background(), tenantA, gcidA, gcidB); err == nil {
		t.Fatal("expected the first severance DeleteFollow error to propagate")
	}
}

func TestRelationship_Block_SecondDeleteFollowErrorPropagates(t *testing.T) {
	svc := newErrService(t, &errTx{deleteFollowErr: errors.New("delete failed"), deleteFollowOn: 2})
	if err := svc.Block(context.Background(), tenantA, gcidA, gcidB); err == nil {
		t.Fatal("expected the second severance DeleteFollow error to propagate")
	}
}

func TestRelationship_Block_EnqueueErrorPropagates(t *testing.T) {
	svc := newErrService(t, &errTx{enqueueErr: errors.New("outbox failed")})
	if err := svc.Block(context.Background(), tenantA, gcidA, gcidB); err == nil {
		t.Fatal("expected enqueue error to propagate")
	}
}

// -----------------------------------------------------------------------------
// Unblock error propagation + guard
// -----------------------------------------------------------------------------

func TestRelationship_Unblock_RejectsBlankGCID(t *testing.T) {
	svc := newErrService(t, &errTx{})
	if _, err := svc.Unblock(context.Background(), tenantA, gcidA, ""); err == nil {
		t.Fatal("expected blank-blocked error")
	}
}

func TestRelationship_Unblock_DeleteBlockErrorPropagates(t *testing.T) {
	svc := newErrService(t, &errTx{deleteBlockErr: errors.New("delete failed")})
	if _, err := svc.Unblock(context.Background(), tenantA, gcidA, gcidB); err == nil {
		t.Fatal("expected DeleteBlock error to propagate")
	}
}

func TestRelationship_Unblock_EnqueueErrorPropagates(t *testing.T) {
	svc := newErrService(t, &errTx{enqueueErr: errors.New("outbox failed")})
	if _, err := svc.Unblock(context.Background(), tenantA, gcidA, gcidB); err == nil {
		t.Fatal("expected enqueue error to propagate")
	}
}

// -----------------------------------------------------------------------------
// Graph: reverse-direction blocker lookup
// -----------------------------------------------------------------------------

func TestGraph_BlockersOf_ReverseDirection(t *testing.T) {
	g := social.NewGraph()
	if err := g.Block("t1", gcidA, gcidB); err != nil {
		t.Fatalf("block: %v", err)
	}
	if err := g.Block("t1", gcidC, gcidB); err != nil {
		t.Fatalf("block: %v", err)
	}
	got := g.BlockersOf(gcidB)
	sort.Strings(got)
	if !reflect.DeepEqual(got, []string{gcidA, gcidC}) {
		t.Fatalf("BlockersOf = %v, want [%s %s]", got, gcidA, gcidC)
	}
	if got := g.BlockersOf(gcidA); len(got) != 0 {
		t.Fatalf("BlockersOf for a non-blocked gcid = %v, want empty", got)
	}
}