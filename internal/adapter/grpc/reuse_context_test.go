// reuse_context_test.go — GetReuseContext RPC (ADR-229 WS-0, CHO-2102).
//
// The RPC returns the caller's reuse context for chora-creation's picker
// disjuncts: friend set (mutual follows minus blocks) + active-grant atom
// ids. Hand-written fakes per the package convention (no mocks).
package grpcadapter

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sharingv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/sharing/v1"

	"github.com/apollo-chora/chora-sharing/internal/domain/grant"
	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

const (
	reuseTenant = "01970000-0000-7000-a000-000000000001"
	reuseGCID   = "01970000-0000-7000-c000-00000000000a"
	friendGCID  = "01970000-0000-7000-c000-00000000000b"
)

// fakeFriendReader is a hand-written social.GraphQueries with only the
// FriendSet surface live (GetReuseContext's read); the follow/block reads
// fail loud if unexpectedly called.
type fakeFriendReader struct {
	friends   map[string][]string // gcid → friend set
	err       error
	gotTenant string
}

func (f *fakeFriendReader) FriendSet(_ context.Context, tenantID, gcid string) ([]string, error) {
	f.gotTenant = tenantID
	if f.err != nil {
		return nil, f.err
	}
	return f.friends[gcid], nil
}

func (f *fakeFriendReader) FollowingGCIDs(context.Context, string, string) ([]string, error) {
	return nil, errors.New("fakeFriendReader: FollowingGCIDs not expected in this test")
}

func (f *fakeFriendReader) FollowersGCIDs(context.Context, string, string) ([]string, error) {
	return nil, errors.New("fakeFriendReader: FollowersGCIDs not expected in this test")
}

func (f *fakeFriendReader) BlockedBy(context.Context, string, string) ([]string, error) {
	return nil, errors.New("fakeFriendReader: BlockedBy not expected in this test")
}

func (f *fakeFriendReader) FriendSuggestions(context.Context, string, string, int) ([]social.FriendSuggestion, error) {
	return nil, errors.New("fakeFriendReader: FriendSuggestions not expected in this test")
}

// fakeReuseGrantRepo implements grant.GrantRepo with only the reuse-context
// surface live; the licensing methods fail loud if unexpectedly called.
type fakeReuseGrantRepo struct {
	atomIDs []string
	err     error
}

func (f *fakeReuseGrantRepo) Authorize(context.Context, *grant.AtomUsageGrant) (*grant.AtomUsageGrant, error) {
	return nil, errors.New("fakeReuseGrantRepo: Authorize not expected in this test")
}
func (f *fakeReuseGrantRepo) Revoke(context.Context, string, string, string) error {
	return errors.New("fakeReuseGrantRepo: Revoke not expected in this test")
}
func (f *fakeReuseGrantRepo) GetActive(context.Context, string, string, grant.Scope) (*grant.AtomUsageGrant, error) {
	return nil, errors.New("fakeReuseGrantRepo: GetActive not expected in this test")
}
func (f *fakeReuseGrantRepo) ListEntitled(context.Context, string, grant.Scope, []string, int) ([]grant.EntitledAtom, error) {
	return nil, errors.New("fakeReuseGrantRepo: ListEntitled not expected in this test")
}
func (f *fakeReuseGrantRepo) ActiveGrantAtomIDs(_ context.Context, gcid string) ([]string, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.atomIDs, nil
}

func TestGetReuseContext_NotWired_Unimplemented(t *testing.T) {
	t.Parallel()
	s := New(Deps{}) // neither Friends nor Grants wired
	_, err := s.GetReuseContext(context.Background(), &sharingv1.GetReuseContextRequest{
		Gcid: reuseGCID, TenantId: reuseTenant,
	})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected Unimplemented; got %v", err)
	}
}

func TestGetReuseContext_MissingArgs_InvalidArgument(t *testing.T) {
	t.Parallel()
	s := New(Deps{
		Friends: &fakeFriendReader{},
		Grants:  &fakeReuseGrantRepo{},
	})
	for name, req := range map[string]*sharingv1.GetReuseContextRequest{
		"nil request":    nil,
		"missing gcid":   {TenantId: reuseTenant},
		"missing tenant": {Gcid: reuseGCID},
	} {
		if _, err := s.GetReuseContext(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("%s: expected InvalidArgument; got %v", name, err)
		}
	}
}

func TestGetReuseContext_HappyPath(t *testing.T) {
	t.Parallel()
	friends := &fakeFriendReader{friends: map[string][]string{reuseGCID: {friendGCID}}}
	grants := &fakeReuseGrantRepo{atomIDs: []string{"atom-1", "atom-2"}}
	s := New(Deps{Friends: friends, Grants: grants})

	resp, err := s.GetReuseContext(context.Background(), &sharingv1.GetReuseContextRequest{
		Gcid: reuseGCID, TenantId: reuseTenant,
	})
	if err != nil {
		t.Fatalf("GetReuseContext: %v", err)
	}
	if len(resp.GetFriendGcids()) != 1 || resp.GetFriendGcids()[0] != friendGCID {
		t.Fatalf("friend_gcids mismatched: %v", resp.GetFriendGcids())
	}
	if len(resp.GetGrantedAtomIds()) != 2 {
		t.Fatalf("granted_atom_ids mismatched: %v", resp.GetGrantedAtomIds())
	}
	if friends.gotTenant != reuseTenant {
		t.Fatalf("FriendReader must receive the request tenant; got %q", friends.gotTenant)
	}
}

func TestGetReuseContext_FriendReaderError_Internal(t *testing.T) {
	t.Parallel()
	s := New(Deps{
		Friends: &fakeFriendReader{err: errors.New("db down")},
		Grants:  &fakeReuseGrantRepo{},
	})
	_, err := s.GetReuseContext(context.Background(), &sharingv1.GetReuseContextRequest{
		Gcid: reuseGCID, TenantId: reuseTenant,
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal on friend-reader failure (fail-loud); got %v", err)
	}
}

func TestGetReuseContext_GrantsError_Internal(t *testing.T) {
	t.Parallel()
	s := New(Deps{
		Friends: &fakeFriendReader{},
		Grants:  &fakeReuseGrantRepo{err: errors.New("db down")},
	})
	_, err := s.GetReuseContext(context.Background(), &sharingv1.GetReuseContextRequest{
		Gcid: reuseGCID, TenantId: reuseTenant,
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal on grants failure (fail-loud); got %v", err)
	}
}
