// block_test.go — Block edge domain specs (ADR-229 WS-0, CHO-2102) + the
// previously-untested Graph block/list surfaces.
package social_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

const (
	blkTenant = "01970000-0000-7000-a000-000000000001"
	blkUserA  = "01970000-0000-7000-c000-00000000000a"
	blkUserB  = "01970000-0000-7000-c000-00000000000b"
)

func TestNewBlock_HappyPath(t *testing.T) {
	t.Parallel()
	b, err := social.NewBlock(blkTenant, blkUserA, blkUserB)
	if err != nil {
		t.Fatalf("NewBlock: %v", err)
	}
	if b.ID == "" {
		t.Fatalf("expected UUIDv7 id")
	}
	if b.TenantID != blkTenant || b.BlockerGCID != blkUserA || b.BlockedGCID != blkUserB {
		t.Fatalf("fields mismatched: %+v", b)
	}
	if b.CreatedAt.IsZero() {
		t.Fatalf("expected CreatedAt set")
	}
}

func TestNewBlock_Guards(t *testing.T) {
	t.Parallel()
	if _, err := social.NewBlock(blkTenant, blkUserA, blkUserA); !errors.Is(err, social.ErrSelfBlock) {
		t.Fatalf("self-block: expected ErrSelfBlock; got %v", err)
	}
	for name, tt := range map[string][3]string{
		"missing tenant":  {"", blkUserA, blkUserB},
		"missing blocker": {blkTenant, "", blkUserB},
		"missing blocked": {blkTenant, blkUserA, ""},
	} {
		if _, err := social.NewBlock(tt[0], tt[1], tt[2]); !errors.Is(err, social.ErrInvalidArgument) {
			t.Fatalf("%s: expected ErrInvalidArgument; got %v", name, err)
		}
	}
}

func TestGraph_BlockUnblockBlockedBy(t *testing.T) {
	t.Parallel()
	g := social.NewGraph()
	if err := g.Block(blkTenant, blkUserA, blkUserA); !errors.Is(err, social.ErrSelfFollow) {
		t.Fatalf("graph self-block: expected guard error; got %v", err)
	}
	if err := g.Block(blkTenant, blkUserA, blkUserB); err != nil {
		t.Fatalf("block: %v", err)
	}
	if err := g.Block(blkTenant, blkUserA, blkUserB); err != nil {
		t.Fatalf("re-block must be idempotent: %v", err)
	}
	if got := g.BlockedBy(blkUserA); !reflect.DeepEqual(got, []string{blkUserB}) {
		t.Fatalf("BlockedBy = %v, want [%s]", got, blkUserB)
	}
	if !g.Unblock(blkUserA, blkUserB) {
		t.Fatalf("unblock: expected true")
	}
	if g.Unblock(blkUserA, blkUserB) {
		t.Fatalf("re-unblock: expected false")
	}
	if got := g.BlockedBy(blkUserA); len(got) != 0 {
		t.Fatalf("BlockedBy after unblock = %v, want empty", got)
	}
}

func TestGraph_FollowingFollowersGCIDs(t *testing.T) {
	t.Parallel()
	g := social.NewGraph()
	if _, _, err := g.Follow(blkTenant, blkUserA, blkUserB); err != nil {
		t.Fatalf("follow: %v", err)
	}
	if got := g.FollowingGCIDs(blkUserA); !reflect.DeepEqual(got, []string{blkUserB}) {
		t.Fatalf("FollowingGCIDs = %v, want [%s]", got, blkUserB)
	}
	if got := g.FollowersGCIDs(blkUserB); !reflect.DeepEqual(got, []string{blkUserA}) {
		t.Fatalf("FollowersGCIDs = %v, want [%s]", got, blkUserA)
	}
	if got := g.FollowingGCIDs(blkUserB); len(got) != 0 {
		t.Fatalf("one-way follow must not reflect: %v", got)
	}
}
