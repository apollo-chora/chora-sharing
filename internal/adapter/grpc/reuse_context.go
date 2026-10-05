// reuse_context.go — GetReuseContext RPC (ADR-229 WS-0, CHO-2102).
package grpcadapter

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sharingv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/sharing/v1"

	"github.com/apollo-chora/chora-common/tracing"
)

// GetReuseContext returns the caller's reuse context (ADR-229 WS-0): the
// friend set (ADR-230 D2: accepted friendships minus blocks in either
// direction — swapped from the WS-0 derived mutual-follow set with the RPC
// contract byte-identical) + the atom ids covered by an ACTIVE
// AtomUsageGrant. chora-creation calls this once per picker search (the
// ListSavedAtomIDs per-request pattern) so the friends-visible + granted
// disjuncts evaluate locally in creation's SQL — never a cross-DB read.
func (s *SharingServer) GetReuseContext(ctx context.Context, req *sharingv1.GetReuseContextRequest) (*sharingv1.GetReuseContextResponse, error) {
	if s.deps.Friends == nil {
		return nil, notWired("Friends")
	}
	if s.deps.Grants == nil {
		return nil, notWired("Grants")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	gcid := strings.TrimSpace(req.GetGcid())
	tenant := strings.TrimSpace(req.GetTenantId())
	if gcid == "" || tenant == "" {
		return nil, status.Error(codes.InvalidArgument, "gcid, tenant_id required")
	}

	// Stamp tenant + gcid so rls.ApplySession scopes both queries.
	qctx := tracing.WithTenantID(ctx, tenant)
	qctx = tracing.WithGCID(qctx, gcid)

	friends, err := s.deps.Friends.FriendSet(qctx, tenant, gcid)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "reuse context friends: %v", err)
	}
	granted, err := s.deps.Grants.ActiveGrantAtomIDs(qctx, gcid)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "reuse context grants: %v", err)
	}
	return &sharingv1.GetReuseContextResponse{
		FriendGcids:    friends,
		GrantedAtomIds: granted,
	}, nil
}
