package clients

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	identityv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/identity/v1"
)

// IdentityDisplayNameClient resolves a GCID to a display name via
// chora-identity's GetMe gRPC RPC. Used by the profiler generate handler
// to populate profiler_profiles.display_name at profile generation time.
type IdentityDisplayNameClient struct {
	client identityv1.IdentityClient
}

// NewIdentityDisplayNameClient constructs an IdentityDisplayNameClient
// from an existing gRPC connection to chora-identity.
func NewIdentityDisplayNameClient(conn *grpc.ClientConn) *IdentityDisplayNameClient {
	return &IdentityDisplayNameClient{client: identityv1.NewIdentityClient(conn)}
}

// ResolveDisplayName calls chora-identity GetMe and returns the user's
// display_name. Returns "" on any error — best-effort: the profiler handler
// treats empty as "leave the column default" and the FE falls back to
// shortGcid().
func (c *IdentityDisplayNameClient) ResolveDisplayName(ctx context.Context, gcid string) (string, error) {
	if gcid == "" {
		return "", nil
	}
	// Hard 3s deadline — this runs on the synchronous 202 path. If the
	// identity gRPC call hangs through the mesh, we must not block the
	// profile generation response. Best-effort: timeout → empty name →
	// FE falls back to shortGcid().
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := c.client.GetMe(ctx, &identityv1.GetMeRequest{Gcid: gcid})
	if err != nil {
		return "", fmt.Errorf("clients: identity GetMe: %w", err)
	}
	if resp.GetMe() == nil {
		return "", nil
	}
	return resp.GetMe().GetDisplayName(), nil
}
