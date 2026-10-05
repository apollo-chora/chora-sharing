// Wave-1 S-FULL TDD coverage for the chora-sharing gRPC server registration.
//
// Boots an in-process *grpc.Server on a bufconn listener with the same
// composition cmd/server/main.go does (Sharing server + Health), dials it,
// and round-trips Health/Check + one trivial Sharing RPC (Follow). Per
// docs/m13/grpc-mass-remediation-2026-05-16.md §3.e the acceptance test
// must (1) boot a server with the new registrations, (2) dial via
// ClientConn, (3) hit Health/Check, (4) hit one domain RPC end-to-end.
//
// The bufconn pattern (mirrors chora-identity mana_grpc_bufconn_test.go) is
// the closest in-test fidelity to the Cloud Service Mesh wire path the
// chora-gateway BFF will use post Wave-2 cutover.
package main_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthgrpc "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"

	sharingv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/sharing/v1"

	grpcadapter "github.com/apollo-chora/chora-sharing/internal/adapter/grpc"
	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
	"github.com/apollo-chora/chora-sharing/internal/domain/reaction"
)

const bufconnSize = 1024 * 1024

// startBufconnSharingServer mirrors the gRPC composition cmd/server/main.go
// performs: Sharing server bound + Health bound on the same *grpc.Server.
func startBufconnSharingServer(t *testing.T) (*grpc.ClientConn, func()) {
	t.Helper()
	lis := bufconn.Listen(bufconnSize)
	srv := grpc.NewServer()

	sharingSrv := grpcadapter.New(grpcadapter.Deps{
		Posts:        inmem.NewPostRepo(),
		Reactions:    reaction.NewRegistry(),
		Graph:        inmem.NewSocialGraph(),
		Leaderboards: leaderboard.NewRankerReader(leaderboard.NewRanker()),
	})
	sharingv1.RegisterSharingServer(srv, sharingSrv)

	healthSrv := healthgrpc.NewServer()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthSrv.SetServingStatus("chora.services.sharing.v1.Sharing", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, healthSrv)

	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Logf("bufconn sharing server stopped: %v", err)
		}
	}()

	//nolint:staticcheck // bufconn requires the legacy DialContext API.
	conn, err := grpc.DialContext(
		context.Background(),
		"bufconn",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("bufconn dial: %v", err)
	}
	cleanup := func() {
		_ = conn.Close()
		srv.GracefulStop()
		_ = lis.Close()
	}
	return conn, cleanup
}

// TestBufconn_HealthCheck verifies the gRPC Health service is bound and
// reports SERVING for the Sharing service. Cloud Service Mesh probe
// routing depends on this contract.
func TestBufconn_HealthCheck(t *testing.T) {
	t.Parallel()
	conn, cleanup := startBufconnSharingServer(t)
	defer cleanup()

	client := healthpb.NewHealthClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Generic SERVING for the gRPC server (empty service name).
	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
	if err != nil {
		t.Fatalf("Health/Check (default): %v", err)
	}
	if resp.Status != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("default health status=%v want SERVING", resp.Status)
	}

	// Service-scoped SERVING for the Sharing service.
	resp, err = client.Check(ctx, &healthpb.HealthCheckRequest{Service: "chora.services.sharing.v1.Sharing"})
	if err != nil {
		t.Fatalf("Health/Check (Sharing): %v", err)
	}
	if resp.Status != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("Sharing health status=%v want SERVING", resp.Status)
	}
}

// TestBufconn_Sharing_Follow_RoundTrip exercises the full chora-gateway →
// chora-sharing wire path via gRPC: Follow inserts an edge, returns the
// canonical proto Follow record. Mirrors the BFF call shape the Wave-2
// switch will dispatch when SVC_SHARING_GRPC_URL lands.
func TestBufconn_Sharing_Follow_RoundTrip(t *testing.T) {
	t.Parallel()
	conn, cleanup := startBufconnSharingServer(t)
	defer cleanup()

	client := sharingv1.NewSharingClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	const (
		tenantID = "01970000-0000-7000-9000-tenant-aaaaa"
		alice    = "01970000-0000-7000-9000-000000000001"
		bob      = "01970000-0000-7000-9000-000000000002"
	)
	resp, err := client.Follow(ctx, &sharingv1.FollowRequest{
		FollowerGcid: alice,
		FolloweeGcid: bob,
		TenantId:     tenantID,
	})
	if err != nil {
		t.Fatalf("Follow: %v", err)
	}
	if resp.GetFollow() == nil {
		t.Fatalf("Follow response nil edge")
	}
	if got := resp.GetFollow().GetFollowerGcid(); got != alice {
		t.Errorf("follower_gcid=%q want %q", got, alice)
	}
	if got := resp.GetFollow().GetFolloweeGcid(); got != bob {
		t.Errorf("followee_gcid=%q want %q", got, bob)
	}
	if got := resp.GetFollow().GetTenantId(); got != tenantID {
		t.Errorf("tenant_id=%q want %q", got, tenantID)
	}
	if !resp.GetFollow().GetFollowedAt().IsValid() {
		t.Errorf("followed_at not set")
	}

	// Idempotent re-follow returns the same edge unchanged (append-only).
	resp2, err := client.Follow(ctx, &sharingv1.FollowRequest{
		FollowerGcid: alice,
		FolloweeGcid: bob,
		TenantId:     tenantID,
	})
	if err != nil {
		t.Fatalf("Follow (repeat): %v", err)
	}
	if resp.GetFollow().GetFollowedAt().AsTime() != resp2.GetFollow().GetFollowedAt().AsTime() {
		t.Errorf("idempotent Follow re-created edge (followed_at changed)")
	}

	// Self-follow rejected with InvalidArgument.
	if _, err := client.Follow(ctx, &sharingv1.FollowRequest{
		FollowerGcid: alice,
		FolloweeGcid: alice,
		TenantId:     tenantID,
	}); err == nil {
		t.Errorf("expected error on self-follow")
	}

	// Unfollow removes the edge.
	unResp, err := client.Unfollow(ctx, &sharingv1.UnfollowRequest{
		FollowerGcid: alice,
		FolloweeGcid: bob,
		TenantId:     tenantID,
	})
	if err != nil {
		t.Fatalf("Unfollow: %v", err)
	}
	if !unResp.GetWasFollowing() {
		t.Errorf("Unfollow.was_following=false want true after Follow")
	}
}
