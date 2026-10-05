package clients

import (
	"context"
	"errors"
	"strings"
	"testing"

	identityv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/identity/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// fakeIdentityClient implements identityv1.IdentityClient for
// IdentityDisplayNameClient tests. The embedded interface satisfies the
// unexercised RPCs; GetMe is overridden with scripted behaviour.
type fakeIdentityClient struct {
	identityv1.IdentityClient
	getMeResp *identityv1.GetMeResponse
	getMeErr  error
}

func (f *fakeIdentityClient) GetMe(_ context.Context, _ *identityv1.GetMeRequest, _ ...grpc.CallOption) (*identityv1.GetMeResponse, error) {
	if f.getMeErr != nil {
		return nil, f.getMeErr
	}
	return f.getMeResp, nil
}

func TestNewIdentityDisplayNameClient(t *testing.T) {
	// grpc.NewClient does not dial eagerly — construct + close cheaply.
	conn, err := grpc.NewClient("passthrough:///unused", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()
	c := NewIdentityDisplayNameClient(conn)
	if c == nil || c.client == nil {
		t.Fatal("NewIdentityDisplayNameClient returned nil client/backend")
	}
}

func TestIdentityDisplayNameClient_ResolveDisplayName_EmptyGCID(t *testing.T) {
	// Empty gcid short-circuits before any gRPC call (best-effort contract).
	client := &IdentityDisplayNameClient{client: &fakeIdentityClient{
		getMeErr: errors.New("must not be invoked"),
	}}
	name, err := client.ResolveDisplayName(context.Background(), "")
	if err != nil {
		t.Fatalf("ResolveDisplayName: %v", err)
	}
	if name != "" {
		t.Errorf("name = %q, want empty", name)
	}
}

func TestIdentityDisplayNameClient_ResolveDisplayName_HappyPath(t *testing.T) {
	client := &IdentityDisplayNameClient{client: &fakeIdentityClient{
		getMeResp: &identityv1.GetMeResponse{Me: &identityv1.Me{DisplayName: "Phyllis Tan"}},
	}}
	name, err := client.ResolveDisplayName(context.Background(), "gcid-phyllis")
	if err != nil {
		t.Fatalf("ResolveDisplayName: %v", err)
	}
	if name != "Phyllis Tan" {
		t.Errorf("name = %q, want Phyllis Tan", name)
	}
}

func TestIdentityDisplayNameClient_ResolveDisplayName_NilMe(t *testing.T) {
	client := &IdentityDisplayNameClient{client: &fakeIdentityClient{
		getMeResp: &identityv1.GetMeResponse{}, // Me is nil
	}}
	name, err := client.ResolveDisplayName(context.Background(), "gcid-phyllis")
	if err != nil {
		t.Fatalf("ResolveDisplayName: %v", err)
	}
	if name != "" {
		t.Errorf("name = %q, want empty (nil Me)", name)
	}
}

func TestIdentityDisplayNameClient_ResolveDisplayName_Error(t *testing.T) {
	client := &IdentityDisplayNameClient{client: &fakeIdentityClient{
		getMeErr: errors.New("identity getme: deadline exceeded"),
	}}
	name, err := client.ResolveDisplayName(context.Background(), "gcid-phyllis")
	if name != "" {
		t.Errorf("name = %q, want empty on error", name)
	}
	if err == nil || !strings.Contains(err.Error(), "identity GetMe") {
		t.Errorf("err = %v, want identity GetMe wrap", err)
	}
}
