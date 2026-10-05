package clients

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/currency"
	identityv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/identity/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeManaServiceClient implements identityv1.ManaServiceClient for
// ManaClient tests. The embedded interface satisfies the unexercised RPCs;
// DeductMana is overridden with scripted behaviour.
type fakeManaServiceClient struct {
	identityv1.ManaServiceClient
	deductResp *identityv1.DeductManaResponse
	deductErr  error
	gotReq     *identityv1.DeductManaRequest
}

func (f *fakeManaServiceClient) DeductMana(_ context.Context, in *identityv1.DeductManaRequest, _ ...grpc.CallOption) (*identityv1.DeductManaResponse, error) {
	f.gotReq = in
	if f.deductErr != nil {
		return nil, f.deductErr
	}
	return f.deductResp, nil
}

func TestNewManaClient(t *testing.T) {
	c := NewManaClient(&fakeManaServiceClient{})
	if c == nil || c.mana == nil {
		t.Fatal("NewManaClient returned nil client/backend")
	}
}

func TestManaClient_DebitReuser_NotConfigured(t *testing.T) {
	var nilC *ManaClient
	if err := nilC.DebitReuser(context.Background(), "g", "t", 5, "action"); !errors.Is(err, currency.ErrManaClientNotConfigured) {
		t.Errorf("nil receiver: err = %v, want ErrManaClientNotConfigured", err)
	}
	c := NewManaClient(nil)
	if err := c.DebitReuser(context.Background(), "g", "t", 5, "action"); !errors.Is(err, currency.ErrManaClientNotConfigured) {
		t.Errorf("nil backend: err = %v, want ErrManaClientNotConfigured", err)
	}
}

func TestManaClient_DebitReuser_InvalidArgs(t *testing.T) {
	c := NewManaClient(&fakeManaServiceClient{})
	cases := []struct {
		name     string
		gcid     string
		tenantID string
		amount   float64
		action   string
	}{
		{"empty gcid", "  ", "t", 5, "action"},
		{"zero amount", "g", "t", 0, "action"},
		{"negative amount", "g", "t", -1, "action"},
		{"empty action code", "g", "t", 5, " "},
	}
	for _, tc := range cases {
		err := c.DebitReuser(context.Background(), tc.gcid, tc.tenantID, tc.amount, tc.action)
		if !errors.Is(err, currency.ErrInvalidArgument) {
			t.Errorf("%s: err = %v, want ErrInvalidArgument", tc.name, err)
		}
	}
}

func TestManaClient_DebitReuser_Success(t *testing.T) {
	backend := &fakeManaServiceClient{deductResp: &identityv1.DeductManaResponse{Success: true}}
	c := NewManaClient(backend)
	if err := c.DebitReuser(context.Background(), "gcid-reuser", "tenant-x", 7, "atom_royalty"); err != nil {
		t.Fatalf("DebitReuser: %v", err)
	}
	req := backend.gotReq
	if req.GetGcid() != "gcid-reuser" {
		t.Errorf("Gcid = %q, want gcid-reuser", req.GetGcid())
	}
	if req.GetActionCode() != "atom_royalty" {
		t.Errorf("ActionCode = %q, want atom_royalty", req.GetActionCode())
	}
	if req.GetUnits() != 7 {
		t.Errorf("Units = %d, want 7", req.GetUnits())
	}
	if req.GetTenantId() != "tenant-x" {
		t.Errorf("TenantId = %q, want tenant-x", req.GetTenantId())
	}
	if req.GetIdempotencyKey() == "" {
		t.Error("IdempotencyKey empty, want minted UUIDv7")
	}
}

func TestManaClient_DebitReuser_Success_UsesCallerIdempotencyKey(t *testing.T) {
	backend := &fakeManaServiceClient{deductResp: &identityv1.DeductManaResponse{Success: true}}
	c := NewManaClient(backend)
	ctx := WithManaIdempotencyKey(context.Background(), "royalty-source-event-42")
	if err := c.DebitReuser(ctx, "g", "t", 5, "atom_royalty"); err != nil {
		t.Fatalf("DebitReuser: %v", err)
	}
	if got := backend.gotReq.GetIdempotencyKey(); got != "royalty-source-event-42" {
		t.Errorf("IdempotencyKey = %q, want royalty-source-event-42 (no double-spend seam)", got)
	}
}

func TestManaClient_DebitReuser_FailedPreconditionIsInsufficient(t *testing.T) {
	backend := &fakeManaServiceClient{deductErr: status.Error(codes.FailedPrecondition, "balance too low")}
	c := NewManaClient(backend)
	err := c.DebitReuser(context.Background(), "g", "t", 100, "atom_royalty")
	if !errors.Is(err, currency.ErrInsufficientMana) {
		t.Errorf("err = %v, want ErrInsufficientMana", err)
	}
}

func TestManaClient_DebitReuser_OtherRPCErrorWrapped(t *testing.T) {
	backend := &fakeManaServiceClient{deductErr: status.Error(codes.Unavailable, "identity down")}
	c := NewManaClient(backend)
	err := c.DebitReuser(context.Background(), "g", "t", 5, "atom_royalty")
	if err == nil || !strings.Contains(err.Error(), "mana debit") {
		t.Errorf("err = %v, want mana debit wrap", err)
	}
}

func TestManaClient_DebitReuser_ResponseNotSuccessIsInsufficient(t *testing.T) {
	backend := &fakeManaServiceClient{deductResp: &identityv1.DeductManaResponse{
		Success:             false,
		RequiredUnits:       10,
		CurrentBalanceUnits: 3,
	}}
	c := NewManaClient(backend)
	err := c.DebitReuser(context.Background(), "g", "t", 100, "atom_royalty")
	if !errors.Is(err, currency.ErrInsufficientMana) {
		t.Errorf("err = %v, want ErrInsufficientMana", err)
	}
	if !strings.Contains(err.Error(), "required=10 available=3") {
		t.Errorf("err = %v, want required/available detail", err)
	}
}

func TestWithManaIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	if got := WithManaIdempotencyKey(ctx, ""); got != ctx {
		t.Error("empty key must return the original context unchanged")
	}
	withKey := WithManaIdempotencyKey(ctx, "k-1")
	if got := newIdempotencyKey(withKey); got != "k-1" {
		t.Errorf("newIdempotencyKey(withKey) = %q, want k-1", got)
	}
}

func TestNewIdempotencyKey_MintsWhenAbsent(t *testing.T) {
	key := newIdempotencyKey(context.Background())
	if key == "" {
		t.Fatal("newIdempotencyKey returned empty key")
	}
}
