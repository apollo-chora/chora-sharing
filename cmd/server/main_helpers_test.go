// main_helpers_test.go - the small pure helper functions in main.go /
// atom wiring that are cheaply unit-testable without booting the server:
// envOrDefault, firstNonEmptyEnv, splitCSV, attrsFromBusEnvelope,
// identityInterceptor, pushVerifier, and the profile broker bridge's nil
// guard. The rest of main() is env-gated wiring and is not exercised here.
package main

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-common/tracing"
	httpadapter "github.com/apollo-chora/chora-sharing/internal/adapter/http"
	"github.com/apollo-chora/chora-sharing/internal/adapter/ws"
)

func TestSplitCSV(t *testing.T) {
	if got := splitCSV(""); got != nil {
		t.Fatalf("empty input: got %v, want nil", got)
	}
	if got := splitCSV("   "); got != nil {
		t.Fatalf("whitespace input: got %v, want nil", got)
	}
	if got := splitCSV("a,b , c"); len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("mixed input: got %v", got)
	}
	if got := splitCSV("a,,b"); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("empty parts must be filtered: got %v", got)
	}
}

func TestEnvOrDefault(t *testing.T) {
	t.Setenv("CHORA_TEST_ENV_DEFAULT", "set-value")
	if got := envOrDefault("CHORA_TEST_ENV_DEFAULT", "def"); got != "set-value" {
		t.Fatalf("set env: got %q, want set-value", got)
	}
	if got := envOrDefault("CHORA_TEST_ENV_DEFAULT_UNSET_XYZ", "def"); got != "def" {
		t.Fatalf("unset env: got %q, want def", got)
	}
}

func TestFirstNonEmptyEnv(t *testing.T) {
	if got := firstNonEmptyEnv("CHORA_TEST_FNEE_UNSET_1"); got != "" {
		t.Fatalf("unset: got %q, want empty", got)
	}
	t.Setenv("CHORA_TEST_FNEE_1", "v1")
	t.Setenv("CHORA_TEST_FNEE_2", "v2")
	if got := firstNonEmptyEnv("CHORA_TEST_FNEE_1", "CHORA_TEST_FNEE_2"); got != "v1" {
		t.Fatalf("first set: got %q, want v1", got)
	}
	t.Setenv("CHORA_TEST_FNEE_3", "  padded  ")
	if got := firstNonEmptyEnv("CHORA_TEST_FNEE_UNSET_2", "CHORA_TEST_FNEE_3"); got != "padded" {
		t.Fatalf("trimmed: got %q, want padded", got)
	}
}

func TestAttrsFromBusEnvelope(t *testing.T) {
	got := attrsFromBusEnvelope(eventbus.Message{})
	for k, v := range got {
		if v != "" {
			t.Fatalf("zero message: attr %q = %q, want empty", k, v)
		}
	}

	t0 := time.Date(2026, 5, 16, 9, 30, 0, 0, time.UTC)
	msg := eventbus.Message{
		Subject: "chora.consumption.familiar.stage_up.v1",
		Envelope: cgcenvelope.Envelope{
			EventID:                "evt-1",
			TenantID:               "tenant-1",
			GCID:                   "gcid-1",
			Traceparent:            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
			Tracestate:             "chora=stage_up",
			OccurredAt:             t0,
			PublishedAt:            t0.Add(time.Millisecond),
			ChoraImdaDimension:     "accountability",
			ImdaLifecycleStage:     "runtime",
			SourceProject:          "chora-489812",
			SourceService:          "chora-sharing",
			SchemaVersion:          1,
		},
	}
	attrs := attrsFromBusEnvelope(msg)
	want := map[string]string{
		"topic":                  "chora.consumption.familiar.stage_up.v1",
		"event_id":               "evt-1",
		"tenant_id":              "tenant-1",
		"gcid":                   "gcid-1",
		"traceparent":            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		"tracestate":             "chora=stage_up",
		"occurred_at":            t0.UTC().Format(time.RFC3339Nano),
		"published_at":           t0.Add(time.Millisecond).UTC().Format(time.RFC3339Nano),
		"chora_imda_dimension":   "accountability",
		"imda_lifecycle_stage":   "runtime",
	}
	for k, v := range want {
		if got := attrs[k]; got != v {
			t.Errorf("attrs[%q] = %q, want %q", k, got, v)
		}
	}
}

func TestIdentityInterceptor(t *testing.T) {
	var gotTenant, gotGCID string
	var called bool
	handler := func(ctx context.Context, _ any) (any, error) {
		called = true
		gotTenant = tracing.TenantIDFromContext(ctx)
		gotGCID = tracing.GCIDFromContext(ctx)
		return "ok", nil
	}
	info := &grpc.UnaryServerInfo{}

	// With gateway-stamped metadata, tenant + gcid must flow into ctx.
	md := metadata.Pairs("x-tenant-id", "tenant-1", "gcid", "gcid-1")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	if _, err := identityInterceptor(ctx, nil, info, handler); err != nil {
		t.Fatalf("identityInterceptor: %v", err)
	}
	if !called {
		t.Fatal("handler not called")
	}
	if gotTenant != "tenant-1" || gotGCID != "gcid-1" {
		t.Fatalf("tenant=%q gcid=%q, want tenant-1/gcid-1", gotTenant, gotGCID)
	}

	// Without metadata the handler must still run (no tenant/gcid injected).
	called = false
	if _, err := identityInterceptor(context.Background(), nil, info, handler); err != nil {
		t.Fatalf("identityInterceptor (no metadata): %v", err)
	}
	if !called {
		t.Fatal("handler not called without metadata")
	}
	if gotTenant != "" || gotGCID != "" {
		t.Fatalf("expected empty tenant/gcid without metadata, got %q/%q", gotTenant, gotGCID)
	}
}

func TestPushVerifier(t *testing.T) {
	t.Setenv("CHORA_PUBSUB_PUSH_AUDIENCE_BASE", "")
	if v := pushVerifier("weakness-grown"); v == nil || v.Enabled() {
		t.Fatal("expected disabled verifier when audience base unset")
	}

	t.Setenv("CHORA_PUBSUB_PUSH_AUDIENCE_BASE", "https://api.chora.site")
	v := pushVerifier("weakness-grown")
	if v == nil || !v.Enabled() {
		t.Fatal("expected enabled verifier when audience base set")
	}
}

func TestProfileBrokerBridge_NilBrokerNoOps(t *testing.T) {
	msg := httpadapter.ProfileMessageKind("profile_ready")
	profileBrokerAdapter{}.PublishProfile("gcid-x", msg, nil) // must not panic
}

func TestProfileBrokerBridge_PublishesWithSubscriber(t *testing.T) {
	broker := ws.NewProfileBroker()
	ch, cleanup := broker.Subscribe("gcid-x", "sub-1")
	defer cleanup()

	profileBrokerAdapter{broker: broker}.PublishProfile("gcid-x", "profile_ready", []byte(`{"ok":true}`))

	select {
	case got := <-ch:
		if got.Kind != "profile_ready" {
			t.Fatalf("kind = %q, want profile_ready", got.Kind)
		}
		if string(got.Payload) != `{"ok":true}` {
			t.Fatalf("payload = %q", got.Payload)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("subscriber did not receive profile message")
	}
}