// Additional coverage for the fail-soft async OTLP init entry point and the
// IMDA evidence log shims - all callable from the public API without
// touching production code.
//
// NOTE: randHex's entropy-failure fallback is not exercised - on Go >= 1.24
// crypto/rand.Read fatals on reader failure (go.dev/issue/66821), so the
// fallback branch is unreachable from a test.
package observability_test

import (
	"context"
	"testing"

	obs "github.com/apollo-chora/chora-sharing/internal/observability"
)

func TestInitAsync_ReturnsHandle(t *testing.T) {
	// An already-cancelled context settles the async OTLP init immediately
	// (fail-soft no-op per C(a).S1 path (b)), so the handle is returned
	// without waiting on a real exporter.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h := obs.InitAsync(ctx)
	if h == nil {
		t.Fatal("InitAsync returned nil handle")
	}
}

func TestLogDraftEvent_DoesNotPanic(t *testing.T) {
	tc := obs.TraceContext{TraceID: "x", SpanID: "y", TraceFlg: "01"}
	obs.LogDraftEvent(tc, "tenant", "gcid", "draft-1", "publish")
}

func TestLogPreferenceUpdate_DoesNotPanic(t *testing.T) {
	tc := obs.TraceContext{TraceID: "x", SpanID: "y", TraceFlg: "01"}
	obs.LogPreferenceUpdate(tc, "tenant", "gcid", "public")
}