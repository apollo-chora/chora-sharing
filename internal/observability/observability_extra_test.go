// Additional coverage for the fail-soft async OTLP init entry point -
// callable from the public API without touching production code.
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
