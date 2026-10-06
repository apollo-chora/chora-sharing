// Package observability holds the OTel SDK wiring for chora-sharing.
//
// The production stack is OTLP-everywhere via OpenInference / OpenLLMetry
// semantic conventions. The SDK layer (added 2026-05-14 / Paydown II) emits
// real spans to the OTLP endpoint (OTEL_EXPORTER_OTLP_ENDPOINT) via
// chora-common/observability.InitOTLPAsync.
//
// Paydown II (tracker #151 / C(a).S1.b follow-on) added the async
// InitAsync entry point that returns a bootstrap.OTLPHandle; service
// bootstrap runs pgx pool init with the FULL bootstrap deadline while
// OTLP wiring proceeds in its own goroutine + own deadline
// (CHORA_OTLP_INIT_TIMEOUT_SECONDS, default 15s). Timeout / init-error
// degrade to a no-op shutdown so pgx pool + event-bus clients get the FULL
// bootstrap budget. See chora-common/bootstrap/README.md.
package observability

import (
	"context"

	"github.com/apollo-chora/chora-common/bootstrap"
	commonobs "github.com/apollo-chora/chora-common/observability"
)

// ServiceName is the canonical service identifier emitted on every log line.
const ServiceName = "chora-sharing"

// ServiceVersion follows semver per OpenInference convention.
const ServiceVersion = "0.2.0"

// InitAsync is the fail-soft non-blocking OTLP SDK init per C(a).S1
// path (b) — tracker #151. Delegates to commonobs.InitOTLPAsync so OTLP
// init runs in its own goroutine with its own deadline
// (CHORA_OTLP_INIT_TIMEOUT_SECONDS, default 15s). Timeout / init-error
// degrade to a no-op shutdown so pgx pool + Pub/Sub clients get the FULL
// bootstrap budget.
func InitAsync(ctx context.Context) *bootstrap.OTLPHandle {
	return commonobs.InitOTLPAsync(ctx, ServiceName, ServiceVersion)
}
