// Package observability holds the OTel SDK wiring + lightweight request-
// logging shim for chora-sharing.
//
// The production stack is OTLP-everywhere via OpenInference / OpenLLMetry
// semantic conventions. The legacy FromRequest / LogRequest /
// TraceparentHeader / TraceContext surface below is preserved for the HTTP
// layer; the SDK layer (added 2026-05-14 / Paydown II) emits real spans to
// the OTLP endpoint (OTEL_EXPORTER_OTLP_ENDPOINT) via
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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

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

// TraceContext holds the W3C traceparent fields the shim threads through
// each request. We mint a fresh trace + span ID per request unless the
// caller already provided traceparent.
type TraceContext struct {
	TraceID  string // 16-byte hex (32 chars)
	SpanID   string // 8-byte hex (16 chars)
	TraceFlg string // 2-char hex
}

// FromRequest extracts traceparent (W3C trace context) or mints a fresh one.
//
// Per CLAUDE.md §6, traceparent + tracestate are mandatory in the event
// envelope; the same propagation rules apply to HTTP. The skeleton
// minimally parses the version-00 form: 00-<trace-id>-<span-id>-<flags>.
func FromRequest(r *http.Request) TraceContext {
	if h := r.Header.Get("traceparent"); h != "" {
		parts := strings.Split(h, "-")
		if len(parts) == 4 && parts[0] == "00" {
			return TraceContext{TraceID: parts[1], SpanID: parts[2], TraceFlg: parts[3]}
		}
	}
	return TraceContext{
		TraceID:  randHex(16),
		SpanID:   randHex(8),
		TraceFlg: "01",
	}
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// Best-effort fallback: derive bytes from time so logs still emit.
		now := time.Now().UnixNano()
		for i := range b {
			b[i] = byte(now >> uint(8*(i%8)))
		}
	}
	return hex.EncodeToString(b)
}

// LogRequest emits a single structured-log line per request.
//
// Format: service=<name> trace_id=<id> span_id=<id> tenant_id=<id>
// gcid=<id> method=<m> path=<p> status=<n> duration_ms=<n>.
//
// The trace_id join key lets a log aggregator correlate request lines with
// OTel spans; M12 swaps to JSON-structured payloads with the full OTel SDK.
func LogRequest(tc TraceContext, tenantID, gcid, method, path string, status int, dur time.Duration) {
	log.Printf(
		"service=%s trace_id=%s span_id=%s tenant_id=%s gcid=%s method=%s path=%s status=%d duration_ms=%d",
		ServiceName, tc.TraceID, tc.SpanID, tenantID, gcid, method, path, status, dur.Milliseconds(),
	)
}

// TraceparentHeader formats a TraceContext as a W3C traceparent string.
// Used when the service emits outbound HTTP / gRPC / event-bus envelopes.
func (tc TraceContext) TraceparentHeader() string {
	return fmt.Sprintf("00-%s-%s-%s", tc.TraceID, tc.SpanID, tc.TraceFlg)
}

// LogDraftEvent emits a structured-log line for a Familiar-milestone post
// draft action (publish | discard). Outcome appears as `outcome=...`.
//
// IMDA D1 evidence trail per ADR-141 — log lines carry the trace_id join
// key for the AssessorFlow audit replay.
func LogDraftEvent(tc TraceContext, tenantID, gcid, draftID, outcome string) {
	log.Printf(
		"service=%s trace_id=%s span_id=%s tenant_id=%s gcid=%s event=draft_action draft_id=%s outcome=%s",
		ServiceName, tc.TraceID, tc.SpanID, tenantID, gcid, draftID, outcome,
	)
}

// LogPreferenceUpdate emits a structured-log line when a user changes
// their Familiar-milestone share preference. IMDA D1 evidence.
func LogPreferenceUpdate(tc TraceContext, tenantID, gcid, newPref string) {
	log.Printf(
		"service=%s trace_id=%s span_id=%s tenant_id=%s gcid=%s event=preference_updated preference=%s",
		ServiceName, tc.TraceID, tc.SpanID, tenantID, gcid, newPref,
	)
}
