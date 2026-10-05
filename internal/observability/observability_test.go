// Package observability_test verifies traceparent parsing + structured-log
// rendering for chora-sharing.
package observability_test

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	obs "github.com/apollo-chora/chora-sharing/internal/observability"
)

func TestFromRequest_AcceptsValidTraceparent(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest("GET", "/x", nil)
	r.Header.Set("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
	tc := obs.FromRequest(r)
	if tc.TraceID != "0af7651916cd43dd8448eb211c80319c" {
		t.Fatalf("TraceID mismatch: %q", tc.TraceID)
	}
	if tc.SpanID != "b7ad6b7169203331" {
		t.Fatalf("SpanID mismatch: %q", tc.SpanID)
	}
	if tc.TraceFlg != "01" {
		t.Fatalf("TraceFlg mismatch: %q", tc.TraceFlg)
	}
}

func TestFromRequest_MintsFreshOnMissingHeader(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest("GET", "/x", nil)
	tc := obs.FromRequest(r)
	if len(tc.TraceID) != 32 {
		t.Fatalf("expected 32-char trace_id, got %q", tc.TraceID)
	}
	if len(tc.SpanID) != 16 {
		t.Fatalf("expected 16-char span_id, got %q", tc.SpanID)
	}
}

func TestFromRequest_RejectsMalformedTraceparent(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest("GET", "/x", nil)
	r.Header.Set("traceparent", "garbage-value")
	tc := obs.FromRequest(r)
	if len(tc.TraceID) != 32 {
		t.Fatalf("expected fresh 32-char trace_id on malformed header, got %q", tc.TraceID)
	}
}

func TestTraceparentHeader_FormatRoundtrip(t *testing.T) {
	t.Parallel()

	tc := obs.TraceContext{
		TraceID:  "0af7651916cd43dd8448eb211c80319c",
		SpanID:   "b7ad6b7169203331",
		TraceFlg: "01",
	}
	got := tc.TraceparentHeader()
	want := "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	if got != want {
		t.Fatalf("traceparent format: got %q want %q", got, want)
	}
	if !strings.HasPrefix(got, "00-") {
		t.Fatalf("traceparent must start with version 00")
	}
}

func TestLogRequest_DoesNotPanic(t *testing.T) {
	t.Parallel()
	tc := obs.TraceContext{TraceID: "x", SpanID: "y", TraceFlg: "01"}
	obs.LogRequest(tc, "tenant", "gcid", "GET", "/p", 200, 5*time.Millisecond)
}

func TestServiceName_IsChoraSharing(t *testing.T) {
	t.Parallel()
	if obs.ServiceName != "chora-sharing" {
		t.Fatalf("expected ServiceName=chora-sharing, got %q", obs.ServiceName)
	}
}
