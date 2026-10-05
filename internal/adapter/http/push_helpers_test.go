// push_helpers_test.go — shared helpers for Pub/Sub push-handler tests
// (httpadapter_test package).
//
// weakness_grown_pubsub_handler_test.go has referenced pushBody /
// dispatchSharing since ADR-196 B3, but the helpers file never landed — the
// http test package did NOT compile on main (`undefined: pushBody`) until
// this file restored it (found during ADR-229 WS-0 / CHO-2102).
package httpadapter_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// pushBody builds the Cloud Pub/Sub push-delivery JSON envelope: the payload
// map marshals to JSON and rides base64-encoded in message.data; the topic
// travels as a message attribute (the dispatch switch keys off it).
func pushBody(t *testing.T, topic string, payload map[string]any) []byte {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("pushBody: marshal payload: %v", err)
	}
	env := map[string]any{
		"message": map[string]any{
			"data":        base64.StdEncoding.EncodeToString(data),
			"messageId":   "m-test-1",
			"publishTime": time.Now().UTC().Format(time.RFC3339),
			"attributes":  map[string]string{"topic": topic},
		},
		"subscription": "projects/chora-test/subscriptions/test-push",
	}
	body, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("pushBody: marshal envelope: %v", err)
	}
	return body
}

// dispatchSharing POSTs a push envelope at the handler and returns the
// recorder (the Pub/Sub push contract: 2xx acks, 5xx nacks -> retry/DLQ).
func dispatchSharing(t *testing.T, h http.Handler, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// pushBodyRaw builds the Pub/Sub push envelope around ALREADY-ENCODED payload
// bytes — used to drive a handler with the real binary-protobuf wire format a
// producer publishes, rather than hand-built JSON that only ever exercises
// protodecode's fallback path.
func pushBodyRaw(t *testing.T, topic string, payload []byte) []byte {
	t.Helper()
	env := map[string]any{
		"message": map[string]any{
			"data":        base64.StdEncoding.EncodeToString(payload),
			"messageId":   "m-test-raw-1",
			"publishTime": time.Now().UTC().Format(time.RFC3339),
			"attributes":  map[string]string{"topic": topic},
		},
		"subscription": "projects/chora-test/subscriptions/test-push",
	}
	body, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("pushBodyRaw: marshal envelope: %v", err)
	}
	return body
}
