package clients

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"
)
// stripCodeFences removes markdown code fences (```json ... ``` or ``` ... ```)
// that LLM models sometimes wrap JSON output in. Models vary by fence habit
// (gemini-2.5-flash fences, gemini-3.5-flash doesn't), and the fallback chain
// can land on a fencer — so every json.Unmarshal of model text must go through
// this helper. Trims surrounding whitespace too.
func stripCodeFences(s string) string {
	s = strings.TrimSpace(s)
	// Strip leading fence: ```json\n or ```\n
	if strings.HasPrefix(s, "```") {
		// Drop the opening fence line.
		if nl := strings.IndexByte(s, '\n'); nl >= 0 {
			s = strings.TrimSpace(s[nl+1:])
		} else {
			// Entire body was just the fence opener — nothing left.
			s = ""
		}
	}
	// Strip trailing fence: \n```
	s = strings.TrimSuffix(strings.TrimSpace(s), "```")
	return strings.TrimSpace(s)
}

// parseJSONEnvelope is the canonical JSON-decode path for LLM terminal text.
// It strips code fences + trims whitespace before unmarshalling into target.
// Every agent client (smith, conjurer, moderator, critic, quiz) must use this
// to avoid "invalid character '`'" errors when a fallback model fences its JSON.
func parseJSONEnvelope(text string, target any) error {
	return json.Unmarshal([]byte(stripCodeFences(text)), target)
}
// traceparentKey is the context.Value key carrying a caller-supplied W3C
// traceparent to forward to the model gateway.
type traceparentKey struct{}

// WithTraceparent returns a ctx carrying tp so a model-gateway Invoke forwards
// the caller's W3C trace context rather than minting a fresh one.
func WithTraceparent(ctx context.Context, tp string) context.Context {
	if tp == "" {
		return ctx
	}
	return context.WithValue(ctx, traceparentKey{}, tp)
}

// resolveTraceparent returns the caller-supplied traceparent from ctx, or mints
// a fresh W3C traceparent (version-00, sampled=01) when none is present.
func resolveTraceparent(ctx context.Context) string {
	if v, ok := ctx.Value(traceparentKey{}).(string); ok && v != "" {
		return v
	}
	return "00-" + randHex(16) + "-" + randHex(8) + "-01"
}

// readRand delegates to crypto/rand.Read (a seam for tests; not exported).
func readRand(b []byte) (int, error) { return rand.Read(b) }

// randHex returns n random bytes as a lowercase hex string.
func randHex(n int) string {
	b := make([]byte, n)
	_, _ = readRand(b)
	return encodeHex(b)
}

// nowNanos returns the current monotonic-ish nanos (seam for tests).
func nowNanos() int64 { return time.Now().UnixNano() }

// encodeHex lowercases to match the W3C traceparent convention.
func encodeHex(b []byte) string { return hex.EncodeToString(b) }
