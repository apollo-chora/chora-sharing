// zz_feed_cursor_a_test.go — unit tests for the unexported feed cursor
// codec (encodeFeedCursor / decodeFeedCursor) in feed_cursor.go.
//
// These functions are unexported, so this file uses the INTERNAL test
// package (`package pg`) while the rest of the suite lives in pg_test —
// Go runs both binaries in one `go test` invocation. No other production
// package file is touched; the cursor codec is pure (no Querier), so no
// stubs are needed here.
package pg

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

func TestEncodeFeedCursor_ZeroTime_ReturnsEmpty(t *testing.T) {
	if got := encodeFeedCursor(time.Time{}, "id"); got != "" {
		t.Fatalf("zero time must produce an empty cursor; got %q", got)
	}
}

func TestEncodeFeedCursor_HappyPath_RoundTrips(t *testing.T) {
	ts := time.Date(2026, 7, 11, 10, 30, 0, 123456789, time.UTC)
	encoded := encodeFeedCursor(ts, "feed-entry-1")
	if encoded == "" {
		t.Fatalf("expected a non-empty cursor")
	}
	if strings.Contains(encoded, "|") || strings.Contains(encoded, "+") || strings.Contains(encoded, "/") || strings.Contains(encoded, "=") {
		t.Fatalf("cursor must be base64-URL encoded (no raw pipe / + / / / =); got %q", encoded)
	}
	decodedTS, decodedID := decodeFeedCursor(encoded)
	if !decodedTS.Equal(ts) {
		t.Fatalf("round-trip timestamp = %v; want %v", decodedTS, ts)
	}
	if decodedID != "feed-entry-1" {
		t.Fatalf("round-trip id = %q; want %q", decodedID, "feed-entry-1")
	}
}

func TestDecodeFeedCursor_Empty_ReturnsZero(t *testing.T) {
	ts, id := decodeFeedCursor("")
	if !ts.IsZero() || id != "" {
		t.Fatalf("empty cursor must decode to (zero, \"\"); got (%v, %q)", ts, id)
	}
}

func TestDecodeFeedCursor_MalformedBase64_ReturnsZero(t *testing.T) {
	ts, id := decodeFeedCursor("!!!not-base64!!!")
	if !ts.IsZero() || id != "" {
		t.Fatalf("malformed base64 must decode to (zero, \"\"); got (%v, %q)", ts, id)
	}
}

func TestDecodeFeedCursor_NoSeparator_ReturnsZero(t *testing.T) {
	raw := b64URL("2026-07-11T10:30:00Z") // no pipe separator
	ts, id := decodeFeedCursor(raw)
	if !ts.IsZero() || id != "" {
		t.Fatalf("cursor without separator must decode to (zero, \"\"); got (%v, %q)", ts, id)
	}
}

func TestDecodeFeedCursor_BadTimestamp_ReturnsZero(t *testing.T) {
	raw := b64URL("not-a-time|id-1")
	ts, id := decodeFeedCursor(raw)
	if !ts.IsZero() || id != "" {
		t.Fatalf("cursor with bad timestamp must decode to (zero, \"\"); got (%v, %q)", ts, id)
	}
}

func TestDecodeFeedCursor_EmptyID_ReturnsZero(t *testing.T) {
	raw := b64URL("2026-07-11T10:30:00Z|  ")
	ts, id := decodeFeedCursor(raw)
	if !ts.IsZero() || id != "" {
		t.Fatalf("cursor with blank id must decode to (zero, \"\"); got (%v, %q)", ts, id)
	}
}

// b64URL is a tiny local base64-URL encoder for building malformed-input
// fixtures (mirrors the production codec's encoding).
func b64URL(s string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(s))
}