// feed_cursor_test.go — white-box coverage of the opaque composite keyset
// cursor (feed_cursor.go). encodeInmemCursor's zero-time branch and the
// malformed-input branches of decodeInmemCursor are fully reachable here;
// the valid round-trip path is also exercised indirectly through
// ListSharedAtoms pagination in share_test.go.
package inmem

import (
	"encoding/base64"
	"testing"
	"time"
)

func TestInmemCursor_RoundTrip(t *testing.T) {
	ts := time.Date(2026, 8, 8, 9, 0, 0, 123456789, time.UTC)
	enc := encodeInmemCursor(ts, "feed-1")
	if enc == "" {
		t.Fatal("expected non-empty cursor")
	}
	gotTS, gotID := decodeInmemCursor(enc)
	if !gotTS.Equal(ts) || gotID != "feed-1" {
		t.Fatalf("round-trip: got %v/%q", gotTS, gotID)
	}

	// Non-UTC input is normalised to UTC on encode.
	local := time.Date(2026, 8, 8, 9, 0, 0, 0, time.FixedZone("x", 3600))
	enc = encodeInmemCursor(local, "feed-2")
	gotTS, _ = decodeInmemCursor(enc)
	if !gotTS.Equal(time.Date(2026, 8, 8, 8, 0, 0, 0, time.UTC)) {
		t.Fatalf("UTC normalisation: got %v", gotTS)
	}

	// Zero time → no cursor.
	if enc := encodeInmemCursor(time.Time{}, "feed-3"); enc != "" {
		t.Fatalf("zero-time encode: want empty, got %q", enc)
	}
}

func TestInmemCursor_DecodeMalformed(t *testing.T) {
	// Empty cursor → zero time + empty id.
	ts, id := decodeInmemCursor("")
	if !ts.IsZero() || id != "" {
		t.Fatalf("empty cursor: got %v/%q", ts, id)
	}

	// Not valid base64.
	ts, id = decodeInmemCursor("!!not-base64!!")
	if !ts.IsZero() || id != "" {
		t.Fatalf("bad base64: got %v/%q", ts, id)
	}

	// Valid base64 but no separator.
	noSep := base64.RawURLEncoding.EncodeToString([]byte("no-pipe-here"))
	ts, id = decodeInmemCursor(noSep)
	if !ts.IsZero() || id != "" {
		t.Fatalf("no separator: got %v/%q", ts, id)
	}

	// Unparseable timestamp.
	badTime := base64.RawURLEncoding.EncodeToString([]byte("not-a-time|feed"))
	ts, id = decodeInmemCursor(badTime)
	if !ts.IsZero() || id != "" {
		t.Fatalf("bad timestamp: got %v/%q", ts, id)
	}

	// Whitespace-only id.
	emptyID := base64.RawURLEncoding.EncodeToString([]byte("2026-08-08T09:00:00Z|   "))
	ts, id = decodeInmemCursor(emptyID)
	if !ts.IsZero() || id != "" {
		t.Fatalf("empty id: got %v/%q", ts, id)
	}
}