// feed_cursor.go — opaque composite keyset cursor for the in-memory shared-atom
// feed. Mirrors the pg adapter's cursor format so the API contract is identical
// across adapters.
//
// Feed entry IDs are UUIDv5 deterministic hashes (NOT UUIDv7), so
// lexicographic id ordering does NOT reflect creation order. The cursor
// encodes (created_at, feed_entry_id) of the last row on the page. The cursor
// is base64-URL encoded ("RFC3339Nano|feedEntryID") so it stays opaque to the
// client. An empty cursor means "first page".
package inmem

import (
	"encoding/base64"
	"strings"
	"time"
)

// encodeInmemCursor packs a created_at + feed_entry_id into an opaque string.
// Returns "" when ts is the zero time (treated as "no cursor").
func encodeInmemCursor(ts time.Time, feedEntryID string) string {
	if ts.IsZero() {
		return ""
	}
	raw := ts.UTC().Format(time.RFC3339Nano) + "|" + feedEntryID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeInmemCursor reverses encodeInmemCursor. Returns the zero time + empty
// id when cursor is "" or malformed (callers treat empty as "first page").
func decodeInmemCursor(cursor string) (time.Time, string) {
	if cursor == "" {
		return time.Time{}, ""
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, ""
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, ""
	}
	ts, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, ""
	}
	id := strings.TrimSpace(parts[1])
	if id == "" {
		return time.Time{}, ""
	}
	return ts, id
}
