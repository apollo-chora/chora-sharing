// feed_cursor.go: the shared-atom feed's page token.
//
// The feed entry IDs are UUIDv5 deterministic hashes (deriveFeedEntryID ->
// uuid.NewSHA1), NOT UUIDv7, so lexicographic ordering on id does NOT
// reflect creation order. Newest-first ordering MUST be by created_at DESC
// (with id DESC as a stable tie-break). The cursor therefore encodes the
// (created_at, feed_entry_id) pair of the last row on the current page so
// the next page resumes strictly after it.
//
// The codec itself is internal/keyset, shared with the comment and bookmark
// feeds, which resume on the same (created_at, id) tuple. These two names are
// kept as the feed's spelling of it.
package pg

import (
	"time"

	"github.com/apollo-chora/chora-sharing/internal/keyset"
)

// encodeFeedCursor packs a created_at + feed_entry_id into an opaque string.
// Returns "" when ts is the zero time (treated as "no cursor").
func encodeFeedCursor(ts time.Time, feedEntryID string) string {
	return keyset.Encode(ts, feedEntryID)
}

// decodeFeedCursor reverses encodeFeedCursor. Returns the zero time + empty
// id when cursor is "" or malformed (callers treat empty as "first page").
func decodeFeedCursor(cursor string) (time.Time, string) {
	return keyset.Decode(cursor)
}
