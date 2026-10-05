// Package keyset is the shared opaque-cursor codec for the chora-sharing
// keyset-paginated feeds (comments, bookmarks, the shared-atom feed).
//
// Every one of those feeds orders newest-first on (created_at DESC, id DESC)
// and resumes strictly after the last row of the previous page. The cursor is
// therefore the (created_at, id) TUPLE of that row — never a bare id, because
// two rows minted inside the same millisecond would tie on created_at and a
// bare id could not disambiguate them.
//
// The wire form is base64url(no padding) of "<RFC3339Nano>|<id>", so the token
// is opaque, URL-safe, and cheap to decode. A malformed, empty, or
// partial cursor decodes to (zero time, "") — which every caller treats as
// "first page". That is deliberately forgiving: a cursor is client-supplied
// and a bad one must degrade to the first page rather than 500.
package keyset

import (
	"encoding/base64"
	"strings"
	"time"
)

// separator delimits the timestamp from the id inside the encoded payload.
const separator = "|"

// Encode packs a (created_at, id) tuple into an opaque cursor token. Returns
// "" when ts is the zero time or id is empty — the caller's "no next page"
// signal.
func Encode(ts time.Time, id string) string {
	if ts.IsZero() || id == "" {
		return ""
	}
	raw := ts.UTC().Format(time.RFC3339Nano) + separator + id
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// Decode reverses Encode. It returns the zero time and an empty id for an
// empty, malformed, separator-less, unparseable-timestamp, or blank-id cursor.
func Decode(cursor string) (time.Time, string) {
	if cursor == "" {
		return time.Time{}, ""
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, ""
	}
	tsPart, idPart, ok := strings.Cut(string(raw), separator)
	if !ok {
		return time.Time{}, ""
	}
	id := strings.TrimSpace(idPart)
	if id == "" {
		return time.Time{}, ""
	}
	ts, err := time.Parse(time.RFC3339Nano, tsPart)
	if err != nil {
		return time.Time{}, ""
	}
	return ts, id
}

// Less reports whether the tuple (aTS, aID) sorts strictly BEFORE (bTS, bID)
// in the feeds' (created_at, id) tuple order — i.e. a is OLDER than b.
//
// Callers use it two ways:
//   - to sort newest-first: sort.Slice(x, func(i, j int) bool { return Less(x[j], x[i]) })
//   - to resume a page: skip while !Less(row, cursor), stopping at the first
//     row strictly older than the cursor tuple.
func Less(aTS time.Time, aID string, bTS time.Time, bID string) bool {
	if aTS.Equal(bTS) {
		return aID < bID
	}
	return aTS.Before(bTS)
}
