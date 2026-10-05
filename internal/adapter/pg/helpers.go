// helpers.go — shared pg adapter helpers (null-coalescing + scan plumbing)
// used across the chora-sharing pg repos.
package pg

import (
	"errors"
	"time"
)

var ErrDuelNotFound = errors.New("duel not found")

// nullStr returns nil for an empty string so Postgres NULLs match. Used for
// nullable UUID/text columns (source_event_id, parent_comment_id, etc.).
func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// nullTime returns nil for the zero time so Postgres NULLs match. Used for
// nullable TIMESTAMPTZ columns (revoked_at, expires_at, completed_at, ...).
func nullTime(t *time.Time) any {
	if t == nil || t.IsZero() {
		return nil
	}
	return *t
}

// nullTimeFromUnix returns nil for zero/unset times. Convenience for *time.Time
// fields where a nil pointer already denotes "unset".
func nullTimeFromUnix(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
// nonNilTags ensures a nil []string is passed as an empty Postgres array
// `{}' not NULL — the duel_sessions.interest_tags column is NOT NULL.
// Go's nil slice serializes to SQL NULL via pgx, which violates the
// constraint. This converts nil → empty slice before the upsert.
func nonNilTags(tags []string) []string {
	if tags == nil {
		return []string{}
	}
	return tags
}
