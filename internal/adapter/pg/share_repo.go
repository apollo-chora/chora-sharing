// share_repo.go — pgx-backed atom_share.ShareRepo.
//
// Maps the immutable shared-atom feed entry to the social_feed_entries table
// (entry_type='share') + the append-only ShareEvent children to
// atom_share_events. RLS is applied on every method via qToExecer(q) before
// any user query, per multi-tenant-rls + the runtime.go contract.
//
// Schema: migrations/0002_social.up.sql (social_feed_entries) +
// migrations/0004_atom_sharing.up.sql (atom_share_events).
package pg

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
)

// shareContent is the JSONB payload stored on social_feed_entries.content for
// entry_type='share'. It carries the R1 attribution snapshot + license + rate
// so the feed never does a cross-DB read of chora_creation or chora_identity.
type shareContent struct {
	AtomID            string                  `json:"atom_id"`
	RevisionID        string                  `json:"atom_revision_id"`
	OwnerGCID         string                  `json:"owner_gcid"`
	AuthorDisplayName string                  `json:"author_display_name"`
	StemPreview       string                  `json:"stem_preview"`
	QuestionType      string                  `json:"question_type"`
	Options           []string                `json:"options"`
	Caption           string                  `json:"caption"`
	License           atom_share.LicenseTerms `json:"license_terms"`
	Rate              atom_share.RoyaltyRate  `json:"royalty_rate,omitempty"`
}

// SQL constants — reviewable, parametrised.
const (
	sqlShareInsert = `
INSERT INTO social_feed_entries
    (id, tenant_id, actor_gcid, entry_type, content, created_at)
VALUES ($1, $2, $3, 'share', $4, $5)
ON CONFLICT (id) DO NOTHING`

	// sqlShareGetByID loads a single share, joining against atom_share_events
	// so a revoked entry returns no row (StatusRevoked → ErrNotFound, per the
	// port contract).
	sqlShareGetByID = `
SELECT e.id, e.tenant_id, e.content, e.created_at
FROM social_feed_entries e
WHERE e.id = $1 AND e.entry_type = 'share'
  AND NOT EXISTS (
      SELECT 1 FROM atom_share_events ev
      WHERE ev.feed_entry_id = e.id AND ev.event_type = 'revoked'
  )`

	// sqlShareGetByAtom loads the newest visible share for a given atom +
	// owner. Revoked entries are excluded (NOT EXISTS on 'revoked' events),
	// mirroring the read-side status contract. Used by the revoke flow to
	// resolve which feed entry to append EventRevoked onto.
	sqlShareGetByAtom = `
SELECT e.id, e.tenant_id, e.content, e.created_at
FROM social_feed_entries e
WHERE e.entry_type = 'share'
  AND (e.content->>'atom_id') = $1
  AND (e.content->>'owner_gcid') = $2
  AND NOT EXISTS (
      SELECT 1 FROM atom_share_events ev
      WHERE ev.feed_entry_id = e.id AND ev.event_type = 'revoked'
  )
ORDER BY e.created_at DESC
LIMIT 1`

	// sqlShareListNewest selects visible shares newest-first, keyset-paginated
	// by (created_at DESC, id DESC). Revoked + hidden shares are excluded by
	// the NOT EXISTS subquery that derives the read-side status from
	// atom_share_events (mirrors atom_share.DeriveStatus precedence).
	//
	// Ordering is by created_at (NOT id) because feed entry IDs are UUIDv5
	// deterministic hashes (deriveFeedEntryID → uuid.NewSHA1), NOT UUIDv7 —
	// lexicographic id ordering does NOT reflect creation order. The keyset
	// cursor encodes (created_at, feed_entry_id) of the last row on the page.
	//
	// The cursor predicate uses NULLIF so an empty cursor (first page)
	// disables the filter: when $7 is NULL the OR-short-circuits and no rows
	// are excluded. The caller passes NULL for the timestamp param when the
	// cursor is empty/first-page.
	sqlShareListNewest = `
SELECT e.id, e.tenant_id, e.content, e.created_at
FROM social_feed_entries e
WHERE e.entry_type = 'share'
  AND ($4::text = 'tenant'   AND e.tenant_id = $1::uuid
       OR $4::text = 'global'
       OR $4::text = 'following' AND e.actor_gcid = ANY($5::uuid[]))
  AND (
      $6::timestamptz IS NULL
      OR e.created_at < $6::timestamptz
      OR (e.created_at = $6::timestamptz AND e.id < $7::uuid)
  )
  AND NOT EXISTS (
      SELECT 1 FROM atom_share_events ev
      WHERE ev.feed_entry_id = e.id
        AND ev.event_type IN ('revoked', 'hidden', 'moderation_hidden')
  )
  AND (cardinality($2::text[]) = 0 OR
       (e.content->>'question_type') ILIKE ANY($2))
  AND (cardinality($8::uuid[]) = 0 OR e.actor_gcid <> ALL($8::uuid[]))
ORDER BY e.created_at DESC, e.id DESC
LIMIT $3`

	// sqlShareEventInsert appends an atom_share_events row. id is defaulted by
	// gen_random_uuid(); ON CONFLICT (source_event_id) makes a replay a no-op.
	sqlShareEventInsert = `
INSERT INTO atom_share_events
    (tenant_id, feed_entry_id, actor_gcid, event_type, payload, source_event_id, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (source_event_id) WHERE source_event_id IS NOT NULL DO NOTHING`

	// sqlOutboxInsert writes a pending outbox row inside the same transaction
	// as the feed entry insert. The Relay dispatcher drains it to Pub/Sub.
	sqlOutboxInsert = `
INSERT INTO sharing_outbox_events
    (id, tenant_id, gcid, aggregate_type, aggregate_id, event_type, topic,
     payload, envelope, idempotency_key, occurred_at, status)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, 'pending')
ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`

	// sqlShareEventExisting loads the conflicting row's payload so a replay
	// with a DIFFERENT payload returns ErrConflict (mirrors inmem behavior).
	sqlShareEventExisting = `
SELECT actor_gcid, event_type, payload, created_at
FROM atom_share_events
WHERE source_event_id = $1`

	// sqlShareTenantForEntry resolves the tenant_id for an event child row
	// from its parent feed entry (ShareEvent has no TenantID field).
	sqlShareTenantForEntry = `
SELECT tenant_id FROM social_feed_entries WHERE id = $1`

	sqlShareEventsByEntry = `
SELECT id, actor_gcid, event_type, payload, source_event_id, created_at
FROM atom_share_events
WHERE feed_entry_id = $1
ORDER BY created_at DESC`
)

// ShareRepo implements atom_share.ShareRepo against Postgres.
type ShareRepo struct {
	tx        TxRunner
	outboxBus OutboxWriter // optional; when set, SaveShare writes an outbox row in the same txn
}

// OutboxWriter is the minimal interface for writing an outbox row within the
// ShareRepo's transaction. When wired (production), SaveShare atomically
// publishes chora.sharing.atom.shared.v1. When nil (tests), SaveShare only
// inserts the feed entry.
type OutboxWriter interface {
	WriteOutboxRow(ctx context.Context, q Querier, row OutboxRow) error
}

// OutboxRow is the outbox row data ShareRepo writes when an OutboxWriter is wired.
type OutboxRow struct {
	ID             string
	TenantID       string
	GCID           string
	AggregateType  string
	AggregateID    string
	EventType      string
	Topic          string
	Payload         []byte
	Envelope       map[string]string
	IdempotencyKey string
	OccurredAt     time.Time
}

// NewShareRepo constructs a ShareRepo bound to a TxRunner.
func NewShareRepo(tx TxRunner) *ShareRepo { return &ShareRepo{tx: tx} }

// WithOutboxBus wires an OutboxWriter so SaveShare publishes domain events
// atomically with the feed entry insert. Returns the receiver for chaining.
func (r *ShareRepo) WithOutboxBus(b OutboxWriter) *ShareRepo {
	r.outboxBus = b
	return r
}

// Compile-time port assertion.
var _ atom_share.ShareRepo = (*ShareRepo)(nil)

// SaveShare persists the immutable feed entry. Idempotent on FeedEntryID via
// ON CONFLICT (id) DO NOTHING — a replay is a no-op.
func (r *ShareRepo) SaveShare(ctx context.Context, s *atom_share.Share) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if s == nil {
		return atom_share.ErrInvalidArgument
	}
	content, err := json.Marshal(shareContent{
		AtomID:            s.AtomID,
		RevisionID:        s.RevisionID,
		OwnerGCID:         s.OwnerGCID,
		AuthorDisplayName: s.AuthorDisplayName,
		StemPreview:       s.StemPreview,
		QuestionType:      s.QuestionType,
		Options:           s.Options,
		Caption:           s.Caption,
		License:           s.License,
		Rate:              s.Rate,
	})
	if err != nil {
		return fmt.Errorf("pg: marshal share content: %w", err)
	}
	created := s.CreatedAt
	if created.IsZero() {
		created = time.Now().UTC()
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		if _, err := q.Exec(ctx, sqlShareInsert,
			s.FeedEntryID, s.TenantID, s.OwnerGCID, content, created,
		); err != nil {
			return fmt.Errorf("pg: insert share feed entry: %w", err)
		}
		// Atomically publish chora.sharing.atom.shared.v1 via the outbox
		// when an OutboxWriter is wired. The outbox row lands in the same
		// transaction so a crash never orphans the event.
		if r.outboxBus != nil {
			envelope := map[string]string{
				"event_id":   s.FeedEntryID,
				"tenant_id":  s.TenantID,
				"gcid":       s.OwnerGCID,
				"occurred_at": created.Format(time.RFC3339Nano),
			}
			payload := map[string]any{
				"feed_entry_id":       s.FeedEntryID,
				"atom_id":             s.AtomID,
				"owner_gcid":          s.OwnerGCID,
				"author_display_name": s.AuthorDisplayName,
				"question_type":      s.QuestionType,
				"caption":             s.Caption,
				"license_terms":       string(s.License),
				"created_at":           created.Format(time.RFC3339Nano),
				"occurred_at":          created.Format(time.RFC3339Nano),
			}
			payloadBytes, err := json.Marshal(payload)
			if err != nil {
				return fmt.Errorf("pg: marshal share outbox payload: %w", err)
			}
			if err := r.outboxBus.WriteOutboxRow(ctx, q, OutboxRow{
				ID:             s.FeedEntryID,
				TenantID:       s.TenantID,
				GCID:           s.OwnerGCID,
				AggregateType:  "atom",
				AggregateID:    s.AtomID,
				EventType:      "atom.shared.v1",
				Topic:          "chora.sharing.atom.shared.v1",
				Payload:         payloadBytes,
				Envelope:       envelope,
				IdempotencyKey: s.FeedEntryID,
				OccurredAt:     created,
			}); err != nil {
				return fmt.Errorf("pg: write share outbox row: %w", err)
			}
		}
		return nil
	})
}

// GetShare loads a single share by feed entry id. Returns ErrNotFound when the
// entry is missing or revoked (revoked entries are excluded by the join
// against atom_share_events — mirrors the read-side status contract).
func (r *ShareRepo) GetShare(ctx context.Context, feedEntryID string) (*atom_share.Share, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	var (
		id      string
		tenant  string
		raw     []byte
		created time.Time
	)
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		if err := q.QueryRow(ctx, sqlShareGetByID, feedEntryID).Scan(
			&id, &tenant, &raw, &created,
		); err != nil {
			return atom_share.ErrNotFound
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return scanShare(id, tenant, raw, created)
}

// GetShareByAtom loads the newest visible share for a given atom + owner.
// Returns ErrNotFound when no visible share exists (never shared or already
// revoked). The revocation exclude uses the same NOT EXISTS pattern as
// sqlShareGetByID so derived status stays consistent across reads.
func (r *ShareRepo) GetShareByAtom(ctx context.Context, atomID, ownerGCID string) (*atom_share.Share, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	var (
		id      string
		tenant  string
		raw     []byte
		created time.Time
	)
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		if err := q.QueryRow(ctx, sqlShareGetByAtom, atomID, ownerGCID).Scan(
			&id, &tenant, &raw, &created,
		); err != nil {
			return atom_share.ErrNotFound
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return scanShare(id, tenant, raw, created)
}

// ListSharedAtoms is a keyset-paginated read (cursor = opaque composite of
// created_at + feed_entry_id, newest-first). Revoked + hidden shares are
// excluded by derived status (the NOT EXISTS subquery over atom_share_events).
func (r *ShareRepo) ListSharedAtoms(ctx context.Context, tenantID string, cursor string, limit int, topicFilter, questionTypeFilter string, scope string, followingGCIDs []string, blockedGCIDs []string) ([]atom_share.Share, string, error) {
	if r == nil || r.tx == nil {
		return nil, "", ErrNotImplemented
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if scope == "" {
		scope = "tenant"
	}
	// topicFilter + questionTypeFilter both filter on question_type (the
	// inmem treats them as the same field); build the ANY array once.
	filters := []string{}
	if topicFilter != "" {
		filters = append(filters, topicFilter)
	}
	if questionTypeFilter != "" && questionTypeFilter != topicFilter {
		filters = append(filters, questionTypeFilter)
	}

	// Decode the opaque composite cursor (created_at, feed_entry_id). An
	// empty/malformed cursor means first page — pass NULL for BOTH params so
	// the SQL predicate short-circuits ($7 IS NULL → OR-true). Passing "" for
	// $8::uuid would error on cast.
	cursorTS, cursorID := decodeFeedCursor(cursor)
	var cursorTSPtr, cursorIDPtr interface{}
	if !cursorTS.IsZero() {
		cursorTSPtr = cursorTS
		cursorIDPtr = cursorID
	} else {
		cursorTSPtr = nil
		cursorIDPtr = nil
	}

	var out []atom_share.Share
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		rows, err := q.Query(ctx, sqlShareListNewest, tenantID, filters, limit, scope, followingGCIDs, cursorTSPtr, cursorIDPtr, blockedGCIDs)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var (
				id      string
				tenant  string
				raw     []byte
				created time.Time
			)
			if err := rows.Scan(&id, &tenant, &raw, &created); err != nil {
				return err
			}
			s, err := scanShare(id, tenant, raw, created)
			if err != nil {
				return err
			}
			out = append(out, *s)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) == limit {
		last := out[limit-1]
		next = encodeFeedCursor(last.CreatedAt, last.FeedEntryID)
	}
	if out == nil {
		out = []atom_share.Share{}
	}
	return out, next, nil
}

// AppendEvent appends an append-only ShareEvent child row. Idempotent on
// SourceEventID when non-empty: a replay with the same payload is a no-op
// (ON CONFLICT DO NOTHING); a replay with a different payload returns
// ErrConflict.
//
// ShareEvent does not carry tenant_id, so the child row inherits it from the
// parent feed entry (social_feed_entries.tenant_id) — resolved inside the
// same RLS-scoped transaction so the FK lineage stays correct.
func (r *ShareRepo) AppendEvent(ctx context.Context, e *atom_share.ShareEvent) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if e == nil {
		return atom_share.ErrInvalidArgument
	}
	payload, err := json.Marshal(e.Payload)
	if err != nil {
		payload = []byte("{}")
	}
	created := e.CreatedAt
	if created.IsZero() {
		created = time.Now().UTC()
	}
	sourceEvent := nullStr(e.SourceEventID)

	var conflict bool
	err = r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		// Resolve tenant_id from the parent feed entry (NOT NULL on the
		// child row; ShareEvent has no TenantID field).
		var tenantID string
		if err := q.QueryRow(ctx, sqlShareTenantForEntry, e.FeedEntryID).Scan(&tenantID); err != nil {
			return fmt.Errorf("pg: resolve share tenant for event: %w", atom_share.ErrNotFound)
		}
		tag, err := q.Exec(ctx, sqlShareEventInsert,
			tenantID, e.FeedEntryID, e.ActorGCID,
			string(e.Type), []byte(payload), sourceEvent, created,
		)
		if err != nil {
			return fmt.Errorf("pg: insert share event: %w", err)
		}
		if tag.RowsAffected > 0 {
			return nil // inserted — done
		}
		// ON CONFLICT path: a row with this source_event_id already exists.
		if e.SourceEventID == "" {
			return nil // empty source_event_id + no-op insert
		}
		// Load the existing payload to detect a conflicting replay.
		var (
			exActor     string
			exType      string
			exPayload   []byte
			exCreatedAt time.Time
		)
		scanErr := q.QueryRow(ctx, sqlShareEventExisting, e.SourceEventID).Scan(
			&exActor, &exType, &exPayload, &exCreatedAt,
		)
		if scanErr != nil {
			return nil // row vanished between insert + select — treat as no-op
		}
		if exActor != e.ActorGCID || exType != string(e.Type) {
			conflict = true
			return atom_share.ErrConflict
		}
		var existingPayload map[string]any
		_ = json.Unmarshal(exPayload, &existingPayload)
		if !payloadEqual(existingPayload, e.Payload) {
			conflict = true
			return atom_share.ErrConflict
		}
		return nil // idempotent replay — same payload
	})
	if err != nil && err != atom_share.ErrConflict {
		return err
	}
	if conflict {
		return atom_share.ErrConflict
	}
	return nil
}

// ResolveDisplayName returns the most recent author_display_name from
// social_feed_entries for the given gcid. This is a same-DB read used by
// the connections handler to show display names without a cross-DB identity
// lookup. Returns "" when no share exists for the gcid.
func (r *ShareRepo) ResolveDisplayName(ctx context.Context, gcid string) (string, error) {
	if r == nil || r.tx == nil {
		return "", ErrNotImplemented
	}
	const sql = `
SELECT e.content->>'author_display_name'
FROM social_feed_entries e
WHERE e.entry_type = 'share'
  AND (e.content->>'owner_gcid') = $1
  AND e.content->>'author_display_name' != ''
ORDER BY e.created_at DESC
LIMIT 1`
	var name string
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		return q.QueryRow(ctx, sql, gcid).Scan(&name)
	})
	if err != nil {
		return "", nil // best-effort — empty name triggers FE gcid fallback
	}
	return name, nil
}

// ListEvents returns the events for a feed entry, newest-first.
func (r *ShareRepo) ListEvents(ctx context.Context, feedEntryID string) ([]atom_share.ShareEvent, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	var out []atom_share.ShareEvent
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		rows, err := q.Query(ctx, sqlShareEventsByEntry, feedEntryID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var (
				id, actor  string
				evType     string
				rawPayload []byte
				sourceEvt  *string
				createdAt  time.Time
			)
			if err := rows.Scan(&id, &actor, &evType, &rawPayload, &sourceEvt, &createdAt); err != nil {
				return err
			}
			ev := atom_share.ShareEvent{
				FeedEntryID: feedEntryID,
				ActorGCID:   actor,
				Type:        atom_share.ShareEventType(evType),
				CreatedAt:   createdAt,
			}
			if sourceEvt != nil {
				ev.SourceEventID = *sourceEvt
			}
			if len(rawPayload) > 0 {
				_ = json.Unmarshal(rawPayload, &ev.Payload)
			}
			out = append(out, ev)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	if out == nil {
		out = []atom_share.ShareEvent{}
	}
	return out, nil
}

// scanShare hydrates a Share from a content JSONB blob.
func scanShare(id, tenant string, raw []byte, created time.Time) (*atom_share.Share, error) {
	var c shareContent
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("pg: unmarshal share content: %w", err)
	}
	return &atom_share.Share{
		FeedEntryID:       id,
		TenantID:          tenant,
		AtomID:            c.AtomID,
		RevisionID:        c.RevisionID,
		OwnerGCID:         c.OwnerGCID,
		AuthorDisplayName: c.AuthorDisplayName,
		StemPreview:       c.StemPreview,
		QuestionType:      c.QuestionType,
		Options:           c.Options,
		Caption:           c.Caption,
		License:           c.License,
		Rate:              c.Rate,
		CreatedAt:         created,
	}, nil
}

// payloadEqual compares two free-form JSON payloads for structural equality.
func payloadEqual(a, b map[string]any) bool {
	aj, _ := json.Marshal(a)
	bj, _ := json.Marshal(b)
	return string(aj) == string(bj)
}
