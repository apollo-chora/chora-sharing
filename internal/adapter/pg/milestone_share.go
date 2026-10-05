// milestone_share.go — pgx-backed, durable implementations of the four
// FamiliarMilestoneSubscriber ports (CHO-2203, parent CHO-1889):
//
//	subscribers.DraftStore        → post_drafts            (migration 0039)
//	subscribers.PreferenceStore   → user_preferences       (migration 0040)
//	subscribers.IdempotencyStore  → subscriber_idempotency (migration 0041)
//	subscribers.PostPublisher     → posts + sharing_outbox_events (atomic)
//
// These REPLACE the in-memory doubles in the subscribers package. The package
// doc's "cmd/server wires Postgres implementations of those ports" was a phantom
// until this file landed: no pg adapter existed, none of the three tables
// existed, and the FamiliarMilestoneSubscriber was bound only to the in-process
// InMemoryBus (nothing external published there). So every Familiar milestone
// share drafted/auto-posted since PROD-G was written to a map and died with the
// pod. This lane is now durable end-to-end.
//
// RLS (multi-tenant-rls + feedback_resilience_priority): every method opens a
// transaction and calls rls.ApplySession (SET LOCAL chora.tenant_id) BEFORE the
// user query, so the tenant_isolation policy on each table enforces isolation
// and PgBouncer transaction-pooling never leaks the GUC across siblings. Three
// adapters carry an explicit tenant (Draft.TenantID / the tenantID arg) and
// stamp it onto ctx themselves (closure_repository.go convention); the
// IdempotencyStore Seen/Mark ports have NO tenant parameter — they are shared by
// 15 call sites across 8 subscribers, so widening them was out of scope — and
// instead read the tenant from ctx (tracing.TenantIDFromContext) and fail LOUD
// when it is absent (never a tenant-less / fail-open dedup read or write).
//
// Cross-DB queries forbidden — this repo reads only chora_sharing.
package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/adapter/subscribers"
	"github.com/apollo-chora/chora-sharing/internal/domain/post"
)

// milestoneTenantTx stamps the explicit tenant onto ctx (fail-loud on empty via
// rls.ErrNoTenantContext inside ApplySession) then applies the RLS session
// before fn. Shared by the three explicit-tenant milestone adapters; mirrors
// ClosureRepository.run without duplicating it four times.
func milestoneTenantTx(ctx context.Context, tx TxRunner, tenantID string, fn func(ctx context.Context, q Querier) error) error {
	ctx = tracing.WithTenantID(ctx, strings.TrimSpace(tenantID))
	return tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		return fn(ctx, q)
	})
}

// -----------------------------------------------------------------------------
// MilestoneDraftStore — post_drafts (content aggregate; soft-delete)
// -----------------------------------------------------------------------------

// MilestoneDraftStore is the pg-backed subscribers.DraftStore (the subscriber's
// write side) AND the owner-facing C+ drafts API store (list / publish /
// discard). One type over one table: the two consumers take the narrow port
// each needs, but there is a single implementation, because two adapters over
// one table is exactly how ReactionStore silently drifted.
type MilestoneDraftStore struct {
	tx     TxRunner
	outbox OutboxWriter
}

// NewMilestoneDraftStore constructs a MilestoneDraftStore using the canonical
// DefaultOutboxWriter (the same outbox spine as ShareRepo + MilestonePostPublisher).
// A nil TxRunner makes every method return ErrNotImplemented (dev-fallback /
// misuse guard).
func NewMilestoneDraftStore(tx TxRunner) *MilestoneDraftStore {
	return &MilestoneDraftStore{tx: tx, outbox: NewDefaultOutboxWriter()}
}

var _ subscribers.DraftStore = (*MilestoneDraftStore)(nil)

// Reconciles the pre-existing out-of-band table (migration 0039):
// composed_from_event_id is UUID, status is enum post_draft_status
// (pending|published|discarded), and the dedup UNIQUE is
// (composed_from_topic, composed_from_event_id) — event_id is a globally-unique
// UUIDv7 so this is a sound global dedup key. The $N::type casts follow the
// established SQLUpsertPost ($7::post_visibility) pattern.
const sqlDraftInsert = `
INSERT INTO post_drafts
    (draft_id, tenant_id, author_gcid, composed_from_topic, composed_from_event_id,
     body, metadata, status, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5::uuid, $6, $7::jsonb, $8::post_draft_status, $9, now())
ON CONFLICT (composed_from_topic, composed_from_event_id) DO NOTHING
RETURNING draft_id`

const sqlDraftListPending = `
SELECT draft_id, tenant_id, author_gcid, composed_from_topic, composed_from_event_id,
       body, metadata, status, created_at
FROM post_drafts
WHERE tenant_id = $1 AND author_gcid = $2 AND status = 'pending' AND deleted_at IS NULL
ORDER BY created_at DESC`

// The owner-facing mutations are OWNER-scoped, not merely tenant-scoped.
//
// RLS constrains the TENANT — every learner in a tenant shares one policy — so
// a (tenant_id, draft_id) predicate lets learner B publish or discard learner
// A's Familiar milestone into the C+ feed. author_gcid = the caller closes that.
// This is why the original MarkPublished/MarkDiscarded (tenant+draft only) are
// gone rather than merely unused: they were a cross-learner write waiting for
// its first caller (CHO-2258).

// sqlDraftLockForPublish takes the row lock and yields the body to compose the
// Post from. FOR UPDATE serialises two concurrent publishes of one draft: the
// loser re-reads status='published' and matches no row → ErrDraftNotPending.
const sqlDraftLockForPublish = `
SELECT body, metadata
FROM post_drafts
WHERE tenant_id = $1 AND draft_id = $2 AND author_gcid = $3
  AND status = 'pending' AND deleted_at IS NULL
FOR UPDATE`

// Publishing is NOT a deletion — a published draft is history, and the post it
// produced lives in `posts`.
const sqlDraftMarkPublished = `
UPDATE post_drafts SET status = 'published', updated_at = now()
WHERE tenant_id = $1 AND draft_id = $2 AND author_gcid = $3 AND deleted_at IS NULL`

// Discard is a SOFT-delete (content aggregate — ddd-enforcement #6).
const sqlDraftDiscard = `
UPDATE post_drafts SET status = 'discarded', deleted_at = now(), updated_at = now()
WHERE tenant_id = $1 AND draft_id = $2 AND author_gcid = $3 AND deleted_at IS NULL`

// Insert idempotently records a Draft. created=true means THIS call inserted a
// fresh row; created=false means the ON CONFLICT fired (a redelivery already
// drafted this milestone) — distinguished by Next()/Err(), never by assuming
// "no row" == conflict (a genuine backing-store error must propagate).
func (r *MilestoneDraftStore) Insert(ctx context.Context, d subscribers.Draft) (bool, error) {
	if r == nil || r.tx == nil {
		return false, ErrNotImplemented
	}
	if strings.TrimSpace(d.TenantID) == "" {
		return false, errors.New("pg.MilestoneDraftStore.Insert: tenant_id required")
	}
	meta, err := marshalMetadata(d.Metadata)
	if err != nil {
		return false, fmt.Errorf("pg.MilestoneDraftStore.Insert: %w", err)
	}
	status := strings.TrimSpace(d.Status)
	if status == "" {
		status = "pending"
	}
	created := d.CreatedAt
	if created.IsZero() {
		created = time.Now().UTC()
	}
	inserted := false
	err = milestoneTenantTx(ctx, r.tx, d.TenantID, func(ctx context.Context, q Querier) error {
		rows, err := q.Query(ctx, sqlDraftInsert,
			d.DraftID, d.TenantID, d.AuthorGCID, d.ComposedFromTopic, d.ComposedFromEventID,
			d.Body, meta, status, created,
		)
		if err != nil {
			return fmt.Errorf("pg: insert post_draft: %w", err)
		}
		defer rows.Close()
		if !rows.Next() {
			return rows.Err() // no RETURNING row: clean conflict (Err()==nil) or real error
		}
		inserted = true
		return rows.Err()
	})
	if err != nil {
		return false, fmt.Errorf("pg.MilestoneDraftStore.Insert: %w", err)
	}
	return inserted, nil
}

// ListPending returns the owner's pending, non-deleted drafts.
func (r *MilestoneDraftStore) ListPending(ctx context.Context, tenantID, gcid string) ([]subscribers.Draft, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	out := make([]subscribers.Draft, 0)
	err := milestoneTenantTx(ctx, r.tx, tenantID, func(ctx context.Context, q Querier) error {
		rows, err := q.Query(ctx, sqlDraftListPending, tenantID, gcid)
		if err != nil {
			return fmt.Errorf("pg: list pending drafts: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			d, err := scanDraft(rows.Scan)
			if err != nil {
				return err
			}
			out = append(out, d)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("pg.MilestoneDraftStore.ListPending: %w", err)
	}
	return out, nil
}

// PublishDraft turns the owner's pending draft into a real C+ Post in ONE
// transaction: lock the draft → upsert the Post → write the
// chora.sharing.post.created.v1 outbox row → flip the draft to 'published'.
//
// The single transaction is the point. As separate transactions, a crash after
// the post insert leaves the draft 'pending' — the learner republishes and the
// milestone appears twice. Here the post, its event, and the state flip commit
// together or not at all (data-consistency: atomic state-write + event-publish).
//
// Returns the minted post_id. The body is read INSIDE the lock, so no read-then-
// write race can publish a stale body.
func (r *MilestoneDraftStore) PublishDraft(ctx context.Context, tenantID, gcid, draftID string) (string, error) {
	if r == nil || r.tx == nil {
		return "", ErrNotImplemented
	}
	if strings.TrimSpace(draftID) == "" {
		return "", errors.New("pg.MilestoneDraftStore.PublishDraft: draft_id required")
	}
	if strings.TrimSpace(gcid) == "" {
		return "", errors.New("pg.MilestoneDraftStore.PublishDraft: gcid required")
	}

	postID := post.NewUUIDv7()
	err := milestoneTenantTx(ctx, r.tx, tenantID, func(ctx context.Context, q Querier) error {
		body, meta, err := r.lockPendingDraft(ctx, q, tenantID, gcid, draftID)
		if err != nil {
			return err
		}
		if err := r.writePostAndEvent(ctx, q, publishTarget{
			PostID:   postID,
			TenantID: tenantID,
			GCID:     gcid,
			DraftID:  draftID,
			Body:     body,
			Metadata: meta,
		}); err != nil {
			return err
		}
		tag, err := q.Exec(ctx, sqlDraftMarkPublished, tenantID, draftID, gcid)
		if err != nil {
			return fmt.Errorf("pg: mark draft published: %w", err)
		}
		if tag.RowsAffected == 0 {
			// Unreachable behind the lock; if it ever fires, the tx rolls back
			// rather than leaving a post whose draft still reads 'pending'.
			return subscribers.ErrDraftNotPending
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("pg.MilestoneDraftStore.PublishDraft: %w", err)
	}
	return postID, nil
}

// lockPendingDraft takes the FOR UPDATE row lock and returns the draft body +
// metadata. No row → ErrDraftNotPending (never a fabricated empty body).
func (r *MilestoneDraftStore) lockPendingDraft(ctx context.Context, q Querier, tenantID, gcid, draftID string) (string, map[string]interface{}, error) {
	rows, err := q.Query(ctx, sqlDraftLockForPublish, tenantID, draftID, gcid)
	if err != nil {
		return "", nil, fmt.Errorf("pg: lock pending draft: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			// A failed read is OUR fault — never report it as "not pending",
			// which the adapter would turn into a 404 the learner cannot act on.
			return "", nil, fmt.Errorf("pg: lock pending draft: %w", err)
		}
		return "", nil, subscribers.ErrDraftNotPending
	}
	var (
		body    string
		metaRaw []byte
	)
	if err := rows.Scan(&body, &metaRaw); err != nil {
		return "", nil, fmt.Errorf("pg: scan pending draft: %w", err)
	}
	meta := map[string]interface{}{}
	if len(metaRaw) > 0 {
		if err := json.Unmarshal(metaRaw, &meta); err != nil {
			return "", nil, fmt.Errorf("pg: unmarshal draft metadata: %w", err)
		}
	}
	return body, meta, rows.Err()
}

// publishTarget bundles what writePostAndEvent needs to emit one published post.
type publishTarget struct {
	PostID   string
	TenantID string
	GCID     string
	DraftID  string
	Body     string
	Metadata map[string]interface{}
}

// writePostAndEvent upserts the Post and writes its outbox row on the caller's
// Querier (i.e. inside the caller's transaction).
func (r *MilestoneDraftStore) writePostAndEvent(ctx context.Context, q Querier, t publishTarget) error {
	now := time.Now().UTC()
	// Milestone shares are tenant-visible: the C+ social graph is intra-tenant,
	// and a learner publishing to their cohort has not consented to the public
	// web. IMDA D1 (internal audit trail) accordingly, not D2.
	const visibility = string(post.VisibilityTenant)

	payload := map[string]any{
		"post_id":              t.PostID,
		"tenant_id":            t.TenantID,
		"author_gcid":          t.GCID,
		"visibility":           visibility,
		"atom_id":              "",
		"body":                 t.Body,
		"created_at":           now.Format(time.RFC3339Nano),
		"occurred_at":          now.Format(time.RFC3339Nano),
		"chora_imda_dimension": "accountability",
		"imda_lifecycle_stage": "runtime",
	}
	if len(t.Metadata) > 0 {
		payload["milestone_metadata"] = t.Metadata
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("pg: marshal post payload: %w", err)
	}

	if _, err := q.Exec(ctx, SQLUpsertPost,
		t.PostID, t.TenantID, t.GCID, t.Body,
		"",         // atom_id (NULLIF '' → NULL)
		[]string{}, // tags
		visibility, // ::post_visibility
		now, now,   // posted_at, updated_at
		nullTime(nil), // deleted_at
	); err != nil {
		return fmt.Errorf("pg: upsert post: %w", err)
	}

	if r.outbox == nil {
		return errors.New("pg: nil outbox writer")
	}
	// event_id is a freshly minted UUIDv7, NOT a sentinel: the dispatcher copies
	// this map's event_id onto the published envelope verbatim, and consumers
	// key idempotency on it against a UUID column. CHO-2225 shipped an
	// `evt-<unixnano>` here from a silent default and NACKed 100% of the lane.
	//
	// idempotency_key is DRAFT-scoped, not post-scoped: the outbox insert is
	// ON CONFLICT (idempotency_key) DO NOTHING, so even if the pending-status
	// guard were ever bypassed, one draft can only ever emit one event.
	return r.outbox.WriteOutboxRow(ctx, q, OutboxRow{
		ID:            t.PostID,
		TenantID:      t.TenantID,
		GCID:          t.GCID,
		AggregateType: "post",
		AggregateID:   t.PostID,
		EventType:     "post.created.v1",
		Topic:         "chora.sharing.post.created.v1",
		Payload:       payloadBytes,
		Envelope: map[string]string{
			"event_id":    post.NewUUIDv7(),
			"tenant_id":   t.TenantID,
			"gcid":        t.GCID,
			"occurred_at": now.Format(time.RFC3339Nano),
		},
		IdempotencyKey: "post_draft_publish:" + t.DraftID,
		OccurredAt:     now,
	})
}

// Discard soft-deletes the owner's draft (status='discarded' + deleted_at=now()).
// A 0-row update fails LOUD: reporting success for a discard that matched
// nothing would tell the learner their draft is gone while it still queues.
func (r *MilestoneDraftStore) Discard(ctx context.Context, tenantID, gcid, draftID string) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if strings.TrimSpace(draftID) == "" {
		return errors.New("pg.MilestoneDraftStore.Discard: draft_id required")
	}
	if strings.TrimSpace(gcid) == "" {
		return errors.New("pg.MilestoneDraftStore.Discard: gcid required")
	}
	err := milestoneTenantTx(ctx, r.tx, tenantID, func(ctx context.Context, q Querier) error {
		tag, err := q.Exec(ctx, sqlDraftDiscard, tenantID, draftID, gcid)
		if err != nil {
			return fmt.Errorf("pg: discard draft: %w", err)
		}
		if tag.RowsAffected == 0 {
			return subscribers.ErrDraftNotPending
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("pg.MilestoneDraftStore.Discard: %w", err)
	}
	return nil
}

// scanDraft consumes a 9-column row (see sqlDraftListPending / sqlDraftGet).
func scanDraft(scan func(...any) error) (subscribers.Draft, error) {
	var (
		d       subscribers.Draft
		metaRaw []byte
		created time.Time
	)
	if err := scan(
		&d.DraftID, &d.TenantID, &d.AuthorGCID, &d.ComposedFromTopic, &d.ComposedFromEventID,
		&d.Body, &metaRaw, &d.Status, &created,
	); err != nil {
		return subscribers.Draft{}, fmt.Errorf("pg: scan draft: %w", err)
	}
	d.CreatedAt = created.UTC()
	if len(metaRaw) > 0 {
		m := map[string]interface{}{}
		if err := json.Unmarshal(metaRaw, &m); err != nil {
			return subscribers.Draft{}, fmt.Errorf("pg: unmarshal draft metadata: %w", err)
		}
		d.Metadata = m
	}
	return d, nil
}

func marshalMetadata(m map[string]interface{}) ([]byte, error) {
	if m == nil {
		return []byte("{}"), nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal metadata: %w", err)
	}
	return b, nil
}

// -----------------------------------------------------------------------------
// MilestonePreferenceStore — user_preferences (natural toggle; no soft-delete)
// -----------------------------------------------------------------------------

// MilestonePreferenceStore is the pg-backed subscribers.PreferenceStore.
type MilestonePreferenceStore struct{ tx TxRunner }

// NewMilestonePreferenceStore constructs a MilestonePreferenceStore.
func NewMilestonePreferenceStore(tx TxRunner) *MilestonePreferenceStore {
	return &MilestonePreferenceStore{tx: tx}
}

var _ subscribers.PreferenceStore = (*MilestonePreferenceStore)(nil)

// Column reconciles the pre-existing out-of-band table (migration 0040):
// `familiar_milestone_share` of enum type familiar_milestone_share_pref
// (values auto|draft|suppress); natural upsert key (tenant_id, gcid).
const sqlPreferenceGet = `
SELECT familiar_milestone_share FROM user_preferences
WHERE tenant_id = $1 AND gcid = $2`

const sqlPreferenceUpsert = `
INSERT INTO user_preferences (tenant_id, gcid, familiar_milestone_share)
VALUES ($1, $2, $3::familiar_milestone_share_pref)
ON CONFLICT (tenant_id, gcid) DO UPDATE
    SET familiar_milestone_share = EXCLUDED.familiar_milestone_share,
        updated_at = now()`

// Get returns the stored policy for (tenant, gcid); ok=false when no row.
func (r *MilestonePreferenceStore) Get(ctx context.Context, tenantID, gcid string) (subscribers.Policy, bool, error) {
	if r == nil || r.tx == nil {
		return "", false, ErrNotImplemented
	}
	var (
		raw string
		ok  bool
	)
	err := milestoneTenantTx(ctx, r.tx, tenantID, func(ctx context.Context, q Querier) error {
		rows, err := q.Query(ctx, sqlPreferenceGet, tenantID, gcid)
		if err != nil {
			return fmt.Errorf("pg: get preference: %w", err)
		}
		defer rows.Close()
		if !rows.Next() {
			return rows.Err()
		}
		if err := rows.Scan(&raw); err != nil {
			return fmt.Errorf("pg: scan preference: %w", err)
		}
		ok = true
		return rows.Err()
	})
	if err != nil {
		return "", false, fmt.Errorf("pg.MilestonePreferenceStore.Get: %w", err)
	}
	if !ok {
		return "", false, nil
	}
	// Validate against the canonical policy set — a value outside the CHECK
	// constraint means a corrupt row, fail loud rather than route on garbage.
	pol, err := subscribers.ParsePolicy(raw)
	if err != nil {
		return "", false, fmt.Errorf("pg.MilestonePreferenceStore.Get: %w", err)
	}
	return pol, true, nil
}

// Upsert persists the policy for (tenant, gcid).
func (r *MilestonePreferenceStore) Upsert(ctx context.Context, tenantID, gcid string, p subscribers.Policy) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	pol, err := subscribers.ParsePolicy(string(p))
	if err != nil {
		return fmt.Errorf("pg.MilestonePreferenceStore.Upsert: %w", err)
	}
	err = milestoneTenantTx(ctx, r.tx, tenantID, func(ctx context.Context, q Querier) error {
		if _, err := q.Exec(ctx, sqlPreferenceUpsert, tenantID, gcid, string(pol)); err != nil {
			return fmt.Errorf("pg: upsert preference: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("pg.MilestonePreferenceStore.Upsert: %w", err)
	}
	return nil
}

// -----------------------------------------------------------------------------
// MilestoneIdempotencyStore — subscriber_idempotency (tenant read FROM ctx)
// -----------------------------------------------------------------------------

// MilestoneIdempotencyStore is the pg-backed subscribers.IdempotencyStore. The
// Seen/Mark ports carry no tenant argument (shared across 8 subscribers), so the
// tenant is read from ctx and the read/write fails LOUD when it is absent.
//
// CHO-2263 split the old single committing Claim (INSERT ... RETURNING in its
// OWN tx, run BEFORE the handler's work) into a READ-ONLY peek (Seen) and a
// post-work commit (Mark). The handler marks ONLY after its side effect lands,
// so a transient failure NACKs unmarked and the redelivery re-processes rather
// than being silently ACK-dropped.
type MilestoneIdempotencyStore struct{ tx TxRunner }

// NewMilestoneIdempotencyStore constructs a MilestoneIdempotencyStore.
func NewMilestoneIdempotencyStore(tx TxRunner) *MilestoneIdempotencyStore {
	return &MilestoneIdempotencyStore{tx: tx}
}

var _ subscribers.IdempotencyStore = (*MilestoneIdempotencyStore)(nil)

// Column names reconcile the pre-existing out-of-band table (migration 0041):
// subscriber_handler / source_event_id, dedup keyed on the legacy composite PK
// (event_id is a globally-unique UUIDv7, so (handler, event) is a sound global
// dedup key); tenant_id is the RLS scoping column, so both statements run under
// rls.ApplySession and never carry the tenant in the WHERE clause.
const (
	// sqlIdempotencySeen is the READ-ONLY peek — RLS scopes it to the ctx tenant.
	sqlIdempotencySeen = `
SELECT 1 FROM subscriber_idempotency
WHERE subscriber_handler = $1 AND source_event_id = $2`

	// sqlIdempotencyMark is the idempotent commit — a repeat Mark is a no-op.
	sqlIdempotencyMark = `
INSERT INTO subscriber_idempotency (tenant_id, subscriber_handler, source_event_id)
VALUES ($1, $2, $3)
ON CONFLICT (subscriber_handler, source_event_id) DO NOTHING`
)

// idempotencyTenant reads the RLS tenant from ctx and validates the key, failing
// LOUD when either is absent — never a tenant-less / fail-open dedup read or
// write. The milestone Cloud PULL handler stamps tracing.WithTenantID(ctx,
// env.TenantID) before dispatch; a missing tenant NACKs + retries rather than
// being mis-recorded across tenants.
func (r *MilestoneIdempotencyStore) idempotencyTenant(ctx context.Context, op, handler, eventID string) (string, error) {
	tenantID := strings.TrimSpace(tracing.TenantIDFromContext(ctx))
	if tenantID == "" {
		return "", fmt.Errorf("pg.MilestoneIdempotencyStore.%s: no tenant on ctx (rls would fail open)", op)
	}
	if strings.TrimSpace(handler) == "" || strings.TrimSpace(eventID) == "" {
		return "", fmt.Errorf("pg.MilestoneIdempotencyStore.%s: handler + event_id required", op)
	}
	return tenantID, nil
}

// Seen reports whether (handler, event) is already recorded for the ctx tenant.
// It NEVER writes (CHO-2263). A genuine backing-store error propagates — it is
// NEVER coerced into "already seen" (which would silently drop the event) or
// "unseen" (which would double-process it).
func (r *MilestoneIdempotencyStore) Seen(ctx context.Context, handler, eventID string) (bool, error) {
	if r == nil || r.tx == nil {
		return false, ErrNotImplemented
	}
	tenantID, err := r.idempotencyTenant(ctx, "Seen", handler, eventID)
	if err != nil {
		return false, err
	}
	seen := false
	err = milestoneTenantTx(ctx, r.tx, tenantID, func(ctx context.Context, q Querier) error {
		rows, err := q.Query(ctx, sqlIdempotencySeen, handler, eventID)
		if err != nil {
			return fmt.Errorf("pg: seen idempotency: %w", err)
		}
		defer rows.Close()
		if rows.Next() {
			seen = true
		}
		return rows.Err()
	})
	if err != nil {
		return false, fmt.Errorf("pg.MilestoneIdempotencyStore.Seen: %w", err)
	}
	return seen, nil
}

// Mark durably records (handler, event) for the ctx tenant. Idempotent
// (ON CONFLICT DO NOTHING), so a concurrent double-process — or a redelivery
// after a marked-then-NACKed commit — collapses to one row. It MUST be called
// ONLY after the side effect has landed.
func (r *MilestoneIdempotencyStore) Mark(ctx context.Context, handler, eventID string) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	tenantID, err := r.idempotencyTenant(ctx, "Mark", handler, eventID)
	if err != nil {
		return err
	}
	err = milestoneTenantTx(ctx, r.tx, tenantID, func(ctx context.Context, q Querier) error {
		if _, err := q.Exec(ctx, sqlIdempotencyMark, tenantID, handler, eventID); err != nil {
			return fmt.Errorf("pg: mark idempotency: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("pg.MilestoneIdempotencyStore.Mark: %w", err)
	}
	return nil
}

// -----------------------------------------------------------------------------
// MilestonePostPublisher — posts + chora.sharing.post.created.v1 (atomic outbox)
// -----------------------------------------------------------------------------

// MilestonePostPublisher is the pg-backed subscribers.PostPublisher. Under
// PolicyAuto it persists a durable Post AND writes a chora.sharing.post.created.v1
// outbox row in the SAME transaction (mirrors ShareRepo.SaveShare —
// atomic state-write + event-publish; the wired sharingoutbox.Dispatcher drains
// the row to Cloud Pub/Sub). Replaces the in-memory slice that dropped every
// auto-posted milestone on pod death.
type MilestonePostPublisher struct {
	tx     TxRunner
	outbox OutboxWriter
}

// NewMilestonePostPublisher constructs a MilestonePostPublisher using the
// canonical DefaultOutboxWriter (same outbox spine as ShareRepo).
func NewMilestonePostPublisher(tx TxRunner) *MilestonePostPublisher {
	return &MilestonePostPublisher{tx: tx, outbox: NewDefaultOutboxWriter()}
}

var _ subscribers.PostPublisher = (*MilestonePostPublisher)(nil)

// Publish persists the Post + emits chora.sharing.post.created.v1 atomically.
func (r *MilestonePostPublisher) Publish(ctx context.Context, rec subscribers.PostRecord) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if strings.TrimSpace(rec.TenantID) == "" {
		return errors.New("pg.MilestonePostPublisher.Publish: tenant_id required")
	}
	if strings.TrimSpace(rec.PostID) == "" {
		return errors.New("pg.MilestonePostPublisher.Publish: post_id required")
	}
	visibility := strings.TrimSpace(rec.Visibility)
	if visibility == "" {
		visibility = "tenant"
	}
	now := time.Now().UTC()
	imdaDim := "accountability" // D1 internal audit; public shares would be D2
	if visibility == "public" {
		imdaDim = "transparency"
	}

	payload := map[string]any{
		"post_id":              rec.PostID,
		"tenant_id":            rec.TenantID,
		"author_gcid":          rec.AuthorGCID,
		"visibility":           visibility,
		"atom_id":              "",
		"body":                 rec.Body,
		"created_at":           now.Format(time.RFC3339Nano),
		"occurred_at":          now.Format(time.RFC3339Nano),
		"chora_imda_dimension": imdaDim,
		"imda_lifecycle_stage": "runtime",
	}
	if len(rec.Metadata) > 0 {
		payload["milestone_metadata"] = rec.Metadata
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("pg.MilestonePostPublisher.Publish: marshal payload: %w", err)
	}

	err = milestoneTenantTx(ctx, r.tx, rec.TenantID, func(ctx context.Context, q Querier) error {
		// 1) Persist the Post durably (idempotent UPSERT on post_id).
		if _, err := q.Exec(ctx, SQLUpsertPost,
			rec.PostID, rec.TenantID, rec.AuthorGCID, rec.Body,
			"",         // atom_id (NULLIF '' → NULL)
			[]string{}, // tags
			visibility, // ::post_visibility
			now, now,   // posted_at, updated_at
			nullTime(nil), // deleted_at
		); err != nil {
			return fmt.Errorf("pg: upsert post: %w", err)
		}
		// 2) Emit chora.sharing.post.created.v1 in the SAME tx (no orphan event).
		if r.outbox == nil {
			return errors.New("pg: nil outbox writer")
		}
		return r.outbox.WriteOutboxRow(ctx, q, OutboxRow{
			ID:            rec.PostID,
			TenantID:      rec.TenantID,
			GCID:          rec.AuthorGCID,
			AggregateType: "post",
			AggregateID:   rec.PostID,
			EventType:     "post.created.v1",
			Topic:         "chora.sharing.post.created.v1",
			Payload:       payloadBytes,
			Envelope: map[string]string{
				"event_id":    rec.PostID,
				"tenant_id":   rec.TenantID,
				"gcid":        rec.AuthorGCID,
				"occurred_at": now.Format(time.RFC3339Nano),
			},
			IdempotencyKey: rec.PostID,
			OccurredAt:     now,
		})
	})
	if err != nil {
		return fmt.Errorf("pg.MilestonePostPublisher.Publish: %w", err)
	}
	return nil
}
