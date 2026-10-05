// milestone_share_test.go — unit tests for the CHO-2203 durable Familiar-
// milestone-lane pg adapters (DraftStore / PreferenceStore / IdempotencyStore /
// PostPublisher). Stub-querier style (harness_test.go): the SQL surface +
// rls.ApplySession ordering are asserted without a live DB; live RLS isolation
// is verified separately against the deployed chora_sharing via the
// migrate-then-boot lane.
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-common/tracing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/adapter/subscribers"
)

// draftRowScan populates the 9-column post_drafts scan surface for stub rows.
func draftRowScan(topic string) func(dest ...any) error {
	return func(dest ...any) error {
		set := func(i int, v string) {
			if p, ok := dest[i].(*string); ok {
				*p = v
			}
		}
		set(0, draftID)
		set(1, tenantID)
		set(2, authorGCID)
		set(3, topic)
		set(4, eventID)
		set(5, "Ember reached Fledgling!")
		if p, ok := dest[6].(*[]byte); ok {
			*p = []byte(`{"familiar_id":"fam-1"}`)
		}
		set(7, "pending")
		if p, ok := dest[8].(*time.Time); ok {
			*p = time.Now().UTC()
		}
		return nil
	}
}

const (
	draftID = "01970000-0000-7000-a000-0000000000d1"
	eventID = "01970000-0000-7000-a000-0000000000e1"
)

// -----------------------------------------------------------------------------
// Compile-time port conformance — the whole point of CHO-2203 is that these pg
// adapters ARE the subscribers ports (not in-memory doubles).
// -----------------------------------------------------------------------------

var (
	_ subscribers.DraftStore       = (*pg.MilestoneDraftStore)(nil)
	_ subscribers.PreferenceStore  = (*pg.MilestonePreferenceStore)(nil)
	_ subscribers.IdempotencyStore = (*pg.MilestoneIdempotencyStore)(nil)
	_ subscribers.PostPublisher    = (*pg.MilestonePostPublisher)(nil)
)

// -----------------------------------------------------------------------------
// Nil-TxRunner → ErrNotImplemented (never a silent success)
// -----------------------------------------------------------------------------

func TestMilestoneAdapters_NilTxRunner_ReturnErrNotImplemented(t *testing.T) {
	t.Parallel()
	ctx := tracing.WithTenantID(context.Background(), tenantID)

	if _, err := pg.NewMilestoneDraftStore(nil).Insert(ctx, subscribers.Draft{TenantID: tenantID}); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("Draft.Insert: expected ErrNotImplemented; got %v", err)
	}
	if _, _, err := pg.NewMilestonePreferenceStore(nil).Get(ctx, tenantID, authorGCID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("Preference.Get: expected ErrNotImplemented; got %v", err)
	}
	if _, err := pg.NewMilestoneIdempotencyStore(nil).Seen(ctx, "h", eventID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("Idempotency.Seen: expected ErrNotImplemented; got %v", err)
	}
	if err := pg.NewMilestoneIdempotencyStore(nil).Mark(ctx, "h", eventID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("Idempotency.Mark: expected ErrNotImplemented; got %v", err)
	}
	if err := pg.NewMilestonePostPublisher(nil).Publish(ctx, subscribers.PostRecord{TenantID: tenantID}); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("PostPublisher.Publish: expected ErrNotImplemented; got %v", err)
	}
}

// -----------------------------------------------------------------------------
// DraftStore
// -----------------------------------------------------------------------------

func TestMilestoneDraftStore_Insert_AppliesRLSThenUpsertReturning_CreatedTrue(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		// RETURNING draft_id yields one row → created=true.
		queryFn: func(sql string, _ ...any) (pg.Rows, error) {
			if strings.Contains(sql, "INSERT INTO post_drafts") {
				return &stubRows{scans: []func(...any) error{func(dest ...any) error {
					if p, ok := dest[0].(*string); ok {
						*p = draftID
					}
					return nil
				}}}, nil
			}
			return &stubRows{}, nil
		},
	}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})

	created, err := r.Insert(context.Background(), subscribers.Draft{
		DraftID:             draftID,
		TenantID:            tenantID,
		AuthorGCID:          authorGCID,
		ComposedFromTopic:   "chora.consumption.familiar.stage_up.v1",
		ComposedFromEventID: eventID,
		Body:                "Ember reached Fledgling!",
		Status:              "pending",
	})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if !created {
		t.Fatalf("Insert: expected created=true on a fresh RETURNING row")
	}
	if len(q.sqls) < 2 || !strings.Contains(q.sqls[0], "SET LOCAL chora.tenant_id") {
		t.Fatalf("first SQL must be SET LOCAL chora.tenant_id; got %#v", q.sqls)
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "INSERT INTO post_drafts") ||
		!strings.Contains(last, "ON CONFLICT") || !strings.Contains(last, "DO NOTHING") ||
		!strings.Contains(last, "RETURNING") {
		t.Fatalf("expected idempotent INSERT ... ON CONFLICT DO NOTHING RETURNING; got %q", last)
	}
}

func TestMilestoneDraftStore_Insert_ConflictNoRow_CreatedFalse(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{} // default Query → empty rows → Next()=false, Err()=nil → conflict
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})

	created, err := r.Insert(context.Background(), subscribers.Draft{
		DraftID: draftID, TenantID: tenantID, AuthorGCID: authorGCID,
		ComposedFromTopic: "t", ComposedFromEventID: eventID, Body: "b", Status: "pending",
	})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if created {
		t.Fatalf("Insert: expected created=false when ON CONFLICT fired (no RETURNING row)")
	}
}

func TestMilestoneDraftStore_ListPending_FiltersSoftDeleteAndStatus(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})

	if _, err := r.ListPending(context.Background(), tenantID, authorGCID); err != nil {
		t.Fatalf("ListPending: %v", err)
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "FROM post_drafts") || !strings.Contains(last, "deleted_at IS NULL") ||
		!strings.Contains(last, "status = 'pending'") {
		t.Fatalf("ListPending must filter deleted_at IS NULL AND status='pending'; got %q", last)
	}
}

func TestMilestoneDraftStore_Insert_MarshalsMetadataJSON(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})

	_, err := r.Insert(context.Background(), subscribers.Draft{
		DraftID: draftID, TenantID: tenantID, AuthorGCID: authorGCID,
		ComposedFromTopic: "t", ComposedFromEventID: eventID, Body: "b", Status: "pending",
		Metadata: map[string]interface{}{"familiar_id": "fam-1", "stage_to_name": "Fledgling"},
	})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	// The metadata map must be bound as a JSON arg (position 7 in sqlDraftInsert).
	if !argsContain(q.args, `{"familiar_id":"fam-1","stage_to_name":"Fledgling"}`) {
		t.Fatalf("Insert must JSON-marshal metadata into the bound args; got %#v", q.args)
	}
}

func TestMilestoneDraftStore_ListPending_ScansRows(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(_ string, _ ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(...any) error{
				draftRowScan("chora.consumption.familiar.stage_up.v1"),
				draftRowScan("chora.consumption.familiar.hatched.v1"),
			}}, nil
		},
	}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})

	got, err := r.ListPending(context.Background(), tenantID, authorGCID)
	if err != nil {
		t.Fatalf("ListPending: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("ListPending: expected 2 drafts; got %d", len(got))
	}
}

// -----------------------------------------------------------------------------
// PreferenceStore
// -----------------------------------------------------------------------------

func TestMilestonePreferenceStore_Get_NoRow_ReturnsNotOK(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{} // default Query → empty rows → no preference row
	r := pg.NewMilestonePreferenceStore(&stubTxRunner{q: q})

	_, ok, err := r.Get(context.Background(), tenantID, authorGCID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok {
		t.Fatalf("Get: expected ok=false when no preference row exists")
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL chora.tenant_id") {
		t.Fatalf("first SQL must be SET LOCAL; got %#v", q.sqls)
	}
}

func TestMilestonePreferenceStore_Get_Row_ReturnsPolicy(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(_ string, _ ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(...any) error{func(dest ...any) error {
				if p, ok := dest[0].(*string); ok {
					*p = "suppress"
				}
				return nil
			}}}, nil
		},
	}
	r := pg.NewMilestonePreferenceStore(&stubTxRunner{q: q})

	pol, ok, err := r.Get(context.Background(), tenantID, authorGCID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || pol != subscribers.PolicySuppress {
		t.Fatalf("Get: expected (suppress, true); got (%q, %v)", pol, ok)
	}
}

func TestMilestonePreferenceStore_Upsert_OnConflictDoUpdate(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestonePreferenceStore(&stubTxRunner{q: q})

	if err := r.Upsert(context.Background(), tenantID, authorGCID, subscribers.PolicyAuto); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "INSERT INTO user_preferences") ||
		!strings.Contains(last, "ON CONFLICT (tenant_id, gcid) DO UPDATE") {
		t.Fatalf("Upsert must be INSERT ... ON CONFLICT (tenant_id, gcid) DO UPDATE; got %q", last)
	}
}

// -----------------------------------------------------------------------------
// IdempotencyStore — tenant from ctx (fail-loud when absent)
// -----------------------------------------------------------------------------

// CHO-2263 split the committing Claim into a READ-ONLY Seen peek + a post-work
// Mark commit. Both fail LOUD without a tenant on ctx (never a tenant-less /
// fail-open dedup read or write).
func TestMilestoneIdempotencyStore_SeenAndMark_FailLoudWithoutTenantOnCtx(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestoneIdempotencyStore(&stubTxRunner{q: q})

	// bare context — no tenant stamped: neither the peek NOR the commit may fail
	// open (a tenant-less dedup read/write would leak across tenants).
	if _, err := r.Seen(context.Background(), "familiar_milestone:stage_up", eventID); err == nil {
		t.Fatalf("Seen: expected a loud error when no tenant on ctx")
	}
	if err := r.Mark(context.Background(), "familiar_milestone:stage_up", eventID); err == nil {
		t.Fatalf("Mark: expected a loud error when no tenant on ctx (must never write a tenant-less/cross-tenant dedup row)")
	}
}

// Seen is a READ-ONLY peek: SELECT, never INSERT. A peek that writes IS the
// committing-claim bug CHO-2263 replaced.
func TestMilestoneIdempotencyStore_Seen_NoRow_ReturnsFalse_ReadOnly(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{} // default Query → empty rows → not seen
	r := pg.NewMilestoneIdempotencyStore(&stubTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)

	seen, err := r.Seen(ctx, "familiar_milestone:stage_up", eventID)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if seen {
		t.Fatalf("Seen: expected false when no row exists")
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL chora.tenant_id") {
		t.Fatalf("first SQL must be SET LOCAL chora.tenant_id (RLS); got %#v", q.sqls)
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "SELECT") || !strings.Contains(last, "subscriber_idempotency") {
		t.Fatalf("Seen must SELECT from subscriber_idempotency; got %q", last)
	}
	if strings.Contains(last, "INSERT") {
		t.Fatalf("Seen must NOT write (no INSERT) — it is a read-only peek; got %q", last)
	}
}

func TestMilestoneIdempotencyStore_Seen_RowExists_ReturnsTrue(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, _ ...any) (pg.Rows, error) {
			if strings.Contains(sql, "SELECT") && strings.Contains(sql, "subscriber_idempotency") {
				return &stubRows{scans: []func(...any) error{func(_ ...any) error { return nil }}}, nil
			}
			return &stubRows{}, nil
		},
	}
	r := pg.NewMilestoneIdempotencyStore(&stubTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)

	seen, err := r.Seen(ctx, "familiar_milestone:stage_up", eventID)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if !seen {
		t.Fatalf("Seen: expected true on a redelivery (the row already exists)")
	}
}

// Mark is the idempotent commit — INSERT ... ON CONFLICT DO NOTHING (no
// RETURNING; a repeat Mark is a no-op), run under SET LOCAL (RLS).
func TestMilestoneIdempotencyStore_Mark_UpsertOnConflictDoNothing(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestoneIdempotencyStore(&stubTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)

	if err := r.Mark(ctx, "familiar_milestone:stage_up", eventID); err != nil {
		t.Fatalf("Mark: %v", err)
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL chora.tenant_id") {
		t.Fatalf("first SQL must be SET LOCAL chora.tenant_id (RLS); got %#v", q.sqls)
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "INSERT INTO subscriber_idempotency") ||
		!strings.Contains(last, "ON CONFLICT") || !strings.Contains(last, "DO NOTHING") {
		t.Fatalf("Mark must be INSERT ... ON CONFLICT DO NOTHING; got %q", last)
	}
}

// -----------------------------------------------------------------------------
// PostPublisher — durable post + atomic post.created.v1 outbox row
// -----------------------------------------------------------------------------

func TestMilestonePostPublisher_Publish_PersistsPostAndOutboxRowInOneTx(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestonePostPublisher(&stubTxRunner{q: q})

	err := r.Publish(context.Background(), subscribers.PostRecord{
		PostID:     "01970000-0000-7000-a000-0000000000c1",
		TenantID:   tenantID,
		AuthorGCID: authorGCID,
		Body:       "Ember reached Fledgling!",
		Visibility: "tenant",
		Metadata:   map[string]any{"familiar_id": "fam-1"},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL chora.tenant_id") {
		t.Fatalf("first SQL must be SET LOCAL; got %#v", q.sqls)
	}
	joined := strings.Join(q.sqls, " || ")
	if !strings.Contains(joined, "INSERT INTO posts") {
		t.Fatalf("Publish must persist the post durably (INSERT INTO posts); got %#v", q.sqls)
	}
	if !strings.Contains(joined, "sharing_outbox_events") {
		t.Fatalf("Publish must emit an outbox row atomically (sharing_outbox_events); got %#v", q.sqls)
	}
	// The post.created.v1 topic is a bound arg to sqlOutboxInsert, not SQL text.
	if !argsContain(q.args, "chora.sharing.post.created.v1") {
		t.Fatalf("Publish must emit chora.sharing.post.created.v1 (topic arg); got args %#v", q.args)
	}
}

// argsContain reports whether any recorded arg equals want (string or []byte,
// since JSONB payloads bind as []byte).
func argsContain(argSets [][]any, want string) bool {
	for _, set := range argSets {
		for _, a := range set {
			switch v := a.(type) {
			case string:
				if v == want {
					return true
				}
			case []byte:
				if string(v) == want {
					return true
				}
			}
		}
	}
	return false
}

// assert the tag helper is used, keeping the linter honest about rls import.
var _ = rls.CommandTag{}
