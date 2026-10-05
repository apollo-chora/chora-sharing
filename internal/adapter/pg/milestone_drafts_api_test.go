// milestone_drafts_api_test.go — the C+ drafts API's pg surface (CHO-2258).
//
// CHO-2203 built post_drafts + the four subscriber ports, but the OWNER-facing
// half was never written: ListPending/Get/MarkPublished/MarkDiscarded had zero
// non-test callers, so every Familiar milestone landed as a draft nobody could
// read. These tests drive the two operations the API needs, and they encode the
// two defects the pre-existing ports would have shipped:
//
//  1. MarkPublished/MarkDiscarded were tenant-scoped but NOT owner-scoped —
//     (tenant_id, draft_id) only. RLS scopes the TENANT, so any learner could
//     have published or discarded any OTHER learner's milestone draft into the
//     C+ feed. Every op here binds author_gcid.
//  2. Publishing is post-insert + outbox-emit + draft-status-flip. As three
//     separate transactions a crash between them republishes on retry (the
//     draft stays 'pending' while the post exists). PublishDraft is ONE tx.
package pg_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-common/tracing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/adapter/subscribers"
)

// sqlSetLocalTenant is the RLS session statement every tenant-scoped op must
// emit FIRST (milestone_share_test.go convention — the rls package exports no
// constant for it).
const sqlSetLocalTenant = "SET LOCAL chora.tenant_id"

// firstSQL returns the first recorded statement (or "" when none ran).
func firstSQL(q *stubQuerier) string {
	if len(q.sqls) == 0 {
		return ""
	}
	return q.sqls[0]
}

// findSQL returns the first recorded statement containing needle, or "".
func findSQL(q *stubQuerier, needle string) string {
	for _, s := range q.sqls {
		if strings.Contains(s, needle) {
			return s
		}
	}
	return ""
}

// argsFor returns the bound args recorded alongside the given statement.
func argsFor(q *stubQuerier, sql string) []any {
	for i, s := range q.sqls {
		if s == sql && i < len(q.args) {
			return q.args[i]
		}
	}
	return nil
}

// argsBoundTo reports whether the given statement bound want as an argument.
// (Distinct from milestone_share_test.go's argsContain, which scans EVERY
// statement's args — this one proves the value reached THAT statement.)
func argsBoundTo(q *stubQuerier, sql, want string) bool {
	for _, a := range argsFor(q, sql) {
		if s, ok := a.(string); ok && s == want {
			return true
		}
	}
	return false
}

// outboxEnvelopeJSON decodes the envelope arg bound to the outbox insert.
// WriteOutboxRow marshals the envelope map and binds it as a JSON string.
func outboxEnvelopeJSON(t *testing.T, q *stubQuerier) map[string]string {
	t.Helper()
	sql := findSQL(q, "INSERT INTO sharing_outbox_events")
	if sql == "" {
		t.Fatal("no outbox insert recorded")
	}
	for _, a := range argsFor(q, sql) {
		s, ok := a.(string)
		if !ok || !strings.HasPrefix(strings.TrimSpace(s), "{") {
			continue
		}
		env := map[string]string{}
		if err := json.Unmarshal([]byte(s), &env); err == nil && len(env) > 0 {
			return env
		}
	}
	t.Fatalf("outbox insert bound no JSON envelope; args=%v", argsFor(q, sql))
	return nil
}

// publishRowScan populates the 2-column (body, metadata) lock-row scan surface
// that PublishDraft's SELECT ... FOR UPDATE returns.
func publishRowScan() func(dest ...any) error {
	return func(dest ...any) error {
		if p, ok := dest[0].(*string); ok {
			*p = "Ember reached Fledgling!"
		}
		if p, ok := dest[1].(*[]byte); ok {
			*p = []byte(`{"familiar_id":"fam-1"}`)
		}
		return nil
	}
}

// publishQuerier returns a stubQuerier whose lock SELECT yields one pending row.
func publishQuerier() *stubQuerier {
	return &stubQuerier{
		queryFn: func(sql string, _ ...any) (pg.Rows, error) {
			if strings.Contains(sql, "FOR UPDATE") {
				return &stubRows{scans: []func(dest ...any) error{publishRowScan()}}, nil
			}
			return &stubRows{}, nil
		},
	}
}

// -----------------------------------------------------------------------------
// Nil-TxRunner → ErrNotImplemented (never a silent success)
// -----------------------------------------------------------------------------

func TestMilestoneDraftStore_PublishDraft_NilTxRunner_ErrNotImplemented(t *testing.T) {
	t.Parallel()
	ctx := tracing.WithTenantID(context.Background(), tenantID)

	if _, err := pg.NewMilestoneDraftStore(nil).PublishDraft(ctx, tenantID, authorGCID, draftID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("PublishDraft: expected ErrNotImplemented; got %v", err)
	}
	if err := pg.NewMilestoneDraftStore(nil).Discard(ctx, tenantID, authorGCID, draftID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("Discard: expected ErrNotImplemented; got %v", err)
	}
}

// -----------------------------------------------------------------------------
// PublishDraft — RLS first, owner-scoped pessimistic lock
// -----------------------------------------------------------------------------

func TestMilestoneDraftStore_PublishDraft_AppliesRLSBeforeTheLockSelect(t *testing.T) {
	t.Parallel()
	q := publishQuerier()
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})

	if _, err := r.PublishDraft(context.Background(), tenantID, authorGCID, draftID); err != nil {
		t.Fatalf("PublishDraft: %v", err)
	}
	if len(q.sqls) == 0 || !strings.Contains(q.sqls[0], sqlSetLocalTenant) {
		t.Fatalf("first statement must be the RLS SET LOCAL; got %q", firstSQL(q))
	}
}

// The defect this encodes: RLS scopes the TENANT, not the learner. Without
// author_gcid in the predicate, learner B publishes learner A's milestone.
func TestMilestoneDraftStore_PublishDraft_LockSelectIsOwnerScopedAndPendingOnly(t *testing.T) {
	t.Parallel()
	q := publishQuerier()
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})

	if _, err := r.PublishDraft(context.Background(), tenantID, authorGCID, draftID); err != nil {
		t.Fatalf("PublishDraft: %v", err)
	}
	sel := findSQL(q, "FOR UPDATE")
	if sel == "" {
		t.Fatal("PublishDraft must SELECT ... FOR UPDATE the draft (pessimistic lock)")
	}
	for _, want := range []string{"author_gcid", "status = 'pending'", "deleted_at IS NULL"} {
		if !strings.Contains(sel, want) {
			t.Errorf("lock SELECT must constrain %s; got:\n%s", want, sel)
		}
	}
	// The caller's gcid must actually be BOUND, not merely named in the SQL.
	if !argsBoundTo(q, sel, authorGCID) {
		t.Errorf("lock SELECT must bind the caller gcid %q; got args %v", authorGCID, argsFor(q, sel))
	}
}

// -----------------------------------------------------------------------------
// PublishDraft — post + outbox + status flip, all in ONE transaction
// -----------------------------------------------------------------------------

func TestMilestoneDraftStore_PublishDraft_UpsertsPostEmitsOutboxAndMarksPublished(t *testing.T) {
	t.Parallel()
	q := publishQuerier()
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})

	postID, err := r.PublishDraft(context.Background(), tenantID, authorGCID, draftID)
	if err != nil {
		t.Fatalf("PublishDraft: %v", err)
	}
	if _, perr := uuid.Parse(postID); perr != nil {
		t.Errorf("PublishDraft must return the minted post_id as a UUID; got %q", postID)
	}
	if findSQL(q, "INSERT INTO posts") == "" {
		t.Error("PublishDraft must persist the Post")
	}
	if findSQL(q, "INSERT INTO sharing_outbox_events") == "" {
		t.Error("PublishDraft must emit chora.sharing.post.created.v1 to the outbox")
	}
	upd := findSQL(q, "UPDATE post_drafts")
	if upd == "" {
		t.Fatal("PublishDraft must flip the draft to 'published'")
	}
	if !strings.Contains(upd, "status = 'published'") {
		t.Errorf("draft must be marked published; got:\n%s", upd)
	}
	// Publishing is NOT a deletion — a published draft is history.
	if strings.Contains(upd, "deleted_at = now()") {
		t.Errorf("PublishDraft must NOT soft-delete the draft; got:\n%s", upd)
	}
	// The status flip must be owner-scoped too, not just the lock.
	if !argsBoundTo(q, upd, authorGCID) {
		t.Errorf("status flip must bind the caller gcid; got args %v", argsFor(q, upd))
	}
}

// A draft that is not pending / not owned / already deleted yields no lock row.
// That must FAIL LOUD: returning nil would report a publish that never happened,
// and writing the post anyway would let learner B post learner A's milestone.
func TestMilestoneDraftStore_PublishDraft_NoPendingRow_FailsLoudAndWritesNothing(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(_ string, _ ...any) (pg.Rows, error) { return &stubRows{}, nil },
	}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})

	_, err := r.PublishDraft(context.Background(), tenantID, authorGCID, draftID)
	if !errors.Is(err, subscribers.ErrDraftNotPending) {
		t.Fatalf("expected ErrDraftNotPending; got %v", err)
	}
	if findSQL(q, "INSERT INTO posts") != "" {
		t.Error("no post may be written when the draft is not publishable")
	}
	if findSQL(q, "INSERT INTO sharing_outbox_events") != "" {
		t.Error("no event may be emitted when the draft is not publishable")
	}
}

// CHO-2225 guard. The dispatcher reads envelope["event_id"] back VERBATIM and
// the downstream subscriber keys idempotency on it against a UUID column: a
// non-UUID sentinel there is a 100%-NACK lane, and it 22P02s at the consumer,
// far from here. Assert the emitted envelope carries a parseable UUID.
func TestMilestoneDraftStore_PublishDraft_OutboxEnvelopeEventIDIsARealUUID(t *testing.T) {
	t.Parallel()
	q := publishQuerier()
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})

	if _, err := r.PublishDraft(context.Background(), tenantID, authorGCID, draftID); err != nil {
		t.Fatalf("PublishDraft: %v", err)
	}
	env := outboxEnvelopeJSON(t, q)
	id, ok := env["event_id"]
	if !ok || strings.TrimSpace(id) == "" {
		t.Fatalf("outbox envelope must carry event_id; got %v", env)
	}
	if _, err := uuid.Parse(id); err != nil {
		t.Fatalf("envelope event_id must be a real UUID (CHO-2225: an evt-<unixnano> sentinel 22P02s the consumer); got %q", id)
	}
	if env["tenant_id"] != tenantID {
		t.Errorf("envelope tenant_id = %q; want %q", env["tenant_id"], tenantID)
	}
	if env["gcid"] != authorGCID {
		t.Errorf("envelope gcid = %q; want %q", env["gcid"], authorGCID)
	}
}

// Double-publish must not double-emit even if the pending guard were bypassed:
// the outbox row is keyed on the draft, whose ON CONFLICT (idempotency_key)
// DO NOTHING collapses a replay.
func TestMilestoneDraftStore_PublishDraft_OutboxIdempotencyKeyIsDraftScoped(t *testing.T) {
	t.Parallel()
	q := publishQuerier()
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})

	if _, err := r.PublishDraft(context.Background(), tenantID, authorGCID, draftID); err != nil {
		t.Fatalf("PublishDraft: %v", err)
	}
	args := argsFor(q, findSQL(q, "INSERT INTO sharing_outbox_events"))
	var found bool
	for _, a := range args {
		if s, ok := a.(string); ok && strings.Contains(s, draftID) {
			found = true
		}
	}
	if !found {
		t.Errorf("outbox idempotency_key must be derived from the draft id %q; got args %v", draftID, args)
	}
}

// -----------------------------------------------------------------------------
// Discard — owner-scoped soft-delete
// -----------------------------------------------------------------------------

func TestMilestoneDraftStore_Discard_IsOwnerScopedSoftDelete(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})

	if err := r.Discard(context.Background(), tenantID, authorGCID, draftID); err != nil {
		t.Fatalf("Discard: %v", err)
	}
	if len(q.sqls) == 0 || !strings.Contains(q.sqls[0], sqlSetLocalTenant) {
		t.Fatalf("first statement must be the RLS SET LOCAL; got %q", firstSQL(q))
	}
	upd := findSQL(q, "UPDATE post_drafts")
	if upd == "" {
		t.Fatal("Discard must UPDATE post_drafts")
	}
	for _, want := range []string{"status = 'discarded'", "deleted_at = now()", "author_gcid", "deleted_at IS NULL"} {
		if !strings.Contains(upd, want) {
			t.Errorf("Discard must be an owner-scoped soft-delete constraining %s; got:\n%s", want, upd)
		}
	}
	if !argsBoundTo(q, upd, authorGCID) {
		t.Errorf("Discard must bind the caller gcid; got args %v", argsFor(q, upd))
	}
}

// A discard that matched nothing (wrong owner / already gone) must not report
// success — a 200 on a no-op tells the learner their draft is gone when it is not.
func TestMilestoneDraftStore_Discard_NoRowsAffected_FailsLoud(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		execTagFn: func(sql string) rls.CommandTag {
			if strings.Contains(sql, "UPDATE post_drafts") {
				return rls.CommandTag{RowsAffected: 0}
			}
			return rls.CommandTag{RowsAffected: 1}
		},
	}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})

	if err := r.Discard(context.Background(), tenantID, authorGCID, draftID); !errors.Is(err, subscribers.ErrDraftNotPending) {
		t.Fatalf("expected ErrDraftNotPending on a 0-row discard; got %v", err)
	}
}
