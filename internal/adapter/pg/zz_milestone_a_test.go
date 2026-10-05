// zz_milestone_a_test.go — "A"-lane coverage for milestone_share.go branches
// the existing suite leaves open: validation guard-clauses, exec/scan error
// propagation, metadata marshalling failures, ApplySession (SET LOCAL)
// failures, and the PostPublisher visibility defaults.
package pg_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/rls"

	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/adapter/subscribers"
)

// milestoneSq is a minimal valid Draft for store tests.
func milestoneSq() subscribers.Draft {
	return subscribers.Draft{
		DraftID:             draftID,
		TenantID:            tenantID,
		AuthorGCID:          authorGCID,
		ComposedFromTopic:   "chora.consumption.familiar.stage_up.v1",
		ComposedFromEventID: eventID,
		Body:                "Ember reached Fledgling!",
		Status:              "pending",
	}
}

// ------------------------------------------------------------------ Insert

func TestMilestoneDraftStore_Insert_EmptyTenant_Refuses(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})
	d := milestoneSq()
	d.TenantID = "  "
	if _, err := r.Insert(aCtx(), d); err == nil {
		t.Fatalf("expected an error for a blank tenant")
	}
	if len(q.sqls) != 0 {
		t.Fatalf("validation failure must not touch the DB; got %v", q.sqls)
	}
}

func TestMilestoneDraftStore_Insert_DefaultsStatusAndCreatedAt(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})
	d := milestoneSq()
	d.Status = ""   // default → pending
	d.CreatedAt = time.Time{} // zero → now
	if _, err := r.Insert(aCtx(), d); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	args := q.args[len(q.args)-1]
	if args[7] != "pending" {
		t.Fatalf("blank status must default to 'pending'; got %v", args[7])
	}
	if args[8] == nil {
		t.Fatalf("zero CreatedAt must be replaced by now()")
	}
	if ts, ok := args[8].(time.Time); !ok || ts.IsZero() {
		t.Fatalf("created_at arg must be a non-zero time; got %T %v", args[8], args[8])
	}
}

func TestMilestoneDraftStore_Insert_MetadataMarshalError_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})
	d := milestoneSq()
	d.Metadata = map[string]interface{}{"bad": func() {}} // json.Marshal fails
	if _, err := r.Insert(aCtx(), d); err == nil {
		t.Fatalf("expected the metadata marshal error to propagate")
	}
	if len(q.sqls) != 0 {
		t.Fatalf("marshal failure must not touch the DB; got %v", q.sqls)
	}
}

func TestMilestoneDraftStore_Insert_QueryError_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("db down") }
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})
	if _, err := r.Insert(aCtx(), milestoneSq()); err == nil {
		t.Fatalf("expected the query error to propagate")
	}
}

func TestMilestoneDraftStore_Insert_ConflictWithRowsErr_Propagates(t *testing.T) {
	t.Parallel()
	boom := errors.New("cursor read failed")
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		return &stubRows{err: boom}, nil // Next()=false AND Err()!=nil — NOT a clean conflict
	}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})
	if _, err := r.Insert(aCtx(), milestoneSq()); err == nil {
		t.Fatalf("a cursor error behind a missing RETURNING row must propagate, never read as a clean conflict")
	}
}

// -------------------------------------------------------------- ListPending

func TestMilestoneDraftStore_ListPending_NilTxRunner(t *testing.T) {
	t.Parallel()
	if _, err := pg.NewMilestoneDraftStore(nil).ListPending(aCtx(), tenantID, authorGCID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

func TestMilestoneDraftStore_ListPending_QueryError_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("db down") }
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})
	if _, err := r.ListPending(aCtx(), tenantID, authorGCID); err == nil {
		t.Fatalf("expected the query error to propagate")
	}
}

func TestMilestoneDraftStore_ListPending_ScanError_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		return &stubRows{scans: []func(dest ...any) error{
			func(dest ...any) error { return errors.New("scan boom") },
		}}, nil
	}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})
	if _, err := r.ListPending(aCtx(), tenantID, authorGCID); err == nil {
		t.Fatalf("expected the scan error to propagate")
	}
}

func TestMilestoneDraftStore_ListPending_BadMetadataJSON_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		return &stubRows{scans: []func(dest ...any) error{
			func(dest ...any) error {
				zqSetString(dest, 0, draftID)
				zqSetString(dest, 1, tenantID)
				zqSetString(dest, 2, authorGCID)
				zqSetString(dest, 3, "topic")
				zqSetString(dest, 4, eventID)
				zqSetString(dest, 5, "body")
				zqSetBytes(dest, 6, []byte("{not-json"))
				zqSetString(dest, 7, "pending")
				zqSetTime(dest, 8, time.Now().UTC())
				return nil
			},
		}}, nil
	}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})
	if _, err := r.ListPending(aCtx(), tenantID, authorGCID); err == nil {
		t.Fatalf("unmarshallable draft metadata must fail loud")
	}
}

// ------------------------------------------------------------- PublishDraft

func TestMilestoneDraftStore_PublishDraft_EmptyDraftID(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})
	if _, err := r.PublishDraft(aCtx(), tenantID, authorGCID, "  "); err == nil {
		t.Fatalf("expected an error for a blank draft id")
	}
	if len(q.sqls) != 0 {
		t.Fatalf("validation failure must not touch the DB; got %v", q.sqls)
	}
}

func TestMilestoneDraftStore_PublishDraft_EmptyGCID(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})
	if _, err := r.PublishDraft(aCtx(), tenantID, "", draftID); err == nil {
		t.Fatalf("expected an error for a blank gcid")
	}
}

func TestMilestoneDraftStore_PublishDraft_LockQueryError_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		if strings.Contains(sql, "FOR UPDATE") {
			return nil, errors.New("db down")
		}
		return &stubRows{}, nil
	}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})
	if _, err := r.PublishDraft(aCtx(), tenantID, authorGCID, draftID); err == nil {
		t.Fatalf("expected the lock query error to propagate")
	}
}

func TestMilestoneDraftStore_PublishDraft_LockCursorError_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		if strings.Contains(sql, "FOR UPDATE") {
			return &stubRows{err: errors.New("cursor error")}, nil // Next()=false + Err — a failed read, NOT "not pending"
		}
		return &stubRows{}, nil
	}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})
	if _, err := r.PublishDraft(aCtx(), tenantID, authorGCID, draftID); err == nil {
		t.Fatalf("the failed lock READ must propagate, not be masked as ErrDraftNotPending")
	}
}

func TestMilestoneDraftStore_PublishDraft_LockScanError_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		if strings.Contains(sql, "FOR UPDATE") {
			return &stubRows{scans: []func(dest ...any) error{
				func(dest ...any) error { return errors.New("scan boom") },
			}}, nil
		}
		return &stubRows{}, nil
	}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})
	if _, err := r.PublishDraft(aCtx(), tenantID, authorGCID, draftID); err == nil {
		t.Fatalf("expected the lock scan error to propagate")
	}
}

func TestMilestoneDraftStore_PublishDraft_BadMetadataJSON_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		if strings.Contains(sql, "FOR UPDATE") {
			return &stubRows{scans: []func(dest ...any) error{
				func(dest ...any) error {
					zqSetString(dest, 0, "body text")
					zqSetBytes(dest, 1, []byte("{not-json"))
					return nil
				},
			}}, nil
		}
		return &stubRows{}, nil
	}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})
	if _, err := r.PublishDraft(aCtx(), tenantID, authorGCID, draftID); err == nil {
		t.Fatalf("unmarshallable lock metadata must fail loud")
	}
}

func TestMilestoneDraftStore_PublishDraft_MarkPublishedExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "SET status = 'published'", execErr: aErrExec}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		if strings.Contains(sql, "FOR UPDATE") {
			return &stubRows{scans: []func(dest ...any) error{
				func(dest ...any) error {
					zqSetString(dest, 0, "body text")
					zqSetBytes(dest, 1, []byte(`{"k":"v"}`))
					return nil
				},
			}}, nil
		}
		return &stubRows{}, nil
	}
	r := pg.NewMilestoneDraftStore(&aTxRunner{q: q})
	if _, err := r.PublishDraft(aCtx(), tenantID, authorGCID, draftID); err == nil {
		t.Fatalf("expected the mark-published exec error to propagate")
	}
}

func TestMilestoneDraftStore_PublishDraft_MarkPublishedZeroRows_ErrDraftNotPending(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		if strings.Contains(sql, "FOR UPDATE") {
			return &stubRows{scans: []func(dest ...any) error{
				func(dest ...any) error {
					zqSetString(dest, 0, "body text")
					zqSetBytes(dest, 1, []byte(`{"k":"v"}`))
					return nil
				},
			}}, nil
		}
		return &stubRows{}, nil
	}
	q.execTagFn = func(sql string) rls.CommandTag {
		if strings.Contains(sql, "SET status = 'published'") {
			return rls.CommandTag{RowsAffected: 0}
		}
		return rls.CommandTag{RowsAffected: 1}
	}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})
	_, err := r.PublishDraft(aCtx(), tenantID, authorGCID, draftID)
	if !errors.Is(err, subscribers.ErrDraftNotPending) {
		t.Fatalf("expected ErrDraftNotPending on a 0-row mark; got %v", err)
	}
}

func TestMilestoneDraftStore_PublishDraft_UpsertPostExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "INSERT INTO posts", execErr: aErrExec}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		if strings.Contains(sql, "FOR UPDATE") {
			return &stubRows{scans: []func(dest ...any) error{
				func(dest ...any) error {
					zqSetString(dest, 0, "body text")
					zqSetBytes(dest, 1, []byte(`{"k":"v"}`))
					return nil
				},
			}}, nil
		}
		return &stubRows{}, nil
	}
	r := pg.NewMilestoneDraftStore(&aTxRunner{q: q})
	if _, err := r.PublishDraft(aCtx(), tenantID, authorGCID, draftID); err == nil {
		t.Fatalf("expected the post upsert exec error to propagate")
	}
}

func TestMilestoneDraftStore_PublishDraft_OutboxExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "sharing_outbox_events", execErr: aErrExec}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		if strings.Contains(sql, "FOR UPDATE") {
			return &stubRows{scans: []func(dest ...any) error{
				func(dest ...any) error {
					zqSetString(dest, 0, "body text")
					zqSetBytes(dest, 1, []byte(`{"k":"v"}`))
					return nil
				},
			}}, nil
		}
		return &stubRows{}, nil
	}
	r := pg.NewMilestoneDraftStore(&aTxRunner{q: q})
	if _, err := r.PublishDraft(aCtx(), tenantID, authorGCID, draftID); err == nil {
		t.Fatalf("expected the outbox insert error to propagate")
	}
}

// ------------------------------------------------------------------ Discard

func TestMilestoneDraftStore_Discard_NilTxRunner(t *testing.T) {
	t.Parallel()
	if err := pg.NewMilestoneDraftStore(nil).Discard(aCtx(), tenantID, authorGCID, draftID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

func TestMilestoneDraftStore_Discard_EmptyDraftID(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})
	if err := r.Discard(aCtx(), tenantID, authorGCID, " "); err == nil {
		t.Fatalf("expected an error for a blank draft id")
	}
	if len(q.sqls) != 0 {
		t.Fatalf("validation failure must not touch the DB; got %v", q.sqls)
	}
}

func TestMilestoneDraftStore_Discard_EmptyGCID(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestoneDraftStore(&stubTxRunner{q: q})
	if err := r.Discard(aCtx(), tenantID, "", draftID); err == nil {
		t.Fatalf("expected an error for a blank gcid")
	}
	if len(q.sqls) != 0 {
		t.Fatalf("validation failure must not touch the DB; got %v", q.sqls)
	}
}

func TestMilestoneDraftStore_Discard_ExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "SET status = 'discarded'", execErr: aErrExec}
	r := pg.NewMilestoneDraftStore(&aTxRunner{q: q})
	if err := r.Discard(aCtx(), tenantID, authorGCID, draftID); err == nil {
		t.Fatalf("expected the discard exec error to propagate")
	}
}

// ------------------------------------------------- PreferenStore.Get / Upsert

func TestMilestonePreferenceStore_Get_QueryError_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("db down") }
	r := pg.NewMilestonePreferenceStore(&stubTxRunner{q: q})
	if _, _, err := r.Get(aCtx(), tenantID, authorGCID); err == nil {
		t.Fatalf("expected the query error to propagate")
	}
}

func TestMilestonePreferenceStore_Get_ScanError_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		return &stubRows{scans: []func(dest ...any) error{
			func(dest ...any) error { return errors.New("scan boom") },
		}}, nil
	}
	r := pg.NewMilestonePreferenceStore(&stubTxRunner{q: q})
	if _, _, err := r.Get(aCtx(), tenantID, authorGCID); err == nil {
		t.Fatalf("expected the scan error to propagate")
	}
}

func TestMilestonePreferenceStore_Get_InvalidPolicyValue_FailsLoud(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) {
		return &stubRows{scans: []func(dest ...any) error{
			func(dest ...any) error { zqSetString(dest, 0, "corrupt-value"); return nil },
		}}, nil
	}
	r := pg.NewMilestonePreferenceStore(&stubTxRunner{q: q})
	if _, _, err := r.Get(aCtx(), tenantID, authorGCID); err == nil {
		t.Fatalf("a value outside the CHECK constraint must fail loud")
	}
}

func TestMilestonePreferenceStore_Upsert_InvalidPolicy_Refuses(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestonePreferenceStore(&stubTxRunner{q: q})
	if err := r.Upsert(aCtx(), tenantID, authorGCID, subscribers.Policy("bogus")); err == nil {
		t.Fatalf("expected an error for an invalid policy")
	}
	if len(q.sqls) != 0 {
		t.Fatalf("invalid policy must not touch the DB; got %v", q.sqls)
	}
}

func TestMilestonePreferenceStore_Upsert_ExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "INSERT INTO user_preferences", execErr: aErrExec}
	r := pg.NewMilestonePreferenceStore(&aTxRunner{q: q})
	if err := r.Upsert(aCtx(), tenantID, authorGCID, subscribers.PolicyAuto); err == nil {
		t.Fatalf("expected the upsert exec error to propagate")
	}
}

func TestMilestonePreferenceStore_Upsert_NilTxRunner(t *testing.T) {
	t.Parallel()
	if err := pg.NewMilestonePreferenceStore(nil).Upsert(aCtx(), tenantID, authorGCID, subscribers.PolicyAuto); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented; got %v", err)
	}
}

// ------------------------------------------------------ IdempotencyStore

func TestMilestoneIdempotencyStore_Seen_EmptyHandler_FailsLoud(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestoneIdempotencyStore(&stubTxRunner{q: q})
	if _, err := r.Seen(aCtx(), "  ", eventID); err == nil {
		t.Fatalf("expected an error for a blank handler (tenant present)")
	}
	if len(q.sqls) != 0 {
		t.Fatalf("validation failure must not touch the DB; got %v", q.sqls)
	}
}

func TestMilestoneIdempotencyStore_Mark_EmptyEventID_FailsLoud(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestoneIdempotencyStore(&stubTxRunner{q: q})
	if err := r.Mark(aCtx(), "handler", "  "); err == nil {
		t.Fatalf("expected an error for a blank event id (tenant present)")
	}
}

func TestMilestoneIdempotencyStore_Seen_QueryError_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	q.queryFn = func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("db down") }
	r := pg.NewMilestoneIdempotencyStore(&stubTxRunner{q: q})
	if _, err := r.Seen(aCtx(), "handler", eventID); err == nil {
		t.Fatalf("expected the query error to propagate")
	}
}

func TestMilestoneIdempotencyStore_Mark_ExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "INSERT INTO subscriber_idempotency", execErr: aErrExec}
	r := pg.NewMilestoneIdempotencyStore(&aTxRunner{q: q})
	if err := r.Mark(aCtx(), "handler", eventID); err == nil {
		t.Fatalf("expected the mark exec error to propagate")
	}
}

func TestMilestoneIdempotencyStore_SeenAndMark_RLSFailure_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "SET LOCAL", execErr: aErrExec}
	r := pg.NewMilestoneIdempotencyStore(&aTxRunner{q: q})
	if _, err := r.Seen(aCtx(), "handler", eventID); err == nil {
		t.Fatalf("expected the SET LOCAL failure to propagate on Seen")
	}
	if err := r.Mark(aCtx(), "handler", eventID); err == nil {
		t.Fatalf("expected the SET LOCAL failure to propagate on Mark")
	}
}

// ---------------------------------------------------------- PostPublisher

func TestMilestonePostPublisher_Publish_EmptyTenant(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestonePostPublisher(&stubTxRunner{q: q})
	if err := r.Publish(aCtx(), subscribers.PostRecord{PostID: "p-1", TenantID: " "}); err == nil {
		t.Fatalf("expected an error for a blank tenant")
	}
}

func TestMilestonePostPublisher_Publish_EmptyPostID(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestonePostPublisher(&stubTxRunner{q: q})
	if err := r.Publish(aCtx(), subscribers.PostRecord{PostID: " ", TenantID: tenantID}); err == nil {
		t.Fatalf("expected an error for a blank post id")
	}
}

func TestMilestonePostPublisher_Publish_DefaultVisibilityTenant(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestonePostPublisher(&stubTxRunner{q: q})
	if err := r.Publish(aCtx(), subscribers.PostRecord{
		PostID: "p-1", TenantID: tenantID, AuthorGCID: authorGCID, Body: "b",
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !argsContain(q.args, "tenant") {
		t.Fatalf("blank visibility must default to tenant; args=%v", q.args)
	}
	if argsContain(q.args, "transparency") {
		t.Fatalf("default path must NOT carry the D2 transparency dimension")
	}
}

func TestMilestonePostPublisher_Publish_PublicVisibility_TransparencyDimension(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestonePostPublisher(&stubTxRunner{q: q})
	if err := r.Publish(aCtx(), subscribers.PostRecord{
		PostID: "p-1", TenantID: tenantID, AuthorGCID: authorGCID, Body: "b", Visibility: "public",
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// The D2 transparency dimension rides in the JSON payload of the outbox row.
	found := false
	for _, argSet := range q.args {
		for _, a := range argSet {
			if b, ok := a.([]byte); ok && strings.Contains(string(b), "transparency") {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("public visibility must carry the D2 transparency dimension; args=%v", q.args)
	}
}

func TestMilestonePostPublisher_Publish_UpsertPostError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "INSERT INTO posts", execErr: aErrExec}
	r := pg.NewMilestonePostPublisher(&aTxRunner{q: q})
	if err := r.Publish(aCtx(), subscribers.PostRecord{
		PostID: "p-1", TenantID: tenantID, AuthorGCID: authorGCID, Body: "b",
	}); err == nil {
		t.Fatalf("expected the post upsert error to propagate")
	}
}

func TestMilestonePostPublisher_Publish_OutboxError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "sharing_outbox_events", execErr: aErrExec}
	r := pg.NewMilestonePostPublisher(&aTxRunner{q: q})
	if err := r.Publish(aCtx(), subscribers.PostRecord{
		PostID: "p-1", TenantID: tenantID, AuthorGCID: authorGCID, Body: "b",
	}); err == nil {
		t.Fatalf("expected the outbox insert error to propagate")
	}
}

func TestMilestonePostPublisher_Publish_RLSFailure_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "SET LOCAL", execErr: aErrExec}
	r := pg.NewMilestonePostPublisher(&aTxRunner{q: q})
	if err := r.Publish(aCtx(), subscribers.PostRecord{
		PostID: "p-1", TenantID: tenantID, AuthorGCID: authorGCID, Body: "b",
	}); err == nil {
		t.Fatalf("expected the SET LOCAL failure to propagate")
	}
}

func TestMilestonePostPublisher_Publish_UnserializableMetadata_Propagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewMilestonePostPublisher(&stubTxRunner{q: q})
	// Metadata arrives from the caller here (unlike Draft metadata, which is
	// DB-scanned JSON): a non-JSON value must fail the payload marshal LOUD.
	if err := r.Publish(aCtx(), subscribers.PostRecord{
		PostID: "p-1", TenantID: tenantID, AuthorGCID: authorGCID, Body: "b",
		Metadata: map[string]interface{}{"bad": func() {}},
	}); err == nil {
		t.Fatalf("expected the payload marshal error to propagate")
	}
	if len(q.sqls) != 0 {
		t.Fatalf("marshal failure must not touch the DB; got %v", q.sqls)
	}
}