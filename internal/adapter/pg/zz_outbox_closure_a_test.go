// zz_outbox_closure_a_test.go — "A"-lane coverage for outbox_writer.go
// (DefaultOutboxWriter.WriteOutboxRow arg/error surface) and the
// closure_repository.go ApplySession-failure branch in run().
package pg_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
)

func aOutboxRow(gcid string) pg.OutboxRow {
	return pg.OutboxRow{
		ID:             "evt-1",
		TenantID:       tenantID,
		GCID:           gcid,
		AggregateType:  "post",
		AggregateID:    "post-1",
		EventType:      "post.created.v1",
		Topic:          "chora.sharing.post.created.v1",
		Payload:        []byte(`{"post_id":"post-1"}`),
		Envelope:       map[string]string{"event_id": "evt-1", "tenant_id": tenantID},
		IdempotencyKey: "ik-1",
		OccurredAt:     time.Now().UTC(),
	}
}

func TestOutboxWriter_WriteOutboxRow_WithGCID_BindsGCID(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	w := pg.NewDefaultOutboxWriter()
	if err := w.WriteOutboxRow(aCtx(), q, aOutboxRow("01970000-0000-7000-a000-0000000000aa")); err != nil {
		t.Fatalf("WriteOutboxRow: %v", err)
	}
	args := q.args[len(q.args)-1]
	if args[2] != "01970000-0000-7000-a000-0000000000aa" {
		t.Fatalf("gcid arg = %v", args[2])
	}
	// Column order per SQLInsertOutboxEvent: id, tenant_id, gcid,
	// aggregate_type, aggregate_id, event_type, topic, payload, envelope,
	// idempotency_key, occurred_at → the envelope JSON is arg index 8.
	env, ok := args[8].(string)
	if !ok || !strings.Contains(env, "evt-1") {
		t.Fatalf("envelope JSON arg malformed: %T %v", args[8], args[8])
	}
}

func TestOutboxWriter_WriteOutboxRow_EmptyGCID_BindsNil(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	w := pg.NewDefaultOutboxWriter()
	if err := w.WriteOutboxRow(aCtx(), q, aOutboxRow("")); err != nil {
		t.Fatalf("WriteOutboxRow: %v", err)
	}
	args := q.args[len(q.args)-1]
	if args[2] != nil {
		t.Fatalf("blank gcid must bind nil (nullable column); got %v", args[2])
	}
}

func TestOutboxWriter_WriteOutboxRow_ExecError_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "INSERT INTO sharing_outbox_events", execErr: aErrExec}
	w := pg.NewDefaultOutboxWriter()
	if err := w.WriteOutboxRow(aCtx(), q, aOutboxRow("")); err == nil {
		t.Fatalf("expected the insert error to propagate")
	}
}

// ------------------------------------------------ closure_repository run()

func TestClosureRepository_Pseudonymise_RLSFailure_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "SET LOCAL", execErr: aErrExec}
	repo := pg.NewClosureRepository(&aTxRunner{q: q})
	if _, err := repo.Pseudonymise(context.Background(), closureTenantID, "gcid-A", nil); err == nil {
		t.Fatalf("expected the SET LOCAL failure to propagate")
	}
}

func TestClosureRepository_IsPseudonymised_RLSFailure_Propagates(t *testing.T) {
	t.Parallel()
	q := &aExecFailQuerier{stubQuerier: &stubQuerier{}, failSubstr: "SET LOCAL", execErr: aErrExec}
	repo := pg.NewClosureRepository(&aTxRunner{q: q})
	if _, err := repo.IsPseudonymised(context.Background(), closureTenantID, "gcid-A"); err == nil {
		t.Fatalf("expected the SET LOCAL failure to propagate")
	}
}