package subscribers

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
)

// stubWriter is an in-memory AtomProjectionWriter for testing the subscriber.
type stubWriter struct {
	mu        sync.Mutex
	upserts   []atom_projection.Projection
	invalid   []string
	sets      []setReuseVisibilityCall
	upsertErr error
	invalErr  error
	setErr    error
}

// setReuseVisibilityCall records one SetReuseVisibility invocation (ADR-229).
type setReuseVisibilityCall struct {
	atomID     string
	visibility string
}

func (w *stubWriter) SetReuseVisibility(_ context.Context, atomID, visibility string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.setErr != nil {
		return w.setErr
	}
	w.sets = append(w.sets, setReuseVisibilityCall{atomID: atomID, visibility: visibility})
	return nil
}

func (w *stubWriter) Upsert(_ context.Context, p atom_projection.Projection) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.upsertErr != nil {
		return w.upsertErr
	}
	w.upserts = append(w.upserts, p)
	return nil
}

func (w *stubWriter) Invalidate(_ context.Context, atomID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.invalErr != nil {
		return w.invalErr
	}
	w.invalid = append(w.invalid, atomID)
	return nil
}

func newProjSub(t *testing.T) (*AtomProjectionSubscriber, *stubWriter, *InMemoryIdempotencyStore) {
	t.Helper()
	w := &stubWriter{}
	idem := NewInMemoryIdempotencyStore()
	sub := NewAtomProjectionSubscriber(AtomProjectionConfig{
		Writer:      w,
		Idempotency: idem,
	})
	return sub, w, idem
}

func validPublishedEnv() AtomPublishedEnvelope {
	return AtomPublishedEnvelope{
		EventID:           "01970000-0000-7000-9000-0000000000p1",
		TenantID:          "tenant-1",
		GCID:              "gcid-author-1",
		AtomID:            "atom-1",
		RevisionID:        "rev-1",
		AuthorGCID:        "gcid-author-1",
		AuthorDisplayName: "Ada Lovelace",
		Stem:              "What is 2+2?",
		QuestionType:      "mcq",
	}
}

func TestAtomProjectionPublished_UpsertsProjection(t *testing.T) {
	sub, w, _ := newProjSub(t)

	if err := sub.HandleAtomPublished(context.Background(), validPublishedEnv()); err != nil {
		t.Fatalf("HandleAtomPublished: %v", err)
	}
	if len(w.upserts) != 1 {
		t.Fatalf("expected 1 upsert, got %d", len(w.upserts))
	}
	got := w.upserts[0]
	if got.AtomID != "atom-1" || got.RevisionID != "rev-1" || got.OwnerGCID != "gcid-author-1" {
		t.Fatalf("unexpected projection upserted: %+v", got)
	}
	if got.AuthorDisplayName != "Ada Lovelace" {
		t.Fatalf("R1 author display name not carried: %+v", got)
	}
}

func TestAtomProjectionPublished_IdempotentOnEventID(t *testing.T) {
	sub, w, _ := newProjSub(t)
	env := validPublishedEnv()

	if err := sub.HandleAtomPublished(context.Background(), env); err != nil {
		t.Fatalf("first call: %v", err)
	}
	// Re-deliver the same event_id — must NOT upsert again.
	if err := sub.HandleAtomPublished(context.Background(), env); err != nil {
		t.Fatalf("replay call: %v", err)
	}
	if len(w.upserts) != 1 {
		t.Fatalf("expected 1 upsert after replay, got %d (double-credit)", len(w.upserts))
	}
}

func TestAtomProjectionPublished_RejectsMissingOwnerGCID(t *testing.T) {
	sub, w, _ := newProjSub(t)
	env := validPublishedEnv()
	env.AuthorGCID = "" // breaks R1 attribution
	env.GCID = ""        // no fallback either

	err := sub.HandleAtomPublished(context.Background(), env)
	if err == nil {
		t.Fatalf("expected error for missing owner_gcid (R1), got nil")
	}
	if len(w.upserts) != 0 {
		t.Fatalf("expected no upsert on invalid projection, got %d", len(w.upserts))
	}
}

func TestAtomProjectionPublished_MissingBaseFieldsRejected(t *testing.T) {
	sub, _, _ := newProjSub(t)
	for _, mut := range []func(AtomPublishedEnvelope) AtomPublishedEnvelope{
		func(e AtomPublishedEnvelope) AtomPublishedEnvelope { e.EventID = ""; return e },
		func(e AtomPublishedEnvelope) AtomPublishedEnvelope { e.TenantID = ""; return e },
		func(e AtomPublishedEnvelope) AtomPublishedEnvelope { e.AtomID = ""; return e },
	} {
		if err := sub.HandleAtomPublished(context.Background(), mut(validPublishedEnv())); err == nil {
			t.Fatalf("expected error for missing base field, got nil")
		}
	}
}

func TestAtomProjectionArchived_Invalidates(t *testing.T) {
	sub, w, _ := newProjSub(t)
	env := AtomArchivedEnvelope{
		EventID:  "01970000-0000-7000-9000-0000000000a1",
		TenantID:  "tenant-1",
		GCID:      "gcid-author-1",
		AtomID:    "atom-1",
		Status:    "archived",
		Reason:    "withdrawn_by_author",
	}
	if err := sub.HandleAtomArchived(context.Background(), env); err != nil {
		t.Fatalf("HandleAtomArchived: %v", err)
	}
	if len(w.invalid) != 1 || w.invalid[0] != "atom-1" {
		t.Fatalf("expected atom-1 invalidated, got %v", w.invalid)
	}
	// Idempotent on replay.
	if err := sub.HandleAtomArchived(context.Background(), env); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(w.invalid) != 1 {
		t.Fatalf("expected 1 invalidate after replay, got %d", len(w.invalid))
	}
}

func TestAtomProjectionWriterError_FailsLoud(t *testing.T) {
	sub, w, _ := newProjSub(t)
	w.upsertErr = errors.New("db down")
	err := sub.HandleAtomPublished(context.Background(), validPublishedEnv())
	if err == nil {
		t.Fatalf("expected writer error to propagate, got nil")
	}
}
