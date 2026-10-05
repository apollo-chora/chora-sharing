// atom_projection_extra_test.go — error + decode branches of the
// AtomProjectionSubscriber not covered by the core specs:
//
//   - preflight failure (nil Writer / Idempotency) on all three handlers,
//   - missing-atom_id NACKs for published + archived (reuse_visibility's is
//     covered in atom_projection_reuse_visibility_test.go),
//   - Writer.Invalidate / Writer.SetReuseVisibility error propagation,
//   - the three wire decoders end to end: binary proto, JSON wrap (+attrs
//     envelope merge), and malformed-bytes errors.
package subscribers

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	creationv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/creation/v1"
)

// --- preflight ------------------------------------------------------------------

func TestAtomProjection_PreflightMissingDeps_AllHandlers(t *testing.T) {
	sub := NewAtomProjectionSubscriber(AtomProjectionConfig{})

	if err := sub.HandleAtomPublished(context.Background(), validPublishedEnv()); err == nil {
		t.Fatalf("published handler must fail loud with nil deps")
	}
	if err := sub.HandleAtomArchived(context.Background(), AtomArchivedEnvelope{
		EventID: "e", TenantID: "t", GCID: "g", AtomID: "a",
	}); err == nil {
		t.Fatalf("archived handler must fail loud with nil deps")
	}
	if err := sub.HandleAtomReuseVisibilityChanged(context.Background(), validReuseVisibilityChangedEnv()); err == nil {
		t.Fatalf("reuse_visibility handler must fail loud with nil deps")
	}

	// Nil Writer only — same preflight gate.
	sub2 := NewAtomProjectionSubscriber(AtomProjectionConfig{Idempotency: NewInMemoryIdempotencyStore()})
	if err := sub2.HandleAtomPublished(context.Background(), validPublishedEnv()); err == nil {
		t.Fatalf("nil Writer must fail loud")
	}
}

// --- validation + writer error branches -------------------------------------------

func TestAtomProjectionPublished_RejectsMissingAtomID(t *testing.T) {
	sub, w, _ := newProjSub(t)
	env := validPublishedEnv()
	env.AtomID = ""

	if err := sub.HandleAtomPublished(context.Background(), env); err == nil {
		t.Fatalf("published event without atom_id must NACK")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.upserts) != 0 {
		t.Fatalf("no upsert may land on a rejected event")
	}
}

func TestAtomProjectionArchived_RejectsMissingAtomID(t *testing.T) {
	sub, w, _ := newProjSub(t)
	env := AtomArchivedEnvelope{
		EventID: "01970000-0000-7000-9000-0000000000a2",
		TenantID: "tenant-1",
		GCID:     "gcid-author-1",
		// AtomID intentionally empty.
	}
	if err := sub.HandleAtomArchived(context.Background(), env); err == nil {
		t.Fatalf("archived event without atom_id must NACK")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.invalid) != 0 {
		t.Fatalf("no invalidation may land on a rejected event")
	}
}

func TestAtomProjectionArchived_InvalidateError_FailsLoud(t *testing.T) {
	sub, w, _ := newProjSub(t)
	w.invalErr = errors.New("db down")

	err := sub.HandleAtomArchived(context.Background(), AtomArchivedEnvelope{
		EventID: "01970000-0000-7000-9000-0000000000a3",
		TenantID: "tenant-1",
		GCID:     "gcid-author-1",
		AtomID:   "atom-1",
	})
	if err == nil {
		t.Fatalf("Invalidate error must propagate, got nil")
	}
}

func TestAtomProjectionReuseVisibility_SetError_FailsLoud(t *testing.T) {
	sub, w, _ := newProjSub(t)
	w.setErr = errors.New("db down")

	if err := sub.HandleAtomReuseVisibilityChanged(context.Background(), validReuseVisibilityChangedEnv()); err == nil {
		t.Fatalf("SetReuseVisibility error must propagate, got nil")
	}
}

func TestAtomProjectionArchived_MissingBaseFields_NACK(t *testing.T) {
	sub, w, _ := newProjSub(t)
	for _, mut := range []func(AtomArchivedEnvelope) AtomArchivedEnvelope{
		func(e AtomArchivedEnvelope) AtomArchivedEnvelope { e.EventID = ""; return e },
		func(e AtomArchivedEnvelope) AtomArchivedEnvelope { e.TenantID = ""; return e },
		func(e AtomArchivedEnvelope) AtomArchivedEnvelope { e.GCID = ""; return e },
	} {
		if err := sub.HandleAtomArchived(context.Background(), mut(AtomArchivedEnvelope{
			EventID: "01970000-0000-7000-9000-0000000000a5",
			TenantID: "tenant-1",
			GCID:     "gcid-author-1",
			AtomID:   "atom-1",
		})); err == nil {
			t.Fatalf("missing base field must NACK")
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.invalid) != 0 {
		t.Fatalf("no invalidation may land on a rejected event")
	}
}

func TestAtomProjectionReuseVisibility_MissingBaseFields_NACK(t *testing.T) {
	sub, w, _ := newProjSub(t)
	for _, mut := range []func(AtomReuseVisibilityChangedEnvelope) AtomReuseVisibilityChangedEnvelope{
		func(e AtomReuseVisibilityChangedEnvelope) AtomReuseVisibilityChangedEnvelope { e.EventID = ""; return e },
		func(e AtomReuseVisibilityChangedEnvelope) AtomReuseVisibilityChangedEnvelope { e.TenantID = ""; return e },
		func(e AtomReuseVisibilityChangedEnvelope) AtomReuseVisibilityChangedEnvelope { e.GCID = ""; return e },
	} {
		if err := sub.HandleAtomReuseVisibilityChanged(context.Background(), mut(validReuseVisibilityChangedEnv())); err == nil {
			t.Fatalf("missing base field must NACK")
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.sets) != 0 {
		t.Fatalf("no set may land on a rejected event")
	}
}

func TestAtomProjectionPublished_ProjectionValidationError_NACKs(t *testing.T) {
	// The consent facts stay mandatory: with GCID blank AND author_gcid blank
	// the R1 attribution cannot be established — the projection must not cache
	// a consent-less row. (OwnerGCID is otherwise guaranteeably non-empty
	// because validateBase already required GCID — this is the belt.)
	sub, w, _ := newProjSub(t)
	env := validPublishedEnv()
	env.AuthorGCID = ""
	env.GCID = ""

	if err := sub.HandleAtomPublished(context.Background(), env); err == nil {
		t.Fatalf("owner-less published event must NACK")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.upserts) != 0 {
		t.Fatalf("no upsert may land on a consent-less event")
	}
}

// --- wire decoders ----------------------------------------------------------------

func decodeAtomPublishedBinary() *creationv1.AtomPublished {
	return &creationv1.AtomPublished{
		Envelope: &commonv1.EventEnvelope{
			EventId:  "01970000-0000-7000-9000-0000000000p2",
			TenantId: "tenant-1",
			Gcid:     "gcid-author-1",
		},
		AtomId:            "atom-1",
		CurrentRevisionId: "rev-1",
		AuthorGcid:        "gcid-author-1",
		QuestionType:      creationv1.AtomType_ATOM_TYPE_MULTIPLE_CHOICE,
		Status:            creationv1.AtomStatus_ATOM_STATUS_PUBLISHED,
		Title:             "Two plus two",
		Stem:              "What is 2+2?",
		ReuseVisibility:   creationv1.ReuseVisibility_REUSE_VISIBILITY_PRIVATE,
	}
}

func TestDecodeAtomPublishedWithAttrs_Binary(t *testing.T) {
	bz, err := proto.Marshal(decodeAtomPublishedBinary())
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	env, err := DecodeAtomPublishedWithAttrs(bz, nil)
	if err != nil {
		t.Fatalf("DecodeAtomPublishedWithAttrs: %v", err)
	}
	if env.EventID != "01970000-0000-7000-9000-0000000000p2" || env.TenantID != "tenant-1" || env.GCID != "gcid-author-1" {
		t.Fatalf("envelope fields: %+v", env)
	}
	if env.AtomID != "atom-1" || env.RevisionID != "rev-1" || env.AuthorGCID != "gcid-author-1" {
		t.Fatalf("payload ids: %+v", env)
	}
	if env.QuestionType != "mcq" || env.Stem != "What is 2+2?" || env.Title != "Two plus two" {
		t.Fatalf("question fields: %+v", env)
	}
	if env.ReuseVisibility != "private" || env.Status != "published" {
		t.Fatalf("consent/status fields: %+v", env)
	}
}

func TestDecodeAtomPublishedWithAttrs_JSONWrap(t *testing.T) {
	body := []byte(`{
		"atom_id": "atom-9",
		"current_revision_id": "rev-9",
		"author_gcid": "gcid-author-9",
		"stem": "What is 9x9?",
		"question_type": "mcq",
		"options": ["81", "18"],
		"correct_answer": "81",
		"reuse_visibility": "tenant"
	}`)
	attrs := map[string]string{"event_id": "evt-pub-json", "tenant_id": "t-json", "gcid": "g-json"}
	env, err := DecodeAtomPublishedWithAttrs(body, attrs)
	if err != nil {
		t.Fatalf("DecodeAtomPublishedWithAttrs: %v", err)
	}
	if env.EventID != "evt-pub-json" || env.TenantID != "t-json" || env.GCID != "g-json" {
		t.Fatalf("envelope fields not merged from attrs: %+v", env)
	}
	if env.AtomID != "atom-9" || env.RevisionID != "rev-9" || env.AuthorGCID != "gcid-author-9" {
		t.Fatalf("payload ids: %+v", env)
	}
	if len(env.Options) != 2 || env.Options[0] != "81" || env.CorrectAnswer != "81" {
		t.Fatalf("options/correct_answer not decoded from JSON: %+v", env)
	}
	if env.ReuseVisibility != "tenant" {
		t.Fatalf("reuse_visibility = %q, want tenant", env.ReuseVisibility)
	}
}

func TestDecodeAtomPublishedWithAttrs_Error(t *testing.T) {
	if _, err := DecodeAtomPublishedWithAttrs([]byte("{not json"), nil); err == nil {
		t.Fatalf("malformed bytes must error")
	}
}

func TestDecodeAtomArchivedWithAttrs_Binary(t *testing.T) {
	msg := &creationv1.AtomArchived{
		Envelope: &commonv1.EventEnvelope{
			EventId:  "01970000-0000-7000-9000-0000000000a4",
			TenantId: "tenant-1",
			Gcid:     "gcid-author-1",
		},
		AtomId: "atom-1",
		Status: creationv1.AtomStatus_ATOM_STATUS_ARCHIVED,
		Reason: "withdrawn_by_author",
	}
	bz, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	env, err := DecodeAtomArchivedWithAttrs(bz, nil)
	if err != nil {
		t.Fatalf("DecodeAtomArchivedWithAttrs: %v", err)
	}
	if env.EventID != "01970000-0000-7000-9000-0000000000a4" || env.TenantID != "tenant-1" || env.GCID != "gcid-author-1" {
		t.Fatalf("envelope fields: %+v", env)
	}
	if env.AtomID != "atom-1" || env.Status != "archived" {
		t.Fatalf("payload fields: %+v", env)
	}
	// NOTE: the binary projector omits reason (projectAtomArchived emits only
	// atom_id + status); the JSON/attrs wrap carries it.
}

func TestDecodeAtomArchivedWithAttrs_JSONWrap(t *testing.T) {
	env, err := DecodeAtomArchivedWithAttrs(
		[]byte(`{"atom_id":"atom-2","status":"archived","reason":"admin_takedown"}`),
		map[string]string{"event_id": "evt-arch-json", "tenant_id": "t-json", "gcid": "g-json"},
	)
	if err != nil {
		t.Fatalf("DecodeAtomArchivedWithAttrs: %v", err)
	}
	if env.EventID != "evt-arch-json" || env.TenantID != "t-json" || env.GCID != "g-json" {
		t.Fatalf("envelope fields not merged from attrs: %+v", env)
	}
	if env.AtomID != "atom-2" || env.Status != "archived" || env.Reason != "admin_takedown" {
		t.Fatalf("payload fields: %+v", env)
	}
}

func TestDecodeAtomArchivedWithAttrs_Error(t *testing.T) {
	if _, err := DecodeAtomArchivedWithAttrs([]byte("{not json"), nil); err == nil {
		t.Fatalf("malformed bytes must error")
	}
}

func TestDecodeAtomReuseVisibilityChangedWithAttrs_Binary(t *testing.T) {
	msg := &creationv1.AtomReuseVisibilityChanged{
		Envelope: &commonv1.EventEnvelope{
			EventId:  "01970000-0000-7000-9000-0000000000r2",
			TenantId: "tenant-1",
			Gcid:     "gcid-author-1",
		},
		AtomId:             "atom-1",
		ReuseVisibility:    creationv1.ReuseVisibility_REUSE_VISIBILITY_FRIENDS,
		PreviousVisibility: creationv1.ReuseVisibility_REUSE_VISIBILITY_TENANT,
		AuthorGcid:         "gcid-author-1",
	}
	bz, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	env, err := DecodeAtomReuseVisibilityChangedWithAttrs(bz, nil)
	if err != nil {
		t.Fatalf("DecodeAtomReuseVisibilityChangedWithAttrs: %v", err)
	}
	if env.EventID != "01970000-0000-7000-9000-0000000000r2" || env.TenantID != "tenant-1" || env.GCID != "gcid-author-1" {
		t.Fatalf("envelope fields: %+v", env)
	}
	if env.AtomID != "atom-1" || env.ReuseVisibility != "friends" || env.PreviousVisibility != "tenant" || env.AuthorGCID != "gcid-author-1" {
		t.Fatalf("payload fields: %+v", env)
	}
}

func TestDecodeAtomReuseVisibilityChangedWithAttrs_JSONWrap(t *testing.T) {
	env, err := DecodeAtomReuseVisibilityChangedWithAttrs(
		[]byte(`{"atom_id":"atom-3","reuse_visibility":"private","previous_visibility":"tenant"}`),
		map[string]string{"event_id": "evt-rv-json", "tenant_id": "t-json", "gcid": "g-json", "author_gcid": "g-json"},
	)
	if err != nil {
		t.Fatalf("DecodeAtomReuseVisibilityChangedWithAttrs: %v", err)
	}
	if env.EventID != "evt-rv-json" || env.TenantID != "t-json" || env.AtomID != "atom-3" {
		t.Fatalf("envelope/atom fields: %+v", env)
	}
	if env.ReuseVisibility != "private" || env.PreviousVisibility != "tenant" {
		t.Fatalf("visibility fields: %+v", env)
	}
}

func TestDecodeAtomReuseVisibilityChangedWithAttrs_Error(t *testing.T) {
	if _, err := DecodeAtomReuseVisibilityChangedWithAttrs([]byte("{not json"), nil); err == nil {
		t.Fatalf("malformed bytes must error")
	}
}