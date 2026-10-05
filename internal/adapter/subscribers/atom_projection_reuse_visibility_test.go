// atom_projection_reuse_visibility_test.go — RED specs for the ADR-229 WS-1
// (CHO-2127) subscriber surface:
//
//   - HandleAtomPublished forwards the event's reuse_visibility label onto
//     the Projection (empty when the producer pre-dates ADR-229 — the SQL
//     preserves/hardens, never this layer).
//   - HandleAtomReuseVisibilityChanged maps the NEW audience onto the cached
//     projection via the Writer's SetReuseVisibility port method, idempotent
//     on event_id, failing loud on a missing atom_id or an unknown label.
package subscribers

import (
	"context"
	"testing"
)

func validReuseVisibilityChangedEnv() AtomReuseVisibilityChangedEnvelope {
	return AtomReuseVisibilityChangedEnvelope{
		EventID:            "01970000-0000-7000-9000-0000000000r1",
		TenantID:           "tenant-1",
		GCID:               "gcid-author-1",
		AtomID:             "atom-1",
		ReuseVisibility:    "friends",
		PreviousVisibility: "tenant",
	}
}

func TestAtomProjectionPublished_CarriesReuseVisibility(t *testing.T) {
	sub, w, _ := newProjSub(t)
	env := validPublishedEnv()
	env.ReuseVisibility = "tenant"

	if err := sub.HandleAtomPublished(context.Background(), env); err != nil {
		t.Fatalf("HandleAtomPublished: %v", err)
	}
	if len(w.upserts) != 1 {
		t.Fatalf("expected 1 upsert, got %d", len(w.upserts))
	}
	if got := w.upserts[0].ReuseVisibility; got != "tenant" {
		t.Fatalf("projection reuse_visibility = %q; want tenant", got)
	}
}

func TestAtomProjectionPublished_EmptyReuseVisibilityStaysEmpty(t *testing.T) {
	sub, w, _ := newProjSub(t)
	env := validPublishedEnv() // no reuse_visibility (pre-ADR-229 producer)

	if err := sub.HandleAtomPublished(context.Background(), env); err != nil {
		t.Fatalf("HandleAtomPublished: %v", err)
	}
	if got := w.upserts[0].ReuseVisibility; got != "" {
		t.Fatalf("projection reuse_visibility = %q; want \"\" (SQL hardens on insert, preserves on update)", got)
	}
}

func TestAtomReuseVisibilityChanged_SetsProjectionAudience(t *testing.T) {
	sub, w, _ := newProjSub(t)

	if err := sub.HandleAtomReuseVisibilityChanged(context.Background(), validReuseVisibilityChangedEnv()); err != nil {
		t.Fatalf("HandleAtomReuseVisibilityChanged: %v", err)
	}
	if len(w.sets) != 1 {
		t.Fatalf("expected 1 SetReuseVisibility call, got %d", len(w.sets))
	}
	if w.sets[0].atomID != "atom-1" || w.sets[0].visibility != "friends" {
		t.Fatalf("SetReuseVisibility(%q, %q); want (atom-1, friends)", w.sets[0].atomID, w.sets[0].visibility)
	}
}

func TestAtomReuseVisibilityChanged_IdempotentOnEventID(t *testing.T) {
	sub, w, _ := newProjSub(t)
	env := validReuseVisibilityChangedEnv()

	if err := sub.HandleAtomReuseVisibilityChanged(context.Background(), env); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if err := sub.HandleAtomReuseVisibilityChanged(context.Background(), env); err != nil {
		t.Fatalf("replay call: %v", err)
	}
	if len(w.sets) != 1 {
		t.Fatalf("expected 1 set after replay, got %d (double-apply)", len(w.sets))
	}
}

func TestAtomReuseVisibilityChanged_RejectsMissingAtomID(t *testing.T) {
	sub, w, _ := newProjSub(t)
	env := validReuseVisibilityChangedEnv()
	env.AtomID = ""

	if err := sub.HandleAtomReuseVisibilityChanged(context.Background(), env); err == nil {
		t.Fatalf("expected error for missing atom_id, got nil")
	}
	if len(w.sets) != 0 {
		t.Fatalf("expected 0 sets on refusal, got %d", len(w.sets))
	}
}

// An unknown audience label must NACK (fail loud) — silently applying it
// would either corrupt the projection or trip the DB CHECK later with less
// context.
func TestAtomReuseVisibilityChanged_RejectsUnknownLabel(t *testing.T) {
	sub, w, _ := newProjSub(t)
	env := validReuseVisibilityChangedEnv()
	env.ReuseVisibility = "everyone"

	if err := sub.HandleAtomReuseVisibilityChanged(context.Background(), env); err == nil {
		t.Fatalf("expected error for unknown reuse_visibility label, got nil")
	}
	if len(w.sets) != 0 {
		t.Fatalf("expected 0 sets on refusal, got %d", len(w.sets))
	}
}

func TestSubscribedTopics_IncludesReuseVisibilityChanged(t *testing.T) {
	sub, _, _ := newProjSub(t)
	topics := sub.SubscribedTopics()
	found := false
	for _, tp := range topics {
		if tp == TopicAtomReuseVisibilityChanged {
			found = true
		}
	}
	if !found {
		t.Fatalf("SubscribedTopics() = %v; want it to include %q", topics, TopicAtomReuseVisibilityChanged)
	}
	if len(topics) != 3 {
		t.Fatalf("SubscribedTopics() = %d topics; want 3 (published, archived, reuse_visibility_changed)", len(topics))
	}
}
