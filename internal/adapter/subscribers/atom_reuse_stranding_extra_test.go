// atom_reuse_stranding_extra_test.go — fault + edge branches of the orphan
// saga subscriber not covered by the core stranding specs:
//
//   - HandleReuseVisibilityChanged validation NACKs (missing atom_id, invalid
//     NEW audience — previous is covered in the core specs),
//   - detectAndAct repo failures (grants list, projection read non-NotFound,
//     edition read),
//   - classify / friendSetOfAuthor edges (FriendSet failure, grants with no
//     author owner → empty friend set),
//   - repointOnOrphanCreated repo failures (edition upsert, grants list,
//     projection read non-NotFound) + the no-active-grants log-and-return,
//   - the decoder's malformed-bytes error path.
package subscribers

import (
	"context"
	"errors"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/atom_reuse"
)

// errFriends is a FriendReads double that always fails, proving classify's
// error propagation.
type errFriends struct{}

func (errFriends) FriendSet(_ context.Context, _, _ string) ([]string, error) {
	return nil, errors.New("social graph down")
}

// errEditions is an EditionStore double whose reads/upserts fail, proving
// detectAndAct / repointOnOrphanCreated error propagation.
type errEditions struct{}

func (errEditions) LatestEdition(_ context.Context, _ string) (atom_reuse.OrphanEdition, bool, error) {
	return atom_reuse.OrphanEdition{}, false, errors.New("editions down")
}
func (errEditions) PutEdition(_ context.Context, _ atom_reuse.OrphanEdition) error {
	return errors.New("editions down")
}

func TestStranding_VisibilityMissingAtomID_NACKs(t *testing.T) {
	h := newStrandingHarness()
	ev := visibilityEvent("tenant", "private")
	ev.AtomID = "" // missing — not the harness's norm

	if err := h.sub.HandleReuseVisibilityChanged(context.Background(), ev); err == nil {
		t.Fatalf("missing atom_id must NACK")
	}
	if len(h.requirer.reqs) != 0 {
		t.Fatalf("no orphan request may fire on a rejected event")
	}
}

func TestStranding_InvalidNewAudience_NACKs(t *testing.T) {
	h := newStrandingHarness()
	ev := visibilityEvent("tenant", "everyone") // junk NEW audience

	if err := h.sub.HandleReuseVisibilityChanged(context.Background(), ev); err == nil {
		t.Fatalf("invalid new audience must NACK (fail loud, never guess)")
	}
	if len(h.requirer.reqs) != 0 {
		t.Fatalf("no orphan request may fire on a rejected event")
	}
}

func TestStranding_GrantsError_NACKs(t *testing.T) {
	h := newStrandingHarness()
	h.grants.err = errors.New("grants down")

	if err := h.sub.HandleReuseVisibilityChanged(context.Background(), visibilityEvent("tenant", "private")); err == nil {
		t.Fatalf("grants read failure must NACK")
	}
}

func TestStranding_ProjectionReadError_NACKs(t *testing.T) {
	h := newStrandingHarness()
	// A non-NotFound projection read failure is fatal in detectAndAct.
	h.projections.err = errors.New("projection db down")

	if err := h.sub.HandleReuseVisibilityChanged(context.Background(), visibilityEvent("tenant", "private")); err == nil {
		t.Fatalf("projection read failure must NACK")
	}
}

func TestStranding_EditionsReadError_NACKs(t *testing.T) {
	sub := NewAtomReuseStrandingSubscriber(AtomReuseStrandingConfig{
		Grants:      &fakeGrantReads{refs: []atom_reuse.GrantRef{{GrantID: "g-bob", GranteeGCID: strBob, OwnerGCID: strAuthor}}},
		Editions:    errEditions{},
		Repointer:   &fakeRepointer{},
		Requirer:    &fakeRequirer{},
		Projections: &fakeProjections{st: atom_reuse.ProjectionState{RevisionID: strRev, OwnerGCID: strAuthor, ReuseVisibility: "private"}},
		Friends:     &fakeFriends{},
		Idempotency: NewInMemoryIdempotencyStore(),
	})

	if err := sub.HandleReuseVisibilityChanged(context.Background(), visibilityEvent("tenant", "private")); err == nil {
		t.Fatalf("edition read failure must NACK")
	}
}

func TestStranding_FriendSetError_NACKs(t *testing.T) {
	sub := NewAtomReuseStrandingSubscriber(AtomReuseStrandingConfig{
		Grants:      &fakeGrantReads{refs: []atom_reuse.GrantRef{{GrantID: "g-bob", GranteeGCID: strBob, OwnerGCID: strAuthor}}},
		Editions:    &fakeEditions{},
		Repointer:   &fakeRepointer{},
		Requirer:    &fakeRequirer{},
		Projections: &fakeProjections{st: atom_reuse.ProjectionState{RevisionID: strRev, OwnerGCID: strAuthor, ReuseVisibility: "private"}},
		Friends:     errFriends{},
		Idempotency: NewInMemoryIdempotencyStore(),
	})

	if err := sub.HandleReuseVisibilityChanged(context.Background(), visibilityEvent("tenant", "friends")); err == nil {
		t.Fatalf("friend-set failure must NACK")
	}
}

func TestStranding_FriendsAudience_NoAuthorOnGrants(t *testing.T) {
	// Grants whose OwnerGCID is blank mean the friend set cannot be resolved —
	// classify degrades to the empty set (a nil friend set strands everyone
	// outside it, but there is nothing to print either way).
	h := newStrandingHarness()
	h.grants.refs = []atom_reuse.GrantRef{
		{GrantID: "g-bob", GranteeGCID: strBob, Scope: "test_set"},
	}
	if err := h.sub.HandleReuseVisibilityChanged(context.Background(), visibilityEvent("tenant", "friends")); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(h.requirer.reqs) != 1 {
		t.Fatalf("requirer calls = %d, want 1", len(h.requirer.reqs))
	}
}

func TestOrphanCreated_PutEditionError_NACKs(t *testing.T) {
	sub := NewAtomReuseStrandingSubscriber(AtomReuseStrandingConfig{
		Grants:      &fakeGrantReads{refs: []atom_reuse.GrantRef{{GrantID: "g-bob", GranteeGCID: strBob, OwnerGCID: strAuthor}}},
		Editions:    errEditions{},
		Repointer:   &fakeRepointer{},
		Requirer:    &fakeRequirer{},
		Projections: &fakeProjections{st: atom_reuse.ProjectionState{RevisionID: strRev, OwnerGCID: strAuthor, ReuseVisibility: "private"}},
		Friends:     &fakeFriends{},
		Idempotency: NewInMemoryIdempotencyStore(),
	})

	if err := sub.HandleAtomOrphanCreated(context.Background(), orphanCreatedEvent()); err == nil {
		t.Fatalf("edition upsert failure must NACK (map must land before any repoint)")
	}
}

func TestOrphanCreated_GrantsError_NACKs(t *testing.T) {
	h := newStrandingHarness()
	h.grants.err = errors.New("grants down")

	if err := h.sub.HandleAtomOrphanCreated(context.Background(), orphanCreatedEvent()); err == nil {
		t.Fatalf("grants read failure on repoint must NACK")
	}
}

func TestOrphanCreated_ProjectionReadError_NACKs(t *testing.T) {
	h := newStrandingHarness()
	h.projections.err = errors.New("projection db down")

	if err := h.sub.HandleAtomOrphanCreated(context.Background(), orphanCreatedEvent()); err == nil {
		t.Fatalf("non-NotFound projection read failure must NACK")
	}
}

func TestOrphanCreated_NoActiveGrants_StoresEditionOnly(t *testing.T) {
	h := newStrandingHarness()
	h.grants.refs = nil

	if err := h.sub.HandleAtomOrphanCreated(context.Background(), orphanCreatedEvent()); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(h.editions.puts) != 1 {
		t.Fatalf("the edition map must still be stored; puts = %d", len(h.editions.puts))
	}
	if len(h.repointer.cmds) != 0 {
		t.Fatalf("no grants → no repoint; cmds = %v", h.repointer.cmds)
	}
}

func TestStranding_VisibilityMissingBaseFields_NACK(t *testing.T) {
	h := newStrandingHarness()
	for _, mut := range []func(AtomReuseVisibilityChangedEnvelope) AtomReuseVisibilityChangedEnvelope{
		func(e AtomReuseVisibilityChangedEnvelope) AtomReuseVisibilityChangedEnvelope { e.EventID = ""; return e },
		func(e AtomReuseVisibilityChangedEnvelope) AtomReuseVisibilityChangedEnvelope { e.TenantID = ""; return e },
		func(e AtomReuseVisibilityChangedEnvelope) AtomReuseVisibilityChangedEnvelope { e.GCID = ""; return e },
	} {
		if err := h.sub.HandleReuseVisibilityChanged(context.Background(), mut(visibilityEvent("tenant", "private"))); err == nil {
			t.Fatalf("missing base field must NACK")
		}
	}
	if len(h.requirer.reqs) != 0 {
		t.Fatalf("no orphan request may fire on a rejected event")
	}
}

func TestOrphanCreated_FriendsError_NACKs(t *testing.T) {
	// The repoint recompute hits the classify error path when the CURRENT
	// projection is a friends audience and the friend-set read fails.
	sub := NewAtomReuseStrandingSubscriber(AtomReuseStrandingConfig{
		Grants:      &fakeGrantReads{refs: []atom_reuse.GrantRef{{GrantID: "g-bob", GranteeGCID: strBob, OwnerGCID: strAuthor}}},
		Editions:    &fakeEditions{},
		Repointer:   &fakeRepointer{},
		Requirer:    &fakeRequirer{},
		Projections: &fakeProjections{st: atom_reuse.ProjectionState{RevisionID: strRev, OwnerGCID: strAuthor, ReuseVisibility: "friends"}},
		Friends:     errFriends{},
		Idempotency: NewInMemoryIdempotencyStore(),
	})

	if err := sub.HandleAtomOrphanCreated(context.Background(), orphanCreatedEvent()); err == nil {
		t.Fatalf("friend-set failure on the repoint recompute must NACK")
	}
}

func TestDecodeAtomOrphanCreatedWithAttrs_Error(t *testing.T) {
	if _, err := DecodeAtomOrphanCreatedWithAttrs([]byte("{not json"), nil); err == nil {
		t.Fatalf("malformed bytes must error")
	}
}