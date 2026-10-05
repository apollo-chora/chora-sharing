// atom_reuse_stranding_subscriber_test.go — ADR-229 Amendment A1 (CHO-2132):
// the sharing legs of the orphan saga. RED-first.
//
// Leg 1 (detection): a withdrawal (reuse-visibility narrowing / archive) with
// >=1 STRANDED active grant asks creation to mint the singleton orphan via
// chora.sharing.atom_reuse.orphan_required.v1 — UNLESS sharing already knows
// the orphan for the atom's current pinned revision (the event-fed
// atom_orphan_editions map), in which case it repoints LOCALLY: creation's
// idempotent mint deliberately emits nothing on a singleton conflict, so a
// repeat withdrawal must never depend on a fresh orphan_created.
//
// Leg 3 (repoint): chora.creation.atom.orphan_created.v1 stores the edition
// mapping and repoints the grants stranded per the CURRENT projection state
// (recompute — an in-flight wave stranded after detection still repoints).
// Grants are NEVER revoked; consumers inside the audience keep live refs.
package subscribers

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/apollo-chora/chora-sharing/internal/domain/atom_reuse"
	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	creationv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/creation/v1"
)

const (
	strTenant  = "01970000-0000-7111-8111-000000000001"
	strAuthor  = "01970000-0000-7000-8000-0000000000aa"
	strBob     = "01970000-0000-7000-8000-0000000000b0"
	strCarol   = "01970000-0000-7000-8000-0000000000c0"
	strAtom    = "01970000-0000-7000-8000-0000000000a1"
	strOrphan  = "01970000-0000-7000-8000-0000000000e1"
	strRev     = "01970000-0000-7000-8000-0000000000f1"
	strEventID = "01970000-0000-7000-8000-00000000e001"
)

// --- port fakes ----------------------------------------------------------------

type fakeGrantReads struct {
	refs []atom_reuse.GrantRef
	err  error
}

func (f *fakeGrantReads) ActiveGrantRefsForAtom(_ context.Context, _ string) ([]atom_reuse.GrantRef, error) {
	return f.refs, f.err
}

type fakeEditions struct {
	edition atom_reuse.OrphanEdition
	known   bool
	puts    []atom_reuse.OrphanEdition
}

func (f *fakeEditions) LatestEdition(_ context.Context, _ string) (atom_reuse.OrphanEdition, bool, error) {
	return f.edition, f.known, nil
}
func (f *fakeEditions) PutEdition(_ context.Context, e atom_reuse.OrphanEdition) error {
	f.puts = append(f.puts, e)
	f.known = true
	f.edition = e
	return nil
}

type fakeRepointer struct {
	cmds []atom_reuse.RepointCommand
	err  error
}

func (f *fakeRepointer) RepointStranded(_ context.Context, cmd atom_reuse.RepointCommand) (atom_reuse.RepointResult, error) {
	if f.err != nil {
		return atom_reuse.RepointResult{}, f.err
	}
	f.cmds = append(f.cmds, cmd)
	return atom_reuse.RepointResult{Repointed: len(cmd.Stranded)}, nil
}

type fakeRequirer struct {
	reqs []atom_reuse.OrphanRequest
	err  error
}

func (f *fakeRequirer) RequireOrphan(_ context.Context, req atom_reuse.OrphanRequest) error {
	if f.err != nil {
		return f.err
	}
	f.reqs = append(f.reqs, req)
	return nil
}

type fakeProjections struct {
	st  atom_reuse.ProjectionState
	err error
}

func (f *fakeProjections) GetAnyState(_ context.Context, _ string) (atom_reuse.ProjectionState, error) {
	if f.err != nil {
		return atom_reuse.ProjectionState{}, f.err
	}
	return f.st, nil
}

type fakeFriends struct {
	set      []string
	askedFor []string // gcids FriendSet was called with
}

func (f *fakeFriends) FriendSet(_ context.Context, _ string, gcid string) ([]string, error) {
	f.askedFor = append(f.askedFor, gcid)
	return f.set, nil
}

// --- harness ---------------------------------------------------------------------

type strandingHarness struct {
	grants      *fakeGrantReads
	editions    *fakeEditions
	repointer   *fakeRepointer
	requirer    *fakeRequirer
	projections *fakeProjections
	friends     *fakeFriends
	sub         *AtomReuseStrandingSubscriber
}

func newStrandingHarness() *strandingHarness {
	h := &strandingHarness{
		grants: &fakeGrantReads{refs: []atom_reuse.GrantRef{
			{GrantID: "g-author", GranteeGCID: strAuthor, OwnerGCID: strAuthor, Scope: "collection"},
			{GrantID: "g-bob", GranteeGCID: strBob, OwnerGCID: strAuthor, Scope: "test_set"},
			{GrantID: "g-carol", GranteeGCID: strCarol, OwnerGCID: strAuthor, Scope: "duel"},
		}},
		editions:    &fakeEditions{},
		repointer:   &fakeRepointer{},
		requirer:    &fakeRequirer{},
		projections: &fakeProjections{st: atom_reuse.ProjectionState{RevisionID: strRev, OwnerGCID: strAuthor, ReuseVisibility: "private"}},
		friends:     &fakeFriends{},
	}
	h.sub = NewAtomReuseStrandingSubscriber(AtomReuseStrandingConfig{
		Grants:      h.grants,
		Editions:    h.editions,
		Repointer:   h.repointer,
		Requirer:    h.requirer,
		Projections: h.projections,
		Friends:     h.friends,
		Idempotency: NewInMemoryIdempotencyStore(),
	})
	return h
}

func visibilityEvent(prev, next string) AtomReuseVisibilityChangedEnvelope {
	return AtomReuseVisibilityChangedEnvelope{
		EventID:            strEventID,
		TenantID:           strTenant,
		GCID:               strAuthor,
		AtomID:             strAtom,
		ReuseVisibility:    next,
		PreviousVisibility: prev,
		AuthorGCID:         strAuthor,
	}
}

func archivedEvent() AtomArchivedEnvelope {
	return AtomArchivedEnvelope{
		EventID:  strEventID,
		TenantID: strTenant,
		GCID:     strAuthor,
		AtomID:   strAtom,
		Status:   "archived",
	}
}

func orphanCreatedEvent() AtomOrphanCreatedEnvelope {
	return AtomOrphanCreatedEnvelope{
		EventID:            strEventID,
		TenantID:           strTenant,
		GCID:               strAuthor,
		OrphanAtomID:       strOrphan,
		OrphanedFromAtomID: strAtom,
		SourceRevisionID:   strRev,
		AuthorGCID:         strAuthor,
		Trigger:            "narrowed",
	}
}

// --- leg 1: narrowing detection ----------------------------------------------------

func TestStranding_NarrowToPrivate_RequiresOrphan(t *testing.T) {
	h := newStrandingHarness()
	if err := h.sub.HandleReuseVisibilityChanged(context.Background(), visibilityEvent("tenant", "private")); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(h.requirer.reqs) != 1 {
		t.Fatalf("requirer calls = %d, want 1", len(h.requirer.reqs))
	}
	req := h.requirer.reqs[0]
	if req.AtomID != strAtom || req.Trigger != atom_reuse.TriggerNarrowed {
		t.Fatalf("request = %+v", req)
	}
	if req.StrandedCount != 2 {
		t.Fatalf("stranded = %d, want 2 (bob + carol; the author self-grant never strands)", req.StrandedCount)
	}
	if req.RevisionID != strRev {
		t.Fatalf("revision hint = %q, want the projection's pinned %q", req.RevisionID, strRev)
	}
	if req.ActorGCID != strAuthor {
		t.Fatalf("actor = %q", req.ActorGCID)
	}
	if len(h.repointer.cmds) != 0 {
		t.Fatalf("no local repoint without a known edition; cmds = %v", h.repointer.cmds)
	}
}

func TestStranding_KnownCurrentEdition_RepointsLocallyNoRoundTrip(t *testing.T) {
	h := newStrandingHarness()
	h.editions.known = true
	h.editions.edition = atom_reuse.OrphanEdition{
		AtomID: strAtom, SourceRevisionID: strRev, OrphanAtomID: strOrphan, OrphanedAt: time.Now().UTC(),
	}
	if err := h.sub.HandleReuseVisibilityChanged(context.Background(), visibilityEvent("tenant", "private")); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(h.requirer.reqs) != 0 {
		t.Fatalf("known-current edition must NOT round-trip creation (its mint emits nothing on conflict); reqs = %v", h.requirer.reqs)
	}
	if len(h.repointer.cmds) != 1 {
		t.Fatalf("local repoint expected; cmds = %d", len(h.repointer.cmds))
	}
	cmd := h.repointer.cmds[0]
	if cmd.OriginalAtomID != strAtom || cmd.OrphanAtomID != strOrphan || len(cmd.Stranded) != 2 {
		t.Fatalf("cmd = %+v", cmd)
	}
}

func TestStranding_StaleEdition_RoundTripsCreation(t *testing.T) {
	h := newStrandingHarness()
	h.editions.known = true
	h.editions.edition = atom_reuse.OrphanEdition{
		AtomID: strAtom, SourceRevisionID: "01970000-0000-7000-8000-00000000old1", OrphanAtomID: strOrphan, OrphanedAt: time.Now().UTC(),
	}
	// Projection pins a NEWER revision — a new orphan is needed.
	if err := h.sub.HandleReuseVisibilityChanged(context.Background(), visibilityEvent("tenant", "private")); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(h.requirer.reqs) != 1 || len(h.repointer.cmds) != 0 {
		t.Fatalf("stale edition must round-trip creation; reqs=%d repoints=%d", len(h.requirer.reqs), len(h.repointer.cmds))
	}
}

func TestStranding_TenantToFriends_StrandsOnlyNonFriends(t *testing.T) {
	h := newStrandingHarness()
	h.friends.set = []string{strBob} // bob stays inside the audience
	if err := h.sub.HandleReuseVisibilityChanged(context.Background(), visibilityEvent("tenant", "friends")); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(h.requirer.reqs) != 1 || h.requirer.reqs[0].StrandedCount != 1 {
		t.Fatalf("want exactly carol stranded; reqs = %+v", h.requirer.reqs)
	}
	if len(h.friends.askedFor) != 1 || h.friends.askedFor[0] != strAuthor {
		t.Fatalf("friend set must be resolved for the AUTHOR; asked = %v", h.friends.askedFor)
	}
}

func TestStranding_WideningAndNoOp_DoNothing(t *testing.T) {
	for _, c := range [][2]string{{"private", "tenant"}, {"friends", "tenant"}, {"tenant", "tenant"}} {
		h := newStrandingHarness()
		if err := h.sub.HandleReuseVisibilityChanged(context.Background(), visibilityEvent(c[0], c[1])); err != nil {
			t.Fatalf("handle(%v): %v", c, err)
		}
		if len(h.requirer.reqs) != 0 || len(h.repointer.cmds) != 0 {
			t.Fatalf("widening %v must strand nobody", c)
		}
	}
}

func TestStranding_ZeroStranded_EmitsNothing(t *testing.T) {
	h := newStrandingHarness()
	h.grants.refs = []atom_reuse.GrantRef{
		{GrantID: "g-author", GranteeGCID: strAuthor, OwnerGCID: strAuthor, Scope: "collection"},
	}
	if err := h.sub.HandleReuseVisibilityChanged(context.Background(), visibilityEvent("tenant", "private")); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(h.requirer.reqs) != 0 || len(h.repointer.cmds) != 0 {
		t.Fatalf("zero stranded must emit nothing")
	}
}

func TestStranding_InvalidPreviousLabel_FailsLoud(t *testing.T) {
	h := newStrandingHarness()
	if err := h.sub.HandleReuseVisibilityChanged(context.Background(), visibilityEvent("", "private")); err == nil {
		t.Fatalf("missing previous_visibility must NACK (cannot classify statelessly)")
	}
	if err := h.sub.HandleReuseVisibilityChanged(context.Background(), visibilityEvent("bogus", "private")); err == nil {
		t.Fatalf("junk previous_visibility must NACK")
	}
}

func TestStranding_DuplicateDelivery_Skips(t *testing.T) {
	h := newStrandingHarness()
	ev := visibilityEvent("tenant", "private")
	if err := h.sub.HandleReuseVisibilityChanged(context.Background(), ev); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := h.sub.HandleReuseVisibilityChanged(context.Background(), ev); err != nil {
		t.Fatalf("redelivery must ack: %v", err)
	}
	if len(h.requirer.reqs) != 1 {
		t.Fatalf("redelivery must not double-publish; reqs = %d", len(h.requirer.reqs))
	}
}

func TestStranding_RequirerFailure_NACKs(t *testing.T) {
	h := newStrandingHarness()
	h.requirer.err = errors.New("outbox down")
	if err := h.sub.HandleReuseVisibilityChanged(context.Background(), visibilityEvent("tenant", "private")); err == nil {
		t.Fatalf("requirer failure must NACK for redelivery")
	}
}

// --- leg 1: archive detection ------------------------------------------------------

func TestStranding_Archive_StrandsAllNonAuthor(t *testing.T) {
	h := newStrandingHarness()
	if err := h.sub.HandleAtomArchivedStranding(context.Background(), archivedEvent()); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(h.requirer.reqs) != 1 {
		t.Fatalf("requirer calls = %d", len(h.requirer.reqs))
	}
	req := h.requirer.reqs[0]
	if req.Trigger != atom_reuse.TriggerArchived || req.StrandedCount != 2 {
		t.Fatalf("request = %+v", req)
	}
}

func TestStranding_ArchiveWithKnownCurrentEdition_RepointsLocally(t *testing.T) {
	h := newStrandingHarness()
	h.editions.known = true
	h.editions.edition = atom_reuse.OrphanEdition{
		AtomID: strAtom, SourceRevisionID: strRev, OrphanAtomID: strOrphan, OrphanedAt: time.Now().UTC(),
	}
	// Projection still readable (archived=true) with the same pinned revision.
	h.projections.st = atom_reuse.ProjectionState{RevisionID: strRev, OwnerGCID: strAuthor, ReuseVisibility: "tenant", Archived: true}
	if err := h.sub.HandleAtomArchivedStranding(context.Background(), archivedEvent()); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(h.requirer.reqs) != 0 || len(h.repointer.cmds) != 1 {
		t.Fatalf("archive with current edition must repoint locally; reqs=%d cmds=%d", len(h.requirer.reqs), len(h.repointer.cmds))
	}
	if h.repointer.cmds[0].Trigger != atom_reuse.TriggerArchived {
		t.Fatalf("trigger = %q", h.repointer.cmds[0].Trigger)
	}
}

func TestStranding_Archive_NoGrants_Nothing(t *testing.T) {
	h := newStrandingHarness()
	h.grants.refs = nil
	if err := h.sub.HandleAtomArchivedStranding(context.Background(), archivedEvent()); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(h.requirer.reqs) != 0 {
		t.Fatalf("no grants → nothing")
	}
}

// --- leg 3: orphan_created repoint ---------------------------------------------------

func TestOrphanCreated_StoresEditionAndRepointsRecomputedStranded(t *testing.T) {
	h := newStrandingHarness()
	// Projection: private → strand all non-author actives at repoint time.
	h.projections.st = atom_reuse.ProjectionState{RevisionID: strRev, OwnerGCID: strAuthor, ReuseVisibility: "private"}
	if err := h.sub.HandleAtomOrphanCreated(context.Background(), orphanCreatedEvent()); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(h.editions.puts) != 1 {
		t.Fatalf("edition not stored; puts = %v", h.editions.puts)
	}
	put := h.editions.puts[0]
	if put.AtomID != strAtom || put.SourceRevisionID != strRev || put.OrphanAtomID != strOrphan {
		t.Fatalf("edition = %+v", put)
	}
	if len(h.repointer.cmds) != 1 {
		t.Fatalf("repoint cmds = %d", len(h.repointer.cmds))
	}
	cmd := h.repointer.cmds[0]
	if len(cmd.Stranded) != 2 || cmd.OrphanAtomID != strOrphan || cmd.OriginalAtomID != strAtom {
		t.Fatalf("cmd = %+v", cmd)
	}
}

func TestOrphanCreated_ReWidenedInFlight_KeepsLiveReferences(t *testing.T) {
	h := newStrandingHarness()
	// Author re-widened to tenant before the orphan_created landed — nobody
	// is stranded any more; consumers keep the live reference while shared.
	h.projections.st = atom_reuse.ProjectionState{RevisionID: strRev, OwnerGCID: strAuthor, ReuseVisibility: "tenant"}
	if err := h.sub.HandleAtomOrphanCreated(context.Background(), orphanCreatedEvent()); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(h.editions.puts) != 1 {
		t.Fatalf("edition must still be stored (future withdrawals repoint locally)")
	}
	if len(h.repointer.cmds) != 0 {
		t.Fatalf("re-widened atom must repoint nothing; cmds = %+v", h.repointer.cmds)
	}
}

func TestOrphanCreated_ArchivedOrMissingProjection_StrandsAll(t *testing.T) {
	// Archived projection.
	h := newStrandingHarness()
	h.projections.st = atom_reuse.ProjectionState{RevisionID: strRev, OwnerGCID: strAuthor, ReuseVisibility: "tenant", Archived: true}
	if err := h.sub.HandleAtomOrphanCreated(context.Background(), orphanCreatedEvent()); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(h.repointer.cmds) != 1 || len(h.repointer.cmds[0].Stranded) != 2 {
		t.Fatalf("archived projection must strand all non-author; cmds = %+v", h.repointer.cmds)
	}

	// Missing projection (never published) — same treatment.
	h2 := newStrandingHarness()
	h2.projections.err = atom_reuse.ErrProjectionNotFound
	if err := h2.sub.HandleAtomOrphanCreated(context.Background(), orphanCreatedEvent()); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(h2.repointer.cmds) != 1 || len(h2.repointer.cmds[0].Stranded) != 2 {
		t.Fatalf("missing projection must strand all non-author; cmds = %+v", h2.repointer.cmds)
	}
}

func TestOrphanCreated_FriendsAudience_RecomputesWithCurrentFriendSet(t *testing.T) {
	h := newStrandingHarness()
	h.projections.st = atom_reuse.ProjectionState{RevisionID: strRev, OwnerGCID: strAuthor, ReuseVisibility: "friends"}
	h.friends.set = []string{strCarol} // carol became a friend since detection
	if err := h.sub.HandleAtomOrphanCreated(context.Background(), orphanCreatedEvent()); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(h.repointer.cmds) != 1 || len(h.repointer.cmds[0].Stranded) != 1 {
		t.Fatalf("only bob should strand (carol is now a friend); cmds = %+v", h.repointer.cmds)
	}
	if h.repointer.cmds[0].Stranded[0].GranteeGCID != strBob {
		t.Fatalf("stranded = %+v", h.repointer.cmds[0].Stranded)
	}
}

func TestOrphanCreated_DuplicateDelivery_Skips(t *testing.T) {
	h := newStrandingHarness()
	ev := orphanCreatedEvent()
	if err := h.sub.HandleAtomOrphanCreated(context.Background(), ev); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := h.sub.HandleAtomOrphanCreated(context.Background(), ev); err != nil {
		t.Fatalf("redelivery must ack: %v", err)
	}
	if len(h.repointer.cmds) != 1 {
		t.Fatalf("redelivery must not re-sweep; cmds = %d", len(h.repointer.cmds))
	}
}

func TestOrphanCreated_Validation(t *testing.T) {
	h := newStrandingHarness()
	ev := orphanCreatedEvent()
	ev.OrphanAtomID = ""
	if err := h.sub.HandleAtomOrphanCreated(context.Background(), ev); err == nil {
		t.Fatalf("missing orphan_atom_id must NACK")
	}
	ev2 := orphanCreatedEvent()
	ev2.TenantID = ""
	if err := h.sub.HandleAtomOrphanCreated(context.Background(), ev2); err == nil {
		t.Fatalf("missing tenant must NACK")
	}
}

// --- validation + decode coverage ---------------------------------------------------

func TestStranding_ArchivedValidation(t *testing.T) {
	h := newStrandingHarness()
	ev := archivedEvent()
	ev.AtomID = ""
	if err := h.sub.HandleAtomArchivedStranding(context.Background(), ev); err == nil {
		t.Fatalf("missing atom_id must NACK")
	}
	ev2 := archivedEvent()
	ev2.TenantID = ""
	if err := h.sub.HandleAtomArchivedStranding(context.Background(), ev2); err == nil {
		t.Fatalf("missing tenant must NACK")
	}
	// Duplicate archived delivery skips.
	h2 := newStrandingHarness()
	if err := h2.sub.HandleAtomArchivedStranding(context.Background(), archivedEvent()); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := h2.sub.HandleAtomArchivedStranding(context.Background(), archivedEvent()); err != nil {
		t.Fatalf("redelivery must ack: %v", err)
	}
	if len(h2.requirer.reqs) != 1 {
		t.Fatalf("redelivery must not double-publish; reqs = %d", len(h2.requirer.reqs))
	}
	// Missing deps preflight loud.
	empty := NewAtomReuseStrandingSubscriber(AtomReuseStrandingConfig{})
	if err := empty.HandleAtomArchivedStranding(context.Background(), archivedEvent()); err == nil {
		t.Fatalf("missing deps must fail loud")
	}
	if err := empty.HandleReuseVisibilityChanged(context.Background(), visibilityEvent("tenant", "private")); err == nil {
		t.Fatalf("missing deps must fail loud")
	}
	if err := empty.HandleAtomOrphanCreated(context.Background(), orphanCreatedEvent()); err == nil {
		t.Fatalf("missing deps must fail loud")
	}
}

func TestStranding_RepointerFailure_NACKs(t *testing.T) {
	h := newStrandingHarness()
	h.editions.known = true
	h.editions.edition = atom_reuse.OrphanEdition{
		AtomID: strAtom, SourceRevisionID: strRev, OrphanAtomID: strOrphan, OrphanedAt: time.Now().UTC(),
	}
	h.repointer.err = errors.New("db down")
	if err := h.sub.HandleReuseVisibilityChanged(context.Background(), visibilityEvent("tenant", "private")); err == nil {
		t.Fatalf("local repoint failure must NACK")
	}
	h2 := newStrandingHarness()
	h2.repointer.err = errors.New("db down")
	if err := h2.sub.HandleAtomOrphanCreated(context.Background(), orphanCreatedEvent()); err == nil {
		t.Fatalf("orphan_created repoint failure must NACK")
	}
}

func TestDecodeAtomOrphanCreatedWithAttrs_Binary(t *testing.T) {
	msg := &creationv1.AtomOrphanCreated{
		Envelope: &commonv1.EventEnvelope{
			EventId:  strEventID,
			TenantId: strTenant,
			Gcid:     strAuthor,
		},
		OrphanAtomId:       strOrphan,
		OrphanedFromAtomId: strAtom,
		SourceRevisionId:   strRev,
		AuthorGcid:         strAuthor,
		Trigger:            "archived",
	}
	bz, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	env, err := DecodeAtomOrphanCreatedWithAttrs(bz, nil)
	if err != nil {
		t.Fatalf("DecodeAtomOrphanCreatedWithAttrs: %v", err)
	}
	if env.EventID != strEventID || env.TenantID != strTenant || env.GCID != strAuthor {
		t.Fatalf("envelope fields: %+v", env)
	}
	if env.OrphanAtomID != strOrphan || env.OrphanedFromAtomID != strAtom || env.SourceRevisionID != strRev {
		t.Fatalf("payload ids: %+v", env)
	}
	if env.AuthorGCID != strAuthor || env.Trigger != "archived" {
		t.Fatalf("author/trigger: %+v", env)
	}
}
