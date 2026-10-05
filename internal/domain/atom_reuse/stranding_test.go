// stranding_test.go — ADR-229 Amendment A1 (CHO-2132): the pure-domain
// stranding classification. RED-first per strict TDD.
//
// A withdrawal strands the consumers left OUTSIDE the new audience:
//   - narrow-to-private / archive → every non-author grant
//     (author self-grants — grantee == owner — are never stranded),
//   - tenant → friends            → grants whose grantee is NOT currently a
//     friend of the author,
//   - widening / same-value       → nobody.
package atom_reuse

import (
	"testing"
	"time"
)

const (
	author = "01970000-0000-7000-8000-0000000000aa"
	bob    = "01970000-0000-7000-8000-0000000000b0"
	carol  = "01970000-0000-7000-8000-0000000000c0"
)

func grants() []GrantRef {
	return []GrantRef{
		{GrantID: "g-author", GranteeGCID: author, OwnerGCID: author, Scope: "collection"},
		{GrantID: "g-bob", GranteeGCID: bob, OwnerGCID: author, Scope: "test_set"},
		{GrantID: "g-carol", GranteeGCID: carol, OwnerGCID: author, Scope: "duel"},
	}
}

func ids(refs []GrantRef) []string {
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.GrantID)
	}
	return out
}

func TestNarrowed_Classification(t *testing.T) {
	cases := []struct {
		prev, next string
		want       bool
	}{
		{"tenant", "private", true},
		{"tenant", "friends", true},
		{"friends", "private", true},
		{"private", "tenant", false},  // widening
		{"private", "friends", false}, // widening
		{"friends", "tenant", false},  // widening
		{"tenant", "tenant", false},   // no-op
		{"", "private", false},        // unclassifiable — caller must fail loud separately
		{"tenant", "", false},
		{"bogus", "private", false},
		{"tenant", "bogus", false},
	}
	for _, c := range cases {
		if got := Narrowed(c.prev, c.next); got != c.want {
			t.Errorf("Narrowed(%q, %q) = %v, want %v", c.prev, c.next, got, c.want)
		}
	}
}

func TestValidAudience(t *testing.T) {
	for _, s := range []string{"private", "friends", "tenant"} {
		if !ValidAudience(s) {
			t.Errorf("ValidAudience(%q) = false", s)
		}
	}
	for _, s := range []string{"", "Public", "TENANT", " private"} {
		if ValidAudience(s) {
			t.Errorf("ValidAudience(%q) = true", s)
		}
	}
}

func TestStrandedByAudience_PrivateStrandsAllNonAuthor(t *testing.T) {
	got := StrandedByAudience(grants(), AudiencePrivate, nil)
	if len(got) != 2 {
		t.Fatalf("stranded = %v, want bob+carol", ids(got))
	}
	for _, g := range got {
		if g.GranteeGCID == author {
			t.Fatalf("author self-grant must never strand: %v", g)
		}
	}
}

func TestStrandedByAudience_FriendsKeepsFriends(t *testing.T) {
	friends := map[string]bool{bob: true}
	got := StrandedByAudience(grants(), AudienceFriends, friends)
	if len(got) != 1 || got[0].GranteeGCID != carol {
		t.Fatalf("stranded = %v, want carol only (bob is a friend)", ids(got))
	}
}

func TestStrandedByAudience_TenantStrandsNobody(t *testing.T) {
	if got := StrandedByAudience(grants(), AudienceTenant, nil); len(got) != 0 {
		t.Fatalf("tenant audience must strand nobody, got %v", ids(got))
	}
}

func TestStrandedByArchive_AllNonAuthor(t *testing.T) {
	got := StrandedByArchive(grants())
	if len(got) != 2 {
		t.Fatalf("stranded = %v, want bob+carol", ids(got))
	}
}

func TestStranded_EmptyGrants(t *testing.T) {
	if got := StrandedByAudience(nil, AudiencePrivate, nil); len(got) != 0 {
		t.Fatalf("no grants → no stranding, got %v", got)
	}
	if got := StrandedByArchive(nil); len(got) != 0 {
		t.Fatalf("no grants → no stranding, got %v", got)
	}
}

// Unknown audience labels classify as strand-NOBODY (the caller validates
// labels loud before classification; this is defence-in-depth against a
// silent over-strand).
func TestStrandedByAudience_UnknownAudienceStrandsNobody(t *testing.T) {
	if got := StrandedByAudience(grants(), "bogus", nil); len(got) != 0 {
		t.Fatalf("unknown audience must strand nobody (fail-safe), got %v", ids(got))
	}
}

func TestTriggers_Canonical(t *testing.T) {
	if TriggerNarrowed != "narrowed" || TriggerUnshared != "unshared" || TriggerArchived != "archived" {
		t.Fatalf("trigger literals drifted from the proto contract")
	}
}

func TestOrphanEdition_Validate(t *testing.T) {
	ok := OrphanEdition{
		AtomID:           "01970000-0000-7000-8000-0000000000a1",
		SourceRevisionID: "01970000-0000-7000-8000-0000000000f1",
		OrphanAtomID:     "01970000-0000-7000-8000-0000000000e1",
		OrphanedAt:       time.Now().UTC(),
	}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid edition rejected: %v", err)
	}
	for _, bad := range []OrphanEdition{
		{SourceRevisionID: "r", OrphanAtomID: "o"},
		{AtomID: "a", OrphanAtomID: "o"},
		{AtomID: "a", SourceRevisionID: "r"},
	} {
		if err := bad.Validate(); err == nil {
			t.Fatalf("edition %+v must fail validation", bad)
		}
	}
}

func TestOrphanRequest_Validate(t *testing.T) {
	valid := OrphanRequest{
		AtomID:        "01970000-0000-7000-8000-0000000000a1",
		RevisionID:    "01970000-0000-7000-8000-0000000000f1",
		Trigger:       TriggerNarrowed,
		StrandedCount: 1,
		DetectedAt:    time.Now().UTC(),
		ActorGCID:     author,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	for _, tr := range []string{TriggerNarrowed, TriggerUnshared, TriggerArchived} {
		r := valid
		r.Trigger = tr
		if err := r.Validate(); err != nil {
			t.Fatalf("trigger %q rejected: %v", tr, err)
		}
	}

	missingAtom := valid
	missingAtom.AtomID = "  "
	if err := missingAtom.Validate(); err == nil {
		t.Fatalf("missing atom_id must refuse")
	}
	badTrigger := valid
	badTrigger.Trigger = "widened"
	if err := badTrigger.Validate(); err == nil {
		t.Fatalf("invalid trigger must refuse")
	}
	zeroStranded := valid
	zeroStranded.StrandedCount = 0
	if err := zeroStranded.Validate(); err == nil {
		t.Fatalf("zero-stranded request must refuse (emit nothing)")
	}
}
