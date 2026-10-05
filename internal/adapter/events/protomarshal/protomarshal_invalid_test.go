// protomarshal_invalid_test.go — fail-loud error paths + loose-coercion edge
// branches for the encoders. Mirrors the style of protomarshal_test.go's
// "Invalid payload types fail loud — no silent coercion" section: every
// string/bytes/numeric slot must reject wrong Go types, and each coercion
// helper's branches (int/int32/int64/float32/json.Number, empty-string skips,
// nil pointers) are exercised through the exported MarshalPayload surface.
package protomarshal_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	sharingv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/sharing/v1"

	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protomarshal"
)

// -----------------------------------------------------------------------------
// writeStringField — empty strings are omitted, non-strings fail loud. Driven
// through relationship topics (actor/subject) and the atom family.
// -----------------------------------------------------------------------------

func TestMarshalRelationshipFollowed_EmptyStringsOmitted(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"follower_gcid": "",
		"followee_gcid": "",
	}
	bz, err := protomarshal.MarshalPayload("chora.sharing.relationship.followed.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	seen := walkTopLevel(t, bz)
	if seen[2] || seen[3] {
		t.Fatalf("empty actor/subject strings must be omitted: %v", seen)
	}
}

func TestMarshalRelationshipFollowed_RejectsNonStringActor(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.relationship.followed.v1", env, map[string]any{
		"follower_gcid": 123,
	})
	if err == nil {
		t.Fatal("expected typed error for follower_gcid=int")
	}
}

func TestMarshalRelationshipFollowed_RejectsNonTimeTimestamp(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.relationship.followed.v1", env, map[string]any{
		"follower_gcid": "g-1",
		"followee_gcid": "g-2",
		"followed_at":   "not-a-time",
	})
	if err == nil {
		t.Fatal("expected typed error for followed_at=string")
	}
}

// Subject slot (field 3) rejects non-strings — the mirror of the actor slot.
func TestMarshalRelationshipFollowed_RejectsNonStringSubject(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.relationship.followed.v1", env, map[string]any{
		"follower_gcid": "g-1",
		"followee_gcid": 123,
	})
	if err == nil {
		t.Fatal("expected typed error for followee_gcid=int")
	}
}

// -----------------------------------------------------------------------------
// encodePostCreated remaining branches: media_ids element/type handling and
// visibility type checks.
// -----------------------------------------------------------------------------

func TestMarshalPostCreated_MediaIDsEmptyElementSkipped(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"post_id":   "p-1",
		"media_ids": []any{"", "m-1", ""},
	}
	bz, err := protomarshal.MarshalPayload("chora.sharing.post.created.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	var msg sharingv1.PostCreated
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if got := msg.GetMediaIds(); len(got) != 1 || got[0] != "m-1" {
		t.Fatalf("media_ids: got %v, want [m-1]", got)
	}
}

func TestMarshalPostCreated_RejectsMediaIDsWrongType(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.post.created.v1", env, map[string]any{
		"post_id":   "p-1",
		"media_ids": "not-a-slice",
	})
	if err == nil {
		t.Fatal("expected typed error for media_ids=string")
	}

	_, err = protomarshal.MarshalPayload("chora.sharing.post.created.v1", env, map[string]any{
		"post_id":   "p-1",
		"media_ids": []any{"m-1", 42},
	})
	if err == nil {
		t.Fatal("expected typed error for media_ids containing non-string")
	}
}

func TestMarshalPostCreated_RejectsVisibilityAsInt(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.post.created.v1", env, map[string]any{
		"post_id":    "p-1",
		"visibility": 2,
	})
	if err == nil {
		t.Fatal("expected typed error for visibility=int")
	}
}

// -----------------------------------------------------------------------------
// AtomReuseOrphanRequired — asInt32 coercion of int / int64 payload shapes and
// the zero-skip.
// -----------------------------------------------------------------------------

func TestMarshalAtomReuseOrphanRequired_IntShapesAndZeroSkip(t *testing.T) {
	env := fixedEnvelope()
	detected := time.Date(2026, 7, 11, 10, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name string
		n    any
		want uint64
	}{
		{"int", int(2), 2},
		{"int64", int64(3), 3},
		{"zero", int32(0), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := map[string]any{
				"atom_id":              "a-1",
				"revision_id":          "r-1",
				"stranded_grant_count": tc.n,
				"detected_at":          detected,
			}
			bz, err := protomarshal.MarshalPayload("chora.sharing.atom_reuse.orphan_required.v1", env, payload)
			if err != nil {
				t.Fatalf("MarshalPayload: %v", err)
			}
			got, found := findVarintField(bz, 5)
			if tc.want == 0 {
				if found {
					t.Fatalf("zero stranded_grant_count must be omitted, got %d", got)
				}
				return
			}
			if !found {
				t.Fatalf("stranded_grant_count field 5 missing")
			}
			if got != tc.want {
				t.Fatalf("stranded_grant_count: got %d, want %d", got, tc.want)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Reaction target/actor double-key resolution — empty + non-string values fall
// through to the alternate key; wrong-typed keys fail loud.
// -----------------------------------------------------------------------------

func TestMarshalReactionAdded_TargetEmptyFallsThroughToAlternateKey(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"reaction_id":   "rx-1",
		"target_post_id": "",
		"target_id":     "post-9",
	}
	bz, err := protomarshal.MarshalPayload("chora.sharing.reaction.added.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	got, found := findStringField(bz, 3)
	if !found || got != "post-9" {
		t.Fatalf("target_post_id: got %q found=%v, want post-9", got, found)
	}
}

func TestMarshalReactionAdded_TargetNonStringFallsThroughToAlternateKey(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"reaction_id":    "rx-1",
		"target_post_id": 123,
		"target_id":      "post-10",
	}
	bz, err := protomarshal.MarshalPayload("chora.sharing.reaction.added.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	got, found := findStringField(bz, 3)
	if !found || got != "post-10" {
		t.Fatalf("target_post_id: got %q found=%v, want post-10", got, found)
	}
}

func TestMarshalReactionAdded_RejectsNonStringTargetPostID(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.reaction.added.v1", env, map[string]any{
		"reaction_id":    "rx-1",
		"target_post_id": 123,
	})
	if err == nil {
		t.Fatal("expected typed error for target_post_id=int")
	}
}

func TestMarshalReactionAdded_RejectsNonStringTargetID(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.reaction.added.v1", env, map[string]any{
		"reaction_id": "rx-1",
		"target_id":   123,
	})
	if err == nil {
		t.Fatal("expected typed error for target_id=int")
	}
}

func TestMarshalReactionAdded_RejectsNonStringActorGcid(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.reaction.added.v1", env, map[string]any{
		"reaction_id":  "rx-1",
		"target_id":    "p-1",
		"actor_gcid":   123,
	})
	if err == nil {
		t.Fatal("expected typed error for actor_gcid=int")
	}
}

func TestMarshalReactionAdded_RejectsNonStringGcid(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.reaction.added.v1", env, map[string]any{
		"reaction_id": "rx-1",
		"target_id":   "p-1",
		"gcid":        123,
	})
	if err == nil {
		t.Fatal("expected typed error for gcid=int")
	}
}

// Empty actor_gcid falls through to the legacy gcid key.
func TestMarshalReactionAdded_EmptyActorFallsThroughToGcid(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"reaction_id":   "rx-1",
		"target_id":     "p-1",
		"actor_gcid":    "",
		"gcid":          "gcid-phyllis",
		"reaction_type": "like",
	}
	bz, err := protomarshal.MarshalPayload("chora.sharing.reaction.added.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	got, found := findStringField(bz, 4)
	if !found || got != "gcid-phyllis" {
		t.Fatalf("actor_gcid: got %q found=%v, want gcid-phyllis", got, found)
	}
}

// An empty target_id resolves to ("",false) in lookupString and then trips the
// fail-loud "present but not a usable string" path — the Publisher never sends
// an empty target, so this is a genuine producer error.
func TestMarshalReactionAdded_EmptyTargetIDFailsLoud(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.reaction.added.v1", env, map[string]any{
		"reaction_id": "rx-1",
		"target_id":   "",
	})
	if err == nil {
		t.Fatal("expected typed error for empty target_id")
	}
}

// -----------------------------------------------------------------------------
// writeTimestampField / asTime — nil + nil-pointer timestamps fail loud.
// -----------------------------------------------------------------------------

func TestMarshalReactionAdded_RejectsNilTimestamp(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.reaction.added.v1", env, map[string]any{
		"reaction_id": "rx-1",
		"created_at":  nil,
	})
	if err == nil {
		t.Fatal("expected typed error for created_at=nil")
	}
}

func TestMarshalReactionAdded_RejectsNilTimestampPointer(t *testing.T) {
	env := fixedEnvelope()
	var nilTime *time.Time
	_, err := protomarshal.MarshalPayload("chora.sharing.reaction.added.v1", env, map[string]any{
		"reaction_id": "rx-1",
		"created_at":  nilTime,
	})
	if err == nil {
		t.Fatal("expected typed error for created_at=(*time.Time)(nil)")
	}
}

// -----------------------------------------------------------------------------
// AtomShared / AtomLicensed fail-loud paths.
// -----------------------------------------------------------------------------

func TestMarshalAtomShared_RejectsNonStringShareEntryID(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.atom.shared.v1", env, map[string]any{
		"share_entry_id": 123,
	})
	if err == nil {
		t.Fatal("expected typed error for share_entry_id=int")
	}
}

func TestMarshalAtomShared_RejectsNonMapRoyaltyRate(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.atom.shared.v1", env, map[string]any{
		"share_entry_id": "se-1",
		"royalty_rate":   "not-a-map",
	})
	if err == nil {
		t.Fatal("expected typed error for royalty_rate=string")
	}
}

func TestMarshalAtomLicensed_RejectsNonStringGrantID(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.atom.licensed.v1", env, map[string]any{
		"grant_id": 123,
	})
	if err == nil {
		t.Fatal("expected typed error for grant_id=int")
	}
}

func TestMarshalAtomLicensed_RejectsNonMapRoyaltyRateSnapshot(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.atom.licensed.v1", env, map[string]any{
		"grant_id":              "g-1",
		"royalty_rate_snapshot": 42,
	})
	if err == nil {
		t.Fatal("expected typed error for royalty_rate_snapshot=int")
	}
}

// A bad json.Number in the royalty value is silently skipped (asFloat64
// reports not-ok) — the submessage still encodes, minus the value field.
func TestMarshalAtomShared_BadJSONNumberRoyaltyValueOmitsValue(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"share_entry_id": "se-1",
		"royalty_rate":   map[string]any{"kind": "ROYALTY_FIXED", "value": json.Number("not-a-number")},
	}
	bz, err := protomarshal.MarshalPayload("chora.sharing.atom.shared.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	var msg sharingv1.AtomShared
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	got := msg.GetRoyaltyRate()
	if got == nil || got.GetKind() != "ROYALTY_FIXED" {
		t.Fatalf("royalty_rate: got %+v", got)
	}
	if got.GetValue() != 0 {
		t.Fatalf("bad json.Number value must be omitted, got %v", got.GetValue())
	}
}

// -----------------------------------------------------------------------------
// AtomRevoked fail-loud paths.
// -----------------------------------------------------------------------------

func TestMarshalAtomRevoked_RejectsNonStringReason(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.atom.revoked.v1", env, map[string]any{
		"grant_id": "g-1",
		"reason":   123,
	})
	if err == nil {
		t.Fatal("expected typed error for reason=int")
	}
}

func TestMarshalAtomRevoked_RejectsNonTimeRevokedAt(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.atom.revoked.v1", env, map[string]any{
		"grant_id":   "g-1",
		"revoked_at": "not-a-time",
	})
	if err == nil {
		t.Fatal("expected typed error for revoked_at=string")
	}
}

// -----------------------------------------------------------------------------
// RoyaltySettled fail-loud paths — writeDoubleField rejects non-numeric.
// -----------------------------------------------------------------------------

func TestMarshalRoyaltySettled_RejectsNonNumericAmount(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.royalty.settled.v1", env, map[string]any{
		"settlement_id": "stl-1",
		"amount":        "12.5",
	})
	if err == nil {
		t.Fatal("expected typed error for amount=string")
	}
}

func TestMarshalRoyaltySettled_RejectsBadJSONNumberAmount(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.royalty.settled.v1", env, map[string]any{
		"settlement_id": "stl-1",
		"amount":        json.Number("not-a-number"),
	})
	if err == nil {
		t.Fatal("expected typed error for amount=json.Number(bad)")
	}
}

// -----------------------------------------------------------------------------
// LeaderboardUpdated fail-loud paths.
// -----------------------------------------------------------------------------

func TestMarshalLeaderboardUpdated_RejectsNonNumericDuelElo(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.leaderboard.updated.v1", env, map[string]any{
		"leaderboard_id": "lb-1",
		"duel_elo":       "1600",
	})
	if err == nil {
		t.Fatal("expected typed error for duel_elo=string")
	}
}

func TestMarshalLeaderboardUpdated_RejectsNonTimezoneUpdatedAt(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.leaderboard.updated.v1", env, map[string]any{
		"leaderboard_id": "lb-1",
		"updated_at":     "not-a-time",
	})
	if err == nil {
		t.Fatal("expected typed error for updated_at=string")
	}
}

func TestMarshalLeaderboardUpdated_RejectsNonMapTopEntry(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.leaderboard.updated.v1", env, map[string]any{
		"leaderboard_id": "lb-1",
		"top_entries":    []any{"not-a-map"},
	})
	if err == nil {
		t.Fatal("expected typed error for top_entries containing non-map")
	}
}

func TestMarshalLeaderboardUpdated_RejectsNonMapRankUp(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.leaderboard.updated.v1", env, map[string]any{
		"leaderboard_id": "lb-1",
		"rank_ups":       []any{42},
	})
	if err == nil {
		t.Fatal("expected typed error for rank_ups containing non-map")
	}
}

// -----------------------------------------------------------------------------
// IsUnsupportedTopic wrapping — non-wrapping errors come back false.
// -----------------------------------------------------------------------------

func TestIsUnsupportedTopic_NonMatchingError(t *testing.T) {
	err := errors.New("unrelated failure")
	if protomarshal.IsUnsupportedTopic(err) {
		t.Fatal("IsUnsupportedTopic must be false for unrelated errors")
	}
	if protomarshal.IsUnsupportedTopic(nil) {
		// errors.Is(nil, x) == false — the helper must mirror it.
		t.Fatal("IsUnsupportedTopic(nil) must be false")
	}
}
// -----------------------------------------------------------------------------
// Remaining writeStringField error branches — every string slot of every
// encoder rejects a wrong Go type (proto3 strings cannot accept int/bytes).
// Each case asserts the encoder fails loud on exactly one slot.
// -----------------------------------------------------------------------------

func TestMarshal_EveryStringSlotRejectsNonString(t *testing.T) {
	env := fixedEnvelope()
	for _, tc := range []struct {
		name    string
		topic   string
		field   string
		wrong   any
	}{
		{"post.author_gcid", "chora.sharing.post.created.v1", "author_gcid", 42},
		{"post.body", "chora.sharing.post.created.v1", "body", 42},
		{"post.parent_post_id", "chora.sharing.post.created.v1", "parent_post_id", 42},
		{"post.group_id", "chora.sharing.post.created.v1", "group_id", 42},
		{"reaction.reaction_id", "chora.sharing.reaction.added.v1", "reaction_id", 42},
		{"atom_shared.author_gcid", "chora.sharing.atom.shared.v1", "author_gcid", 42},
		{"atom_shared.author_display_name", "chora.sharing.atom.shared.v1", "author_display_name", 42},
		{"atom_shared.atom_id", "chora.sharing.atom.shared.v1", "atom_id", 42},
		{"atom_shared.atom_revision_id", "chora.sharing.atom.shared.v1", "atom_revision_id", 42},
		{"atom_shared.caption", "chora.sharing.atom.shared.v1", "caption", 42},
		{"atom_licensed.owner_gcid", "chora.sharing.atom.licensed.v1", "owner_gcid", 42},
		{"atom_licensed.grantee_gcid", "chora.sharing.atom.licensed.v1", "grantee_gcid", 42},
		{"atom_licensed.atom_id", "chora.sharing.atom.licensed.v1", "atom_id", 42},
		{"atom_licensed.atom_revision_id", "chora.sharing.atom.licensed.v1", "atom_revision_id", 42},
		{"atom_revoked.grant_id", "chora.sharing.atom.revoked.v1", "grant_id", 42},
		{"atom_revoked.owner_gcid", "chora.sharing.atom.revoked.v1", "owner_gcid", 42},
		{"atom_revoked.grantee_gcid", "chora.sharing.atom.revoked.v1", "grantee_gcid", 42},
		{"atom_revoked.atom_id", "chora.sharing.atom.revoked.v1", "atom_id", 42},
		{"atom_revoked.revoked_by_gcid", "chora.sharing.atom.revoked.v1", "revoked_by_gcid", 42},
		{"royalty.settlement_id", "chora.sharing.royalty.settled.v1", "settlement_id", 42},
		{"royalty.grant_id", "chora.sharing.royalty.settled.v1", "grant_id", 42},
		{"royalty.owner_gcid", "chora.sharing.royalty.settled.v1", "owner_gcid", 42},
		{"royalty.grantee_tenant_id", "chora.sharing.royalty.settled.v1", "grantee_tenant_id", 42},
		{"royalty.atom_id", "chora.sharing.royalty.settled.v1", "atom_id", 42},
		{"royalty.source_event_id", "chora.sharing.royalty.settled.v1", "source_event_id", 42},
		{"leaderboard.leaderboard_id", "chora.sharing.leaderboard.updated.v1", "leaderboard_id", 42},
		{"leaderboard.scope_id", "chora.sharing.leaderboard.updated.v1", "scope_id", 42},
		{"leaderboard.season_id", "chora.sharing.leaderboard.updated.v1", "season_id", 42},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := protomarshal.MarshalPayload(tc.topic, env, map[string]any{tc.field: tc.wrong})
			if err == nil {
				t.Fatalf("expected typed error for %s=%T", tc.field, tc.wrong)
			}
		})
	}
}

// Orphan-required revision_id + trigger slots reject non-strings.
func TestMarshalAtomReuseOrphanRequired_RemainingStringSlotsRejectNonString(t *testing.T) {
	env := fixedEnvelope()
	for _, field := range []string{"revision_id", "trigger"} {
		_, err := protomarshal.MarshalPayload("chora.sharing.atom_reuse.orphan_required.v1", env, map[string]any{
			field: 42,
		})
		if err == nil {
			t.Fatalf("expected typed error for %s=int", field)
		}
	}
}

// media_ids: nil is "present but unusable" — stringSlice reports not-ok and
// the encoder fails loud rather than silently dropping the field.
func TestMarshalPostCreated_RejectsNilMediaIDs(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.post.created.v1", env, map[string]any{
		"post_id":   "p-1",
		"media_ids": nil,
	})
	if err == nil {
		t.Fatal("expected typed error for media_ids=nil")
	}
}

// Nil payload on the five extended topics — envelope-only valid message.
func TestMarshal_ExtendedTopicsNilPayload_ProducesEnvelopeOnly(t *testing.T) {
	env := fixedEnvelope()
	for _, topic := range []string{
		"chora.sharing.atom_reuse.orphan_required.v1",
		"chora.sharing.atom.shared.v1",
		"chora.sharing.atom.licensed.v1",
		"chora.sharing.atom.revoked.v1",
		"chora.sharing.royalty.settled.v1",
		"chora.sharing.leaderboard.updated.v1",
	} {
		t.Run(topic, func(t *testing.T) {
			bz, err := protomarshal.MarshalPayload(topic, env, nil)
			if err != nil {
				t.Fatalf("nil payload: %v", err)
			}
			if len(bz) == 0 {
				t.Fatal("nil payload should still emit envelope bytes")
			}
			num, _, n := protowire.ConsumeTag(bz)
			if n < 0 || num != 1 {
				t.Fatalf("expected leading field=1 (envelope), got %d", num)
			}
		})
	}
}

// leaderboard atoms_completed rejects non-numeric (writeInt32Field error).
func TestMarshalLeaderboardUpdated_RejectsNonNumericAtomsCompleted(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.leaderboard.updated.v1", env, map[string]any{
		"leaderboard_id":  "lb-1",
		"atoms_completed": "42",
	})
	if err == nil {
		t.Fatal("expected typed error for atoms_completed=string")
	}
}
