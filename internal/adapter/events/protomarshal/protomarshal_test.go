// Package protomarshal_test verifies binary protobuf wire-format encoding for
// chora-sharing's outbox event payloads. RED tests written BEFORE the encoder
// lands per CLAUDE.md §development-execution + feedback_strict_tdd.
//
// Gap: outbox writer was persisting JSON-marshalled payload bytes that the
// binary wire contract rejects at publish time with "Invalid binary proto
// message". Fix is producer-side: marshal to canonical proto wire bytes
// before the outbox row is written. Dispatcher passes bytes through
// unchanged.
package protomarshal_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protomarshal"
)

// fixedEnvelope returns an envelope with deterministic values for byte-level
// assertions.
func fixedEnvelope() protomarshal.Envelope {
	t := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	return protomarshal.Envelope{
		EventID:        "01971b00-0000-7000-8000-000000000001",
		IdempotencyKey: "idemp-sharing-1",
		TenantID:       "tenant-acme",
		GCID:           "gcid-phyllis",
		OccurredAt:     t,
		PublishedAt:    t.Add(time.Millisecond),
		Traceparent:    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		Tracestate:     "",
		SourceProject:  "chora-489812",
		SourceService:  "chora-sharing",
		SchemaVersion:  1,
	}
}

// walkTopLevel walks every TLV in bz, asserts wire validity, and returns the
// set of field numbers seen. Failures call t.Fatalf with the offset.
func walkTopLevel(t *testing.T, bz []byte) map[protowire.Number]bool {
	t.Helper()
	seen := map[protowire.Number]bool{}
	rem := bz
	for len(rem) > 0 {
		num, typ, n := protowire.ConsumeTag(rem)
		if n < 0 {
			t.Fatalf("invalid tag at offset %d", len(bz)-len(rem))
		}
		rem = rem[n:]
		seen[num] = true
		switch typ {
		case protowire.BytesType:
			_, m := protowire.ConsumeBytes(rem)
			if m < 0 {
				t.Fatalf("invalid length-delimited value for field %d", num)
			}
			rem = rem[m:]
		case protowire.VarintType:
			_, m := protowire.ConsumeVarint(rem)
			if m < 0 {
				t.Fatalf("invalid varint value for field %d", num)
			}
			rem = rem[m:]
		default:
			t.Fatalf("unexpected wire type %d for field %d", typ, num)
		}
	}
	return seen
}

// -----------------------------------------------------------------------------
// Relationship spine (chora.sharing.relationship.*.v1, ADR-230 D4)
//
//	1  bytes  Envelope envelope
//	2  string <actor gcid>
//	3  string <subject gcid>
//	4  bytes  Timestamp <event timestamp>
// -----------------------------------------------------------------------------

// relationshipPayloads mirrors pg.relationshipPayloadKeys — the (actor,
// subject, timestamp) key names per topic; every topic shares wire fields
// 1 (envelope), 2 (actor), 3 (subject), 4 (timestamp).
var relationshipPayloads = map[string][3]string{
	"chora.sharing.relationship.followed.v1":         {"follower_gcid", "followee_gcid", "followed_at"},
	"chora.sharing.relationship.unfollowed.v1":       {"follower_gcid", "followee_gcid", "unfollowed_at"},
	"chora.sharing.relationship.friend_requested.v1": {"requester_gcid", "addressee_gcid", "requested_at"},
	"chora.sharing.relationship.friend_accepted.v1":  {"accepter_gcid", "requester_gcid", "accepted_at"},
	"chora.sharing.relationship.friend_removed.v1":   {"remover_gcid", "removed_gcid", "removed_at"},
	"chora.sharing.relationship.blocked.v1":          {"blocker_gcid", "blocked_gcid", "blocked_at"},
	"chora.sharing.relationship.unblocked.v1":        {"unblocker_gcid", "unblocked_gcid", "unblocked_at"},
}

func TestMarshalRelationship_AllSevenTopicsRoundTripBinaryProto(t *testing.T) {
	env := fixedEnvelope()
	for topic, keys := range relationshipPayloads {
		payload := map[string]any{
			keys[0]: "gcid-phyllis",
			keys[1]: "gcid-maya",
			keys[2]: env.OccurredAt,
		}
		bz, err := protomarshal.MarshalPayload(topic, env, payload)
		if err != nil {
			t.Fatalf("MarshalPayload(%s): %v", topic, err)
		}
		if len(bz) == 0 {
			t.Fatalf("%s: empty bytes", topic)
		}
		seen := walkTopLevel(t, bz)
		for _, want := range []protowire.Number{1, 2, 3, 4} {
			if !seen[want] {
				t.Fatalf("%s missing field %d (seen=%v)", topic, want, seen)
			}
		}
	}
}

// Relationship events tolerate a missing timestamp key (fields 1 + 2 + 3
// still encode).
func TestMarshalRelationshipFollowed_NoTimestampStillEncodes(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"follower_gcid": "gcid-phyllis",
		"followee_gcid": "gcid-maya",
	}
	bz, err := protomarshal.MarshalPayload("chora.sharing.relationship.followed.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	seen := walkTopLevel(t, bz)
	for _, want := range []protowire.Number{1, 2, 3} {
		if !seen[want] {
			t.Fatalf("RelationshipFollowed missing field %d", want)
		}
	}
}

// The legacy follow.* family is RETIRED (ADR-230 D4) — encoding it must fail
// loud so a stray legacy publisher dead-letters instead of shipping bytes no
// schema validates.
func TestMarshalFollowFamily_RetiredFailsLoud(t *testing.T) {
	env := fixedEnvelope()
	for _, topic := range []string{
		"chora.sharing.follow.created.v1",
		"chora.sharing.follow.removed.v1",
		"chora.sharing.follow.blocked.v1",
	} {
		_, err := protomarshal.MarshalPayload(topic, env, map[string]any{})
		if !protomarshal.IsUnsupportedTopic(err) {
			t.Fatalf("%s must be unsupported after retirement; got %v", topic, err)
		}
	}
}

// -----------------------------------------------------------------------------
// PostCreated (chora.sharing.post.created.v1)
//
//	1  bytes  Envelope envelope
//	2  string post_id
//	3  string author_gcid
//	4  string body
//	5  repeated string media_ids
//	6  varint PostVisibility visibility
//	7  string parent_post_id
//	8  string group_id
//	9  bytes  Timestamp created_at
// -----------------------------------------------------------------------------

func TestMarshalPostCreated_RoundTripsBinaryProto(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"post_id":     "post-1",
		"author_gcid": "gcid-phyllis",
		"body":        "hello chora",
		"media_ids":   []string{"media-1", "media-2"},
		"visibility":  "public",
		"atom_id":     "atom-1",
		"created_at":  env.OccurredAt,
	}

	bz, err := protomarshal.MarshalPayload("chora.sharing.post.created.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	seen := walkTopLevel(t, bz)
	for _, want := range []protowire.Number{1, 2, 3, 4, 6, 9} {
		if !seen[want] {
			t.Fatalf("PostCreated missing field %d (seen=%v)", want, seen)
		}
	}
}

// PostCreated maps the "tenant_only" visibility string to the
// POST_VISIBILITY_TENANT_ONLY (3) enum value at the wire level.
func TestMarshalPostCreated_VisibilityEnumMapping(t *testing.T) {
	env := fixedEnvelope()
	for _, tc := range []struct {
		input     string
		wantValue uint64
	}{
		{"public", 1},
		{"followers", 2},
		{"followers_only", 2},
		{"tenant", 3},
		{"tenant_only", 3},
		{"group", 4},
		{"group_only", 4},
		{"unknown", 0},
		{"", 0},
	} {
		t.Run(tc.input, func(t *testing.T) {
			payload := map[string]any{
				"post_id":    "p-1",
				"visibility": tc.input,
			}
			bz, err := protomarshal.MarshalPayload("chora.sharing.post.created.v1", env, payload)
			if err != nil {
				t.Fatalf("MarshalPayload: %v", err)
			}

			// Walk to find field 6 — varint — and assert its value.
			gotValue, found := findVarintField(bz, 6)
			if tc.wantValue == 0 {
				if found {
					t.Fatalf("expected visibility field 6 to be omitted for %q, got value=%d", tc.input, gotValue)
				}
				return
			}
			if !found {
				t.Fatalf("expected visibility field 6 present for %q", tc.input)
			}
			if gotValue != tc.wantValue {
				t.Fatalf("visibility %q: got enum value %d, want %d", tc.input, gotValue, tc.wantValue)
			}
		})
	}
}

// findVarintField returns the value of the first occurrence of the given
// varint field number in bz, or (0,false) if not found.
func findVarintField(bz []byte, target protowire.Number) (uint64, bool) {
	rem := bz
	for len(rem) > 0 {
		num, typ, n := protowire.ConsumeTag(rem)
		if n < 0 {
			return 0, false
		}
		rem = rem[n:]
		switch typ {
		case protowire.VarintType:
			v, m := protowire.ConsumeVarint(rem)
			if m < 0 {
				return 0, false
			}
			if num == target {
				return v, true
			}
			rem = rem[m:]
		case protowire.BytesType:
			_, m := protowire.ConsumeBytes(rem)
			if m < 0 {
				return 0, false
			}
			rem = rem[m:]
		default:
			return 0, false
		}
	}
	return 0, false
}

// -----------------------------------------------------------------------------
// ReactionAdded (chora.sharing.reaction.added.v1)
//
//	1  bytes  Envelope envelope
//	2  string reaction_id
//	3  string target_post_id
//	4  string actor_gcid
//	5  varint ReactionType reaction_type
//	6  bytes  Timestamp created_at
// -----------------------------------------------------------------------------

func TestMarshalReactionAdded_RoundTripsBinaryProto(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"reaction_id":   "rx-1",
		"target_id":     "post-1",
		"target_type":   "post",
		"actor_gcid":    "gcid-phyllis",
		"reaction_type": "like",
		"created_at":    env.OccurredAt,
	}

	bz, err := protomarshal.MarshalPayload("chora.sharing.reaction.added.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	seen := walkTopLevel(t, bz)
	for _, want := range []protowire.Number{1, 2, 3, 4, 5, 6} {
		if !seen[want] {
			t.Fatalf("ReactionAdded missing field %d (seen=%v)", want, seen)
		}
	}
}

// ReactionAdded enum mapping — "like"=1, "insightful"=2, "curious"=3,
// "cheer"=4, "celebrate"=5, unknown=0.
func TestMarshalReactionAdded_ReactionTypeEnumMapping(t *testing.T) {
	env := fixedEnvelope()
	for _, tc := range []struct {
		input     string
		wantValue uint64
	}{
		{"like", 1},
		{"insightful", 2},
		{"curious", 3},
		{"cheer", 4},
		{"celebrate", 5},
		{"unknown", 0},
		{"", 0},
	} {
		t.Run(tc.input, func(t *testing.T) {
			payload := map[string]any{
				"reaction_id":   "rx-1",
				"target_id":     "post-1",
				"reaction_type": tc.input,
			}
			bz, err := protomarshal.MarshalPayload("chora.sharing.reaction.added.v1", env, payload)
			if err != nil {
				t.Fatalf("MarshalPayload: %v", err)
			}
			gotValue, found := findVarintField(bz, 5)
			if tc.wantValue == 0 {
				if found {
					t.Fatalf("expected reaction_type field 5 to be omitted for %q, got value=%d", tc.input, gotValue)
				}
				return
			}
			if !found {
				t.Fatalf("expected reaction_type field 5 present for %q", tc.input)
			}
			if gotValue != tc.wantValue {
				t.Fatalf("reaction_type %q: got %d, want %d", tc.input, gotValue, tc.wantValue)
			}
		})
	}
}

// Atom-target reactions: payload arrives with target_type="atom" + target_id;
// schema has only target_post_id. Encoder routes target_id through field 3
// regardless of target_type so the row remains schema-compliant. Subscribers
// distinguish atom vs post via envelope or follow-on lookup.
func TestMarshalReactionAdded_AtomTargetFlowsToTargetPostID(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"reaction_id":   "rx-2",
		"target_type":   "atom",
		"target_id":     "atom-1",
		"actor_gcid":    "gcid-phyllis",
		"reaction_type": "insightful",
	}
	bz, err := protomarshal.MarshalPayload("chora.sharing.reaction.added.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	// field 3 = target_post_id — must be the atom-1 string.
	got, found := findStringField(bz, 3)
	if !found {
		t.Fatal("target_post_id (field 3) missing")
	}
	if got != "atom-1" {
		t.Fatalf("target_post_id: got %q, want atom-1", got)
	}
}

// findStringField returns the first occurrence of a length-delimited string
// field at the top level of bz.
func findStringField(bz []byte, target protowire.Number) (string, bool) {
	rem := bz
	for len(rem) > 0 {
		num, typ, n := protowire.ConsumeTag(rem)
		if n < 0 {
			return "", false
		}
		rem = rem[n:]
		switch typ {
		case protowire.BytesType:
			val, m := protowire.ConsumeBytes(rem)
			if m < 0 {
				return "", false
			}
			if num == target {
				return string(val), true
			}
			rem = rem[m:]
		case protowire.VarintType:
			_, m := protowire.ConsumeVarint(rem)
			if m < 0 {
				return "", false
			}
			rem = rem[m:]
		default:
			return "", false
		}
	}
	return "", false
}

// -----------------------------------------------------------------------------
// ReactionRemoved (chora.sharing.reaction.removed.v1)
//
//	1  bytes  Envelope envelope
//	2  string reaction_id
//	3  string target_post_id
//	4  string actor_gcid
//	5  varint ReactionType reaction_type
//	6  bytes  Timestamp removed_at
// -----------------------------------------------------------------------------

func TestMarshalReactionRemoved_RoundTripsBinaryProto(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"target_id":     "post-1",
		"actor_gcid":    "gcid-phyllis",
		"reaction_type": "like",
		"removed_at":    env.OccurredAt,
	}

	bz, err := protomarshal.MarshalPayload("chora.sharing.reaction.removed.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	seen := walkTopLevel(t, bz)
	for _, want := range []protowire.Number{1, 3, 4, 5, 6} {
		if !seen[want] {
			t.Fatalf("ReactionRemoved missing field %d (seen=%v)", want, seen)
		}
	}
}

// -----------------------------------------------------------------------------
// Envelope is wire-compatible with chora.common.v1.EventEnvelope
// -----------------------------------------------------------------------------

func TestMarshalRelationshipFollowed_EnvelopeIsWireCompatible(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"follower_gcid": "gcid-phyllis",
		"followee_gcid": "gcid-maya",
	}

	bz, err := protomarshal.MarshalPayload("chora.sharing.relationship.followed.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}

	// Field 1 must be the envelope submessage.
	num, typ, n := protowire.ConsumeTag(bz)
	if num != 1 || typ != protowire.BytesType || n < 0 {
		t.Fatalf("expected envelope tag (1, bytes), got num=%d typ=%d", num, typ)
	}
	envBytes, m := protowire.ConsumeBytes(bz[n:])
	if m < 0 || len(envBytes) == 0 {
		t.Fatal("empty envelope")
	}

	seen := map[protowire.Number]bool{}
	rem := envBytes
	for len(rem) > 0 {
		num, typ, n := protowire.ConsumeTag(rem)
		if n < 0 {
			t.Fatal("invalid envelope inner tag")
		}
		rem = rem[n:]
		seen[num] = true
		switch typ {
		case protowire.BytesType:
			_, m := protowire.ConsumeBytes(rem)
			if m < 0 {
				t.Fatalf("invalid envelope bytes field %d", num)
			}
			rem = rem[m:]
		case protowire.VarintType:
			_, m := protowire.ConsumeVarint(rem)
			if m < 0 {
				t.Fatalf("invalid envelope varint field %d", num)
			}
			rem = rem[m:]
		}
	}

	// event_id (1), idempotency_key (2), tenant_id (3), gcid (4),
	// occurred_at (5), published_at (6), traceparent (7),
	// source_project (9), source_service (10), schema_version (11).
	for _, want := range []protowire.Number{1, 2, 3, 4, 5, 6, 7, 9, 10, 11} {
		if !seen[want] {
			t.Fatalf("envelope missing field %d (seen=%v)", want, seen)
		}
	}
}

// -----------------------------------------------------------------------------
// IMDA evidence projection — envelope fields 14 + 15 sourced from payload.
// -----------------------------------------------------------------------------

func TestMarshalRelationshipFollowed_IMDAEvidenceProjectedFromPayload(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"follower_gcid":        "gcid-phyllis",
		"followee_gcid":        "gcid-maya",
		"chora_imda_dimension": "accountability",
		"imda_lifecycle_stage": "runtime",
	}
	bz, err := protomarshal.MarshalPayload("chora.sharing.relationship.followed.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}

	// Drill into envelope (field 1) and assert fields 14 + 15 carry the strings.
	_, typ, n := protowire.ConsumeTag(bz)
	if typ != protowire.BytesType {
		t.Fatal("expected envelope as first field")
	}
	envBytes, _ := protowire.ConsumeBytes(bz[n:])

	dim, ok := findStringField(envBytes, 14)
	if !ok || dim != "accountability" {
		t.Fatalf("envelope.chora_imda_dimension: got %q ok=%v", dim, ok)
	}
	stage, ok := findStringField(envBytes, 15)
	if !ok || stage != "runtime" {
		t.Fatalf("envelope.imda_lifecycle_stage: got %q ok=%v", stage, ok)
	}
}

// -----------------------------------------------------------------------------
// Unsupported topic — fails loud.
// -----------------------------------------------------------------------------

func TestMarshal_UnknownTopic_FailsLoud(t *testing.T) {
	env := fixedEnvelope()
	_, err := protomarshal.MarshalPayload("chora.sharing.unwired.thing.v1", env, nil)
	if err == nil {
		t.Fatal("expected ErrUnsupportedTopic, got nil")
	}
	if !protomarshal.IsUnsupportedTopic(err) {
		t.Fatalf("expected IsUnsupportedTopic(true), got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Nil payload → envelope-only valid message.
// -----------------------------------------------------------------------------

func TestMarshal_NilPayload_ProducesEnvelopeOnly(t *testing.T) {
	env := fixedEnvelope()
	for _, topic := range []string{
		"chora.sharing.relationship.followed.v1",
		"chora.sharing.relationship.blocked.v1",
		"chora.sharing.post.created.v1",
		"chora.sharing.reaction.added.v1",
		"chora.sharing.reaction.removed.v1",
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

// -----------------------------------------------------------------------------
// Invalid payload types fail loud — no silent coercion.
// -----------------------------------------------------------------------------

func TestMarshalPostCreated_RejectsNonStringPostID(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"post_id": 12345, // schema demands string
	}
	_, err := protomarshal.MarshalPayload("chora.sharing.post.created.v1", env, payload)
	if err == nil {
		t.Fatal("expected typed error for post_id=int")
	}
}

func TestMarshalPostCreated_RejectsCreatedAtAsString(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"post_id":    "p-1",
		"created_at": "not-a-time",
	}
	_, err := protomarshal.MarshalPayload("chora.sharing.post.created.v1", env, payload)
	if err == nil {
		t.Fatal("expected typed error for created_at=string")
	}
}

func TestMarshalReactionAdded_RejectsReactionTypeAsInt(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"reaction_id":   "rx-1",
		"target_id":     "p-1",
		"reaction_type": 12345, // schema demands string for the encoder mapping
	}
	_, err := protomarshal.MarshalPayload("chora.sharing.reaction.added.v1", env, payload)
	if err == nil {
		t.Fatal("expected typed error for reaction_type=int")
	}
}

// -----------------------------------------------------------------------------
// PostCreated media_ids accepts []string and []any (JSON-decoded shape).
// -----------------------------------------------------------------------------

func TestMarshalPostCreated_MediaIDsAcceptInterfaceSlice(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"post_id":     "p-1",
		"author_gcid": "g-1",
		"media_ids":   []any{"m-1", "m-2"},
	}
	bz, err := protomarshal.MarshalPayload("chora.sharing.post.created.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	if len(bz) == 0 {
		t.Fatal("empty bytes")
	}
}

// -----------------------------------------------------------------------------
// Timestamp pointer payloads accepted.
// -----------------------------------------------------------------------------

func TestMarshalRelationshipFollowed_TimestampPointerAccepted(t *testing.T) {
	env := fixedEnvelope()
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
	payload := map[string]any{
		"follower_gcid": "gcid-phyllis",
		"followee_gcid": "gcid-maya",
		"followed_at":   &now,
	}
	bz, err := protomarshal.MarshalPayload("chora.sharing.relationship.followed.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload with *time.Time: %v", err)
	}
	if len(bz) == 0 {
		t.Fatal("empty bytes")
	}
}
