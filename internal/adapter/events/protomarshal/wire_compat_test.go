// wire_compat_test verifies the binary bytes emitted by MarshalPayload parse
// cleanly into the generated proto types from chora-contracts. This is the
// load-bearing assertion — if these tests pass, the bytes are wire-compatible
// with the canonical event schemas.
//
// The events-flat schemas (in chora-contracts/proto/events-flat/sharing/*)
// are wire-compatible with the non-flat generated bindings (in
// chora-contracts/gen/go/chora/sharing/v1) because field numbers + types
// are identical — only the import structure differs.
package protomarshal_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	sharingv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/sharing/v1"

	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protomarshal"
)

func wireCompatEnvelope() protomarshal.Envelope {
	t0 := time.Date(2026, 5, 16, 9, 30, 0, 0, time.UTC)
	return protomarshal.Envelope{
		EventID:        "01971b00-0000-7000-8000-000000000abc",
		IdempotencyKey: "idemp-wire-1",
		TenantID:       "tenant-acme",
		GCID:           "gcid-phyllis",
		OccurredAt:     t0,
		PublishedAt:    t0.Add(time.Millisecond),
		Traceparent:    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		Tracestate:     "vendor=value",
		SourceProject:  "chora-489812",
		SourceService:  "chora-sharing",
		SchemaVersion:  1,
	}
}

func TestWireCompat_RelationshipFollowed_DecodesIntoGeneratedType(t *testing.T) {
	env := wireCompatEnvelope()
	payload := map[string]any{
		"follower_gcid":        "gcid-phyllis",
		"followee_gcid":        "gcid-maya",
		"followed_at":          env.OccurredAt,
		"chora_imda_dimension": "accountability",
		"imda_lifecycle_stage": "runtime",
	}

	bz, err := protomarshal.MarshalPayload("chora.sharing.relationship.followed.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}

	var msg sharingv1.RelationshipFollowed
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}

	if msg.GetEnvelope() == nil {
		t.Fatal("envelope not decoded")
	}
	if got := msg.GetEnvelope().GetEventId(); got != env.EventID {
		t.Fatalf("envelope.event_id: got %q want %q", got, env.EventID)
	}
	if got := msg.GetEnvelope().GetTenantId(); got != env.TenantID {
		t.Fatalf("envelope.tenant_id: got %q want %q", got, env.TenantID)
	}
	if got := msg.GetEnvelope().GetGcid(); got != env.GCID {
		t.Fatalf("envelope.gcid: got %q want %q", got, env.GCID)
	}
	if got := msg.GetEnvelope().GetTraceparent(); got != env.Traceparent {
		t.Fatalf("envelope.traceparent: got %q want %q", got, env.Traceparent)
	}
	if got := msg.GetEnvelope().GetSchemaVersion(); got != env.SchemaVersion {
		t.Fatalf("envelope.schema_version: got %d want %d", got, env.SchemaVersion)
	}
	if got := msg.GetEnvelope().GetChoraImdaDimension(); got != "accountability" {
		t.Fatalf("envelope.chora_imda_dimension: got %q", got)
	}
	if got := msg.GetEnvelope().GetImdaLifecycleStage(); got != "runtime" {
		t.Fatalf("envelope.imda_lifecycle_stage: got %q", got)
	}
	if got := msg.GetFollowerGcid(); got != "gcid-phyllis" {
		t.Fatalf("follower_gcid: got %q", got)
	}
	if got := msg.GetFolloweeGcid(); got != "gcid-maya" {
		t.Fatalf("followee_gcid: got %q", got)
	}
	if got := msg.GetFollowedAt(); got == nil || got.AsTime().Unix() != env.OccurredAt.Unix() {
		t.Fatalf("followed_at not round-tripped: %+v", got)
	}
}

func TestWireCompat_RelationshipBlocked_DecodesIntoGeneratedType(t *testing.T) {
	env := wireCompatEnvelope()
	payload := map[string]any{
		"blocker_gcid": "gcid-phyllis",
		"blocked_gcid": "gcid-maya",
		"blocked_at":   env.OccurredAt,
	}

	bz, err := protomarshal.MarshalPayload("chora.sharing.relationship.blocked.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}

	var msg sharingv1.RelationshipBlocked
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if got := msg.GetBlockerGcid(); got != "gcid-phyllis" {
		t.Fatalf("blocker_gcid: got %q", got)
	}
	if got := msg.GetBlockedGcid(); got != "gcid-maya" {
		t.Fatalf("blocked_gcid: got %q", got)
	}
	if got := msg.GetBlockedAt(); got == nil || got.AsTime().Unix() != env.OccurredAt.Unix() {
		t.Fatalf("blocked_at: %+v", got)
	}
}

func TestWireCompat_RelationshipFriendAccepted_DecodesIntoGeneratedType(t *testing.T) {
	env := wireCompatEnvelope()
	payload := map[string]any{
		"accepter_gcid":  "gcid-maya",
		"requester_gcid": "gcid-phyllis",
		"accepted_at":    env.OccurredAt,
	}

	bz, err := protomarshal.MarshalPayload("chora.sharing.relationship.friend_accepted.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}

	var msg sharingv1.RelationshipFriendAccepted
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if got := msg.GetAccepterGcid(); got != "gcid-maya" {
		t.Fatalf("accepter_gcid: got %q", got)
	}
	if got := msg.GetRequesterGcid(); got != "gcid-phyllis" {
		t.Fatalf("requester_gcid: got %q", got)
	}
	if got := msg.GetAcceptedAt(); got == nil || got.AsTime().Unix() != env.OccurredAt.Unix() {
		t.Fatalf("accepted_at: %+v", got)
	}
}

func TestWireCompat_PostCreated_DecodesIntoGeneratedType(t *testing.T) {
	env := wireCompatEnvelope()
	payload := map[string]any{
		"post_id":     "post-1",
		"author_gcid": "gcid-phyllis",
		"body":        "hello chora",
		"media_ids":   []string{"m-1", "m-2"},
		"visibility":  "tenant",
		"created_at":  env.OccurredAt,
	}

	bz, err := protomarshal.MarshalPayload("chora.sharing.post.created.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}

	var msg sharingv1.PostCreated
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if got := msg.GetPostId(); got != "post-1" {
		t.Fatalf("post_id: got %q", got)
	}
	if got := msg.GetAuthorGcid(); got != "gcid-phyllis" {
		t.Fatalf("author_gcid: got %q", got)
	}
	if got := msg.GetBody(); got != "hello chora" {
		t.Fatalf("body: got %q", got)
	}
	if got := msg.GetMediaIds(); len(got) != 2 || got[0] != "m-1" || got[1] != "m-2" {
		t.Fatalf("media_ids: got %v", got)
	}
	if got := msg.GetVisibility(); got != sharingv1.PostVisibility_POST_VISIBILITY_TENANT_ONLY {
		t.Fatalf("visibility: got %v (want TENANT_ONLY=3)", got)
	}
	if got := msg.GetCreatedAt(); got == nil || got.AsTime().Unix() != env.OccurredAt.Unix() {
		t.Fatalf("created_at: %+v", got)
	}
}

func TestWireCompat_ReactionAdded_DecodesIntoGeneratedType(t *testing.T) {
	env := wireCompatEnvelope()
	payload := map[string]any{
		"reaction_id":   "rx-1",
		"target_id":     "post-1",
		"actor_gcid":    "gcid-phyllis",
		"reaction_type": "insightful",
		"created_at":    env.OccurredAt,
	}

	bz, err := protomarshal.MarshalPayload("chora.sharing.reaction.added.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}

	var msg sharingv1.ReactionAdded
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if got := msg.GetReactionId(); got != "rx-1" {
		t.Fatalf("reaction_id: got %q", got)
	}
	if got := msg.GetTargetPostId(); got != "post-1" {
		t.Fatalf("target_post_id: got %q", got)
	}
	if got := msg.GetActorGcid(); got != "gcid-phyllis" {
		t.Fatalf("actor_gcid: got %q", got)
	}
	if got := msg.GetReactionType(); got != sharingv1.ReactionType_REACTION_TYPE_INSIGHTFUL {
		t.Fatalf("reaction_type: got %v (want INSIGHTFUL=2)", got)
	}
	if got := msg.GetCreatedAt(); got == nil || got.AsTime().Unix() != env.OccurredAt.Unix() {
		t.Fatalf("created_at: %+v", got)
	}
}

func TestWireCompat_ReactionRemoved_DecodesIntoGeneratedType(t *testing.T) {
	env := wireCompatEnvelope()
	payload := map[string]any{
		"reaction_id":   "rx-1",
		"target_id":     "post-1",
		"actor_gcid":    "gcid-phyllis",
		"reaction_type": "like",
		"removed_at":    env.OccurredAt,
	}

	bz, err := protomarshal.MarshalPayload("chora.sharing.reaction.removed.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}

	var msg sharingv1.ReactionRemoved
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if got := msg.GetReactionType(); got != sharingv1.ReactionType_REACTION_TYPE_LIKE {
		t.Fatalf("reaction_type: got %v (want LIKE=1)", got)
	}
	if got := msg.GetTargetPostId(); got != "post-1" {
		t.Fatalf("target_post_id: got %q", got)
	}
}

// Atom-target reactions: payload uses target_id with target_type=atom; on the
// wire the atom-id flows into target_post_id. Generated bindings can decode
// either way — subscribers learn it's an atom via lookup, not the schema.
func TestWireCompat_ReactionAdded_AtomTarget_DecodesIntoGeneratedType(t *testing.T) {
	env := wireCompatEnvelope()
	payload := map[string]any{
		"reaction_id":   "rx-2",
		"target_type":   "atom",
		"target_id":     "atom-99",
		"actor_gcid":    "gcid-phyllis",
		"reaction_type": "curious",
	}

	bz, err := protomarshal.MarshalPayload("chora.sharing.reaction.added.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	var msg sharingv1.ReactionAdded
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if got := msg.GetTargetPostId(); got != "atom-99" {
		t.Fatalf("target_post_id (atom slot): got %q want atom-99", got)
	}
	if got := msg.GetReactionType(); got != sharingv1.ReactionType_REACTION_TYPE_CURIOUS {
		t.Fatalf("reaction_type: got %v", got)
	}
}
