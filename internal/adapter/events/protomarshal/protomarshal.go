// Package protomarshal encodes chora-sharing outbox event payloads to
// canonical binary protobuf wire format — the canonical wire shape on the
// event bus.
//
// Why hand-rolled
// ---------------
// We use google.golang.org/protobuf/encoding/protowire to emit canonical
// wire bytes for the exact subset of fields each event schema expects, with
// zero dependency on the generated bindings. This keeps the adapter
// independent of chora-contracts codegen lifecycle and decouples the
// producer from the binding's import-graph surface.
//
// Field numbers + wire types are pinned to chora-contracts/proto/events-flat/
// sharing/* — those flat protos ARE the canonical event schemas.
//
// Invariants per the binary-encoded event protos:
//
//   - Field 1 = envelope (length-delimited nested message)
//   - Envelope nested fields 1..15 follow chora.common.v1.EventEnvelope layout
//   - Timestamps are nested messages: int64 seconds (field 1) + int32 nanos
//     (field 2)
//   - Unknown topics fail loud (ErrUnsupportedTopic) so the dispatcher
//     dead-letters rather than retrying forever against a schema mismatch
//
// Wire format MUST be binary protobuf for event-bus topics. JSON encoding is
// rejected at publish time with "Invalid binary proto message".
//
// Mirrors the chora-consumption protomarshal adapter — the codebase-wide
// JSON→binary-protobuf fix surfaced in task #33.
package protomarshal

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

// Envelope is the producer-side flat shape of chora.common.v1.EventEnvelope
// that the encoder needs. Mirrors services/chora-sharing/internal/adapter/
// events.Envelope; defined locally to keep this package import-cycle-free.
type Envelope struct {
	EventID        string
	IdempotencyKey string
	TenantID       string
	GCID           string
	OccurredAt     time.Time
	PublishedAt    time.Time
	Traceparent    string
	Tracestate     string
	SourceProject  string
	SourceService  string
	SchemaVersion  int32
}

// ErrUnsupportedTopic is returned by MarshalPayload when the topic has no
// registered binary encoder. The dispatcher should dead-letter rows that
// surface this error rather than retrying forever — the row is structurally
// incompatible with its destination schema.
var ErrUnsupportedTopic = errors.New("protomarshal: topic has no binary encoder; outbox row will dead-letter")

// IsUnsupportedTopic reports whether err is (or wraps) ErrUnsupportedTopic.
func IsUnsupportedTopic(err error) bool { return errors.Is(err, ErrUnsupportedTopic) }

// MarshalPayload converts a topic + envelope + loose payload map into the
// canonical binary protobuf wire bytes for that topic's Schema Registry
// schema. Returns ErrUnsupportedTopic if no encoder is registered for the
// supplied topic.
//
// Topics with registered encoders:
//
//   - chora.sharing.relationship.{followed,unfollowed,friend_requested,
//     friend_accepted,friend_removed,blocked,unblocked}.v1 (ADR-230 D4)
//   - chora.sharing.post.created.v1
//   - chora.sharing.reaction.added.v1
//   - chora.sharing.reaction.removed.v1
//   - chora.sharing.atom.shared.v1
//   - chora.sharing.atom.licensed.v1
//   - chora.sharing.atom.revoked.v1
//   - chora.sharing.royalty.settled.v1
//   - chora.sharing.leaderboard.updated.v1
//
// The legacy chora.sharing.follow.{created,removed}.v1 encoders are RETIRED
// (ADR-230 D4 — the family was declared but never published; the
// relationship spine supersedes it).
//
// Adding more topics: append a case to the switch + implement
// encode{X}(env, payload) returning the wire bytes.
func MarshalPayload(topic string, env Envelope, payload map[string]any) ([]byte, error) {
	if keys, ok := relationshipWireKeys[topic]; ok {
		return encodeRelationship(env, payload, keys)
	}
	switch topic {
	case "chora.sharing.atom_reuse.orphan_required.v1":
		return encodeAtomReuseOrphanRequired(env, payload)
	case "chora.sharing.post.created.v1":
		return encodePostCreated(env, payload)
	case "chora.sharing.reaction.added.v1":
		return encodeReactionAdded(env, payload)
	case "chora.sharing.reaction.removed.v1":
		return encodeReactionRemoved(env, payload)
	case "chora.sharing.atom.shared.v1":
		return encodeAtomShared(env, payload)
	case "chora.sharing.atom.licensed.v1":
		return encodeAtomLicensed(env, payload)
	case "chora.sharing.atom.revoked.v1":
		return encodeAtomRevoked(env, payload)
	case "chora.sharing.royalty.settled.v1":
		return encodeRoyaltySettled(env, payload)
	case "chora.sharing.leaderboard.updated.v1":
		return encodeLeaderboardUpdated(env, payload)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedTopic, topic)
	}
}

// -----------------------------------------------------------------------------
// Relationship spine (chora.sharing.relationship.*.v1, ADR-230 D4)
// Field layout — chora-contracts/proto/events-flat/sharing/relationship/*.proto
// (every message shares the same wire shape; the field NAMES differ per event
// for documentation, the NUMBERS are pinned):
//
//	1  bytes  Envelope envelope
//	2  string <actor gcid>    (follower / requester / accepter / remover / blocker / unblocker)
//	3  string <subject gcid>  (followee / addressee / requester / removed / blocked / unblocked)
//	4  bytes  Timestamp <event timestamp>
// -----------------------------------------------------------------------------

// relationshipWireKeys maps each relationship topic to its (actor, subject,
// timestamp) payload keys. One shared shape = one Schema Registry schema
// (chora-sharing-relationship-v1) validates all seven topics.
var relationshipWireKeys = map[string][3]string{
	"chora.sharing.relationship.followed.v1":         {"follower_gcid", "followee_gcid", "followed_at"},
	"chora.sharing.relationship.unfollowed.v1":       {"follower_gcid", "followee_gcid", "unfollowed_at"},
	"chora.sharing.relationship.friend_requested.v1": {"requester_gcid", "addressee_gcid", "requested_at"},
	"chora.sharing.relationship.friend_accepted.v1":  {"accepter_gcid", "requester_gcid", "accepted_at"},
	"chora.sharing.relationship.friend_removed.v1":   {"remover_gcid", "removed_gcid", "removed_at"},
	"chora.sharing.relationship.blocked.v1":          {"blocker_gcid", "blocked_gcid", "blocked_at"},
	"chora.sharing.relationship.unblocked.v1":        {"unblocker_gcid", "unblocked_gcid", "unblocked_at"},
}

// encodeRelationship emits the shared relationship wire shape.
func encodeRelationship(env Envelope, payload map[string]any, keys [3]string) ([]byte, error) {
	out := make([]byte, 0, 256)

	envBz, err := encodeEnvelope(env, payload)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	out = appendLengthDelimited(out, 1, envBz)

	if payload == nil {
		return out, nil
	}

	if err := writeStringField(&out, payload, 2, keys[0]); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 3, keys[1]); err != nil {
		return nil, err
	}
	if err := writeTimestampField(&out, payload, 4, keys[2]); err != nil {
		return nil, err
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// AtomReuseOrphanRequired (chora.sharing.atom_reuse.orphan_required.v1)
// Field layout — chora-contracts/proto/events-flat/sharing/atom_reuse/
// orphan_required.proto (ADR-229 Amendment A1, CHO-2132)
// -----------------------------------------------------------------------------
//
//	1  bytes  Envelope envelope
//	2  string atom_id
//	3  string revision_id
//	4  string trigger               (narrowed | unshared | archived)
//	5  varint int32 stranded_grant_count
//	6  bytes  Timestamp detected_at
func encodeAtomReuseOrphanRequired(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 256)

	envBz, err := encodeEnvelope(env, payload)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	out = appendLengthDelimited(out, 1, envBz)

	if payload == nil {
		return out, nil
	}

	if err := writeStringField(&out, payload, 2, "atom_id"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 3, "revision_id"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 4, "trigger"); err != nil {
		return nil, err
	}
	if raw, present := payload["stranded_grant_count"]; present {
		n, ok := asInt32(raw)
		if !ok {
			return nil, fmt.Errorf("field stranded_grant_count: expected int32/int, got %T", raw)
		}
		if n != 0 {
			out = appendVarint(out, 5, uint64(uint32(n)))
		}
	}
	if err := writeTimestampField(&out, payload, 6, "detected_at"); err != nil {
		return nil, err
	}
	return out, nil
}

// asInt32 coerces int-ish values to int32.
func asInt32(v any) (int32, bool) {
	switch n := v.(type) {
	case int32:
		return n, true
	case int:
		return int32(n), true
	case int64:
		return int32(n), true
	default:
		return 0, false
	}
}

// -----------------------------------------------------------------------------
// PostCreated (chora.sharing.post.created.v1)
// Field layout — chora-contracts/proto/events-flat/sharing/post/created.proto
// -----------------------------------------------------------------------------
//
//	1  bytes           Envelope envelope
//	2  string          post_id
//	3  string          author_gcid
//	4  string          body
//	5  repeated string media_ids
//	6  varint          PostVisibility visibility
//	7  string          parent_post_id
//	8  string          group_id
//	9  bytes           Timestamp created_at
func encodePostCreated(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 512)

	envBz, err := encodeEnvelope(env, payload)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	out = appendLengthDelimited(out, 1, envBz)

	if payload == nil {
		return out, nil
	}

	if err := writeStringField(&out, payload, 2, "post_id"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 3, "author_gcid"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 4, "body"); err != nil {
		return nil, err
	}

	if raw, present := payload["media_ids"]; present {
		ids, ok := stringSlice(raw)
		if !ok {
			return nil, fmt.Errorf("field media_ids: expected []string or []any-of-string, got %T", raw)
		}
		for _, id := range ids {
			if id == "" {
				continue
			}
			out = appendString(out, 5, id)
		}
	}

	if raw, present := payload["visibility"]; present {
		s, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("field visibility: expected string, got %T", raw)
		}
		if v := postVisibilityEnum(s); v != 0 {
			out = appendVarint(out, 6, uint64(v))
		}
	}

	if err := writeStringField(&out, payload, 7, "parent_post_id"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 8, "group_id"); err != nil {
		return nil, err
	}
	if err := writeTimestampField(&out, payload, 9, "created_at"); err != nil {
		return nil, err
	}

	return out, nil
}

// postVisibilityEnum maps the loose string supplied by chora-sharing's
// Publisher to the PostVisibility enum value in the events-flat schema:
//
//	POST_VISIBILITY_UNSPECIFIED   = 0
//	POST_VISIBILITY_PUBLIC        = 1
//	POST_VISIBILITY_FOLLOWERS_ONLY= 2
//	POST_VISIBILITY_TENANT_ONLY   = 3
//	POST_VISIBILITY_GROUP_ONLY    = 4
//
// Unknown strings collapse to UNSPECIFIED (0) which is omitted on the wire
// per proto3 default behaviour.
func postVisibilityEnum(s string) int32 {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "public":
		return 1
	case "followers", "followers_only":
		return 2
	case "tenant", "tenant_only":
		return 3
	case "group", "group_only":
		return 4
	default:
		return 0
	}
}

// -----------------------------------------------------------------------------
// ReactionAdded (chora.sharing.reaction.added.v1)
// Field layout — chora-contracts/proto/events-flat/sharing/reaction/added.proto
// -----------------------------------------------------------------------------
//
//	1  bytes  Envelope envelope
//	2  string reaction_id
//	3  string target_post_id
//	4  string actor_gcid
//	5  varint ReactionType reaction_type
//	6  bytes  Timestamp created_at
func encodeReactionAdded(env Envelope, payload map[string]any) ([]byte, error) {
	return encodeReactionLike(env, payload, "created_at")
}

// -----------------------------------------------------------------------------
// ReactionRemoved (chora.sharing.reaction.removed.v1)
// Field layout — chora-contracts/proto/events-flat/sharing/reaction/removed.proto
// -----------------------------------------------------------------------------
//
//	1  bytes  Envelope envelope
//	2  string reaction_id
//	3  string target_post_id
//	4  string actor_gcid
//	5  varint ReactionType reaction_type
//	6  bytes  Timestamp removed_at
func encodeReactionRemoved(env Envelope, payload map[string]any) ([]byte, error) {
	return encodeReactionLike(env, payload, "removed_at")
}

// encodeReactionLike shares the wire layout between ReactionAdded +
// ReactionRemoved. Producer payloads use `target_id` (with optional
// `target_type`) — both encoded into field 3 (target_post_id). The schema
// has no atom-vs-post discriminator; subscribers infer target type from the
// post-DB lookup or the envelope.
func encodeReactionLike(env Envelope, payload map[string]any, timestampKey string) ([]byte, error) {
	out := make([]byte, 0, 256)

	envBz, err := encodeEnvelope(env, payload)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	out = appendLengthDelimited(out, 1, envBz)

	if payload == nil {
		return out, nil
	}

	if err := writeStringField(&out, payload, 2, "reaction_id"); err != nil {
		return nil, err
	}

	// target_post_id (field 3) — accept either "target_post_id" or "target_id"
	// (the chora-sharing Publisher historically used "target_id" so it could
	// also carry atom targets).
	if raw, ok := lookupString(payload, "target_post_id", "target_id"); ok {
		if raw != "" {
			out = appendString(out, 3, raw)
		}
	} else if _, present := payload["target_post_id"]; present {
		return nil, fmt.Errorf("field target_post_id: expected string, got %T", payload["target_post_id"])
	} else if _, present := payload["target_id"]; present {
		return nil, fmt.Errorf("field target_id: expected string, got %T", payload["target_id"])
	}

	// actor_gcid (field 4) — accept either "actor_gcid" or "gcid" (the
	// chora-sharing Publisher uses "gcid" for the reacting learner).
	if raw, ok := lookupString(payload, "actor_gcid", "gcid"); ok {
		if raw != "" {
			out = appendString(out, 4, raw)
		}
	} else if _, present := payload["actor_gcid"]; present {
		return nil, fmt.Errorf("field actor_gcid: expected string, got %T", payload["actor_gcid"])
	} else if _, present := payload["gcid"]; present {
		// envelope.gcid is set separately — payload gcid is the actor.
		return nil, fmt.Errorf("field gcid (actor): expected string, got %T", payload["gcid"])
	}

	if raw, present := payload["reaction_type"]; present {
		s, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("field reaction_type: expected string, got %T", raw)
		}
		if v := reactionTypeEnum(s); v != 0 {
			out = appendVarint(out, 5, uint64(v))
		}
	}

	if err := writeTimestampField(&out, payload, 6, timestampKey); err != nil {
		return nil, err
	}

	return out, nil
}

// reactionTypeEnum maps the loose string supplied by chora-sharing's
// Publisher to the ReactionType enum value:
//
//	REACTION_TYPE_UNSPECIFIED = 0
//	REACTION_TYPE_LIKE        = 1
//	REACTION_TYPE_INSIGHTFUL  = 2
//	REACTION_TYPE_CURIOUS     = 3
//	REACTION_TYPE_CHEER       = 4
//	REACTION_TYPE_CELEBRATE   = 5
//
// `inspired` is accepted as a legacy alias for INSIGHTFUL (the chora-sharing
// publisher.go comment lists it among the documented values).
func reactionTypeEnum(s string) int32 {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "like":
		return 1
	case "insightful", "inspired":
		return 2
	case "curious":
		return 3
	case "cheer":
		return 4
	case "celebrate":
		return 5
	default:
		return 0
	}
}

// -----------------------------------------------------------------------------
// AtomShared (chora.sharing.atom.shared.v1)
// Field layout — chora-contracts/proto/events-flat/sharing/atom/shared.proto
// -----------------------------------------------------------------------------
//
//	1  bytes        Envelope envelope
//	2  string       share_entry_id
//	3  string       author_gcid
//	4  string       author_display_name
//	5  string       atom_id
//	6  string       atom_revision_id
//	7  string       caption
//	8  varint       LicenseTerms license_terms
//	9  bytes        RoyaltyRate royalty_rate
func encodeAtomShared(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 512)

	envBz, err := encodeEnvelope(env, payload)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	out = appendLengthDelimited(out, 1, envBz)

	if payload == nil {
		return out, nil
	}

	if err := writeStringField(&out, payload, 2, "share_entry_id"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 3, "author_gcid"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 4, "author_display_name"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 5, "atom_id"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 6, "atom_revision_id"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 7, "caption"); err != nil {
		return nil, err
	}
	if v := licenseTermsEnum(payload["license_terms"]); v != 0 {
		out = appendVarint(out, 8, uint64(v))
	}
	if rb, err := encodeRoyaltyRatePayload(payload); err != nil {
		return nil, err
	} else if rb != nil {
		out = appendLengthDelimited(out, 9, rb)
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// AtomLicensed (chora.sharing.atom.licensed.v1)
// Field layout — chora-contracts/proto/events-flat/sharing/atom/licensed.proto
// -----------------------------------------------------------------------------
//
//	1  bytes        Envelope envelope
//	2  string       grant_id
//	3  string       owner_gcid
//	4  string       grantee_gcid
//	5  string       atom_id
//	6  string       atom_revision_id
//	7  varint       GrantScope scope
//	8  varint       LicenseTerms license_terms_snapshot
//	9  bytes        RoyaltyRate royalty_rate_snapshot
func encodeAtomLicensed(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 512)

	envBz, err := encodeEnvelope(env, payload)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	out = appendLengthDelimited(out, 1, envBz)

	if payload == nil {
		return out, nil
	}

	if err := writeStringField(&out, payload, 2, "grant_id"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 3, "owner_gcid"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 4, "grantee_gcid"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 5, "atom_id"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 6, "atom_revision_id"); err != nil {
		return nil, err
	}
	if v := grantScopeEnum(payload["scope"]); v != 0 {
		out = appendVarint(out, 7, uint64(v))
	}
	if v := licenseTermsEnum(payload["license_terms_snapshot"]); v != 0 {
		out = appendVarint(out, 8, uint64(v))
	}
	if rb, err := encodeRoyaltyRatePayloadAt(payload, "royalty_rate_snapshot"); err != nil {
		return nil, err
	} else if rb != nil {
		out = appendLengthDelimited(out, 9, rb)
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// AtomGrantRevoked (chora.sharing.atom.revoked.v1)
// Field layout — chora-contracts/proto/events-flat/sharing/atom/revoked.proto
// -----------------------------------------------------------------------------
//
//	1  bytes        Envelope envelope
//	2  string       grant_id
//	3  string       owner_gcid
//	4  string       grantee_gcid
//	5  string       atom_id
//	6  string       revoked_by_gcid
//	7  string       reason
//	8  bytes        Timestamp revoked_at
func encodeAtomRevoked(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 512)

	envBz, err := encodeEnvelope(env, payload)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	out = appendLengthDelimited(out, 1, envBz)

	if payload == nil {
		return out, nil
	}

	if err := writeStringField(&out, payload, 2, "grant_id"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 3, "owner_gcid"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 4, "grantee_gcid"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 5, "atom_id"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 6, "revoked_by_gcid"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 7, "reason"); err != nil {
		return nil, err
	}
	if err := writeTimestampField(&out, payload, 8, "revoked_at"); err != nil {
		return nil, err
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// RoyaltySettled (chora.sharing.royalty.settled.v1)
// Field layout — chora-contracts/proto/events-flat/sharing/royalty/settled.proto
// -----------------------------------------------------------------------------
//
//	1  bytes        Envelope envelope
//	2  string       settlement_id
//	3  string       grant_id
//	4  string       owner_gcid
//	5  string       grantee_tenant_id
//	6  string       atom_id
//	7  double       amount
//	8  varint       RoyaltyCurrency currency
//	9  varint       RoyaltyUsageContext usage_context
//	10 string       source_event_id
func encodeRoyaltySettled(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 512)

	envBz, err := encodeEnvelope(env, payload)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	out = appendLengthDelimited(out, 1, envBz)

	if payload == nil {
		return out, nil
	}

	if err := writeStringField(&out, payload, 2, "settlement_id"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 3, "grant_id"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 4, "owner_gcid"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 5, "grantee_tenant_id"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 6, "atom_id"); err != nil {
		return nil, err
	}
	if err := writeDoubleField(&out, payload, 7, "amount"); err != nil {
		return nil, err
	}
	if v := royaltyCurrencyEnum(payload["currency"]); v != 0 {
		out = appendVarint(out, 8, uint64(v))
	}
	if v := royaltyUsageContextEnum(payload["usage_context"]); v != 0 {
		out = appendVarint(out, 9, uint64(v))
	}
	if err := writeStringField(&out, payload, 10, "source_event_id"); err != nil {
		return nil, err
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// LeaderboardUpdated (chora.sharing.leaderboard.updated.v1)
// Field layout — chora-contracts/proto/events-flat/sharing/leaderboard/updated.proto
// -----------------------------------------------------------------------------
//
//	1  bytes        Envelope envelope
//	2  string       leaderboard_id
//	3  varint       LeaderboardScope scope
//	4  string       scope_id
//	5  string       season_id
//	6  repeated     bytes LeaderboardEntry top_entries
//	7  bytes        Timestamp updated_at
//	8  varint       int32 duel_elo
//	9  varint       int32 atoms_completed
//	10 repeated     bytes RankUp rank_ups
func encodeLeaderboardUpdated(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 1024)

	envBz, err := encodeEnvelope(env, payload)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	out = appendLengthDelimited(out, 1, envBz)

	if payload == nil {
		return out, nil
	}

	if err := writeStringField(&out, payload, 2, "leaderboard_id"); err != nil {
		return nil, err
	}
	if v := leaderboardScopeEnum(payload["scope"]); v != 0 {
		out = appendVarint(out, 3, uint64(v))
	}
	if err := writeStringField(&out, payload, 4, "scope_id"); err != nil {
		return nil, err
	}
	if err := writeStringField(&out, payload, 5, "season_id"); err != nil {
		return nil, err
	}
	if entries, ok := anySlice(payload["top_entries"]); ok {
		for _, e := range entries {
			eb, err := encodeLeaderboardEntry(e)
			if err != nil {
				return nil, fmt.Errorf("field top_entries: %w", err)
			}
			out = appendLengthDelimited(out, 6, eb)
		}
	}
	if err := writeTimestampField(&out, payload, 7, "updated_at"); err != nil {
		return nil, err
	}
	if err := writeInt32Field(&out, payload, 8, "duel_elo"); err != nil {
		return nil, err
	}
	if err := writeInt32Field(&out, payload, 9, "atoms_completed"); err != nil {
		return nil, err
	}
	if ups, ok := anySlice(payload["rank_ups"]); ok {
		for _, u := range ups {
			ub, err := encodeRankUp(u)
			if err != nil {
				return nil, fmt.Errorf("field rank_ups: %w", err)
			}
			out = appendLengthDelimited(out, 10, ub)
		}
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// Enum coercion — maps loose payload values to proto3 enum ordinals. Unknown or
// zero values are omitted on the wire per proto3 default behaviour.
// -----------------------------------------------------------------------------

func licenseTermsEnum(v any) int32 {
	s, ok := v.(string)
	if !ok {
		return 0
	}
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "FREE", "LICENSE_TERMS_FREE":
		return 1
	case "ROYALTY_PCT", "LICENSE_TERMS_ROYALTY_PCT":
		return 2
	case "ROYALTY_FIXED", "LICENSE_TERMS_ROYALTY_FIXED":
		return 3
	case "CC_BY_SA", "LICENSE_TERMS_CC_BY_SA":
		return 4
	case "CC_ND", "LICENSE_TERMS_CC_ND":
		return 5
	default:
		return 0
	}
}

func grantScopeEnum(v any) int32 {
	s, ok := v.(string)
	if !ok {
		return 0
	}
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "TEST_SET", "GRANT_SCOPE_TEST_SET":
		return 1
	case "DUEL", "GRANT_SCOPE_DUEL":
		return 2
	case "LIVE_QUIZ", "GRANT_SCOPE_LIVE_QUIZ":
		return 3
	case "COLLECTION", "GRANT_SCOPE_COLLECTION":
		return 4
	case "UNLIMITED", "GRANT_SCOPE_UNLIMITED":
		return 5
	default:
		return 0
	}
}

func royaltyCurrencyEnum(v any) int32 {
	s, ok := v.(string)
	if !ok {
		return 0
	}
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "MANA", "ROYALTY_CURRENCY_MANA":
		return 1
	case "COINS", "ROYALTY_CURRENCY_COINS":
		return 2
	case "REPUTATION", "ROYALTY_CURRENCY_REPUTATION":
		return 3
	case "USD", "ROYALTY_CURRENCY_USD":
		return 4
	default:
		return 0
	}
}

func royaltyUsageContextEnum(v any) int32 {
	s, ok := v.(string)
	if !ok {
		return 0
	}
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "TEST_SET", "ROYALTY_USAGE_CONTEXT_TEST_SET":
		return 1
	case "DUEL", "ROYALTY_USAGE_CONTEXT_DUEL":
		return 2
	case "LIVE_QUIZ", "ROYALTY_USAGE_CONTEXT_LIVE_QUIZ":
		return 3
	default:
		return 0
	}
}

func leaderboardScopeEnum(v any) int32 {
	s, ok := v.(string)
	if !ok {
		return 0
	}
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "TENANT", "LEADERBOARD_SCOPE_TENANT":
		return 1
	case "COURSE", "LEADERBOARD_SCOPE_COURSE":
		return 2
	case "COHORT", "LEADERBOARD_SCOPE_COHORT":
		return 3
	case "GLOBAL", "LEADERBOARD_SCOPE_GLOBAL":
		return 4
	default:
		return 0
	}
}

// -----------------------------------------------------------------------------
// RoyaltyRate submessage encoder.
// -----------------------------------------------------------------------------

// encodeRoyaltyRatePayload extracts the "royalty_rate" map from the payload
// and encodes it as a nested RoyaltyRate submessage. Returns nil if absent.
func encodeRoyaltyRatePayload(payload map[string]any) ([]byte, error) {
	return encodeRoyaltyRatePayloadAt(payload, "royalty_rate")
}

func encodeRoyaltyRatePayloadAt(payload map[string]any, key string) ([]byte, error) {
	raw, present := payload[key]
	if !present || raw == nil {
		return nil, nil
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("field %s: expected map[string]any, got %T", key, raw)
	}
	out := make([]byte, 0, 32)
	if v, ok := m["kind"].(string); ok && v != "" {
		out = appendString(out, 1, v)
	}
	if v, ok := asFloat64(m["value"]); ok && v != 0 {
		out = appendFixed64(out, 2, math.Float64bits(v))
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// LeaderboardEntry + RankUp submessage encoders.
// -----------------------------------------------------------------------------

func encodeLeaderboardEntry(v any) ([]byte, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected map[string]any, got %T", v)
	}
	out := make([]byte, 0, 64)
	if s, ok := m["gcid"].(string); ok && s != "" {
		out = appendString(out, 1, s)
	}
	if v, ok := asInt64(m["score"]); ok && v != 0 {
		out = appendVarint(out, 2, uint64(v))
	}
	if v, ok := asInt32(m["rank"]); ok && v != 0 {
		out = appendVarint(out, 3, uint64(uint32(v)))
	}
	return out, nil
}

func encodeRankUp(v any) ([]byte, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected map[string]any, got %T", v)
	}
	out := make([]byte, 0, 64)
	if s, ok := m["gcid"].(string); ok && s != "" {
		out = appendString(out, 1, s)
	}
	if v, ok := asInt32(m["old_rank"]); ok && v != 0 {
		out = appendVarint(out, 2, uint64(uint32(v)))
	}
	if v, ok := asInt32(m["new_rank"]); ok && v != 0 {
		out = appendVarint(out, 3, uint64(uint32(v)))
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// Additional wire-format helpers.
// -----------------------------------------------------------------------------

func appendFixed64(b []byte, field protowire.Number, v uint64) []byte {
	b = protowire.AppendTag(b, field, protowire.Fixed64Type)
	b = protowire.AppendFixed64(b, v)
	return b
}

// writeRepeatedStringField writes payload[key] as a repeated string field.
func writeRepeatedStringField(out *[]byte, payload map[string]any, field protowire.Number, key string) error {
	raw, present := payload[key]
	if !present || raw == nil {
		return nil
	}
	ids, ok := stringSlice(raw)
	if !ok {
		return fmt.Errorf("field %s: expected []string or []any-of-string, got %T", key, raw)
	}
	for _, id := range ids {
		if id == "" {
			continue
		}
		*out = appendString(*out, field, id)
	}
	return nil
}

// writeFloatField writes payload[key] as a proto float (fixed32).
func writeFloatField(out *[]byte, payload map[string]any, field protowire.Number, key string) error {
	raw, present := payload[key]
	if !present {
		return nil
	}
	v, ok := asFloat64(raw)
	if !ok {
		return fmt.Errorf("field %s: expected numeric, got %T", key, raw)
	}
	if v == 0 {
		return nil
	}
	*out = protowire.AppendTag(*out, field, protowire.Fixed32Type)
	*out = protowire.AppendFixed32(*out, math.Float32bits(float32(v)))
	return nil
}

// writeDoubleField writes payload[key] as a proto double (fixed64).
func writeDoubleField(out *[]byte, payload map[string]any, field protowire.Number, key string) error {
	raw, present := payload[key]
	if !present {
		return nil
	}
	v, ok := asFloat64(raw)
	if !ok {
		return fmt.Errorf("field %s: expected numeric, got %T", key, raw)
	}
	if v == 0 {
		return nil
	}
	*out = protowire.AppendTag(*out, field, protowire.Fixed64Type)
	*out = protowire.AppendFixed64(*out, math.Float64bits(v))
	return nil
}

// writeInt32Field writes payload[key] as a varint-encoded int32.
func writeInt32Field(out *[]byte, payload map[string]any, field protowire.Number, key string) error {
	raw, present := payload[key]
	if !present {
		return nil
	}
	v, ok := asInt32(raw)
	if !ok {
		return fmt.Errorf("field %s: expected int, got %T", key, raw)
	}
	if v == 0 {
		return nil
	}
	*out = appendVarint(*out, field, uint64(uint32(v)))
	return nil
}

// writeBoolField writes payload[key] as a varint-encoded bool.
func writeBoolField(out *[]byte, payload map[string]any, field protowire.Number, key string) error {
	raw, present := payload[key]
	if !present {
		return nil
	}
	b, ok := raw.(bool)
	if !ok {
		return fmt.Errorf("field %s: expected bool, got %T", key, raw)
	}
	if !b {
		return nil
	}
	*out = appendVarint(*out, field, 1)
	return nil
}

// anySlice coerces []any, []map[string]any, or []string to []any.
func anySlice(v any) ([]any, bool) {
	if v == nil {
		return nil, false
	}
	switch s := v.(type) {
	case []any:
		return s, true
	case []map[string]any:
		out := make([]any, len(s))
		for i, e := range s {
			out[i] = e
		}
		return out, true
	default:
		return nil, false
	}
}

// asFloat64 coerces numeric types to float64.
func asFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

// asInt64 coerces numeric types to int64.
func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		return int64(n), true
	case float32:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	default:
		return 0, false
	}
}

// -----------------------------------------------------------------------------
// Envelope (nested in every event message; field layout matches
// chora.common.v1.EventEnvelope as flattened by chora-contracts/internal/
// protoflatten and embedded as a NESTED type in every events-flat schema).
// -----------------------------------------------------------------------------
//
//	1  string event_id
//	2  string idempotency_key
//	3  string tenant_id
//	4  string gcid
//	5  bytes  Timestamp occurred_at
//	6  bytes  Timestamp published_at
//	7  string traceparent
//	8  string tracestate
//	9  string source_project
//	10 string source_service
//	11 varint int32 schema_version
//	12 string correlation_id
//	13 string causation_id
//	14 string chora_imda_dimension
//	15 string imda_lifecycle_stage
func encodeEnvelope(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 256)

	if env.EventID != "" {
		out = appendString(out, 1, env.EventID)
	}
	if env.IdempotencyKey != "" {
		out = appendString(out, 2, env.IdempotencyKey)
	}
	if env.TenantID != "" {
		out = appendString(out, 3, env.TenantID)
	}
	if env.GCID != "" {
		out = appendString(out, 4, env.GCID)
	}
	if !env.OccurredAt.IsZero() {
		out = appendLengthDelimited(out, 5, encodeTimestamp(env.OccurredAt))
	}
	if !env.PublishedAt.IsZero() {
		out = appendLengthDelimited(out, 6, encodeTimestamp(env.PublishedAt))
	}
	if env.Traceparent != "" {
		out = appendString(out, 7, env.Traceparent)
	}
	if env.Tracestate != "" {
		out = appendString(out, 8, env.Tracestate)
	}
	if env.SourceProject != "" {
		out = appendString(out, 9, env.SourceProject)
	}
	if env.SourceService != "" {
		out = appendString(out, 10, env.SourceService)
	}
	if env.SchemaVersion > 0 {
		out = appendVarint(out, 11, uint64(uint32(env.SchemaVersion)))
	}

	// Optional IMDA evidence fields sourced from the payload.
	if payload != nil {
		if v, ok := payload["chora_imda_dimension"].(string); ok && v != "" {
			out = appendString(out, 14, v)
		}
		if v, ok := payload["imda_lifecycle_stage"].(string); ok && v != "" {
			out = appendString(out, 15, v)
		}
	}

	return out, nil
}

// encodeTimestamp emits the nested google.protobuf.Timestamp wire shape:
//
//	1 varint int64  seconds
//	2 varint int32  nanos
func encodeTimestamp(t time.Time) []byte {
	out := make([]byte, 0, 16)
	t = t.UTC()
	secs := t.Unix()
	nanos := int32(t.Nanosecond())
	if secs != 0 {
		out = appendVarint(out, 1, uint64(secs))
	}
	if nanos != 0 {
		out = appendVarint(out, 2, uint64(uint32(nanos)))
	}
	return out
}

// -----------------------------------------------------------------------------
// Wire-format helpers (thin protowire wrappers; reuse keeps callers tidy).
// -----------------------------------------------------------------------------

func appendString(b []byte, field protowire.Number, v string) []byte {
	b = protowire.AppendTag(b, field, protowire.BytesType)
	b = protowire.AppendString(b, v)
	return b
}

func appendVarint(b []byte, field protowire.Number, v uint64) []byte {
	b = protowire.AppendTag(b, field, protowire.VarintType)
	b = protowire.AppendVarint(b, v)
	return b
}

func appendLengthDelimited(b []byte, field protowire.Number, payload []byte) []byte {
	b = protowire.AppendTag(b, field, protowire.BytesType)
	b = protowire.AppendBytes(b, payload)
	return b
}

// -----------------------------------------------------------------------------
// Per-field writers — used by encoders to keep error-handling tidy.
// -----------------------------------------------------------------------------

// writeStringField writes the value at payload[key] as a string field if
// present + non-empty. Wrong Go type fails loud — proto3 string slots cannot
// silently accept int/bytes/etc.
func writeStringField(out *[]byte, payload map[string]any, field protowire.Number, key string) error {
	raw, present := payload[key]
	if !present {
		return nil
	}
	s, ok := raw.(string)
	if !ok {
		return fmt.Errorf("field %s: expected string, got %T", key, raw)
	}
	if s == "" {
		return nil
	}
	*out = appendString(*out, field, s)
	return nil
}

// writeTimestampField writes payload[key] as a nested Timestamp submessage.
// Accepts time.Time and *time.Time; wrong type fails loud.
func writeTimestampField(out *[]byte, payload map[string]any, field protowire.Number, key string) error {
	raw, present := payload[key]
	if !present {
		return nil
	}
	t, ok := asTime(raw)
	if !ok {
		return fmt.Errorf("field %s: expected time.Time, got %T", key, raw)
	}
	*out = appendLengthDelimited(*out, field, encodeTimestamp(t))
	return nil
}

// lookupString returns the first non-zero string value found at any of the
// supplied keys. Returns ("", false) if no key resolves to a string.
func lookupString(payload map[string]any, keys ...string) (string, bool) {
	for _, k := range keys {
		raw, present := payload[k]
		if !present {
			continue
		}
		s, ok := raw.(string)
		if !ok {
			continue
		}
		if s == "" {
			continue
		}
		return s, true
	}
	return "", false
}

// -----------------------------------------------------------------------------
// Loose-typed payload coercion (in/out: map[string]any).
// -----------------------------------------------------------------------------

// asTime coerces a value to time.Time. Accepts time.Time directly + nil.
func asTime(v any) (time.Time, bool) {
	if v == nil {
		return time.Time{}, false
	}
	switch t := v.(type) {
	case time.Time:
		return t, true
	case *time.Time:
		if t == nil {
			return time.Time{}, false
		}
		return *t, true
	default:
		return time.Time{}, false
	}
}

// stringSlice coerces []string or []any-of-string to a string slice.
func stringSlice(v any) ([]string, bool) {
	if v == nil {
		return nil, false
	}
	switch s := v.(type) {
	case []string:
		return s, true
	case []any:
		out := make([]string, 0, len(s))
		for _, e := range s {
			str, ok := e.(string)
			if !ok {
				return nil, false
			}
			out = append(out, str)
		}
		return out, true
	default:
		return nil, false
	}
}
