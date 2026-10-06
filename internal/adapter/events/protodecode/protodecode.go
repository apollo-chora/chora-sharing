// Package protodecode is the symmetric inverse of chora-sharing's
// protomarshal — it consumes inbound event-bus message bytes and returns a
// snake_case map[string]any compatible with the legacy json.Unmarshal flow.
//
// Why this package exists
// -----------------------
// Producer-side wave (task #33 / #38) flipped chora-sharing outbox payloads
// to binary protobuf — the canonical wire shape on the event bus. The four
// milestone events chora-sharing subscribes to — the canonical
// chora.consumption.companion.{stage_up, breed_revealed, hatched,
// source_revelation}.v1 subjects (ADR-254) plus the legacy familiar.* set —
// are CANDIDATES for the same flip on the chora-consumption side, and the
// companion.* set has already flipped. Subscribers that previously
// used `json.Unmarshal(msg.Data, &m)` now MUST accept binary bytes —
// `json: cannot unmarshal …` is the failure mode at runtime once the
// producer flips.
//
// Decode strategy
// ---------------
//  1. If the topic is registered for binary decoding (see binaryDecoders),
//     attempt proto.Unmarshal first. On success, project the proto message
//     into a snake_case map[string]any compatible with the legacy JSON shape.
//  2. On binary failure (or unregistered topic), fall back to json.Unmarshal.
//  3. On both-fail, return a wrapped error — caller fails loud, Pub/Sub Nacks,
//     broker retries + eventually deadletters.
//
// One-shot WARN logs surface (a) unknown topics + (b) binary-registered
// topics that succeed via the JSON fallback. The first is fine during the
// transition; the second should fade to zero once every producer is fully
// cut over.
package protodecode

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	consumptionv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/consumption/v1"
	creationv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/creation/v1"
	deliveryv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/delivery/v1"
)

// ErrEmptyPayload is returned when the inbound bytes are empty.
var ErrEmptyPayload = errors.New("protodecode: empty payload")

// binaryDecoder takes raw wire bytes for a topic and returns the unmarshalled
// proto.Message.
type binaryDecoder func(payload []byte) (proto.Message, error)

// projector maps a successfully-unmarshalled proto.Message onto the
// snake_case map[string]any the legacy JSON-decoded handlers consume.
type projector func(msg proto.Message, out map[string]any)

// binaryDecoders is the per-topic registry of binary-protobuf decoders +
// projectors.
var binaryDecoders = map[string]struct {
	decode  binaryDecoder
	project projector
}{
	"chora.consumption.familiar.stage_up.v1": {
		decode: func(payload []byte) (proto.Message, error) {
			var m consumptionv1.CompanionStageUp
			if err := proto.Unmarshal(payload, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
		project: projectFamiliarStageUp,
	},
	"chora.consumption.familiar.breed_revealed.v1": {
		decode: func(payload []byte) (proto.Message, error) {
			var m consumptionv1.CompanionBreedRevealed
			if err := proto.Unmarshal(payload, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
		project: projectFamiliarBreedRevealed,
	},
	"chora.consumption.familiar.hatched.v1": {
		decode: func(payload []byte) (proto.Message, error) {
			var m consumptionv1.CompanionHatched
			if err := proto.Unmarshal(payload, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
		project: projectFamiliarHatched,
	},
	"chora.consumption.familiar.source_revelation.v1": {
		decode: func(payload []byte) (proto.Message, error) {
			var m consumptionv1.CompanionSourceRevelation
			if err := proto.Unmarshal(payload, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
		project: projectFamiliarSourceRevelation,
	},
	// Canonical ADR-254 subjects — chora-consumption emits ONLY these today.
	// Same proto messages as the familiar.* entries above (the generated types
	// were always companion-named); only the subject keys differ. Without these
	// entries the first live companion.* message would NACK-loop on the JSON
	// fallback — the exact failure the creation.atom entries document above.
	"chora.consumption.companion.stage_up.v1": {
		decode: func(payload []byte) (proto.Message, error) {
			var m consumptionv1.CompanionStageUp
			if err := proto.Unmarshal(payload, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
		project: projectFamiliarStageUp,
	},
	"chora.consumption.companion.breed_revealed.v1": {
		decode: func(payload []byte) (proto.Message, error) {
			var m consumptionv1.CompanionBreedRevealed
			if err := proto.Unmarshal(payload, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
		project: projectFamiliarBreedRevealed,
	},
	"chora.consumption.companion.hatched.v1": {
		decode: func(payload []byte) (proto.Message, error) {
			var m consumptionv1.CompanionHatched
			if err := proto.Unmarshal(payload, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
		project: projectFamiliarHatched,
	},
	"chora.consumption.companion.source_revelation.v1": {
		decode: func(payload []byte) (proto.Message, error) {
			var m consumptionv1.CompanionSourceRevelation
			if err := proto.Unmarshal(payload, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
		project: projectFamiliarSourceRevelation,
	},
	"chora.delivery.live_quiz_session.score_awarded.v1": {
		decode: func(payload []byte) (proto.Message, error) {
			var m deliveryv1.LiveQuizSessionScoreAwarded
			if err := proto.Unmarshal(payload, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
		project: projectLiveQuizScoreAwarded,
	},
	"chora.delivery.course.published.v1": {
		decode:  decodeCoursePublished,
		project: projectCoursePublished,
	},
	"chora.consumption.weakness.grown.v1": {
		decode: func(payload []byte) (proto.Message, error) {
			var m consumptionv1.WeaknessGrown
			if err := proto.Unmarshal(payload, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
		project: projectWeaknessGrown,
	},
	// chora.creation.atom.{published,archived}.v1 — chora-creation flipped
	// these to binary protobuf long before the sharing-side subscriber went
	// live; the entries were never registered here, so the first live traffic
	// (WS-0 deploy, 2026-07-11) NACK-looped on the JSON fallback and
	// atom_projections never hydrated. Identical failure mode to
	// consumption's OPEN-1 (atom.created). Generated types live in
	// gen/go/chora/creation/v1/atom.pb.go.
	"chora.creation.atom.published.v1": {
		decode: func(payload []byte) (proto.Message, error) {
			var m creationv1.AtomPublished
			if err := proto.Unmarshal(payload, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
		project: projectAtomPublished,
	},
	"chora.creation.atom.archived.v1": {
		decode: func(payload []byte) (proto.Message, error) {
			var m creationv1.AtomArchived
			if err := proto.Unmarshal(payload, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
		project: projectAtomArchived,
	},
	// chora.creation.atom.reuse_visibility_changed.v1 — ADR-229 WS-1
	// (CHO-2127): the author changed the atom's reuse-consent audience; the
	// subscriber maps it onto atom_projections.reuse_visibility. Registered
	// at birth so the FIRST live message binary-decodes (never the
	// NACK-loop-on-JSON-fallback failure the lifecycle entries above fixed).
	"chora.creation.atom.reuse_visibility_changed.v1": {
		decode: func(payload []byte) (proto.Message, error) {
			var m creationv1.AtomReuseVisibilityChanged
			if err := proto.Unmarshal(payload, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
		project: projectAtomReuseVisibilityChanged,
	},
	// chora.creation.atom.orphan_created.v1 — ADR-229 Amendment A1
	// (CHO-2132): creation minted the singleton orphan edition; the sharing
	// repoint leg moves stranded grants onto it. Registered at birth so the
	// FIRST live message binary-decodes.
	"chora.creation.atom.orphan_created.v1": {
		decode: func(payload []byte) (proto.Message, error) {
			var m creationv1.AtomOrphanCreated
			if err := proto.Unmarshal(payload, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
		project: projectAtomOrphanCreated,
	},
}

// DecodePayloadMap decodes inbound Pub/Sub message bytes into a snake_case
// map[string]any compatible with the legacy json.Unmarshal flow.
//
// Use DecodePayloadMapWithAttrs when the caller has Pub/Sub msg.Attributes
// available — the publisher places envelope fields (event_id / tenant_id /
// gcid / traceparent) there, not in the payload body, so the JSON fallback
// path returns empty envelope fields without them.
func DecodePayloadMap(topic string, payload []byte) (map[string]any, error) {
	return DecodePayloadMapWithAttrs(topic, payload, nil)
}

// DecodePayloadMapWithAttrs decodes inbound Pub/Sub message bytes + the
// publisher-supplied msg.Attributes into a snake_case map[string]any.
//
// Field precedence (high → low):
//  1. Binary proto Envelope (when payload is binary-decodable for this topic)
//  2. Pub/Sub msg.Attributes (publisher's canonical envelope projection per
//     libs/chora-go-common/pubsub.envelopeAttributes)
//  3. JSON payload body
//
// Binary always wins because the producer flipped to binary AS the canonical
// wire shape (#33 / #38). Attributes win over the JSON body during the
// transition window because envelope fields live there, not in the JSON
// body. The JSON body remains the fallback for legacy/topic-specific data.
//
// Attribute key mapping (publisher → consumer field name):
//   - attrs["event_id"]      → out["event_id"]
//   - attrs["tenant_id"]     → out["tenant_id"]
//   - attrs["gcid"]          → out["owner_gcid"] (Familiar events use
//     owner_gcid; gcid is the canonical envelope
//     field name on the wire)
//   - attrs["traceparent"]   → out["traceparent"]
//   - attrs["tracestate"]    → out["tracestate"]
//   - attrs["occurred_at"]   → out["occurred_at"]
//   - attrs["chora_imda_dimension"], attrs["imda_lifecycle_stage"] → same
//
// nil attrs ⇒ legacy DecodePayloadMap behaviour. Empty payload ⇒
// ErrEmptyPayload regardless of attrs.
func DecodePayloadMapWithAttrs(topic string, payload []byte, attrs map[string]string) (map[string]any, error) {
	if len(payload) == 0 {
		return nil, ErrEmptyPayload
	}

	if entry, ok := binaryDecoders[topic]; ok {
		msg, err := entry.decode(payload)
		if err == nil && msg != nil && entry.project != nil {
			out := make(map[string]any)
			// Lay down attrs first so binary projection overrides them.
			mergeAttrsEnvelope(attrs, out)
			entry.project(msg, out)
			return out, nil
		}
		warnBinaryFallback(topic, err)
	} else {
		warnUnknownTopic(topic)
	}

	var body map[string]any
	if err := json.Unmarshal(payload, &body); err != nil {
		return nil, fmt.Errorf("protodecode: topic %q neither binary-decodable nor JSON-decodable: %w", topic, err)
	}
	out := make(map[string]any, len(body)+len(attrs))
	for k, v := range body {
		out[k] = v
	}
	// Attrs override the JSON body for envelope fields — body has the
	// domain payload, attrs carry the canonical envelope projection.
	mergeAttrsEnvelope(attrs, out)
	return out, nil
}

// mergeAttrsEnvelope projects Pub/Sub msg.Attributes onto the decoded map's
// canonical envelope keys. Empty / missing attrs are no-ops.
func mergeAttrsEnvelope(attrs map[string]string, out map[string]any) {
	if len(attrs) == 0 {
		return
	}
	if v := attrs["event_id"]; v != "" {
		out["event_id"] = v
	}
	if v := attrs["tenant_id"]; v != "" {
		out["tenant_id"] = v
	}
	// Familiar events use owner_gcid; envelope field name is gcid.
	if v := attrs["gcid"]; v != "" {
		out["owner_gcid"] = v
		// Also retain canonical gcid for non-Familiar consumers.
		if _, ok := out["gcid"]; !ok {
			out["gcid"] = v
		}
	}
	if v := attrs["traceparent"]; v != "" {
		out["traceparent"] = v
	}
	if v := attrs["tracestate"]; v != "" {
		out["tracestate"] = v
	}
	if v := attrs["occurred_at"]; v != "" {
		out["occurred_at"] = v
	}
	if v := attrs["published_at"]; v != "" {
		out["published_at"] = v
	}
	if v := attrs["chora_imda_dimension"]; v != "" {
		out["chora_imda_dimension"] = v
	}
	if v := attrs["imda_lifecycle_stage"]; v != "" {
		out["imda_lifecycle_stage"] = v
	}
}

// -----------------------------------------------------------------------------
// Projectors
// -----------------------------------------------------------------------------

func mergeEnvelope(env *commonv1.EventEnvelope, out map[string]any) {
	if env == nil {
		return
	}
	if v := env.GetEventId(); v != "" {
		out["event_id"] = v
	}
	if v := env.GetTenantId(); v != "" {
		out["tenant_id"] = v
	}
	if v := env.GetGcid(); v != "" {
		out["gcid"] = v
	}
	if v := env.GetTraceparent(); v != "" {
		out["traceparent"] = v
	}
	if v := env.GetTracestate(); v != "" {
		out["tracestate"] = v
	}
	if v := env.GetChoraImdaDimension(); v != "" {
		out["chora_imda_dimension"] = v
	}
	if v := env.GetImdaLifecycleStage(); v != "" {
		out["imda_lifecycle_stage"] = v
	}
}

// projectAtomPublished maps the creation atom-published event onto the
// snake_case keys AtomPublishedEnvelope reads (atom_projection_subscriber):
// atom_id / current_revision_id / author_gcid / title / stem / question_type
// / status, plus the envelope fields via mergeEnvelope. Omit-zero convention.
// (author_display_name has no proto field — the wire never carried it.)
func projectAtomPublished(msg proto.Message, out map[string]any) {
	m, ok := msg.(*creationv1.AtomPublished)
	if !ok || m == nil {
		return
	}
	mergeEnvelope(m.GetEnvelope(), out)
	if v := m.GetAtomId(); v != "" {
		out["atom_id"] = v
	}
	if v := m.GetCurrentRevisionId(); v != "" {
		out["current_revision_id"] = v
	}
	if v := m.GetAuthorGcid(); v != "" {
		out["author_gcid"] = v
	}
	if v := m.GetTitle(); v != "" {
		out["title"] = v
	}
	if v := m.GetStem(); v != "" {
		out["stem"] = v
	}
	if v := atomTypeToDomain(m.GetQuestionType()); v != "" {
		out["question_type"] = v
	}
	if v := atomStatusToDomain(m.GetStatus()); v != "" {
		out["status"] = v
	}
	// reuse_visibility (field 30, ADR-229 WS-1) — omit-zero: an UNSPECIFIED
	// value (pre-ADR-229 producer) leaves the key ABSENT so the pg upsert
	// preserves an audience already applied via reuse_visibility_changed.v1.
	if v := reuseVisibilityToDomain(m.GetReuseVisibility()); v != "" {
		out["reuse_visibility"] = v
	}
	// author_display_name (field 31) — denormalised by the creation service
	// at publish time. Empty when identity was unavailable; consumers fall
	// back to a short GCID label. NOTE: the proto field was removed/never
	// generated (GetAuthorDisplayName undefined) — this is a no-op until
	// the proto is regenerated with the field. Consumers fall back to GCID.
}

// projectAtomReuseVisibilityChanged maps the ADR-229 WS-1 audience-change
// event onto the snake_case keys AtomReuseVisibilityChangedEnvelope reads:
// atom_id / reuse_visibility / previous_visibility / author_gcid, plus the
// envelope fields via mergeEnvelope. Omit-zero convention.
func projectAtomReuseVisibilityChanged(msg proto.Message, out map[string]any) {
	m, ok := msg.(*creationv1.AtomReuseVisibilityChanged)
	if !ok || m == nil {
		return
	}
	mergeEnvelope(m.GetEnvelope(), out)
	if v := m.GetAtomId(); v != "" {
		out["atom_id"] = v
	}
	if v := reuseVisibilityToDomain(m.GetReuseVisibility()); v != "" {
		out["reuse_visibility"] = v
	}
	if v := reuseVisibilityToDomain(m.GetPreviousVisibility()); v != "" {
		out["previous_visibility"] = v
	}
	if v := m.GetAuthorGcid(); v != "" {
		out["author_gcid"] = v
	}
}

// reuseVisibilityToDomain maps the proto ReuseVisibility enum onto the
// lowercase ADR-229 audience labels the projection layer stores. UNSPECIFIED
// maps to "" — the CONSUMER decides hardening (the pg upsert defaults
// 'private' on insert; the changed-subscriber refuses "" outright).
func reuseVisibilityToDomain(v creationv1.ReuseVisibility) string {
	switch v {
	case creationv1.ReuseVisibility_REUSE_VISIBILITY_PRIVATE:
		return "private"
	case creationv1.ReuseVisibility_REUSE_VISIBILITY_FRIENDS:
		return "friends"
	case creationv1.ReuseVisibility_REUSE_VISIBILITY_TENANT:
		return "tenant"
	default:
		return ""
	}
}

// projectAtomOrphanCreated maps the ADR-229 A1 orphan-mint event onto the
// snake_case keys AtomOrphanCreatedEnvelope reads. Omit-zero convention.
func projectAtomOrphanCreated(msg proto.Message, out map[string]any) {
	m, ok := msg.(*creationv1.AtomOrphanCreated)
	if !ok || m == nil {
		return
	}
	mergeEnvelope(m.GetEnvelope(), out)
	if v := m.GetOrphanAtomId(); v != "" {
		out["orphan_atom_id"] = v
	}
	if v := m.GetOrphanedFromAtomId(); v != "" {
		out["orphaned_from_atom_id"] = v
	}
	if v := m.GetSourceRevisionId(); v != "" {
		out["source_revision_id"] = v
	}
	if v := m.GetAuthorGcid(); v != "" {
		out["author_gcid"] = v
	}
	if v := m.GetTrigger(); v != "" {
		out["trigger"] = v
	}
	if ts := m.GetOrphanedAt(); ts != nil {
		out["orphaned_at"] = ts.AsTime().UTC().Format(time.RFC3339Nano)
	}
}

// projectAtomArchived maps the creation atom-archived event onto the keys
// AtomArchivedEnvelope reads: atom_id / status + envelope fields.
func projectAtomArchived(msg proto.Message, out map[string]any) {
	m, ok := msg.(*creationv1.AtomArchived)
	if !ok || m == nil {
		return
	}
	mergeEnvelope(m.GetEnvelope(), out)
	if v := m.GetAtomId(); v != "" {
		out["atom_id"] = v
	}
	if v := atomStatusToDomain(m.GetStatus()); v != "" {
		out["status"] = v
	}
}

// atomStatusToDomain maps the proto AtomStatus enum onto the lowercase
// domain labels the projection layer stores (mirrors chora-consumption's
// protodecode — keep in lockstep).
func atomStatusToDomain(s creationv1.AtomStatus) string {
	switch s {
	case creationv1.AtomStatus_ATOM_STATUS_DRAFT:
		return "draft"
	case creationv1.AtomStatus_ATOM_STATUS_PUBLISHED:
		return "published"
	case creationv1.AtomStatus_ATOM_STATUS_ARCHIVED:
		return "archived"
	default:
		return ""
	}
}

// atomTypeToDomain maps the proto AtomType enum onto the lowercase domain
// label, DERIVED from the generated descriptor per the naming law in
// chora-contracts/proto/events/creation/atom.proto (CHO-2178).
//
// It used to be a hand-written switch that stopped at ATOM_TYPE_CODE (7) and
// said "mirrors chora-consumption's protodecode — keep in lockstep". It was not
// in lockstep: chora-creation emits ATOM_TYPE_ESSAY (8) for every essay atom,
// this fell to `default: return ""`, and the projection stored a BLANK
// question_type. Ten essay atoms sat in prod that way, unmatchable by the
// question_type filter this service exposes — and every unit test was green,
// because each one only ever fed the decoder a value it already knew.
//
// Derivation removes the lockstep obligation entirely: a value added to the
// contract decodes correctly here with no code change.
func atomTypeToDomain(t creationv1.AtomType) string {
	// UNSPECIFIED means "the producer had no opinion". Return absent, not
	// blank: the pg upsert is non-blanking (COALESCE(NULLIF(...,''), existing)),
	// so a pre-CHO-2178 producer must not clobber a flavour already projected.
	if t == creationv1.AtomType_ATOM_TYPE_UNSPECIFIED {
		return ""
	}
	// The one label the naming law cannot produce (wire name MULTIPLE_CHOICE).
	if t == creationv1.AtomType_ATOM_TYPE_MULTIPLE_CHOICE {
		return "mcq"
	}
	name, declared := creationv1.AtomType_name[int32(t)]
	if !declared {
		// A value this binary's contract does not know — only reachable if a
		// producer was deployed AHEAD of this consumer, which the CHO-2178
		// rollout order (consumers first) exists to prevent. Say so loudly
		// rather than quietly blanking the column, which is the exact failure
		// this function is being fixed for.
		warnUnknownAtomTypeOnce(int32(t))
		return ""
	}
	return strings.ToLower(strings.TrimPrefix(name, "ATOM_TYPE_"))
}

// warnUnknownAtomTypeOnce logs each unrecognised AtomType wire value a single
// time — enough to page a human, not enough to flood the log on a replayed
// backlog.
var (
	warnedAtomTypesMu sync.Mutex
	warnedAtomTypes   = map[int32]bool{}
)

func warnUnknownAtomTypeOnce(v int32) {
	warnedAtomTypesMu.Lock()
	defer warnedAtomTypesMu.Unlock()
	if warnedAtomTypes[v] {
		return
	}
	warnedAtomTypes[v] = true
	log.Printf("WARN protodecode: chora.creation.v1.AtomType value %d is not declared in this "+
		"build's contract — question_type will be left ABSENT on the projection. A producer is "+
		"running ahead of chora-sharing; redeploy this service against current chora-contracts.", v)
}

// projectLiveQuizScoreAwarded maps the ADR-168 delivery score event onto the
// snake_case keys ScoreAwardedEnvelope reads. The learner is the envelope gcid
// (set by mergeEnvelope); no separate learner_gcid field exists on the wire.
func projectLiveQuizScoreAwarded(msg proto.Message, out map[string]any) {
	m, ok := msg.(*deliveryv1.LiveQuizSessionScoreAwarded)
	if !ok || m == nil {
		return
	}
	mergeEnvelope(m.GetEnvelope(), out)
	if v := m.GetSessionId(); v != "" {
		out["session_id"] = v
	}
	if v := m.GetLiveQuizId(); v != "" {
		out["live_quiz_id"] = v
	}
	if v := m.GetQuestionId(); v != "" {
		out["question_id"] = v
	}
	if v := m.GetAwardedPoints(); v != 0 {
		out["awarded_points"] = int(v)
	}
	if v := m.GetCumulativeScore(); v != 0 {
		out["cumulative_score"] = int(v)
	}
	out["correct"] = m.GetCorrect()
	if v := m.GetAnswerMillis(); v != 0 {
		out["answer_millis"] = v
	}
}

// projectWeaknessGrown maps the ADR-196 B1 consumption weakness.grown event
// onto the snake_case keys WeaknessGrownEnvelope reads. The learner is carried
// both in the envelope gcid (via mergeEnvelope) AND the payload learner_gcid;
// tenant_id is likewise duplicated for sharded filters.
func projectWeaknessGrown(msg proto.Message, out map[string]any) {
	m, ok := msg.(*consumptionv1.WeaknessGrown)
	if !ok || m == nil {
		return
	}
	mergeEnvelope(m.GetEnvelope(), out)
	if v := m.GetGrowthEdgeId(); v != "" {
		out["growth_edge_id"] = v
	}
	if v := m.GetTenantId(); v != "" {
		out["tenant_id"] = v
	}
	if v := m.GetLearnerGcid(); v != "" {
		out["learner_gcid"] = v
	}
	if v := m.GetConceptLabel(); v != "" {
		out["concept_label"] = v
	}
	if v := m.GetConceptKey(); v != "" {
		out["concept_key"] = v
	}
	if v := m.GetFinalStrength(); v != 0 {
		out["final_strength"] = float64(v)
	}
	if v := m.GetRecoverySource(); v != "" {
		out["recovery_source"] = v
	}
	if v := m.GetTags(); len(v) > 0 {
		out["tags"] = stringsToAny(v)
	}
	if t := m.GetGrownAt(); t != nil {
		out["grown_at"] = t.AsTime().UTC().Format(time.RFC3339Nano)
	}
}

func projectFamiliarStageUp(msg proto.Message, out map[string]any) {
	m, ok := msg.(*consumptionv1.CompanionStageUp)
	if !ok || m == nil {
		return
	}
	mergeEnvelope(m.GetEnvelope(), out)
	if v := m.GetCompanionId(); v != "" {
		out["familiar_id"] = v
	}
	if v := m.GetOwnerGcid(); v != "" {
		out["owner_gcid"] = v
	}
	if v := m.GetStageFrom(); v != 0 {
		out["stage_from"] = int(v)
	}
	if v := m.GetStageTo(); v != 0 {
		out["stage_to"] = int(v)
	}
	if v := m.GetStageFromName(); v != "" {
		out["stage_from_name"] = v
	}
	if v := m.GetStageToName(); v != "" {
		out["stage_to_name"] = v
	}
	if v := m.GetNewlyUnlockedTools(); len(v) > 0 {
		out["newly_unlocked_tools"] = stringsToAny(v)
	}
	if v := m.GetNewlyRevealedKgNeighbors(); len(v) > 0 {
		out["newly_revealed_kg_neighbors"] = stringsToAny(v)
	}
	if v := m.GetNewLlmTier(); v != "" {
		out["new_llm_tier"] = v
	}
	if v := m.GetNewMemoryMode(); v != "" {
		out["new_memory_mode"] = v
	}
	if t := m.GetStageUpAt(); t != nil {
		out["stage_up_at"] = t.AsTime().UTC().Format(time.RFC3339Nano)
	}
	// familiar_display_name (field 13, CHO-2266) — denormalised name so the C+
	// milestone copy names the Familiar. Omit-zero: absent leaves the neutral
	// template form (the template branches on presence).
	if v := m.GetCompanionDisplayName(); v != "" {
		out["familiar_display_name"] = v
	}
}

func projectFamiliarBreedRevealed(msg proto.Message, out map[string]any) {
	m, ok := msg.(*consumptionv1.CompanionBreedRevealed)
	if !ok || m == nil {
		return
	}
	mergeEnvelope(m.GetEnvelope(), out)
	if v := m.GetCompanionId(); v != "" {
		out["familiar_id"] = v
	}
	if v := m.GetOwnerGcid(); v != "" {
		out["owner_gcid"] = v
	}
	// species is LEARNER-FACING copy: emit the plain-language label, never the
	// generated enum name (CHO-2259 — String() put FAMILIAR_SPECIES_FOX in a
	// C+ post body). Absent when this build cannot name the value.
	putSpeciesLabel(m.GetSpecies(), out)
	out["shiny_variant"] = m.GetShinyVariant()
	if v := m.GetRarity(); v != "" {
		out["rarity"] = v
	}
	if v := m.GetEggSku(); v != "" {
		out["egg_sku"] = v
	}
	if v := m.GetRolledProbability(); v != 0 {
		out["rolled_probability"] = v
	}
	if t := m.GetRevealedAt(); t != nil {
		out["revealed_at"] = t.AsTime().UTC().Format(time.RFC3339Nano)
	}
}

func projectFamiliarHatched(msg proto.Message, out map[string]any) {
	m, ok := msg.(*consumptionv1.CompanionHatched)
	if !ok || m == nil {
		return
	}
	mergeEnvelope(m.GetEnvelope(), out)
	if v := m.GetCompanionId(); v != "" {
		out["familiar_id"] = v
	}
	if v := m.GetOwnerGcid(); v != "" {
		out["owner_gcid"] = v
	}
	if v := m.GetDisplayName(); v != "" {
		out["display_name"] = v
	}
	// species is LEARNER-FACING copy: emit the plain-language label, never the
	// generated enum name (CHO-2259 — String() put FAMILIAR_SPECIES_FOX in a
	// C+ post body). Absent when this build cannot name the value.
	putSpeciesLabel(m.GetSpecies(), out)
	out["shiny_variant"] = m.GetShinyVariant()
	if v := m.GetResonantAtomId(); v != "" {
		out["resonant_atom_id"] = v
	}
	if v := m.GetTone(); v != "" {
		out["tone"] = v
	}
	if v := m.GetLearnerPersona(); v != "" {
		out["learner_persona"] = v
	}
	if t := m.GetHatchedAt(); t != nil {
		out["hatched_at"] = t.AsTime().UTC().Format(time.RFC3339Nano)
	}
}

func projectFamiliarSourceRevelation(msg proto.Message, out map[string]any) {
	m, ok := msg.(*consumptionv1.CompanionSourceRevelation)
	if !ok || m == nil {
		return
	}
	mergeEnvelope(m.GetEnvelope(), out)
	if v := m.GetCompanionId(); v != "" {
		out["familiar_id"] = v
	}
	if v := m.GetOwnerGcid(); v != "" {
		out["owner_gcid"] = v
	}
	if v := m.GetPreviewLlmTier(); v != "" {
		out["preview_llm_tier"] = v
	}
	if v := m.GetPreviewTools(); len(v) > 0 {
		out["preview_tools"] = stringsToAny(v)
	}
	if t := m.GetRevelationAt(); t != nil {
		out["revelation_at"] = t.AsTime().UTC().Format(time.RFC3339Nano)
	}
	if t := m.GetWindowExpiresAt(); t != nil {
		out["window_expires_at"] = t.AsTime().UTC().Format(time.RFC3339Nano)
	}
	if v := m.GetWindowDurationSeconds(); v != 0 {
		out["window_duration_seconds"] = int(v)
	}
	// familiar_display_name (field 9, CHO-2266) — denormalised name so the C+
	// Stage-3 ceremony copy names the Familiar. Omit-zero (branch on presence).
	if v := m.GetCompanionDisplayName(); v != "" {
		out["familiar_display_name"] = v
	}
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func stringsToAny(in []string) []any {
	out := make([]any, len(in))
	for i, v := range in {
		out[i] = v
	}
	return out
}

// -----------------------------------------------------------------------------
// One-shot WARN logging
// -----------------------------------------------------------------------------

var (
	warnedFallbackMu sync.Mutex
	warnedFallback   = map[string]bool{}

	warnedUnknownMu sync.Mutex
	warnedUnknown   = map[string]bool{}
)

func warnBinaryFallback(topic string, err error) {
	warnedFallbackMu.Lock()
	defer warnedFallbackMu.Unlock()
	if warnedFallback[topic] {
		return
	}
	warnedFallback[topic] = true
	log.Printf("WARN protodecode: topic %q registered for binary but binary unmarshal failed (%v) — falling back to JSON. Expected during producer-side flip; investigate if persistent.", topic, err)
}

func warnUnknownTopic(topic string) {
	warnedUnknownMu.Lock()
	defer warnedUnknownMu.Unlock()
	if warnedUnknown[topic] {
		return
	}
	warnedUnknown[topic] = true
	log.Printf("WARN protodecode: topic %q has no binary decoder registered — using JSON fallback. Add an entry to internal/adapter/events/protodecode/protodecode.go when the producer flips to binary.", topic)
}
