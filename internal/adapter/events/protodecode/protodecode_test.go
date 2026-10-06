// protodecode_test verifies the symmetric inverse of chora-sharing's
// protomarshal — given the canonical binary protobuf wire bytes emitted by
// a producer that has flipped to binary, DecodePayloadMap returns a
// map[string]any with snake_case keys matching the topic's schema. JSON
// fallback exercised for topics still on JSON during the producer-side flip.
package protodecode_test

import (
	"encoding/json"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"

	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	consumptionv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/consumption/v1"
	deliveryv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/delivery/v1"

	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protodecode"
)

// TestDecode_FamiliarStageUp_Binary verifies a real proto-encoded
// FamiliarStageUp message (the future shape once chora-consumption flips the
// stage_up.v1 topic) round-trips into a snake_case map.
func TestDecode_FamiliarStageUp_Binary(t *testing.T) {
	t0 := time.Date(2026, 5, 16, 9, 30, 0, 0, time.UTC)
	msg := &consumptionv1.CompanionStageUp{
		Envelope: &commonv1.EventEnvelope{
			EventId:       "01971a90-0000-7000-8000-000000000abc",
			TenantId:      "tenant-acme",
			Gcid:          "gcid-phyllis",
			Traceparent:   "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
			OccurredAt:    timestamppb.New(t0),
			SchemaVersion: 1,
		},
		CompanionId:   "fam-1",
		OwnerGcid:     "gcid-phyllis",
		StageFrom:     3,
		StageTo:       4,
		StageFromName: "Awakened",
		StageToName:   "Teen",
		NewLlmTier:    "flash",
		StageUpAt:     timestamppb.New(t0),
	}
	bz, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}

	got, err := protodecode.DecodePayloadMap("chora.consumption.familiar.stage_up.v1", bz)
	if err != nil {
		t.Fatalf("DecodePayloadMap binary: %v", err)
	}
	if v, _ := got["event_id"].(string); v != "01971a90-0000-7000-8000-000000000abc" {
		t.Errorf("event_id = %q, want envelope-sourced UUID", v)
	}
	if v, _ := got["tenant_id"].(string); v != "tenant-acme" {
		t.Errorf("tenant_id = %q", v)
	}
	if v, _ := got["owner_gcid"].(string); v != "gcid-phyllis" {
		t.Errorf("owner_gcid = %q", v)
	}
	if v, _ := got["stage_to_name"].(string); v != "Teen" {
		t.Errorf("stage_to_name = %q", v)
	}
}

// TestDecode_CompanionTopics_Binary (ADR-254) — chora-consumption emits ONLY
// the canonical chora.consumption.companion.* subjects; each must binary-decode
// (same proto messages as the familiar.* entries — only the subject keys
// differ). Without registry entries these would NACK-loop on the JSON
// fallback, the failure mode the creation.atom entries document above.
func TestDecode_CompanionTopics_Binary(t *testing.T) {
	t0 := time.Date(2026, 5, 16, 9, 30, 0, 0, time.UTC)
	env := &commonv1.EventEnvelope{
		EventId:       "01971a90-0000-7000-8000-000000000abc",
		TenantId:      "tenant-acme",
		Gcid:          "gcid-phyllis",
		OccurredAt:    timestamppb.New(t0),
		SchemaVersion: 1,
	}
	cases := []struct {
		topic string
		msg   proto.Message
		key   string
		want  string
	}{
		{
			topic: "chora.consumption.companion.stage_up.v1",
			msg: &consumptionv1.CompanionStageUp{
				Envelope: env, CompanionId: "fam-1", OwnerGcid: "gcid-phyllis",
				StageToName: "Teen", NewLlmTier: "flash",
			},
			key:  "stage_to_name",
			want: "Teen",
		},
		{
			topic: "chora.consumption.companion.breed_revealed.v1",
			msg: &consumptionv1.CompanionBreedRevealed{
				Envelope: env, CompanionId: "fam-1", OwnerGcid: "gcid-phyllis",
				Rarity: "rare", EggSku: "egg.standard.v1",
			},
			key:  "rarity",
			want: "rare",
		},
		{
			topic: "chora.consumption.companion.hatched.v1",
			msg: &consumptionv1.CompanionHatched{
				Envelope: env, CompanionId: "fam-1", OwnerGcid: "gcid-phyllis",
				DisplayName: "Eira", Tone: "socratic",
			},
			key:  "display_name",
			want: "Eira",
		},
		{
			topic: "chora.consumption.companion.source_revelation.v1",
			msg: &consumptionv1.CompanionSourceRevelation{
				Envelope: env, CompanionId: "fam-1", OwnerGcid: "gcid-phyllis",
				PreviewLlmTier: "pro",
			},
			key:  "preview_llm_tier",
			want: "pro",
		},
	}
	for _, c := range cases {
		bz, err := proto.Marshal(c.msg)
		if err != nil {
			t.Fatalf("proto.Marshal(%s): %v", c.topic, err)
		}
		got, err := protodecode.DecodePayloadMap(c.topic, bz)
		if err != nil {
			t.Fatalf("DecodePayloadMap(%s) binary: %v", c.topic, err)
		}
		if v, _ := got[c.key].(string); v != c.want {
			t.Errorf("%s: %s = %q, want %q", c.topic, c.key, v, c.want)
		}
		if v, _ := got["event_id"].(string); v != env.EventId {
			t.Errorf("%s: event_id = %q, want envelope-sourced UUID", c.topic, v)
		}
	}
}

// TestDecode_FamiliarStageUp_JSON exercises the JSON fallback (current
// producer shape until the protomarshal flip).
func TestDecode_FamiliarStageUp_JSON(t *testing.T) {
	payload := map[string]any{
		"familiar_id":     "fam-1",
		"owner_gcid":      "gcid-phyllis",
		"stage_from":      3,
		"stage_to":        4,
		"stage_from_name": "Awakened",
		"stage_to_name":   "Teen",
	}
	bz, _ := json.Marshal(payload)
	got, err := protodecode.DecodePayloadMap("chora.consumption.familiar.stage_up.v1", bz)
	if err != nil {
		t.Fatalf("DecodePayloadMap json: %v", err)
	}
	if v, _ := got["owner_gcid"].(string); v != "gcid-phyllis" {
		t.Errorf("owner_gcid = %q", v)
	}
	if v, _ := got["stage_to_name"].(string); v != "Teen" {
		t.Errorf("stage_to_name = %q", v)
	}
}

// TestDecode_FamiliarStageUp_CarriesDisplayName (CHO-2266) proves the projector
// surfaces the denormalised familiar_display_name so the milestone copy can name
// the Familiar. Driven from a marshalled event — a hand-filled map would pass
// even if the projector dropped the field (the CHO-2259 lesson).
func TestDecode_FamiliarStageUp_CarriesDisplayName(t *testing.T) {
	msg := &consumptionv1.CompanionStageUp{
		Envelope:             &commonv1.EventEnvelope{EventId: "01971a90-0000-7000-8000-000000000abc", TenantId: "t", Gcid: "g"},
		CompanionId:          "fam-1",
		OwnerGcid:            "gcid-phyllis",
		StageToName:          "awakened",
		CompanionDisplayName: "Tempo",
	}
	bz, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	got, err := protodecode.DecodePayloadMap("chora.consumption.familiar.stage_up.v1", bz)
	if err != nil {
		t.Fatalf("DecodePayloadMap: %v", err)
	}
	if v, _ := got["familiar_display_name"].(string); v != "Tempo" {
		t.Errorf("familiar_display_name = %q, want Tempo", v)
	}
}

// TestDecode_FamiliarSourceRevelation_CarriesDisplayName (CHO-2266) — the
// Stage-3 ceremony projector surfaces the name for the Aha-moment copy.
func TestDecode_FamiliarSourceRevelation_CarriesDisplayName(t *testing.T) {
	msg := &consumptionv1.CompanionSourceRevelation{
		Envelope:             &commonv1.EventEnvelope{EventId: "01971a90-0000-7000-8000-000000000abd", TenantId: "t", Gcid: "g"},
		CompanionId:          "fam-1",
		OwnerGcid:            "gcid-phyllis",
		PreviewLlmTier:       "pro",
		CompanionDisplayName: "Tempo",
	}
	bz, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	got, err := protodecode.DecodePayloadMap("chora.consumption.familiar.source_revelation.v1", bz)
	if err != nil {
		t.Fatalf("DecodePayloadMap: %v", err)
	}
	if v, _ := got["familiar_display_name"].(string); v != "Tempo" {
		t.Errorf("familiar_display_name = %q, want Tempo", v)
	}
}

// TestDecode_AtomSessionCompleted_JSON exercises the discovery-side JSON
// fallback — that topic has no protomarshal encoder yet.
func TestDecode_AtomSessionCompleted_JSON(t *testing.T) {
	payload := map[string]any{
		"session_id":   "sess-1",
		"atom_id":      "atom-1",
		"tenant_id":    "tenant-acme",
		"completed_at": "2026-05-16T09:30:00Z",
	}
	bz, _ := json.Marshal(payload)
	got, err := protodecode.DecodePayloadMap("chora.consumption.atom_session.completed.v1", bz)
	if err != nil {
		t.Fatalf("DecodePayloadMap atom_session: %v", err)
	}
	if v, _ := got["atom_id"].(string); v != "atom-1" {
		t.Errorf("atom_id = %q", v)
	}
}

// TestDecode_EmptyPayload_FailsLoud asserts the decoder fails loud.
func TestDecode_EmptyPayload_FailsLoud(t *testing.T) {
	if _, err := protodecode.DecodePayloadMap("chora.consumption.familiar.stage_up.v1", nil); err == nil {
		t.Fatal("expected error on nil payload")
	}
}

// TestDecode_UnknownTopic_FallsBackToJSON ensures forward-compat: unknown
// topics succeed via JSON fallback.
func TestDecode_UnknownTopic_FallsBackToJSON(t *testing.T) {
	bz, _ := json.Marshal(map[string]any{"foo": "bar"})
	got, err := protodecode.DecodePayloadMap("chora.unknown.x.v1", bz)
	if err != nil {
		t.Fatalf("unknown topic decode: %v", err)
	}
	if v, _ := got["foo"].(string); v != "bar" {
		t.Errorf("foo = %q", v)
	}
}

// TestDecode_BinaryRegisteredTopic_AcceptsJSONFallback covers the transition
// window: producer still emits JSON for a registered binary topic. Decoder
// must accept JSON without losing the message.
func TestDecode_BinaryRegisteredTopic_AcceptsJSONFallback(t *testing.T) {
	bz, _ := json.Marshal(map[string]any{"familiar_id": "fam-1", "owner_gcid": "gcid-x"})
	got, err := protodecode.DecodePayloadMap("chora.consumption.familiar.hatched.v1", bz)
	if err != nil {
		t.Fatalf("json fallback for binary-registered topic: %v", err)
	}
	if v, _ := got["familiar_id"].(string); v != "fam-1" {
		t.Errorf("familiar_id = %q", v)
	}
}

// -----------------------------------------------------------------------------
// Debt #3 — JSON-fallback envelope-from-attributes fix
//
// Publisher (per libs/chora-go-common/pubsub.envelopeAttributes) emits the
// envelope fields (event_id, tenant_id, gcid, traceparent, ...) on Pub/Sub
// msg.Attributes and the *domain* fields in the payload body. Binary protobuf
// payloads bundle the envelope inline (EventEnvelope on the proto), so the
// binary path already has them. The JSON fallback path was returning empty
// strings for envelope fields — DecodePayloadMapWithAttrs closes the gap by
// merging attributes into the result with the rule:
//
//	 binary projection > attributes > json body
//
// Binary is the new canonical wire shape; attributes are the canonical
// envelope projection; the JSON body remains the legacy fallback.
// -----------------------------------------------------------------------------

// TestDecodeWithAttrs_JSONPayload_AttrsPopulateEnvelope asserts the JSON
// fallback path is enriched with attribute-sourced envelope fields.
func TestDecodeWithAttrs_JSONPayload_AttrsPopulateEnvelope(t *testing.T) {
	payload := map[string]any{
		"familiar_id":     "fam-1",
		"stage_from":      3,
		"stage_to":        4,
		"stage_from_name": "Awakened",
		"stage_to_name":   "Teen",
	}
	bz, _ := json.Marshal(payload)

	attrs := map[string]string{
		"event_id":     "01971a90-0000-7000-8000-000000000abc",
		"tenant_id":    "tenant-acme",
		"gcid":         "gcid-phyllis",
		"traceparent":  "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		"tracestate":   "chora=stage_up",
		"topic":        "chora.consumption.familiar.stage_up.v1",
		"occurred_at":  "2026-05-16T09:30:00Z",
		"published_at": "2026-05-16T09:30:00.001Z",
	}

	got, err := protodecode.DecodePayloadMapWithAttrs("chora.consumption.familiar.stage_up.v1", bz, attrs)
	if err != nil {
		t.Fatalf("DecodePayloadMapWithAttrs: %v", err)
	}
	if v, _ := got["event_id"].(string); v != "01971a90-0000-7000-8000-000000000abc" {
		t.Errorf("event_id = %q (want attr-sourced)", v)
	}
	if v, _ := got["tenant_id"].(string); v != "tenant-acme" {
		t.Errorf("tenant_id = %q", v)
	}
	// owner_gcid is mapped from attrs["gcid"] (publisher's canonical name).
	if v, _ := got["owner_gcid"].(string); v != "gcid-phyllis" {
		t.Errorf("owner_gcid = %q (want attrs.gcid)", v)
	}
	if v, _ := got["traceparent"].(string); v == "" {
		t.Errorf("traceparent not propagated from attrs")
	}
	// Domain field from the JSON body must still be present.
	if v, _ := got["stage_to_name"].(string); v != "Teen" {
		t.Errorf("stage_to_name = %q", v)
	}
}

// TestDecodeWithAttrs_BinaryPayload_EnvelopeWinsOverAttrs asserts binary is
// canonical — when both are present, the proto Envelope's values are kept.
func TestDecodeWithAttrs_BinaryPayload_EnvelopeWinsOverAttrs(t *testing.T) {
	t0 := time.Date(2026, 5, 16, 9, 30, 0, 0, time.UTC)
	msg := &consumptionv1.CompanionStageUp{
		Envelope: &commonv1.EventEnvelope{
			EventId:       "01971a90-0000-7000-8000-binary-CANON",
			TenantId:      "tenant-from-binary",
			Gcid:          "gcid-from-binary",
			OccurredAt:    timestamppb.New(t0),
			SchemaVersion: 1,
		},
		CompanionId: "fam-1",
		OwnerGcid:   "gcid-from-binary",
		StageFrom:   3,
		StageTo:     4,
		StageToName: "Teen",
	}
	bz, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}

	// Attrs disagree with the binary envelope — binary must win.
	attrs := map[string]string{
		"event_id":  "DIFFERENT-event-id-from-attrs",
		"tenant_id": "tenant-from-attrs",
		"gcid":      "gcid-from-attrs",
	}

	got, err := protodecode.DecodePayloadMapWithAttrs("chora.consumption.familiar.stage_up.v1", bz, attrs)
	if err != nil {
		t.Fatalf("DecodePayloadMapWithAttrs binary: %v", err)
	}
	if v, _ := got["event_id"].(string); v != "01971a90-0000-7000-8000-binary-CANON" {
		t.Errorf("event_id = %q, expected the binary canonical value (binary wins over attrs)", v)
	}
	if v, _ := got["tenant_id"].(string); v != "tenant-from-binary" {
		t.Errorf("tenant_id = %q, expected binary canonical", v)
	}
}

// TestDecodeWithAttrs_BinaryPayload_EmptyAttrs asserts the binary path
// continues to work when attrs are missing.
func TestDecodeWithAttrs_BinaryPayload_EmptyAttrs(t *testing.T) {
	t0 := time.Date(2026, 5, 16, 9, 30, 0, 0, time.UTC)
	msg := &consumptionv1.CompanionStageUp{
		Envelope: &commonv1.EventEnvelope{
			EventId:       "01971a90-0000-7000-8000-onlybinary",
			TenantId:      "tenant-acme",
			Gcid:          "gcid-phyllis",
			OccurredAt:    timestamppb.New(t0),
			SchemaVersion: 1,
		},
		CompanionId: "fam-1",
		OwnerGcid:   "gcid-phyllis",
		StageToName: "Teen",
	}
	bz, _ := proto.Marshal(msg)

	got, err := protodecode.DecodePayloadMapWithAttrs("chora.consumption.familiar.stage_up.v1", bz, nil)
	if err != nil {
		t.Fatalf("DecodePayloadMapWithAttrs nil attrs: %v", err)
	}
	if v, _ := got["event_id"].(string); v != "01971a90-0000-7000-8000-onlybinary" {
		t.Errorf("event_id = %q (binary envelope must populate with nil attrs)", v)
	}
	if v, _ := got["tenant_id"].(string); v != "tenant-acme" {
		t.Errorf("tenant_id = %q", v)
	}
}

// TestDecodeWithAttrs_EmptyPayload_FailsLoud asserts fail-loud is preserved.
func TestDecodeWithAttrs_EmptyPayload_FailsLoud(t *testing.T) {
	if _, err := protodecode.DecodePayloadMapWithAttrs("chora.consumption.familiar.stage_up.v1", nil, map[string]string{"event_id": "x"}); err == nil {
		t.Fatal("expected error on nil payload even when attrs present")
	}
}

// TestDecodeWithAttrs_NilAttrs_EqualsLegacyAPI asserts back-compat — passing
// nil attrs to DecodePayloadMapWithAttrs must behave like DecodePayloadMap.
func TestDecodeWithAttrs_NilAttrs_EqualsLegacyAPI(t *testing.T) {
	payload := map[string]any{
		"familiar_id":   "fam-x",
		"stage_to_name": "Teen",
	}
	bz, _ := json.Marshal(payload)

	a, err := protodecode.DecodePayloadMap("chora.consumption.familiar.stage_up.v1", bz)
	if err != nil {
		t.Fatalf("legacy: %v", err)
	}
	b, err := protodecode.DecodePayloadMapWithAttrs("chora.consumption.familiar.stage_up.v1", bz, nil)
	if err != nil {
		t.Fatalf("with nil attrs: %v", err)
	}
	if av, _ := a["familiar_id"].(string); av != "fam-x" {
		t.Errorf("legacy familiar_id = %q", av)
	}
	if bv, _ := b["familiar_id"].(string); bv != "fam-x" {
		t.Errorf("with-attrs familiar_id = %q", bv)
	}
}

// -----------------------------------------------------------------------------
// Coverage tests for the 3 unexercised projectors (BreedRevealed, Hatched,
// SourceRevelation). The legacy DecodePayloadMap tests only cover StageUp;
// these tests round-trip binary protobuf for the remaining 3 topics.
// -----------------------------------------------------------------------------

func TestDecode_FamiliarBreedRevealed_Binary(t *testing.T) {
	t0 := time.Date(2026, 5, 16, 9, 30, 0, 0, time.UTC)
	msg := &consumptionv1.CompanionBreedRevealed{
		Envelope: &commonv1.EventEnvelope{
			EventId:       "01971a90-0000-7000-8000-breed",
			TenantId:      "tenant-acme",
			Gcid:          "gcid-phyllis",
			OccurredAt:    timestamppb.New(t0),
			SchemaVersion: 1,
		},
		CompanionId:       "fam-1",
		OwnerGcid:         "gcid-phyllis",
		ShinyVariant:      true,
		Rarity:            "rare",
		EggSku:            "egg-mystic-001",
		RolledProbability: 0.07,
		RevealedAt:        timestamppb.New(t0),
	}
	bz, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	got, err := protodecode.DecodePayloadMap("chora.consumption.familiar.breed_revealed.v1", bz)
	if err != nil {
		t.Fatalf("DecodePayloadMap breed_revealed: %v", err)
	}
	if v, _ := got["familiar_id"].(string); v != "fam-1" {
		t.Errorf("familiar_id = %q", v)
	}
	if v, _ := got["rarity"].(string); v != "rare" {
		t.Errorf("rarity = %q", v)
	}
	if v, _ := got["egg_sku"].(string); v != "egg-mystic-001" {
		t.Errorf("egg_sku = %q", v)
	}
	if v, _ := got["shiny_variant"].(bool); !v {
		t.Errorf("shiny_variant = %v", v)
	}
}

func TestDecode_FamiliarHatched_Binary(t *testing.T) {
	t0 := time.Date(2026, 5, 16, 9, 30, 0, 0, time.UTC)
	msg := &consumptionv1.CompanionHatched{
		Envelope: &commonv1.EventEnvelope{
			EventId:       "01971a90-0000-7000-8000-hatched",
			TenantId:      "tenant-acme",
			Gcid:          "gcid-phyllis",
			OccurredAt:    timestamppb.New(t0),
			SchemaVersion: 1,
		},
		CompanionId:    "fam-1",
		OwnerGcid:      "gcid-phyllis",
		DisplayName:    "Eira",
		ShinyVariant:   false,
		ResonantAtomId: "atom-1",
		Tone:           "gentle",
		LearnerPersona: "curious-explorer",
		HatchedAt:      timestamppb.New(t0),
	}
	bz, _ := proto.Marshal(msg)
	got, err := protodecode.DecodePayloadMap("chora.consumption.familiar.hatched.v1", bz)
	if err != nil {
		t.Fatalf("DecodePayloadMap hatched: %v", err)
	}
	if v, _ := got["display_name"].(string); v != "Eira" {
		t.Errorf("display_name = %q", v)
	}
	if v, _ := got["tone"].(string); v != "gentle" {
		t.Errorf("tone = %q", v)
	}
	if v, _ := got["learner_persona"].(string); v != "curious-explorer" {
		t.Errorf("learner_persona = %q", v)
	}
}

func TestDecode_FamiliarSourceRevelation_Binary(t *testing.T) {
	t0 := time.Date(2026, 5, 16, 9, 30, 0, 0, time.UTC)
	exp := t0.Add(48 * time.Hour)
	msg := &consumptionv1.CompanionSourceRevelation{
		Envelope: &commonv1.EventEnvelope{
			EventId:       "01971a90-0000-7000-8000-revelation",
			TenantId:      "tenant-acme",
			Gcid:          "gcid-phyllis",
			OccurredAt:    timestamppb.New(t0),
			SchemaVersion: 1,
		},
		CompanionId:           "fam-1",
		OwnerGcid:             "gcid-phyllis",
		PreviewLlmTier:        "pro",
		PreviewTools:          []string{"propose_kg_merge", "rag_atomic_search"},
		RevelationAt:          timestamppb.New(t0),
		WindowExpiresAt:       timestamppb.New(exp),
		WindowDurationSeconds: 172800,
	}
	bz, _ := proto.Marshal(msg)
	got, err := protodecode.DecodePayloadMap("chora.consumption.familiar.source_revelation.v1", bz)
	if err != nil {
		t.Fatalf("DecodePayloadMap source_revelation: %v", err)
	}
	if v, _ := got["preview_llm_tier"].(string); v != "pro" {
		t.Errorf("preview_llm_tier = %q", v)
	}
	if v, _ := got["window_duration_seconds"].(int); v != 172800 {
		t.Errorf("window_duration_seconds = %v", v)
	}
}

// TestDecode_LiveQuizScoreAwarded_Binary verifies the ADR-168 §2.4 binary
// decoder: chora-delivery emits LiveQuizSessionScoreAwarded as canonical
// protobuf; chora-sharing's subscriber must decode it (not JSON-fall-back) and
// project the snake_case keys its ScoreAwardedEnvelope reads, with the learner
// resolved from the envelope gcid.
func TestDecode_LiveQuizScoreAwarded_Binary(t *testing.T) {
	msg := &deliveryv1.LiveQuizSessionScoreAwarded{
		Envelope: &commonv1.EventEnvelope{
			EventId:       "01971a90-0000-7000-8000-0000000000aa",
			TenantId:      "tenant-acme",
			Gcid:          "gcid-learner",
			SchemaVersion: 1,
		},
		SessionId:       "sess-1",
		LiveQuizId:      "lq-1",
		QuestionId:      "q1",
		AwardedPoints:   950,
		CumulativeScore: 950,
		Correct:         true,
		AnswerMillis:    1200,
	}
	bz, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	got, err := protodecode.DecodePayloadMap("chora.delivery.live_quiz_session.score_awarded.v1", bz)
	if err != nil {
		t.Fatalf("DecodePayloadMap binary: %v", err)
	}
	if v, _ := got["gcid"].(string); v != "gcid-learner" {
		t.Errorf("gcid (learner) = %q, want envelope-sourced", v)
	}
	if v, _ := got["tenant_id"].(string); v != "tenant-acme" {
		t.Errorf("tenant_id = %q", v)
	}
	if v, _ := got["session_id"].(string); v != "sess-1" {
		t.Errorf("session_id = %q", v)
	}
	if v, _ := got["awarded_points"].(int); v != 950 {
		t.Errorf("awarded_points = %v (%T), want 950 int", got["awarded_points"], got["awarded_points"])
	}
	if v, _ := got["correct"].(bool); !v {
		t.Errorf("correct = %v, want true", got["correct"])
	}
}

// TestDecode_WeaknessGrown_Binary verifies a real proto-encoded WeaknessGrown
// message (chora.consumption.weakness.grown.v1, ADR-196 B1) round-trips into a
// snake_case map the WeaknessGrownSubscriber consumes. The learner is carried
// both in the envelope gcid AND the payload learner_gcid.
func TestDecode_WeaknessGrown_Binary(t *testing.T) {
	t0 := time.Date(2026, 6, 28, 10, 0, 0, 0, time.UTC)
	msg := &consumptionv1.WeaknessGrown{
		Envelope: &commonv1.EventEnvelope{
			EventId:       "01971a90-0000-7000-8000-0000000000cc",
			TenantId:      "tenant-acme",
			Gcid:          "gcid-learner",
			SchemaVersion: 1,
		},
		GrowthEdgeId:   "edge-1",
		TenantId:       "tenant-acme",
		LearnerGcid:    "gcid-learner",
		ConceptLabel:   "Single Responsibility Principle",
		ConceptKey:     "single-responsibility-principle",
		FinalStrength:  0.05,
		RecoverySource: "drill_atom",
		Tags:           []string{"oop", "design"},
		GrownAt:        timestamppb.New(t0),
	}
	bz, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	got, err := protodecode.DecodePayloadMap("chora.consumption.weakness.grown.v1", bz)
	if err != nil {
		t.Fatalf("DecodePayloadMap binary: %v", err)
	}
	if v, _ := got["event_id"].(string); v != "01971a90-0000-7000-8000-0000000000cc" {
		t.Errorf("event_id = %q, want envelope-sourced UUID", v)
	}
	if v, _ := got["tenant_id"].(string); v != "tenant-acme" {
		t.Errorf("tenant_id = %q", v)
	}
	if v, _ := got["learner_gcid"].(string); v != "gcid-learner" {
		t.Errorf("learner_gcid = %q", v)
	}
	if v, _ := got["growth_edge_id"].(string); v != "edge-1" {
		t.Errorf("growth_edge_id = %q", v)
	}
	if v, _ := got["concept_key"].(string); v != "single-responsibility-principle" {
		t.Errorf("concept_key = %q", v)
	}
	if v, _ := got["recovery_source"].(string); v != "drill_atom" {
		t.Errorf("recovery_source = %q", v)
	}
	if v, _ := got["grown_at"].(string); v != "2026-06-28T10:00:00Z" {
		t.Errorf("grown_at = %q, want RFC3339 2026-06-28T10:00:00Z", v)
	}
	tags, _ := got["tags"].([]any)
	if len(tags) != 2 {
		t.Errorf("tags = %v, want 2 entries", got["tags"])
	}
}
