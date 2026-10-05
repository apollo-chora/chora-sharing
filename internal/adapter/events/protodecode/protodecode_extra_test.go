// Edge coverage for the decode surface: binary decoder failure branches for
// every registered topic, the both-decode-fail path, IMDA attribute/envelope
// projection, the atom enum edge values (reuse_visibility=private, status=
// draft, unknown AtomType), the unknown-topic one-shot-WARN re-entry, and
// the unknown-species one-shot-WARN re-entry.
package protodecode_test

import (
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	consumptionv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/consumption/v1"
	creationv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/creation/v1"
	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protodecode"
)

// TestDecode_BinaryRegisteredTopic_BadBytesFailsLoud drives every binary
// decoder's error branch with bytes that are neither valid proto nor valid
// JSON - the decoder must fail loud (never silently return a partial map).
func TestDecode_BinaryRegisteredTopic_BadBytesFailsLoud(t *testing.T) {
	topics := []string{
		"chora.consumption.familiar.breed_revealed.v1",
		"chora.consumption.familiar.source_revelation.v1",
		"chora.delivery.live_quiz_session.score_awarded.v1",
		"chora.delivery.course.published.v1",
		"chora.consumption.weakness.grown.v1",
		"chora.creation.atom.published.v1",
		"chora.creation.atom.archived.v1",
		"chora.creation.atom.reuse_visibility_changed.v1",
		"chora.creation.atom.orphan_created.v1",
	}
	for _, topic := range topics {
		t.Run(topic, func(t *testing.T) {
			if _, err := protodecode.DecodePayloadMap(topic, []byte("%%%not-proto-not-json%%%")); err == nil {
				t.Fatalf("expected error for undecodable bytes on %s", topic)
			}
		})
	}
}

// TestDecodeWithAttrs_CarriesIMDAAttrs covers the IMDA envelope attributes
// merge keys on the JSON fallback path.
func TestDecodeWithAttrs_CarriesIMDAAttrs(t *testing.T) {
	bz, _ := json.Marshal(map[string]any{"familiar_id": "fam-1"})
	attrs := map[string]string{
		"chora_imda_dimension": "accountability",
		"imda_lifecycle_stage": "runtime",
	}
	got, err := protodecode.DecodePayloadMapWithAttrs("chora.consumption.familiar.stage_up.v1", bz, attrs)
	if err != nil {
		t.Fatalf("DecodePayloadMapWithAttrs: %v", err)
	}
	if v, _ := got["chora_imda_dimension"].(string); v != "accountability" {
		t.Errorf("chora_imda_dimension = %q, want accountability", v)
	}
	if v, _ := got["imda_lifecycle_stage"].(string); v != "runtime" {
		t.Errorf("imda_lifecycle_stage = %q, want runtime", v)
	}
}

// TestDecode_FamiliarStageUp_Binary_FullProjection covers the envelope
// tracestate + IMDA fields and the slice/string branches of the StageUp
// projector that happy-path fixtures leave unset.
func TestDecode_FamiliarStageUp_Binary_FullProjection(t *testing.T) {
	t0 := time.Date(2026, 5, 16, 9, 30, 0, 0, time.UTC)
	msg := &consumptionv1.CompanionStageUp{
		Envelope: &commonv1.EventEnvelope{
			EventId:                "01971a90-0000-7000-8000-0000000000ee",
			TenantId:               "tenant-acme",
			Gcid:                   "gcid-phyllis",
			Traceparent:            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
			Tracestate:             "chora=stage_up",
			ChoraImdaDimension:     "accountability",
			ImdaLifecycleStage:     "runtime",
			OccurredAt:             timestamppb.New(t0),
			SchemaVersion:          1,
		},
		CompanionId:                 "fam-1",
		OwnerGcid:                  "gcid-phyllis",
		StageFrom:                  3,
		StageTo:                    4,
		NewlyUnlockedTools:         []string{"propose_kg_merge"},
		NewlyRevealedKgNeighbors:   []string{"kw-1", "kw-2"},
		NewMemoryMode:              "longterm",
		StageUpAt:                  timestamppb.New(t0),
	}
	bz, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}

	got, err := protodecode.DecodePayloadMap("chora.consumption.familiar.stage_up.v1", bz)
	if err != nil {
		t.Fatalf("DecodePayloadMap: %v", err)
	}
	if v, _ := got["tracestate"].(string); v != "chora=stage_up" {
		t.Errorf("tracestate = %q", v)
	}
	if v, _ := got["chora_imda_dimension"].(string); v != "accountability" {
		t.Errorf("chora_imda_dimension = %q", v)
	}
	if v, _ := got["imda_lifecycle_stage"].(string); v != "runtime" {
		t.Errorf("imda_lifecycle_stage = %q", v)
	}
	if tools, _ := got["newly_unlocked_tools"].([]any); len(tools) != 1 {
		t.Errorf("newly_unlocked_tools = %v", got["newly_unlocked_tools"])
	}
	if neighbors, _ := got["newly_revealed_kg_neighbors"].([]any); len(neighbors) != 2 {
		t.Errorf("newly_revealed_kg_neighbors = %v", got["newly_revealed_kg_neighbors"])
	}
	if v, _ := got["new_memory_mode"].(string); v != "longterm" {
		t.Errorf("new_memory_mode = %q", v)
	}
}

// TestDecode_AtomPublished_EnumEdgeValues covers the reuse PRIVATE label,
// the DRAFT status label, and the unknown-AtomType path (warn-once, then
// absent question_type). Compile-time link: these constants exist on the
// generated enum, so the values are real wire shapes.
func TestDecode_AtomPublished_EnumEdgeValues(t *testing.T) {
	for i := 0; i < 2; i++ { // second pass covers the warn-once early return
		msg := &creationv1.AtomPublished{
			Envelope: &commonv1.EventEnvelope{
				EventId:  "evt-999",
				TenantId: "tenant-1",
				Gcid:     "gcid-author",
			},
			AtomId:          "atom-999",
			AuthorGcid:      "gcid-author",
			ReuseVisibility: creationv1.ReuseVisibility_REUSE_VISIBILITY_PRIVATE,
			Status:          creationv1.AtomStatus_ATOM_STATUS_DRAFT,
			QuestionType:    creationv1.AtomType(999),
		}
		blob, err := proto.Marshal(msg)
		if err != nil {
			t.Fatalf("marshal fixture: %v", err)
		}
		out, err := protodecode.DecodePayloadMap("chora.creation.atom.published.v1", blob)
		if err != nil {
			t.Fatalf("DecodePayloadMap: %v", err)
		}
		if v, _ := out["reuse_visibility"].(string); v != "private" {
			t.Errorf("reuse_visibility = %q, want private", v)
		}
		if v, _ := out["status"].(string); v != "draft" {
			t.Errorf("status = %q, want draft", v)
		}
		if _, ok := out["question_type"]; ok {
			t.Errorf("question_type must be ABSENT for unknown AtomType, got %v", out["question_type"])
		}
	}
}

// TestDecode_UnknownTopic_SecondCallSameTopic covers the one-shot WARN
// guard: the same unknown topic is warned once, then the guard returns.
func TestDecode_UnknownTopic_SecondCallSameTopic(t *testing.T) {
	bz, _ := json.Marshal(map[string]any{"foo": "bar"})
	for i := 0; i < 2; i++ {
		got, err := protodecode.DecodePayloadMap("chora.unknown.warn-once.v1", bz)
		if err != nil {
			t.Fatalf("decode pass %d: %v", i, err)
		}
		if v, _ := got["foo"].(string); v != "bar" {
			t.Fatalf("pass %d: foo = %q", i, v)
		}
	}
}

// TestDecode_FamiliarBreedRevealed_UnknownSpeciesTwice covers the
// species warn-once guard: an undeclared FamiliarSpecies leaves the
// species key ABSENT (never an internal identifier in learner copy).
func TestDecode_FamiliarBreedRevealed_UnknownSpeciesTwice(t *testing.T) {
	for i := 0; i < 2; i++ {
		msg := &consumptionv1.CompanionBreedRevealed{
			Envelope: &commonv1.EventEnvelope{
				EventId:  "evt-species",
				TenantId: "tenant-acme",
				Gcid:     "gcid-phyllis",
			},
			CompanionId:   "fam-1",
			OwnerGcid:    "gcid-phyllis",
			Species:      consumptionv1.CompanionSpecies(999),
			ShinyVariant: false,
		}
		blob, err := proto.Marshal(msg)
		if err != nil {
			t.Fatalf("marshal fixture: %v", err)
		}
		out, err := protodecode.DecodePayloadMap("chora.consumption.familiar.breed_revealed.v1", blob)
		if err != nil {
			t.Fatalf("DecodePayloadMap pass %d: %v", i, err)
		}
		if _, ok := out["species"]; ok {
			t.Fatalf("pass %d: species must be ABSENT for unknown species, got %v", i, out["species"])
		}
	}
}