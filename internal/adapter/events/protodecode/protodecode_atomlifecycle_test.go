// protodecode_atomlifecycle_test.go — RED specs for the creation atom
// lifecycle binary decoders (WS-0 deploy defect, CHO-2121).
//
// Deployed-reality catch (2026-07-11 deploy): chora-creation publishes
// chora.creation.atom.{published,archived}.v1 as BINARY protobuf, but the
// sharing protodecode registry had no entries for either topic — every
// message fell through to the JSON path and NACK-looped
// ("neither binary-decodable nor JSON-decodable"), so atom_projections
// never hydrated and ShareAtom kept 412ing. Identical failure mode to
// consumption's OPEN-1 (atom.created, fixed 2026-06-01). These specs pin
// the registry entries + projector key set the AtomProjectionSubscriber
// reads.
package protodecode_test

import (
	"testing"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	creationv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/creation/v1"
	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protodecode"
)

func TestDecodePayloadMap_AtomPublished_Binary(t *testing.T) {
	t.Parallel()
	msg := &creationv1.AtomPublished{
		Envelope: &commonv1.EventEnvelope{
			EventId:  "evt-1",
			TenantId: "tenant-1",
			Gcid:     "gcid-author",
		},
		AtomId:            "atom-1",
		QuestionType:      creationv1.AtomType_ATOM_TYPE_MULTIPLE_CHOICE,
		Status:            creationv1.AtomStatus_ATOM_STATUS_PUBLISHED,
		Title:             "Photosynthesis basics",
		AuthorGcid:        "gcid-author",
		CurrentRevisionId: "rev-1",
		Stem:              "What does chlorophyll absorb?",
	}
	blob, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}

	out, err := protodecode.DecodePayloadMap("chora.creation.atom.published.v1", blob)
	if err != nil {
		t.Fatalf("DecodePayloadMap must binary-decode atom.published: %v", err)
	}
	want := map[string]string{
		"event_id":            "evt-1",
		"tenant_id":           "tenant-1",
		"gcid":                "gcid-author",
		"atom_id":             "atom-1",
		"current_revision_id": "rev-1",
		"author_gcid":         "gcid-author",
		"title":               "Photosynthesis basics",
		"stem":                "What does chlorophyll absorb?",
		"question_type":       "mcq",
		"status":              "published",
	}
	for k, v := range want {
		if got, _ := out[k].(string); got != v {
			t.Errorf("out[%q] = %q; want %q (full=%v)", k, got, v, out)
		}
	}
}

func TestDecodePayloadMap_AtomArchived_Binary(t *testing.T) {
	t.Parallel()
	msg := &creationv1.AtomArchived{
		Envelope: &commonv1.EventEnvelope{
			EventId:  "evt-2",
			TenantId: "tenant-1",
			Gcid:     "gcid-archiver",
		},
		AtomId: "atom-1",
		Status: creationv1.AtomStatus_ATOM_STATUS_ARCHIVED,
	}
	blob, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}

	out, err := protodecode.DecodePayloadMap("chora.creation.atom.archived.v1", blob)
	if err != nil {
		t.Fatalf("DecodePayloadMap must binary-decode atom.archived: %v", err)
	}
	want := map[string]string{
		"event_id":  "evt-2",
		"tenant_id": "tenant-1",
		"atom_id":   "atom-1",
		"status":    "archived",
	}
	for k, v := range want {
		if got, _ := out[k].(string); got != v {
			t.Errorf("out[%q] = %q; want %q (full=%v)", k, got, v, out)
		}
	}
}
