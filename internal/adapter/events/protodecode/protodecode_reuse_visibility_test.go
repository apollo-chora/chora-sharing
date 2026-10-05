// protodecode_reuse_visibility_test.go — RED specs for the ADR-229 WS-1
// (CHO-2127) reuse-consent decode surface:
//
//   - atom.published.v1 now carries reuse_visibility (field 30); the
//     projector must surface the lowercase label so the subscriber maps it
//     onto atom_projections.reuse_visibility.
//   - chora.creation.atom.reuse_visibility_changed.v1 is a NEW binary topic;
//     without a registry entry it would fall through to the JSON path and
//     NACK-loop (the exact WS-0 deploy defect these lifecycle decoders fixed).
package protodecode_test

import (
	"testing"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	creationv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/creation/v1"
	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protodecode"
)

func TestDecodePayloadMap_AtomPublished_CarriesReuseVisibility(t *testing.T) {
	t.Parallel()
	msg := &creationv1.AtomPublished{
		Envelope: &commonv1.EventEnvelope{
			EventId:  "evt-rv-1",
			TenantId: "tenant-1",
			Gcid:     "gcid-author",
		},
		AtomId:            "atom-rv-1",
		Status:            creationv1.AtomStatus_ATOM_STATUS_PUBLISHED,
		AuthorGcid:        "gcid-author",
		CurrentRevisionId: "rev-1",
		ReuseVisibility:   creationv1.ReuseVisibility_REUSE_VISIBILITY_TENANT,
	}
	blob, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}

	out, err := protodecode.DecodePayloadMap("chora.creation.atom.published.v1", blob)
	if err != nil {
		t.Fatalf("DecodePayloadMap: %v", err)
	}
	if got, _ := out["reuse_visibility"].(string); got != "tenant" {
		t.Errorf("out[reuse_visibility] = %q; want tenant", got)
	}
}

// A pre-ADR-229 producer never sets field 30 — the key must be ABSENT (not
// hardened here) so the SQL preserves an already-set audience on re-publish.
func TestDecodePayloadMap_AtomPublished_OmitsUnsetReuseVisibility(t *testing.T) {
	t.Parallel()
	msg := &creationv1.AtomPublished{
		Envelope:          &commonv1.EventEnvelope{EventId: "evt-rv-2", TenantId: "tenant-1", Gcid: "g"},
		AtomId:            "atom-rv-2",
		AuthorGcid:        "g",
		CurrentRevisionId: "rev-1",
	}
	blob, _ := proto.Marshal(msg)
	out, err := protodecode.DecodePayloadMap("chora.creation.atom.published.v1", blob)
	if err != nil {
		t.Fatalf("DecodePayloadMap: %v", err)
	}
	if v, present := out["reuse_visibility"]; present {
		t.Errorf("out[reuse_visibility] present as %v; want ABSENT for an unset field", v)
	}
}

func TestDecodePayloadMap_AtomReuseVisibilityChanged_Binary(t *testing.T) {
	t.Parallel()
	msg := &creationv1.AtomReuseVisibilityChanged{
		Envelope: &commonv1.EventEnvelope{
			EventId:  "evt-rv-3",
			TenantId: "tenant-1",
			Gcid:     "gcid-author",
		},
		AtomId:             "atom-rv-3",
		ReuseVisibility:    creationv1.ReuseVisibility_REUSE_VISIBILITY_FRIENDS,
		PreviousVisibility: creationv1.ReuseVisibility_REUSE_VISIBILITY_TENANT,
		AuthorGcid:         "gcid-author",
	}
	blob, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}

	out, err := protodecode.DecodePayloadMap("chora.creation.atom.reuse_visibility_changed.v1", blob)
	if err != nil {
		t.Fatalf("DecodePayloadMap must binary-decode reuse_visibility_changed: %v", err)
	}
	want := map[string]string{
		"event_id":            "evt-rv-3",
		"tenant_id":           "tenant-1",
		"gcid":                "gcid-author",
		"atom_id":             "atom-rv-3",
		"reuse_visibility":    "friends",
		"previous_visibility": "tenant",
		"author_gcid":         "gcid-author",
	}
	for k, v := range want {
		if got, _ := out[k].(string); got != v {
			t.Errorf("out[%q] = %q; want %q (full=%v)", k, got, v, out)
		}
	}
}
