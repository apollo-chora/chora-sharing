// protodecode_orphan_created_test.go — ADR-229 Amendment A1 (CHO-2132):
// chora.creation.atom.orphan_created.v1 must binary-decode via the generated
// binding FROM BIRTH (the atom-lifecycle entries earned this rule the hard
// way: an unregistered binary topic falls back to JSON-decode and NACK-loops
// its very first live message).
package protodecode_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protodecode"
	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	creationv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/creation/v1"
)

func TestDecodePayloadMap_AtomOrphanCreated_Binary(t *testing.T) {
	msg := &creationv1.AtomOrphanCreated{
		Envelope: &commonv1.EventEnvelope{
			EventId:  "01970000-0000-7000-8000-00000000e001",
			TenantId: "01970000-0000-7111-8111-000000000001",
			Gcid:     "01970000-0000-7000-8000-0000000000aa",
		},
		OrphanAtomId:       "01970000-0000-7000-8000-0000000000e1",
		OrphanedFromAtomId: "01970000-0000-7000-8000-0000000000a1",
		SourceRevisionId:   "01970000-0000-7000-8000-0000000000f1",
		AuthorGcid:         "01970000-0000-7000-8000-0000000000aa",
		Trigger:            "narrowed",
		OrphanedAt:         timestamppb.New(time.Date(2026, 7, 11, 9, 0, 0, 0, time.UTC)),
	}
	bz, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}

	m, err := protodecode.DecodePayloadMapWithAttrs("chora.creation.atom.orphan_created.v1", bz, nil)
	if err != nil {
		t.Fatalf("DecodePayloadMapWithAttrs: %v", err)
	}
	want := map[string]string{
		"event_id":              "01970000-0000-7000-8000-00000000e001",
		"tenant_id":             "01970000-0000-7111-8111-000000000001",
		"orphan_atom_id":        "01970000-0000-7000-8000-0000000000e1",
		"orphaned_from_atom_id": "01970000-0000-7000-8000-0000000000a1",
		"source_revision_id":    "01970000-0000-7000-8000-0000000000f1",
		"author_gcid":           "01970000-0000-7000-8000-0000000000aa",
		"trigger":               "narrowed",
	}
	for k, v := range want {
		if got, _ := m[k].(string); got != v {
			t.Errorf("m[%q] = %q, want %q (full map: %v)", k, got, v, m)
		}
	}
}
