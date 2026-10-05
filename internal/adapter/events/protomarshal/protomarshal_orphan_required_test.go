// protomarshal_orphan_required_test.go — ADR-229 Amendment A1 (CHO-2132)
// binary encoding for chora.sharing.atom_reuse.orphan_required.v1.
//
// RED before the encoder lands: without a registered case the outbox row
// falls back through ErrUnsupportedTopic and dead-letters against the BINARY
// schema chora-sharing-atom_reuse-orphan_required-v1. Round-trip asserts via
// the generated chora/sharing/v1 binding so the wire bytes stay pinned.
package protomarshal_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protomarshal"
	sharingv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/sharing/v1"
)

func TestMarshalPayload_AtomReuseOrphanRequired_RoundTrips(t *testing.T) {
	detected := time.Date(2026, 7, 11, 10, 0, 0, 0, time.UTC)
	env := protomarshal.Envelope{
		EventID:        "01970000-0000-7000-8000-00000000e001",
		IdempotencyKey: "01970000-0000-7000-8000-00000000e001",
		TenantID:       "01970000-0000-7111-8111-000000000001",
		GCID:           "01970000-0000-7000-8000-0000000000aa",
		OccurredAt:     detected,
		PublishedAt:    detected,
		SourceProject:  "chora-489812",
		SourceService:  "chora-sharing",
		SchemaVersion:  1,
	}
	payload := map[string]any{
		"atom_id":              "01970000-0000-7000-8000-0000000000a1",
		"revision_id":          "01970000-0000-7000-8000-0000000000f1",
		"trigger":              "narrowed",
		"stranded_grant_count": int32(2),
		"detected_at":          detected,
	}

	bz, err := protomarshal.MarshalPayload("chora.sharing.atom_reuse.orphan_required.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	var m sharingv1.AtomReuseOrphanRequired
	if err := proto.Unmarshal(bz, &m); err != nil {
		t.Fatalf("proto.Unmarshal AtomReuseOrphanRequired: %v", err)
	}
	if m.GetAtomId() != "01970000-0000-7000-8000-0000000000a1" {
		t.Errorf("atom_id = %q (field 2)", m.GetAtomId())
	}
	if m.GetRevisionId() != "01970000-0000-7000-8000-0000000000f1" {
		t.Errorf("revision_id = %q (field 3)", m.GetRevisionId())
	}
	if m.GetTrigger() != "narrowed" {
		t.Errorf("trigger = %q (field 4)", m.GetTrigger())
	}
	if m.GetStrandedGrantCount() != 2 {
		t.Errorf("stranded_grant_count = %d (field 5)", m.GetStrandedGrantCount())
	}
	if got := m.GetDetectedAt(); got == nil || got.AsTime() != detected {
		t.Errorf("detected_at = %v (field 6)", got)
	}
	if m.GetEnvelope().GetTenantId() != env.TenantID || m.GetEnvelope().GetEventId() != env.EventID {
		t.Errorf("envelope not carried: %+v", m.GetEnvelope())
	}
}

func TestMarshalPayload_AtomReuseOrphanRequired_WrongTypesFailLoud(t *testing.T) {
	env := protomarshal.Envelope{EventID: "e1", TenantID: "t1"}
	if _, err := protomarshal.MarshalPayload("chora.sharing.atom_reuse.orphan_required.v1", env, map[string]any{
		"atom_id": 42,
	}); err == nil {
		t.Fatalf("int atom_id must fail loud")
	}
	if _, err := protomarshal.MarshalPayload("chora.sharing.atom_reuse.orphan_required.v1", env, map[string]any{
		"stranded_grant_count": "two",
	}); err == nil {
		t.Fatalf("string stranded_grant_count must fail loud")
	}
	if _, err := protomarshal.MarshalPayload("chora.sharing.atom_reuse.orphan_required.v1", env, map[string]any{
		"detected_at": "yesterday",
	}); err == nil {
		t.Fatalf("string detected_at must fail loud")
	}
}
