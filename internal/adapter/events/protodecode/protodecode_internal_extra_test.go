// Internal-only tests that drive the projectors' defensive type guards.
//
// The guards (msg.(*T); if !ok || m == nil { return }) are unreachable
// through the public DecodePayloadMap API — the registry's decoders always
// produce the exact message type each projector owns, and the dispatch
// checks `err == nil && msg != nil && entry.project != nil` first. They are
// exercised here directly so the defensive branch is pinned, mirroring the
// *_internal_test.go convention used across chora-go-common.
package protodecode

import (
	"testing"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
)

func TestProjectors_IgnoreWrongMessageTypes(t *testing.T) {
	out := map[string]any{}
	wrong := proto.Message(&commonv1.EventEnvelope{})

	projectAtomPublished(wrong, out)
	projectAtomReuseVisibilityChanged(wrong, out)
	projectAtomOrphanCreated(wrong, out)
	projectAtomArchived(wrong, out)
	projectLiveQuizScoreAwarded(wrong, out)
	projectWeaknessGrown(wrong, out)
	projectFamiliarStageUp(wrong, out)
	projectFamiliarBreedRevealed(wrong, out)
	projectFamiliarHatched(wrong, out)
	projectFamiliarSourceRevelation(wrong, out)
	projectCoursePublished(wrong, out)

	if len(out) != 0 {
		t.Fatalf("wrong-typed messages must leave the map untouched, got %v", out)
	}
}