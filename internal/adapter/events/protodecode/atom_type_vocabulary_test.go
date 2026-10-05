// Package protodecode_test — CHO-2178: the atom-type decoder totality guard.
//
// THE DEFECT THIS PINS. atomTypeToDomain was a hand-written reverse table that
// stopped at ATOM_TYPE_CODE (7). chora-creation happily emits ATOM_TYPE_ESSAY
// (8), so every essay atom decoded to "" and the projection wrote a BLANK
// question_type. Ten of them sat in prod that way — atom_projections held
// 101 mcq + 11 blank, and the 11 blanks were 10 essays that had lost their
// flavour plus one genuinely-untyped atom. chora-sharing FILTERS on that
// column, so an essay atom was unfindable by question-type filter, and nobody
// noticed because the row was still there and the consent facts were correct.
//
// The old tests only ever fed the decoder values it already knew. This one
// walks the CONTRACT — every value the generated descriptor declares — so a
// value the producer can emit but the consumer cannot name is now a build
// failure rather than a silently blanked column.
package protodecode_test

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protodecode"
	creationv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/creation/v1"
)

const atomPublishedTopic = "chora.creation.atom.published.v1"

// decodeQuestionType round-trips an AtomPublished carrying the given wire enum
// through the production decode surface and returns the projected label.
// A missing key and an empty label are the same failure (the pg upsert treats
// both as "no opinion"), so collapse them.
func decodeQuestionType(t *testing.T, v creationv1.AtomType) string {
	t.Helper()
	bz, err := proto.Marshal(&creationv1.AtomPublished{
		AtomId:       "01971a90-aaaa-7000-8000-000000000001",
		AuthorGcid:   "00000000-0000-7000-8000-000000001999",
		QuestionType: v,
	})
	if err != nil {
		t.Fatalf("marshal AtomPublished(question_type=%s): %v", v, err)
	}
	out, err := protodecode.DecodePayloadMap(atomPublishedTopic, bz)
	if err != nil {
		t.Fatalf("DecodePayloadMap(question_type=%s): %v", v, err)
	}
	label, _ := out["question_type"].(string)
	return label
}

// TestAtomTypeToDomain_TotalOverContract — every value the contract DECLARES
// must decode to a non-empty label. This is the RED test for the essay
// blanking: ATOM_TYPE_ESSAY / MULTIMEDIA / SIMULATION currently fall through to
// the default arm and return "".
func TestAtomTypeToDomain_TotalOverContract(t *testing.T) {
	for value, name := range creationv1.AtomType_name {
		if creationv1.AtomType(value) == creationv1.AtomType_ATOM_TYPE_UNSPECIFIED {
			continue // UNSPECIFIED legitimately means "no opinion"
		}
		if got := decodeQuestionType(t, creationv1.AtomType(value)); got == "" {
			t.Errorf("%s (%d) decodes to \"\" — chora-creation can emit this value, so the "+
				"projection silently loses the flavour and question_type filters can never "+
				"match the atom. The decoder must be TOTAL over the contract.", name, value)
		}
	}
}

// TestAtomTypeToDomain_DerivesLabelFromDescriptor — the decoder must derive the
// label from the enum's own name (lower(trim("ATOM_TYPE_"))) rather than
// hand-listing it, with the single documented exception MULTIPLE_CHOICE -> mcq.
// Derivation is what makes a NEW contract value correct here for free — the
// hand-written table is exactly what rotted.
func TestAtomTypeToDomain_DerivesLabelFromDescriptor(t *testing.T) {
	for value, name := range creationv1.AtomType_name {
		v := creationv1.AtomType(value)
		if v == creationv1.AtomType_ATOM_TYPE_UNSPECIFIED {
			continue
		}
		want := strings.ToLower(strings.TrimPrefix(name, "ATOM_TYPE_"))
		if v == creationv1.AtomType_ATOM_TYPE_MULTIPLE_CHOICE {
			want = "mcq" // the one pair whose label is not its wire name
		}
		if got := decodeQuestionType(t, v); got != want {
			t.Errorf("%s (%d) decodes to %q, want %q", name, value, got, want)
		}
	}
}

// TestAtomTypeToDomain_DomainFlavours — the five flavours chora-creation
// actually authors, spelled out. Belt-and-braces over the descriptor walk: if
// someone "fixes" the derivation by renaming a constant, this still fails.
func TestAtomTypeToDomain_DomainFlavours(t *testing.T) {
	for _, tc := range []struct {
		wire creationv1.AtomType
		want string
	}{
		{creationv1.AtomType_ATOM_TYPE_MULTIPLE_CHOICE, "mcq"},
		{creationv1.AtomType_ATOM_TYPE_ESSAY, "essay"},
		{creationv1.AtomType_ATOM_TYPE_OUTLINE, "outline"},
		{creationv1.AtomType_ATOM_TYPE_FLASHCARD, "flashcard"},
		{creationv1.AtomType_ATOM_TYPE_VIDEO, "video"},
	} {
		if got := decodeQuestionType(t, tc.wire); got != tc.want {
			t.Errorf("%s decodes to %q, want %q", tc.wire, got, tc.want)
		}
	}
}

// TestAtomTypeToDomain_UnspecifiedStaysAbsent — UNSPECIFIED must leave the key
// ABSENT, not write an empty string. The pg upsert is non-blanking
// (COALESCE(NULLIF(EXCLUDED.question_type,''), existing)), and a pre-CHO-2178
// producer that never set the field must not clobber a flavour a later event
// already established.
func TestAtomTypeToDomain_UnspecifiedStaysAbsent(t *testing.T) {
	if got := decodeQuestionType(t, creationv1.AtomType_ATOM_TYPE_UNSPECIFIED); got != "" {
		t.Errorf("ATOM_TYPE_UNSPECIFIED decoded to %q — it must leave question_type absent", got)
	}
}
