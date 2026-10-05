// Package protodecode_test — CHO-2259: the FamiliarSpecies decoder totality guard.
//
// THE DEFECT THIS PINS. projectFamiliarBreedRevealed / projectFamiliarHatched
// did `out["species"] = v.String()`, so the generated enum NAME reached learner
// copy. The live hatched draft in prod reads:
//
//	Meet Tempo, a common FAMILIAR_SPECIES_FOX with a shiny shimmer!
//
// Nobody saw it for the whole life of the lane because the default policy is
// `draft` and nothing could read a draft until CHO-2258.
//
// Mirrors atom_type_vocabulary_test.go (CHO-2178, same file, same defect class):
// walk the CONTRACT via the generated descriptor and assert through the
// PRODUCTION decode surface, so a value the producer can emit but the consumer
// cannot name is a build failure rather than a silent leak. A test that fed the
// decoder only the values it already knew is exactly what let CHO-2178 through.
package protodecode_test

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protodecode"
	consumptionv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/consumption/v1"
)

const (
	breedRevealedTopic = "chora.consumption.familiar.breed_revealed.v1"
	hatchedTopic       = "chora.consumption.familiar.hatched.v1"
)

// decodeSpecies round-trips a FamiliarBreedRevealed carrying the given wire enum
// through the production decode surface and returns the projected label.
// A missing key and an empty label are the same outcome (absent), so collapse them.
func decodeSpecies(t *testing.T, v consumptionv1.CompanionSpecies) string {
	t.Helper()
	bz, err := proto.Marshal(&consumptionv1.CompanionBreedRevealed{
		CompanionId: "019f6eba-0000-7000-a000-000000000001",
		OwnerGcid:   "019f65a7-ce2a-739b-b563-d54653d2b0b6",
		Species:     v,
		Rarity:      "common",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out, err := protodecode.DecodePayloadMap(breedRevealedTopic, bz)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	label, _ := out["species"].(string)
	return label
}

// Every species the CONTRACT declares must decode to learner-safe plain
// language. Cases come from the generated name table, so adding
// FAMILIAR_SPECIES_BADGER to the proto covers it here automatically.
func TestSpeciesVocabulary_IsTotalOverTheContract(t *testing.T) {
	t.Parallel()
	if len(consumptionv1.CompanionSpecies_name) < 2 {
		t.Fatalf("contract declares %d FamiliarSpecies values; expected UNSPECIFIED + the species",
			len(consumptionv1.CompanionSpecies_name))
	}
	for num, name := range consumptionv1.CompanionSpecies_name {
		v := consumptionv1.CompanionSpecies(num)
		got := decodeSpecies(t, v)

		if v == consumptionv1.CompanionSpecies_COMPANION_SPECIES_UNSPECIFIED {
			// The proto3 zero means the producer sent NO species. Absent — never
			// rendered to a learner as a species called "unspecified".
			if got != "" {
				t.Errorf("UNSPECIFIED must decode to absent; got %q", got)
			}
			continue
		}
		if got == "" {
			t.Errorf("%s (%d): declared in the contract but decodes to ABSENT — the producer can emit "+
				"a species this consumer cannot name", name, num)
			continue
		}
		if strings.Contains(got, "_") || strings.ToLower(got) != got {
			t.Errorf("%s (%d): label %q is not learner-facing plain language", name, num, got)
		}
		if strings.Contains(strings.ToUpper(got), "FAMILIAR_SPECIES") {
			t.Errorf("%s (%d): label %q still carries the proto identifier — this is the CHO-2259 leak",
				name, num, got)
		}
	}
}

// The label must read as the animal a learner would recognise.
func TestSpeciesVocabulary_RendersTheAnimal(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		in   consumptionv1.CompanionSpecies
		want string
	}{
		{consumptionv1.CompanionSpecies_COMPANION_SPECIES_FOX, "fox"},
		{consumptionv1.CompanionSpecies_COMPANION_SPECIES_OWL, "owl"},
		{consumptionv1.CompanionSpecies_COMPANION_SPECIES_DRAGON, "dragon"},
		{consumptionv1.CompanionSpecies_COMPANION_SPECIES_PENGUIN, "penguin"},
	} {
		if got := decodeSpecies(t, tc.in); got != tc.want {
			t.Errorf("%v = %q; want %q", tc.in, got, tc.want)
		}
	}
}

// A value outside the contract (a producer deployed ahead of this consumer) must
// leave the species ABSENT — never the digits String() would return, and never a
// guess. The warn-once log is the loud half.
func TestSpeciesVocabulary_OffContractValueDecodesToAbsent(t *testing.T) {
	t.Parallel()
	if got := decodeSpecies(t, consumptionv1.CompanionSpecies(9999)); got != "" {
		t.Errorf("an off-contract species must decode to absent; got %q", got)
	}
}

// hatched is the one message carrying a name, and it is where the live leak was
// sighted — pin the whole projection, not just breed_revealed.
func TestSpeciesVocabulary_HatchedProjectsLabelBesideTheName(t *testing.T) {
	t.Parallel()
	bz, err := proto.Marshal(&consumptionv1.CompanionHatched{
		CompanionId: "019f6eba-0000-7000-a000-000000000001",
		OwnerGcid:   "019f65a7-ce2a-739b-b563-d54653d2b0b6",
		DisplayName: "Tempo",
		Species:     consumptionv1.CompanionSpecies_COMPANION_SPECIES_FOX,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out, err := protodecode.DecodePayloadMap(hatchedTopic, bz)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got, _ := out["species"].(string); got != "fox" {
		t.Errorf("hatched species = %q; want %q", got, "fox")
	}
	if got, _ := out["display_name"].(string); got != "Tempo" {
		t.Errorf("hatched display_name = %q; want %q (the one name the wire carries)", got, "Tempo")
	}
}
