// species_label.go: CHO-2259 CompanionSpecies → learner-facing plain language.
//
// THE DEFECT THIS FIXES. projectCompanionBreedRevealed / projectCompanionHatched
// did `out["species"] = v.String()`. String() on a protobuf enum yields the
// generated NAME, so the milestone composer dropped an internal identifier
// straight into a C+ post body: the live hatched draft reads
//
//	Meet Tempo, a common COMPANION_SPECIES_FOX with a shiny shimmer!
//
// It survived because the lane's default policy is `draft` and nothing could
// read a draft until CHO-2258 — the first person to read one found it at once.
//
// This mirrors atomTypeToDomain (CHO-2178) deliberately: same file, same defect
// class, same derivation law. A hand-written label↔enum map is what CHO-2178
// was fixed FOR — it stopped at ATOM_TYPE_CODE while the producer emitted
// ATOM_TYPE_ESSAY, and ten atoms silently lost their flavour. Derivation removes
// the lockstep obligation: a species added to the contract labels correctly here
// with no code change.
package protodecode

import (
	"log"
	"strings"
	"sync"

	consumptionv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/consumption/v1"
)

// speciesToLabel returns the learner-facing label for a CompanionSpecies
// ("fox", "owl", "dragon"), or "" when there is nothing honest to say.
//
// Every declared value derives from its own name by the same law
// (COMPANION_SPECIES_FOX → fox), so the mapping is total over the contract by
// construction; species_vocabulary_test.go walks the descriptor to prove it.
func speciesToLabel(v consumptionv1.CompanionSpecies) string {
	// UNSPECIFIED means the producer sent no species. Absent, not a species
	// called "unspecified" — the composer omits the clause rather than stating
	// a fact the wire never carried.
	if v == consumptionv1.CompanionSpecies_COMPANION_SPECIES_UNSPECIFIED {
		return ""
	}
	name, declared := consumptionv1.CompanionSpecies_name[int32(v)]
	if !declared {
		// A value this binary's contract does not know — only reachable if a
		// producer was deployed AHEAD of this consumer. Say so loudly rather
		// than leaking the digits String() would return, or blanking silently.
		warnUnknownSpeciesOnce(int32(v))
		return ""
	}
	return strings.ToLower(strings.TrimPrefix(name, speciesEnumPrefix()))
}

// speciesEnumPrefix derives the enum's value-name prefix from the contract
// itself rather than repeating it as a literal.
//
// A hardcoded "FAMILIAR_SPECIES_" survived the ADR-254 D9 rename silently at
// the type level and turned every label back into the identifier this file
// exists to keep out of a C+ post ("companion_species_fox"). The prefix is
// exactly the zero value's name minus "UNSPECIFIED", which protobuf's own
// naming rule guarantees, so deriving it makes the next rename a no-op here
// instead of a defect that only a reader of a published post would notice.
func speciesEnumPrefix() string {
	return strings.TrimSuffix(
		consumptionv1.CompanionSpecies_COMPANION_SPECIES_UNSPECIFIED.String(),
		"UNSPECIFIED",
	)
}

// putSpeciesLabel writes the label onto the decoded map, omitting the key when
// there is no honest label.
//
// Omission over failure is deliberate: the species is a cosmetic clause in one
// sentence, and NACKing a whole milestone over it would cost the learner the
// event (this lane has no DLQ — CHO-2225 item 1 — so a NACK loop expires
// silently). The composer renders copy that simply does not name the species.
func putSpeciesLabel(v consumptionv1.CompanionSpecies, out map[string]any) {
	if label := speciesToLabel(v); label != "" {
		out["species"] = label
	}
}

// warnUnknownSpeciesOnce logs each unrecognised CompanionSpecies wire value a
// single time — enough to page a human, not enough to flood the log on a
// replayed backlog. Mirrors warnUnknownAtomTypeOnce.
var (
	warnedSpeciesMu sync.Mutex
	warnedSpecies   = map[int32]bool{}
)

func warnUnknownSpeciesOnce(v int32) {
	warnedSpeciesMu.Lock()
	defer warnedSpeciesMu.Unlock()
	if warnedSpecies[v] {
		return
	}
	warnedSpecies[v] = true
	log.Printf("WARN protodecode: chora.consumption.v1.CompanionSpecies value %d is not declared in "+
		"this build's contract — the species will be left ABSENT from the milestone copy. A producer "+
		"is running ahead of chora-sharing; redeploy this service against current chora-contracts.", v)
}
