// protomarshal_extended_wire_test.go — wire-compat round-trips for the event
// kinds not exercised by the original suite: atom.shared, atom.licensed,
// atom.revoked, royalty.settled and leaderboard.updated. Same approach as
// wire_compat_test.go: marshal via MarshalPayload, unmarshal into the
// generated chora-contracts bindings, and assert every pinned field.
//
// Field layouts are pinned to chora-contracts/proto/events-flat/sharing/* —
// the flat protos ARE the Schema Registry schemas (see protomarshal.go).
package protomarshal_test

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/encoding/protowire"

	sharingv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/sharing/v1"

	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protomarshal"
)

// findFixed64Field returns the raw fixed64 value of the first occurrence of a
// Fixed64Type field at the top level of bz, or (0,false) if not found.
func findFixed64Field(bz []byte, target protowire.Number) (uint64, bool) {
	rem := bz
	for len(rem) > 0 {
		num, typ, n := protowire.ConsumeTag(rem)
		if n < 0 {
			return 0, false
		}
		rem = rem[n:]
		switch typ {
		case protowire.Fixed64Type:
			v, m := protowire.ConsumeFixed64(rem)
			if m < 0 {
				return 0, false
			}
			if num == target {
				return v, true
			}
			rem = rem[m:]
		case protowire.BytesType:
			_, m := protowire.ConsumeBytes(rem)
			if m < 0 {
				return 0, false
			}
			rem = rem[m:]
		case protowire.VarintType:
			_, m := protowire.ConsumeVarint(rem)
			if m < 0 {
				return 0, false
			}
			rem = rem[m:]
		default:
			return 0, false
		}
	}
	return 0, false
}

// -----------------------------------------------------------------------------
// AtomShared (chora.sharing.atom.shared.v1)
//
//	1  bytes        Envelope envelope
//	2  string       share_entry_id
//	3  string       author_gcid
//	4  string       author_display_name
//	5  string       atom_id
//	6  string       atom_revision_id
//	7  string       caption
//	8  varint       LicenseTerms license_terms
//	9  bytes        RoyaltyRate royalty_rate
// -----------------------------------------------------------------------------

func TestWireCompat_AtomShared_DecodesIntoGeneratedType(t *testing.T) {
	env := wireCompatEnvelope()
	payload := map[string]any{
		"share_entry_id":      "se-1",
		"author_gcid":         "gcid-phyllis",
		"author_display_name": "Phyllis",
		"atom_id":             "atom-1",
		"atom_revision_id":    "atom-rev-1",
		"caption":             "my atom",
		"license_terms":       "royalty_pct",
		"royalty_rate":        map[string]any{"kind": "ROYALTY_PCT", "value": 12.5},
	}

	bz, err := protomarshal.MarshalPayload("chora.sharing.atom.shared.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}

	var msg sharingv1.AtomShared
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if got := msg.GetShareEntryId(); got != "se-1" {
		t.Fatalf("share_entry_id: got %q", got)
	}
	if got := msg.GetAuthorGcid(); got != "gcid-phyllis" {
		t.Fatalf("author_gcid: got %q", got)
	}
	if got := msg.GetAuthorDisplayName(); got != "Phyllis" {
		t.Fatalf("author_display_name: got %q", got)
	}
	if got := msg.GetAtomId(); got != "atom-1" {
		t.Fatalf("atom_id: got %q", got)
	}
	if got := msg.GetAtomRevisionId(); got != "atom-rev-1" {
		t.Fatalf("atom_revision_id: got %q", got)
	}
	if got := msg.GetCaption(); got != "my atom" {
		t.Fatalf("caption: got %q", got)
	}
	if got := msg.GetLicenseTerms(); got != sharingv1.LicenseTerms_LICENSE_TERMS_ROYALTY_PCT {
		t.Fatalf("license_terms: got %v (want ROYALTY_PCT=2)", got)
	}
	if got := msg.GetRoyaltyRate(); got == nil || got.GetKind() != "ROYALTY_PCT" || got.GetValue() != 12.5 {
		t.Fatalf("royalty_rate: got %+v", got)
	}
}

// AtomShared maps the loose license_terms string to the LicenseTerms enum value.
func TestMarshalAtomShared_LicenseTermsEnumMapping(t *testing.T) {
	env := fixedEnvelope()
	for _, tc := range []struct {
		input     any
		wantValue uint64
	}{
		{"free", 1},
		{"LICENSE_TERMS_FREE", 1},
		{"royalty_pct", 2},
		{"LICENSE_TERMS_ROYALTY_PCT", 2},
		{"royalty_fixed", 3},
		{"LICENSE_TERMS_ROYALTY_FIXED", 3},
		{"cc_by_sa", 4},
		{"LICENSE_TERMS_CC_BY_SA", 4},
		{"cc_nd", 5},
		{"LICENSE_TERMS_CC_ND", 5},
		{"unknown", 0},
		{"", 0},
		{42, 0}, // non-string collapses to UNSPECIFIED
	} {
		t.Run(fmt.Sprintf("%v", tc.input), func(t *testing.T) {
			payload := map[string]any{
				"share_entry_id": "se-1",
				"license_terms":  tc.input,
			}
			bz, err := protomarshal.MarshalPayload("chora.sharing.atom.shared.v1", env, payload)
			if err != nil {
				t.Fatalf("MarshalPayload: %v", err)
			}
			gotValue, found := findVarintField(bz, 8)
			if tc.wantValue == 0 {
				if found {
					t.Fatalf("expected license_terms field 8 to be omitted for %v, got value=%d", tc.input, gotValue)
				}
				return
			}
			if !found {
				t.Fatalf("expected license_terms field 8 present for %v", tc.input)
			}
			if gotValue != tc.wantValue {
				t.Fatalf("license_terms %v: got %d, want %d", tc.input, gotValue, tc.wantValue)
			}
		})
	}
}

// The royalty_rate submessage accepts float/int/json.Number value shapes and
// emits kind (1) + value (2, fixed64) inside field 9.
func TestMarshalAtomShared_RoyaltyRateValueShapes(t *testing.T) {
	env := fixedEnvelope()
	for _, tc := range []struct {
		name  string
		value any
		want  float64
	}{
		{"float64", 12.5, 12.5},
		{"float32", float32(1.5), 1.5},
		{"int", 7, 7},
		{"int32", int32(8), 8},
		{"int64", int64(9), 9},
		{"jsonNumber", json.Number("7.25"), 7.25},
		{"zero", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := map[string]any{
				"share_entry_id": "se-1",
				"royalty_rate":   map[string]any{"kind": "ROYALTY_FIXED", "value": tc.value},
			}
			bz, err := protomarshal.MarshalPayload("chora.sharing.atom.shared.v1", env, payload)
			if err != nil {
				t.Fatalf("MarshalPayload: %v", err)
			}
			var msg sharingv1.AtomShared
			if err := proto.Unmarshal(bz, &msg); err != nil {
				t.Fatalf("proto.Unmarshal: %v", err)
			}
			if got := msg.GetRoyaltyRate(); got == nil || got.GetKind() != "ROYALTY_FIXED" {
				t.Fatalf("royalty_rate: got %+v", got)
			} else if got.GetValue() != tc.want {
				t.Fatalf("royalty_rate.value: got %v, want %v", got.GetValue(), tc.want)
			}
		})
	}
}

// Missing / nil / empty royalty_rate leaves field 9 absent or empty — never an
// error. A zero value omits the fixed64 inside the submessage.
func TestMarshalAtomShared_RoyaltyRateAbsentOrNil(t *testing.T) {
	env := fixedEnvelope()
	for _, tc := range []struct {
		name    string
		payload map[string]any
	}{
		{"absent", map[string]any{"share_entry_id": "se-1"}},
		{"nilValue", map[string]any{"share_entry_id": "se-1", "royalty_rate": nil}},
		{"emptyMap", map[string]any{"share_entry_id": "se-1", "royalty_rate": map[string]any{}}},
		{"zeroValue", map[string]any{"share_entry_id": "se-1", "royalty_rate": map[string]any{"value": 0}}},
		{"emptyKind", map[string]any{"share_entry_id": "se-1", "royalty_rate": map[string]any{"kind": "", "value": 0.0}}},
		{"nonStringKind", map[string]any{"share_entry_id": "se-1", "royalty_rate": map[string]any{"kind": 5}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bz, err := protomarshal.MarshalPayload("chora.sharing.atom.shared.v1", env, tc.payload)
			if err != nil {
				t.Fatalf("MarshalPayload: %v", err)
			}
			if len(bz) == 0 {
				t.Fatal("empty bytes")
			}
			var msg sharingv1.AtomShared
			if err := proto.Unmarshal(bz, &msg); err != nil {
				t.Fatalf("proto.Unmarshal: %v", err)
			}
			// Must never decode into a nil envelope or fail — the row is still
			// schema-valid with only the envelope populated.
			if msg.GetEnvelope() == nil {
				t.Fatal("envelope lost")
			}
		})
	}
}

// A nil payload still emits the envelope and stays schema-valid.
func TestMarshal_AtomSharedNilPayload_ProducesEnvelopeOnly(t *testing.T) {
	env := fixedEnvelope()
	bz, err := protomarshal.MarshalPayload("chora.sharing.atom.shared.v1", env, nil)
	if err != nil {
		t.Fatalf("nil payload: %v", err)
	}
	num, _, n := protowire.ConsumeTag(bz)
	if n < 0 || num != 1 {
		t.Fatalf("expected leading field=1 (envelope), got %d", num)
	}
}

// -----------------------------------------------------------------------------
// AtomLicensed (chora.sharing.atom.licensed.v1)
//
//	1  bytes        Envelope envelope
//	2  string       grant_id
//	3  string       owner_gcid
//	4  string       grantee_gcid
//	5  string       atom_id
//	6  string       atom_revision_id
//	7  varint       GrantScope scope
//	8  varint       LicenseTerms license_terms_snapshot
//	9  bytes        RoyaltyRate royalty_rate_snapshot
// -----------------------------------------------------------------------------

func TestWireCompat_AtomLicensed_DecodesIntoGeneratedType(t *testing.T) {
	env := wireCompatEnvelope()
	payload := map[string]any{
		"grant_id":              "g-1",
		"owner_gcid":            "gcid-phyllis",
		"grantee_gcid":          "gcid-maya",
		"atom_id":               "atom-1",
		"atom_revision_id":      "atom-rev-1",
		"scope":                 "duel",
		"license_terms_snapshot": "free",
		"royalty_rate_snapshot":  map[string]any{"kind": "FREE", "value": 3.5},
	}

	bz, err := protomarshal.MarshalPayload("chora.sharing.atom.licensed.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}

	var msg sharingv1.AtomLicensed
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if got := msg.GetGrantId(); got != "g-1" {
		t.Fatalf("grant_id: got %q", got)
	}
	if got := msg.GetOwnerGcid(); got != "gcid-phyllis" {
		t.Fatalf("owner_gcid: got %q", got)
	}
	if got := msg.GetGranteeGcid(); got != "gcid-maya" {
		t.Fatalf("grantee_gcid: got %q", got)
	}
	if got := msg.GetAtomId(); got != "atom-1" {
		t.Fatalf("atom_id: got %q", got)
	}
	if got := msg.GetAtomRevisionId(); got != "atom-rev-1" {
		t.Fatalf("atom_revision_id: got %q", got)
	}
	if got := msg.GetScope(); got != sharingv1.GrantScope_GRANT_SCOPE_DUEL {
		t.Fatalf("scope: got %v (want DUEL=2)", got)
	}
	if got := msg.GetLicenseTermsSnapshot(); got != sharingv1.LicenseTerms_LICENSE_TERMS_FREE {
		t.Fatalf("license_terms_snapshot: got %v (want FREE=1)", got)
	}
	if got := msg.GetRoyaltyRateSnapshot(); got == nil || got.GetKind() != "FREE" || got.GetValue() != 3.5 {
		t.Fatalf("royalty_rate_snapshot: got %+v", got)
	}
}

// AtomLicensed scope enum mapping.
func TestMarshalAtomLicensed_ScopeEnumMapping(t *testing.T) {
	env := fixedEnvelope()
	for _, tc := range []struct {
		input     any
		wantValue uint64
	}{
		{"test_set", 1},
		{"GRANT_SCOPE_TEST_SET", 1},
		{"duel", 2},
		{"GRANT_SCOPE_DUEL", 2},
		{"live_quiz", 3},
		{"GRANT_SCOPE_LIVE_QUIZ", 3},
		{"collection", 4},
		{"GRANT_SCOPE_COLLECTION", 4},
		{"unlimited", 5},
		{"GRANT_SCOPE_UNLIMITED", 5},
		{"unknown", 0},
		{"", 0},
		{7, 0},
	} {
		t.Run(fmt.Sprintf("%v", tc.input), func(t *testing.T) {
			payload := map[string]any{
				"grant_id": "g-1",
				"scope":    tc.input,
			}
			bz, err := protomarshal.MarshalPayload("chora.sharing.atom.licensed.v1", env, payload)
			if err != nil {
				t.Fatalf("MarshalPayload: %v", err)
			}
			gotValue, found := findVarintField(bz, 7)
			if tc.wantValue == 0 {
				if found {
					t.Fatalf("expected scope field 7 omitted for %v, got %d", tc.input, gotValue)
				}
				return
			}
			if !found {
				t.Fatalf("expected scope field 7 present for %v", tc.input)
			}
			if gotValue != tc.wantValue {
				t.Fatalf("scope %v: got %d, want %d", tc.input, gotValue, tc.wantValue)
			}
		})
	}
}

func TestMarshal_AtomLicensedNilPayload_ProducesEnvelopeOnly(t *testing.T) {
	env := fixedEnvelope()
	bz, err := protomarshal.MarshalPayload("chora.sharing.atom.licensed.v1", env, nil)
	if err != nil {
		t.Fatalf("nil payload: %v", err)
	}
	num, _, n := protowire.ConsumeTag(bz)
	if n < 0 || num != 1 {
		t.Fatalf("expected leading field=1 (envelope), got %d", num)
	}
}

// -----------------------------------------------------------------------------
// AtomRevoked (chora.sharing.atom.revoked.v1)
//
//	1  bytes        Envelope envelope
//	2  string       grant_id
//	3  string       owner_gcid
//	4  string       grantee_gcid
//	5  string       atom_id
//	6  string       revoked_by_gcid
//	7  string       reason
//	8  bytes        Timestamp revoked_at
// -----------------------------------------------------------------------------

func TestWireCompat_AtomRevoked_DecodesIntoGeneratedType(t *testing.T) {
	env := wireCompatEnvelope()
	payload := map[string]any{
		"grant_id":        "g-1",
		"owner_gcid":      "gcid-phyllis",
		"grantee_gcid":    "gcid-maya",
		"atom_id":         "atom-1",
		"revoked_by_gcid": "gcid-maya",
		"reason":          "expired license",
		"revoked_at":      env.OccurredAt,
	}

	bz, err := protomarshal.MarshalPayload("chora.sharing.atom.revoked.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}

	var msg sharingv1.AtomGrantRevoked
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if got := msg.GetGrantId(); got != "g-1" {
		t.Fatalf("grant_id: got %q", got)
	}
	if got := msg.GetOwnerGcid(); got != "gcid-phyllis" {
		t.Fatalf("owner_gcid: got %q", got)
	}
	if got := msg.GetGranteeGcid(); got != "gcid-maya" {
		t.Fatalf("grantee_gcid: got %q", got)
	}
	if got := msg.GetAtomId(); got != "atom-1" {
		t.Fatalf("atom_id: got %q", got)
	}
	if got := msg.GetRevokedByGcid(); got != "gcid-maya" {
		t.Fatalf("revoked_by_gcid: got %q", got)
	}
	if got := msg.GetReason(); got != "expired license" {
		t.Fatalf("reason: got %q", got)
	}
	if got := msg.GetRevokedAt(); got == nil || got.AsTime().Unix() != env.OccurredAt.Unix() {
		t.Fatalf("revoked_at: %+v", got)
	}
}

func TestMarshal_AtomRevokedNilPayload_ProducesEnvelopeOnly(t *testing.T) {
	env := fixedEnvelope()
	bz, err := protomarshal.MarshalPayload("chora.sharing.atom.revoked.v1", env, nil)
	if err != nil {
		t.Fatalf("nil payload: %v", err)
	}
	num, _, n := protowire.ConsumeTag(bz)
	if n < 0 || num != 1 {
		t.Fatalf("expected leading field=1 (envelope), got %d", num)
	}
}

// -----------------------------------------------------------------------------
// RoyaltySettled (chora.sharing.royalty.settled.v1)
//
//	1  bytes        Envelope envelope
//	2  string       settlement_id
//	3  string       grant_id
//	4  string       owner_gcid
//	5  string       grantee_tenant_id
//	6  string       atom_id
//	7  double       amount
//	8  varint       RoyaltyCurrency currency
//	9  varint       RoyaltyUsageContext usage_context
//	10 string       source_event_id
// -----------------------------------------------------------------------------

func TestWireCompat_RoyaltySettled_DecodesIntoGeneratedType(t *testing.T) {
	env := wireCompatEnvelope()
	payload := map[string]any{
		"settlement_id":     "stl-1",
		"grant_id":          "g-1",
		"owner_gcid":        "gcid-phyllis",
		"grantee_tenant_id": "tenant-maya",
		"atom_id":           "atom-1",
		"amount":            42.75,
		"currency":          "mana",
		"usage_context":     "duel",
		"source_event_id":   "evt-1",
	}

	bz, err := protomarshal.MarshalPayload("chora.sharing.royalty.settled.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}

	var msg sharingv1.RoyaltySettled
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if got := msg.GetSettlementId(); got != "stl-1" {
		t.Fatalf("settlement_id: got %q", got)
	}
	if got := msg.GetGrantId(); got != "g-1" {
		t.Fatalf("grant_id: got %q", got)
	}
	if got := msg.GetOwnerGcid(); got != "gcid-phyllis" {
		t.Fatalf("owner_gcid: got %q", got)
	}
	if got := msg.GetGranteeTenantId(); got != "tenant-maya" {
		t.Fatalf("grantee_tenant_id: got %q", got)
	}
	if got := msg.GetAtomId(); got != "atom-1" {
		t.Fatalf("atom_id: got %q", got)
	}
	if got := msg.GetAmount(); got != 42.75 {
		t.Fatalf("amount: got %v, want 42.75", got)
	}
	if got := msg.GetCurrency(); got != sharingv1.RoyaltyCurrency_ROYALTY_CURRENCY_MANA {
		t.Fatalf("currency: got %v (want MANA=1)", got)
	}
	if got := msg.GetUsageContext(); got != sharingv1.RoyaltyUsageContext_ROYALTY_USAGE_CONTEXT_DUEL {
		t.Fatalf("usage_context: got %v (want DUEL=2)", got)
	}
	if got := msg.GetSourceEventId(); got != "evt-1" {
		t.Fatalf("source_event_id: got %q", got)
	}
}

// RoyaltySettled.amount is a proto double — assert the exact fixed64 wire bits
// as well as the round-tripped value for one case.
func TestMarshalRoyaltySettled_AmountDoubleWireBits(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"settlement_id": "stl-1",
		"amount":        float32(1.5), // float32 coerces through asFloat64
		"currency":      "coins",
		"usage_context": "live_quiz",
	}

	bz, err := protomarshal.MarshalPayload("chora.sharing.royalty.settled.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	bits, found := findFixed64Field(bz, 7)
	if !found {
		t.Fatal("amount field 7 missing")
	}
	if bits != math.Float64bits(1.5) {
		t.Fatalf("amount fixed64 bits: got %d, want %d", bits, math.Float64bits(1.5))
	}

	// Round-trip through the generated binding for the coercions.
	for _, tc := range []struct {
		name  string
		value any
		want  float64
	}{
		{"float64", 2.25, 2.25},
		{"int", 3, 3},
		{"int32", int32(4), 4},
		{"int64", int64(5), 5},
		{"jsonNumber", json.Number("6.5"), 6.5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := map[string]any{"amount": tc.value}
			b, err := protomarshal.MarshalPayload("chora.sharing.royalty.settled.v1", env, p)
			if err != nil {
				t.Fatalf("MarshalPayload: %v", err)
			}
			var msg sharingv1.RoyaltySettled
			if err := proto.Unmarshal(b, &msg); err != nil {
				t.Fatalf("proto.Unmarshal: %v", err)
			}
			if got := msg.GetAmount(); got != tc.want {
				t.Fatalf("amount: got %v, want %v", got, tc.want)
			}
		})
	}
}

// A zero amount is omitted per proto3 default; missing amount is fine too.
func TestMarshalRoyaltySettled_ZeroOrMissingAmountOmitted(t *testing.T) {
	env := fixedEnvelope()
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"zero", 0.0},
		{"missing", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := map[string]any{}
			if tc.value != nil {
				payload["amount"] = tc.value
			}
			bz, err := protomarshal.MarshalPayload("chora.sharing.royalty.settled.v1", env, payload)
			if err != nil {
				t.Fatalf("MarshalPayload: %v", err)
			}
			if _, found := findFixed64Field(bz, 7); found {
				t.Fatal("expected amount field 7 to be omitted")
			}
		})
	}
}

// RoyaltySettled currency + usage_context enum mappings.
func TestMarshalRoyaltySettled_CurrencyUsageEnumMapping(t *testing.T) {
	env := fixedEnvelope()
	for _, tc := range []struct {
		name  string
		key   string
		input any
		field protowire.Number
		want  uint64
	}{
		{"mana", "currency", "mana", 8, 1},
		{"ROYALTY_CURRENCY_MANA", "currency", "ROYALTY_CURRENCY_MANA", 8, 1},
		{"coins", "currency", "coins", 8, 2},
		{"reputation", "currency", "reputation", 8, 3},
		{"usd", "currency", "usd", 8, 4},
		{"unknown-currency", "currency", "unknown", 8, 0},
		{"non-string-currency", "currency", 3, 8, 0},
		{"test_set", "usage_context", "test_set", 9, 1},
		{"duel", "usage_context", "duel", 9, 2},
		{"live_quiz", "usage_context", "live_quiz", 9, 3},
		{"unknown-context", "usage_context", "unknown", 9, 0},
		{"non-string-context", "usage_context", 3, 9, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := map[string]any{"settlement_id": "stl-1", tc.key: tc.input}
			bz, err := protomarshal.MarshalPayload("chora.sharing.royalty.settled.v1", env, payload)
			if err != nil {
				t.Fatalf("MarshalPayload: %v", err)
			}
			gotValue, found := findVarintField(bz, tc.field)
			if tc.want == 0 {
				if found {
					t.Fatalf("expected field %d omitted for %v, got %d", tc.field, tc.input, gotValue)
				}
				return
			}
			if !found {
				t.Fatalf("expected field %d present for %v", tc.field, tc.input)
			}
			if gotValue != tc.want {
				t.Fatalf("%s %v: got %d, want %d", tc.key, tc.input, gotValue, tc.want)
			}
		})
	}
}

func TestMarshal_RoyaltySettledNilPayload_ProducesEnvelopeOnly(t *testing.T) {
	env := fixedEnvelope()
	bz, err := protomarshal.MarshalPayload("chora.sharing.royalty.settled.v1", env, nil)
	if err != nil {
		t.Fatalf("nil payload: %v", err)
	}
	num, _, n := protowire.ConsumeTag(bz)
	if n < 0 || num != 1 {
		t.Fatalf("expected leading field=1 (envelope), got %d", num)
	}
}

// -----------------------------------------------------------------------------
// LeaderboardUpdated (chora.sharing.leaderboard.updated.v1)
//
//	1  bytes        Envelope envelope
//	2  string       leaderboard_id
//	3  varint       LeaderboardScope scope
//	4  string       scope_id
//	5  string       season_id
//	6  repeated     bytes LeaderboardEntry top_entries
//	7  bytes        Timestamp updated_at
//	8  varint       int32 duel_elo
//	9  varint       int32 atoms_completed
//	10 repeated     bytes RankUp rank_ups
// -----------------------------------------------------------------------------

func TestWireCompat_LeaderboardUpdated_DecodesIntoGeneratedType(t *testing.T) {
	env := wireCompatEnvelope()
	payload := map[string]any{
		"leaderboard_id":   "lb-1",
		"scope":            "tenant",
		"scope_id":         "tenant-acme",
		"season_id":        "s1",
		"top_entries": []map[string]any{
			{"gcid": "gcid-phyllis", "score": int64(1200), "rank": 1},
			{"gcid": "gcid-maya", "score": 900, "rank": 2},
		},
		"updated_at":     env.OccurredAt,
		"duel_elo":       1600,
		"atoms_completed": 42,
		"rank_ups": []map[string]any{
			{"gcid": "gcid-maya", "old_rank": 3, "new_rank": 2},
		},
	}

	bz, err := protomarshal.MarshalPayload("chora.sharing.leaderboard.updated.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}

	var msg sharingv1.LeaderboardUpdated
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if got := msg.GetLeaderboardId(); got != "lb-1" {
		t.Fatalf("leaderboard_id: got %q", got)
	}
	if got := msg.GetScope(); got != sharingv1.LeaderboardScope_LEADERBOARD_SCOPE_TENANT {
		t.Fatalf("scope: got %v (want TENANT=1)", got)
	}
	if got := msg.GetScopeId(); got != "tenant-acme" {
		t.Fatalf("scope_id: got %q", got)
	}
	if got := msg.GetSeasonId(); got != "s1" {
		t.Fatalf("season_id: got %q", got)
	}
	if got := msg.GetTopEntries(); len(got) != 2 {
		t.Fatalf("top_entries: got %d entries", len(got))
	} else {
		if e := got[0]; e.GetGcid() != "gcid-phyllis" || e.GetScore() != 1200 || e.GetRank() != 1 {
			t.Fatalf("top_entries[0]: %+v", e)
		}
		if e := got[1]; e.GetGcid() != "gcid-maya" || e.GetScore() != 900 || e.GetRank() != 2 {
			t.Fatalf("top_entries[1]: %+v", e)
		}
	}
	if got := msg.GetUpdatedAt(); got == nil || got.AsTime().Unix() != env.OccurredAt.Unix() {
		t.Fatalf("updated_at: %+v", got)
	}
	if got := msg.GetDuelElo(); got != 1600 {
		t.Fatalf("duel_elo: got %d", got)
	}
	if got := msg.GetAtomsCompleted(); got != 42 {
		t.Fatalf("atoms_completed: got %d", got)
	}
	if got := msg.GetRankUps(); len(got) != 1 {
		t.Fatalf("rank_ups: got %d entries", len(got))
	} else if u := got[0]; u.GetGcid() != "gcid-maya" || u.GetOldRank() != 3 || u.GetNewRank() != 2 {
		t.Fatalf("rank_ups[0]: %+v", u)
	}
}

// LeaderboardUpdated scope enum mapping.
func TestMarshalLeaderboardUpdated_ScopeEnumMapping(t *testing.T) {
	env := fixedEnvelope()
	for _, tc := range []struct {
		input     any
		wantValue uint64
	}{
		{"tenant", 1},
		{"LEADERBOARD_SCOPE_TENANT", 1},
		{"course", 2},
		{"LEADERBOARD_SCOPE_COURSE", 2},
		{"cohort", 3},
		{"LEADERBOARD_SCOPE_COHORT", 3},
		{"global", 4},
		{"LEADERBOARD_SCOPE_GLOBAL", 4},
		{"unknown", 0},
		{"", 0},
		{2, 0},
	} {
		t.Run(fmt.Sprintf("%v", tc.input), func(t *testing.T) {
			payload := map[string]any{
				"leaderboard_id": "lb-1",
				"scope":          tc.input,
			}
			bz, err := protomarshal.MarshalPayload("chora.sharing.leaderboard.updated.v1", env, payload)
			if err != nil {
				t.Fatalf("MarshalPayload: %v", err)
			}
			gotValue, found := findVarintField(bz, 3)
			if tc.wantValue == 0 {
				if found {
					t.Fatalf("expected scope field 3 omitted for %v, got %d", tc.input, gotValue)
				}
				return
			}
			if !found {
				t.Fatalf("expected scope field 3 present for %v", tc.input)
			}
			if gotValue != tc.wantValue {
				t.Fatalf("scope %v: got %d, want %d", tc.input, gotValue, tc.wantValue)
			}
		})
	}
}

// LeaderboardEntry score/rank value coercions — int, int32, int64, float64,
// float32, json.Number, zero-omitted, and wrong-type-omitted.
func TestMarshalLeaderboardUpdated_EntryValueShapes(t *testing.T) {
	env := fixedEnvelope()
	for _, tc := range []struct {
		name     string
		score    any
		rank     any
		want     int64
		wantRank int32
	}{
		{"int", 10, 1, 10, 1},
		{"int32", int32(11), int32(2), 11, 2},
		{"int64", int64(12), 3, 12, 3},
		{"float64", float64(13), 4, 13, 4},
		{"float32", float32(14), 5, 14, 5},
		{"jsonNumber", json.Number("15"), 6, 15, 6},
		{"zeroScore", 0, 7, 0, 7},
		{"skipBadScore", "no", 8, 0, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := map[string]any{
				"leaderboard_id": "lb-1",
				"top_entries":    []any{map[string]any{"gcid": "g", "score": tc.score, "rank": tc.rank}},
			}
			bz, err := protomarshal.MarshalPayload("chora.sharing.leaderboard.updated.v1", env, payload)
			if err != nil {
				t.Fatalf("MarshalPayload: %v", err)
			}
			var msg sharingv1.LeaderboardUpdated
			if err := proto.Unmarshal(bz, &msg); err != nil {
				t.Fatalf("proto.Unmarshal: %v", err)
			}
			entries := msg.GetTopEntries()
			if len(entries) != 1 {
				t.Fatalf("top_entries: got %d entries", len(entries))
			}
			if got := entries[0].GetScore(); got != tc.want {
				t.Fatalf("score: got %d, want %d", got, tc.want)
			}
			if got := entries[0].GetRank(); got != tc.wantRank {
				t.Fatalf("rank: got %d, want %d", got, tc.wantRank)
			}
		})
	}
}

// LeaderboardUpdated duel_elo / atoms_completed accept int-ish values; zero is
// omitted; negatives round-trip through the two's-complement varint.
func TestMarshalLeaderboardUpdated_Int32FieldShapes(t *testing.T) {
	env := fixedEnvelope()
	for _, tc := range []struct {
		name  string
		elo   any
		atoms any
		want  int32
	}{
		{"int", 1600, 7, 1600},
		{"int32", int32(1500), int32(8), 1500},
		{"int64", int64(1700), int64(9), 1700},
		{"negative", -5, 0, -5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := map[string]any{"leaderboard_id": "lb-1", "duel_elo": tc.elo, "atoms_completed": tc.atoms}
			bz, err := protomarshal.MarshalPayload("chora.sharing.leaderboard.updated.v1", env, payload)
			if err != nil {
				t.Fatalf("MarshalPayload: %v", err)
			}
			var msg sharingv1.LeaderboardUpdated
			if err := proto.Unmarshal(bz, &msg); err != nil {
				t.Fatalf("proto.Unmarshal: %v", err)
			}
			if got := msg.GetDuelElo(); got != tc.want {
				t.Fatalf("duel_elo: got %d, want %d", got, tc.want)
			}
		})
	}

	// Both omitted when absent and when zero.
	payload := map[string]any{"leaderboard_id": "lb-1", "duel_elo": 0, "atoms_completed": 0}
	bz, err := protomarshal.MarshalPayload("chora.sharing.leaderboard.updated.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	if _, found := findVarintField(bz, 8); found {
		t.Fatal("zero duel_elo must be omitted")
	}
	if _, found := findVarintField(bz, 9); found {
		t.Fatal("zero atoms_completed must be omitted")
	}
}

// rank_ups accept []any or []map[string]any; empty entries omit inner fields.
func TestMarshalLeaderboardUpdated_RankUpsShapes(t *testing.T) {
	env := fixedEnvelope()
	payload := map[string]any{
		"leaderboard_id": "lb-1",
		"rank_ups": []any{
			map[string]any{"gcid": "gcid-phyllis", "old_rank": 5, "new_rank": 3},
			map[string]any{"gcid": "", "old_rank": 0, "new_rank": 0},
		},
	}
	bz, err := protomarshal.MarshalPayload("chora.sharing.leaderboard.updated.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	var msg sharingv1.LeaderboardUpdated
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	ups := msg.GetRankUps()
	if len(ups) != 2 {
		t.Fatalf("rank_ups: got %d entries", len(ups))
	}
	if u := ups[0]; u.GetGcid() != "gcid-phyllis" || u.GetOldRank() != 5 || u.GetNewRank() != 3 {
		t.Fatalf("rank_ups[0]: %+v", u)
	}
}

// top_entries / rank_ups with a non-slice or nil value are silently skipped —
// the Receiver never sends them, but a stray producer must not blow up.
func TestMarshalLeaderboardUpdated_NonSliceEntriesSkipped(t *testing.T) {
	env := fixedEnvelope()
	for _, tc := range []struct {
		name string
		key  string
		val  any
	}{
		{"nil-entries", "top_entries", nil},
		{"wrong-type-entries", "top_entries", "not-a-slice"},
		{"nil-rankups", "rank_ups", nil},
		{"wrong-type-rankups", "rank_ups", "not-a-slice"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := map[string]any{"leaderboard_id": "lb-1", tc.key: tc.val}
			bz, err := protomarshal.MarshalPayload("chora.sharing.leaderboard.updated.v1", env, payload)
			if err != nil {
				t.Fatalf("MarshalPayload: %v", err)
			}
			if len(bz) == 0 {
				t.Fatal("empty bytes")
			}
		})
	}
}

func TestMarshal_LeaderboardUpdatedNilPayload_ProducesEnvelopeOnly(t *testing.T) {
	env := fixedEnvelope()
	bz, err := protomarshal.MarshalPayload("chora.sharing.leaderboard.updated.v1", env, nil)
	if err != nil {
		t.Fatalf("nil payload: %v", err)
	}
	num, _, n := protowire.ConsumeTag(bz)
	if n < 0 || num != 1 {
		t.Fatalf("expected leading field=1 (envelope), got %d", num)
	}
}

// -----------------------------------------------------------------------------
// Timestamp encodeTimestamp seconds/nanos edge: an exact-second boundary with
// zero nanos omits field 2, and a negative (pre-epoch) instant still encodes.
// -----------------------------------------------------------------------------

func TestMarshalRelationshipFollowed_TimestampEdgeShapes(t *testing.T) {
	env := fixedEnvelope()
	exactSecond := time.Unix(1700000000, 0).UTC() // nanos == 0 → field 2 omitted
	preEpoch := time.Unix(-3600, 500000000).UTC() // negative seconds + nanos

	for _, tc := range []struct {
		name  string
		t     time.Time
		check func(t *testing.T, bz []byte)
	}{
		{
			name: "exact-second",
			t:    exactSecond,
			check: func(t *testing.T, bz []byte) {
				var msg sharingv1.RelationshipFollowed
				if err := proto.Unmarshal(bz, &msg); err != nil {
					t.Fatalf("proto.Unmarshal: %v", err)
				}
				if got := msg.GetFollowedAt(); got == nil || got.AsTime() != exactSecond {
					t.Fatalf("followed_at: %+v", got)
				}
			},
		},
		{
			name: "pre-epoch",
			t:    preEpoch,
			check: func(t *testing.T, bz []byte) {
				var msg sharingv1.RelationshipFollowed
				if err := proto.Unmarshal(bz, &msg); err != nil {
					t.Fatalf("proto.Unmarshal: %v", err)
				}
				got := msg.GetFollowedAt()
				if got == nil {
					t.Fatal("followed_at nil")
				}
				// Sub-second resolution round-trips; the Unix() second matches.
				if got.AsTime().Unix() != preEpoch.Unix() {
					t.Fatalf("followed_at seconds: got %d, want %d", got.AsTime().Unix(), preEpoch.Unix())
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := map[string]any{
				"follower_gcid": "gcid-phyllis",
				"followee_gcid": "gcid-maya",
				"followed_at":   tc.t,
			}
			bz, err := protomarshal.MarshalPayload("chora.sharing.relationship.followed.v1", env, payload)
			if err != nil {
				t.Fatalf("MarshalPayload: %v", err)
			}
			tc.check(t, bz)
		})
	}
}