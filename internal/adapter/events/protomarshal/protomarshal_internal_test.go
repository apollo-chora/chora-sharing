// protomarshal_internal_test.go — same-package (internal) tests for the three
// per-field writers protomarshal.go declares but never calls from an encoder:
//
//	writeRepeatedStringField, writeFloatField, writeBoolField
//
// They exist for symmetry with the other per-field writers and are exercised
// only here. Because they are unexported and unreferenced, an external
// (`protomarshal_test`) test file cannot reach them — Go's coverage tool
// counts their statements against the package total, so without this file the
// package's maximum achievable coverage is 92.3%. The standard stdlib pattern
// of coexisting internal + external test files is used deliberately.
//
// No production code is touched; assertions decode the emitted bytes with
// protowire to pin the wire shape.
package protomarshal

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

// writeRepeatedStringField writes payload[key] as repeated length-delimited
// fields, skipping empty elements. Wrong types fail loud.
func TestWriteRepeatedStringField_Shapes(t *testing.T) {
	payload := map[string]any{
		"str_slice": []string{"a", "", "b"},
		"any_slice": []any{"c", "d"},
	}

	// Missing key → no error, no bytes.
	var out []byte
	if err := writeRepeatedStringField(&out, map[string]any{}, protowire.Number(1), "nope"); err != nil {
		t.Fatalf("missing key: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("missing key must write nothing, got %x", out)
	}

	// Nil value → treated as absent.
	if err := writeRepeatedStringField(&out, map[string]any{"nope": nil}, protowire.Number(1), "nope"); err != nil {
		t.Fatalf("nil value: %v", err)
	}

	// []string with an empty element skipped.
	if err := writeRepeatedStringField(&out, payload, protowire.Number(1), "str_slice"); err != nil {
		t.Fatalf("[]string: %v", err)
	}
	assertRepeatedStrings(t, out, []string{"a", "b"})

	// []any of strings.
	out = nil
	if err := writeRepeatedStringField(&out, payload, protowire.Number(2), "any_slice"); err != nil {
		t.Fatalf("[]any: %v", err)
	}
	assertRepeatedStrings(t, out, []string{"c", "d"})

	// Wrong-typed element → fail loud.
	if err := writeRepeatedStringField(&out, map[string]any{"bad": []any{"x", 5}}, protowire.Number(1), "bad"); err == nil {
		t.Fatal("expected error for []any containing non-string")
	}

	// Wrong-typed top-level value → fail loud.
	if err := writeRepeatedStringField(&out, map[string]any{"bad": 5}, protowire.Number(1), "bad"); err == nil {
		t.Fatal("expected error for non-slice value")
	}
}

func assertRepeatedStrings(t *testing.T, bz []byte, want []string) {
	t.Helper()
	var got []string
	rem := bz
	for len(rem) > 0 {
		_, typ, n := protowire.ConsumeTag(rem)
		if n < 0 {
			t.Fatalf("bad tag in %x: %d", bz, n)
		}
		rem = rem[n:]
		if typ != protowire.BytesType {
			t.Fatalf("expected bytes field, got type %d", typ)
		}
		val, m := protowire.ConsumeBytes(rem)
		if m < 0 {
			t.Fatalf("bad bytes in %x", rem)
		}
		rem = rem[m:]
		got = append(got, string(val))
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

// writeFloatField writes payload[key] as a proto float (fixed32). Zero and
// absent values are omitted; non-numeric values fail loud.
func TestWriteFloatField_Shapes(t *testing.T) {
	// Absent → nothing.
	var out []byte
	if err := writeFloatField(&out, map[string]any{}, protowire.Number(1), "f"); err != nil {
		t.Fatalf("absent: %v", err)
	}

	// float64 → fixed32 bits; float32 and int coerce through asFloat64.
	for _, tc := range []struct {
		name string
		v    any
		want uint32
	}{
		{"float64", 1.5, 0x3FC00000}, // math.Float32bits(1.5)
		{"float32", float32(2.5), 0x40200000},
		{"int", 3, 0x40400000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out = nil
			if err := writeFloatField(&out, map[string]any{"f": tc.v}, protowire.Number(1), "f"); err != nil {
				t.Fatalf("writeFloatField(%s): %v", tc.name, err)
			}
			num, typ, n := protowire.ConsumeTag(out)
			if n < 0 || num != 1 || typ != protowire.Fixed32Type {
				t.Fatalf("expected tag (1, fixed32), got num=%d typ=%d", num, typ)
			}
			v, m := protowire.ConsumeFixed32(out[n:])
			if m < 0 || v != tc.want {
				t.Fatalf("fixed32 value: got %#x, want %#x", v, tc.want)
			}
		})
	}

	// Zero → omitted.
	out = nil
	if err := writeFloatField(&out, map[string]any{"f": 0.0}, protowire.Number(1), "f"); err != nil {
		t.Fatalf("zero: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("zero float must be omitted, got %x", out)
	}

	// Non-numeric → fail loud.
	if err := writeFloatField(&out, map[string]any{"f": "x"}, protowire.Number(1), "f"); err == nil {
		t.Fatal("expected error for non-numeric value")
	}
}

// writeBoolField writes payload[key] as a varint bool. true emits tag+1,
// false and absent emit nothing, non-bool fails loud.
func TestWriteBoolField_Shapes(t *testing.T) {
	var out []byte
	if err := writeBoolField(&out, map[string]any{}, protowire.Number(1), "b"); err != nil {
		t.Fatalf("absent: %v", err)
	}

	if err := writeBoolField(&out, map[string]any{"b": true}, protowire.Number(1), "b"); err != nil {
		t.Fatalf("true: %v", err)
	}
	num, typ, n := protowire.ConsumeTag(out)
	if n < 0 || num != 1 || typ != protowire.VarintType {
		t.Fatalf("expected tag (1, varint), got num=%d typ=%d", num, typ)
	}
	v, m := protowire.ConsumeVarint(out[n:])
	if m < 0 || v != 1 {
		t.Fatalf("bool value: got %d, want 1", v)
	}

	// false → omitted.
	out = nil
	if err := writeBoolField(&out, map[string]any{"b": false}, protowire.Number(1), "b"); err != nil {
		t.Fatalf("false: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("false must be omitted, got %x", out)
	}

	// Non-bool → fail loud.
	if err := writeBoolField(&out, map[string]any{"b": "yes"}, protowire.Number(1), "b"); err == nil {
		t.Fatal("expected error for non-bool value")
	}
}