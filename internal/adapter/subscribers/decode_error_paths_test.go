// decode_error_paths_test.go — the remaining decoder/coercion branches:
// malformed-bytes errors for the live-quiz + weakness-grow decoders, and the
// asInt64 float64 coercion (the JSON path produces float64 numbers).
package subscribers

import (
	"testing"
)

func TestDecodeScoreAwardedWithAttrs_Error(t *testing.T) {
	if _, err := DecodeScoreAwardedWithAttrs([]byte("{not json"), nil); err == nil {
		t.Fatalf("malformed bytes must error")
	}
}

func TestDecodeWeaknessGrownWithAttrs_Error(t *testing.T) {
	if _, err := DecodeWeaknessGrownWithAttrs([]byte("{not json"), nil); err == nil {
		t.Fatalf("malformed bytes must error")
	}
}

func TestAsInt64_Float64Coercion(t *testing.T) {
	// json.Unmarshal turns JSON numbers into float64; asInt64 must accept them.
	if got := asInt64(float64(42)); got != 42 {
		t.Fatalf("asInt64(float64) = %d, want 42", got)
	}
}

func TestAsInt_Float64AlreadyCoveredByDecode(t *testing.T) {
	// Belt-and-braces direct check of the float64 branch of asInt.
	if got := asInt(float64(7.0)); got != 7 {
		t.Fatalf("asInt(float64) = %d, want 7", got)
	}
}