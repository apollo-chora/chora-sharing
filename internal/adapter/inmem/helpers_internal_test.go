// helpers_internal_test.go — white-box checks for the unexported helper
// functions in duel.go whose fallback branches are unreachable through the
// exported API (gcidFromCatKey on a key without a separator, abs on either
// sign). Internal-test-package is required to reach them.
package inmem

import "testing"

func TestDuelHelperFunctions(t *testing.T) {
	if got := catKey("g-1", "overall"); got != "g-1|overall" {
		t.Fatalf("catKey: got %q", got)
	}
	if got := gcidFromCatKey("g-1|overall"); got != "g-1" {
		t.Fatalf("gcidFromCatKey: got %q", got)
	}
	// Fallback: no separator — the whole key is the gcid.
	if got := gcidFromCatKey("no-pipe"); got != "no-pipe" {
		t.Fatalf("gcidFromCatKey fallback: got %q", got)
	}
	if got := abs(-5); got != 5 {
		t.Fatalf("abs(-5): got %d", got)
	}
	if got := abs(7); got != 7 {
		t.Fatalf("abs(7): got %d", got)
	}
}

// TestCloneProfileNil covers cloneProfile's nil guard, which is unreachable
// through SaveProfile/GetProfile (both guard nil before cloning).
func TestCloneProfileNil(t *testing.T) {
	if got := cloneProfile(nil); got != nil {
		t.Fatalf("cloneProfile(nil): want nil, got %+v", got)
	}
}