// round_deadline_test.go: the next round's deadline DERIVES from the instant
// the previous round resolved.
//
// The original defect this guards (WS3): every round shared round 1's absolute
// deadline, so the server-side sweeper mass-expired rounds 2..N mid-duel. The
// existing ordering test asserts round 2's deadline is AFTER round 1's, which
// catches that. But it does so through the wall clock: both rounds are stamped
// by separate time.Now() reads, and on a fast machine those reads land in the
// same microsecond, so the deadlines come out EQUAL and "after" is false. That
// test has been failing on main for exactly this reason, and it is a fixture
// problem, not a product one: it models zero elapsed time between a round being
// served and being answered, which cannot happen to a learner.
//
// Relaxing it to ">=" would be the wrong fix: ">=" is satisfied by the very
// defect the test exists to catch, since one shared deadline is equal to
// itself. So the guard is restated as the DERIVATION instead of the ordering.
// round 2's deadline == round 1's ResolvedAt + the round timer is exact, is
// independent of clock resolution, and is false under the original defect (a
// shared deadline is derived from round 1's SERVE, not its resolution).
package duel

import (
	"testing"
	"time"
)

// The invariant, stated exactly: the next round's deadline is the resolution
// instant plus the configured timer.
func TestResolveRound_NextRoundDeadlineDerivesFromTheResolutionInstant(t *testing.T) {
	d := newInProgressDuel(t)
	stampRound1(d)

	if _, err := d.ResolveRound("gcid-a", 1, true, 5000, []int{1, 2, 3, 5}); err != nil {
		t.Fatalf("ResolveRound: %v", err)
	}

	resolved := d.Rounds[0].ResolvedAt
	if resolved == nil {
		t.Fatal("round 1 has no ResolvedAt after resolving")
	}
	deadline := d.Rounds[1].DeadlineAt
	if deadline == nil {
		t.Fatal("round 2 has no DeadlineAt after round 1 resolved")
	}

	want := resolved.Add(30 * time.Second)
	if !deadline.Equal(want) {
		t.Errorf("round 2 deadline = %v; want round 1's ResolvedAt + 30s = %v.\n"+
			"The deadline must derive from the SAME instant the round resolved, not from a second clock read: "+
			"two reads can straddle a tick, and a deadline that does not derive from the resolution cannot be "+
			"checked against it at all.", deadline, want)
	}
}

// The same derivation on the sweeper path, where the caller supplies `now`
// precisely so the sweep is deterministic. If the timeout path re-read the
// clock instead of using the instant it was handed, a sweep replayed at a
// fixed `now` would stamp a deadline unrelated to that `now`.
func TestResolveRoundTimeout_NextRoundDeadlineDerivesFromTheSuppliedNow(t *testing.T) {
	d := newInProgressDuel(t)
	past := time.Now().UTC().Add(-time.Hour)
	d.Rounds[0].DeadlineAt = &past

	sweepAt := past.Add(90 * time.Second)
	if _, err := d.ResolveRoundTimeout(1, sweepAt); err != nil {
		t.Fatalf("ResolveRoundTimeout: %v", err)
	}

	deadline := d.Rounds[1].DeadlineAt
	if deadline == nil {
		t.Fatal("round 2 has no DeadlineAt after round 1 timed out")
	}
	want := sweepAt.Add(30 * time.Second)
	if !deadline.Equal(want) {
		t.Errorf("round 2 deadline = %v; want the SUPPLIED now + 30s = %v.\n"+
			"The sweep passes `now` so it is deterministic and testable without sleeping; stamping from a "+
			"fresh time.Now() would quietly reintroduce the nondeterminism the parameter exists to remove.",
			deadline, want)
	}
}

// The original defect, restated so it cannot come back by another route: two
// rounds must never share one absolute deadline.
func TestResolveRound_RoundsDoNotShareOneAbsoluteDeadline(t *testing.T) {
	d := newInProgressDuel(t)
	stampRound1(d)
	round1Deadline := *d.Rounds[0].DeadlineAt

	if _, err := d.ResolveRound("gcid-a", 1, true, 5000, []int{1, 2, 3, 5}); err != nil {
		t.Fatalf("ResolveRound: %v", err)
	}

	if d.Rounds[1].DeadlineAt.Equal(round1Deadline) {
		t.Fatalf("rounds 1 and 2 share the deadline %v; the sweeper would mass-expire rounds 2..N mid-duel",
			round1Deadline)
	}
}
