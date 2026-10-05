// governance_policy_violation_subscriber_test.go — §8.2 moderation-hide
// subscriber.
//
// GovernancePolicyViolationSubscriber consumes
// chora.governance.policy.violation_detected.v1 and appends a
// 'moderation_hidden' ShareEvent to the atom_share_events audit log for EACH
// flagged share entry (the immutable feed entry is never mutated — R-15-A).
//
// These specs cover the whole surface over a fake ShareEventAppender:
// the happy append fan-out, the missing-dependency preconditions, an append
// failure mid-fan-out (NACK unmarked → redelivery re-applies the remaining
// entries), and the idempotency replay path.
package subscribers

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// appendCall records one AppendModerationHidden invocation.
type appendCall struct {
	tenantID      string
	feedEntryID   string
	actorGCID     string
	reason        string
	sourceEventID string
}

// fakeShareAppender is an in-memory ShareEventAppender double that fails on
// demand (failOn = Nth call fails).
type fakeShareAppender struct {
	mu     sync.Mutex
	calls  []appendCall
	err    error
	failOn int
}

func (f *fakeShareAppender) AppendModerationHidden(_ context.Context, tenantID, feedEntryID, actorGCID, reason, sourceEventID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	// Fail BEFORE recording the Nth call so a failure mid-fan-out leaves the
	// entries that already landed observable.
	if f.failOn > 0 && len(f.calls)+1 == f.failOn {
		return errors.New("audit log down")
	}
	f.calls = append(f.calls, appendCall{
		tenantID: tenantID, feedEntryID: feedEntryID, actorGCID: actorGCID,
		reason: reason, sourceEventID: sourceEventID,
	})
	return nil
}

func (f *fakeShareAppender) entries() []appendCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]appendCall(nil), f.calls...)
}

func newGovSub(t *testing.T) (*GovernancePolicyViolationSubscriber, *fakeShareAppender, *InMemoryIdempotencyStore) {
	t.Helper()
	a := &fakeShareAppender{}
	idem := NewInMemoryIdempotencyStore()
	sub := NewGovernancePolicyViolationSubscriber(GovernancePolicyViolationConfig{
		Appender:    a,
		Idempotency: idem,
	})
	return sub, a, idem
}

func samplePolicyViolation() GovernancePolicyViolationEnvelope {
	return GovernancePolicyViolationEnvelope{
		EventID:       "01970000-0000-7000-9000-0000000000g1",
		TenantID:      "tenant-1",
		GCID:          "gcid-author-1",
		ShareEntryIDs: []string{"entry-1", "entry-2"},
		Reason:        "inappropriate_content",
		Severity:      "high",
	}
}

func TestGovernancePolicyViolation_AppendsForEachEntry(t *testing.T) {
	sub, a, _ := newGovSub(t)
	env := samplePolicyViolation()

	if err := sub.HandleGovernancePolicyViolation(context.Background(), env); err != nil {
		t.Fatalf("HandleGovernancePolicyViolation: %v", err)
	}
	got := a.entries()
	if len(got) != 2 {
		t.Fatalf("append calls = %d, want 2 (one per flagged share entry)", len(got))
	}
	for i, want := range []string{"entry-1", "entry-2"} {
		if got[i].feedEntryID != want || got[i].tenantID != env.TenantID ||
			got[i].actorGCID != env.GCID || got[i].reason != env.Reason || got[i].sourceEventID != env.EventID {
			t.Fatalf("append[%d] = %+v; want the envelope identity for entry %s", i, got[i], want)
		}
	}
}

func TestGovernancePolicyViolation_AppendFailure_NACKsUnmarked(t *testing.T) {
	sub, a, idem := newGovSub(t)
	a.failOn = 2 // fails on the SECOND entry

	err := sub.HandleGovernancePolicyViolation(context.Background(), samplePolicyViolation())
	if err == nil {
		t.Fatalf("append failure mid-fan-out must NACK, got nil")
	}
	if got := a.entries(); len(got) != 1 {
		t.Fatalf("append calls = %d, want 1 (first entry landed before the failure)", len(got))
	}
	if idem.Recorded(HandlerGovernancePolicyViolation, samplePolicyViolation().EventID) {
		t.Fatalf("the key must stay UNMARKED so the redelivery re-applies")
	}
}

func TestGovernancePolicyViolation_IdempotentOnReplay(t *testing.T) {
	sub, a, idem := newGovSub(t)
	env := samplePolicyViolation()

	if err := sub.HandleGovernancePolicyViolation(context.Background(), env); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	if err := sub.HandleGovernancePolicyViolation(context.Background(), env); err != nil {
		t.Fatalf("redelivery must ack: %v", err)
	}
	if got := a.entries(); len(got) != 2 {
		t.Fatalf("append calls after replay = %d, want 2 (no double-append)", len(got))
	}
	if !idem.Recorded(HandlerGovernancePolicyViolation, env.EventID) {
		t.Fatalf("idempotency key not recorded")
	}
}

func TestGovernancePolicyViolation_MissingDeps_FailsLoud(t *testing.T) {
	idem := NewInMemoryIdempotencyStore()

	noAppender := NewGovernancePolicyViolationSubscriber(GovernancePolicyViolationConfig{
		Idempotency: idem,
	})
	if err := noAppender.HandleGovernancePolicyViolation(context.Background(), samplePolicyViolation()); err == nil {
		t.Fatalf("missing appender must fail loud")
	}

	noIdem := NewGovernancePolicyViolationSubscriber(GovernancePolicyViolationConfig{
		Appender: &fakeShareAppender{},
	})
	if err := noIdem.HandleGovernancePolicyViolation(context.Background(), samplePolicyViolation()); err == nil {
		t.Fatalf("missing idempotency store must fail loud")
	}
}

func TestNewGovernancePolicyViolationSubscriber_DefaultLogger(t *testing.T) {
	sub := NewGovernancePolicyViolationSubscriber(GovernancePolicyViolationConfig{})
	if sub.cfg.Logger == nil {
		t.Fatalf("nil Logger must fall back to log.Default()")
	}
}