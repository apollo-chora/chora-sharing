// atom_session_completed_subscriber_test.go — §8.2 XP-credit subscriber.
//
// AtomSessionCompletedSubscriber consumes chora.consumption.atom_session.completed.v1
// and credits XP to the atom_xp_credits projection. These specs cover the
// whole surface over a fake XPCreditWriter:
//
//   - the happy credit path (CreditXP called with the envelope's identity),
//   - the missing-dependency preconditions (Writer / Idempotency),
//   - a CreditXP failure → NACK UNMARKED so the redelivery re-credits,
//   - idempotency: a re-delivery of the same event_id skips (never double-
//     credits) once the first delivery marked the key.
package subscribers

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// xpCreditCall records one CreditXP invocation.
type xpCreditCall struct {
	tenantID   string
	gcid       string
	atomID     string
	sessionID  string
	eventID    string
	xp         int
	occurredAt time.Time
}

// fakeXPCredit is an in-memory XPCreditWriter double that fails on demand.
type fakeXPCredit struct {
	mu       sync.Mutex
	calls    []xpCreditCall
	failOn   int // 0 = never fail; >0 = fail on the Nth call
	err      error
}

func (f *fakeXPCredit) CreditXP(_ context.Context, tenantID, gcid, atomID, sessionID, eventID string, xp int, occurredAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.calls = append(f.calls, xpCreditCall{
		tenantID: tenantID, gcid: gcid, atomID: atomID, sessionID: sessionID, eventID: eventID,
		xp: xp, occurredAt: occurredAt,
	})
	if f.failOn > 0 && len(f.calls) == f.failOn {
		return errors.New("xp ledger down")
	}
	return nil
}

func (f *fakeXPCredit) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func newSessionSub(t *testing.T) (*AtomSessionCompletedSubscriber, *fakeXPCredit, *InMemoryIdempotencyStore) {
	t.Helper()
	c := &fakeXPCredit{}
	idem := NewInMemoryIdempotencyStore()
	sub := NewAtomSessionCompletedSubscriber(AtomSessionCompletedConfig{
		XPWriter:    c,
		Idempotency: idem,
	})
	return sub, c, idem
}

func sampleSessionCompleted() AtomSessionCompletedEnvelope {
	return AtomSessionCompletedEnvelope{
		EventID:    "01970000-0000-7000-9000-0000000000e1",
		TenantID:   "tenant-1",
		GCID:       "gcid-learner-1",
		AtomID:     "atom-1",
		SessionID:  "sess-1",
		XP:         25,
		OccurredAt: time.Date(2026, 7, 10, 8, 30, 0, 0, time.UTC),
	}
}

func TestAtomSessionCompleted_CreditsXP(t *testing.T) {
	sub, c, _ := newSessionSub(t)
	env := sampleSessionCompleted()

	if err := sub.HandleAtomSessionCompleted(context.Background(), env); err != nil {
		t.Fatalf("HandleAtomSessionCompleted: %v", err)
	}
	if got := c.count(); got != 1 {
		t.Fatalf("CreditXP calls = %d, want 1", got)
	}
	c.mu.Lock()
	call := c.calls[0]
	c.mu.Unlock()
	if call.tenantID != env.TenantID || call.gcid != env.GCID || call.atomID != env.AtomID ||
		call.sessionID != env.SessionID || call.eventID != env.EventID || call.xp != env.XP {
		t.Fatalf("CreditXP call = %+v; want the full envelope identity", call)
	}
	if !call.occurredAt.Equal(env.OccurredAt) {
		t.Fatalf("occurred_at = %v; want %v", call.occurredAt, env.OccurredAt)
	}
}

func TestAtomSessionCompleted_IdempotentOnReplay(t *testing.T) {
	sub, c, idem := newSessionSub(t)
	env := sampleSessionCompleted()

	if err := sub.HandleAtomSessionCompleted(context.Background(), env); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	// Same event_id redelivery — the peek sees the marked key and skips.
	if err := sub.HandleAtomSessionCompleted(context.Background(), env); err != nil {
		t.Fatalf("redelivery must ack: %v", err)
	}
	if got := c.count(); got != 1 {
		t.Fatalf("CreditXP calls after replay = %d, want 1 (no double-credit)", got)
	}
	if !idem.Recorded(HandlerAtomSessionCompleted, env.EventID) {
		t.Fatalf("idempotency key not recorded")
	}
}

func TestAtomSessionCompleted_CreditFailure_NACKsUnmarked(t *testing.T) {
	sub, c, idem := newSessionSub(t)
	c.err = errors.New("ledger down")

	if err := sub.HandleAtomSessionCompleted(context.Background(), sampleSessionCompleted()); err == nil {
		t.Fatalf("CreditXP failure must NACK (error), got nil")
	}
	if idem.Recorded(HandlerAtomSessionCompleted, sampleSessionCompleted().EventID) {
		t.Fatalf("the key must stay UNMARKED after a failed credit so redelivery re-runs")
	}
}

func TestAtomSessionCompleted_MissingDeps_FailsLoud(t *testing.T) {
	idem := NewInMemoryIdempotencyStore()

	noWriter := NewAtomSessionCompletedSubscriber(AtomSessionCompletedConfig{
		XPWriter:    nil,
		Idempotency: idem,
	})
	if err := noWriter.HandleAtomSessionCompleted(context.Background(), sampleSessionCompleted()); err == nil {
		t.Fatalf("missing XP writer must fail loud")
	}

	noIdem := NewAtomSessionCompletedSubscriber(AtomSessionCompletedConfig{
		XPWriter: &fakeXPCredit{},
	})
	if err := noIdem.HandleAtomSessionCompleted(context.Background(), sampleSessionCompleted()); err == nil {
		t.Fatalf("missing idempotency store must fail loud")
	}
}

func TestNewAtomSessionCompletedSubscriber_DefaultLogger(t *testing.T) {
	sub := NewAtomSessionCompletedSubscriber(AtomSessionCompletedConfig{})
	if sub.cfg.Logger == nil {
		t.Fatalf("nil Logger must fall back to log.Default()")
	}
}