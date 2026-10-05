// coverage_extra_test.go — black-box coverage for the event Publisher
// methods that lacked tests (PublishDuelCompleted / PublishRoyaltySettled /
// timestamp zero-fallback / publish error branches) and the
// InMemoryClosureRepo.Pseudonymise rows-sum loop. Reuses the fakeBus
// double from publisher_test.go and the InMemoryClosureRepo default store.
package events_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/adapter/events"
	"github.com/apollo-chora/chora-sharing/internal/config"
)

func TestPublisher_PublishDuelCompleted(t *testing.T) {
	bus := &fakeBus{}
	now := time.Now().UTC()
	p := events.NewPublisher(events.Config{Bus: bus, Now: func() time.Time { return now }})

	err := p.PublishDuelCompleted(context.Background(), events.DuelEvent{
		DuelID:          "duel-1",
		TenantID:        tenantA,
		ChallengerGCID:  gcidA,
		OpponentGCID:    gcidB,
		WinnerGCID:      gcidA,
		Scope:           "ranked",
		ScoreChallenger: 3,
		ScoreOpponent:   1,
		OccurredAt:      now,
	})
	if err != nil {
		t.Fatalf("PublishDuelCompleted: %v", err)
	}
	calls := bus.recorded()
	if len(calls) != 1 {
		t.Fatalf("publishes = %d, want 1", len(calls))
	}
	got := calls[0]
	if got.topic != events.TopicDuelCompleted {
		t.Errorf("topic = %q, want %q", got.topic, events.TopicDuelCompleted)
	}
	if got.env.TenantID != tenantA {
		t.Errorf("env.TenantID = %q, want %q", got.env.TenantID, tenantA)
	}
	if got.env.GCID != gcidA {
		t.Errorf("env.GCID = %q, want challenger %q", got.env.GCID, gcidA)
	}
	if got.payload == nil {
		t.Fatal("payload is nil")
	}
}

func TestPublisher_PublishRoyaltySettled(t *testing.T) {
	bus := &fakeBus{}
	now := time.Now().UTC()
	p := events.NewPublisher(events.Config{Bus: bus, Now: func() time.Time { return now }})

	err := p.PublishRoyaltySettled(context.Background(), events.RoyaltySettledEvent{
		SettlementID:    "settle-1",
		GrantID:         "grant-1",
		OwnerGCID:       gcidA,
		GranteeTenantID: tenantA,
		AtomID:          "atom-1",
		Amount:          12.5,
		Currency:        "mana",
		UsageContext:    "duel",
		SourceEventID:   "src-1",
		TenantID:        tenantA,
		OccurredAt:      now,
	})
	if err != nil {
		t.Fatalf("PublishRoyaltySettled: %v", err)
	}
	calls := bus.recorded()
	if len(calls) != 1 {
		t.Fatalf("publishes = %d, want 1", len(calls))
	}
	got := calls[0]
	if got.topic != events.TopicRoyaltySettled {
		t.Errorf("topic = %q, want %q", got.topic, events.TopicRoyaltySettled)
	}
	if got.env.GCID != gcidA {
		t.Errorf("env.GCID = %q, want owner %q", got.env.GCID, gcidA)
	}
}

func TestPublisher_TimestampZeroFallsBackToNow(t *testing.T) {
	bus := &fakeBus{}
	now := time.Now().UTC()
	p := events.NewPublisher(events.Config{Bus: bus, Now: func() time.Time { return now }})
	// OccurredAt zero → timestamp returns cfg.Now(); both the zero branch
	// and the derived payload lines execute.
	if err := p.PublishPostCreated(context.Background(), events.PostEvent{
		PostID: "p-1", TenantID: tenantA, AuthorGCID: gcidA, Visibility: "public", AtomID: "a-1",
	}); err != nil {
		t.Fatalf("PublishPostCreated: %v", err)
	}
	calls := bus.recorded()
	if len(calls) != 1 {
		t.Fatalf("publishes = %d, want 1", len(calls))
	}
	if calls[0].topic != events.TopicPostCreated {
		t.Errorf("topic = %q, want %q", calls[0].topic, events.TopicPostCreated)
	}
}

func TestPublisher_TimestampNonZeroUTC(t *testing.T) {
	bus := &fakeBus{}
	p := events.NewPublisher(events.Config{Bus: bus, Now: func() time.Time { return time.Now().UTC() }})
	// Non-zero fixed timestamp in a non-UTC zone: timestamp() reverts to UTC.
	ts := time.Date(2026, 8, 8, 9, 0, 0, 0, time.FixedZone("X", 3600))
	if err := p.PublishReactionAdded(context.Background(), events.ReactionEvent{
		ReactionID: "r-1", TenantID: tenantA, GCID: gcidA, TargetType: "post", TargetID: "p-1", ReactionType: "like", OccurredAt: ts,
	}); err != nil {
		t.Fatalf("PublishReactionAdded: %v", err)
	}
	if len(bus.recorded()) != 1 {
		t.Fatalf("publishes = %d, want 1", len(bus.recorded()))
	}
}

func TestPublisher_Publish_NilBus(t *testing.T) {
	p := events.NewPublisher(events.Config{}) // Bus nil
	err := p.PublishPostCreated(context.Background(), events.PostEvent{PostID: "p-1"})
	if err == nil || !strings.Contains(err.Error(), "bus not configured") {
		t.Fatalf("expected bus-not-configured error, got %v", err)
	}
}

func TestPublisher_Publish_BusErrorPropagates(t *testing.T) {
	bus := &fakeBus{failOnce: context.Canceled}
	p := events.NewPublisher(events.Config{Bus: bus, Now: func() time.Time { return time.Now().UTC() }})
	err := p.PublishReactionAdded(context.Background(), events.ReactionEvent{
		ReactionID: "r-1", TenantID: tenantA, GCID: gcidA, TargetType: "post", TargetID: "p-1", ReactionType: "like", OccurredAt: time.Now().UTC(),
	})
	if err == nil || !strings.Contains(err.Error(), "publish") {
		t.Fatalf("expected bus publish error to propagate, got %v", err)
	}
}

func TestInMemoryClosureRepo_Pseudonymise_RowsSumAndIdempotent(t *testing.T) {
	repo := events.NewInMemoryClosureRepo()
	spec := []config.TableSpec{
		{Table: "profile", Columns: []config.ColumnSpec{{Column: "nick"}, {Column: "email"}}},
		{Table: "posts", Columns: []config.ColumnSpec{{Column: "body"}}},
	}
	n, err := repo.Pseudonymise(context.Background(), testTenantID, testGCID, spec)
	if err != nil {
		t.Fatalf("Pseudonymise: %v", err)
	}
	if n != 3 {
		t.Fatalf("rows = %d, want 3 (2 + 1 columns)", n)
	}
	// Idempotent: second call short-circuits to 0 rows.
	n, err = repo.Pseudonymise(context.Background(), testTenantID, testGCID, nil)
	if err != nil {
		t.Fatalf("Pseudonymise second: %v", err)
	}
	if n != 0 {
		t.Fatalf("rows on replay = %d, want 0", n)
	}
	// Empty spec on a fresh pair yields a zero row count but marks done.
	n, err = repo.Pseudonymise(context.Background(), testTenantID, "gcid-fresh", nil)
	if err != nil {
		t.Fatalf("Pseudonymise empty spec: %v", err)
	}
	if n != 0 {
		t.Fatalf("rows empty spec = %d, want 0", n)
	}
	done, err := repo.IsPseudonymised(context.Background(), testTenantID, "gcid-fresh")
	if err != nil || !done {
		t.Fatalf("IsPseudonymised after empty-spec call = %v/%v, want true/nil", done, err)
	}
	// Tenant-scoped dedup: same gcid under another tenant is not done.
	done, err = repo.IsPseudonymised(context.Background(), "other-tenant", "gcid-fresh")
	if err != nil || done {
		t.Fatalf("IsPseudonymised other tenant = %v/%v, want false/nil", done, err)
	}
}