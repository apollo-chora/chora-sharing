package subscribers

import (
	"context"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
)

// newWeaknessGrownSub builds a WeaknessGrownSubscriber wired to a fresh Ranker
// + in-memory idempotency store for tests.
func newWeaknessGrownSub(t *testing.T) (*WeaknessGrownSubscriber, *leaderboard.Ranker, *InMemoryIdempotencyStore) {
	t.Helper()
	ranker := leaderboard.NewRanker()
	idem := NewInMemoryIdempotencyStore()
	sub := NewWeaknessGrownSubscriber(WeaknessGrownConfig{
		Ranker:      ranker,
		Idempotency: idem,
	})
	return sub, ranker, idem
}

func sampleWeaknessGrown() WeaknessGrownEnvelope {
	return WeaknessGrownEnvelope{
		EventID:        "01970000-0000-7000-9000-0000000000b1",
		TenantID:       "tenant-1",
		LearnerGCID:    "gcid-learner-1",
		GrowthEdgeID:   "edge-1",
		ConceptLabel:   "Single Responsibility Principle",
		ConceptKey:     "single-responsibility-principle",
		FinalStrength:  0.05,
		RecoverySource: "drill_atom",
		Tags:           []string{"oop", "design"},
		GrownAt:        "2026-06-28T10:00:00Z",
	}
}

func TestWeaknessGrown_CreditsRanker_TenantWeeklyAndAllTime(t *testing.T) {
	sub, ranker, _ := newWeaknessGrownSub(t)

	if err := sub.HandleWeaknessGrown(context.Background(), sampleWeaknessGrown()); err != nil {
		t.Fatalf("HandleWeaknessGrown: %v", err)
	}

	tenantScope := leaderboard.Scope{Kind: leaderboard.ScopeTenant}

	// Weekly board credited with the default XP award.
	if got := ranker.RankOf(tenantScope, leaderboard.PeriodWeekly, "tenant-1", "gcid-learner-1"); got != 1 {
		t.Fatalf("weekly RankOf = %d, want 1", got)
	}
	weekly := ranker.TopByPeriod(tenantScope, leaderboard.PeriodWeekly, "tenant-1", 10)
	if len(weekly) != 1 || weekly[0].Score != DefaultWeaknessGrownXP {
		t.Fatalf("weekly top = %+v, want one entry score %d", weekly, DefaultWeaknessGrownXP)
	}

	// All-time board credited.
	allTime := ranker.TopByPeriod(tenantScope, leaderboard.PeriodAllTime, "tenant-1", 10)
	if len(allTime) != 1 || allTime[0].Score != DefaultWeaknessGrownXP {
		t.Fatalf("all-time top = %+v, want one entry score %d", allTime, DefaultWeaknessGrownXP)
	}
}

func TestWeaknessGrown_CustomXPAward(t *testing.T) {
	ranker := leaderboard.NewRanker()
	idem := NewInMemoryIdempotencyStore()
	sub := NewWeaknessGrownSubscriber(WeaknessGrownConfig{
		Ranker:        ranker,
		Idempotency:   idem,
		XPAwardPoints: 125,
	})

	if err := sub.HandleWeaknessGrown(context.Background(), sampleWeaknessGrown()); err != nil {
		t.Fatalf("HandleWeaknessGrown: %v", err)
	}

	tenantScope := leaderboard.Scope{Kind: leaderboard.ScopeTenant}
	top := ranker.TopByPeriod(tenantScope, leaderboard.PeriodWeekly, "tenant-1", 10)
	if len(top) != 1 || top[0].Score != 125 {
		t.Fatalf("weekly top = %+v, want score 125 (custom XPAwardPoints)", top)
	}
}

// TestWeaknessGrown_IdempotentOnDomainKey is the core "one reward per real
// grow" guarantee: a producer re-publish of the SAME grow (same growth_edge_id
// + grown_at) carrying a DIFFERENT event_id must NOT double-award.
func TestWeaknessGrown_IdempotentOnDomainKey(t *testing.T) {
	sub, ranker, idem := newWeaknessGrownSub(t)

	env1 := sampleWeaknessGrown()
	env2 := sampleWeaknessGrown()
	env2.EventID = "01970000-0000-7000-9000-0000000000b2" // fresh event_id, same grow

	if err := sub.HandleWeaknessGrown(context.Background(), env1); err != nil {
		t.Fatalf("first HandleWeaknessGrown: %v", err)
	}
	if err := sub.HandleWeaknessGrown(context.Background(), env2); err != nil {
		t.Fatalf("republish HandleWeaknessGrown: %v", err)
	}

	// Dedup is keyed on the domain identity (edge:grown_at), matching the
	// producer's idempotency_key, NOT on event_id.
	if !idem.Recorded(HandlerWeaknessGrown, "edge-1:2026-06-28T10:00:00Z") {
		t.Fatalf("idempotency store did not record the domain key")
	}

	tenantScope := leaderboard.Scope{Kind: leaderboard.ScopeTenant}
	top := ranker.TopByPeriod(tenantScope, leaderboard.PeriodWeekly, "tenant-1", 10)
	if len(top) != 1 || top[0].Score != DefaultWeaknessGrownXP {
		t.Fatalf("after republish weekly top = %+v, want score %d (no double-credit)", top, DefaultWeaknessGrownXP)
	}
}

// TestWeaknessGrown_IdempotentOnRedelivery covers Pub/Sub at-least-once
// redelivery of the exact same message.
func TestWeaknessGrown_IdempotentOnRedelivery(t *testing.T) {
	sub, ranker, _ := newWeaknessGrownSub(t)

	env := sampleWeaknessGrown()
	if err := sub.HandleWeaknessGrown(context.Background(), env); err != nil {
		t.Fatalf("first HandleWeaknessGrown: %v", err)
	}
	if err := sub.HandleWeaknessGrown(context.Background(), env); err != nil {
		t.Fatalf("redelivery HandleWeaknessGrown: %v", err)
	}

	tenantScope := leaderboard.Scope{Kind: leaderboard.ScopeTenant}
	top := ranker.TopByPeriod(tenantScope, leaderboard.PeriodWeekly, "tenant-1", 10)
	if len(top) != 1 || top[0].Score != DefaultWeaknessGrownXP {
		t.Fatalf("after redelivery weekly top = %+v, want score %d (no double-credit)", top, DefaultWeaknessGrownXP)
	}
}

// TestWeaknessGrown_DistinctGrowsAward proves a genuine RE-grow of the same
// edge (active->grown->active->grown later, distinct grown_at) is rewarded
// again — the dedup must not over-collapse distinct real grows.
func TestWeaknessGrown_DistinctGrowsAward(t *testing.T) {
	sub, ranker, _ := newWeaknessGrownSub(t)

	first := sampleWeaknessGrown()
	first.GrownAt = "2026-06-28T10:00:00Z"
	second := sampleWeaknessGrown()
	second.EventID = "01970000-0000-7000-9000-0000000000b9"
	second.GrownAt = "2026-07-15T09:00:00Z" // a later, distinct grow of the same edge

	if err := sub.HandleWeaknessGrown(context.Background(), first); err != nil {
		t.Fatalf("first grow: %v", err)
	}
	if err := sub.HandleWeaknessGrown(context.Background(), second); err != nil {
		t.Fatalf("second grow: %v", err)
	}

	tenantScope := leaderboard.Scope{Kind: leaderboard.ScopeTenant}
	top := ranker.TopByPeriod(tenantScope, leaderboard.PeriodWeekly, "tenant-1", 10)
	if len(top) != 1 || top[0].Score != 2*DefaultWeaknessGrownXP {
		t.Fatalf("two distinct grows weekly top = %+v, want score %d", top, 2*DefaultWeaknessGrownXP)
	}
}

// TestWeaknessGrown_FallbackDedupOnEventID covers the degraded case where the
// payload carries no grown_at — dedup falls back to event_id so redelivery is
// still safe.
func TestWeaknessGrown_FallbackDedupOnEventID(t *testing.T) {
	sub, ranker, idem := newWeaknessGrownSub(t)

	env := sampleWeaknessGrown()
	env.GrownAt = ""

	if err := sub.HandleWeaknessGrown(context.Background(), env); err != nil {
		t.Fatalf("first HandleWeaknessGrown: %v", err)
	}
	if err := sub.HandleWeaknessGrown(context.Background(), env); err != nil {
		t.Fatalf("redelivery HandleWeaknessGrown: %v", err)
	}

	if !idem.Recorded(HandlerWeaknessGrown, env.EventID) {
		t.Fatalf("fallback dedup should claim the event_id when grown_at is empty")
	}
	tenantScope := leaderboard.Scope{Kind: leaderboard.ScopeTenant}
	top := ranker.TopByPeriod(tenantScope, leaderboard.PeriodWeekly, "tenant-1", 10)
	if len(top) != 1 || top[0].Score != DefaultWeaknessGrownXP {
		t.Fatalf("fallback weekly top = %+v, want score %d (no double-credit)", top, DefaultWeaknessGrownXP)
	}
}

func TestWeaknessGrown_ValidationErrors(t *testing.T) {
	sub, _, _ := newWeaknessGrownSub(t)

	cases := map[string]WeaknessGrownEnvelope{
		"missing event_id":       {TenantID: "t", LearnerGCID: "g", GrowthEdgeID: "e"},
		"missing tenant_id":      {EventID: "e", LearnerGCID: "g", GrowthEdgeID: "e"},
		"missing learner_gcid":   {EventID: "e", TenantID: "t", GrowthEdgeID: "e"},
		"missing growth_edge_id": {EventID: "e", TenantID: "t", LearnerGCID: "g"},
	}
	for name, env := range cases {
		if err := sub.HandleWeaknessGrown(context.Background(), env); err == nil {
			t.Fatalf("%s: expected validation error, got nil", name)
		}
	}
}

func TestWeaknessGrown_Preflight(t *testing.T) {
	sub := NewWeaknessGrownSubscriber(WeaknessGrownConfig{})
	err := sub.HandleWeaknessGrown(context.Background(), sampleWeaknessGrown())
	if err == nil {
		t.Fatalf("expected preflight error with nil deps, got nil")
	}
}

func TestWeaknessGrown_SubscribedTopics(t *testing.T) {
	sub, _, _ := newWeaknessGrownSub(t)
	topics := sub.SubscribedTopics()
	if len(topics) != 1 || topics[0] != TopicWeaknessGrown {
		t.Fatalf("SubscribedTopics = %v, want [%s]", topics, TopicWeaknessGrown)
	}
}

func TestWeaknessGrown_DefaultXPApplied(t *testing.T) {
	// XPAwardPoints <= 0 in config must fall back to DefaultWeaknessGrownXP.
	sub := NewWeaknessGrownSubscriber(WeaknessGrownConfig{
		Ranker:        leaderboard.NewRanker(),
		Idempotency:   NewInMemoryIdempotencyStore(),
		XPAwardPoints: 0,
	})
	if got := sub.XPAward(); got != DefaultWeaknessGrownXP {
		t.Fatalf("XPAward() = %d, want default %d", got, DefaultWeaknessGrownXP)
	}
}

// -----------------------------------------------------------------------------
// Decode
// -----------------------------------------------------------------------------

func TestDecodeWeaknessGrown_NoAttrs(t *testing.T) {
	body := []byte(`{"event_id":"e1","tenant_id":"t1","learner_gcid":"g1","growth_edge_id":"edge-9","concept_label":"X","grown_at":"2026-06-28T10:00:00Z"}`)
	env, err := DecodeWeaknessGrownWithAttrs(body, nil)
	if err != nil {
		t.Fatalf("DecodeWeaknessGrownWithAttrs: %v", err)
	}
	if env.EventID != "e1" || env.TenantID != "t1" || env.LearnerGCID != "g1" || env.GrowthEdgeID != "edge-9" {
		t.Fatalf("decoded %+v, want e1/t1/g1/edge-9", env)
	}
	if env.GrownAt != "2026-06-28T10:00:00Z" {
		t.Fatalf("grown_at = %q", env.GrownAt)
	}
}

func TestDecodeWeaknessGrown_GcidFallback(t *testing.T) {
	// When the wire carries gcid (not learner_gcid), LearnerGCID falls back.
	body := []byte(`{"event_id":"e1","tenant_id":"t1","gcid":"env-gcid","growth_edge_id":"edge-9"}`)
	env, err := DecodeWeaknessGrownWithAttrs(body, nil)
	if err != nil {
		t.Fatalf("DecodeWeaknessGrownWithAttrs: %v", err)
	}
	if env.LearnerGCID != "env-gcid" {
		t.Fatalf("LearnerGCID = %q, want env-gcid (gcid fallback)", env.LearnerGCID)
	}
}

func TestDecodeWeaknessGrownWithAttrs(t *testing.T) {
	body := []byte(`{
		"growth_edge_id": "edge-7",
		"concept_label": "Recursion",
		"concept_key": "recursion",
		"final_strength": 0.04,
		"recovery_source": "drill_atom",
		"tags": ["cs", "fundamentals"],
		"grown_at": "2026-06-28T11:22:33Z"
	}`)
	attrs := map[string]string{
		"event_id":  "evt-1",
		"tenant_id": "tenant-1",
		"gcid":      "gcid-1",
	}
	env, err := DecodeWeaknessGrownWithAttrs(body, attrs)
	if err != nil {
		t.Fatalf("DecodeWeaknessGrownWithAttrs: %v", err)
	}
	if env.EventID != "evt-1" || env.TenantID != "tenant-1" || env.LearnerGCID != "gcid-1" {
		t.Fatalf("envelope fields not merged from attrs: %+v", env)
	}
	if env.GrowthEdgeID != "edge-7" || env.ConceptKey != "recursion" || env.RecoverySource != "drill_atom" {
		t.Fatalf("payload fields not decoded: %+v", env)
	}
	if len(env.Tags) != 2 || env.Tags[0] != "cs" {
		t.Fatalf("tags not decoded: %+v", env.Tags)
	}
	if env.GrownAt != "2026-06-28T11:22:33Z" {
		t.Fatalf("grown_at = %q", env.GrownAt)
	}
}
