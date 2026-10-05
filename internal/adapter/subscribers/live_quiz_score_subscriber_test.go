package subscribers

import (
	"context"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
)

// newScoreSub builds a LiveQuizScoreSubscriber wired to a fresh Ranker +
// in-memory idempotency store for tests.
func newScoreSub(t *testing.T) (*LiveQuizScoreSubscriber, *leaderboard.Ranker, *InMemoryIdempotencyStore) {
	t.Helper()
	ranker := leaderboard.NewRanker()
	idem := NewInMemoryIdempotencyStore()
	sub := NewLiveQuizScoreSubscriber(LiveQuizScoreConfig{
		Ranker:      ranker,
		Idempotency: idem,
	})
	return sub, ranker, idem
}

func TestLiveQuizScore_CreditsRanker_TenantWeeklyAndAllTime(t *testing.T) {
	sub, ranker, _ := newScoreSub(t)

	env := ScoreAwardedEnvelope{
		EventID:         "01970000-0000-7000-9000-0000000000a1",
		TenantID:        "tenant-1",
		GCID:            "gcid-learner-1",
		SessionID:       "sess-1",
		LiveQuizID:      "lq-1",
		QuestionID:      "q-1",
		AwardedPoints:   120,
		CumulativeScore: 120,
		Correct:         true,
		AnswerMillis:    3400,
	}

	if err := sub.HandleScoreAwarded(context.Background(), env); err != nil {
		t.Fatalf("HandleScoreAwarded: %v", err)
	}

	tenantScope := leaderboard.Scope{Kind: leaderboard.ScopeTenant}

	// Weekly board credited.
	if got := ranker.RankOf(tenantScope, leaderboard.PeriodWeekly, "tenant-1", "gcid-learner-1"); got != 1 {
		t.Fatalf("weekly RankOf = %d, want 1", got)
	}
	weekly := ranker.TopByPeriod(tenantScope, leaderboard.PeriodWeekly, "tenant-1", 10)
	if len(weekly) != 1 || weekly[0].Score != 120 {
		t.Fatalf("weekly top = %+v, want one entry score 120", weekly)
	}

	// All-time board credited.
	allTime := ranker.TopByPeriod(tenantScope, leaderboard.PeriodAllTime, "tenant-1", 10)
	if len(allTime) != 1 || allTime[0].Score != 120 {
		t.Fatalf("all-time top = %+v, want one entry score 120", allTime)
	}
}

func TestLiveQuizScore_Accumulates(t *testing.T) {
	sub, ranker, _ := newScoreSub(t)

	for i, pts := range []int{50, 75} {
		env := ScoreAwardedEnvelope{
			EventID:       "evt-" + string(rune('a'+i)),
			TenantID:      "tenant-1",
			GCID:          "gcid-1",
			AwardedPoints: pts,
		}
		if err := sub.HandleScoreAwarded(context.Background(), env); err != nil {
			t.Fatalf("HandleScoreAwarded #%d: %v", i, err)
		}
	}

	tenantScope := leaderboard.Scope{Kind: leaderboard.ScopeTenant}
	top := ranker.TopByPeriod(tenantScope, leaderboard.PeriodWeekly, "tenant-1", 10)
	if len(top) != 1 || top[0].Score != 125 {
		t.Fatalf("accumulated weekly top = %+v, want score 125", top)
	}
}

func TestLiveQuizScore_IdempotentOnDuplicateEventID(t *testing.T) {
	sub, ranker, idem := newScoreSub(t)

	env := ScoreAwardedEnvelope{
		EventID:       "dup-event-1",
		TenantID:      "tenant-1",
		GCID:          "gcid-1",
		AwardedPoints: 100,
	}

	if err := sub.HandleScoreAwarded(context.Background(), env); err != nil {
		t.Fatalf("first HandleScoreAwarded: %v", err)
	}
	// Replay same event_id — must be a no-op (no double credit).
	if err := sub.HandleScoreAwarded(context.Background(), env); err != nil {
		t.Fatalf("replay HandleScoreAwarded: %v", err)
	}

	if !idem.Recorded(HandlerLiveQuizScoreAwarded, "dup-event-1") {
		t.Fatalf("idempotency store did not record the event")
	}

	tenantScope := leaderboard.Scope{Kind: leaderboard.ScopeTenant}
	top := ranker.TopByPeriod(tenantScope, leaderboard.PeriodWeekly, "tenant-1", 10)
	if len(top) != 1 || top[0].Score != 100 {
		t.Fatalf("after replay weekly top = %+v, want score 100 (no double-credit)", top)
	}
}

func TestLiveQuizScore_NonPositivePointsNoOp(t *testing.T) {
	sub, ranker, _ := newScoreSub(t)

	env := ScoreAwardedEnvelope{
		EventID:       "evt-zero",
		TenantID:      "tenant-1",
		GCID:          "gcid-1",
		AwardedPoints: 0,
		Correct:       false,
	}
	if err := sub.HandleScoreAwarded(context.Background(), env); err != nil {
		t.Fatalf("HandleScoreAwarded zero points: %v", err)
	}

	tenantScope := leaderboard.Scope{Kind: leaderboard.ScopeTenant}
	top := ranker.TopByPeriod(tenantScope, leaderboard.PeriodWeekly, "tenant-1", 10)
	if len(top) != 0 {
		t.Fatalf("zero-point award credited board = %+v, want empty", top)
	}
}

func TestLiveQuizScore_ValidationErrors(t *testing.T) {
	sub, _, _ := newScoreSub(t)

	cases := map[string]ScoreAwardedEnvelope{
		"missing event_id":  {TenantID: "t", GCID: "g", AwardedPoints: 10},
		"missing tenant_id": {EventID: "e", GCID: "g", AwardedPoints: 10},
		"missing gcid":      {EventID: "e", TenantID: "t", AwardedPoints: 10},
	}
	for name, env := range cases {
		if err := sub.HandleScoreAwarded(context.Background(), env); err == nil {
			t.Fatalf("%s: expected validation error, got nil", name)
		}
	}
}

func TestLiveQuizScore_Preflight(t *testing.T) {
	sub := NewLiveQuizScoreSubscriber(LiveQuizScoreConfig{})
	err := sub.HandleScoreAwarded(context.Background(), ScoreAwardedEnvelope{
		EventID: "e", TenantID: "t", GCID: "g", AwardedPoints: 10,
	})
	if err == nil {
		t.Fatalf("expected preflight error with nil deps, got nil")
	}
}

func TestLiveQuizScore_SubscribedTopics(t *testing.T) {
	sub, _, _ := newScoreSub(t)
	topics := sub.SubscribedTopics()
	if len(topics) != 1 || topics[0] != TopicLiveQuizScoreAwarded {
		t.Fatalf("SubscribedTopics = %v, want [%s]", topics, TopicLiveQuizScoreAwarded)
	}
}

func TestDecodeScoreAwarded_NoAttrs(t *testing.T) {
	body := []byte(`{"event_id":"e1","tenant_id":"t1","gcid":"g1","awarded_points":42}`)
	env, err := DecodeScoreAwardedWithAttrs(body, nil)
	if err != nil {
		t.Fatalf("DecodeScoreAwardedWithAttrs: %v", err)
	}
	if env.EventID != "e1" || env.AwardedPoints != 42 {
		t.Fatalf("decoded %+v, want event_id e1 / awarded 42", env)
	}
}

func TestDecodeScoreAwarded_OwnerGcidFallback(t *testing.T) {
	// When the wire carries owner_gcid (not gcid), GCID falls back to it.
	body := []byte(`{"event_id":"e1","tenant_id":"t1","owner_gcid":"owner-g","awarded_points":5}`)
	env, err := DecodeScoreAwardedWithAttrs(body, nil)
	if err != nil {
		t.Fatalf("DecodeScoreAwardedWithAttrs: %v", err)
	}
	if env.GCID != "owner-g" {
		t.Fatalf("GCID = %q, want owner-g (owner_gcid fallback)", env.GCID)
	}
}

func TestScoreAwarded_IntCoercions(t *testing.T) {
	if got := asInt(int(7)); got != 7 {
		t.Fatalf("asInt(int) = %d", got)
	}
	if got := asInt(int64(8)); got != 8 {
		t.Fatalf("asInt(int64) = %d", got)
	}
	if got := asInt("nope"); got != 0 {
		t.Fatalf("asInt(string) = %d, want 0", got)
	}
	if got := asInt64(int64(9)); got != 9 {
		t.Fatalf("asInt64(int64) = %d", got)
	}
	if got := asInt64(int(10)); got != 10 {
		t.Fatalf("asInt64(int) = %d", got)
	}
	if got := asInt64("nope"); got != 0 {
		t.Fatalf("asInt64(string) = %d, want 0", got)
	}
}

func TestDecodeScoreAwardedWithAttrs(t *testing.T) {
	body := []byte(`{
		"session_id": "sess-1",
		"live_quiz_id": "lq-1",
		"question_id": "q-1",
		"awarded_points": 90,
		"cumulative_score": 270,
		"correct": true,
		"answer_millis": 4200
	}`)
	attrs := map[string]string{
		"event_id":  "evt-1",
		"tenant_id": "tenant-1",
		"gcid":      "gcid-1",
	}
	env, err := DecodeScoreAwardedWithAttrs(body, attrs)
	if err != nil {
		t.Fatalf("DecodeScoreAwardedWithAttrs: %v", err)
	}
	if env.EventID != "evt-1" || env.TenantID != "tenant-1" || env.GCID != "gcid-1" {
		t.Fatalf("envelope fields not merged from attrs: %+v", env)
	}
	if env.AwardedPoints != 90 || env.CumulativeScore != 270 || !env.Correct || env.AnswerMillis != 4200 {
		t.Fatalf("payload fields not decoded: %+v", env)
	}
	if env.SessionID != "sess-1" || env.LiveQuizID != "lq-1" || env.QuestionID != "q-1" {
		t.Fatalf("id fields not decoded: %+v", env)
	}
}
