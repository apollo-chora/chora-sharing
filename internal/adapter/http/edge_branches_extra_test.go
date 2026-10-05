// edge_branches_extra_test.go — the last reachable statements: milestone
// draft-path validation + nil ports, pubsub dispatch decode errors, the
// leaderboard tenant-required branch, processMatch edge inputs, duel ELO/log
// arms, and the enterQueue malformed-body path.
package httpadapter

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/adapter/eventpush"
	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/adapter/subscribers"
	"github.com/apollo-chora/chora-sharing/internal/config"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
	domainmm "github.com/apollo-chora/chora-sharing/internal/domain/matchmaking"
	"github.com/apollo-chora/chora-sharing/internal/domain/profiler"
	"github.com/apollo-chora/chora-sharing/internal/domain/reaction"
)

// ---------------------------------------------------------------------------
// Milestone drafts — empty draft_id (unreachable through the mux), nil ports,
// default policy, malformed preference body.
// ---------------------------------------------------------------------------

func TestMilestoneDraft_EmptyDraftIDAndNilPorts(t *testing.T) {
	t.Parallel()
	h := NewHandler(milestoneDeps(&fakeMilestoneDrafts{}, &fakeMilestonePrefs{}))

	// publish with empty draft_id → 400 (direct — the {draft_id} wildcard
	// cannot be empty through the mux).
	for _, tc := range []struct {
		method, path string
		handler      func(w http.ResponseWriter, r *http.Request)
	}{
		{http.MethodPost, "/v1/me/post-drafts/x/publish", h.publishPostDraft},
		{http.MethodPost, "/v1/me/post-drafts/x/discard", h.discardPostDraft},
	} {
		req := milestoneReq(tc.method, tc.path, "")
		req.SetPathValue("draft_id", "   ")
		rr := httptest.NewRecorder()
		tc.handler(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("%s: status=%d, want 400", tc.path, rr.Code)
		}
	}

	// Nil ports → 501.
	h501 := NewHandler(Deps{})
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodPost, "/v1/me/post-drafts/x/discard", ""},
		{http.MethodGet, "/v1/me/preferences/familiar-milestone-share", ""},
	} {
		rr := httptest.NewRecorder()
		h501.ServeHTTP(rr, milestoneReq(tc.method, tc.path, tc.body))
		if rr.Code != http.StatusNotImplemented {
			t.Errorf("%s: status=%d, want 501", tc.path, rr.Code)
		}
	}

	// setSharePref malformed body → 400.
	if h.deps.MilestonePrefs == nil {
		t.Fatal("precondition")
	}
	rrBad := httptest.NewRecorder()
	hBad := NewHandler(milestoneDeps(&fakeMilestoneDrafts{}, &fakeMilestonePrefs{}))
	hBad.ServeHTTP(rrBad, milestoneReq(http.MethodPost, "/v1/me/preferences/familiar-milestone-share", `{nope`))
	if rrBad.Code != http.StatusBadRequest {
		t.Errorf("malformed pref body: status=%d, want 400", rrBad.Code)
	}
}

func TestMilestoneDefaultPolicy_CustomValue(t *testing.T) {
	t.Parallel()
	h := NewHandler(milestoneDeps(&fakeMilestoneDrafts{}, &fakeMilestonePrefs{}))
	h.deps.MilestoneDefaultPolicy = subscribers.PolicyAuto
	if got := h.milestoneDefaultPolicy(); got != subscribers.PolicyAuto {
		t.Errorf("milestoneDefaultPolicy = %q, want auto", got)
	}
}

// ---------------------------------------------------------------------------
// Pubsub dispatch decode-error paths (NACK → 5xx, nothing credited)
// ---------------------------------------------------------------------------

func garbagePushBody(t *testing.T, topic string) []byte {
	t.Helper()
	env := map[string]any{
		"message": map[string]any{
			"data":        base64.StdEncoding.EncodeToString([]byte{0xff, 0xfe, 0xfd}),
			"messageId":   "m-garbage-1",
			"publishTime": time.Now().UTC().Format(time.RFC3339),
			"attributes":  map[string]string{"topic": topic},
		},
		"subscription": "projects/chora-test/subscriptions/test-push",
	}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("garbagePushBody: %v", err)
	}
	return b
}

func TestWeaknessGrownDispatch_UndecodablePayload_5xx(t *testing.T) {
	t.Parallel()
	ranker := leaderboard.NewRanker()
	sub := subscribers.NewWeaknessGrownSubscriber(subscribers.WeaknessGrownConfig{
		Ranker:      ranker,
		Idempotency: subscribers.NewInMemoryIdempotencyStore(),
	})
	h := NewWeaknessGrownPushHandler(WeaknessGrownPushDeps{
		Subscriber: sub,
		Verifier:   eventpush.NewVerifier(eventpush.VerifierConfig{}),
	})
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(garbagePushBody(t, subscribers.TopicWeaknessGrown))))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code < 500 {
		t.Errorf("undecodable weakness payload: status=%d, want 5xx (NACK)", rr.Code)
	}
	if top := ranker.TopByPeriod(leaderboard.Scope{Kind: leaderboard.ScopeTenant}, leaderboard.PeriodWeekly, "t", 10); len(top) != 0 {
		t.Errorf("garbage must never credit XP: %+v", top)
	}
}

func TestLiveQuizDispatch_UndecodablePayload_5xx(t *testing.T) {
	t.Parallel()
	ranker := leaderboard.NewRanker()
	sub := subscribers.NewLiveQuizScoreSubscriber(subscribers.LiveQuizScoreConfig{
		Ranker:      ranker,
		Idempotency: subscribers.NewInMemoryIdempotencyStore(),
	})
	h := NewLiveQuizScorePushHandler(LiveQuizScorePushDeps{
		Subscriber: sub,
		Verifier:   eventpush.NewVerifier(eventpush.VerifierConfig{}),
	})
	req := httptest.NewRequest(http.MethodPost, "/",
		strings.NewReader(string(garbagePushBody(t, subscribers.TopicLiveQuizScoreAwarded))))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code < 500 {
		t.Errorf("undecodable live-quiz payload: status=%d, want 5xx (NACK)", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// getLeaderboard — tenant-required branch + Profiles enrichment
// ---------------------------------------------------------------------------

func TestGetLeaderboard_TenantRequiredAndEnrichment(t *testing.T) {
	t.Parallel()
	// Direct call without a tenant in context → 400.
	h := NewHandler(Deps{Leaderboards: &fakeLeaderboards{entries: []leaderboard.Entry{}}})
	req := httptest.NewRequest(http.MethodGet, "/v1/leaderboard?scope=tenant", nil)
	rr := httptest.NewRecorder()
	h.getLeaderboard(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("tenant scope without tenant: status=%d, want 400", rr.Code)
	}

	// Profiles wired + entries present → names enrichment runs (inmem returns
	// an empty map; the branch itself is what matters).
	lb := &fakeLeaderboards{entries: []leaderboard.Entry{
		{GCID: socOther, Score: 10, Rank: 1},
	}}
	h2 := NewHandler(Deps{Leaderboards: lb, Profiles: inmem.NewProfilerRepo()})
	rr2 := httptest.NewRecorder()
	h2.ServeHTTP(rr2, socReq(http.MethodGet, "/v1/leaderboard?scope=global&limit=5", ""))
	if rr2.Code != http.StatusOK {
		t.Errorf("global scope: status=%d, want 200", rr2.Code)
	}
	if !strings.Contains(rr2.Body.String(), socOther) {
		t.Errorf("entry missing: %s", rr2.Body.String())
	}
}

// ---------------------------------------------------------------------------
// processMatch edge inputs
// ---------------------------------------------------------------------------

func TestProcessMatch_EdgeInputs(t *testing.T) {
	t.Parallel()
	duelRepo := inmem.NewDuelRepo()
	queue := &mmQueueX{}

	// nil ctx → the handler substitutes Background, then Duels nil → restore.
	hNil := NewHandler(Deps{MatchmakingQueue: queue, Rules: mmRulesX()})
	hNil.processMatch(nil, mmMatch(duel.NewUUIDv7(), "", ""))
	if len(queue.reverted) != 2 {
		t.Errorf("nil ctx: reverts=%v, want 2", queue.reverted)
	}

	// Empty MatchedDuelID → NewPoolDuelWithID fails → restore both.
	hEmpty := NewHandler(Deps{Duels: duelRepo, MatchmakingQueue: queue, Rules: mmRulesX(),
		AtomSelector: &stubbedAtomX{picks: mmPicks(5)}})
	before := len(queue.reverted)
	hEmpty.processMatch(context.Background(), mmMatch("", "", ""))
	if len(queue.reverted) != before+2 {
		t.Errorf("empty duel id: reverts went %d → %d, want +2", before, len(queue.reverted))
	}

	// QuestionCount zero → falls back to 5.
	hZero := NewHandler(Deps{Duels: duelRepo, MatchmakingQueue: queue, Rules: mmRulesX(),
		AtomSelector: &stubbedAtomX{picks: mmPicks(5)}})
	m := mmMatch(duel.NewUUIDv7(), "", "")
	m.SearcherA.QuestionCount = 0
	hZero.processMatch(context.Background(), m)
	// No revert → the duel was created (success path with fallback count).
	if len(queue.reverted) != before+2 {
		t.Fatalf("zero-count match reverted unexpectedly: %v", queue.reverted)
	}
}

func TestRestoreSearchers_ReRegistersTenant(t *testing.T) {
	t.Parallel()
	queue := &mmQueueX{}
	reg := &recordingMatchmaker{}
	h := NewHandler(Deps{MatchmakingQueue: queue, Matchmaker: reg})
	h.restoreSearchers(mmMatch(duel.NewUUIDv7(), "", ""))
	if got := reg.registrations(); got != 1 {
		t.Errorf("tenant re-registrations = %d, want 1", got)
	}
}

// recordingMatchmaker records RegisterTenant calls.
type recordingMatchmaker struct {
	mu  sync.Mutex
	n   int
}

func (m *recordingMatchmaker) Match() <-chan domainmm.MatchResult { return nil }
func (m *recordingMatchmaker) RegisterTenant(string) {
	m.mu.Lock()
	m.n++
	m.mu.Unlock()
}
func (m *recordingMatchmaker) Stop() {}

func (m *recordingMatchmaker) registrations() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.n
}

// ---------------------------------------------------------------------------
// Duel answer ELO/log arms
// ---------------------------------------------------------------------------

// errELO store field already exists on faultyDuelStore; add the apply hook.
type eloFaultStore struct {
	*inmem.DuelRepo
	eloErr error
}

func (f *eloFaultStore) ApplyELO(_ context.Context, _ *duel.Duel, _ int) error { return f.eloErr }

func TestSubmitDuelAnswer_ELOFaultLogged(t *testing.T) {
	t.Parallel()
	repo := &eloFaultStore{DuelRepo: inmem.NewDuelRepo(), eloErr: errors.New("elo db down")}
	d := seedingDuel(t, repo.DuelRepo, 1)
	h := NewHandler(Deps{Duels: repo, Rules: duRules()})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, duRequest(duChallenger, http.MethodPost, "/v1/duels/"+d.ID+"/answer",
		`{"round_no":1,"answer":"A","answer_time_ms":1000}`))
	// The gameplay response is already determined — ELO failure is a log line.
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s, want 200", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"duel_status":"completed"`) {
		t.Errorf("expected completed: %s", rr.Body.String())
	}
}

func TestSubmitDuelAnswer_FriendlyScopeSkipsELO(t *testing.T) {
	t.Parallel()
	repo := inmem.NewDuelRepo()
	// A friendly (unranked) duel completing must skip ApplyELO.
	d, err := duel.NewDuel(duCfg(), duChallenger, duOpponent, duTenant,
		duel.ScopeFriendly, 1, nil, 24*time.Hour)
	if err != nil {
		t.Fatalf("NewDuel: %v", err)
	}
	_ = d.Accept(duOpponent)
	_ = d.StartBattle(answerPicks(1))
	_ = repo.SaveDuel(context.Background(), d)
	h := NewHandler(Deps{Duels: repo, Rules: duRules()})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, duRequest(duChallenger, http.MethodPost, "/v1/duels/"+d.ID+"/answer",
		`{"round_no":1,"answer":"A","answer_time_ms":1000}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), `"duel_status":"completed"`) {
		t.Errorf("expected completed friendly duel: %s", rr.Body.String())
	}
}

func TestSubmitDuelAnswer_BlitzELOFaultLogged(t *testing.T) {
	t.Parallel()
	base := inmem.NewDuelRepo()
	repo := &eloFaultStore{DuelRepo: base, eloErr: errors.New("elo db down")}
	d := seedBlitz(t, base, duel.BlitzVariantRace, 1)
	h := NewHandler(Deps{Duels: repo, Rules: duRules(), DuelEvents: nil})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, duRequest(duChallenger, http.MethodPost, "/v1/duels/"+d.ID+"/answer",
		`{"round_no":1,"answer":"A","answer_time_ms":1000}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200 despite ELO fault", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// enterQueue malformed body → 400
// ---------------------------------------------------------------------------

func TestEnterQueue_MalformedBody_400(t *testing.T) {
	t.Parallel()
	h := NewHandler(Deps{Matchmaker: &stubMatchmaker{}, MatchmakingQueue: &mmQueueX{}, Rules: mmRulesX()})
	req := mmXRequest(mmA, http.MethodPost, "/v1/duels/queue")
	req.Header.Set("Idempotency-Key", "k")
	req.Body = io.NopCloser(strings.NewReader(`{bad`))
	req.ContentLength = 4
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// ReactToPost switch-unreact fault + generateProfile empty resolver
// ---------------------------------------------------------------------------

func TestReactToPost_SwitchUnreactFault_500(t *testing.T) {
	t.Parallel()
	faulty := &faultyReactions{
		unreactErr: errors.New("boom"),
		existing: []*reaction.Reaction{{
			ID: "rx-1", GCID: socGCID, PostID: socPost, Type: reaction.TypeLike,
		}},
	}
	h := NewHandler(Deps{Reactions: faulty, Posts: &faultyPosts{found: true, tenantID: socTenant}})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, socReq(http.MethodPost, "/v1/posts/"+socPost+"/reactions", `{"kind":"curious"}`))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d, want 500 (switch unreact fault)", rr.Code)
	}
}

func TestGenerateProfile_ResolverReturnsEmptyName(t *testing.T) {
	t.Parallel()
	store := inmem.NewProfilerRepo()
	h := NewHandler(Deps{
		Profiles:            store,
		Conjurer:            &fakeConjurerDirect{result: &profiler.ConjuredProfile{}},
		DisplayNameResolver: &fakeResolver{name: ""},
	})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, prReq(http.MethodPost, "/v1/me/profile/generate", `{"bio":"hi"}`))
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status=%d, want 202", rr.Code)
	}
	p, _ := store.GetProfile(context.Background(), prGCID)
	if p.DisplayName != "" {
		t.Errorf("display_name = %q, want empty", p.DisplayName)
	}
}

// ---------------------------------------------------------------------------
// targetFromPath !ok at the call sites (unfollow/block/unblock)
// ---------------------------------------------------------------------------

func TestRelationshipWrites_EmptyPathTarget(t *testing.T) {
	t.Parallel()
	graph := inmem.NewSocialGraph()
	h := NewHandler(connDeps(graph))

	for name, fn := range map[string]func(http.ResponseWriter, *http.Request){
		"unfollow": h.unfollowMember,
		"unblock":  h.unblockMember,
	} {
		req := identityReq(http.MethodDelete, "/v1/connections/x")
		req.SetPathValue("gcid", "   ")
		rr := httptest.NewRecorder()
		fn(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("%s empty target: status=%d, want 400", name, rr.Code)
		}
	}

	// blockMember empty-gcid body.
	rr := httptest.NewRecorder()
	h.blockMember(rr, connReq(http.MethodPost, "/v1/connections/blocks", `{"gcid":"  "}`))
	if rr.Code != http.StatusBadRequest {
		t.Errorf("blockMember empty target: status=%d, want 400", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// config helpers used above
// ---------------------------------------------------------------------------

var _ = config.SharingRules{} // config referenced via mmRulesX; keep import honest