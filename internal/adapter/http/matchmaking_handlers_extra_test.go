// matchmaking_handlers_extra_test.go — the processMatch / startBlitzBattle /
// writeQueueState branches not reachable through the happy-path HTTP tests in
// matchmaking_handlers_test.go (package httpadapter_test). These call the
// unexported pipeline directly so every fail-loud compensating action is
// pinned.
package httpadapter

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/config"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
	domainmm "github.com/apollo-chora/chora-sharing/internal/domain/matchmaking"
	"github.com/apollo-chora/chora-sharing/internal/domain/profiler"
)

const (
	mmTenantX = "01970000-0000-7000-8000-0000000000a1"
	mmA       = "01970000-0000-7000-9000-0000000000a1"
	mmB       = "01970000-0000-7000-9000-0000000000a2"
)

func mmRulesX() config.SharingRules {
	return config.SharingRules{
		ELOBaseline:         1200,
		ELOKFactor:          32,
		ComboTiers:          []int{1, 2, 3, 5},
		RoundTimerSec:       30,
		MatchmakingTimeout:  10 * time.Minute,
		MatchmakingHeartbeatStale: 30 * time.Second,
		MatchmakingMatchTick:      100 * time.Millisecond,
		MatchmakingSweepTick:      100 * time.Millisecond,
		BlitzTimeLimitSec:         120,
		BlitzRaceTarget:           3,
	}
}

// mmQueueX is a MatchmakingQueueRepo that records revert + status calls.
type mmQueueX struct {
	reverted     []string
	statusCalls  []domainmm.QueueStatus
	revertErr    error
	statusErr    error
	enqueueErr   error
	getByGCID    func(string) (*domainmm.Searcher, error)
	heartbeatErr error
}

func (q *mmQueueX) Enqueue(_ context.Context, _ domainmm.Searcher) error { return q.enqueueErr }
func (q *mmQueueX) UpdateStatus(_ context.Context, _, _ string, status domainmm.QueueStatus, _ string) error {
	q.statusCalls = append(q.statusCalls, status)
	return q.statusErr
}
func (q *mmQueueX) UpdateHeartbeat(_ context.Context, _, _ string) error { return q.heartbeatErr }
func (q *mmQueueX) GetByGCID(_ context.Context, _, gcid string) (*domainmm.Searcher, error) {
	if q.getByGCID != nil {
		return q.getByGCID(gcid)
	}
	return nil, nil
}
func (q *mmQueueX) RevertToFinding(_ context.Context, _, gcid, _ string) error {
	q.reverted = append(q.reverted, gcid)
	return q.revertErr
}

// stubbedAtomX supplies round picks for processMatch.
type stubbedAtomX struct {
	picks []duel.AtomPick
	err   error
}

func (s *stubbedAtomX) SelectAtoms(_ context.Context, _ AtomSelectionRequest) ([]duel.AtomPick, error) {
	return s.picks, s.err
}
func (s *stubbedAtomX) GetAtomForRound(_ context.Context, _ *duel.Duel, _ int) (duel.AtomPick, error) {
	return duel.AtomPick{}, nil
}

func mmPicks(n int) []duel.AtomPick {
	out := make([]duel.AtomPick, n)
	for i := range out {
		out[i] = duel.AtomPick{AtomID: "a", Question: "Q", Options: []string{"A", "B"}, Answer: "A"}
	}
	return out
}

// mmMatch builds a MatchResult for two searchers.
func mmMatch(duelID, mode, variant string) domainmm.MatchResult {
	now := time.Now().UTC()
	return domainmm.MatchResult{
		SearcherA: domainmm.Searcher{
			GCID: mmA, TenantID: mmTenantX, Proficiency: 1250,
			InterestTags: []string{"inheritance"}, QuestionCount: 5,
			Category: "overall", Mode: mode, BlitzVariant: variant,
			EnteredAt: now, ExpiresAt: now.Add(10 * time.Minute), LastHeartbeat: now,
		},
		SearcherB: domainmm.Searcher{
			GCID: mmB, TenantID: mmTenantX, Proficiency: 1200,
			InterestTags: []string{"inheritance"}, QuestionCount: 5,
			Category: "overall", Mode: mode, BlitzVariant: variant,
			EnteredAt: now, ExpiresAt: now.Add(10 * time.Minute), LastHeartbeat: now,
		},
		SharedTags:    []string{"inheritance"},
		MatchedDuelID: duelID,
	}
}

func TestProcessMatch_ClassicSuccess(t *testing.T) {
	t.Parallel()
	duelRepo := inmem.NewDuelRepo()
	queue := &mmQueueX{}
	reg := &recRegistrar{}
	h := NewHandler(Deps{
		Duels:                  duelRepo,
		MatchmakingQueue:       queue,
		Rules:                  mmRulesX(),
		AtomSelector:           &stubbedAtomX{picks: mmPicks(5)},
		RoundSweeperRegistrar:  reg,
	})

	duelID := duel.NewUUIDv7()
	h.processMatch(context.Background(), mmMatch(duelID, "", ""))

	d, err := duelRepo.GetDuel(context.Background(), duelID)
	if err != nil {
		t.Fatalf("duel not created: %v", err)
	}
	if d.Status != duel.StatusInProgress {
		t.Errorf("duel status=%s, want in_progress", d.Status)
	}
	if len(reg.tenants) != 1 || reg.tenants[0] != mmTenantX {
		t.Errorf("round sweeper not registered: %v", reg.tenants)
	}
	// Pending matches must be stored for both players.
	if h.consumePendingMatch(mmA) == "" || h.consumePendingMatch(mmB) == "" {
		t.Error("pending matches not stored for both searchers")
	}
	if len(queue.reverted) != 0 {
		t.Errorf("no revert expected on success, got %v", queue.reverted)
	}
}

func TestProcessMatch_ClassicCompensationBranches(t *testing.T) {
	t.Parallel()
	duelRepo := inmem.NewDuelRepo()
	queue := &mmQueueX{}

	// Duels nil → restore both.
	hNilDuels := NewHandler(Deps{MatchmakingQueue: queue, Rules: mmRulesX()})
	hNilDuels.processMatch(context.Background(), mmMatch(duel.NewUUIDv7(), "", ""))
	if len(queue.reverted) != 2 {
		t.Errorf("nil Duels: reverts=%v, want both searchers", queue.reverted)
	}

	// AtomSelector nil → restore both.
	hNoSel := NewHandler(Deps{Duels: duelRepo, MatchmakingQueue: queue, Rules: mmRulesX()})
	hNoSel.processMatch(context.Background(), mmMatch(duel.NewUUIDv7(), "", ""))
	if len(queue.reverted) != 4 {
		t.Errorf("nil selector: reverts=%v, want both searchers", queue.reverted)
	}

	// SelectAtoms error → restore.
	hSelErr := NewHandler(Deps{
		Duels: duelRepo, MatchmakingQueue: queue, Rules: mmRulesX(),
		AtomSelector: &stubbedAtomX{err: errors.New("agent down")},
	})
	hSelErr.processMatch(context.Background(), mmMatch(duel.NewUUIDv7(), "", ""))
	if len(queue.reverted) != 6 {
		t.Errorf("selector error: reverts=%v, want both searchers", queue.reverted)
	}

	// SelectAtoms returns zero picks → restore.
	hZero := NewHandler(Deps{
		Duels: duelRepo, MatchmakingQueue: queue, Rules: mmRulesX(),
		AtomSelector: &stubbedAtomX{picks: nil},
	})
	hZero.processMatch(context.Background(), mmMatch(duel.NewUUIDv7(), "", ""))
	if len(queue.reverted) != 8 {
		t.Errorf("zero picks: reverts=%v, want both searchers", queue.reverted)
	}

	// SaveDuel fault → restore.
	sel := &stubbedAtomX{picks: mmPicks(5)}
	faulty := &faultyDuelStore{DuelRepo: duelRepo, errSave: errors.New("pg down")}
	hSave := NewHandler(Deps{
		Duels: faulty, MatchmakingQueue: queue, Rules: mmRulesX(), AtomSelector: sel,
	})
	hSave.processMatch(context.Background(), mmMatch(duel.NewUUIDv7(), "", ""))
	if len(queue.reverted) != 10 {
		t.Errorf("save fault: reverts=%v, want both searchers", queue.reverted)
	}
}

func TestProcessMatch_BlitzPaths(t *testing.T) {
	t.Parallel()
	duelRepo := inmem.NewDuelRepo()
	queue := &mmQueueX{}
	reg := &recRegistrar{}

	// Timed variant success.
	hTimed := NewHandler(Deps{
		Duels: duelRepo, MatchmakingQueue: queue, Rules: mmRulesX(),
		AtomSelector: &stubbedAtomX{picks: mmPicks(5)}, RoundSweeperRegistrar: reg,
	})
	duelID := duel.NewUUIDv7()
	hTimed.processMatch(context.Background(), mmMatch(duelID, "blitz", "timed"))
	d, err := duelRepo.GetDuel(context.Background(), duelID)
	if err != nil || !d.IsBlitz() {
		t.Fatalf("blitz duel not created: %v", err)
	}

	// Race variant success.
	hRace := NewHandler(Deps{
		Duels: duelRepo, MatchmakingQueue: queue, Rules: mmRulesX(),
		AtomSelector: &stubbedAtomX{picks: mmPicks(5)}, RoundSweeperRegistrar: reg,
	})
	raceID := duel.NewUUIDv7()
	hRace.processMatch(context.Background(), mmMatch(raceID, "blitz", "race"))
	dRace, err := duelRepo.GetDuel(context.Background(), raceID)
	if err != nil || !dRace.IsBlitz() {
		t.Fatalf("race blitz duel not created: %v", err)
	}

	// Missing BlitzTimeLimitSec (rules zeroed) → NewBlitzPoolDuelWithID
	// fails → restore both (the "blitz variant missing" branch).
	zeroRules := mmRulesX()
	zeroRules.BlitzTimeLimitSec = 0
	zeroRules.BlitzRaceTarget = 0
	hBad := NewHandler(Deps{
		Duels: duelRepo, MatchmakingQueue: queue, Rules: zeroRules,
		AtomSelector: &stubbedAtomX{picks: mmPicks(5)},
	})
	before := len(queue.reverted)
	hBad.processMatch(context.Background(), mmMatch(duel.NewUUIDv7(), "blitz", "timed"))
	if len(queue.reverted) != before+2 {
		t.Errorf("blitz config fault: reverts went %d → %d, want +2", before, len(queue.reverted))
	}

	// startBlitzBattle errors: selector nil → restore.
	hNoSel := NewHandler(Deps{
		Duels: duelRepo, MatchmakingQueue: queue, Rules: mmRulesX(),
	})
	before = len(queue.reverted)
	hNoSel.processMatch(context.Background(), mmMatch(duel.NewUUIDv7(), "blitz", "timed"))
	if len(queue.reverted) != before+2 {
		t.Errorf("blitz nil selector: reverts went %d → %d, want +2", before, len(queue.reverted))
	}
}

func TestStartBlitzBattle_DirectBranches(t *testing.T) {
	t.Parallel()
	duelRepo := inmem.NewDuelRepo()
	queue := &mmQueueX{}

	// SelectAtoms error → error returned (the caller restores).
	h := NewHandler(Deps{Duels: duelRepo, MatchmakingQueue: queue, Rules: mmRulesX(),
		AtomSelector: &stubbedAtomX{err: errors.New("agent down")}})
	match := mmMatch(duel.NewUUIDv7(), "blitz", "timed")
	d, err := duel.NewBlitzPoolDuelWithID(duCfg(), mmA, mmB, mmTenantX,
		duel.ScopeRanked, 5, nil, duel.BlitzConfig{Variant: duel.BlitzVariantTimed, TimeLimitSec: 120}, match.MatchedDuelID)
	if err != nil {
		t.Fatalf("NewBlitzPoolDuelWithID: %v", err)
	}
	if err := h.startBlitzBattle(context.Background(), d, match); err == nil {
		t.Error("selector error must propagate")
	}

	// Zero picks → error.
	z := NewHandler(Deps{Duels: duelRepo, MatchmakingQueue: queue, Rules: mmRulesX(),
		AtomSelector: &stubbedAtomX{picks: nil}})
	if err := z.startBlitzBattle(context.Background(), d, match); err == nil {
		t.Error("zero picks must error")
	}

	// Success → duel transitions to in_progress.
	ok := NewHandler(Deps{Duels: duelRepo, MatchmakingQueue: queue, Rules: mmRulesX(),
		AtomSelector: &stubbedAtomX{picks: mmPicks(5)}})
	if err := ok.startBlitzBattle(context.Background(), d, match); err != nil {
		t.Fatalf("happy path: %v", err)
	}
	if d.Status != duel.StatusInProgress {
		t.Errorf("status=%s, want in_progress", d.Status)
	}
}

func TestRestoreSearchers_RevertFailureLoggedButContinues(t *testing.T) {
	t.Parallel()
	queue := &mmQueueX{revertErr: errors.New("pg down")}
	reg := &recRegistrar{}
	h := NewHandler(Deps{MatchmakingQueue: queue, Matchmaker: &stubMatchmaker{}, Rules: mmRulesX()})

	h.restoreSearchers(mmMatch(duel.NewUUIDv7(), "", ""))
	// Both searchers attempted even though the first reverts failed.
	if len(queue.reverted) != 2 {
		t.Errorf("reverts=%v, want both attempted", queue.reverted)
	}
	if len(reg.tenants) != 0 { // Matchmaker registrar is separate
		t.Errorf("unexpected registrations %v", reg.tenants)
	}
}

// stubMatchmaker satisfies MatchmakerPort (no-op).
type stubMatchmaker struct{}

func (m *stubMatchmaker) Match() <-chan domainmm.MatchResult { return nil }
func (m *stubMatchmaker) RegisterTenant(string)              {}
func (m *stubMatchmaker) Stop()                              {}

// recRegistrar records registered tenants.
type recRegistrar struct {
	tenants []string
}

func (r *recRegistrar) RegisterTenant(tenantID string) {
	r.tenants = append(r.tenants, tenantID)
}

func TestStoreAndConsumePendingMatch_NilMapReinit(t *testing.T) {
	t.Parallel()
	h := NewHandler(Deps{})
	// Simulate a handler whose map was never initialised.
	h.pendingMatches.matches = nil
	h.storePendingMatch(mmA, "duel-1")
	if got := h.consumePendingMatch(mmA); got != "duel-1" {
		t.Errorf("consume = %q, want duel-1", got)
	}
	// Consuming again returns "" (deleted).
	if got := h.consumePendingMatch(mmA); got != "" {
		t.Errorf("second consume = %q, want empty", got)
	}
}

func TestDuelForMatch_Branches(t *testing.T) {
	t.Parallel()
	// Nil Duels or empty id → not found.
	hNil := NewHandler(Deps{})
	if _, ok := hNil.duelForMatch(context.Background(), ""); ok {
		t.Error("empty duel id with nil Duels must be not-found")
	}
	repo := inmem.NewDuelRepo()
	h := NewHandler(Deps{Duels: repo})
	if _, ok := h.duelForMatch(context.Background(), "missing"); ok {
		t.Error("unknown duel must be not-found")
	}
	d := seedingDuel(t, repo, 1)
	if _, ok := h.duelForMatch(context.Background(), d.ID); !ok {
		t.Error("existing duel must be found")
	}
}

func TestIsPlayableDuel_AllStatuses(t *testing.T) {
	t.Parallel()
	cases := map[duel.Status]bool{
		duel.StatusPending:     true,
		duel.StatusAccepted:    true,
		duel.StatusInProgress:  true,
		duel.StatusCompleted:   false,
		duel.StatusForfeited:   false,
		duel.StatusExpired:     false,
		"mystery":              false,
	}
	for status, want := range cases {
		if got := isPlayableDuel(status); got != want {
			t.Errorf("isPlayableDuel(%s) = %v, want %v", status, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// writeQueueState branches through the HTTP surface
// ---------------------------------------------------------------------------

func mmXRequest(gcid, method, path string) *http.Request {
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("gcid", gcid)
	req.Header.Set("X-Tenant-Id", mmTenantX)
	return req
}

// Consumed pending-match fast path: a match stored by processMatch is
// reported as matched via heartbeat even before any queue row exists.
func TestQueueState_PendingMatchFastPath(t *testing.T) {
	t.Parallel()
	duelRepo := inmem.NewDuelRepo()
	queue := &mmQueueX{}
	h := NewHandler(Deps{Duels: duelRepo, MatchmakingQueue: queue, Rules: mmRulesX(),
		AtomSelector: &stubbedAtomX{picks: mmPicks(5)}})

	duelID := duel.NewUUIDv7()
	h.processMatch(context.Background(), mmMatch(duelID, "", ""))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, mmXRequest(mmA, http.MethodGet, "/v1/duels/queue/status"))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"status":"matched"`) || !strings.Contains(rr.Body.String(), duelID) {
		t.Errorf("pending fast path body=%s", rr.Body.String())
	}
}

func TestQueueState_MatchedCorruptRow_500(t *testing.T) {
	t.Parallel()
	queue := &mmQueueX{getByGCID: func(_ string) (*domainmm.Searcher, error) {
		return &domainmm.Searcher{Status: domainmm.StatusMatched}, nil // no MatchedDuelID
	}}
	h := NewHandler(Deps{MatchmakingQueue: queue})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, mmXRequest(mmA, http.MethodGet, "/v1/duels/queue/status"))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d, want 500 (corrupt matched row)", rr.Code)
	}
}

func TestQueueState_MatchedTerminalDuel_404AndAbandon(t *testing.T) {
	t.Parallel()
	duelRepo := inmem.NewDuelRepo()
	queue := &mmQueueX{getByGCID: func(_ string) (*domainmm.Searcher, error) {
		return &domainmm.Searcher{Status: domainmm.StatusMatched, MatchedDuelID: "duel-dead"}, nil
	}}
	// A completed duel (terminal) — the queue row is stale.
	dead, _ := duel.NewDuel(duCfg(), mmA, mmB, mmTenantX, duel.ScopeRanked, 1, nil, time.Hour)
	dead.Status = duel.StatusCompleted
	_ = duelRepo.SaveDuel(context.Background(), dead)
	// The inmem repo keys by ID; store under the MatchedDuelID.
	dead.ID = "duel-dead"
	_ = duelRepo.SaveDuel(context.Background(), dead)

	h := NewHandler(Deps{Duels: duelRepo, MatchmakingQueue: queue})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, mmXRequest(mmA, http.MethodGet, "/v1/duels/queue/status"))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status=%d, want 404 (stale terminal match)", rr.Code)
	}
	if len(queue.statusCalls) != 1 || queue.statusCalls[0] != domainmm.StatusAbandoned {
		t.Errorf("queue row must be marked abandoned, got %v", queue.statusCalls)
	}
}

func TestQueueState_DefaultTerminalStatus_404(t *testing.T) {
	t.Parallel()
	queue := &mmQueueX{getByGCID: func(_ string) (*domainmm.Searcher, error) {
		return &domainmm.Searcher{Status: domainmm.StatusCancelled}, nil
	}}
	h := NewHandler(Deps{MatchmakingQueue: queue})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, mmXRequest(mmA, http.MethodGet, "/v1/duels/queue/status"))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status=%d, want 404", rr.Code)
	}
}

func TestQueueState_HeartbeatFault_500(t *testing.T) {
	t.Parallel()
	queue := &mmQueueX{
		heartbeatErr: errors.New("pg down"),
		getByGCID: func(_ string) (*domainmm.Searcher, error) {
			return &domainmm.Searcher{Status: domainmm.StatusFinding, ExpiresAt: time.Now().Add(time.Minute)}, nil
		},
	}
	h := NewHandler(Deps{MatchmakingQueue: queue})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, mmXRequest(mmA, http.MethodPost, "/v1/duels/queue/heartbeat"))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d, want 500", rr.Code)
	}
}

func TestQueueState_NilPort_501(t *testing.T) {
	t.Parallel()
	h := NewHandler(Deps{})
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/v1/duels/queue/heartbeat"},
		{http.MethodGet, "/v1/duels/queue/status"},
	} {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, mmXRequest(mmA, tc.method, tc.path))
		if rr.Code != http.StatusNotImplemented {
			t.Errorf("%s %s: status=%d, want 501", tc.method, tc.path, rr.Code)
		}
	}
}

// ---------------------------------------------------------------------------
// enterQueue / cancelQueue remaining branches
// ---------------------------------------------------------------------------

func TestEnterQueue_Branches(t *testing.T) {
	t.Parallel()
	// Missing MatchmakingQueue → 501 (matchmaker is wired).
	mm := &stubMatchmaker{}
	h501 := NewHandler(Deps{Matchmaker: mm, Rules: mmRulesX()})
	req := mmXRequest(mmA, http.MethodPost, "/v1/duels/queue")
	req.Header.Set("Idempotency-Key", "k")
	rr := httptest.NewRecorder()
	h501.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("nil queue: status=%d, want 501", rr.Code)
	}

	// Enqueue fault → 500.
	h500 := NewHandler(Deps{Matchmaker: mm, MatchmakingQueue: &mmQueueX{enqueueErr: errors.New("pg down")}, Rules: mmRulesX()})
	req = mmXRequest(mmA, http.MethodPost, "/v1/duels/queue")
	req.Header.Set("Idempotency-Key", "k")
	rr = httptest.NewRecorder()
	h500.ServeHTTP(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("enqueue fault: status=%d, want 500", rr.Code)
	}

	// Matchmaker nil → 501 (fail-loud — the queue alone cannot drive matching).
	h501mm := NewHandler(Deps{MatchmakingQueue: &mmQueueX{}, Rules: mmRulesX()})
	req = mmXRequest(mmA, http.MethodPost, "/v1/duels/queue")
	req.Header.Set("Idempotency-Key", "k")
	rr = httptest.NewRecorder()
	h501mm.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("no matchmaker: status=%d, want 501", rr.Code)
	}
}

func TestEnterQueue_ProficiencyAndTagsFromStores(t *testing.T) {
	t.Parallel()
	duelRepo := inmem.NewDuelRepo()
	profiles := inmem.NewProfilerRepo()
	p, err := profiler.NewProfile(mmA, mmTenantX, "bio", nil)
	if err != nil {
		t.Fatalf("NewProfile: %v", err)
	}
	p.SetTags([]profiler.InterestTag{{Category: profiler.CategoryProgramming, Tag: "inheritance"}})
	_ = profiles.SaveProfile(context.Background(), p)

	var gotSearcher domainmm.Searcher
	queue := &mmQueueX{}
	queue.enqueueErr = nil
	h := NewHandler(Deps{
		Matchmaker:       &stubMatchmaker{},
		MatchmakingQueue: &capQueue{onEnqueue: func(s domainmm.Searcher) { gotSearcher = s }},
		Duels:            duelRepo,
		Profiles:         profiles,
		Rules:            mmRulesX(),
	})
	_ = queue
	req := mmXRequest(mmA, http.MethodPost, "/v1/duels/queue")
	req.Header.Set("Idempotency-Key", "k")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	// Interest tags fall back to the profiler profile when the body omits them.
	if len(gotSearcher.InterestTags) != 1 || gotSearcher.InterestTags[0] != "inheritance" {
		t.Errorf("tags = %v, want profile-derived", gotSearcher.InterestTags)
	}
	// Proficiency comes from the duel rating (baseline w/o duels).
	if gotSearcher.Proficiency != 1200 {
		t.Errorf("proficiency = %d, want ELO baseline 1200", gotSearcher.Proficiency)
	}
}

// capQueue records the enqueued searcher.
type capQueue struct {
	onEnqueue func(domainmm.Searcher)
}

func (q *capQueue) Enqueue(_ context.Context, s domainmm.Searcher) error {
	if q.onEnqueue != nil {
		q.onEnqueue(s)
	}
	return nil
}
func (q *capQueue) UpdateStatus(_ context.Context, _, _ string, _ domainmm.QueueStatus, _ string) error {
	return nil
}
func (q *capQueue) UpdateHeartbeat(_ context.Context, _, _ string) error { return nil }
func (q *capQueue) GetByGCID(_ context.Context, _, _ string) (*domainmm.Searcher, error) {
	return nil, nil
}
func (q *capQueue) RevertToFinding(_ context.Context, _, _, _ string) error { return nil }

func TestCancelQueue_NilPort_501(t *testing.T) {
	t.Parallel()
	h := NewHandler(Deps{})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, mmXRequest(mmA, http.MethodDelete, "/v1/duels/queue"))
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("status=%d, want 501", rr.Code)
	}
}

func TestEnterQueue_ValidationBranchBlitzVariantOnClassicAndDuelsErr(t *testing.T) {
	t.Parallel()
	// Duels present but stats fail → entropy falls back to baseline (covered
	// by TestEnterQueue_ProficiencyAndTagsFromStores); here pin the
	// GetRatingStats error path directly.
	faulty := &faultyDuelStore{DuelRepo: inmem.NewDuelRepo(), errStats: errors.New("pg down")}
	var gotSearcher domainmm.Searcher
	h := NewHandler(Deps{
		Matchmaker: &stubMatchmaker{}, MatchmakingQueue: &capQueue{onEnqueue: func(s domainmm.Searcher) { gotSearcher = s }},
		Duels: faulty, Rules: mmRulesX(),
	})
	req := mmXRequest(mmA, http.MethodPost, "/v1/duels/queue")
	req.Header.Set("Idempotency-Key", "k")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status=%d, want 202 despite rating fault", rr.Code)
	}
	if gotSearcher.Proficiency != 1200 {
		t.Errorf("proficiency=%d, want baseline on stats fault", gotSearcher.Proficiency)
	}
}