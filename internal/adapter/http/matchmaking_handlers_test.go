// matchmaking_handlers_test.go — tests for the PG-backed matchmaking
// HTTP endpoints: POST /v1/duels/queue, DELETE /v1/duels/queue,
// POST /v1/duels/queue/heartbeat, GET /v1/duels/queue/status.
//
// Uses a fake MatchmakingQueueRepo + a real Matchmaker (with the same
// fake as QueueRepoPort) so we exercise the full HTTP path (identity
// middleware → handler → JSON) without a database.
package httpadapter_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	httpadapter "github.com/apollo-chora/chora-sharing/internal/adapter/http"
	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	mmadapter "github.com/apollo-chora/chora-sharing/internal/adapter/matchmaking"
	"github.com/apollo-chora/chora-sharing/internal/config"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
	domainmm "github.com/apollo-chora/chora-sharing/internal/domain/matchmaking"
)

const (
	mmTenant = "01970000-0000-7000-8000-0000000000a1"
	mmGCID1  = "01970000-0000-7000-9000-0000000000a1"
	mmGCID2  = "01970000-0000-7000-9000-0000000000a2"
)

func mmRules() config.SharingRules {
	return config.SharingRules{
		ELOBaseline:               1200,
		ELOKFactor:                32,
		ComboTiers:                []int{1, 2, 3, 5},
		MatchmakingTimeout:        10 * time.Minute,
		MatchmakingHeartbeatStale: 30 * time.Second,
		MatchmakingMatchTick:      100 * time.Millisecond,
		MatchmakingSweepTick:      100 * time.Millisecond,
	}
}

// fakeQueueRepo is a test double for both the handler's MatchmakingQueueRepo
// and the matchmaker's QueueRepoPort. It models the queue-row lifecycle
// (finding → matched → finding on revert) with DB-like semantics: claims
// remove rows from the finding pool, and GetByGCID returns the latest entry
// regardless of status.
type fakeQueueRepo struct {
	mu           sync.Mutex
	entries      map[string]*domainmm.Searcher // gcid → queue entry (any status)
	reverts      []string                      // gcids reverted to finding
	getErr       error                         // injected GetByGCID error
	statusErr    error                         // injected UpdateStatus error
	heartbeatErr error                         // injected UpdateHeartbeat error
}

func newFakeQueueRepo() *fakeQueueRepo {
	return &fakeQueueRepo{entries: make(map[string]*domainmm.Searcher)}
}

func (f *fakeQueueRepo) Enqueue(_ context.Context, s domainmm.Searcher) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s.Status = domainmm.StatusFinding
	s.MatchedDuelID = ""
	f.entries[s.GCID] = &s
	return nil
}

func (f *fakeQueueRepo) UpdateStatus(_ context.Context, _, gcid string, status domainmm.QueueStatus, duelID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.statusErr != nil {
		return f.statusErr
	}
	if e, ok := f.entries[gcid]; ok && e.Status == domainmm.StatusFinding {
		e.Status = status
		e.MatchedDuelID = duelID
	}
	return nil
}

func (f *fakeQueueRepo) UpdateHeartbeat(_ context.Context, _, gcid string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.heartbeatErr != nil {
		return f.heartbeatErr
	}
	if e, ok := f.entries[gcid]; ok && e.Status == domainmm.StatusFinding {
		e.LastHeartbeat = time.Now().UTC()
	}
	return nil
}

// GetByGCID returns the caller's queue entry regardless of status (nil when
// the user has no entry), mirroring the pg repo's latest-row semantics.
func (f *fakeQueueRepo) GetByGCID(_ context.Context, _, gcid string) (*domainmm.Searcher, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	if e, ok := f.entries[gcid]; ok {
		cp := *e
		return &cp, nil
	}
	return nil, nil
}

func (f *fakeQueueRepo) RevertToFinding(_ context.Context, _, gcid, duelID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.entries[gcid]
	if !ok || e.Status != domainmm.StatusMatched || e.MatchedDuelID != duelID {
		return errors.New("row not in claimed state")
	}
	e.Status = domainmm.StatusFinding
	e.MatchedDuelID = ""
	f.reverts = append(f.reverts, gcid)
	return nil
}

func (f *fakeQueueRepo) FindCandidates(_ context.Context, tenantID string) ([]domainmm.Searcher, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []domainmm.Searcher
	for _, e := range f.entries {
		if e.TenantID == tenantID && e.Status == domainmm.StatusFinding {
			out = append(out, *e)
		}
	}
	return out, nil
}

func (f *fakeQueueRepo) ClaimMatch(_ context.Context, _, gcidA, gcidB, duelID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	a, okA := f.entries[gcidA]
	b, okB := f.entries[gcidB]
	if !okA || !okB || a.Status != domainmm.StatusFinding || b.Status != domainmm.StatusFinding {
		return errors.New("rows no longer finding")
	}
	a.Status = domainmm.StatusMatched
	a.MatchedDuelID = duelID
	b.Status = domainmm.StatusMatched
	b.MatchedDuelID = duelID
	return nil
}

func (f *fakeQueueRepo) ExpireStaleForTenant(_ context.Context, _ string) error { return nil }

// setMatched places an entry directly in the claimed state — simulating a
// claim performed by ANOTHER pod (multi-pod pickup path).
func (f *fakeQueueRepo) setMatched(gcid, duelID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e, ok := f.entries[gcid]; ok {
		e.Status = domainmm.StatusMatched
		e.MatchedDuelID = duelID
	}
}

func (f *fakeQueueRepo) revertCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reverts)
}

// stubAtomSelector returns fixed atoms for test duels.
type stubAtomSelector struct{}

func (s *stubAtomSelector) SelectAtoms(_ context.Context, _ httpadapter.AtomSelectionRequest) ([]duel.AtomPick, error) {
	count := 5
	picks := make([]duel.AtomPick, count)
	for i := range picks {
		picks[i] = duel.AtomPick{
			AtomID:   "atom-" + string(rune('a'+i)),
			Question: "Q" + string(rune('1'+i)),
			Options:  []string{"A", "B"},
			Answer:   "A",
		}
	}
	return picks, nil
}

func (s *stubAtomSelector) GetAtomForRound(_ context.Context, d *duel.Duel, roundNo int) (duel.AtomPick, error) {
	if d == nil || roundNo < 1 || roundNo > len(d.Rounds) {
		return duel.AtomPick{}, duel.ErrRoundOutOfRange
	}
	return duel.AtomPick{AtomID: d.Rounds[roundNo-1].AtomID, RevisionID: d.Rounds[roundNo-1].AtomRevisionID}, nil
}

// errAtomSelector fails SelectAtoms — drives the processMatch compensation
// path (searchers must be restored to finding).
type errAtomSelector struct{ stubAtomSelector }

func (s *errAtomSelector) SelectAtoms(_ context.Context, _ httpadapter.AtomSelectionRequest) ([]duel.AtomPick, error) {
	return nil, errors.New("atom projection unavailable")
}

func mmHandler(repo *fakeQueueRepo) (http.Handler, *inmem.DuelRepo, *mmadapter.Matchmaker) {
	return mmHandlerWithSelector(repo, &stubAtomSelector{})
}

func mmHandlerWithSelector(repo *fakeQueueRepo, sel httpadapter.AtomSelectorPort) (http.Handler, *inmem.DuelRepo, *mmadapter.Matchmaker) {
	mm := mmadapter.NewMatchmaker(mmadapter.Config{
		Timeout:           10 * time.Minute,
		HeartbeatStale:    30 * time.Second,
		MatchTickInterval: 100 * time.Millisecond,
		SweepTickInterval: 100 * time.Millisecond,
	}, repo)
	mm.RegisterTenant(mmTenant)
	duelRepo := inmem.NewDuelRepo()
	h := httpadapter.NewHandler(httpadapter.Deps{
		Duels:            duelRepo,
		Matchmaker:       mm,
		MatchmakingQueue: repo,
		AtomSelector:     sel,
		Rules:            mmRules(),
	})
	return h, duelRepo, mm
}

func mmRequest(method, path, gcid, body string) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("gcid", gcid)
	req.Header.Set("X-Tenant-Id", mmTenant)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	return req
}

func decodeMMBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode body %q: %v", rec.Body.String(), err)
	}
	return out
}

func enterQueue(t *testing.T, h http.Handler, gcid, idemKey string) {
	t.Helper()
	req := mmRequest(http.MethodPost, "/v1/duels/queue", gcid, `{"interest_tags":["inheritance"]}`)
	req.Header.Set("Idempotency-Key", idemKey)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("enter %s: status=%d body=%s", gcid, rec.Code, rec.Body.String())
	}
}

// seedPoolDuel creates + persists a pool duel with the given ID, mimicking
// what processMatch does after a claim.
func seedPoolDuel(t *testing.T, duelRepo *inmem.DuelRepo, duelID string) {
	t.Helper()
	cfg := duel.DuelConfig{
		ELOBaseline:   1200,
		ELOKFactor:    32,
		ComboTiers:    []int{1, 2, 3, 5},
		RoundTimerSec: 30,
	}
	d, err := duel.NewPoolDuelWithID(cfg, mmGCID1, mmGCID2, mmTenant,
		duel.ScopeRanked, 5, []string{"inheritance"}, duelID)
	if err != nil {
		t.Fatalf("NewPoolDuelWithID: %v", err)
	}
	picks, err := (&stubAtomSelector{}).SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{Count: d.RoundCount})
	if err != nil {
		t.Fatalf("SelectAtoms: %v", err)
	}
	if err := d.StartBattle(picks); err != nil {
		t.Fatalf("StartBattle: %v", err)
	}
	if err := duelRepo.SaveDuel(context.Background(), d); err != nil {
		t.Fatalf("SaveDuel: %v", err)
	}
}

func TestQueue_Enter(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	req := mmRequest(http.MethodPost, "/v1/duels/queue", mmGCID1, `{"interest_tags":["inheritance"]}`)
	req.Header.Set("Idempotency-Key", "queue-1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := decodeMMBody(t, rec)
	if body["status"] != "finding" {
		t.Errorf("status=%v want finding", body["status"])
	}
	if _, ok := body["expires_at"]; !ok {
		t.Error("missing expires_at")
	}
}

func TestQueue_EnterNoIdempotencyKey(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	req := mmRequest(http.MethodPost, "/v1/duels/queue", mmGCID1, `{}`)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want 400", rec.Code)
	}
}

func TestQueue_Cancel(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	enterQueue(t, h, mmGCID1, "queue-1")

	reqCancel := mmRequest(http.MethodDelete, "/v1/duels/queue", mmGCID1, "")
	recCancel := httptest.NewRecorder()
	h.ServeHTTP(recCancel, reqCancel)

	if recCancel.Code != http.StatusOK {
		t.Fatalf("cancel status=%d body=%s", recCancel.Code, recCancel.Body.String())
	}
	body := decodeMMBody(t, recCancel)
	if body["status"] != "cancelled" {
		t.Errorf("status=%v want cancelled", body["status"])
	}
}

// TestQueue_CancelRepoError pins fail-loud: a persistence failure surfaces
// as 500, not a fake 200 "cancelled".
func TestQueue_CancelRepoError(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	enterQueue(t, h, mmGCID1, "queue-1")
	repo.statusErr = errors.New("db down")

	req := mmRequest(http.MethodDelete, "/v1/duels/queue", mmGCID1, "")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want 500", rec.Code)
	}
}

func TestQueue_Heartbeat(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	enterQueue(t, h, mmGCID1, "queue-1")

	reqHB := mmRequest(http.MethodPost, "/v1/duels/queue/heartbeat", mmGCID1, "")
	recHB := httptest.NewRecorder()
	h.ServeHTTP(recHB, reqHB)

	if recHB.Code != http.StatusOK {
		t.Fatalf("heartbeat status=%d body=%s", recHB.Code, recHB.Body.String())
	}
	body := decodeMMBody(t, recHB)
	if body["status"] != "finding" {
		t.Errorf("status=%v want finding", body["status"])
	}
}

// TestQueue_HeartbeatIncludesExpiresAt pins WS0 Bug 1: the heartbeat
// response MUST carry expires_at so the FE can render a matchmaking
// countdown. The queue row already stores ExpiresAt (set at enqueue time
// as now + MatchmakingTimeout); writeQueueState must surface it on the
// finding branch, not just enterQueue. A `now` field (server time) is
// included so the FE can compute clock-skew-safe countdowns.
func TestQueue_HeartbeatIncludesExpiresAt(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	enterQueue(t, h, mmGCID1, "queue-1")

	reqHB := mmRequest(http.MethodPost, "/v1/duels/queue/heartbeat", mmGCID1, "")
	recHB := httptest.NewRecorder()
	h.ServeHTTP(recHB, reqHB)
	if recHB.Code != http.StatusOK {
		t.Fatalf("heartbeat status=%d body=%s", recHB.Code, recHB.Body.String())
	}
	body := decodeMMBody(t, recHB)
	exp, ok := body["expires_at"].(string)
	if !ok || exp == "" {
		t.Fatalf("heartbeat finding response missing expires_at: %v", body)
	}
	ts, err := time.Parse(time.RFC3339, exp)
	if err != nil {
		t.Fatalf("expires_at %q not RFC3339: %v", exp, err)
	}
	if !ts.After(time.Now().Add(time.Minute)) {
		t.Errorf("expires_at %v should be >1min in the future", ts)
	}
	nowStr, ok := body["now"].(string)
	if !ok || nowStr == "" {
		t.Fatalf("heartbeat finding response missing now: %v", body)
	}
	if _, err := time.Parse(time.RFC3339, nowStr); err != nil {
		t.Fatalf("now %q not RFC3339: %v", nowStr, err)
	}
}

// TestQueue_StatusIncludesExpiresAt pins WS0 Bug 1 for the status polling
// fallback endpoint — same requirement as heartbeat (expires_at + now).
func TestQueue_StatusIncludesExpiresAt(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	enterQueue(t, h, mmGCID1, "queue-1")

	req := mmRequest(http.MethodGet, "/v1/duels/queue/status", mmGCID1, "")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := decodeMMBody(t, rec)
	exp, ok := body["expires_at"].(string)
	if !ok || exp == "" {
		t.Fatalf("status finding response missing expires_at: %v", body)
	}
	if _, err := time.Parse(time.RFC3339, exp); err != nil {
		t.Fatalf("expires_at %q not RFC3339: %v", exp, err)
	}
	if _, ok := body["now"].(string); !ok {
		t.Fatalf("status finding response missing now: %v", body)
	}
}

// TestQueue_HeartbeatRepoError pins fail-loud: a GetByGCID failure surfaces
// as 500, not a misleading 404 "not in queue".
func TestQueue_HeartbeatRepoError(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	enterQueue(t, h, mmGCID1, "queue-1")
	repo.getErr = errors.New("db down")

	req := mmRequest(http.MethodPost, "/v1/duels/queue/heartbeat", mmGCID1, "")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want 500", rec.Code)
	}
}

func TestQueue_HeartbeatNotInQueue(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	req := mmRequest(http.MethodPost, "/v1/duels/queue/heartbeat", mmGCID1, "")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d want 404 (not in queue)", rec.Code)
	}
}

func TestQueue_Status(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	enterQueue(t, h, mmGCID1, "queue-1")

	reqStatus := mmRequest(http.MethodGet, "/v1/duels/queue/status", mmGCID1, "")
	recStatus := httptest.NewRecorder()
	h.ServeHTTP(recStatus, reqStatus)

	if recStatus.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recStatus.Code, recStatus.Body.String())
	}
	body := decodeMMBody(t, recStatus)
	if body["status"] != "finding" {
		t.Errorf("status=%v want finding", body["status"])
	}
}

func TestQueue_StatusNotInQueue(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	req := mmRequest(http.MethodGet, "/v1/duels/queue/status", mmGCID1, "")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d want 404", rec.Code)
	}
}

func TestQueue_MatchFound(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	enterQueue(t, h, mmGCID1, "queue-1")
	enterQueue(t, h, mmGCID2, "queue-2")

	// Wait for match to be processed.
	time.Sleep(500 * time.Millisecond)

	// User 1 heartbeats — should see matched status.
	reqHB := mmRequest(http.MethodPost, "/v1/duels/queue/heartbeat", mmGCID1, "")
	recHB := httptest.NewRecorder()
	h.ServeHTTP(recHB, reqHB)

	if recHB.Code != http.StatusOK {
		t.Fatalf("heartbeat status=%d body=%s", recHB.Code, recHB.Body.String())
	}
	body := decodeMMBody(t, recHB)
	if body["status"] != "matched" {
		t.Errorf("status=%v want matched", body["status"])
	}
	if _, ok := body["duel_id"]; !ok {
		t.Error("missing duel_id in matched response")
	}
}

// TestQueue_HeartbeatMatchedViaDBLookup pins the multi-pod pickup path:
// the claim + duel were made by ANOTHER pod, so this pod's pending-match
// map is empty — the queue row's matched_duel_id is the durable signal.
func TestQueue_HeartbeatMatchedViaDBLookup(t *testing.T) {
	repo := newFakeQueueRepo()
	h, duelRepo, mm := mmHandler(repo)
	defer mm.Stop()

	enterQueue(t, h, mmGCID1, "queue-1")

	// Another pod claims the row and creates the duel.
	duelID := duel.NewUUIDv7()
	repo.setMatched(mmGCID1, duelID)
	seedPoolDuel(t, duelRepo, duelID)

	req := mmRequest(http.MethodPost, "/v1/duels/queue/heartbeat", mmGCID1, "")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("heartbeat status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := decodeMMBody(t, rec)
	if body["status"] != "matched" {
		t.Errorf("status=%v want matched", body["status"])
	}
	if body["duel_id"] != duelID {
		t.Errorf("duel_id=%v want %s", body["duel_id"], duelID)
	}
}

// TestQueue_StatusMatchedViaDBLookup is the same multi-pod path through
// the polling-fallback endpoint.
func TestQueue_StatusMatchedViaDBLookup(t *testing.T) {
	repo := newFakeQueueRepo()
	h, duelRepo, mm := mmHandler(repo)
	defer mm.Stop()

	enterQueue(t, h, mmGCID1, "queue-1")

	duelID := duel.NewUUIDv7()
	repo.setMatched(mmGCID1, duelID)
	seedPoolDuel(t, duelRepo, duelID)

	req := mmRequest(http.MethodGet, "/v1/duels/queue/status", mmGCID1, "")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := decodeMMBody(t, rec)
	if body["status"] != "matched" {
		t.Errorf("status=%v want matched", body["status"])
	}
	if body["duel_id"] != duelID {
		t.Errorf("duel_id=%v want %s", body["duel_id"], duelID)
	}
}

// TestQueue_HeartbeatMatchedClaimInFlight pins the claim→SaveDuel window:
// the row is claimed but the duel row is not visible yet, so the endpoint
// must report finding (keep polling) rather than a dangling duel ID.
func TestQueue_HeartbeatMatchedClaimInFlight(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	enterQueue(t, h, mmGCID1, "queue-1")

	// Claim landed, but no duel row exists (creation mid-flight or failed).
	repo.setMatched(mmGCID1, duel.NewUUIDv7())

	req := mmRequest(http.MethodPost, "/v1/duels/queue/heartbeat", mmGCID1, "")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("heartbeat status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := decodeMMBody(t, rec)
	if body["status"] != "finding" {
		t.Errorf("status=%v want finding (duel not persisted yet)", body["status"])
	}
	if _, ok := body["duel_id"]; ok {
		t.Error("must not leak a duel_id whose duel row does not exist")
	}
}

// TestQueue_ProcessMatchFailureRestoresFinding pins the §2.3 compensating
// action: when duel creation fails, both claimed rows revert to finding
// instead of stranding in 'matched'.
func TestQueue_ProcessMatchFailureRestoresFinding(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandlerWithSelector(repo, &errAtomSelector{})
	defer mm.Stop()

	enterQueue(t, h, mmGCID1, "queue-1")
	enterQueue(t, h, mmGCID2, "queue-2")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && repo.revertCount() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if repo.revertCount() < 2 {
		t.Fatalf("expected both searchers restored to finding; reverts=%v", repo.reverts)
	}
}

// Ensure the domainmm import is used (for type reference in future tests).
var _ = domainmm.StatusFinding

// --- WS4: configurable question count (handler validation) ---

// TestQueue_EnterWithQuestionCount pins WS4: the queue request accepts a
// question_count field + persists it on the Searcher so the matchmaker can
// pair only matching counts. Default is 5 when omitted.
func TestQueue_EnterWithQuestionCount(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	req := mmRequest(http.MethodPost, "/v1/duels/queue", mmGCID1, `{"interest_tags":["inheritance"],"question_count":15}`)
	req.Header.Set("Idempotency-Key", "queue-qc-1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}

	// The searcher in the repo must carry question_count=15.
	s, err := repo.GetByGCID(context.Background(), mmTenant, mmGCID1)
	if err != nil || s == nil {
		t.Fatalf("GetByGCID: err=%v s=%v", err, s)
	}
	if s.QuestionCount != 15 {
		t.Errorf("QuestionCount=%d want 15", s.QuestionCount)
	}
}

// TestQueue_EnterDefaultsQuestionCountTo5 pins WS4: omitting question_count
// defaults to 5 (the Quick preset), preserving backward compatibility.
func TestQueue_EnterDefaultsQuestionCountTo5(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	req := mmRequest(http.MethodPost, "/v1/duels/queue", mmGCID1, `{"interest_tags":["inheritance"]}`)
	req.Header.Set("Idempotency-Key", "queue-qc-default")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}

	s, err := repo.GetByGCID(context.Background(), mmTenant, mmGCID1)
	if err != nil || s == nil {
		t.Fatalf("GetByGCID: err=%v s=%v", err, s)
	}
	if s.QuestionCount != 5 {
		t.Errorf("QuestionCount=%d want 5 (default)", s.QuestionCount)
	}
}

// TestQueue_EnterRejectsInvalidQuestionCount pins WS4 fail-loud: an
// out-of-bounds question_count (e.g. 7, not a preset) is rejected with
// 400. Accepted presets are 5 (Quick), 10 (Standard), 15 (Marathon).
// Zero is valid (means "default" → 5).
func TestQueue_EnterRejectsInvalidQuestionCount(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	for _, bad := range []int{4, 7, 16, 20, -1} {
		body := fmt.Sprintf(`{"question_count":%d}`, bad)
		req := mmRequest(http.MethodPost, "/v1/duels/queue", mmGCID1, body)
		req.Header.Set("Idempotency-Key", fmt.Sprintf("queue-qc-bad-%d", bad))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("question_count=%d: status=%d want 400 (not a valid preset: 5/10/15)", bad, rec.Code)
		}
	}
}

// --- WS1: per-category matchmaking (handler validation) ---

// TestQueue_EnterWithCategory pins WS1: the queue request accepts a
// category field + persists it on the Searcher so the matchmaker can
// pair only same-category entries.
func TestQueue_EnterWithCategory(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	req := mmRequest(http.MethodPost, "/v1/duels/queue", mmGCID1, `{"interest_tags":["calculus"],"category":"mathematics"}`)
	req.Header.Set("Idempotency-Key", "queue-cat-1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}

	s, err := repo.GetByGCID(context.Background(), mmTenant, mmGCID1)
	if err != nil || s == nil {
		t.Fatalf("GetByGCID: err=%v s=%v", err, s)
	}
	if s.Category != "mathematics" {
		t.Errorf("Category=%q want mathematics", s.Category)
	}
}

// TestQueue_EnterRejectsInvalidCategory pins WS1 fail-loud: a category
// not in the 6-category profiler taxonomy is rejected with 400.
func TestQueue_EnterRejectsInvalidCategory(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	for _, bad := range []string{"cooking", "gaming", "sports"} {
		body := fmt.Sprintf(`{"category":%q}`, bad)
		req := mmRequest(http.MethodPost, "/v1/duels/queue", mmGCID1, body)
		req.Header.Set("Idempotency-Key", fmt.Sprintf("queue-cat-bad-%s", bad))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("category=%q: status=%d want 400 (not a valid category)", bad, rec.Code)
		}
	}
}

// TestQueue_EnterDefaultsCategoryToOverall pins WS1: omitting category
// defaults to "overall" so existing un-categorized duels continue to work.
func TestQueue_EnterDefaultsCategoryToOverall(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	req := mmRequest(http.MethodPost, "/v1/duels/queue", mmGCID1, `{"interest_tags":["inheritance"]}`)
	req.Header.Set("Idempotency-Key", "queue-cat-default")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}

	s, err := repo.GetByGCID(context.Background(), mmTenant, mmGCID1)
	if err != nil || s == nil {
		t.Fatalf("GetByGCID: err=%v s=%v", err, s)
	}
	if s.Category != "overall" {
		t.Errorf("Category=%q want overall (default)", s.Category)
	}
}

// --- Blitz mode tests ---

func TestQueue_EnterWithBlitzMode(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	req := mmRequest(http.MethodPost, "/v1/duels/queue", mmGCID1, `{"mode":"blitz","blitz_variant":"timed"}`)
	req.Header.Set("Idempotency-Key", "queue-blitz-1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}

	s, err := repo.GetByGCID(context.Background(), mmTenant, mmGCID1)
	if err != nil || s == nil {
		t.Fatalf("GetByGCID: err=%v s=%v", err, s)
	}
	if s.Mode != "blitz" {
		t.Errorf("Mode=%q want blitz", s.Mode)
	}
	if s.BlitzVariant != "timed" {
		t.Errorf("BlitzVariant=%q want timed", s.BlitzVariant)
	}
}

func TestQueue_EnterBlitzDefaultsVariantToTimed(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	req := mmRequest(http.MethodPost, "/v1/duels/queue", mmGCID1, `{"mode":"blitz"}`)
	req.Header.Set("Idempotency-Key", "queue-blitz-default-variant")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}

	s, err := repo.GetByGCID(context.Background(), mmTenant, mmGCID1)
	if err != nil || s == nil {
		t.Fatalf("GetByGCID: err=%v s=%v", err, s)
	}
	if s.Mode != "blitz" {
		t.Errorf("Mode=%q want blitz", s.Mode)
	}
	if s.BlitzVariant != "timed" {
		t.Errorf("BlitzVariant=%q want timed (default)", s.BlitzVariant)
	}
}

func TestQueue_EnterDefaultsModeToClassic(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	req := mmRequest(http.MethodPost, "/v1/duels/queue", mmGCID1, `{"interest_tags":["inheritance"]}`)
	req.Header.Set("Idempotency-Key", "queue-mode-default")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}

	s, err := repo.GetByGCID(context.Background(), mmTenant, mmGCID1)
	if err != nil || s == nil {
		t.Fatalf("GetByGCID: err=%v s=%v", err, s)
	}
	if s.Mode != "classic" {
		t.Errorf("Mode=%q want classic (default)", s.Mode)
	}
}

func TestQueue_EnterRejectsInvalidMode(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	for _, bad := range []string{"turbo", "sudden_death", "lightning"} {
		body := fmt.Sprintf(`{"mode":%q}`, bad)
		req := mmRequest(http.MethodPost, "/v1/duels/queue", mmGCID1, body)
		req.Header.Set("Idempotency-Key", fmt.Sprintf("queue-mode-bad-%s", bad))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("mode=%q: status=%d want 400 (not a valid mode)", bad, rec.Code)
		}
	}
}

func TestQueue_EnterRejectsInvalidBlitzVariant(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	req := mmRequest(http.MethodPost, "/v1/duels/queue", mmGCID1, `{"mode":"blitz","blitz_variant":"sudden_death"}`)
	req.Header.Set("Idempotency-Key", "queue-blitz-bad-variant")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status=%d want 400 (invalid blitz_variant)", rec.Code)
	}
}

func TestQueue_EnterRejectsBlitzVariantOnClassicMode(t *testing.T) {
	repo := newFakeQueueRepo()
	h, _, mm := mmHandler(repo)
	defer mm.Stop()

	req := mmRequest(http.MethodPost, "/v1/duels/queue", mmGCID1, `{"mode":"classic","blitz_variant":"timed"}`)
	req.Header.Set("Idempotency-Key", "queue-blitz-variant-on-classic")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status=%d want 400 (blitz_variant only valid when mode=blitz)", rec.Code)
	}
}
