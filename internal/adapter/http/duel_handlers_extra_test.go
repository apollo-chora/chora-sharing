// duel_handlers_extra_test.go — the duel gameplay surface not covered by
// duel_rating_handlers_test.go:
//
//	POST /v1/duels/{duel_id}/answer   — classic + blitz answer flows
//	GET  /v1/duels                    — list with filters/cursor
//	GET  /v1/duels/{duel_id}          — single duel (+ tenant isolation)
//	GET  /v1/duels/{duel_id}/ws       — WS delegation / 501
//	GET  /v1/duels/my-rating          — error + course-bonus branches
//	GET  /v1/duels/leaderboard        — category + error branches
package httpadapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/config"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
	"github.com/apollo-chora/chora-sharing/internal/domain/profiler"
)

const (
	duTenant       = "01970000-0000-7000-8000-0000000000f1"
	duChallenger   = "01970000-0000-7000-9000-0000000000f1"
	duOpponent     = "01970000-0000-7000-9000-0000000000f2"
)

func duRules() config.SharingRules {
	return config.SharingRules{
		ELOBaseline:   1200,
		ELOKFactor:    32,
		ComboTiers:    []int{1, 2, 3, 5},
		RoundTimerSec: 30,
	}
}

func duCfg() duel.DuelConfig {
	return duel.DuelConfig{
		ELOBaseline:   1200,
		ELOKFactor:    32,
		ComboTiers:    []int{1, 2, 3, 5},
		RoundTimerSec: 30,
	}
}

func duRequest(gcid, method, path, body string) *http.Request {
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	r.Header.Set("gcid", gcid)
	r.Header.Set("X-Tenant-Id", duTenant)
	return r
}

// answerPicks builds n rounds with embedded content (CorrectAnswer "A").
func answerPicks(n int) []duel.AtomPick {
	out := make([]duel.AtomPick, n)
	for i := range out {
		out[i] = duel.AtomPick{
			AtomID:   "atom-" + string(rune('a'+i)),
			Question: "Q" + string(rune('1'+i)),
			Options:  []string{"A", "B"},
			Answer:   "A",
		}
	}
	return out
}

// seedingDuel creates + persists a classic ranked duel in the repo, returning it.
func seedingDuel(t *testing.T, repo *inmem.DuelRepo, rounds int) *duel.Duel {
	t.Helper()
	d, err := duel.NewDuel(duCfg(), duChallenger, duOpponent, duTenant,
		duel.ScopeRanked, rounds, nil, 24*time.Hour)
	if err != nil {
		t.Fatalf("NewDuel: %v", err)
	}
	if err := d.Accept(duOpponent); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if err := d.StartBattle(answerPicks(rounds)); err != nil {
		t.Fatalf("StartBattle: %v", err)
	}
	if err := repo.SaveDuel(context.Background(), d); err != nil {
		t.Fatalf("SaveDuel: %v", err)
	}
	return d
}

// recPublisher records PublishDuelCompleted calls.
type recPublisher struct {
	calls int
}

func (p *recPublisher) PublishDuelCompleted(_ context.Context, _, _, _, _, _, _ string, _, _ int) error {
	p.calls++
	return nil
}

// stubAtomSelectorDuel implements AtomSelectorPort for the legacy-round
// fallback (rounds without embedded content).
type stubAtomSelectorDuel struct{}

func (s *stubAtomSelectorDuel) SelectAtoms(_ context.Context, _ AtomSelectionRequest) ([]duel.AtomPick, error) {
	return nil, nil
}
func (s *stubAtomSelectorDuel) GetAtomForRound(_ context.Context, d *duel.Duel, roundNo int) (duel.AtomPick, error) {
	if d == nil || roundNo < 1 || roundNo > len(d.Rounds) {
		return duel.AtomPick{}, duel.ErrRoundOutOfRange
	}
	return duel.AtomPick{Answer: "B"}, nil
}

// faultyDuelStore wraps an inmem.DuelRepo with selective failures.
type faultyDuelStore struct {
	*inmem.DuelRepo
	errResolve error
	errSave    error
	errList    error
	errStats   error
	errTop     error
	errTopCat  error
}

func (f *faultyDuelStore) ResolveRound(ctx context.Context, d *duel.Duel, r int, g string, res duel.RoundResolution) error {
	if f.errResolve != nil {
		return f.errResolve
	}
	return f.DuelRepo.ResolveRound(ctx, d, r, g, res)
}
func (f *faultyDuelStore) SaveDuel(ctx context.Context, d *duel.Duel) error {
	if f.errSave != nil {
		return f.errSave
	}
	return f.DuelRepo.SaveDuel(ctx, d)
}
func (f *faultyDuelStore) ListDuels(ctx context.Context, t, g, s string, l int, c string) ([]*duel.Duel, string, error) {
	if f.errList != nil {
		return nil, "", f.errList
	}
	return f.DuelRepo.ListDuels(ctx, t, g, s, l, c)
}
func (f *faultyDuelStore) GetRatingStats(ctx context.Context, t, g string) (duel.RatingStats, error) {
	if f.errStats != nil {
		return duel.RatingStats{}, f.errStats
	}
	return f.DuelRepo.GetRatingStats(ctx, t, g)
}
func (f *faultyDuelStore) TopRatings(ctx context.Context, t string, l int) ([]duel.RatingStats, error) {
	if f.errTop != nil {
		return nil, f.errTop
	}
	return f.DuelRepo.TopRatings(ctx, t, l)
}
func (f *faultyDuelStore) TopRatingsForCategory(ctx context.Context, t, c string, l int) ([]duel.RatingStats, error) {
	if f.errTopCat != nil {
		return nil, f.errTopCat
	}
	return f.DuelRepo.TopRatingsForCategory(ctx, t, c, l)
}

// ---------------------------------------------------------------------------
// POST /v1/duels/{duel_id}/answer — classic
// ---------------------------------------------------------------------------

func TestSubmitDuelAnswer_ClassicCompletesWithELOAndEvent(t *testing.T) {
	t.Parallel()
	repo := &faultyDuelStore{DuelRepo: inmem.NewDuelRepo()}
	d := seedingDuel(t, repo.DuelRepo, 1)
	pub := &recPublisher{}
	h := NewHandler(Deps{Duels: repo, Rules: duRules(), DuelEvents: pub})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, duRequest(duChallenger, http.MethodPost, "/v1/duels/"+d.ID+"/answer",
		`{"round_no":1,"answer":"A","answer_time_ms":5000}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s, want 200", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"duel_status":"completed"`) {
		t.Errorf("expected completed: %s", rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"correct":true`) {
		t.Errorf("expected correct=true: %s", rr.Body.String())
	}
	if pub.calls != 1 {
		t.Errorf("DuelEvents.PublishDuelCompleted calls=%d, want 1", pub.calls)
	}
	// ELO must have been applied: winner above baseline.
	stats, _ := repo.GetRatingStats(context.Background(), duTenant, duChallenger)
	if stats.Rating <= 1200 {
		t.Errorf("challenger rating=%d, want > 1200 after a win", stats.Rating)
	}
}

func TestSubmitDuelAnswer_TwoRoundMidFlow(t *testing.T) {
	t.Parallel()
	repo := inmem.NewDuelRepo()
	d := seedingDuel(t, repo, 2)
	h := NewHandler(Deps{Duels: repo, Rules: duRules()})

	// Round 1 correct → duel still in_progress.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, duRequest(duChallenger, http.MethodPost, "/v1/duels/"+d.ID+"/answer",
		`{"round_no":1,"answer":"A","answer_time_ms":3000}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"duel_status":"in_progress"`) {
		t.Errorf("expected in_progress: %s", rr.Body.String())
	}

	// Same round again → 409 round already resolved.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, duRequest(duChallenger, http.MethodPost, "/v1/duels/"+d.ID+"/answer",
		`{"round_no":1,"answer":"A","answer_time_ms":3000}`))
	if rr2.Code != http.StatusConflict {
		t.Errorf("repeat answer: status=%d, want 409", rr2.Code)
	}
	if !strings.Contains(rr2.Body.String(), "already resolved") {
		t.Errorf("repeat answer body=%s, want 'round already resolved'", rr2.Body.String())
	}
}

func TestSubmitDuelAnswer_LegacyFallbackAndOutOfRange(t *testing.T) {
	t.Parallel()
	repo := inmem.NewDuelRepo()
	// A duel whose rounds have NO embedded content (legacy snapshot).
	d, err := duel.NewDuel(duCfg(), duChallenger, duOpponent, duTenant,
		duel.ScopeRanked, 1, nil, 24*time.Hour)
	if err != nil {
		t.Fatalf("NewDuel: %v", err)
	}
	_ = d.Accept(duOpponent)
	if err := d.StartBattle([]duel.AtomPick{{AtomID: "atom-x", Question: "Q", Options: []string{"A", "B"}}}); err != nil {
		t.Fatalf("StartBattle: %v", err)
	}
	_ = repo.SaveDuel(context.Background(), d)

	// AtomSelector resolves the answer (returns "B").
	h := NewHandler(Deps{Duels: repo, Rules: duRules(), AtomSelector: &stubAtomSelectorDuel{}})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, duRequest(duChallenger, http.MethodPost, "/v1/duels/"+d.ID+"/answer",
		`{"round_no":1,"answer":"B","answer_time_ms":1000}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"correct":true`) {
		t.Errorf("expected correct=true via AtomSelector: %s", rr.Body.String())
	}
}

func TestSubmitDuelAnswer_Branches(t *testing.T) {
	t.Parallel()
	// 501 nil Duels.
	h501 := NewHandler(Deps{})
	rr := httptest.NewRecorder()
	h501.ServeHTTP(rr, duRequest(duChallenger, http.MethodPost, "/v1/duels/x/answer", `{}`))
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("nil duels: status=%d, want 501", rr.Code)
	}

	repo := inmem.NewDuelRepo()
	h := NewHandler(Deps{Duels: repo, Rules: duRules()})

	// Malformed body → 400.
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, duRequest(duChallenger, http.MethodPost, "/v1/duels/x/answer", `{bad`))
	if rr.Code != http.StatusBadRequest {
		t.Errorf("malformed body: status=%d, want 400", rr.Code)
	}

	// Unknown duel → 404.
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, duRequest(duChallenger, http.MethodPost, "/v1/duels/nope/answer", `{}`))
	if rr.Code != http.StatusNotFound {
		t.Errorf("unknown duel: status=%d, want 404", rr.Code)
	}

	// ResolveRound repo fault → 500.
	faulty := &faultyDuelStore{DuelRepo: inmem.NewDuelRepo()}
	d := seedingDuel(t, faulty.DuelRepo, 1)
	faulty.errResolve = errors.New("pg down")
	h500 := NewHandler(Deps{Duels: faulty, Rules: duRules()})
	rr = httptest.NewRecorder()
	h500.ServeHTTP(rr, duRequest(duChallenger, http.MethodPost, "/v1/duels/"+d.ID+"/answer",
		`{"round_no":1,"answer":"A","answer_time_ms":1000}`))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("resolve fault: status=%d, want 500", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// POST /v1/duels/{duel_id}/answer — blitz
// ---------------------------------------------------------------------------

// seedBlitz creates a blitz duel (timed or race) in the repo.
func seedBlitz(t *testing.T, repo *inmem.DuelRepo, variant duel.BlitzVariant, rounds int) *duel.Duel {
	t.Helper()
	blitzCfg := duel.BlitzConfig{Variant: variant}
	if variant == duel.BlitzVariantTimed {
		blitzCfg.TimeLimitSec = 120
	} else {
		blitzCfg.RaceTarget = 1
	}
	id := duel.NewUUIDv7()
	d, err := duel.NewBlitzPoolDuelWithID(duCfg(), duChallenger, duOpponent, duTenant,
		duel.ScopeRanked, rounds, nil, blitzCfg, id)
	if err != nil {
		t.Fatalf("NewBlitzPoolDuelWithID: %v", err)
	}
	// Pool duels start as StatusAccepted (pool matching IS the acceptance) —
	// StartBattle transitions them to in_progress directly.
	if err := d.StartBattle(answerPicks(rounds)); err != nil {
		t.Fatalf("StartBattle: %v", err)
	}
	if err := repo.SaveDuel(context.Background(), d); err != nil {
		t.Fatalf("SaveDuel: %v", err)
	}
	return d
}

func TestSubmitDuelAnswer_BlitzTimedBothPlayers(t *testing.T) {
	t.Parallel()
	repo := &faultyDuelStore{DuelRepo: inmem.NewDuelRepo()}
	d := seedBlitz(t, repo.DuelRepo, duel.BlitzVariantTimed, 1)
	h := NewHandler(Deps{Duels: repo, Rules: duRules()})

	// Challenger answers — round stays open (opponent pending).
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, duRequest(duChallenger, http.MethodPost, "/v1/duels/"+d.ID+"/answer",
		`{"round_no":1,"answer":"A","answer_time_ms":1000}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"round_resolved":false`) {
		t.Errorf("expected round_resolved:false on first blitz answer: %s", rr.Body.String())
	}

	// Opponent answers — round resolves.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, duRequest(duOpponent, http.MethodPost, "/v1/duels/"+d.ID+"/answer",
		`{"round_no":1,"answer":"A","answer_time_ms":1000}`))
	if rr2.Code != http.StatusOK {
		t.Fatalf("opponent status=%d body=%s", rr2.Code, rr2.Body.String())
	}
	if !strings.Contains(rr2.Body.String(), `"round_resolved":true`) {
		t.Errorf("expected round_resolved:true on second blitz answer: %s", rr2.Body.String())
	}

	// Same player answers the resolved round again → 409.
	rr3 := httptest.NewRecorder()
	h.ServeHTTP(rr3, duRequest(duOpponent, http.MethodPost, "/v1/duels/"+d.ID+"/answer",
		`{"round_no":1,"answer":"A","answer_time_ms":1000}`))
	if rr3.Code != http.StatusConflict {
		t.Errorf("repeat blitz answer: status=%d, want 409", rr3.Code)
	}
}

func TestSubmitDuelAnswer_BlitzRaceCompletes(t *testing.T) {
	t.Parallel()
	repo := &faultyDuelStore{DuelRepo: inmem.NewDuelRepo()}
	d := seedBlitz(t, repo.DuelRepo, duel.BlitzVariantRace, 1)
	pub := &recPublisher{}
	h := NewHandler(Deps{Duels: repo, Rules: duRules(), DuelEvents: pub})

	// Race variant: RaceTarget=1 — one correct answer completes the duel
	// (ranked → ELO applied).
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, duRequest(duChallenger, http.MethodPost, "/v1/duels/"+d.ID+"/answer",
		`{"round_no":1,"answer":"A","answer_time_ms":1000}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"duel_status":"completed"`) {
		t.Errorf("expected completed blitz: %s", rr.Body.String())
	}
	if pub.calls != 1 {
		t.Errorf("DuelEvents calls=%d, want 1", pub.calls)
	}
	stats, _ := repo.GetRatingStats(context.Background(), duTenant, duChallenger)
	if stats.Wins != 1 {
		t.Errorf("challenger wins=%d, want 1 (blitz ELO)", stats.Wins)
	}
}

func TestSubmitDuelAnswer_BlitzPersistFault(t *testing.T) {
	t.Parallel()
	faulty := &faultyDuelStore{DuelRepo: inmem.NewDuelRepo()}
	d := seedBlitz(t, faulty.DuelRepo, duel.BlitzVariantTimed, 1)
	faulty.errSave = errors.New("pg down")
	h := NewHandler(Deps{Duels: faulty, Rules: duRules()})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, duRequest(duChallenger, http.MethodPost, "/v1/duels/"+d.ID+"/answer",
		`{"round_no":1,"answer":"A","answer_time_ms":1000}`))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("blitz save fault: status=%d, want 500", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// GET /v1/duels — list
// ---------------------------------------------------------------------------

func TestListDuels_Success(t *testing.T) {
	t.Parallel()
	repo := inmem.NewDuelRepo()
	seedingDuel(t, repo, 1)
	seedingDuel(t, repo, 1) // second duel for the same pair
	h := NewHandler(Deps{Duels: repo})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, duRequest(duChallenger, http.MethodGet, "/v1/duels?limit=10", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var body struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Data) < 1 {
		t.Fatalf("expected duels, got %d", len(body.Data))
	}
	for _, d := range body.Data {
		if d["challenger_gcid"] != duChallenger {
			t.Errorf("duel card challenger=%v", d["challenger_gcid"])
		}
	}
}

func TestListDuels_Branches(t *testing.T) {
	t.Parallel()
	// 501 nil Duels.
	h501 := NewHandler(Deps{})
	rr := httptest.NewRecorder()
	h501.ServeHTTP(rr, duRequest(duChallenger, http.MethodGet, "/v1/duels", ""))
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("nil duels: status=%d, want 501", rr.Code)
	}

	// Status filter + 500 fault.
	repo := inmem.NewDuelRepo()
	seedingDuel(t, repo, 1)
	h := NewHandler(Deps{Duels: repo})

	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, duRequest(duChallenger, http.MethodGet, "/v1/duels?status=completed&limit=50", ""))
	if rr.Code != http.StatusOK {
		t.Errorf("status filter: status=%d, want 200", rr.Code)
	}

	faulty := &faultyDuelStore{DuelRepo: repo, errList: errors.New("pg down")}
	h500 := NewHandler(Deps{Duels: faulty})
	rr = httptest.NewRecorder()
	h500.ServeHTTP(rr, duRequest(duChallenger, http.MethodGet, "/v1/duels", ""))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("list fault: status=%d, want 500", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// GET /v1/duels/{duel_id}
// ---------------------------------------------------------------------------

func TestGetDuel_Success(t *testing.T) {
	t.Parallel()
	repo := inmem.NewDuelRepo()
	d := seedingDuel(t, repo, 1)
	h := NewHandler(Deps{Duels: repo})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, duRequest(duChallenger, http.MethodGet, "/v1/duels/"+d.ID, ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"duel_id":"`+d.ID+`"`) {
		t.Errorf("duel_id missing: %s", rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"round_count":1`) {
		t.Errorf("round_count missing: %s", rr.Body.String())
	}
}

func TestGetDuel_Branches(t *testing.T) {
	t.Parallel()
	// 501 nil Duels.
	h501 := NewHandler(Deps{})
	rr := httptest.NewRecorder()
	h501.ServeHTTP(rr, duRequest(duChallenger, http.MethodGet, "/v1/duels/x", ""))
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("nil duels: status=%d, want 501", rr.Code)
	}

	repo := inmem.NewDuelRepo()
	h := NewHandler(Deps{Duels: repo})

	// Unknown duel → 404.
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, duRequest(duChallenger, http.MethodGet, "/v1/duels/nope", ""))
	if rr.Code != http.StatusNotFound {
		t.Errorf("unknown: status=%d, want 404", rr.Code)
	}

	// Tenant mismatch → 404.
	other, err := duel.NewDuel(duCfg(), duChallenger, duOpponent, "other-tenant",
		duel.ScopeRanked, 1, nil, 24*time.Hour)
	if err != nil {
		t.Fatalf("NewDuel: %v", err)
	}
	_ = repo.SaveDuel(context.Background(), other)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, duRequest(duChallenger, http.MethodGet, "/v1/duels/"+other.ID, ""))
	if rr.Code != http.StatusNotFound {
		t.Errorf("cross-tenant: status=%d, want 404", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// GET /v1/duels/{duel_id}/ws
// ---------------------------------------------------------------------------

func TestDuelWS(t *testing.T) {
	t.Parallel()
	// 501 when unwired.
	h501 := NewHandler(Deps{})
	rr := httptest.NewRecorder()
	h501.ServeHTTP(rr, duRequest(duChallenger, http.MethodGet, "/v1/duels/x/ws", ""))
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("unwired: status=%d, want 501", rr.Code)
	}

	// Delegates when wired.
	var called bool
	ws := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusSwitchingProtocols)
	})
	h := NewHandler(Deps{WSHandler: ws})
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, duRequest(duChallenger, http.MethodGet, "/v1/duels/x/ws", ""))
	if !called {
		t.Error("WSHandler not invoked")
	}
	if rr.Code != http.StatusSwitchingProtocols {
		t.Errorf("delegation status=%d, want 101", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// GET /v1/duels/my-rating + /v1/duels/leaderboard — remaining branches
// ---------------------------------------------------------------------------

func TestGetMyRating_Branches(t *testing.T) {
	t.Parallel()
	// 501 nil Duels.
	h501 := NewHandler(Deps{})
	rr := httptest.NewRecorder()
	h501.ServeHTTP(rr, duRequest(duChallenger, http.MethodGet, "/v1/duels/my-rating", ""))
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("nil duels: status=%d, want 501", rr.Code)
	}

	// Stats fault → 500.
	faulty := &faultyDuelStore{DuelRepo: inmem.NewDuelRepo(), errStats: errors.New("pg down")}
	h500 := NewHandler(Deps{Duels: faulty})
	rr = httptest.NewRecorder()
	h500.ServeHTTP(rr, duRequest(duChallenger, http.MethodGet, "/v1/duels/my-rating", ""))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("stats fault: status=%d, want 500", rr.Code)
	}

	// Profiles wired → completed_courses + course_bonus + proficiency.
	repo := inmem.NewDuelRepo()
	profiles := inmem.NewProfilerRepo()
	p, err := profiler.NewProfile(duChallenger, duTenant, "bio", nil)
	if err != nil {
		t.Fatalf("NewProfile: %v", err)
	}
	p.CourseTitles = []string{"c1", "c2", "c3"}
	_ = profiles.SaveProfile(context.Background(), p)
	h := NewHandler(Deps{Duels: repo, Profiles: profiles})

	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, duRequest(duChallenger, http.MethodGet, "/v1/duels/my-rating", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"completed_courses":3`) || !strings.Contains(rr.Body.String(), `"course_bonus":60`) {
		t.Errorf("course bonus not computed: %s", rr.Body.String())
	}
}

func TestGetDuelLeaderboard_Branches(t *testing.T) {
	t.Parallel()
	// 501 nil Duels.
	h501 := NewHandler(Deps{})
	rr := httptest.NewRecorder()
	h501.ServeHTTP(rr, duRequest(duChallenger, http.MethodGet, "/v1/duels/leaderboard", ""))
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("nil duels: status=%d, want 501", rr.Code)
	}

	// Overall fault → 500.
	faulty := &faultyDuelStore{DuelRepo: inmem.NewDuelRepo(), errTop: errors.New("pg down")}
	h500 := NewHandler(Deps{Duels: faulty})
	rr = httptest.NewRecorder()
	h500.ServeHTTP(rr, duRequest(duChallenger, http.MethodGet, "/v1/duels/leaderboard", ""))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("top fault: status=%d, want 500", rr.Code)
	}

	// Category fault → 500.
	faultyCat := &faultyDuelStore{DuelRepo: inmem.NewDuelRepo(), errTopCat: errors.New("pg down")}
	h500c := NewHandler(Deps{Duels: faultyCat})
	rr = httptest.NewRecorder()
	h500c.ServeHTTP(rr, duRequest(duChallenger, http.MethodGet, "/v1/duels/leaderboard?category=mathematics", ""))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("category fault: status=%d, want 500", rr.Code)
	}
}