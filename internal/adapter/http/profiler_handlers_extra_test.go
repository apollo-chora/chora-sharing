// profiler_handlers_extra_test.go — profiler branches not covered by
// profiler_handlers_test.go (which pins the async 202 flow):
//
//	GET  /v1/me/profile       — fault, empty, populated
//	PUT  /v1/me/profile/tags  — update paths + faults (0% before this file)
//	POST /v1/me/profile/generate — resolver branches, save fault, bad body
//	generateProfileAsync      — direct, deterministic branch coverage
//	GET  /v1/me/profile/ws    — wired delegation
package httpadapter

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/profiler"
)

const (
	prTenant = "01970000-0000-7000-8000-0000000000b1"
	prGCID   = "01970000-0000-7000-9000-0000000000b1"
)

func prReq(method, path, body string) *http.Request {
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	r.Header.Set("gcid", prGCID)
	r.Header.Set("X-Tenant-Id", prTenant)
	return r
}

// fakeResolver is a DisplayNameResolver returning a fixed name or error.
type fakeResolver struct {
	name string
	err  error
}

func (r *fakeResolver) ResolveDisplayName(_ context.Context, _ string) (string, error) {
	return r.name, r.err
}

// faultyProfiles is a ProfileStore with failure + capture hooks.
type faultyProfiles struct {
	*inmem.ProfilerRepo
	saveErr    error
	getErr     error
	saveCalls  int
}

func (f *faultyProfiles) SaveProfile(ctx context.Context, p *profiler.Profile) error {
	f.saveCalls++
	if f.saveErr != nil {
		return f.saveErr
	}
	return f.ProfilerRepo.SaveProfile(ctx, p)
}

func (f *faultyProfiles) GetProfile(ctx context.Context, gcid string) (*profiler.Profile, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.ProfilerRepo.GetProfile(ctx, gcid)
}

// ---------------------------------------------------------------------------
// GET /v1/me/profile
// ---------------------------------------------------------------------------

func TestGetProfile_Branches(t *testing.T) {
	t.Parallel()
	// 501 nil Profiles.
	h501 := NewHandler(Deps{})
	rr := httptest.NewRecorder()
	h501.ServeHTTP(rr, prReq(http.MethodGet, "/v1/me/profile", ""))
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("nil port: status=%d, want 501", rr.Code)
	}

	// Read fault → 200 with an empty profile (the FE treats it as unset).
	store := &faultyProfiles{ProfilerRepo: inmem.NewProfilerRepo(), getErr: errors.New("pg down")}
	hFault := NewHandler(Deps{Profiles: store})
	rr = httptest.NewRecorder()
	hFault.ServeHTTP(rr, prReq(http.MethodGet, "/v1/me/profile", ""))
	if rr.Code != http.StatusOK {
		t.Errorf("fault: status=%d, want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), `"gcid":"`+prGCID+`"`) {
		t.Errorf("empty profile must carry the gcid: %s", rr.Body.String())
	}

	// Populated profile → fields surfaced.
	store2 := inmem.NewProfilerRepo()
	p, _ := profiler.NewProfile(prGCID, prTenant, "bio text", []string{"c1"})
	p.SetTags([]profiler.InterestTag{{Category: profiler.CategoryArts, Tag: "music_theory"}})
	_ = store2.SaveProfile(context.Background(), p)
	hOK := NewHandler(Deps{Profiles: store2})
	rr = httptest.NewRecorder()
	hOK.ServeHTTP(rr, prReq(http.MethodGet, "/v1/me/profile", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "bio text") || !strings.Contains(rr.Body.String(), "music_theory") {
		t.Errorf("populated profile missing fields: %s", rr.Body.String())
	}
}

// ---------------------------------------------------------------------------
// PUT /v1/me/profile/tags
// ---------------------------------------------------------------------------

func TestUpdateProfileTags_SuccessAndBranches(t *testing.T) {
	t.Parallel()
	// 501 nil Profiles.
	h501 := NewHandler(Deps{})
	rr := httptest.NewRecorder()
	h501.ServeHTTP(rr, prReq(http.MethodPut, "/v1/me/profile/tags", `{"tags":[]}`))
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("nil port: status=%d, want 501", rr.Code)
	}

	// New profile: no existing row → a fresh profile is created with the tags.
	store := inmem.NewProfilerRepo()
	hNew := NewHandler(Deps{Profiles: store})
	rr = httptest.NewRecorder()
	hNew.ServeHTTP(rr, prReq(http.MethodPut, "/v1/me/profile/tags",
		`{"tags":[{"category":"programming","tag":"golang"}]}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("new: status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "golang") {
		t.Errorf("saved tags missing: %s", rr.Body.String())
	}

	// Existing profile: tags are replaced.
	store2 := inmem.NewProfilerRepo()
	p, _ := profiler.NewProfile(prGCID, prTenant, "bio", nil)
	p.SetTags([]profiler.InterestTag{{Category: profiler.CategoryScience, Tag: "physics"}})
	_ = store2.SaveProfile(context.Background(), p)
	hUpd := NewHandler(Deps{Profiles: store2})
	rr = httptest.NewRecorder()
	hUpd.ServeHTTP(rr, prReq(http.MethodPut, "/v1/me/profile/tags",
		`{"tags":[{"category":"languages","tag":"mandarin"}]}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("update: status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "mandarin") || strings.Contains(rr.Body.String(), "physics") {
		t.Errorf("tags not replaced: %s", rr.Body.String())
	}

	// Malformed JSON → 400.
	hBad := NewHandler(Deps{Profiles: inmem.NewProfilerRepo()})
	rr = httptest.NewRecorder()
	hBad.ServeHTTP(rr, prReq(http.MethodPut, "/v1/me/profile/tags", `{nope`))
	if rr.Code != http.StatusBadRequest {
		t.Errorf("malformed: status=%d, want 400", rr.Code)
	}

	// Save fault → 500.
	faulty := &faultyProfiles{ProfilerRepo: inmem.NewProfilerRepo(), saveErr: errors.New("pg down")}
	h500 := NewHandler(Deps{Profiles: faulty})
	rr = httptest.NewRecorder()
	h500.ServeHTTP(rr, prReq(http.MethodPut, "/v1/me/profile/tags", `{"tags":[]}`))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("save fault: status=%d, want 500", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// POST /v1/me/profile/generate — non-async branches
// ---------------------------------------------------------------------------

func TestGenerateProfile_ResolverAndFaultBranches(t *testing.T) {
	t.Parallel()

	// Resolver error → display_name stays empty; the 202 still returns.
	conj := &fakeConjurerDirect{result: &profiler.ConjuredProfile{}, err: nil}
	store := inmem.NewProfilerRepo()
	h1 := NewHandler(Deps{
		Profiles:            store,
		Conjurer:            conj,
		DisplayNameResolver: &fakeResolver{err: errors.New("identity down")},
	})
	rr := httptest.NewRecorder()
	h1.ServeHTTP(rr, prReq(http.MethodPost, "/v1/me/profile/generate", `{"bio":"hi"}`))
	if rr.Code != http.StatusAccepted {
		t.Errorf("resolver err: status=%d, want 202", rr.Code)
	}

	// Resolver returns a name → display_name resolved.
	conj2 := &fakeConjurerDirect{result: &profiler.ConjuredProfile{}, err: nil}
	store2 := inmem.NewProfilerRepo()
	h2 := NewHandler(Deps{
		Profiles:            store2,
		Conjurer:            conj2,
		DisplayNameResolver: &fakeResolver{name: "Phyllis"},
	})
	rr = httptest.NewRecorder()
	h2.ServeHTTP(rr, prReq(http.MethodPost, "/v1/me/profile/generate", `{"bio":"hi"}`))
	if rr.Code != http.StatusAccepted {
		t.Errorf("resolver ok: status=%d, want 202", rr.Code)
	}
	p2, err := store2.GetProfile(context.Background(), prGCID)
	if err != nil || p2.DisplayName != "Phyllis" {
		t.Errorf("display_name not resolved: %+v err=%v", p2, err)
	}

	// SaveProfile fault on the generating-state write → 500 (no goroutine).
	faulty := &faultyProfiles{ProfilerRepo: inmem.NewProfilerRepo(), saveErr: errors.New("pg down")}
	h500 := NewHandler(Deps{Profiles: faulty, Conjurer: &fakeConjurerDirect{result: &profiler.ConjuredProfile{}}})
	rr = httptest.NewRecorder()
	h500.ServeHTTP(rr, prReq(http.MethodPost, "/v1/me/profile/generate", `{"bio":"hi"}`))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("save fault: status=%d, want 500", rr.Code)
	}

	// Malformed JSON → 400.
	hBad := NewHandler(Deps{Profiles: inmem.NewProfilerRepo(), Conjurer: &fakeConjurerDirect{}})
	rr = httptest.NewRecorder()
	hBad.ServeHTTP(rr, prReq(http.MethodPost, "/v1/me/profile/generate", `{nope`))
	if rr.Code != http.StatusBadRequest {
		t.Errorf("malformed: status=%d, want 400", rr.Code)
	}

	// Profiles nil → 501 (conjurer wired).
	h501 := NewHandler(Deps{Conjurer: &fakeConjurerDirect{}})
	rr = httptest.NewRecorder()
	h501.ServeHTTP(rr, prReq(http.MethodPost, "/v1/me/profile/generate", `{"bio":"hi"}`))
	if rr.Code != http.StatusNotImplemented {
		t.Errorf("nil profiles: status=%d, want 501", rr.Code)
	}
}

// fakeConjurerDirect returns the configured result immediately (no release
// channel — for the synchronous branches).
type fakeConjurerDirect struct {
	result *profiler.ConjuredProfile
	err    error
}

func (c *fakeConjurerDirect) Conjure(_ context.Context, _, _, _ string, _ []string) (*profiler.ConjuredProfile, error) {
	return c.result, c.err
}

// ---------------------------------------------------------------------------
// generateProfileAsync — direct, deterministic
// ---------------------------------------------------------------------------

func TestGenerateProfileAsync_SuccessPublishesReady(t *testing.T) {
	t.Parallel()
	store := inmem.NewProfilerRepo()
	p, _ := profiler.NewProfile(prGCID, prTenant, "bio", []string{"c1"})
	_ = store.SaveProfile(context.Background(), p)
	pub := &captureBroker{}

	h := NewHandler(Deps{
		Profiles:      store,
		Conjurer:      &fakeConjurerDirect{result: &profiler.ConjuredProfile{
			Tags:        []profiler.InterestTag{{Category: profiler.CategoryProgramming, Tag: "golang"}},
			Proficiency: profiler.Proficiency{PerCategory: map[string]profiler.ProficiencyLevel{string(profiler.CategoryProgramming): profiler.ProficiencyAdvanced}},
		}},
		ProfileBroker: pub,
	})

	h.generateProfileAsync(prTenant, prGCID, "bio", []string{"c1"})

	if len(pub.frames) != 1 || pub.frames[0].kind != ProfileKindReady {
		t.Fatalf("expected one profile_ready frame, got %+v", pub.frames)
	}
	if !strings.Contains(string(pub.frames[0].payload), "golang") {
		t.Errorf("ready payload missing tags: %s", pub.frames[0].payload)
	}
	saved, _ := store.GetProfile(context.Background(), prGCID)
	if len(saved.Tags) != 1 || saved.Tags[0].Tag != "golang" {
		t.Errorf("final tags not persisted: %+v", saved.Tags)
	}
}

func TestGenerateProfileAsync_GetProfileFault_RebuildsProfile(t *testing.T) {
	t.Parallel()
	store := &faultyProfiles{ProfilerRepo: inmem.NewProfilerRepo(), getErr: errors.New("row vanished")}
	pub := &captureBroker{}
	h := NewHandler(Deps{
		Profiles:      store,
		Conjurer:      &fakeConjurerDirect{result: &profiler.ConjuredProfile{Tags: []profiler.InterestTag{}}},
		ProfileBroker: pub,
	})

	h.generateProfileAsync(prTenant, prGCID, "bio", nil)

	// Rebuild succeeded → ready published, and the rebuilt profile saved once.
	if len(pub.frames) != 1 || pub.frames[0].kind != ProfileKindReady {
		t.Fatalf("expected ready frame after rebuild, got %+v", pub.frames)
	}
	if store.saveCalls != 1 {
		t.Errorf("expected exactly 1 save after rebuild, got %d", store.saveCalls)
	}
}

func TestGenerateProfileAsync_SaveFault_PublishesError(t *testing.T) {
	t.Parallel()
	store := &faultyProfiles{ProfilerRepo: inmem.NewProfilerRepo(), saveErr: errors.New("pg down")}
	pub := &captureBroker{}
	h := NewHandler(Deps{
		Profiles:      store,
		Conjurer:      &fakeConjurerDirect{result: &profiler.ConjuredProfile{Tags: []profiler.InterestTag{}}},
		ProfileBroker: pub,
	})

	h.generateProfileAsync(prTenant, prGCID, "bio", nil)

	if len(pub.frames) != 1 || pub.frames[0].kind != ProfileKindError {
		t.Fatalf("expected error frame, got %+v", pub.frames)
	}
}

func TestGenerateProfileAsync_ConjureFault_PublishesError(t *testing.T) {
	t.Parallel()
	h := NewHandler(Deps{
		Profiles:      inmem.NewProfilerRepo(),
		Conjurer:      &fakeConjurerDirect{err: errors.New("agent timeout")},
		ProfileBroker: &captureBroker{},
	})

	h.generateProfileAsync(prTenant, prGCID, "bio", nil)
	// No broker assertion needed beyond: with a nil broker the error path is a
	// silent no-op; here the broker is present so a frame MUST have arrived.
	if h.deps.ProfileBroker.(*captureBroker).len() != 1 {
		t.Errorf("expected an error frame from the conjurer failure")
	}
}

// captureBroker records published profile frames.
type captureBroker struct {
	mu     sync.Mutex
	frames []brokerFrame
}

type brokerFrame struct {
	kind    ProfileMessageKind
	payload []byte
}

func (b *captureBroker) PublishProfile(_ string, kind ProfileMessageKind, payload []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.frames = append(b.frames, brokerFrame{kind: kind, payload: payload})
}

func (b *captureBroker) len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.frames)
}

func TestPublishProfileHelpers_NilBrokerNoop(t *testing.T) {
	t.Parallel()
	h := NewHandler(Deps{}) // no broker
	h.publishProfileReady(prGCID, profilerResponse{GCID: prGCID})
	h.publishProfileError(prGCID, "boom")
	// Nothing to assert beyond "did not panic".
}

// ---------------------------------------------------------------------------
// GET /v1/me/profile/ws — wired delegation
// ---------------------------------------------------------------------------

func TestProfileWS_WiredDelegation(t *testing.T) {
	t.Parallel()
	var called bool
	ws := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusSwitchingProtocols)
	})
	h := NewHandler(Deps{ProfileWSHandler: ws})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, prReq(http.MethodGet, "/v1/me/profile/ws", ""))
	if !called {
		t.Error("ProfileWSHandler not invoked")
	}
	if rr.Code != http.StatusSwitchingProtocols {
		t.Errorf("status=%d, want 101", rr.Code)
	}
}