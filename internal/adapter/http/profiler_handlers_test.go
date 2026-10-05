// profiler_handlers_test.go — tests for the async fire-and-forget
// generateProfile path (Step 1 of the async-profile-gen plan) + the
// tags:null → [] fix (Step 5).
//
// Coverage:
//   - generateProfile returns 202 immediately with {status:"generating", profile:{...}}
//     and the persisted profile has empty tags (the generating state).
//   - the background goroutine calls Conjure, applies the result, saves
//     the final profile, + publishes a profile_ready frame to the broker.
//   - on Conjure error, a profile_error frame is published + the profile
//     stays in the generating state.
//   - toProfilerResponse never serialises Tags as nil (Step 5 regression).
package httpadapter_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	httpadapter "github.com/apollo-chora/chora-sharing/internal/adapter/http"
	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/profiler"
)

const (
	profTenant = "01970000-0000-7000-8000-0000000000b1"
	profGCID   = "01970000-0000-7000-9000-0000000000b1"
)

// fakeConjurer is a test double for httpadapter.ConjurerPort. It blocks
// on a release channel until the test is ready to observe the goroutine
// — so the 202 can be asserted BEFORE the async work completes.
type fakeConjurer struct {
	mu       sync.Mutex
	release  chan struct{}
	result   *profiler.ConjuredProfile
	err      error
	called   bool
	bioSeen  string
	coursesSeen []string
}

func newFakeConjurer(result *profiler.ConjuredProfile, err error) *fakeConjurer {
	return &fakeConjurer{
		release: make(chan struct{}),
		result:  result,
		err:     err,
	}
}

func (f *fakeConjurer) Conjure(_ context.Context, _, _, bio string, courseTitles []string) (*profiler.ConjuredProfile, error) {
	f.mu.Lock()
	f.called = true
	f.bioSeen = bio
	f.coursesSeen = append([]string(nil), courseTitles...)
	f.mu.Unlock()
	// Block until the test releases us — this is how the test asserts
	// the 202 returned BEFORE the conjurer finished.
	<-f.release
	return f.result, f.err
}

func (f *fakeConjurer) wasCalled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.called
}

// capturePublisher is a test double for httpadapter.ProfileMessagePublisher.
// It records every PublishProfile call so the test can assert the frame
// kind + payload after the goroutine completes.
type capturePublisher struct {
	mu       sync.Mutex
	frames   []publishedFrame
	signal   chan struct{} // closed on first publish so tests can wait
}

type publishedFrame struct {
	gcid    string
	kind    httpadapter.ProfileMessageKind
	payload []byte
}

func newCapturePublisher() *capturePublisher {
	return &capturePublisher{signal: make(chan struct{})}
}

func (p *capturePublisher) PublishProfile(gcid string, kind httpadapter.ProfileMessageKind, payload []byte) {
	p.mu.Lock()
	p.frames = append(p.frames, publishedFrame{gcid: gcid, kind: kind, payload: payload})
	if len(p.frames) == 1 {
		close(p.signal)
	}
	p.mu.Unlock()
}

func (p *capturePublisher) frames_() []publishedFrame {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]publishedFrame, len(p.frames))
	copy(out, p.frames)
	return out
}

func (p *capturePublisher) waitForPublish(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-p.signal:
	case <-time.After(timeout):
		t.Fatalf("publisher: no frame received within %s", timeout)
	}
}

func newProfilerHandler(conjurer httpadapter.ConjurerPort, pub httpadapter.ProfileMessagePublisher) http.Handler {
	return httpadapter.NewHandler(httpadapter.Deps{
		Profiles:      inmem.NewProfilerRepo(),
		Conjurer:      conjurer,
		ProfileBroker: pub,
	})
}

func profilerRequest(method, path, body string) (*http.Request, *httptest.ResponseRecorder) {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("gcid", profGCID)
	req.Header.Set("X-Tenant-Id", profTenant)
	return req, httptest.NewRecorder()
}

// TestGenerateProfile_Returns202Immediately asserts the fire-and-forget
// contract: the handler returns 202 + {status:"generating"} BEFORE the
// conjurer agent call completes. The conjurer blocks on a release channel;
// if the handler waited for it, this test would time out.
func TestGenerateProfile_Returns202Immediately(t *testing.T) {
	t.Parallel()
	conjurer := newFakeConjurer(nil, nil) // never released — must not block the 202
	handler := newProfilerHandler(conjurer, newCapturePublisher())

	req, rec := profilerRequest(http.MethodPost, "/v1/me/profile/generate",
		`{"bio":"I love astronomy and physics","course_titles":["Intro to Cosmology"]}`)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d; want 202 Accepted (fire-and-forget)", rec.Code)
	}
	var body struct {
		Status  string `json:"status"`
		Profile struct {
			GCID string   `json:"gcid"`
			Bio  string   `json:"bio"`
			Tags []struct {
				Category string `json:"category"`
				Tag       string `json:"tag"`
			} `json:"tags"`
		} `json:"profile"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode 202 body: %v (%s)", err, rec.Body.String())
	}
	if body.Status != "generating" {
		t.Errorf("status = %q; want %q", body.Status, "generating")
	}
	if body.Profile.GCID != profGCID {
		t.Errorf("profile.gcid = %q; want %q", body.Profile.GCID, profGCID)
	}
	if body.Profile.Bio != "I love astronomy and physics" {
		t.Errorf("profile.bio = %q; want the submitted bio", body.Profile.Bio)
	}
	// The generating-state profile MUST have empty (not null) tags so
	// the FE's tagCount() === 0 check holds + JSON serialises as [].
	if body.Profile.Tags == nil {
		t.Errorf("profile.tags = nil; want [] (generating state must not be null)")
	}
	if len(body.Profile.Tags) != 0 {
		t.Errorf("profile.tags = %v; want empty slice (generating state)", body.Profile.Tags)
	}

	// The conjurer MUST have been called (goroutine spawned) even though
	// the 202 already returned. Give the goroutine a moment to reach the
	// blocking Conjure call.
	if !waitFor(conjurer.wasCalled, time.Second) {
		t.Fatal("conjurer.Conjure was never called — goroutine not spawned")
	}
}

// TestGenerateProfile_PublishesReadyOnSuccess drives the full async
// path: 202 → goroutine calls Conjure → succeeds → publishes a
// profile_ready frame with the final tags + proficiency.
func TestGenerateProfile_PublishesReadyOnSuccess(t *testing.T) {
	t.Parallel()
	result := &profiler.ConjuredProfile{
		Tags: []profiler.InterestTag{
			{Category: profiler.CategoryScience, Tag: "astronomy"},
			{Category: profiler.CategoryScience, Tag: "physics"},
		},
		Proficiency: profiler.Proficiency{PerCategory: map[string]profiler.ProficiencyLevel{"programming": profiler.ProficiencyIntermediate}},
	}
	conjurer := newFakeConjurer(result, nil)
	pub := newCapturePublisher()
	handler := newProfilerHandler(conjurer, pub)

	req, rec := profilerRequest(http.MethodPost, "/v1/me/profile/generate",
		`{"bio":"I love astronomy and physics","course_titles":["Intro to Cosmology"]}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d; want 202", rec.Code)
	}

	// Release the conjurer + assert the goroutine publishes profile_ready.
	close(conjurer.release)
	pub.waitForPublish(t, 2*time.Second)

	frames := pub.frames_()
	if len(frames) != 1 {
		t.Fatalf("expected 1 published frame; got %d", len(frames))
	}
	if frames[0].gcid != profGCID {
		t.Errorf("frame.gcid = %q; want %q", frames[0].gcid, profGCID)
	}
	if frames[0].kind != httpadapter.ProfileKindReady {
		t.Errorf("frame.kind = %q; want %q", frames[0].kind, httpadapter.ProfileKindReady)
	}
	var resp struct {
		GCID string                 `json:"gcid"`
		Tags []profiler.InterestTag `json:"tags"`
	}
	if err := json.Unmarshal(frames[0].payload, &resp); err != nil {
		t.Fatalf("decode ready payload: %v (%s)", err, frames[0].payload)
	}
	if len(resp.Tags) != 2 {
		t.Errorf("ready payload tags = %v; want 2", resp.Tags)
	}
}

// TestGenerateProfile_PublishesErrorOnConjureFailure asserts that a
// failing Conjure call surfaces as a profile_error frame (so the FE
// can show the error instead of hanging on "generating").
func TestGenerateProfile_PublishesErrorOnConjureFailure(t *testing.T) {
	t.Parallel()
	conjurer := newFakeConjurer(nil, errors.New("agent timeout"))
	pub := newCapturePublisher()
	handler := newProfilerHandler(conjurer, pub)

	req, rec := profilerRequest(http.MethodPost, "/v1/me/profile/generate",
		`{"bio":"I love astronomy","course_titles":[]}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d; want 202 (error surfaces via WS, not HTTP)", rec.Code)
	}

	close(conjurer.release)
	pub.waitForPublish(t, 2*time.Second)

	frames := pub.frames_()
	if len(frames) != 1 {
		t.Fatalf("expected 1 published frame; got %d", len(frames))
	}
	if frames[0].kind != httpadapter.ProfileKindError {
		t.Errorf("frame.kind = %q; want %q", frames[0].kind, httpadapter.ProfileKindError)
	}
	var errBody struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(frames[0].payload, &errBody); err != nil {
		t.Fatalf("decode error payload: %v (%s)", err, frames[0].payload)
	}
	if !strings.Contains(errBody.Error, "agent timeout") {
		t.Errorf("error message = %q; want it to contain the agent error", errBody.Error)
	}
}

// TestGenerateProfile_PersistedProfileIsQueryable asserts the
// generating-state profile is persisted immediately (so the FE's
// GET /v1/me/profile fallback after a WS failure sees the bio).
func TestGenerateProfile_PersistedProfileIsQueryable(t *testing.T) {
	t.Parallel()
	conjurer := newFakeConjurer(nil, nil) // never released
	handler := newProfilerHandler(conjurer, newCapturePublisher())

	req, rec := profilerRequest(http.MethodPost, "/v1/me/profile/generate",
		`{"bio":"I love astronomy","course_titles":[]}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d; want 202", rec.Code)
	}

	// GET the profile — must return the persisted generating state.
	getReq, getRec := profilerRequest(http.MethodGet, "/v1/me/profile", "")
	handler.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GET profile status = %d; want 200", getRec.Code)
	}
	var p struct {
		Bio  string `json:"bio"`
		Tags []any  `json:"tags"`
	}
	if err := json.Unmarshal(getRec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode get body: %v (%s)", err, getRec.Body.String())
	}
	if p.Bio != "I love astronomy" {
		t.Errorf("bio = %q; want persisted bio", p.Bio)
	}
	if p.Tags == nil {
		t.Errorf("tags = nil; want [] — Step 5 regression: null tags break the FE")
	}
}

// TestToProfilerResponse_TagsNeverNull is the Step 5 regression test:
// a profile with nil tags (e.g. loaded from a DB row with NULL tags)
// must serialise to [] not null. NewProfile initialises Tags to [] —
// the nil path is only reachable via direct struct manipulation or a
// NULL DB column, so we force it here to exercise the toProfilerResponse
// nil-guard.
func TestToProfilerResponse_TagsNeverNull(t *testing.T) {
	t.Parallel()
	p, err := profiler.NewProfile(profGCID, profTenant, "bio", nil)
	if err != nil {
		t.Fatalf("NewProfile: %v", err)
	}
	p.Tags = nil // simulate a NULL-tags DB row

	store := inmem.NewProfilerRepo()
	_ = store.SaveProfile(context.Background(), p)
	h := httpadapter.NewHandler(httpadapter.Deps{
		Profiles: store,
	})
	req, rec := profilerRequest(http.MethodGet, "/v1/me/profile", "")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET status = %d; want 200", rec.Code)
	}
	// Assert the raw JSON has "tags":[] — NOT "tags":null.
	raw := rec.Body.String()
	if strings.Contains(raw, `"tags":null`) {
		t.Errorf("response contains \"tags\":null — Step 5 regression: %s", raw)
	}
	if !strings.Contains(raw, `"tags":[]`) {
		t.Errorf("response missing \"tags\":[] — got: %s", raw)
	}
}

// TestGenerateProfile_NotWired501 asserts the fail-loud contract: when
// the conjurer is not wired (INTEREST_PROFILER_GKE_ENDPOINT unset), the
// handler returns 501 rather than spawning a goroutine that can't
// complete.
func TestGenerateProfile_NotWired501(t *testing.T) {
	t.Parallel()
	// No Conjurer, no ProfileBroker — simulates a dev env without the
	// interest_profiler agent.
	handler := httpadapter.NewHandler(httpadapter.Deps{
		Profiles: inmem.NewProfilerRepo(),
	})
	req, rec := profilerRequest(http.MethodPost, "/v1/me/profile/generate",
		`{"bio":"I love astronomy","course_titles":[]}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d; want 501 (conjurer not wired)", rec.Code)
	}
}

// TestProfileWS_NotWired501 asserts the WS endpoint fails loud when
// the ProfileWSHandler is not wired (mirrors duelWS's 501 path).
func TestProfileWS_NotWired501(t *testing.T) {
	t.Parallel()
	handler := httpadapter.NewHandler(httpadapter.Deps{
		Profiles: inmem.NewProfilerRepo(),
	})
	req, rec := profilerRequest(http.MethodGet, "/v1/me/profile/ws", "")
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d; want 501 (ProfileWSHandler not wired)", rec.Code)
	}
}

// waitFor polls fn every 5ms until it returns true or the timeout elapses.
func waitFor(fn func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fn()
}
