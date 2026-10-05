package httpadapter

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/bookmark"
)

// failingBookmarks implements BookmarkStore and fails the way the live
// chora_sharing DB failed in CHO-2177: migration 0008 was never applied, so
// every bookmark read came back as a Postgres undefined-table error.
type failingBookmarks struct{ err error }

func (f *failingBookmarks) Save(_ context.Context, _ *bookmark.Bookmark) error { return f.err }

func (f *failingBookmarks) Delete(_ context.Context, _, _, _ string) error { return f.err }

func (f *failingBookmarks) ListByOwner(_ context.Context, _, _, _ string, _ int) ([]bookmark.Bookmark, string, error) {
	return nil, "", f.err
}

// captureLogs redirects the stdlib log sink for the duration of the test.
// Tests that use it must not call t.Parallel — the sink is global.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	return &buf
}

// TestServerError_IsLogged pins the fail-loud invariant: a 5xx MUST leave a
// trace in the service's own logs, carrying enough detail to triage it.
//
// Before this, the error text existed ONLY in the response body — and the BFF
// discards that body when it normalises 500 -> 502. The sole copy of the error
// was destroyed in transit, so C+ bookmarks was totally dead for 15 days
// without emitting a single log line (CHO-2177).
func TestServerError_IsLogged(t *testing.T) {
	logs := captureLogs(t)

	dbErr := errors.New(`ERROR: relation "atom_bookmarks" does not exist (SQLSTATE 42P01)`)
	h := NewHandler(Deps{Bookmarks: &failingBookmarks{err: dbErr}})

	req := httptest.NewRequest("GET", "/v1/me/bookmarks", nil)
	req.Header.Set("gcid", "11111111-1111-7111-8111-111111111111")
	req.Header.Set("X-Tenant-Id", "11111111-1111-7111-8111-111111111111")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (body: %s)", rr.Code, rr.Body.String())
	}

	got := logs.String()
	if got == "" {
		t.Fatal("the 5xx produced NO log line — the failure is invisible to operators")
	}
	// The log must identify the route, the status, and the underlying cause;
	// a bare "request failed" would be just as untriageable as silence.
	for _, want := range []string{"GET", "/v1/me/bookmarks", "500", "atom_bookmarks", "42P01"} {
		if !strings.Contains(got, want) {
			t.Errorf("log line is missing %q — got: %s", want, got)
		}
	}
}

// TestNotImplemented_IsLogged — a 501 means a port was never wired. That is a
// deployment fault, not a client error, and must be just as loud.
func TestNotImplemented_IsLogged(t *testing.T) {
	logs := captureLogs(t)

	h := NewHandler(Deps{}) // no Bookmarks port

	req := httptest.NewRequest("GET", "/v1/me/bookmarks", nil)
	req.Header.Set("gcid", "11111111-1111-7111-8111-111111111111")
	req.Header.Set("X-Tenant-Id", "11111111-1111-7111-8111-111111111111")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", rr.Code)
	}
	if !strings.Contains(logs.String(), "501") {
		t.Errorf("501 was not logged — an unwired port would ship silently; got: %q", logs.String())
	}
}

// TestSuccess_IsNotLogged keeps the chokepoint a FAULT log, not a request log.
// Health probes hit /healthz continuously; logging them would bury the 5xx
// this whole mechanism exists to surface.
func TestSuccess_IsNotLogged(t *testing.T) {
	logs := captureLogs(t)

	h := NewHandler(testDeps())

	req := httptest.NewRequest("GET", "/healthz", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if logs.String() != "" {
		t.Errorf("a 200 emitted a fault log: %q", logs.String())
	}
}
