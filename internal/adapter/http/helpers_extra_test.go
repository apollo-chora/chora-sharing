// handlers_helpers_extra_test.go — the pure HTTP helpers (writeJSON fallback,
// readJSON contract, parseLimit cap, license/reaction/metric mappers), the
// statusRecorder Hijack/Flush forwarding, and the remaining pubsub/milestone/
// listConnections misc branches. Every test reaches statements that the route
// tests cannot (e.g. the writeJSON marshal fallback).
package httpadapter

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/adapter/eventpush"
	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
	"github.com/apollo-chora/chora-sharing/internal/domain/reaction"
)

// ---------------------------------------------------------------------------
// writeJSON / readJSON / parseLimit
// ---------------------------------------------------------------------------

func TestWriteJSON_MarshalFallback(t *testing.T) {
	t.Parallel()
	rr := httptest.NewRecorder()
	// A channel cannot be JSON-marshalled — forces the fallback write.
	writeJSON(rr, http.StatusInternalServerError, make(chan int))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status=%d, want 500", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), `{"error":"internal"}`) {
		t.Errorf("fallback body = %q", rr.Body.String())
	}
}

func TestReadJSON_Contract(t *testing.T) {
	t.Parallel()
	rr := httptest.NewRecorder()

	// Empty body → 400 "request body required".
	req := httptest.NewRequest(http.MethodPost, "/x", nil)
	if readJSON(rr, req, &struct{}{}) {
		t.Error("nil body must be rejected")
	}
	if rr.Code != http.StatusBadRequest {
		t.Errorf("nil body: status=%d, want 400", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "request body required") {
		t.Errorf("nil body message: %q", rr.Body.String())
	}

	// Malformed → 400.
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{bad`))
	if readJSON(rr2, req2, &struct{}{}) {
		t.Error("malformed body must be rejected")
	}
	if rr2.Code != http.StatusBadRequest || !strings.Contains(rr2.Body.String(), "malformed JSON") {
		t.Errorf("malformed: code=%d body=%q", rr2.Code, rr2.Body.String())
	}

	// Valid → true.
	rr3 := httptest.NewRecorder()
	type payload struct{ A string `json:"a"` }
	var out payload
	req3 := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"a":"b"}`))
	if !readJSON(rr3, req3, &out) || out.A != "b" {
		t.Errorf("valid body rejected: %+v", out)
	}
}

func TestParseLimit_CapAndDefaults(t *testing.T) {
	t.Parallel()
	if got := parseLimit("500", 20, 100); got != 100 {
		t.Errorf("parseLimit(500) = %d, want 100 (hard cap)", got)
	}
	if got := parseLimit("banana", 20, 100); got != 20 {
		t.Errorf("parseLimit(banana) = %d, want default 20", got)
	}
	if got := parseLimit("0", 7, 100); got != 7 {
		t.Errorf("parseLimit(0) = %d, want default 7", got)
	}
	if got := parseLimit("42", 7, 100); got != 42 {
		t.Errorf("parseLimit(42) = %d, want 42", got)
	}
}

// ---------------------------------------------------------------------------
// Wire mappers (license / reaction kind / metric)
// ---------------------------------------------------------------------------

func TestLicenseStringToDomain_AllLabels(t *testing.T) {
	t.Parallel()
	cases := map[string]atom_share.LicenseTerms{
		"free":                atom_share.LicenseFree,
		"license_terms_free":  atom_share.LicenseFree,
		"LICENSE_TERMS_FREE":  atom_share.LicenseFree,
		"royalty_pct":         atom_share.LicenseRoyaltyPct,
		"license_terms_royalty_pct": atom_share.LicenseRoyaltyPct,
		"royalty_fixed":       atom_share.LicenseRoyaltyFixed,
		"license_terms_royalty_fixed": atom_share.LicenseRoyaltyFixed,
		"cc_by_sa":            atom_share.LicenseCCBySA,
		"license_terms_cc_by_sa": atom_share.LicenseCCBySA,
		"cc_nd":               atom_share.LicenseCCND,
		"license_terms_cc_nd": atom_share.LicenseCCND,
		"":                    "",
		"pirate":              "",
	}
	for in, want := range cases {
		if got := licenseStringToDomain(in); got != want {
			t.Errorf("licenseStringToDomain(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestReactionKindStringToDomain_AllLabels(t *testing.T) {
	t.Parallel()
	cases := map[string]reaction.Type{
		"like":                reaction.TypeLike,
		"reaction_kind_like":  reaction.TypeLike,
		"LIKE":                reaction.TypeLike,
		"inspired":            reaction.TypeInspired,
		"clap":                reaction.TypeInspired,
		"star":                reaction.TypeInspired,
		"reaction_kind_clap":  reaction.TypeInspired,
		"reaction_kind_star":  reaction.TypeInspired,
		"insightful":          reaction.TypeInsightful,
		"reaction_kind_insightful": reaction.TypeInsightful,
		"curious":             reaction.TypeCurious,
		"reaction_kind_curious": reaction.TypeCurious,
		"banana":              "",
	}
	for in, want := range cases {
		if got := reactionKindStringToDomain(in); got != want {
			t.Errorf("reactionKindStringToDomain(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestLeaderboardMetricFromQuery_AllMetrics(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"":            "xp",
		"xp":          "xp",
		"duel_wins":   "duel_wins",
		"DUEL_ELO":    "duel_elo",
		"reputation":  "reputation",
		"streak_days": "streak_days",
		"magic":       "xp",
	}
	for in, want := range cases {
		if got := leaderboardMetricFromQuery(in); got != want {
			t.Errorf("leaderboardMetricFromQuery(%q) = %q, want %q", in, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// statusRecorder Hijack / Flush
// ---------------------------------------------------------------------------

type fakeHijacker struct {
	http.ResponseWriter
	hijacked bool
}

func (f *fakeHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	f.hijacked = true
	return nil, nil, nil
}

type noFlusher struct{ http.ResponseWriter }

func TestStatusRecorder_HijackAndFlush(t *testing.T) {
	t.Parallel()
	// Hijack error path: wrapped writer is not a Hijacker.
	rec := httptest.NewRecorder()
	sr := &statusRecorder{ResponseWriter: rec, status: http.StatusOK}
	if _, _, err := sr.Hijack(); err == nil {
		t.Error("Hijack over a non-Hijacker must fail")
	}

	// Hijack success path: forwards to the underlying Hijacker.
	fh := &fakeHijacker{ResponseWriter: rec}
	sr2 := &statusRecorder{ResponseWriter: fh, status: http.StatusOK}
	if _, _, err := sr2.Hijack(); err != nil {
		t.Errorf("Hijack over a Hijacker must forward: %v", err)
	}
	if !fh.hijacked {
		t.Error("Hijack was not forwarded to the underlying writer")
	}

	// Flush success (recorder implements http.Flusher).
	sr3 := &statusRecorder{ResponseWriter: httptest.NewRecorder(), status: http.StatusOK}
	sr3.Flush() // must not panic

	// Flush no-op over a non-Flusher.
	sr4 := &statusRecorder{ResponseWriter: noFlusher{httptest.NewRecorder()}, status: http.StatusOK}
	sr4.Flush() // must not panic
}

func TestStatusRecorder_WriteBoundedBody(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	sr := &statusRecorder{ResponseWriter: rec, status: http.StatusOK}
	sr.WriteHeader(http.StatusInternalServerError)
	long := bytes.Repeat([]byte("x"), 10_000)
	_, _ = sr.Write(long)
	if len(sr.body) != maxLoggedFaultBody {
		t.Errorf("bounded body len=%d, want %d", len(sr.body), maxLoggedFaultBody)
	}
	// A non-5xx response must not be buffered.
	rec2 := httptest.NewRecorder()
	sr2 := &statusRecorder{ResponseWriter: rec2, status: http.StatusOK}
	_, _ = sr2.Write([]byte("fine"))
	if len(sr2.body) != 0 {
		t.Errorf("non-5xx body must not be retained, got %d bytes", len(sr2.body))
	}
}

// ---------------------------------------------------------------------------
// listConnections empty-list + display-name enrichment
// ---------------------------------------------------------------------------

func TestListConnections_EmptyAndProfiles(t *testing.T) {
	t.Parallel()
	graph := inmem.NewSocialGraph()
	h := NewHandler(Deps{Graph: graph, Profiles: inmem.NewProfilerRepo()})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, connReq(http.MethodGet, "/v1/connections?type=followers", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"connections":[]`) {
		t.Errorf("empty connections must render []: %s", rr.Body.String())
	}
}

// ---------------------------------------------------------------------------
// reactToPost: recount-fault after a successful react → 500
// ---------------------------------------------------------------------------

func TestReactToPost_RecountFault_500(t *testing.T) {
	t.Parallel()
	faulty := &faultyReactions{listErr: errors.New("db down"), failListOnCall: 1}
	h := NewHandler(Deps{Reactions: faulty, Posts: &faultyPosts{found: true, tenantID: socTenant}})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, socReq(http.MethodPost, "/v1/posts/"+socPost+"/reactions", `{"kind":"like"}`))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d, want 500 (recount fault)", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// course_published push handler — nil registry panic + busMessageFromPush
// ---------------------------------------------------------------------------

func TestNewCoursePublishedPushHandler_NilRegistry_Panics(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("nil Registry must panic at construction")
		}
	}()
	NewCoursePublishedPushHandler(CoursePublishedPushDeps{})
}

func TestBusMessageFromPush_TimestampParsing(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 7, 14, 9, 0, 0, 0, time.UTC)
	m := busMessageFromPush("t", eventpush.PushMessage{
		Attributes: map[string]string{
			"occurred_at":  ts.Format(time.RFC3339Nano),
			"published_at": ts.Format(time.RFC3339Nano),
		},
	})
	if m.Subject != "t" {
		t.Errorf("topic = %q", m.Subject)
	}
	if !m.Envelope.OccurredAt.Equal(ts) || !m.Envelope.PublishedAt.Equal(ts) {
		t.Errorf("timestamps not parsed: %+v", m.Envelope)
	}

	// Unparseable timestamps → zero times, no error.
	m2 := busMessageFromPush("t", eventpush.PushMessage{
		Attributes: map[string]string{
			"occurred_at":  "nope",
			"published_at": "nope",
		},
	})
	if !m2.Envelope.OccurredAt.IsZero() || !m2.Envelope.PublishedAt.IsZero() {
		t.Errorf("bad timestamps must stay zero: %+v", m2.Envelope)
	}
}