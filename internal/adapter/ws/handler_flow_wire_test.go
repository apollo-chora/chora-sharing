package ws

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

	"golang.org/x/net/websocket"
)

// --- scripted answers stub ---

type blitzStartResult struct {
	frame BlitzStartFrame
	ok    bool
}

type submitBlitzResult struct {
	frame     BlitzAnswerResolvedFrame
	completed bool
	err       error
}

type submitResult struct {
	frame     RoundResolvedFrame
	completed bool
	err       error
}

type roundStartResult struct {
	frame RoundStartFrame
	ok    bool
}

// scriptedAnswers is a call-ordered AnswerProcessor double. Result queues
// are consumed in order; an exhausted queue returns the zero/false value.
// blitzStartAlways makes GetBlitzStart answer ok=true forever (blitz duels
// re-check the mode on every inbound frame).
type scriptedAnswers struct {
	mu              sync.Mutex
	calls           []string
	blitzStart      []blitzStartResult
	blitzStartAlways bool
	blitzFrame      BlitzStartFrame
	submitBlitz     []submitBlitzResult
	submit          []submitResult
	roundStart      []roundStartResult
}

func popBlitzStart(q []blitzStartResult) (blitzStartResult, bool) {
	if len(q) == 0 {
		return blitzStartResult{}, false
	}
	return q[0], true
}

func (s *scriptedAnswers) GetBlitzStart(context.Context, string) (BlitzStartFrame, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "GetBlitzStart")
	if s.blitzStartAlways {
		return s.blitzFrame, true
	}
	if r, ok := popBlitzStart(s.blitzStart); ok {
		s.blitzStart = s.blitzStart[1:]
		return r.frame, r.ok
	}
	return BlitzStartFrame{}, false
}

func (s *scriptedAnswers) GetCurrentRound(context.Context, string) (RoundStartFrame, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "GetCurrentRound")
	if len(s.roundStart) == 0 {
		return RoundStartFrame{}, false
	}
	r := s.roundStart[0]
	s.roundStart = s.roundStart[1:]
	return r.frame, r.ok
}

func (s *scriptedAnswers) SubmitAnswer(context.Context, string, string, int32, string, int64) (RoundResolvedFrame, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "SubmitAnswer")
	if len(s.submit) == 0 {
		return RoundResolvedFrame{}, false, nil
	}
	r := s.submit[0]
	s.submit = s.submit[1:]
	return r.frame, r.completed, r.err
}

func (s *scriptedAnswers) SubmitBlitzAnswer(context.Context, string, string, int32, string, int64) (BlitzAnswerResolvedFrame, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "SubmitBlitzAnswer")
	if len(s.submitBlitz) == 0 {
		return BlitzAnswerResolvedFrame{}, false, nil
	}
	r := s.submitBlitz[0]
	s.submitBlitz = s.submitBlitz[1:]
	return r.frame, r.completed, r.err
}

// --- wire helpers (mirror handler_wire_test.go) ---

func newDuelWSServer(t *testing.T, h *Handler) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/duels/{duel_id}/ws", h.ServeHTTP)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func dialDuelWS(t *testing.T, srv *httptest.Server, duelID, tenant, gcid string) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/duels/" + duelID + "/ws"
	cfg, err := websocket.NewConfig(wsURL, "http://localhost/")
	if err != nil {
		t.Fatalf("ws config: %v", err)
	}
	cfg.Header.Set("X-Tenant-Id", tenant)
	cfg.Header.Set("gcid", gcid)
	conn, err := websocket.DialConfig(cfg)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

type wireFrame struct {
	kind    MessageKind
	payload map[string]any
}

// readWireFrame decodes one envelope frame. Payload is decoded as a JSON
// object (the invariant the package guarantees for all frame kinds).
func readWireFrame(t *testing.T, conn *websocket.Conn, timeout time.Duration) (wireFrame, error) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	var raw map[string]json.RawMessage
	if err := websocket.JSON.Receive(conn, &raw); err != nil {
		return wireFrame{}, err
	}
	var kind string
	if err := json.Unmarshal(raw["kind"], &kind); err != nil {
		return wireFrame{}, err
	}
	var payload map[string]any
	if err := json.Unmarshal(raw["payload"], &payload); err != nil {
		return wireFrame{}, err
	}
	return wireFrame{kind: MessageKind(kind), payload: payload}, nil
}

// readUntilKinds reads frames until every wanted kind has been seen (or the
// deadline expires) and returns the seen-kind set.
func readUntilKinds(t *testing.T, conn *websocket.Conn, want map[MessageKind]bool) map[MessageKind]bool {
	t.Helper()
	seen := map[MessageKind]bool{}
	deadline := time.Now().Add(3 * time.Second)
	for !hasAll(seen, want) && time.Now().Before(deadline) {
		frame, err := readWireFrame(t, conn, 2*time.Second)
		if err != nil {
			break
		}
		seen[frame.kind] = true
	}
	return seen
}

func hasAll(seen, want map[MessageKind]bool) bool {
	for k := range want {
		if !seen[k] {
			return false
		}
	}
	return true
}

// --- ServeHTTP branches ---

// TestHandler_WithLogger pins the WithLogger option: the custom logger is
// wired into the handler.
func TestHandler_WithLogger(t *testing.T) {
	logged := false
	h := NewHandler(NewBroker(), nil, nil,
		WithLogger(func(string, ...any) { logged = true }),
	)
	if h.logf == nil {
		t.Fatal("WithLogger did not wire logf")
	}
	// The connect-time snapshot send happens only over a real conn, so just
	// exercise the option setter + zero-value handler (no deps → 503).
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/duels/x/ws", nil))
	if got := rec.Code; got != http.StatusServiceUnavailable {
		t.Errorf("ServeHTTP without deps: status=%d want 503", got)
	}
	_ = logged
}

// TestHandler_ServeHTTP_MissingHeaders pins the identity guard: connections
// without X-Tenant-Id / gcid headers are rejected with 401.
func TestHandler_ServeHTTP_MissingHeaders(t *testing.T) {
	h := NewHandler(NewBroker(), stubLookup{snap: &DuelSnapshot{}}, stubAnswers{})
	req := httptest.NewRequest(http.MethodGet, "/v1/duels/x/ws", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got := rec.Code; got != http.StatusUnauthorized {
		t.Errorf("status=%d want 401", got)
	}
}

// TestHandler_ServeHTTP_MissingDuelID pins the path-param guard: a request
// without a duel_id (no route match) is rejected with 400.
func TestHandler_ServeHTTP_MissingDuelID(t *testing.T) {
	h := NewHandler(NewBroker(), stubLookup{snap: &DuelSnapshot{}}, stubAnswers{})
	req := httptest.NewRequest(http.MethodGet, "/v1/duels//ws", nil)
	req.Header.Set("X-Tenant-Id", "tenant-1")
	req.Header.Set("gcid", "user-1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got := rec.Code; got != http.StatusBadRequest {
		t.Errorf("status=%d want 400", got)
	}
}

// TestHandler_ServeHTTP_DuelNotFound pins the lookup guard: a duel the
// requesting tenant cannot see is rejected with 404.
func TestHandler_ServeHTTP_DuelNotFound(t *testing.T) {
	h := NewHandler(NewBroker(), stubLookup{snap: nil}, stubAnswers{})
	req := httptest.NewRequest(http.MethodGet, "/v1/duels/x/ws", nil)
	req.Header.Set("X-Tenant-Id", "tenant-1")
	req.Header.Set("gcid", "user-1")
	req.SetPathValue("duel_id", "x")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got := rec.Code; got != http.StatusNotFound {
		t.Errorf("status=%d want 404", got)
	}
}

// --- maybeSendCurrentRound (blitz on connect) ---

// TestHandler_BlitzStartOnConnect pins the blitz connect flow: instead of a
// round_start, a blitz duel receives ALL questions in a single blitz_start
// frame right after the snapshot.
func TestHandler_BlitzStartOnConnect(t *testing.T) {
	snap := &DuelSnapshot{DuelID: "duel-b", Status: "in_progress", TotalRounds: 2}
	answers := &scriptedAnswers{
		blitzStartAlways:  true,
		blitzFrame:        BlitzStartFrame{Mode: "blitz", BlitzVariant: "timed", TimeLimitSec: 120, ServerNow: "now"},
	}

	h := NewHandler(NewBroker(), stubLookup{snap: snap}, answers)
	srv := newDuelWSServer(t, h)
	conn := dialDuelWS(t, srv, "duel-b", "tenant-1", "user-1")

	seen := readUntilKinds(t, conn, map[MessageKind]bool{KindSnapshot: true, KindBlitzStart: true})
	if !seen[KindSnapshot] {
		t.Error("snapshot frame missing on connect")
	}
	if !seen[KindBlitzStart] {
		t.Error("blitz_start frame missing on connect (blitz duel must deliver all questions at once)")
	}
}

// --- readLoop: classic answer flow ---

// TestHandler_ReadLoop_ClassicAnswerAdvancesRound pins the classic round
// life-cycle: an answer resolves the round (round_resolved), and the next
// round_start is auto-pushed (multi-round advance via the broker fan-out).
func TestHandler_ReadLoop_ClassicAnswerAdvancesRound(t *testing.T) {
	snap := &DuelSnapshot{DuelID: "duel-1", Status: "in_progress", TotalRounds: 2}
	answers := &scriptedAnswers{
		roundStart: []roundStartResult{
			{frame: RoundStartFrame{RoundNo: 1, Question: "Q1"}, ok: true},
			{frame: RoundStartFrame{RoundNo: 2, Question: "Q2"}, ok: true},
		},
		submit: []submitResult{
			{frame: RoundResolvedFrame{RoundNo: 1, GCID: "user-1", Correct: true, DuelStatus: "in_progress"}, completed: false},
		},
	}

	h := NewHandler(NewBroker(), stubLookup{snap: snap}, answers,
		WithWriteGrace(2*time.Second),
	)
	srv := newDuelWSServer(t, h)
	conn := dialDuelWS(t, srv, "duel-1", "tenant-1", "user-1")

	seen := readUntilKinds(t, conn, map[MessageKind]bool{KindSnapshot: true, KindRoundStart: true})
	if !seen[KindSnapshot] || !seen[KindRoundStart] {
		t.Fatalf("connect frames missing: snapshot=%v round_start=%v", seen[KindSnapshot], seen[KindRoundStart])
	}

	if err := websocket.JSON.Send(conn, InboundAnswerFrame{RoundNo: 1, Answer: "A", AnswerTimeMs: 5}); err != nil {
		t.Fatalf("send answer: %v", err)
	}

	seen = readUntilKinds(t, conn, map[MessageKind]bool{KindRoundResolved: true})
	if !seen[KindRoundResolved] {
		t.Error("round_resolved frame missing after answer")
	}

	// The last round_start must be for round 2 (the auto-advance).
	frame, err := readWireFrame(t, conn, time.Second)
	if err != nil {
		t.Fatalf("expected the advance round_start: %v", err)
	}
	if frame.kind != KindRoundStart || int(frame.payload["round_no"].(float64)) != 2 {
		t.Errorf("advance frame kind=%s round_no=%v want round_start for round 2", frame.kind, frame.payload["round_no"])
	}
}

// TestHandler_ReadLoop_NoAdvanceOnSameRound pins the FCFS guard: a wrong
// answer that does NOT advance the round must not re-publish round_start
// (that would flood both clients with a duplicate frame).
func TestHandler_ReadLoop_NoAdvanceOnSameRound(t *testing.T) {
	snap := &DuelSnapshot{DuelID: "duel-1", Status: "in_progress", TotalRounds: 1}
	answers := &scriptedAnswers{
		roundStart: []roundStartResult{
			{frame: RoundStartFrame{RoundNo: 1, Question: "Q1"}, ok: true},
			// After the wrong answer: still round 1 (round stays open for the
			// opponent to steal).
			{frame: RoundStartFrame{RoundNo: 1, Question: "Q1"}, ok: true},
		},
		submit: []submitResult{
			{frame: RoundResolvedFrame{RoundNo: 1, GCID: "user-1", Correct: false, DuelStatus: "in_progress"}, completed: false},
		},
	}

	h := NewHandler(NewBroker(), stubLookup{snap: snap}, answers)
	srv := newDuelWSServer(t, h)
	conn := dialDuelWS(t, srv, "duel-1", "tenant-1", "user-1")

	// Consume the connect frames (snapshot + round_start).
	seen := readUntilKinds(t, conn, map[MessageKind]bool{KindSnapshot: true, KindRoundStart: true})
	if !seen[KindSnapshot] || !seen[KindRoundStart] {
		t.Fatalf("connect frames missing")
	}

	if err := websocket.JSON.Send(conn, InboundAnswerFrame{RoundNo: 1, Answer: "wrong", AnswerTimeMs: 5}); err != nil {
		t.Fatalf("send answer: %v", err)
	}

	seen = readUntilKinds(t, conn, map[MessageKind]bool{KindRoundResolved: true})
	if !seen[KindRoundResolved] {
		t.Fatal("round_resolved frame missing")
	}

	// No advance: a duplicate round_start must NOT arrive.
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	var probe map[string]json.RawMessage
	if err := websocket.JSON.Receive(conn, &probe); err == nil {
		t.Errorf("unexpected extra frame after non-advancing answer: %s", probe["kind"])
	}
}

// TestHandler_ReadLoop_SubmitErrorPublishesErrorFrame pins the error path:
// a failed Submission publishes an error frame to both participants.
func TestHandler_ReadLoop_SubmitErrorPublishesErrorFrame(t *testing.T) {
	snap := &DuelSnapshot{DuelID: "duel-1", Status: "in_progress", TotalRounds: 1}
	answers := &scriptedAnswers{
		roundStart: []roundStartResult{
			{frame: RoundStartFrame{RoundNo: 1, Question: "Q1"}, ok: true},
		},
		submit: []submitResult{
			{err: errors.New("duel not found")},
		},
	}

	h := NewHandler(NewBroker(), stubLookup{snap: snap}, answers)
	srv := newDuelWSServer(t, h)
	conn := dialDuelWS(t, srv, "duel-1", "tenant-1", "user-1")

	seen := readUntilKinds(t, conn, map[MessageKind]bool{KindSnapshot: true, KindRoundStart: true})
	if !seen[KindSnapshot] || !seen[KindRoundStart] {
		t.Fatalf("connect frames missing")
	}

	if err := websocket.JSON.Send(conn, InboundAnswerFrame{RoundNo: 1, Answer: "A", AnswerTimeMs: 5}); err != nil {
		t.Fatalf("send answer: %v", err)
	}

	seen = readUntilKinds(t, conn, map[MessageKind]bool{KindError: true})
	if !seen[KindError] {
		t.Fatal("error frame missing after a failed submission")
	}
}

// TestHandler_ReadLoop_ClassicCompleted pins the completion fan-out: when
// the final answer completes the duel, both a round_resolved and a
// duel_completed frame are published.
func TestHandler_ReadLoop_ClassicCompleted(t *testing.T) {
	snap := &DuelSnapshot{DuelID: "duel-1", Status: "in_progress", TotalRounds: 1}
	answers := &scriptedAnswers{
		roundStart: []roundStartResult{
			{frame: RoundStartFrame{RoundNo: 1, Question: "Q1"}, ok: true},
		},
		submit: []submitResult{
			{frame: RoundResolvedFrame{RoundNo: 1, GCID: "user-1", Correct: true, DuelStatus: "completed", WinnerGCID: "user-1"}, completed: true},
		},
	}

	h := NewHandler(NewBroker(), stubLookup{snap: snap}, answers)
	srv := newDuelWSServer(t, h)
	conn := dialDuelWS(t, srv, "duel-1", "tenant-1", "user-1")

	seen := readUntilKinds(t, conn, map[MessageKind]bool{KindSnapshot: true, KindRoundStart: true})

	if err := websocket.JSON.Send(conn, InboundAnswerFrame{RoundNo: 1, Answer: "A", AnswerTimeMs: 5}); err != nil {
		t.Fatalf("send answer: %v", err)
	}

	seen = readUntilKinds(t, conn, map[MessageKind]bool{KindRoundResolved: true, KindDuelCompleted: true})
	if !seen[KindRoundResolved] {
		t.Error("round_resolved frame missing")
	}
	if !seen[KindDuelCompleted] {
		t.Error("duel_completed frame missing after the completing answer")
	}
	// A round_start must never arrive after completion.
	_ = conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	var probe map[string]json.RawMessage
	if err := websocket.JSON.Receive(conn, &probe); err == nil {
		t.Errorf("unexpected frame after completion: %s", probe["kind"])
	}
}

// --- readLoop: blitz flow ---

// TestHandler_ReadLoop_BlitzDispatch pins the blitz routing: answers are
// submitted via SubmitBlitzAnswer and resolved frames are published as
// blitz_answer_resolved; a completed blitz answer also emits duel_completed;
// a failed blitz answer emits an error frame.
func TestHandler_ReadLoop_BlitzDispatch(t *testing.T) {
	snap := &DuelSnapshot{DuelID: "duel-b", Status: "in_progress", TotalRounds: 2}
	answers := &scriptedAnswers{
		blitzStartAlways: true,
		blitzFrame:       BlitzStartFrame{Mode: "blitz", BlitzVariant: "timed", TimeLimitSec: 120},
		submitBlitz: []submitBlitzResult{
			{frame: BlitzAnswerResolvedFrame{RoundNo: 1, GCID: "user-1", Correct: true, DuelStatus: "in_progress"}, completed: false},
			{frame: BlitzAnswerResolvedFrame{RoundNo: 1, GCID: "user-1", Correct: true, DuelStatus: "completed"}, completed: true},
			{err: errors.New("duel not found")},
		},
	}

	h := NewHandler(NewBroker(), stubLookup{snap: snap}, answers)
	srv := newDuelWSServer(t, h)
	conn := dialDuelWS(t, srv, "duel-b", "tenant-1", "user-1")

	if seen := readUntilKinds(t, conn, map[MessageKind]bool{KindSnapshot: true, KindBlitzStart: true}); !seen[KindBlitzStart] {
		t.Fatal("blitz_start frame missing on connect")
	}

	// Answer 1: resolves nothing yet (round stays open for the opponent) →
	// blitz_answer_resolved only.
	sendAnswer := func(round int32) {
		if err := websocket.JSON.Send(conn, InboundAnswerFrame{RoundNo: round, Answer: "A", AnswerTimeMs: 5}); err != nil {
			t.Fatalf("send answer: %v", err)
		}
	}
	sendAnswer(1)
	if seen := readUntilKinds(t, conn, map[MessageKind]bool{KindBlitzAnswerResolved: true}); !seen[KindBlitzAnswerResolved] {
		t.Fatal("blitz_answer_resolved frame missing after answer 1")
	}

	// Answer 2: completes the duel → blitz_answer_resolved + duel_completed.
	sendAnswer(1)
	seen := readUntilKinds(t, conn, map[MessageKind]bool{KindBlitzAnswerResolved: true, KindDuelCompleted: true})
	if !seen[KindBlitzAnswerResolved] {
		t.Error("blitz_answer_resolved frame missing after the completing answer")
	}
	if !seen[KindDuelCompleted] {
		t.Error("duel_completed frame missing after the completing blitz answer")
	}

	// Answer 3: submission fails → error frame, and the loop keeps running.
	sendAnswer(1)
	if seen := readUntilKinds(t, conn, map[MessageKind]bool{KindError: true}); !seen[KindError] {
		t.Fatal("error frame missing after a failed blitz answer")
	}
}

// --- readLoop: abnormal read ---

// TestHandler_ReadLoop_AbnormalReadIsLogged pins the fail-loud read path: a
// non-close Receive error (garbage frame) is logged and ends the loop.
func TestHandler_ReadLoop_AbnormalReadIsLogged(t *testing.T) {
	srv := httptest.NewServer(websocket.Handler(func(c *websocket.Conn) {
		// One garbage (non-JSON) frame, then close.
		if _, err := c.Write([]byte("{{{{ not json")); err != nil {
			return
		}
		c.Close()
	}))
	defer srv.Close()

	conn := dialDuelWS(t, srv, "unused", "tenant-1", "user-1")

	var logged string
	h := &Handler{logf: func(format string, args ...any) { logged = format }}
	done := make(chan struct{})
	h.readLoop(context.Background(), conn, nil, "duel-1", "user-1", done)
	<-done
	if !strings.Contains(logged, "read error") {
		t.Errorf("read error not logged (got %q)", logged)
	}
}

// --- writeLoop branches ---

// TestHandler_WriteLoop_ClosedChannel pins the write-loop exit: when the
// broker channel closes (cleanup), the loop returns.
func TestHandler_WriteLoop_ClosedChannel(t *testing.T) {
	ch := make(chan Message)
	close(ch)
	h := &Handler{}
	h.writeLoop(nil, ch, "duel-1", "user-1", nil)
}

// TestHandler_WriteLoop_DoneClosed pins the write-loop exit: when the read
// loop signals done, the loop returns even with no messages in flight.
func TestHandler_WriteLoop_DoneClosed(t *testing.T) {
	ch := make(chan Message)
	done := make(chan struct{})
	close(done)
	h := &Handler{}
	h.writeLoop(nil, ch, "duel-1", "user-1", done)
}

// TestHandler_WriteLoop_WriteDeadlineDropsStuckClient pins the WS0 Bug 3
// fix end-to-end: a client that never reads must be dropped after the
// configured write grace instead of holding the write goroutine forever.
func TestHandler_WriteLoop_WriteDeadlineDropsStuckClient(t *testing.T) {
	// Peer holds the connection open but NEVER reads (a stuck client).
	hold := make(chan struct{})
	srv := httptest.NewServer(websocket.Handler(func(c *websocket.Conn) {
		<-hold
		c.Close()
	}))
	defer func() { close(hold); srv.Close() }()

	conn := dialDuelWS(t, srv, "unused", "tenant-1", "user-1")

	// Pre-load the channel with far more data than the socket buffer can
	// hold, so the writer must block and hit the write deadline.
	bigPayload := json.RawMessage(`"` + strings.Repeat("x", 256*1024) + `"`)
	ch := make(chan Message, 64)
	for i := 0; i < 64; i++ {
		ch <- Message{DuelID: "duel-1", Kind: KindError, Payload: bigPayload}
	}

	var logged string
	h := &Handler{writeGrace: 100 * time.Millisecond, logf: func(format string, args ...any) { logged = format }}

	start := time.Now()
	h.writeLoop(conn, ch, "duel-1", "user-1", make(chan struct{}))
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("writeLoop took %v — the grace deadline did not drop the stuck client", elapsed)
	}
	if !strings.Contains(logged, "write failed") {
		t.Errorf("write failure not logged (got %q)", logged)
	}
}