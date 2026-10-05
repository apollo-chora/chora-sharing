package ws

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/websocket"
)

// stubLookup returns a fixed snapshot so the handler always sends one on
// connect — enough to exercise the wire path.
type stubLookup struct {
	snap *DuelSnapshot
}

func (s stubLookup) GetDuel(_ context.Context, _ string, _ string) (*DuelSnapshot, bool) {
	if s.snap == nil {
		return nil, false
	}
	return s.snap, true
}

// stubAnswers returns a fixed round-start frame so the handler sends a
// round_start frame on connect (the path that carries Question/Options).
type stubAnswers struct {
	round RoundStartFrame
}

func (s stubAnswers) SubmitAnswer(_ context.Context, _ string, _ string, _ int32, _ string, _ int64) (RoundResolvedFrame, bool, error) {
	return RoundResolvedFrame{}, false, nil
}

func (s stubAnswers) GetCurrentRound(_ context.Context, _ string) (RoundStartFrame, bool) {
	return s.round, true
}

func (s stubAnswers) SubmitBlitzAnswer(_ context.Context, _ string, _ string, _ int32, _ string, _ int64) (BlitzAnswerResolvedFrame, bool, error) {
	return BlitzAnswerResolvedFrame{}, false, nil
}

func (s stubAnswers) GetBlitzStart(_ context.Context, _ string) (BlitzStartFrame, bool) {
	return BlitzStartFrame{}, false
}

// TestHandler_WireFrame_PayloadIsJSONObjectNotBase64 is the regression
// test for the "Duel Arena 0/0, no atom" blocker: Message.Payload was
// `[]byte`, which encoding/json marshals as a base64-encoded string. The
// FE reads `msg.payload.total_rounds` / `msg.payload.round_no` /
// `msg.payload.question` — all undefined when payload is a string, so the
// arena stayed at 0/0 with no question rendered.
//
// With json.RawMessage the frame serializes as nested JSON and the FE
// decodes the object fields. This test drives the handler over a real
// websocket connection and asserts the payload arrives as a JSON object.
func TestHandler_WireFrame_PayloadIsJSONObjectNotBase64(t *testing.T) {
	snap := &DuelSnapshot{
		DuelID:          "duel-1",
		Status:          "in_progress",
		CurrentRound:    1,
		TotalRounds:     1,
		ScoreChallenger: 0,
		ScoreOpponent:   0,
		Rounds: []SnapshotRound{{
			RoundNo:            1,
			ChallengerAnswered: false,
			OpponentAnswered:   false,
		}},
	}
	round := RoundStartFrame{
		RoundNo:  1,
		AtomID:   "atom-1",
		Question: "Which planet has the most known moons?",
		Options:  []string{"Mars", "Jupiter", "Saturn", "Neptune"},
		TimerSec: 30,
	}

	broker := NewBroker()
	h := NewHandler(broker, stubLookup{snap: snap}, stubAnswers{round: round})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/duels/{duel_id}/ws", h.ServeHTTP)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/duels/duel-1/ws"
	cfg, err := websocket.NewConfig(wsURL, "http://localhost/")
	if err != nil {
		t.Fatalf("ws config: %v", err)
	}
	cfg.Header.Set("X-Tenant-Id", "tenant-1")
	cfg.Header.Set("gcid", "user-1")

	conn, err := websocket.DialConfig(cfg)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close()

	// The handler sends two frames on connect: snapshot, then round_start.
	// Read both within a deadline and assert each payload is a JSON object
	// (not a base64 string). A base64 string would fail to unmarshal into
	// a map[string]any — that is the regression signal.
	deadline := time.Now().Add(3 * time.Second)
	frames := map[string]map[string]any{}
	for len(frames) < 2 && time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		var raw map[string]json.RawMessage
		if err := websocket.JSON.Receive(conn, &raw); err != nil {
			break
		}
		var kind string
		if err := json.Unmarshal(raw["kind"], &kind); err != nil {
			t.Fatalf("could not unmarshal frame kind: %v", err)
		}
		payloadBytes := raw["payload"]
		// The critical assertion: payload must be a JSON object, not a
		// base64 string. json.Unmarshal into map[string]any succeeds only
		// for a JSON object; a base64 string fails with a type error.
		var obj map[string]any
		if err := json.Unmarshal(payloadBytes, &obj); err != nil {
			t.Fatalf("kind=%s payload did not unmarshal as JSON object: %v (raw=%q) — this is the base64-string bug: Message.Payload was []byte", kind, err, string(payloadBytes))
		}
		frames[kind] = obj
	}

	if len(frames) == 0 {
		t.Fatal("no frames received over websocket within deadline")
	}

	snapFrame, ok := frames["snapshot"]
	if !ok {
		t.Fatalf("snapshot frame not received; got kinds=%v", frameKinds(frames))
	}
	// total_rounds lives on the snapshot frame — the FE reads
	// snapshot.total_rounds. Before the fix this was undefined (payload
	// was base64), leaving totalRounds at 0 → "0/0".
	totalRounds, _ := snapFrame["total_rounds"].(float64)
	if totalRounds != 1 {
		t.Errorf("snapshot.total_rounds = %v, want 1 (the bug: payload was base64 → undefined → 0/0)", snapFrame["total_rounds"])
	}

	roundFrame, ok := frames["round_start"]
	if !ok {
		t.Fatalf("round_start frame not received; got kinds=%v", frameKinds(frames))
	}
	// round_no + question live on the round_start frame. Before the fix
	// these were undefined, so the arena never rendered the question.
	roundNo, _ := roundFrame["round_no"].(float64)
	if roundNo != 1 {
		t.Errorf("round_start.round_no = %v, want 1", roundFrame["round_no"])
	}
	question, _ := roundFrame["question"].(string)
	if question == "" {
		t.Errorf("round_start.question = %q, want non-empty (the bug: empty question rendered as no atom)", question)
	}
	opts, _ := roundFrame["options"].([]any)
	if len(opts) != 4 {
		t.Errorf("round_start.options len = %d, want 4", len(opts))
	}
}

func frameKinds(frames map[string]map[string]any) []string {
	kinds := make([]string, 0, len(frames))
	for k := range frames {
		kinds = append(kinds, k)
	}
	return kinds
}

// TestHandler_AppliesConfiguredWriteDeadline pins WS0 Bug 3: the
// WebSocketGrace config (SharingRules.WebSocketGrace) MUST be threaded
// into the WS handler so the write loop sets a per-write deadline. A
// stuck client that stops reading must be dropped after the grace
// period, not hold a goroutine forever. The Handler exposes its
// configured grace so the bootstrap layer + tests can verify wiring
// without timing out a real connection.
func TestHandler_AppliesConfiguredWriteDeadline(t *testing.T) {
	broker := NewBroker()
	snap := &DuelSnapshot{DuelID: "duel-grace", Status: "in_progress", TotalRounds: 1}
	round := RoundStartFrame{RoundNo: 1, Question: "Q", Options: []string{"A", "B"}, TimerSec: 30}

	h := NewHandler(broker, stubLookup{snap: snap}, stubAnswers{round: round},
		WithWriteGrace(5*time.Second),
	)
	if got := h.WriteGrace(); got != 5*time.Second {
		t.Fatalf("WriteGrace() = %v, want 5s (WebSocketGrace config must be wired, not unused)", got)
	}
}

// TestHandler_DefaultWriteGraceIsZero pins the zero-value default: a
// handler constructed without WithWriteGrace reports 0 (no deadline) so
// the bootstrap layer can detect a missing config wire-up loudly.
func TestHandler_DefaultWriteGraceIsZero(t *testing.T) {
	broker := NewBroker()
	snap := &DuelSnapshot{DuelID: "duel-default", Status: "in_progress", TotalRounds: 1}
	h := NewHandler(broker, stubLookup{snap: snap}, stubAnswers{})
	if got := h.WriteGrace(); got != 0 {
		t.Fatalf("WriteGrace() = %v, want 0 (default before WithWriteGrace wiring)", got)
	}
}
