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

// TestProfileHandler_WithLogger pins the WithProfileLogger option: the
// custom logger is wired into the handler.
func TestProfileHandler_WithLogger(t *testing.T) {
	h := NewProfileHandler(NewProfileBroker(), WithProfileLogger(func(string, ...any) {}))
	if h.logf == nil {
		t.Fatal("NewProfileHandler with WithProfileLogger did not wire logf")
	}
	// Default constructor must fall back to log.Printf (non-nil).
	dflt := NewProfileHandler(NewProfileBroker())
	if dflt.logf == nil {
		t.Fatal("NewProfileHandler default logf must be non-nil")
	}
}

// TestProfileHandler_ServeHTTP_NoBroker pins the bootstrap guard: a handler
// without a broker returns 503.
func TestProfileHandler_ServeHTTP_NoBroker(t *testing.T) {
	h := NewProfileHandler(nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/me/profile/ws", nil))
	if got := rec.Code; got != http.StatusServiceUnavailable {
		t.Errorf("status=%d want 503", got)
	}
}

// TestProfileHandler_ServeHTTP_MissingHeaders pins the identity guard: the
// profile WS requires the gateway-stamped gcid + tenant headers.
func TestProfileHandler_ServeHTTP_MissingHeaders(t *testing.T) {
	h := NewProfileHandler(NewProfileBroker())
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/me/profile/ws", nil)
	req.Header.Set("X-Tenant-Id", "tenant-1")
	h.ServeHTTP(rec, req)
	if got := rec.Code; got != http.StatusUnauthorized {
		t.Errorf("status=%d want 401 (missing gcid)", got)
	}
}

// TestProfileHandler_FullWireFlow pins the receive-only life-cycle: the
// client connects, receives published profile frames via the write loop,
// and the loops shut down cleanly when the client closes the connection.
func TestProfileHandler_FullWireFlow(t *testing.T) {
	broker := NewProfileBroker()
	h := NewProfileHandler(broker)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/me/profile/ws", h.ServeHTTP)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/me/profile/ws"
	cfg, err := websocket.NewConfig(wsURL, "http://localhost/")
	if err != nil {
		t.Fatalf("ws config: %v", err)
	}
	cfg.Header.Set("X-Tenant-Id", "tenant-1")
	cfg.Header.Set("gcid", "gc-1")
	conn, err := websocket.DialConfig(cfg)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close()

	// Publish from the "async generator" side; the write loop must deliver
	// both frames to the client.
	broker.Publish("gc-1", ProfileMessage{Kind: KindProfileReady, Payload: json.RawMessage(`{"ok":true}`)})
	broker.Publish("gc-1", ProfileMessage{Kind: KindProfileError, Payload: json.RawMessage(`{"error":"x"}`)})

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var raw map[string]json.RawMessage
	if err := websocket.JSON.Receive(conn, &raw); err != nil {
		t.Fatalf("read profile frame: %v", err)
	}
	var kind string
	if err := json.Unmarshal(raw["kind"], &kind); err != nil {
		t.Fatalf("kind: %v", err)
	}
	if kind != string(KindProfileReady) {
		t.Fatalf("first frame kind=%s want profile_ready", kind)
	}

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if err := websocket.JSON.Receive(conn, &raw); err != nil {
		t.Fatalf("read second profile frame: %v", err)
	}
	if err := json.Unmarshal(raw["kind"], &kind); err != nil {
		t.Fatalf("kind: %v", err)
	}
	if kind != string(KindProfileError) {
		t.Errorf("second frame kind=%s want profile_error", kind)
	}

	// Client close → readLoop observes the close → done → writeLoop exits.
	conn.Close()
}

// TestProfileHandler_ReadLoop_AbnormalReadLogs pins the fail-loud read path:
// a non-close Receive error is logged and ends the loop.
func TestProfileHandler_ReadLoop_AbnormalReadLogs(t *testing.T) {
	srv := httptest.NewServer(websocket.Handler(func(c *websocket.Conn) {
		if _, err := c.Write([]byte("{{{{ not json")); err != nil {
			return
		}
		c.Close()
	}))
	defer srv.Close()

	cfg, err := websocket.NewConfig("ws"+strings.TrimPrefix(srv.URL, "http"), "http://localhost/")
	if err != nil {
		t.Fatalf("ws config: %v", err)
	}
	conn, err := websocket.DialConfig(cfg)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close()

	var logged string
	h := &ProfileHandler{logf: func(format string, args ...any) { logged = format }}
	done := make(chan struct{})
	h.readLoop(context.Background(), conn, "gc-1", done)
	<-done
	if !strings.Contains(logged, "read closed") {
		t.Errorf("abnormal read not logged (got %q)", logged)
	}
}

// TestProfileHandler_WriteLoop_ClosedChannel pins the write-loop exit: a
// closed broker channel (cleanup) terminates the loop.
func TestProfileHandler_WriteLoop_ClosedChannel(t *testing.T) {
	ch := make(chan ProfileMessage)
	close(ch)
	h := &ProfileHandler{}
	h.writeLoop(nil, ch, "gc-1", nil)
}

// TestProfileHandler_WriteLoop_WriteFailure pins the write-fail branch: a
// stuck/closed connection makes Send fail and the loop returns. The test
// arms the connection's write deadline itself (the write loop relies on the
// transport for liveness here) and overflows the socket buffer so the
// blocked write trips it.
func TestProfileHandler_WriteLoop_WriteFailure(t *testing.T) {
	hold := make(chan struct{})
	srv := httptest.NewServer(websocket.Handler(func(c *websocket.Conn) {
		<-hold
		c.Close()
	}))
	defer func() { close(hold); srv.Close() }()

	cfg, err := websocket.NewConfig("ws"+strings.TrimPrefix(srv.URL, "http"), "http://localhost/")
	if err != nil {
		t.Fatalf("ws config: %v", err)
	}
	conn, err := websocket.DialConfig(cfg)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close()

	big := json.RawMessage(`"` + strings.Repeat("x", 256*1024) + `"`)
	ch := make(chan ProfileMessage, 64)
	for i := 0; i < 64; i++ {
		ch <- ProfileMessage{Kind: KindProfileReady, Payload: big}
	}

	var logged string
	h := &ProfileHandler{logf: func(format string, args ...any) { logged = format }}
	_ = conn.SetWriteDeadline(time.Now().Add(150 * time.Millisecond))

	start := time.Now()
	h.writeLoop(conn, ch, "gc-1", make(chan struct{}))
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("writeLoop took %v — stuck client was never dropped", elapsed)
	}
	if !strings.Contains(logged, "write failed") {
		t.Errorf("write failure not logged (got %q)", logged)
	}
}