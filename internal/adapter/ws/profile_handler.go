package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"golang.org/x/net/websocket"

	"github.com/apollo-chora/chora-common/tracing"
)

// ProfileHandler serves /v1/me/profile/ws. It is a receive-only WS: the
// client never sends a meaningful payload, it only waits for the
// profile_ready / profile_error frame that the async generateProfile
// goroutine publishes to the ProfileBroker.
//
// Mirrors the duel Handler (handler.go) but stripped of the readLoop
// answer-submission path — the read loop here only drains inbound frames
// to detect a client close (so ping/pong + TCP teardown are observed).
type ProfileHandler struct {
	broker *ProfileBroker
	logf   func(format string, args ...any)
}

// ProfileHandlerOption configures a ProfileHandler.
type ProfileHandlerOption func(*ProfileHandler)

// WithProfileLogger overrides the default log.Printf logger.
func WithProfileLogger(logf func(format string, args ...any)) ProfileHandlerOption {
	return func(h *ProfileHandler) { h.logf = logf }
}

// NewProfileHandler constructs a ProfileHandler bound to broker.
func NewProfileHandler(broker *ProfileBroker, opts ...ProfileHandlerOption) *ProfileHandler {
	h := &ProfileHandler{broker: broker, logf: log.Printf}
	for _, o := range opts {
		o(h)
	}
	return h
}

// ServeHTTP upgrades the connection and runs the write loop. The gcid
// is read from the gateway-stamped `gcid` header (set by the gateway's
// WS proxy from the validated session JWT — the same header the REST
// requireIdentity middleware reads).
func (h *ProfileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.broker == nil {
		http.Error(w, "profile broker not configured", http.StatusServiceUnavailable)
		return
	}

	tenantID := r.Header.Get("X-Tenant-Id")
	gcid := r.Header.Get("gcid")
	if tenantID == "" || gcid == "" {
		http.Error(w, "missing identity headers", http.StatusUnauthorized)
		return
	}
	// Thread tenant + gcid into the context so any downstream call
	// (e.g. a future RLS read) sees them. Mirrors the duel handler
	// (handler.go:75-77).
	ctx := tracing.WithTenantID(r.Context(), tenantID)
	ctx = tracing.WithGCID(ctx, gcid)
	r = r.WithContext(ctx)

	subID := fmt.Sprintf("%s:%d", gcid, time.Now().UnixNano())
	ch, cleanup := h.broker.Subscribe(gcid, subID)
	defer cleanup()

	wsHandler := websocket.Handler(func(conn *websocket.Conn) {
		defer conn.Close()

		done := make(chan struct{})
		// readLoop: drain inbound frames so a client close is observed
		// promptly (without this the writeLoop blocks on a dead conn).
		go h.readLoop(ctx, conn, gcid, done)
		go func() {
			<-done
			conn.Close()
		}()

		h.writeLoop(conn, ch, gcid, done)
	})

	wsHandler.ServeHTTP(w, r)
}

// readLoop drains inbound frames until the client closes (or sends
// anything — which we ignore, the profile WS is receive-only). Any
// error closes `done` so the writeLoop unblocks.
func (h *ProfileHandler) readLoop(ctx context.Context, conn *websocket.Conn, gcid string, done chan struct{}) {
	defer close(done)
	for {
		// websocket.JSON.Receive blocks until a frame arrives or the
		// connection closes. We discard the payload — the profile WS
		// is receive-only from the client's perspective.
		var ignored json.RawMessage
		if err := websocket.JSON.Receive(conn, &ignored); err != nil {
			if !isNormalClose(err) && !errors.Is(err, context.Canceled) {
				h.logf("profile ws: read closed gcid=%s err=%v", gcid, err)
			}
			return
		}
	}
}

// writeLoop pumps broker messages to the client until the channel
// closes (Unsubscribe) or a write fails.
func (h *ProfileHandler) writeLoop(conn *websocket.Conn, ch <-chan ProfileMessage, gcid string, done chan struct{}) {
	for {
		select {
		case <-done:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			if err := websocket.JSON.Send(conn, msg); err != nil {
				h.logf("profile ws: write failed gcid=%s err=%v", gcid, err)
				return
			}
		}
	}
}
