package ws

import (
	"context"
	"net/http"
	"time"
)

// Conn is the minimal WebSocket connection interface the handler needs.
//
// It abstracts over gorilla/websocket, nhooyr/websocket, or a test fake.
// Neither library is currently in the module's go.sum; defining the
// interface here means the package compiles with zero new external deps,
// and the bootstrap layer can inject a real Conn implementation (an
// adapter struct wrapping the chosen library's *Conn) once the dep is
// added. The interface is intentionally tiny: ReadJSON / WriteJSON /
// Close cover the full handler round-trip.
//
// Write must be safe for concurrent use by one goroutine (the write
// loop). The handler never writes from two goroutines.
type Conn interface {
	// ReadJSON decodes the next inbound frame into v. It blocks until a
	// frame arrives or the connection closes; a closed connection returns
	// a net-ErrClosed-equivalent error.
	ReadJSON(v interface{}) error
	// WriteJSON marshals v and sends it as one frame. Called from the
	// write-loop goroutine only.
	WriteJSON(v interface{}) error
	// Close terminates the connection with the given status code + text.
	Close(code int, text string) error
	// SetWriteDeadline bounds a single write — used by the write loop so
	// a stuck client is eventually dropped rather than leaking a goroutine.
	SetWriteDeadline(t time.Time) error
}

// Upgrader promotes an HTTP request to a WebSocket Conn. The real
// implementation wraps the chosen library's Upgrader.Upgrade; tests use
// a fake that returns an in-process pipe pair.
type Upgrader interface {
	Upgrade(w http.ResponseWriter, r *http.Request, responseHeader http.Header) (Conn, error)
}

// Dialer connects an outbound WS (used only by tests that simulate a
// second pod / client). Not wired in production.
type Dialer interface {
	DialContext(ctx context.Context, url string, h http.Header) (Conn, *http.Response, error)
}
