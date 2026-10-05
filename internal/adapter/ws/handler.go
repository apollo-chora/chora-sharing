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

type DuelLookup interface {
	GetDuel(ctx context.Context, duelID, tenantID string) (*DuelSnapshot, bool)
}

type AnswerProcessor interface {
	SubmitAnswer(ctx context.Context, duelID, gcid string, roundNo int32, answer string, answerTimeMs int64) (RoundResolvedFrame, bool, error)
	GetCurrentRound(ctx context.Context, duelID string) (RoundStartFrame, bool)
	// SubmitBlitzAnswer processes a blitz answer. Returns the resolution
	// frame + whether the duel completed.
	SubmitBlitzAnswer(ctx context.Context, duelID, gcid string, roundNo int32, answer string, answerTimeMs int64) (BlitzAnswerResolvedFrame, bool, error)
	// GetBlitzStart returns all questions at once for a blitz duel.
	GetBlitzStart(ctx context.Context, duelID string) (BlitzStartFrame, bool)
}

type Handler struct {
	broker    *Broker
	lookup    DuelLookup
	answers   AnswerProcessor
	writeGrace time.Duration
	logf      func(format string, args ...any)
}

type HandlerOption func(*Handler)

func WithLogger(logf func(format string, args ...any)) HandlerOption {
	return func(h *Handler) { h.logf = logf }
}

// WithWriteGrace wires the WebSocketGrace config (SharingRules.WebSocketGrace)
// so the write loop sets a per-write deadline. A stuck client that stops
// reading is dropped after the grace period rather than holding a goroutine
// forever. Zero (the default) means no deadline — the bootstrap layer
// should always pass the configured value.
func WithWriteGrace(d time.Duration) HandlerOption {
	return func(h *Handler) { h.writeGrace = d }
}

// WriteGrace returns the configured write-deadline grace period. Exposed so
// the bootstrap layer + tests can verify the config is wired (the value
// comes from SharingRules.WebSocketGrace). Zero means unwired.
func (h *Handler) WriteGrace() time.Duration {
	return h.writeGrace
}

func NewHandler(broker *Broker, lookup DuelLookup, answers AnswerProcessor, opts ...HandlerOption) *Handler {
	h := &Handler{
		broker:  broker,
		lookup:  lookup,
		answers: answers,
		logf:    log.Printf,
	}
	for _, o := range opts {
		o(h)
	}
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.lookup == nil || h.answers == nil {
		http.Error(w, "duel websocket dependencies not configured", http.StatusServiceUnavailable)
		return
	}

	tenantID := r.Header.Get("X-Tenant-Id")
	gcid := r.Header.Get("gcid")
	if tenantID == "" || gcid == "" {
		http.Error(w, "missing identity headers", http.StatusUnauthorized)
		return
	}
	// Inject tenant + gcid into the request context so RLS ApplySession
	// (called inside GetDuel → repo.getDuel) can SET LOCAL chora.tenant_id
	// and satisfy the duel_sessions_tenant_isolation policy. Without this,
	// ApplySession returns ErrNoTenantContext and every duel lookup fails
	// with "duel not found" even when the row exists. Mirrors the REST
	// requireIdentity middleware (handlers.go:307-308).
	//
	// The context is also threaded into readLoop so SubmitAnswer (called
	// from the read goroutine) inherits the same tenant + gcid — the
	// previous code passed context.Background() there, dropping the tenant
	// and making every answer fail with "duel not found".
	ctx := tracing.WithTenantID(r.Context(), tenantID)
	ctx = tracing.WithGCID(ctx, gcid)
	r = r.WithContext(ctx)

	duelID := r.PathValue("duel_id")
	if duelID == "" {
		http.Error(w, "missing duel_id", http.StatusBadRequest)
		return
	}

	snapshot, ok := h.lookup.GetDuel(r.Context(), duelID, tenantID)
	if !ok {
		http.Error(w, "duel not found", http.StatusNotFound)
		return
	}

	subID := fmt.Sprintf("%s:%s:%d", duelID, gcid, time.Now().UnixNano())
	ch, cleanup := h.broker.Subscribe(duelID, subID)
	defer cleanup()

	wsHandler := websocket.Handler(func(conn *websocket.Conn) {
		defer conn.Close()

		snapMsg := Message{
			DuelID:  duelID,
			Kind:    KindSnapshot,
			Payload: mustMarshal(snapshot),
		}
		if err := websocket.JSON.Send(conn, snapMsg); err != nil {
			h.logf("ws: snapshot send failed duel=%s gcid=%s err=%v", duelID, gcid, err)
			return
		}

		// Send the first round_start on connect.
		h.maybeSendCurrentRound(r.Context(), conn, duelID)

		done := make(chan struct{})
		go h.readLoop(r.Context(), conn, ch, duelID, gcid, done)
		go func() {
			<-done
			conn.Close()
		}()

		h.writeLoop(conn, ch, duelID, gcid, done)
	})

	wsHandler.ServeHTTP(w, r)
}

// maybeSendCurrentRound sends a round_start frame if the duel has an
// unresolved round to play. Called on connect AND after each round
// resolves (so multi-round duels advance to the next round). For blitz
// duels, sends a blitz_start frame with all questions instead.
func (h *Handler) maybeSendCurrentRound(ctx context.Context, conn *websocket.Conn, duelID string) {
	// Blitz: send all questions at once.
	if bf, ok := h.answers.GetBlitzStart(ctx, duelID); ok {
		_ = websocket.JSON.Send(conn, Message{
			DuelID:  duelID,
			Kind:    KindBlitzStart,
			Payload: mustMarshal(bf),
		})
		return
	}
	if rf, ok := h.answers.GetCurrentRound(ctx, duelID); ok {
		_ = websocket.JSON.Send(conn, Message{
			DuelID:  duelID,
			Kind:     KindRoundStart,
			Payload:  mustMarshal(rf),
		})
	}
}

// readLoop reads inbound answer frames, submits them, and publishes the
// resolution to the broker (fan-out to both participants). After a round
// resolves, if the duel is not yet complete, it pushes the next
// round_start so multi-round duels advance without the FE re-requesting.
//
// For blitz duels, the answer is submitted via SubmitBlitzAnswer and the
// resolution is broadcast as blitz_answer_resolved. No new round_start is
// pushed (all questions were already sent in the blitz_start frame).
//
// ctx MUST carry the tenant + gcid (threaded from ServeHTTP) — using
// context.Background() here drops the tenant and every SubmitAnswer
// fails with ErrNoTenantContext → "duel not found".
func (h *Handler) readLoop(ctx context.Context, conn *websocket.Conn, ch <-chan Message, duelID, gcid string, done chan struct{}) {
	defer close(done)
	for {
		var frame InboundAnswerFrame
		if err := websocket.JSON.Receive(conn, &frame); err != nil {
			if !isNormalClose(err) {
				h.logf("ws: read error duel=%s gcid=%s err=%v", duelID, gcid, err)
			}
			return
		}

		// Detect blitz mode: try GetBlitzStart first. If the duel is a
		// blitz duel, dispatch via SubmitBlitzAnswer. This avoids a
		// separate inbound frame kind — the FE sends the same
		// InboundAnswerFrame for both modes; the server routes based
		// on the duel's mode.
		if _, isBlitz := h.answers.GetBlitzStart(ctx, duelID); isBlitz {
			verdict, completed, err := h.answers.SubmitBlitzAnswer(ctx, duelID, gcid, frame.RoundNo, frame.Answer, frame.AnswerTimeMs)
			if err != nil {
				h.logf("ws: submit blitz answer failed duel=%s gcid=%s round=%d err=%v", duelID, gcid, frame.RoundNo, err)
				errPayload, _ := json.Marshal(ErrorFrame{Message: err.Error()})
				h.broker.Publish(Message{DuelID: duelID, Kind: KindError, Payload: errPayload})
				continue
			}
			h.broker.Publish(Message{DuelID: duelID, Kind: KindBlitzAnswerResolved, Payload: mustMarshal(verdict)})
			if completed {
				completedFrame := DuelCompletedFrame{
					WinnerGCID:      verdict.WinnerGCID,
					ScoreChallenger: verdict.ScoreChallenger,
					ScoreOpponent:   verdict.ScoreOpponent,
				}
				h.broker.Publish(Message{DuelID: duelID, Kind: KindDuelCompleted, Payload: mustMarshal(completedFrame)})
			}
			continue
		}

		verdict, completed, err := h.answers.SubmitAnswer(ctx, duelID, gcid, frame.RoundNo, frame.Answer, frame.AnswerTimeMs)
		if err != nil {
			h.logf("ws: submit answer failed duel=%s gcid=%s round=%d err=%v", duelID, gcid, frame.RoundNo, err)
			errPayload, _ := json.Marshal(ErrorFrame{Message: err.Error()})
			h.broker.Publish(Message{DuelID: duelID, Kind: KindError, Payload: errPayload})
			continue
		}
		h.broker.Publish(Message{DuelID: duelID, Kind: KindRoundResolved, Payload: mustMarshal(verdict)})
		if completed {
			completedFrame := DuelCompletedFrame{
				WinnerGCID:      verdict.WinnerGCID,
				ScoreChallenger: verdict.ScoreChallenger,
				ScoreOpponent:   verdict.ScoreOpponent,
			}
			h.broker.Publish(Message{DuelID: duelID, Kind: KindDuelCompleted, Payload: mustMarshal(completedFrame)})
		} else if rf, ok := h.answers.GetCurrentRound(ctx, duelID); ok {
			// FCFS: only push round_start if the round ADVANCED. A wrong
			// answer (round stays open, same round number) must NOT
			// re-publish round_start — that would flood both clients with
			// a duplicate frame and eat B's steal response. The round
			// advanced iff GetCurrentRound returns a round number > the
			// one just resolved.
			if int(rf.RoundNo) > int(verdict.RoundNo) {
				h.broker.Publish(Message{DuelID: duelID, Kind: KindRoundStart, Payload: mustMarshal(rf)})
			}
		}
	}
}

func (h *Handler) writeLoop(conn *websocket.Conn, ch <-chan Message, duelID, gcid string, done chan struct{}) {
	for {
		select {
		case <-done:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			// Apply the configured WebSocketGrace as a per-write deadline
			// so a client that stops reading is dropped after the grace
			// period instead of blocking the write goroutine forever.
			// Zero (unwired) skips the deadline to preserve the prior
			// behaviour for tests that don't pass WithWriteGrace.
			if h.writeGrace > 0 {
				_ = conn.SetWriteDeadline(time.Now().Add(h.writeGrace))
			}
			if err := websocket.JSON.Send(conn, msg); err != nil {
				h.logf("ws: write failed duel=%s gcid=%s err=%v", duelID, gcid, err)
				return
			}
		}
	}
}

func mustMarshal(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return b
}

func isNormalClose(err error) bool {
	if err == nil {
		return true
	}
	msg := err.Error()
	return contains(msg, "close") || contains(msg, "EOF") || contains(msg, "closed") || errors.Is(err, context.Canceled)
}

func contains(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
