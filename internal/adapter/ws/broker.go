package ws

import (
	"encoding/json"
	"sync"
)

type MessageKind string

const (
	KindSnapshot           MessageKind = "snapshot"
	KindRoundStart         MessageKind = "round_start"
	KindRoundResolved      MessageKind = "round_resolved"
	KindRoundTimeout       MessageKind = "round_timeout"
	KindDuelCompleted      MessageKind = "duel_completed"
	KindError              MessageKind = "error"
	KindBlitzStart         MessageKind = "blitz_start"
	KindBlitzAnswerResolved MessageKind = "blitz_answer_resolved"
)

// Message is the envelope sent over the WebSocket. Payload carries the
// frame-specific JSON as an embedded object (not a base64 string).
//
// MUST be json.RawMessage, NOT []byte: encoding/json marshals []byte as a
// base64-encoded string, which would make the FE read `msg.payload` as a
// string ("eyJkdWVsX2lk...") instead of an object — every field access
// (payload.total_rounds, payload.round_no, payload.question) returns
// undefined and the arena renders 0/0 with no question. json.RawMessage
// is `type RawMessage []byte` but implements MarshalJSON returning the
// bytes verbatim, so the frame serializes as nested JSON.
type Message struct {
	DuelID  string          `json:"duel_id"`
	Kind    MessageKind     `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

const subscriptionBufferSize = 32

type Broker struct {
	mu   sync.RWMutex
	subs map[string]map[string]chan Message
}

func NewBroker() *Broker {
	return &Broker{subs: make(map[string]map[string]chan Message)}
}

func (b *Broker) Subscribe(duelID, subID string) (<-chan Message, func()) {
	ch := make(chan Message, subscriptionBufferSize)
	b.mu.Lock()
	if b.subs[duelID] == nil {
		b.subs[duelID] = make(map[string]chan Message)
	}
	if old, ok := b.subs[duelID][subID]; ok {
		delete(b.subs[duelID], subID)
		close(old)
	}
	b.subs[duelID][subID] = ch
	b.mu.Unlock()

	cleanup := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		set, ok := b.subs[duelID]
		if !ok {
			return
		}
		if c, ok := set[subID]; ok {
			delete(set, subID)
			close(c)
		}
		if len(set) == 0 {
			delete(b.subs, duelID)
		}
	}
	return ch, cleanup
}

func (b *Broker) Publish(msg Message) {
	b.mu.RLock()
	set := b.subs[msg.DuelID]
	if set == nil {
		b.mu.RUnlock()
		return
	}
	channels := make([]chan Message, 0, len(set))
	for _, ch := range set {
		channels = append(channels, ch)
	}
	b.mu.RUnlock()

	for _, ch := range channels {
		select {
		case ch <- msg:
		default:
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- msg:
			default:
			}
		}
	}
}

func (b *Broker) SubscriberCount(duelID string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs[duelID])
}
