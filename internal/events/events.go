// Package events is the in-memory envelope-conforming event bus the
// chora-sharing service emits to.
//
// Production wiring replaces this with the NATS JetStream event bus
// (chora-common/eventbus). Until then, the bus exposes the same envelope
// shape so handlers + tests can validate the taxonomy + envelope
// mandatory fields without standing up the broker.
//
// Envelope mandatory fields (per CLAUDE.md §6 + .claude/rules/ddd-enforcement.md):
//
//	event_id        UUIDv7
//	idempotency_key (defaults to event_id)
//	tenant_id
//	gcid            (actor — empty for system events)
//	occurred_at
//	published_at
//	traceparent     (W3C — required across the bus)
//	tracestate
//	source_project  (chora-content / chora-delivery / chora-local / chora-golden)
//	source_service  (chora-sharing)
//	schema_version
//
// Topic taxonomy: chora.{domain}.{aggregate}.{event_type}.v{N}
package events

import (
	"crypto/rand"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	// SourceService is hardcoded — only chora-sharing emits via this bus.
	SourceService = "chora-sharing"
)

// Envelope mirrors chora-contracts/proto/common/envelope.proto in pure Go.
//
// We hand-roll the struct in the skeleton to avoid pulling the full
// protobuf-generated package — the M12 swap-in will replace these fields
// with the typed Protobuf message.
type Envelope struct {
	EventID        string    `json:"event_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	TenantID       string    `json:"tenant_id"`
	GCID           string    `json:"gcid"`
	OccurredAt     time.Time `json:"occurred_at"`
	PublishedAt    time.Time `json:"published_at"`
	Traceparent    string    `json:"traceparent"`
	Tracestate     string    `json:"tracestate"`
	SourceProject  string    `json:"source_project"`
	SourceService  string    `json:"source_service"`
	SchemaVersion  int32     `json:"schema_version"`
}

// NewEnvelope constructs an Envelope with all mandatory fields populated.
//
//   - traceparent: pass "" to mint a fresh one; an upstream-supplied
//     W3C-format value is accepted as-is.
//   - idempotencyKey: empty string → defaults to event_id.
func NewEnvelope(traceparent, idempotencyKey, tenantID, gcid string) Envelope {
	now := time.Now().UTC()
	eventID := newUUIDv7()
	if idempotencyKey == "" {
		idempotencyKey = eventID
	}
	if traceparent == "" {
		traceparent = mintTraceparent()
	}
	// SourceProject is the project label stamped into every envelope;
	// CHORA_SOURCE_PROJECT overrides (default: chora-local).
	src := os.Getenv("CHORA_SOURCE_PROJECT")
	if src == "" {
		src = "chora-local"
	}
	return Envelope{
		EventID:        eventID,
		IdempotencyKey: idempotencyKey,
		TenantID:       tenantID,
		GCID:           gcid,
		OccurredAt:     now,
		PublishedAt:    now,
		Traceparent:    traceparent,
		Tracestate:     "",
		SourceProject:  src,
		SourceService:  SourceService,
		SchemaVersion:  1,
	}
}

// Event bundles an envelope with its topic + payload. This is the unit
// stored in the bus and surfaced to subscribers / tests.
type Event struct {
	Topic    string                 `json:"topic"`
	Envelope Envelope               `json:"envelope"`
	Payload  map[string]interface{} `json:"payload"`
}

// Bus is an in-memory FIFO event bus.
//
// Drain returns the buffered events and resets the buffer; Subscribe
// returns a channel for live delivery (the buffer is the producer of
// truth; Subscribe is best-effort and drops events when the channel is
// full to keep the producer non-blocking).
type Bus struct {
	mu      sync.Mutex
	events  []Event
	subs    []chan Event
	subsCap int
}

// NewBus returns an empty bus with a default subscriber-channel capacity.
func NewBus() *Bus {
	return &Bus{subsCap: 64}
}

// topicRe enforces chora.{domain}.{aggregate}.{event_type}.v{N}.
//
// Per CLAUDE.md §1 + chora-contracts/CLAUDE.md §2: domain is one of 11
// canonical names; aggregate + event_type are snake_case.
var topicRe = regexp.MustCompile(`^chora\.[a-z_]+\.[a-z_]+\.[a-z_]+\.v[0-9]+$`)

// Publish records an event on the bus.
//
// The topic must match the chora.{domain}.{aggregate}.{event_type}.v{N}
// pattern (per chora-contracts/CLAUDE.md §2). Malformed topics panic —
// they indicate a programmer error and would never pass Schema Registry
// validation in production.
func (b *Bus) Publish(topic string, env Envelope, payload map[string]interface{}) {
	if !topicRe.MatchString(topic) {
		panic(fmt.Sprintf("events.Bus: malformed topic %q (must match chora.{domain}.{aggregate}.{event_type}.v{N})", topic))
	}
	if !strings.HasPrefix(topic, "chora.sharing.") {
		// We don't enforce this hard at the bus layer — other domains may
		// theoretically share the bus during M12 migration — but we keep
		// the comment as a reminder for code review.
		_ = topic
	}
	if payload == nil {
		payload = map[string]interface{}{}
	}
	e := Event{Topic: topic, Envelope: env, Payload: payload}

	b.mu.Lock()
	b.events = append(b.events, e)
	subs := make([]chan Event, len(b.subs))
	copy(subs, b.subs)
	b.mu.Unlock()

	// Best-effort fan-out — non-blocking on full channels.
	for _, ch := range subs {
		select {
		case ch <- e:
		default:
		}
	}
}

// Drain returns and clears the buffered events.
func (b *Bus) Drain() []Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.events
	b.events = nil
	return out
}

// Subscribe returns a buffered channel of live events.
//
// Subscribe is intended for read-models / fan-out tests. Producers do
// NOT block — events are dropped on a full subscriber channel. For
// canonical durable consumption, the M12 Cloud Pub/Sub adapter manages
// ack semantics + DLQ.
func (b *Bus) Subscribe() <-chan Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan Event, b.subsCap)
	b.subs = append(b.subs, ch)
	return ch
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

// newUUIDv7 — RFC 9562 §5.7 UUIDv7.
func newUUIDv7() string {
	const buflen = 16
	var b [buflen]byte
	now := uint64(time.Now().UnixMilli())
	b[0] = byte(now >> 40)
	b[1] = byte(now >> 32)
	b[2] = byte(now >> 24)
	b[3] = byte(now >> 16)
	b[4] = byte(now >> 8)
	b[5] = byte(now)
	if _, err := rand.Read(b[6:]); err != nil {
		for i := 6; i < buflen; i++ {
			b[i] = byte(now >> uint(8*(i-6)))
		}
	}
	b[6] = (b[6] & 0x0F) | 0x70
	b[8] = (b[8] & 0x3F) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// mintTraceparent generates a fresh W3C traceparent (version-00).
func mintTraceparent() string {
	hex := func(n int) string {
		buf := make([]byte, n)
		if _, err := rand.Read(buf); err != nil {
			now := time.Now().UnixNano()
			for i := range buf {
				buf[i] = byte(now >> uint(8*(i%8)))
			}
		}
		const hexDigits = "0123456789abcdef"
		out := make([]byte, len(buf)*2)
		for i, v := range buf {
			out[i*2] = hexDigits[v>>4]
			out[i*2+1] = hexDigits[v&0x0F]
		}
		return string(out)
	}
	return "00-" + hex(16) + "-" + hex(8) + "-01"
}
