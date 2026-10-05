package ws

import (
	"encoding/json"
	"sync"
)

// ProfileMessageKind is the frame kind for profile-generation notifications.
type ProfileMessageKind string

const (
	// KindProfileReady is published when the async Conjure call succeeds —
	// the payload is the full profilerResponse JSON so the FE can apply it
	// verbatim (same shape as GET /v1/me/profile + POST .../generate).
	KindProfileReady ProfileMessageKind = "profile_ready"
	// KindProfileError is published when the async Conjure call fails —
	// the payload is {"error": "<message>"} so the FE can surface the
	// failure without falling back to polling.
	KindProfileError ProfileMessageKind = "profile_error"
)

// ProfileMessage is the envelope pushed over the /v1/me/profile/ws
// connection. Payload is json.RawMessage (NOT []byte) so it serialises
// as nested JSON rather than a base64 string — same invariant as the
// duel Message frame (see broker.go).
type ProfileMessage struct {
	Kind    ProfileMessageKind `json:"kind"`
	Payload json.RawMessage    `json:"payload"`
}

// ProfileBroker is a per-gcid message broker for async profile-generation
// notifications. Mirrors Broker (the duel broker) but keyed by gcid
// instead of duel_id — each user subscribes to their own notifications.
// Subscribe/Publish are mutex-protected; the goroutine spawned by
// generateProfile publishes, the WS handler subscribes.
type ProfileBroker struct {
	mu   sync.RWMutex
	subs map[string]map[string]chan ProfileMessage // gcid → subID → ch
}

// NewProfileBroker constructs an empty broker.
func NewProfileBroker() *ProfileBroker {
	return &ProfileBroker{subs: make(map[string]map[string]chan ProfileMessage)}
}

// Subscribe registers a receiver for the given gcid. Returns the channel
// (closed on Unsubscribe) + a cleanup func. Re-subscribing with the same
// subID closes the prior channel and replaces it — same semantics as
// Broker.Subscribe so a reconnecting client doesn't leak.
func (b *ProfileBroker) Subscribe(gcid, subID string) (<-chan ProfileMessage, func()) {
	ch := make(chan ProfileMessage, subscriptionBufferSize)
	b.mu.Lock()
	if b.subs[gcid] == nil {
		b.subs[gcid] = make(map[string]chan ProfileMessage)
	}
	if old, ok := b.subs[gcid][subID]; ok {
		delete(b.subs[gcid], subID)
		close(old)
	}
	b.subs[gcid][subID] = ch
	b.mu.Unlock()

	cleanup := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		set, ok := b.subs[gcid]
		if !ok {
			return
		}
		if c, ok := set[subID]; ok {
			delete(set, subID)
			close(c)
		}
		if len(set) == 0 {
			delete(b.subs, gcid)
		}
	}
	return ch, cleanup
}

// Publish fans msg out to every subscriber on gcid. Non-blocking: if a
// subscriber's buffer is full the oldest message is dropped (same
// drop-oldest policy as Broker.Publish — a fresh profile_ready is more
// valuable than a stale one).
func (b *ProfileBroker) Publish(gcid string, msg ProfileMessage) {
	b.mu.RLock()
	set := b.subs[gcid]
	if set == nil {
		b.mu.RUnlock()
		return
	}
	channels := make([]chan ProfileMessage, 0, len(set))
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

// SubscriberCount returns the number of live subscribers for a gcid.
// Used by tests + diagnostics; not on any hot path.
func (b *ProfileBroker) SubscriberCount(gcid string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs[gcid])
}
