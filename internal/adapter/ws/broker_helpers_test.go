package ws

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

// TestBroker_ResubscribeClosesOldChannel pins the reconnect semantics: the
// SAME subID replaces the prior channel (and closes it) — a reconnecting
// client must not leak the old buffered channel.
func TestBroker_ResubscribeClosesOldChannel(t *testing.T) {
	b := NewBroker()
	first, cleanup1 := b.Subscribe("duel-1", "sub")
	defer cleanup1()
	if b.SubscriberCount("duel-1") != 1 {
		t.Fatalf("SubscriberCount=%d want 1", b.SubscriberCount("duel-1"))
	}

	// Re-subscribe with the same subID: the old channel must be closed.
	second, cleanup2 := b.Subscribe("duel-1", "sub")
	defer cleanup2()
	select {
	case _, ok := <-first:
		if ok {
			t.Fatal("old channel received a message (it should be closed + replaced)")
		}
	default:
		// Not yet closed — acceptable; the sender is closed by Subscribe.
	}
	// Publish must reach only the new channel.
	b.Publish(Message{DuelID: "duel-1", Kind: KindError, Payload: json.RawMessage(`{}`)})
	if got := <-second; got.Kind != KindError {
		t.Errorf("received kind=%s want error", got.Kind)
	}
}

// TestBroker_CleanupRemovesSubscriber pins the cleanup contract: cleanup
// unsubscribes the subID and drops the duel entry when empty. A second
// cleanup is a no-op (duel key already gone).
func TestBroker_CleanupRemovesSubscriber(t *testing.T) {
	b := NewBroker()
	_, cleanup := b.Subscribe("duel-1", "sub")
	if b.SubscriberCount("duel-1") != 1 {
		t.Fatalf("SubscriberCount=%d want 1", b.SubscriberCount("duel-1"))
	}

	cleanup()
	if b.SubscriberCount("duel-1") != 0 {
		t.Errorf("SubscriberCount=%d want 0 after cleanup", b.SubscriberCount("duel-1"))
	}
	// No subscribers left: the duel key must be deleted so a later
	// SubscriberCount sees zero (and the second cleanup is a no-op).
	cleanup()

	// Publish with no subscribers is a no-op (drop-oldest path untouched).
	b.Publish(Message{DuelID: "duel-1", Kind: KindError, Payload: json.RawMessage(`{}`)})
	if b.SubscriberCount("duel-1") != 0 {
		t.Errorf("SubscriberCount=%d want 0 after unsubscribing everything", b.SubscriberCount("duel-1"))
	}
}

// TestBroker_PublishNoSubscribers pins the early-return: publishing to a
// duel nobody subscribes to drops the message without panicking.
func TestBroker_PublishNoSubscribers(t *testing.T) {
	b := NewBroker()
	b.Publish(Message{DuelID: "unrelated", Kind: KindError})
	// No panic, nothing to assert beyond that.
}

// TestBroker_PublishDropOldest pins the drop-oldest policy: a subscriber
// whose buffer is full gets the newest message (the most recent one sticks).
func TestBroker_PublishDropOldest(t *testing.T) {
	b := NewBroker()
	ch, cleanup := b.Subscribe("duel-1", "sub")
	defer cleanup()

	// Fill the buffer to capacity (subscriptionBufferSize = 32).
	for i := 0; i < subscriptionBufferSize; i++ {
		b.Publish(Message{DuelID: "duel-1", Kind: KindError, Payload: json.RawMessage(`{"n":1}`)})
	}
	// One more publish with no reader: the oldest queued message is dropped
	// and the newest one is queued at the tail.
	b.Publish(Message{DuelID: "duel-1", Kind: KindDuelCompleted, Payload: json.RawMessage(`{"n":2}`)})

	// Drain the buffer: 31 stale KindError messages followed by the newest
	// KindDuelCompleted (drop-oldest keeps the newest).
	gotCompleted := false
	received := 0
	for {
		select {
		case m := <-ch:
			received++
			if m.Kind == KindDuelCompleted {
				gotCompleted = true
			}
		default:
			goto done
		}
	}
done:
	if received != subscriptionBufferSize {
		t.Errorf("received=%d want %d (one message dropped, newest retained)", received, subscriptionBufferSize)
	}
	if !gotCompleted {
		t.Error("newest message missing after drop-oldest (buffer must retain the latest frame)")
	}
}

// --- ProfileBroker (mirrors Broker; fully uncovered before) ---

func TestProfileBroker_Lifecycle(t *testing.T) {
	b := NewProfileBroker()
	ch, cleanup := b.Subscribe("gcid-1", "sub")
	defer cleanup()

	if n := b.SubscriberCount("gcid-1"); n != 1 {
		t.Fatalf("SubscriberCount=%d want 1", n)
	}
	b.Publish("gcid-1", ProfileMessage{Kind: KindProfileReady, Payload: json.RawMessage(`{"ok":true}`)})
	if got := <-ch; got.Kind != KindProfileReady {
		t.Errorf("received kind=%s want profile_ready", got.Kind)
	}

	cleanup()
	if n := b.SubscriberCount("gcid-1"); n != 0 {
		t.Errorf("SubscriberCount=%d want 0 after cleanup", n)
	}
	// Second cleanup: no-op.
	cleanup()
}

func TestProfileBroker_ResubscribeClosesOldChannel(t *testing.T) {
	b := NewProfileBroker()
	first, cleanup1 := b.Subscribe("gcid-1", "sub")
	defer cleanup1()
	second, cleanup2 := b.Subscribe("gcid-1", "sub")
	defer cleanup2()

	// Old channel is closed (replacing the prior subID).
	select {
	case _, ok := <-first:
		if ok {
			t.Fatal("old channel received a message; it must be closed")
		}
	default:
	}
	b.Publish("gcid-1", ProfileMessage{Kind: KindProfileError, Payload: json.RawMessage(`{"error":"x"}`)})
	if got := <-second; got.Kind != KindProfileError {
		t.Errorf("received kind=%s want profile_error", got.Kind)
	}
}

func TestProfileBroker_PublishNoSubscribers(t *testing.T) {
	b := NewProfileBroker()
	b.Publish("gcid-nowhere", ProfileMessage{Kind: KindProfileReady})
	// No panic.
}

func TestProfileBroker_PublishDropOldest(t *testing.T) {
	b := NewProfileBroker()
	ch, cleanup := b.Subscribe("gcid-1", "sub")
	defer cleanup()

	for i := 0; i < subscriptionBufferSize; i++ {
		b.Publish("gcid-1", ProfileMessage{Kind: KindProfileReady})
	}
	b.Publish("gcid-1", ProfileMessage{Kind: KindProfileError})

	// Drop-oldest: the newest (profile_error) survives at the tail.
	gotErr := false
	received := 0
	for {
		select {
		case m := <-ch:
			received++
			if m.Kind == KindProfileError {
				gotErr = true
			}
		default:
			goto done
		}
	}
done:
	if received != subscriptionBufferSize {
		t.Errorf("received=%d want %d", received, subscriptionBufferSize)
	}
	if !gotErr {
		t.Error("profile_error missing after drop-oldest (newest must be retained)")
	}
}

// --- Pure helpers ---

// TestMustMarshal pins the encode path: marshalable types produce bytes;
// un-marshalable values (channels) produce nil without panicking.
func TestMustMarshal(t *testing.T) {
	if b := mustMarshal(map[string]int{"a": 1}); string(b) != `{"a":1}` {
		t.Errorf("mustMarshal = %s, want {\"a\":1}", b)
	}
	if b := mustMarshal(make(chan int)); b != nil {
		t.Error("mustMarshal must return nil for un-marshalable values")
	}
}

// TestIsNormalClose pins the close classification: nil, close-family
// messages, EOF, and context cancellation are all "normal"; other errors
// are not.
func TestIsNormalClose(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, true},
		{"close word", errors.New("websocket: close 1000"), true},
		{"closed word", errors.New("use of closed network connection"), true},
		{"EOF", errors.New("unexpected EOF"), true},
		{"context canceled", context.Canceled, true},
		{"other", errors.New("write tcp: broken pipe"), false},
	}
	for _, tc := range cases {
		if got := isNormalClose(tc.err); got != tc.want {
			t.Errorf("isNormalClose(%q)=%v want %v", tc.name, got, tc.want)
		}
	}
}

// TestContains pins the substring helper: any string contains the empty
// substring; exact substrings match; otherwise false.
func TestContains(t *testing.T) {
	if !contains("abcdef", "") {
		t.Error("contains(s, \"\") must be true")
	}
	if !contains("abcdef", "cd") {
		t.Error("contains must match a substring")
	}
	if contains("abcdef", "xyz") {
		t.Error("contains must not match a missing substring")
	}
	if contains("", "x") {
		t.Error("contains(\"\", \"x\") must be false")
	}
}