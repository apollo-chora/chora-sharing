// subscriber_wiring_test.go - guard branches + register loops of the
// event-bus subscriber wiring. The error paths of the hand-written handler
// decoders run with garbage payloads (no ports needed - they NACK before
// dispatch); the register functions are exercised against the in-memory bus or
// a stub bus. The durable consumer loops that need a live NATS server are not
// booted here.
package main

import (
	"context"
	"errors"
	"testing"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-sharing/internal/adapter/subscribers"
)

// stubBus is an eventbus.Bus double. Its Subscribe outcome is controllable so
// the register* goroutines hit their subscribe-exit log branches.
type stubBus struct {
	subErr error
}

func (stubBus) Publish(context.Context, string, cgcenvelope.Envelope, []byte) error { return nil }
func (s stubBus) Subscribe(context.Context, eventbus.ConsumerConfig, eventbus.Handler) error {
	return s.subErr
}
func (stubBus) Close() error { return nil }

func TestRegisterFamiliarMilestoneSubscriber_NilGuards(t *testing.T) {
	if err := registerFamiliarMilestoneSubscriber(context.Background(), nil, nil); err == nil {
		t.Fatal("expected error for nil subscriber")
	}
	sub := subscribers.NewFamiliarMilestoneSubscriber(subscribers.Config{})
	if err := registerFamiliarMilestoneSubscriber(context.Background(), nil, sub); err != nil {
		t.Fatalf("nil bus must no-op, got %v", err)
	}
}

func TestRegisterAtomProjectionSubscriber_NilGuards(t *testing.T) {
	if err := registerAtomProjectionSubscriber(context.Background(), nil, nil, nil); err == nil {
		t.Fatal("expected error for nil subscriber")
	}
	sub := &subscribers.AtomProjectionSubscriber{}
	if err := registerAtomProjectionSubscriber(context.Background(), nil, sub, nil); err != nil {
		t.Fatalf("nil bus must no-op, got %v", err)
	}
}

func TestRegisterMilestoneSubscriber_BindsAllTopics(t *testing.T) {
	bus := eventbus.NewInMemoryBus()
	sub := subscribers.NewFamiliarMilestoneSubscriber(subscribers.Config{})
	registerMilestoneSubscriber(context.Background(), bus, sub)
	// No assertion beyond no-panic: the Subscribe calls + log are the
	// unit under test; handlers only run when a message is published.
}

func TestRegisterLiveQuizScoreSubscriber_BindsTopic(t *testing.T) {
	bus := eventbus.NewInMemoryBus()
	sub := subscribers.NewLiveQuizScoreSubscriber(subscribers.LiveQuizScoreConfig{})
	registerLiveQuizScoreSubscriber(context.Background(), bus, sub)
}

func TestRegisterDuelCompletedSubscriber_BindsTopic(t *testing.T) {
	bus := eventbus.NewInMemoryBus()
	sub := subscribers.NewDuelCompletedSubscriber(subscribers.DuelCompletedConfig{})
	registerDuelCompletedSubscriber(context.Background(), bus, sub)
}

func TestBuildFamiliarMilestoneHandler_ErrorPaths(t *testing.T) {
	sub := subscribers.NewFamiliarMilestoneSubscriber(subscribers.Config{})
	garbage := eventbus.Message{Payload: []byte("%%%not-json%%%")}

	if err := buildFamiliarMilestoneHandler(sub, subscribers.TopicFamiliarStageUp)(context.Background(), garbage); err == nil {
		t.Fatal("expected decode error for garbage payload")
	}
	if err := buildFamiliarMilestoneHandler(sub, "chora.unknown.x.v1")(context.Background(), garbage); err == nil {
		t.Fatal("expected unknown-topic error")
	}
}

func TestBuildAtomProjectionHandler_ErrorPaths(t *testing.T) {
	sub := &subscribers.AtomProjectionSubscriber{}
	garbage := eventbus.Message{Payload: []byte("%%%not-json%%%")}

	if err := buildAtomProjectionHandler(sub, nil, subscribers.TopicAtomPublished)(context.Background(), garbage); err == nil {
		t.Fatal("expected decode error for garbage payload")
	}
	if err := buildAtomProjectionHandler(sub, nil, "chora.unknown.x.v1")(context.Background(), garbage); err == nil {
		t.Fatal("expected unknown-topic error")
	}
}

func TestBuildOrphanCreatedHandler_ErrorPaths(t *testing.T) {
	stranding := &subscribers.AtomReuseStrandingSubscriber{}
	if err := buildOrphanCreatedHandler(stranding)(context.Background(), eventbus.Message{Payload: []byte("%%%not-json%%%")}); err == nil {
		t.Fatal("expected decode error for garbage payload")
	}
}

func TestRegisterSubscriber_SubscribeErrorLogged(t *testing.T) {
	ctx := context.Background()
	failing := stubBus{subErr: errors.New("nats: subscribe failed")}

	sub := &subscribers.AtomProjectionSubscriber{}
	if err := registerAtomProjectionSubscriber(ctx, failing, sub, nil); err != nil {
		t.Fatalf("register atom projection: %v", err)
	}
	// Non-nil stranding spawns the orphan-created leg, whose subscribe-exit
	// log branch is also hit by the failing bus.
	if err := registerAtomProjectionSubscriber(ctx, failing, sub, &subscribers.AtomReuseStrandingSubscriber{}); err != nil {
		t.Fatalf("register atom projection (stranding): %v", err)
	}

	fam := subscribers.NewFamiliarMilestoneSubscriber(subscribers.Config{})
	if err := registerFamiliarMilestoneSubscriber(ctx, failing, fam); err != nil {
		t.Fatalf("register familiar milestone: %v", err)
	}
}
