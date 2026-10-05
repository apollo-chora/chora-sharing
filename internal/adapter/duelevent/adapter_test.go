// duelevent bridge tests - the flat-param publisher adapter just maps its
// args onto the events.Publisher's typed DuelEvent / RoyaltySettledEvent
// structs and delegates; these spec the bridge against the in-memory bus
// so the full envelope-validating publish path runs.
package duelevent_test

import (
	"context"
	"testing"

	"github.com/apollo-chora/chora-common/eventbus"

	"github.com/apollo-chora/chora-sharing/internal/adapter/duelevent"
	eventsadapter "github.com/apollo-chora/chora-sharing/internal/adapter/events"
)

func newAdapter() *duelevent.PublisherAdapter {
	pub := eventsadapter.NewPublisher(eventsadapter.Config{Bus: eventbus.NewInMemoryBus()})
	return duelevent.New(pub)
}

func TestNew_ReturnsNonNil(t *testing.T) {
	if newAdapter() == nil {
		t.Fatal("New returned nil adapter")
	}
}

func TestPublishDuelCompleted_Delegates(t *testing.T) {
	a := newAdapter()
	err := a.PublishDuelCompleted(
		context.Background(),
		"ranked", "duel-1", "tenant-1", "gcid-a", "gcid-b", "gcid-a",
		3, 1,
	)
	if err != nil {
		t.Fatalf("PublishDuelCompleted: %v", err)
	}
}

func TestPublishRoyaltySettled_Delegates(t *testing.T) {
	a := newAdapter()
	err := a.PublishRoyaltySettled(
		context.Background(),
		"settle-1", "grant-1", "gcid-owner", "tenant-2", "atom-1",
		5.5, "mana", "duel", "evt-1", "tenant-3",
	)
	if err != nil {
		t.Fatalf("PublishRoyaltySettled: %v", err)
	}
}