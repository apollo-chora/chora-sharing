// Edge coverage for the in-memory bus: non-sharing topics and the
// non-blocking fan-out drop branch.
//
// NOTE: the entropy-failure fallbacks inside newUUIDv7 / mintTraceparent are
// not exercised - on Go >= 1.24 crypto/rand.Read fatals on reader failure
// (go.dev/issue/66821), so those branches are unreachable from a test.
package events_test

import (
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/events"
)

func TestBus_Publish_AcceptsNonSharingDomainTopic(t *testing.T) {
	bus := events.NewBus()
	// Matches chora.{domain}.{aggregate}.{event_type}.v{N} but sits outside
	// the chora.sharing.* namespace - must not panic during the M12 shared
	// bus migration.
	bus.Publish("chora.content.post.published.v1", events.NewEnvelope("", "", tenantA, gcidA), nil)
	if got := len(bus.Drain()); got != 1 {
		t.Fatalf("expected 1 drained event, got %d", got)
	}
}

func TestBus_Publish_DropsOnFullSubscriberChannel(t *testing.T) {
	bus := events.NewBus()
	ch := bus.Subscribe()
	// Fill the subscriber channel (cap 64) plus one extra so the fan-out
	// selects the non-blocking default branch on the last publish. The bus
	// buffer stays authoritative regardless of subscriber drops.
	for i := 0; i < 65; i++ {
		bus.Publish("chora.sharing.post.created.v1", events.NewEnvelope("", "", tenantA, gcidA), nil)
	}
	if got := len(bus.Drain()); got != 65 {
		t.Fatalf("expected 65 buffered events, got %d", got)
	}
	select {
	case <-ch:
	default:
	}
}