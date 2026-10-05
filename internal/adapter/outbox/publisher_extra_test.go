// Package outbox_test — Publisher branch-coverage tests.
//
// Drives the residual Publisher / BuildRow branch left uncovered by
// publisher_test.go: aggregate_id falling back to event_id when the
// envelope carries no gcid (system-emitted events). No production code
// touched.
package outbox_test

import (
	"context"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/adapter/outbox"
)

func TestPublisher_Publish_EmptyGCID_FallsBackToEventIDForAggregate(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store})
	env := newPublishEnvelope(time.Now().UTC())
	env.GCID = "" // system-emitted events may omit gcid (envelope.Validate allows it)
	if err := pub.Publish(context.Background(), "chora.sharing.follow.created.v1", env, []byte(`{}`)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 1)
	if len(rows) != 1 {
		t.Fatalf("rows = %d; want 1", len(rows))
	}
	row := rows[0]
	if row.AggregateID != env.EventID {
		t.Errorf("row.AggregateID = %q; want fallback to EventID %q", row.AggregateID, env.EventID)
	}
	if row.GCID != "" {
		t.Errorf("row.GCID = %q; want empty (no gcid on envelope)", row.GCID)
	}
	if row.AggregateType != "follow" {
		t.Errorf("row.AggregateType = %q; want 'follow' (parts[2] of topic)", row.AggregateType)
	}
}