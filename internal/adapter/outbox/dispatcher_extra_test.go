// Package outbox_test — Dispatcher branch-coverage tests.
//
// These tests drive the residual Dispatcher branches that the main
// dispatcher_test.go suite leaves uncovered: Store failure propagation,
// mid-batch context cancellation, MarkPublished/MarkFailed/Deadletter
// failure logging, the Run loop's error exits, and the reconstructEnvelope
// / parseEnvelopeTime fallback branches — all through the exported API with
// fakes (failStore, cancelAfterFirstBus). No production code touched.
package outbox_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"

	"github.com/apollo-chora/chora-sharing/internal/adapter/outbox"
)

// failStore wraps an InMemoryStore and fails one chosen Store method.
// Zero-value error fields delegate to the embedded store.
type failStore struct {
	*outbox.InMemoryStore
	fetchErr      error
	publishErr    error // MarkPublished
	failedErr     error // MarkFailed
	deadletterErr error // Deadletter
}

func (s *failStore) FetchPending(ctx context.Context, limit int) ([]outbox.Row, error) {
	if s.fetchErr != nil {
		return nil, s.fetchErr
	}
	return s.InMemoryStore.FetchPending(ctx, limit)
}

func (s *failStore) MarkPublished(ctx context.Context, id string) error {
	if s.publishErr != nil {
		return s.publishErr
	}
	return s.InMemoryStore.MarkPublished(ctx, id)
}

func (s *failStore) MarkFailed(ctx context.Context, id string, errMsg string) error {
	if s.failedErr != nil {
		return s.failedErr
	}
	return s.InMemoryStore.MarkFailed(ctx, id, errMsg)
}

func (s *failStore) Deadletter(ctx context.Context, id, failureReason string, attemptCount int) error {
	if s.deadletterErr != nil {
		return s.deadletterErr
	}
	return s.InMemoryStore.Deadletter(ctx, id, failureReason, attemptCount)
}

// cancelAfterFirstBus cancels the drain context on its first Publish call.
type cancelAfterFirstBus struct {
	mu     sync.Mutex
	calls  int
	cancel context.CancelFunc
}

func (b *cancelAfterFirstBus) Publish(_ context.Context, _ string, _ cgcenvelope.Envelope, _ []byte) error {
	b.mu.Lock()
	b.calls++
	n := b.calls
	b.mu.Unlock()
	if n == 1 {
		b.cancel()
	}
	return nil
}

func TestDispatcher_DrainOnce_StoreFetchError_Wrapped(t *testing.T) {
	t.Parallel()
	store := &failStore{InMemoryStore: outbox.NewInMemoryStore(), fetchErr: errors.New("db down")}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: &recordingBus{}, WorkerID: "w1", MaxAttempts: 3,
	})
	_, err := d.DrainOnce(context.Background(), 10)
	if err == nil || !strings.Contains(err.Error(), "outbox.Dispatcher.DrainOnce") {
		t.Errorf("DrainOnce(store err) = %v; want wrapped 'outbox.Dispatcher.DrainOnce' error", err)
	}
	if !strings.Contains(err.Error(), "db down") {
		t.Errorf("DrainOnce(store err) = %v; want wrapped cause 'db down'", err)
	}
}

func TestDispatcher_DrainOnce_CancellationMidBatch_StopsAfterCommitted(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "cb1", "t1")
	insertRow(t, store, "cb2", "t1")
	ctx, cancel := context.WithCancel(context.Background())
	bus := &cancelAfterFirstBus{cancel: cancel}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3,
	})
	n, err := d.DrainOnce(ctx, 10)
	// First row is published+committed; the loop then observes the cancel.
	if n != 1 {
		t.Errorf("DrainOnce count = %d; want 1 (first row committed before cancel)", n)
	}
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Errorf("DrainOnce err = %v; want context.Canceled", err)
	}
	if len(store.Published()) != 1 {
		t.Errorf("Published = %v; want exactly [cb1] committed", store.Published())
	}
	pending, _ := store.FetchPending(context.Background(), 10)
	if len(pending) != 1 {
		t.Errorf("pending = %d; want 1 (second row aborted)", len(pending))
	}
}

func TestDispatcher_DrainOnce_MarkPublishedFailure_LoggedAndReturnsError(t *testing.T) {
	t.Parallel()
	store := &failStore{InMemoryStore: outbox.NewInMemoryStore(), publishErr: errors.New("mark published boom")}
	insertRow(t, store.InMemoryStore, "mp", "t1")
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: &recordingBus{}, WorkerID: "w1", MaxAttempts: 3,
	})
	n, err := d.DrainOnce(context.Background(), 10)
	// DrainOnce swallows publishOne errors by design.
	if err != nil {
		t.Fatalf("DrainOnce err = %v; want nil (publishOne errors are recorded, not raised)", err)
	}
	if n != 0 {
		t.Errorf("publish count = %d; want 0 (MarkPublished failed)", n)
	}
	pending, _ := store.FetchPending(context.Background(), 10)
	if len(pending) != 1 {
		t.Errorf("pending = %d; want 1 (row not marked published)", len(pending))
	}
}

func TestDispatcher_DrainOnce_DeadletterFailure_LoggedNotRaised(t *testing.T) {
	t.Parallel()
	store := &failStore{InMemoryStore: outbox.NewInMemoryStore(), deadletterErr: errors.New("dl write boom")}
	insertRow(t, store.InMemoryStore, "dlerr", "t1")
	bus := &recordingBus{fails: 99, failErr: errors.New("permanent")}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 1,
	})
	n, err := d.DrainOnce(context.Background(), 10)
	// attempt = 0+1 >= MaxAttempts(1) → Deadletter fails → warn + return pubErr.
	if err != nil {
		t.Fatalf("DrainOnce err = %v; want nil", err)
	}
	if n != 0 {
		t.Errorf("publish count = %d; want 0", n)
	}
	// Row stays pending because the deadletter write failed.
	pending, _ := store.FetchPending(context.Background(), 10)
	if len(pending) != 1 {
		t.Errorf("pending = %d; want 1 (deadletter write failed)", len(pending))
	}
	if len(store.DeadLetters()) != 0 {
		t.Errorf("DeadLetters = %d; want 0 (write failed)", len(store.DeadLetters()))
	}
}

func TestDispatcher_DrainOnce_MarkFailedFailure_LoggedNotRaised(t *testing.T) {
	t.Parallel()
	store := &failStore{InMemoryStore: outbox.NewInMemoryStore(), failedErr: errors.New("mark failed boom")}
	insertRow(t, store.InMemoryStore, "mferr", "t1")
	bus := &recordingBus{fails: 1, failErr: errors.New("blip")}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3,
	})
	n, err := d.DrainOnce(context.Background(), 10)
	// attempt = 1 < 3 → MarkFailed fails → warn + return pubErr.
	if err != nil {
		t.Fatalf("DrainOnce err = %v; want nil", err)
	}
	if n != 0 {
		t.Errorf("publish count = %d; want 0", n)
	}
	pending, _ := store.FetchPending(context.Background(), 10)
	if len(pending) != 1 {
		t.Errorf("pending = %d; want 1", len(pending))
	}
	if pending[0].RetryCount != 0 {
		t.Errorf("retry_count = %d; want 0 (MarkFailed write failed)", pending[0].RetryCount)
	}
}

func TestDispatcher_Run_PreCancelledContext_ReturnsImmediately(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "pc1", "t1")
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: &recordingBus{}, WorkerID: "w1", MaxAttempts: 3,
		PollInterval: time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := d.Run(ctx, 10); !errors.Is(err, context.Canceled) {
		t.Errorf("Run(pre-cancelled) = %v; want context.Canceled", err)
	}
}

func TestDispatcher_Run_TransientDrainError_LogsAndKeepsPolling(t *testing.T) {
	t.Parallel()
	store := &failStore{InMemoryStore: outbox.NewInMemoryStore(), fetchErr: errors.New("db down")}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: &recordingBus{}, WorkerID: "w1", MaxAttempts: 3,
		PollInterval: time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := d.Run(ctx, 10)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Errorf("Run(transient err) = %v; want context.DeadlineExceeded/Canceled after polling", err)
	}
}

func TestDispatcher_Run_ContextErrorFromDrain_Propagates(t *testing.T) {
	t.Parallel()
	store := &failStore{InMemoryStore: outbox.NewInMemoryStore(), fetchErr: context.Canceled}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: &recordingBus{}, WorkerID: "w1", MaxAttempts: 3,
		PollInterval: time.Millisecond,
	})
	// DrainOnce wraps with %w, so errors.Is unwraps to context.Canceled and
	// Run returns it directly instead of polling.
	err := d.Run(context.Background(), 10)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run(ctx-error) = %v; want context.Canceled", err)
	}
}

func TestDispatcher_ReconstructEnvelope_DefaultsAndFallbacks(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	now := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)

	// Row 1: schema_version key absent → else branch defaults to 1;
	// occurred_at/published_at absent → parseEnvelopeTime fallback.
	r1 := outbox.Row{
		ID: "env-missing", TenantID: "t1", GCID: "00000000-0000-0000-0000-000000000001",
		AggregateType: "post", AggregateID: "env-missing", EventType: "sharing.post.created",
		Topic: "chora.sharing.post.created.v1", Payload: []byte(`{}`),
		Envelope: map[string]string{
			"event_id": "env-missing", "idempotency_key": "idem-env-missing", "tenant_id": "t1",
			"traceparent": "00-deadbeef-01", "source_project": "chora-489812", "source_service": "chora-sharing",
		},
		IdempotencyKey: "idem-env-missing", OccurredAt: now,
	}

	// Row 2: schema_version non-numeric → Sscanf yields 0 → coerced to 1;
	// occurred_at/published_at garbage → both parses fail → fallback.
	r2 := outbox.Row{
		ID: "env-garbage", TenantID: "t1", GCID: "00000000-0000-0000-0000-000000000001",
		AggregateType: "post", AggregateID: "env-garbage", EventType: "sharing.post.created",
		Topic: "chora.sharing.post.created.v1", Payload: []byte(`{}`),
		Envelope: map[string]string{
			"event_id": "env-garbage", "idempotency_key": "idem-env-garbage", "tenant_id": "t1",
			"schema_version": "notanumber", "occurred_at": "not-a-date", "published_at": "also-bogus",
		},
		IdempotencyKey: "idem-env-garbage", OccurredAt: now,
	}
	if err := store.Insert(context.Background(), r1); err != nil {
		t.Fatalf("Insert r1: %v", err)
	}
	if err := store.Insert(context.Background(), r2); err != nil {
		t.Fatalf("Insert r2: %v", err)
	}

	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3,
	})
	n, err := d.DrainOnce(context.Background(), 10)
	if err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if n != 2 {
		t.Fatalf("published = %d; want 2", n)
	}
	if len(bus.calls) != 2 {
		t.Fatalf("bus.calls = %d; want 2", len(bus.calls))
	}
	for i, env := range []struct {
		env cgcenvelope.Envelope
	}{
		{bus.calls[0].Envelope},
		{bus.calls[1].Envelope},
	} {
		if env.env.SchemaVersion != 1 {
			t.Errorf("envelope %d SchemaVersion = %d; want 1 (default/coerced)", i, env.env.SchemaVersion)
		}
		if !env.env.OccurredAt.Equal(now) {
			t.Errorf("envelope %d OccurredAt = %v; want fallback %v", i, env.env.OccurredAt, now)
		}
	}
}

func TestDispatcher_ReconstructEnvelope_NumericSchemaVersion(t *testing.T) {
	t.Parallel()
	// Sanity: a numeric schema_version round-trips through Sscanf unchanged
	// (guards against the fallback coercing valid versions to 1).
	store := outbox.NewInMemoryStore()
	now := time.Now().UTC()
	row := outbox.Row{
		ID: "env-num", TenantID: "t1", AggregateType: "post", AggregateID: "env-num",
		EventType: "sharing.post.created", Topic: "chora.sharing.post.created.v1",
		Payload: []byte(`{}`),
		Envelope: map[string]string{
			"event_id": "env-num", "idempotency_key": "idem-env-num", "tenant_id": "t1",
			"schema_version": "3",
		},
		IdempotencyKey: "idem-env-num", OccurredAt: now,
	}
	if err := store.Insert(context.Background(), row); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3,
	})
	if _, err := d.DrainOnce(context.Background(), 10); err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if len(bus.calls) != 1 {
		t.Fatalf("bus.calls = %d; want 1", len(bus.calls))
	}
	if got := bus.calls[0].Envelope.SchemaVersion; got != 3 {
		t.Errorf("SchemaVersion = %d; want 3 (numeric passes through)", got)
	}
}