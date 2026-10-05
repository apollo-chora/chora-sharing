package events_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apollo-chora/chora-sharing/internal/adapter/events"
)

// TestBootstrapClosureSubscriber_LoadsRealMap loads the on-disk
// consumption PII_Closure_Map.yaml and asserts a fully-wired subscriber is
// returned bound to the canonical inbound topic. Exercises loadPIIMap + the
// bootstrap wiring used by cmd/server/main.go.
func TestBootstrapClosureSubscriber_LoadsRealMap(t *testing.T) {
	// Locate the committed map relative to the service root (this test file
	// lives at internal/adapter/events).
	path := filepath.Join("..", "..", "..", "config", "PII_Closure_Map.yaml")
	if _, err := os.Stat(path); err != nil {
		t.Skipf("real PII map not found at %s: %v", path, err)
	}

	repo := events.NewInMemoryClosureRepo()
	pub := events.NewInMemoryClosurePublisher()

	sub, err := events.BootstrapClosureSubscriber(path, repo, pub, nil)
	require.NoError(t, err)
	require.NotNil(t, sub)
	assert.Equal(t, events.TopicPseudonymiseRequested, sub.SubscribedTopic())
}

// TestBootstrapClosureSubscriber_WrittenMap proves the bootstrap parses a
// well-formed map written to a temp file and the resulting subscriber
// processes a request end-to-end (loadPIIMap → NewClosureSubscriber → Handle).
func TestBootstrapClosureSubscriber_WrittenMap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "PII_Closure_Map.yaml")
	yaml := `domain: chora_sharing
version: "1.0"
fields_to_tokenize:
  - table: atomic_session
    columns:
      - column: notes
        strategy: drop
        value: ""
      - column: actor_display_name
        strategy: tombstone_string
        value: "Former member"
on_creator_closure:
  strategy: cascade_pseudonymise
  show_authorship_as: "Former member"
`
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o600))

	repo := events.NewInMemoryClosureRepo()
	pub := events.NewInMemoryClosurePublisher()
	sub, err := events.BootstrapClosureSubscriber(path, repo, pub, nil)
	require.NoError(t, err)

	// And it actually processes a closure request using the loaded spec.
	require.NoError(t, sub.Handle(t.Context(), events.PseudonymiseRequestedPayload{
		SagaID:   "saga-1",
		Gcid:     "gcid-1",
		TenantID: "tenant-1",
	}))
	completed := pub.ClosureRecordedByTopic(events.TopicPseudonymiseCompleted)
	require.Len(t, completed, 1)
	assert.Equal(t, "ok", completed[0].Payload["status"])
	// Two columns declared → repo reports two rows touched.
	assert.Equal(t, 2, completed[0].Payload["rows_touched"])
}

// TestBootstrapClosureSubscriber_MissingFile asserts the bootstrap surfaces a
// load error (rather than panicking or returning a nil-but-no-error
// subscriber) when the map path does not exist.
func TestBootstrapClosureSubscriber_MissingFile(t *testing.T) {
	repo := events.NewInMemoryClosureRepo()
	pub := events.NewInMemoryClosurePublisher()
	sub, err := events.BootstrapClosureSubscriber(filepath.Join(t.TempDir(), "nope.yaml"), repo, pub, nil)
	require.Error(t, err)
	assert.Nil(t, sub)
}

// TestBootstrapClosureSubscriber_InvalidMap asserts a map with an
// unsupported tokenisation strategy fails validation during load.
func TestBootstrapClosureSubscriber_InvalidMap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	yaml := `domain: chora_sharing
fields_to_tokenize:
  - table: t1
    columns:
      - column: c1
        strategy: not_a_real_strategy
`
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o600))

	repo := events.NewInMemoryClosureRepo()
	pub := events.NewInMemoryClosurePublisher()
	sub, err := events.BootstrapClosureSubscriber(path, repo, pub, nil)
	require.Error(t, err)
	assert.Nil(t, sub)
}
