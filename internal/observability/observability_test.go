// Package observability_test verifies the OTel SDK wiring surface for
// chora-sharing.
package observability_test

import (
	"testing"

	obs "github.com/apollo-chora/chora-sharing/internal/observability"
)

func TestServiceName_IsChoraSharing(t *testing.T) {
	t.Parallel()
	if obs.ServiceName != "chora-sharing" {
		t.Fatalf("expected ServiceName=chora-sharing, got %q", obs.ServiceName)
	}
}
