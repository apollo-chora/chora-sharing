// moderation_gke_wiring_test.go — white-box coverage for the moderation
// engine's GKE web-mode wiring helper (ADR-169 / CHO-1631 — Vertex AI Agent
// Engine decommissioned; the content_moderation crew runs on GKE).
//
// buildModerationGKEClient constructs the cluster-local plaintext GKE client
// (no ADC token, injected HTTP doer ⇒ no dial on construction), so the branch
// is exercisable without network or credentials.
package main

import "testing"

func TestBuildModerationGKEClient_ReturnsPort(t *testing.T) {
	me, err := buildModerationGKEClient("http://moderation-gke.invalid:8080")
	if err != nil {
		t.Fatalf("buildModerationGKEClient err = %v; want nil", err)
	}
	if me == nil {
		t.Fatal("buildModerationGKEClient returned a nil ModerationEnginePort")
	}
}
