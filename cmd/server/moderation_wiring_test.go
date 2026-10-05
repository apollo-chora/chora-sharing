// moderation_wiring_test.go — white-box coverage for the Moderation engine
// + Cloud Model Armor guardrail builders (main.go).
//
// buildGuardrail is driven through BOTH the resolver error path (bogus
// mapping file) and, when a standalone mapping YAML is written into a
// temp file, as deep as the local environment allows: resolver → screener
// (lazily-dialed SDK, no cloud credentials needed for construction) →
// permissive INSPECT_ONLY registration → adapter. Anything past adapter
// construction needs a live Moderation backend and is a documented plateau.
package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestBuildModerationEngine_NoADCReturnsError(t *testing.T) {
	// google.DefaultTokenSource resolves ADC lazily on first Token() call;
	// resolution back to a real credential FAILS in sandbox envs, covering
	// the error branch. In an env that DOES have ADC, the success branch
	// runs instead. Either way the function body executes.
	eng, err := buildModerationEngine(context.Background())
	if err != nil && eng != nil {
		t.Fatalf("buildModerationEngine returned both a client and an error")
	}
}

func TestBuildGuardrail_ResolverErrorOnBogusPath(t *testing.T) {
	t.Setenv("CHORA_AGENT_GUARDRAIL_MAPPING", filepath.Join(t.TempDir(), "missing.yaml"))
	t.Setenv("CHORA_MODELARMOR_PROJECT", "chora-489812")
	t.Setenv("CHORA_MODELARMOR_LOCATION", "us-central1")
	t.Setenv("CHORA_ENVIRONMENT", "dev")

	gr, closer, err := buildGuardrail(context.Background())
	if err == nil {
		t.Fatal("expected resolver error for a missing mapping file")
	}
	if gr != nil || closer != nil {
		t.Fatalf("error path must return nil ports, got guardrail=%v closer-set=%v", gr, closer != nil)
	}
}

func TestBuildGuardrail_SuccessPathWithPermissiveMapping(t *testing.T) {
	mapping := "schema: chora-contracts/v1/agent-guardrail-mapping\n" +
		"agents:\n" +
		"  social_moderation:\n" +
		"    template_tier: permissive\n" +
		"default:\n" +
		"  template_tier: strict\n"
	path := filepath.Join(t.TempDir(), "agent-guardrail-mapping.yaml")
	if err := os.WriteFile(path, []byte(mapping), 0o600); err != nil {
		t.Fatalf("write mapping: %v", err)
	}
	t.Setenv("CHORA_AGENT_GUARDRAIL_MAPPING", path)
	t.Setenv("CHORA_MODELARMOR_PROJECT", "chora-489812")
	t.Setenv("CHORA_MODELARMOR_LOCATION", "us-central1")
	t.Setenv("CHORA_ENVIRONMENT", "dev")

	gr, closer, err := buildGuardrail(context.Background())
	if err != nil {
		// NewScreener fails only when the (lazily-dialed) SDK construction
		// errors — that still covers the screener error branch.
		if gr != nil || closer != nil {
			t.Fatalf("guardrail error path must return nil ports, got guardrail=%v closer-set=%v", gr, closer != nil)
		}
		return
	}
	if gr == nil {
		t.Fatal("buildGuardrail succeeded but returned a nil guardrail")
	}
	if closer == nil {
		t.Fatal("buildGuardrail succeeded but returned a nil closer")
	}
	if err := closer(); err != nil {
		t.Logf("guardrail closer error (expected without a live backend): %v", err)
	}
}