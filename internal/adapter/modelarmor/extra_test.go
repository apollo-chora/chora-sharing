package modelarmor

// Extra branch + error-path coverage for adapter.go and resolver.go that
// the canonical tests in adapter_test.go do not exercise: the adapter's
// resolve-error wrap, no-match filter rows, the Allow-with-match `pass`
// decision mapping, permissive-template de-duplication / permissive
// default, the file-based loader, resolver validation errors, and the
// nil-receiver / empty-agent_id guards.
//
// Reuses the fakes + harness from adapter_test.go (StubScreener,
// mustResolver, minimalYAML, testProject/testLocation/testEnv).

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	cgcmodelarmor "github.com/apollo-chora/chora-common/modelarmor"
)

// TestAdapter_Screen_ResolveError_PropagatesWrapped pins the fail-loud
// path when the template resolver rejects the request's agent_id — the
// adapter must surface the resolve failure as a wrapped error instead of
// calling the screener.
func TestAdapter_Screen_ResolveError_PropagatesWrapped(t *testing.T) {
	t.Parallel()
	stub := cgcmodelarmor.NewStubScreener()
	adapter, err := NewAdapter(stub, mustResolver(t))
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	_, err = adapter.Screen(context.Background(), ScreenRequest{AgentID: ""})
	if err == nil || !strings.Contains(err.Error(), "resolve template") {
		t.Fatalf("Screen err = %v; want wrapped resolve-template error", err)
	}
}

// TestAdapter_Screen_Allow_DropsNoMatchFilters verifies that filter rows
// whose MatchState != MATCH_FOUND are skipped — the audit trail only
// carries MATCH_FOUND hits, so an all-no-match response yields zero
// violations.
func TestAdapter_Screen_Allow_DropsNoMatchFilters(t *testing.T) {
	t.Parallel()
	stub := cgcmodelarmor.NewStubScreener()
	stub.SetUserPromptResult(func(req cgcmodelarmor.ScreenRequest) (cgcmodelarmor.ScreenResult, error) {
		return cgcmodelarmor.ScreenResult{
			Verdict: cgcmodelarmor.VerdictAllow,
			Filters: []cgcmodelarmor.FilterHit{
				{
					FilterName: cgcmodelarmor.FilterNameRAI,
					MatchState: cgcmodelarmor.MatchStateNoMatchFound,
				},
			},
		}, nil
	})
	adapter, err := NewAdapter(stub, mustResolver(t))
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	got, err := adapter.Screen(context.Background(), ScreenRequest{
		TenantID:   "tenant-x",
		AuthorGCID: "gcid-clean",
		AgentID:    AgentIDSocialModeration,
		Content:    "Totally benign.",
	})
	if err != nil {
		t.Fatalf("Screen: %v", err)
	}
	if got.Verdict != VerdictApproved {
		t.Errorf("Verdict = %q; want %q", got.Verdict, VerdictApproved)
	}
	if len(got.Violations) != 0 {
		t.Errorf("Violations = %v; want none (no MATCH_FOUND rows)", got.Violations)
	}
}

// TestAdapter_Screen_AllowWithMatch_SurfacesPassDecision pins the `pass`
// per-decision Result mapping for an Allow verdict that still carries a
// MATCH_FOUND row — such rows must not be dropped from the audit trail.
func TestAdapter_Screen_AllowWithMatch_SurfacesPassDecision(t *testing.T) {
	t.Parallel()
	stub := cgcmodelarmor.NewStubScreener()
	stub.SetUserPromptResult(func(req cgcmodelarmor.ScreenRequest) (cgcmodelarmor.ScreenResult, error) {
		return cgcmodelarmor.ScreenResult{
			Verdict: cgcmodelarmor.VerdictAllow,
			Filters: []cgcmodelarmor.FilterHit{
				{
					FilterName: cgcmodelarmor.FilterNameMaliciousURI,
					MatchState: cgcmodelarmor.MatchStateMatchFound,
					Severity:   cgcmodelarmor.SeverityLow,
				},
			},
		}, nil
	})
	adapter, err := NewAdapter(stub, mustResolver(t))
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	got, err := adapter.Screen(context.Background(), ScreenRequest{
		TenantID:   "tenant-x",
		AuthorGCID: "gcid-x",
		AgentID:    AgentIDSocialModeration,
		Content:    "Edge case.",
	})
	if err != nil {
		t.Fatalf("Screen: %v", err)
	}
	if got.Verdict != VerdictApproved {
		t.Errorf("Verdict = %q; want %q", got.Verdict, VerdictApproved)
	}
	if len(got.Violations) != 1 {
		t.Fatalf("Violations len = %d; want 1", len(got.Violations))
	}
	v := got.Violations[0]
	if v.Result != "pass" {
		t.Errorf("Violation.Result = %q; want pass", v.Result)
	}
	if v.Tier != "model_armor" {
		t.Errorf("Violation.Tier = %q; want model_armor", v.Tier)
	}
	if v.ViolationType != "malicious_uri" {
		t.Errorf("Violation.ViolationType = %q; want malicious_uri", v.ViolationType)
	}
}

// twoPermissiveAgentsYAML maps two distinct agent_ids to the same
// permissive tier so PermissiveTemplates must de-duplicate the identical
// resolved template resource name.
const twoPermissiveAgentsYAML = `
version: 1
schema: chora-contracts/v1/agent-guardrail-mapping
agents:
  legacy_agent_a:
    template_tier: permissive
    rationale: "Audit-only legacy surface."
  legacy_agent_b:
    template_tier: permissive
    rationale: "Audit-only legacy surface."
default:
  template_tier: strict
  rationale: "Unknown agent_id — fall back to strict."
`

// TestTemplateResolver_PermissiveTemplates_Deduplicates ensures the same
// permissive template resource name is listed once even when multiple
// agents share the tier.
func TestTemplateResolver_PermissiveTemplates_Deduplicates(t *testing.T) {
	t.Parallel()
	resolver, err := LoadResolverFromBytes([]byte(twoPermissiveAgentsYAML), testProject, testLocation, testEnv)
	if err != nil {
		t.Fatalf("LoadResolverFromBytes: %v", err)
	}
	got := resolver.PermissiveTemplates()
	want := "projects/chora-489812/locations/us-central1/templates/chora-guardrail-permissive-dev"
	if len(got) != 1 || got[0] != want {
		t.Fatalf("PermissiveTemplates() = %v; want single deduped [%q]", got, want)
	}
}

// permissiveDefaultYAML declares a permissive default tier with no
// permissive agents — the unknown-agent fallback template must still be
// registered as INSPECT_ONLY by the caller.
const permissiveDefaultYAML = `
version: 1
schema: chora-contracts/v1/agent-guardrail-mapping
agents:
  social_moderation:
    template_tier: strict
    rationale: "Strict tier."
default:
  template_tier: permissive
  rationale: "Unknown agent_id — audit only."
`

// TestTemplateResolver_PermissiveTemplates_IncludesPermissiveDefault
// verifies the default-tier permissive template lands in the audit-only
// list and that unknown agents resolve to it.
func TestTemplateResolver_PermissiveTemplates_IncludesPermissiveDefault(t *testing.T) {
	t.Parallel()
	resolver, err := LoadResolverFromBytes([]byte(permissiveDefaultYAML), testProject, testLocation, testEnv)
	if err != nil {
		t.Fatalf("LoadResolverFromBytes: %v", err)
	}
	got := resolver.PermissiveTemplates()
	want := "projects/chora-489812/locations/us-central1/templates/chora-guardrail-permissive-dev"
	if len(got) != 1 || got[0] != want {
		t.Fatalf("PermissiveTemplates() = %v; want [%q]", got, want)
	}
	resolved, err := resolver.Resolve("some_unknown_agent")
	if err != nil {
		t.Fatalf("Resolve(unknown): %v", err)
	}
	if resolved != want {
		t.Errorf("Resolve(unknown) = %q; want permissive default %q", resolved, want)
	}
}

// TestLoadResolverFromFile_MissingFile pins the fail-loud wrapped read
// error.
func TestLoadResolverFromFile_MissingFile(t *testing.T) {
	t.Parallel()
	_, err := LoadResolverFromFile(filepath.Join(t.TempDir(), "no-such-mapping.yaml"), testProject, testLocation, testEnv)
	if err == nil || !strings.Contains(err.Error(), "modelarmor resolver: read") {
		t.Fatalf("LoadResolverFromFile(missing) err = %v; want wrapped read error", err)
	}
}

// TestLoadResolverFromFile_LoadsValidFile drives the file-backed loader
// end-to-end with a real temp mapping file.
func TestLoadResolverFromFile_LoadsValidFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "mapping.yaml")
	if err := os.WriteFile(path, []byte(minimalYAML), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	resolver, err := LoadResolverFromFile(path, testProject, testLocation, testEnv)
	if err != nil {
		t.Fatalf("LoadResolverFromFile: %v", err)
	}
	got, err := resolver.Resolve(AgentIDSocialModeration)
	if err != nil {
		t.Fatalf("Resolve(%q): %v", AgentIDSocialModeration, err)
	}
	want := "projects/chora-489812/locations/us-central1/templates/chora-guardrail-strict-dev"
	if got != want {
		t.Fatalf("Resolve(%q) = %q; want %q", AgentIDSocialModeration, got, want)
	}
}

// TestLoadResolverFromBytes_RejectsMissingIdentifiers pins the required
// env-var guards — no inline defaults per the env-only config posture.
func TestLoadResolverFromBytes_RejectsMissingIdentifiers(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		project   string
		location  string
		env       string
		wantError string
	}{
		{"project", "", testLocation, testEnv, "project required"},
		{"location", testProject, "", testEnv, "location required"},
		{"environment", testProject, testLocation, "", "environment required"},
	}
	for _, tc := range cases {
		_, err := LoadResolverFromBytes([]byte(minimalYAML), tc.project, tc.location, tc.env)
		if err == nil || !strings.Contains(err.Error(), tc.wantError) {
			t.Errorf("%s: err = %v; want %q", tc.name, err, tc.wantError)
		}
	}
}

// TestLoadResolverFromBytes_RejectsMalformedYAML ensures a YAML syntax
// error surfaces as a wrapped parse failure.
func TestLoadResolverFromBytes_RejectsMalformedYAML(t *testing.T) {
	t.Parallel()
	_, err := LoadResolverFromBytes([]byte("agents: [unterminated"), testProject, testLocation, testEnv)
	if err == nil || !strings.Contains(err.Error(), "yaml parse") {
		t.Fatalf("LoadResolverFromBytes(malformed) err = %v; want yaml parse error", err)
	}
}

// noDefaultYAML omits the default tier entirely — the loader must fall
// back to strict (safe fail-loud).
const noDefaultYAML = `
version: 1
schema: chora-contracts/v1/agent-guardrail-mapping
agents:
  social_moderation:
    template_tier: strict
    rationale: "Strict tier."
`

// TestLoadResolverFromBytes_NoDefault_FallsBackToStrict pins the strict
// fallback when the mapping file declares no default tier.
func TestLoadResolverFromBytes_NoDefault_FallsBackToStrict(t *testing.T) {
	t.Parallel()
	resolver, err := LoadResolverFromBytes([]byte(noDefaultYAML), testProject, testLocation, testEnv)
	if err != nil {
		t.Fatalf("LoadResolverFromBytes: %v", err)
	}
	got, err := resolver.Resolve("unlisted_agent")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	want := "projects/chora-489812/locations/us-central1/templates/chora-guardrail-strict-dev"
	if got != want {
		t.Errorf("Resolve(unknown) = %q; want default-strict %q", got, want)
	}
}

// badDefaultYAML carries an illegal default tier — the loader must fail
// at boot rather than at first call.
const badDefaultYAML = `
version: 1
agents:
  social_moderation:
    template_tier: strict
default:
  template_tier: bogus
`

// TestLoadResolverFromBytes_RejectsUnknownDefaultTier pins the default
// tier validation branch.
func TestLoadResolverFromBytes_RejectsUnknownDefaultTier(t *testing.T) {
	t.Parallel()
	_, err := LoadResolverFromBytes([]byte(badDefaultYAML), testProject, testLocation, testEnv)
	if err == nil || !strings.Contains(err.Error(), "default tier") {
		t.Fatalf("LoadResolverFromBytes err = %v; want unknown-default-tier error", err)
	}
}

// TestTemplateResolver_Resolve_NilReceiver pins the nil-receiver guard.
func TestTemplateResolver_Resolve_NilReceiver(t *testing.T) {
	t.Parallel()
	var r *TemplateResolver
	_, err := r.Resolve(AgentIDSocialModeration)
	if err == nil || !strings.Contains(err.Error(), "nil receiver") {
		t.Fatalf("nil Resolve err = %v; want nil receiver error", err)
	}
}

// TestTemplateResolver_Resolve_EmptyAgentID pins the agent_id-required
// guard (whitespace-only ids rejected too).
func TestTemplateResolver_Resolve_EmptyAgentID(t *testing.T) {
	t.Parallel()
	resolver := mustResolver(t)
	for _, id := range []string{"", "   "} {
		_, err := resolver.Resolve(id)
		if err == nil || !strings.Contains(err.Error(), "agent_id required") {
			t.Errorf("Resolve(%q) err = %v; want agent_id required", id, err)
		}
	}
}
