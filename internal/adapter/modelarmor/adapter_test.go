package modelarmor

import (
	"context"
	"errors"
	"strings"
	"testing"

	cgcmodelarmor "github.com/apollo-chora/chora-common/modelarmor"
)

const (
	testProject  = "chora-489812"
	testLocation = "us-central1"
	testEnv      = "dev"
)

// minimalYAML returns the smallest valid agent-guardrail-mapping.yaml
// shape the resolver accepts. Mirrors the canonical file at
// chora-contracts/yaml/agent-guardrail-mapping.yaml for the
// `social_moderation` agent_id (tier: strict).
const minimalYAML = `
version: 1
schema: chora-contracts/v1/agent-guardrail-mapping
agents:
  social_moderation:
    template_tier: strict
    rationale: "Social posts + replies pre-publish; blocks RAI + PI + URI."
  qgen_question_generator:
    template_tier: balanced
    rationale: "Learner-issued atom-authoring prompts."
  closure_saga_orchestrator:
    template_tier: permissive
    rationale: "Internal compensation messages."
default:
  template_tier: strict
  rationale: "Unknown agent_id — fall back to strict."
`

// TestTemplateResolver_SocialModeration_ResolvesToStrictTemplate is the
// canonical tier-resolution test required by the rewire task. It
// verifies that the `social_moderation` agent_id (per
// chora-contracts/yaml/agent-guardrail-mapping.yaml) resolves to the
// full Cloud Model Armor template resource name
// `projects/chora-489812/locations/us-central1/templates/chora-guardrail-strict-dev`.
func TestTemplateResolver_SocialModeration_ResolvesToStrictTemplate(t *testing.T) {
	t.Parallel()
	resolver, err := LoadResolverFromBytes([]byte(minimalYAML), testProject, testLocation, testEnv)
	if err != nil {
		t.Fatalf("LoadResolverFromBytes: %v", err)
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

// TestTemplateResolver_UnknownAgent_FallsBackToDefaultStrict pins the
// safe-fallback policy — unknown agent_id → default tier (strict per
// the canonical YAML).
func TestTemplateResolver_UnknownAgent_FallsBackToDefaultStrict(t *testing.T) {
	t.Parallel()
	resolver, err := LoadResolverFromBytes([]byte(minimalYAML), testProject, testLocation, testEnv)
	if err != nil {
		t.Fatalf("LoadResolverFromBytes: %v", err)
	}
	got, err := resolver.Resolve("never_registered_agent")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	want := "projects/chora-489812/locations/us-central1/templates/chora-guardrail-strict-dev"
	if got != want {
		t.Fatalf("Resolve(unknown) = %q; want default-strict %q", got, want)
	}
}

// TestTemplateResolver_PermissiveTemplates_EnumeratesAuditOnly verifies
// that the resolver lists exactly the permissive-tier templates so the
// caller can register them as INSPECT_ONLY on the SDK Client.
func TestTemplateResolver_PermissiveTemplates_EnumeratesAuditOnly(t *testing.T) {
	t.Parallel()
	resolver, err := LoadResolverFromBytes([]byte(minimalYAML), testProject, testLocation, testEnv)
	if err != nil {
		t.Fatalf("LoadResolverFromBytes: %v", err)
	}
	got := resolver.PermissiveTemplates()
	want := "projects/chora-489812/locations/us-central1/templates/chora-guardrail-permissive-dev"
	if len(got) != 1 || got[0] != want {
		t.Fatalf("PermissiveTemplates() = %v; want [%q]", got, want)
	}
}

// TestTemplateResolver_RejectsUnknownTier ensures yaml load surfaces
// invalid tier values at boot rather than at first call.
func TestTemplateResolver_RejectsUnknownTier(t *testing.T) {
	t.Parallel()
	bad := `
version: 1
agents:
  foo:
    template_tier: bogus
default:
  template_tier: strict
`
	_, err := LoadResolverFromBytes([]byte(bad), testProject, testLocation, testEnv)
	if err == nil {
		t.Fatal("LoadResolverFromBytes: expected error for unknown tier")
	}
	if !strings.Contains(err.Error(), "bogus") {
		t.Fatalf("error %v missing offending tier name", err)
	}
}

// TestAdapter_Screen_Allow_MapsToApproved drives the adapter with a
// StubScreener that returns the canonical Allow verdict.
func TestAdapter_Screen_Allow_MapsToApproved(t *testing.T) {
	t.Parallel()
	stub := cgcmodelarmor.NewStubScreener()
	resolver := mustResolver(t)
	adapter, err := NewAdapter(stub, resolver)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	got, err := adapter.Screen(context.Background(), ScreenRequest{
		TenantID:   "tenant-x",
		AuthorGCID: "gcid-phyllis",
		AgentID:    AgentIDSocialModeration,
		Content:    "Just earned my CSPO from MTM!",
	})
	if err != nil {
		t.Fatalf("Screen: %v", err)
	}
	if got.Verdict != VerdictApproved {
		t.Errorf("Verdict = %q; want %q", got.Verdict, VerdictApproved)
	}
	if len(got.Violations) != 0 {
		t.Errorf("Violations = %v; want none on approved", got.Violations)
	}
}

// TestAdapter_Screen_Block_MapsToBlockedWithViolations drives the adapter
// with a StubScreener returning MATCH_FOUND on RAI and asserts the
// adapter surfaces the violation rows so audit captures the filter that
// fired.
func TestAdapter_Screen_Block_MapsToBlockedWithViolations(t *testing.T) {
	t.Parallel()
	stub := cgcmodelarmor.NewStubScreener()
	stub.SetUserPromptResult(func(req cgcmodelarmor.ScreenRequest) (cgcmodelarmor.ScreenResult, error) {
		return cgcmodelarmor.ScreenResult{
			Verdict: cgcmodelarmor.VerdictBlock,
			Reason:  "rai:HATE_SPEECH",
			Filters: []cgcmodelarmor.FilterHit{
				{
					FilterName:  cgcmodelarmor.FilterNameRAI,
					MatchState:  cgcmodelarmor.MatchStateMatchFound,
					Severity:    cgcmodelarmor.SeverityHigh,
					Subcategory: "HATE_SPEECH",
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
		AuthorGCID: "gcid-bad",
		AgentID:    AgentIDSocialModeration,
		Content:    "Posting hateful content.",
	})
	if err != nil {
		t.Fatalf("Screen: %v", err)
	}
	if got.Verdict != VerdictBlocked {
		t.Errorf("Verdict = %q; want %q", got.Verdict, VerdictBlocked)
	}
	if len(got.Violations) != 1 {
		t.Fatalf("Violations len = %d; want 1", len(got.Violations))
	}
	v := got.Violations[0]
	if v.Tier != "model_armor" {
		t.Errorf("Violation.Tier = %q; want model_armor", v.Tier)
	}
	if v.Result != "block" {
		t.Errorf("Violation.Result = %q; want block", v.Result)
	}
	if v.ViolationType != "rai:HATE_SPEECH" {
		t.Errorf("Violation.ViolationType = %q; want rai:HATE_SPEECH", v.ViolationType)
	}
}

// TestAdapter_Screen_InspectOnly_MapsToFlagged drives the adapter with
// the canonical InspectOnly verdict (audit-only template).
func TestAdapter_Screen_InspectOnly_MapsToFlagged(t *testing.T) {
	t.Parallel()
	stub := cgcmodelarmor.NewStubScreener()
	stub.SetUserPromptResult(func(req cgcmodelarmor.ScreenRequest) (cgcmodelarmor.ScreenResult, error) {
		return cgcmodelarmor.ScreenResult{
			Verdict: cgcmodelarmor.VerdictInspectOnly,
			Reason:  "pi_and_jailbreak:audit-only",
			Filters: []cgcmodelarmor.FilterHit{
				{
					FilterName: cgcmodelarmor.FilterNamePIAndJailbreak,
					MatchState: cgcmodelarmor.MatchStateMatchFound,
					Severity:   cgcmodelarmor.SeverityMedium,
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
		AuthorGCID: "gcid-y",
		AgentID:    AgentIDSocialModeration,
		Content:    "Audit me.",
	})
	if err != nil {
		t.Fatalf("Screen: %v", err)
	}
	if got.Verdict != VerdictFlagged {
		t.Errorf("Verdict = %q; want %q", got.Verdict, VerdictFlagged)
	}
	if len(got.Violations) != 1 || got.Violations[0].Result != "flag" {
		t.Errorf("Violations = %+v; want one with Result=flag", got.Violations)
	}
}

// TestAdapter_Screen_PropagatesTemplateName ensures the resolver's
// strict-tier template name reaches the SDK call site verbatim.
func TestAdapter_Screen_PropagatesTemplateName(t *testing.T) {
	t.Parallel()
	stub := cgcmodelarmor.NewStubScreener()
	var captured cgcmodelarmor.ScreenRequest
	stub.SetUserPromptResult(func(req cgcmodelarmor.ScreenRequest) (cgcmodelarmor.ScreenResult, error) {
		captured = req
		return cgcmodelarmor.ScreenResult{Verdict: cgcmodelarmor.VerdictAllow}, nil
	})
	adapter, err := NewAdapter(stub, mustResolver(t))
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	if _, err := adapter.Screen(context.Background(), ScreenRequest{
		TenantID:   "tenant-z",
		AuthorGCID: "gcid-z",
		AgentID:    AgentIDSocialModeration,
		Content:    "Hello world.",
	}); err != nil {
		t.Fatalf("Screen: %v", err)
	}
	wantTemplate := "projects/chora-489812/locations/us-central1/templates/chora-guardrail-strict-dev"
	if captured.TemplateName != wantTemplate {
		t.Errorf("SDK call TemplateName = %q; want %q", captured.TemplateName, wantTemplate)
	}
	if captured.AgentID != AgentIDSocialModeration {
		t.Errorf("SDK call AgentID = %q; want %q", captured.AgentID, AgentIDSocialModeration)
	}
	if captured.GCID != "gcid-z" {
		t.Errorf("SDK call GCID = %q; want gcid-z (author_gcid propagation)", captured.GCID)
	}
}

// TestAdapter_Screen_PropagatesSDKError ensures SDK failures are
// surfaced as wrapped errors (fail-loud per feedback_resilience_priority).
func TestAdapter_Screen_PropagatesSDKError(t *testing.T) {
	t.Parallel()
	stub := cgcmodelarmor.NewStubScreener()
	stub.SetUserPromptResult(func(req cgcmodelarmor.ScreenRequest) (cgcmodelarmor.ScreenResult, error) {
		return cgcmodelarmor.ScreenResult{}, cgcmodelarmor.ErrStubExplicit
	})
	adapter, err := NewAdapter(stub, mustResolver(t))
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	_, err = adapter.Screen(context.Background(), ScreenRequest{
		TenantID:   "t",
		AuthorGCID: "g",
		AgentID:    AgentIDSocialModeration,
		Content:    "x",
	})
	if err == nil || !errors.Is(err, cgcmodelarmor.ErrStubExplicit) {
		t.Fatalf("Screen err = %v; want wrapped ErrStubExplicit", err)
	}
}

// TestNewAdapter_RejectsNilDeps pins the constructor's nil-guard.
func TestNewAdapter_RejectsNilDeps(t *testing.T) {
	t.Parallel()
	_, err := NewAdapter(nil, mustResolver(t))
	if err == nil {
		t.Error("NewAdapter(nil screener): expected error")
	}
	_, err = NewAdapter(cgcmodelarmor.NewStubScreener(), nil)
	if err == nil {
		t.Error("NewAdapter(nil resolver): expected error")
	}
}

// mustResolver builds a resolver from the canonical minimal YAML or
// fatally fails the test.
func mustResolver(t *testing.T) *TemplateResolver {
	t.Helper()
	r, err := LoadResolverFromBytes([]byte(minimalYAML), testProject, testLocation, testEnv)
	if err != nil {
		t.Fatalf("LoadResolverFromBytes: %v", err)
	}
	return r
}
