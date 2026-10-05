// Package modelarmor adapts the canonical Cloud Model Armor Screener
// (libs/chora-go-common/modelarmor) to the chora-sharing Moderation
// crew's GuardrailPort contract.
//
// Per ADR-152 (chora-guardrail superseded by Cloud Model Armor) the
// chora-sharing Moderation crew no longer hops through the retired
// chora-guardrail Cloud Run service. Pre-LLM screening calls (the post
// text before Moderator + Critic run) now flow directly from
// chora-sharing to Cloud Model Armor via the Vertex AI SDK.
//
// Hexagonal boundary: this is an OUTBOUND ADAPTER. It wraps the
// canonical Screener interface from chora-go-common + implements the
// GuardrailPort port consumed by the postsHandler. Domain code NEVER
// imports this package.
//
// Per-agent risk tier comes from `chora-contracts/yaml/agent-guardrail-mapping.yaml`
// — loaded at boot via the TemplateResolver — and surfaced as the full
// template resource name:
//
//	projects/{CHORA_MODELARMOR_PROJECT}/locations/{CHORA_MODELARMOR_LOCATION}/templates/chora-guardrail-{tier}-{CHORA_ENVIRONMENT}
//
// The Moderation crew's agent_id (`social_moderation`) resolves to tier
// `strict` per the YAML mapping — RAI + PI + URI block on match.
package modelarmor

import (
	"context"
	"errors"
	"fmt"

	cgcmodelarmor "github.com/apollo-chora/chora-common/modelarmor"
)

// AgentIDSocialModeration is the canonical agent_id for the chora-sharing
// Moderation crew. Resolves to tier `strict` in
// chora-contracts/yaml/agent-guardrail-mapping.yaml.
const AgentIDSocialModeration = "social_moderation"

// GuardrailPort is the chora-sharing-facing port the Moderation crew
// consumes. It exposes a single Screen call over the user-generated
// post text BEFORE the Moderator + Critic sub-agents run. The legacy
// HTTP-to-chora-guardrail integration emitted the same conceptual
// contract; this port preserves the method signature so the call site
// in postsHandler is one-line.
//
// The port lives in the adapter package (not domain) because no other
// chora-sharing code consumes it — the postsHandler is the sole caller.
// If a second consumer surfaces later, lift the interface to a domain
// ports file.
type GuardrailPort interface {
	// Screen runs Cloud Model Armor's SanitizeUserPrompt over the
	// supplied content. Returns a verdict the caller switches on; on
	// Block the caller MUST short-circuit + emit a violation event.
	Screen(ctx context.Context, req ScreenRequest) (ScreenVerdict, error)
}

// ScreenRequest is the chora-sharing → Guardrail input. Mirrors the
// Reporter shape in chora-agent-executor but specialised for the social-
// moderation use case (no AGID — posts are user-issued).
type ScreenRequest struct {
	// TenantID — Chora tenant under which the post is being created.
	TenantID string

	// AuthorGCID — Global Chora ID of the post author. Surfaces on the
	// emitted OTel span + violation event.
	AuthorGCID string

	// AgentID — Chora agent identifier for the consuming crew. For the
	// chora-sharing Moderation crew this is `social_moderation`
	// (see AgentIDSocialModeration). The resolver maps this to the
	// canonical risk tier.
	AgentID string

	// Content — the post text to screen.
	Content string
}

// ScreenVerdict is the guardrail's reply. The string vocabulary
// (`approved` | `blocked` | `flagged`) intentionally mirrors the legacy
// chora-guardrail HTTP envelope so the postsHandler's switch is
// vocabulary-compatible if/when other surfaces wire the same port.
type ScreenVerdict struct {
	// Verdict — `approved` (allow), `blocked` (hard block), `flagged`
	// (INSPECT_ONLY audit — caller MAY proceed but MUST emit a
	// violation event).
	Verdict string

	// Reason — short human-readable summary of which filter(s) hit.
	// Empty when Verdict == `approved`.
	Reason string

	// Violations — per-filter audit trail. Empty for approved verdicts.
	Violations []GuardrailDecision
}

// GuardrailDecision captures one filter's outcome for audit. One row per
// MATCH_FOUND filter hit.
type GuardrailDecision struct {
	// Tier — always `model_armor` for this adapter (vs the legacy
	// `regex` / `dlp` / `model_armor` tiering chora-guardrail emitted).
	Tier string

	// Result — `block` | `flag` | `pass` — canonical per-decision
	// vocabulary mirroring the executor adapter.
	Result string

	// ViolationType — `{filter_name}[:subcategory]` (e.g. `rai:HATE_SPEECH`,
	// `pi_and_jailbreak`).
	ViolationType string
}

// Canonical verdict strings.
const (
	VerdictApproved = "approved"
	VerdictBlocked  = "blocked"
	VerdictFlagged  = "flagged"
)

// Adapter wraps cgcmodelarmor.Screener + an agent→template resolver and
// implements GuardrailPort. The Moderation crew calls Screen(ctx, req);
// the adapter resolves the agent's template tier, builds the full
// resource name, and dispatches to Screener.SanitizeUserPrompt.
type Adapter struct {
	screener cgcmodelarmor.Screener
	resolver *TemplateResolver
}

// Compile-time check that *Adapter implements GuardrailPort.
var _ GuardrailPort = (*Adapter)(nil)

// NewAdapter wires the canonical Screener with a template resolver. The
// caller is responsible for SDK lifecycle (defer screener.Close()).
func NewAdapter(screener cgcmodelarmor.Screener, resolver *TemplateResolver) (*Adapter, error) {
	if screener == nil {
		return nil, errors.New("modelarmor adapter: screener required")
	}
	if resolver == nil {
		return nil, errors.New("modelarmor adapter: resolver required")
	}
	return &Adapter{screener: screener, resolver: resolver}, nil
}

// Screen implements GuardrailPort. The post text is screened as the LLM
// INPUT (pre-LLM) via Cloud Model Armor's SanitizeUserPrompt path —
// user-generated content fed into the downstream Moderator + Critic
// sub-agents. The verdict mapping:
//
//   - VerdictAllow       → "approved"
//   - VerdictBlock       → "blocked"
//   - VerdictInspectOnly → "flagged" (audit-only — caller MAY proceed)
//
// Violations are surfaced as []GuardrailDecision, one per MATCH_FOUND
// filter hit, so the caller's audit trail records which filter fired.
func (a *Adapter) Screen(ctx context.Context, req ScreenRequest) (ScreenVerdict, error) {
	templateName, err := a.resolver.Resolve(req.AgentID)
	if err != nil {
		return ScreenVerdict{}, fmt.Errorf("modelarmor adapter: resolve template for agent_id=%q: %w", req.AgentID, err)
	}
	result, err := a.screener.SanitizeUserPrompt(ctx, cgcmodelarmor.ScreenRequest{
		TenantID:     req.TenantID,
		AgentID:      req.AgentID,
		GCID:         req.AuthorGCID,
		TemplateName: templateName,
		Text:         req.Content,
	})
	if err != nil {
		return ScreenVerdict{}, fmt.Errorf("modelarmor adapter: SanitizeUserPrompt: %w", err)
	}
	return toScreenVerdict(result), nil
}

// toScreenVerdict maps a canonical Screener ScreenResult into the
// sharing-shaped ScreenVerdict.
func toScreenVerdict(r cgcmodelarmor.ScreenResult) ScreenVerdict {
	v := ScreenVerdict{
		Reason:     r.Reason,
		Violations: buildViolations(r),
	}
	switch r.Verdict {
	case cgcmodelarmor.VerdictBlock:
		v.Verdict = VerdictBlocked
	case cgcmodelarmor.VerdictInspectOnly:
		v.Verdict = VerdictFlagged
	default:
		v.Verdict = VerdictApproved
	}
	return v
}

// buildViolations converts canonical FilterHit rows into the sharing
// adapter's GuardrailDecision shape. Only MATCH_FOUND hits become
// decisions — allow/no-match rows are dropped to keep the audit payload
// tight.
func buildViolations(r cgcmodelarmor.ScreenResult) []GuardrailDecision {
	if len(r.Filters) == 0 {
		return nil
	}
	decisions := make([]GuardrailDecision, 0, len(r.Filters))
	for _, hit := range r.Filters {
		if hit.MatchState != cgcmodelarmor.MatchStateMatchFound {
			continue
		}
		violationType := hit.FilterName
		if hit.Subcategory != "" {
			violationType = hit.FilterName + ":" + hit.Subcategory
		}
		decisions = append(decisions, GuardrailDecision{
			Tier:          "model_armor",
			Result:        resultFor(r.Verdict),
			ViolationType: violationType,
		})
	}
	if len(decisions) == 0 {
		return nil
	}
	return decisions
}

// resultFor returns the canonical per-decision Result string for a
// given canonical Verdict.
func resultFor(v cgcmodelarmor.Verdict) string {
	switch v {
	case cgcmodelarmor.VerdictBlock:
		return "block"
	case cgcmodelarmor.VerdictInspectOnly:
		return "flag"
	default:
		return "pass"
	}
}
