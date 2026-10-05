// Package clients — Moderation engine client wrapper.
//
// ModerationEngineClient invokes the deployed Vertex AI Agent Engine
// (us-central1 per ADR-148) that hosts the Content Moderation crew
// (P6 Reflection — Moderator + Critic, single-pass). Used by
// POST /api/posts to pre-gate ChoraCircle posts (Phyllis Step 9).
//
// Aligned with M14.iter5.B fail-loud gate
// (moderation_engine_not_configured 503 when env empty). 5.E wires
// the actual engine invocation.
package clients

import (
	"context"
	"fmt"
	"strings"

	"github.com/apollo-chora/chora-common/agentengine"
)

// ModerationEnginePort is the abstraction over the Moderation engine.
type ModerationEnginePort interface {
	Moderate(ctx context.Context, req ModerationRequest) (ModerationResponse, error)
}

// Verdict is the canonical outcome of the moderation pipeline.
type Verdict string

const (
	VerdictPass   Verdict = "pass"
	VerdictRefine Verdict = "refine"
	VerdictReject Verdict = "reject"
	// VerdictUnknown is returned when the Critic emitted neither a valid
	// JSON envelope nor a recognised final_verdict value. Callers MUST
	// treat unknown as a soft-reject (fail-safe) and not as a pass.
	VerdictUnknown Verdict = "unknown"
)

// ModerationRequest carries the per-post payload. The engine's
// manaplugin reads tenant_id + author_gcid + mana_tier; the Moderator's
// prompt consumes post_text. Author + Critic both read author_gcid for
// per-poster context.
type ModerationRequest struct {
	TenantID    string
	AuthorGCID  string
	ManaTier    string
	PostText    string
	PostID      string // optional; included in state if non-empty (for audit log linkage)
}

// ModerationResponse carries the parsed Critic verdict + per-sub-agent
// trace for IMDA D2 evidence.
type ModerationResponse struct {
	FinalVerdict Verdict           // pass | refine | reject | unknown
	Reason       string            // human-readable explanation from Critic
	Agreement    string            // confirm | revise | escalate (Critic's stance on Moderator)
	ModeratorVerdict Verdict       // Moderator's pre-Critic verdict
	ModeratorReason  string
	PipelineTrace    []SubAgentTrace
	TotalOutputTokens int
	TotalPromptTokens int
}

// SubAgentTrace summarises one sub-agent's contribution.
type SubAgentTrace struct {
	Author       string `json:"author"`        // moderator | critic
	Model        string `json:"model"`         // gemini-2.5-flash | gemini-2.5-pro
	OutputTokens int    `json:"output_tokens"`
	FinalText    string `json:"final_text"`
	FinishReason string `json:"finish_reason"`
}

// criticVerdictEnvelope mirrors the Critic's terminal JSON output.
type criticVerdictEnvelope struct {
	FinalVerdict string `json:"final_verdict"`
	Agreement    string `json:"agreement"`
	Reason       string `json:"reason"`
}

// moderatorVerdictEnvelope mirrors the Moderator's terminal JSON output.
type moderatorVerdictEnvelope struct {
	Verdict string `json:"verdict"`
	Reason  string `json:"reason"`
}

// ModerationEngineClient is the production implementation.
type ModerationEngineClient struct {
	engine         agentengine.Client
	engineResource string
}

// NewModerationEngineClient constructs a client bound to a specific
// engine resource_name.
func NewModerationEngineClient(engine agentengine.Client, engineResource string) *ModerationEngineClient {
	return &ModerationEngineClient{engine: engine, engineResource: engineResource}
}

// Moderate runs the Moderator + Critic Reflection cycle and returns the
// Critic's verdict.
func (c *ModerationEngineClient) Moderate(ctx context.Context, req ModerationRequest) (ModerationResponse, error) {
	if c.engineResource == "" {
		return ModerationResponse{}, agentengine.ErrEngineNotConfigured
	}
	if req.TenantID == "" || req.AuthorGCID == "" || req.ManaTier == "" {
		return ModerationResponse{}, fmt.Errorf("%w: tenant_id + author_gcid + mana_tier required",
			agentengine.ErrInvalidRequest)
	}
	if strings.TrimSpace(req.PostText) == "" {
		return ModerationResponse{}, fmt.Errorf("%w: post_text required", agentengine.ErrInvalidRequest)
	}

	state := map[string]any{
		"tenant_id":   req.TenantID,
		"author_gcid": req.AuthorGCID,
		"user_gcid":   req.AuthorGCID, // manaplugin reads user_gcid
		"mana_tier":   req.ManaTier,
		"post_text":   req.PostText,
	}
	if req.PostID != "" {
		state["post_id"] = req.PostID
	}

	sessionID, err := c.engine.CreateSession(ctx, agentengine.CreateSessionRequest{
		EngineResource: c.engineResource,
		UserID:         req.AuthorGCID,
		State:          state,
	})
	if err != nil {
		return ModerationResponse{}, fmt.Errorf("moderation create session: %w", err)
	}
	defer func() {
		_ = c.engine.DeleteSession(ctx, agentengine.DeleteSessionRequest{
			EngineResource: c.engineResource,
			UserID:         req.AuthorGCID,
			SessionID:      sessionID,
		})
	}()

	stream, err := c.engine.StreamQuery(ctx, agentengine.StreamQueryRequest{
		EngineResource: c.engineResource,
		UserID:         req.AuthorGCID,
		SessionID:      sessionID,
		Message:        "Moderate this post per policy. Emit verdict JSON only.",
	})
	if err != nil {
		return ModerationResponse{}, fmt.Errorf("moderation stream query: %w", err)
	}

	type subAccum struct {
		finalText    string
		model        string
		finishReason string
		outputTokens int
		promptTokens int
	}
	accum := map[string]*subAccum{}
	order := []string{}

	for ev := range stream {
		if ev.Err != nil {
			return ModerationResponse{}, fmt.Errorf("moderation stream: %w", ev.Err)
		}
		if ev.Author == "" {
			continue
		}
		a, ok := accum[ev.Author]
		if !ok {
			a = &subAccum{}
			accum[ev.Author] = a
			order = append(order, ev.Author)
		}
		if ev.Text != "" && !ev.Partial {
			a.finalText = ev.Text
		}
		if ev.Model != "" {
			a.model = ev.Model
		}
		if ev.FinishReason != "" {
			a.finishReason = ev.FinishReason
		}
		if ev.UsageMetadata != nil {
			a.outputTokens += ev.UsageMetadata.CandidatesTokenCount
			a.promptTokens += ev.UsageMetadata.PromptTokenCount
		}
	}

	resp := ModerationResponse{FinalVerdict: VerdictUnknown}
	for _, author := range order {
		a := accum[author]
		resp.PipelineTrace = append(resp.PipelineTrace, SubAgentTrace{
			Author:       author,
			Model:        a.model,
			OutputTokens: a.outputTokens,
			FinalText:    a.finalText,
			FinishReason: a.finishReason,
		})
		resp.TotalOutputTokens += a.outputTokens
		resp.TotalPromptTokens += a.promptTokens
	}

	// Parse Moderator verdict (advisory; Critic is authoritative).
	if mod, ok := accum["moderator"]; ok && mod.finalText != "" {
		var v moderatorVerdictEnvelope
		if err := parseJSONEnvelope(mod.finalText, &v); err == nil {
			resp.ModeratorVerdict = normaliseVerdict(v.Verdict)
			resp.ModeratorReason = v.Reason
		}
	}

	// Parse Critic verdict (authoritative).
	critic, ok := accum["critic"]
	if !ok || critic.finalText == "" {
		return resp, fmt.Errorf("%w: critic sub-agent emitted no terminal text", agentengine.ErrStreamAborted)
	}
	var verdict criticVerdictEnvelope
	if err := parseJSONEnvelope(critic.finalText, &verdict); err != nil {
		// Critic output unparseable — fail-safe: keep VerdictUnknown so
		// callers treat as soft-reject.
		resp.Reason = critic.finalText
		return resp, fmt.Errorf("%w: parse critic verdict: %v", agentengine.ErrStreamAborted, err)
	}
	resp.FinalVerdict = normaliseVerdict(verdict.FinalVerdict)
	resp.Reason = verdict.Reason
	resp.Agreement = verdict.Agreement
	return resp, nil
}

// normaliseVerdict canonicalises whatever the model emitted into one of
// the 4 known verdicts. Unknown values map to VerdictUnknown (fail-safe).
func normaliseVerdict(raw string) Verdict {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "pass", "approve", "approved", "allow":
		return VerdictPass
	case "refine", "revise", "needs_refinement", "rewrite":
		return VerdictRefine
	case "reject", "deny", "block", "refused":
		return VerdictReject
	default:
		return VerdictUnknown
	}
}
