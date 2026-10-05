package clients

import (
	"context"
	"errors"
	"testing"

	"github.com/apollo-chora/chora-common/agentengine"
)

const modTestEngine = "projects/381315455325/locations/us-central1/reasoningEngines/6834103033127763968"

// fakeModerationEngine implements agentengine.Client for tests.
type fakeModerationEngine struct {
	sessionID    string
	createErr    error
	streamErr    error
	streamEvents []agentengine.StreamEvent
	deleteCalled bool

	gotCreateReq agentengine.CreateSessionRequest
	gotStreamReq agentengine.StreamQueryRequest
}

func (f *fakeModerationEngine) CreateSession(_ context.Context, req agentengine.CreateSessionRequest) (string, error) {
	f.gotCreateReq = req
	if f.createErr != nil {
		return "", f.createErr
	}
	return f.sessionID, nil
}

func (f *fakeModerationEngine) StreamQuery(_ context.Context, req agentengine.StreamQueryRequest) (<-chan agentengine.StreamEvent, error) {
	f.gotStreamReq = req
	if f.streamErr != nil {
		return nil, f.streamErr
	}
	ch := make(chan agentengine.StreamEvent, len(f.streamEvents)+1)
	go func() {
		defer close(ch)
		for _, ev := range f.streamEvents {
			ch <- ev
		}
	}()
	return ch, nil
}

func (f *fakeModerationEngine) DeleteSession(_ context.Context, _ agentengine.DeleteSessionRequest) error {
	f.deleteCalled = true
	return nil
}

func TestModerationEngineClient_Moderate_PassVerdict(t *testing.T) {
	engine := &fakeModerationEngine{
		sessionID: "sess-mod",
		streamEvents: []agentengine.StreamEvent{
			{Author: "moderator",
				Text:         `{"verdict": "pass", "reason": "benign learning-achievement post"}`,
				Partial:      false, FinishReason: "STOP", Model: "gemini-2.5-flash",
				UsageMetadata: &agentengine.UsageMetadata{CandidatesTokenCount: 14}},
			{Author: "critic",
				Text:         `{"final_verdict": "pass", "agreement": "confirm", "reason": "Moderator's pass is correct."}`,
				Partial:      false, FinishReason: "STOP", Model: "gemini-2.5-pro",
				UsageMetadata: &agentengine.UsageMetadata{CandidatesTokenCount: 51}},
		},
	}
	client := NewModerationEngineClient(engine, modTestEngine)
	resp, err := client.Moderate(context.Background(), ModerationRequest{
		TenantID: "tenant-x", AuthorGCID: "gcid-phyllis", ManaTier: "standard",
		PostText: "Just earned my CSPO from MTM!",
	})
	if err != nil {
		t.Fatalf("Moderate: %v", err)
	}
	if resp.FinalVerdict != VerdictPass {
		t.Errorf("FinalVerdict = %q; want pass", resp.FinalVerdict)
	}
	if resp.ModeratorVerdict != VerdictPass {
		t.Errorf("ModeratorVerdict = %q; want pass", resp.ModeratorVerdict)
	}
	if resp.Agreement != "confirm" {
		t.Errorf("Agreement = %q; want confirm", resp.Agreement)
	}
	if len(resp.PipelineTrace) != 2 {
		t.Errorf("PipelineTrace len = %d; want 2", len(resp.PipelineTrace))
	}
	if resp.TotalOutputTokens != 65 {
		t.Errorf("TotalOutputTokens = %d; want 65", resp.TotalOutputTokens)
	}
}

func TestModerationEngineClient_Moderate_RejectVerdict(t *testing.T) {
	engine := &fakeModerationEngine{
		sessionID: "sess",
		streamEvents: []agentengine.StreamEvent{
			{Author: "moderator", Text: `{"verdict": "reject", "reason": "PII + solicitation"}`,
				Partial: false, FinishReason: "STOP"},
			{Author: "critic",
				Text:    `{"final_verdict": "reject", "agreement": "confirm", "reason": "violates policy + PII"}`,
				Partial: false, FinishReason: "STOP"},
		},
	}
	client := NewModerationEngineClient(engine, modTestEngine)
	resp, err := client.Moderate(context.Background(), ModerationRequest{
		TenantID: "t", AuthorGCID: "g", ManaTier: "basic",
		PostText: "Selling CSPO answer key, dm me +6512345678",
	})
	if err != nil {
		t.Fatalf("Moderate: %v", err)
	}
	if resp.FinalVerdict != VerdictReject {
		t.Errorf("FinalVerdict = %q; want reject", resp.FinalVerdict)
	}
	if resp.Reason == "" {
		t.Error("Reason empty; want explanation")
	}
}

func TestModerationEngineClient_Moderate_RefineVerdict(t *testing.T) {
	engine := &fakeModerationEngine{
		sessionID: "sess",
		streamEvents: []agentengine.StreamEvent{
			{Author: "moderator", Text: `{"verdict": "refine", "reason": "tone too absolute"}`,
				Partial: false, FinishReason: "STOP"},
			{Author: "critic",
				Text:    `{"final_verdict": "refine", "agreement": "confirm", "reason": "soften the claim"}`,
				Partial: false, FinishReason: "STOP"},
		},
	}
	client := NewModerationEngineClient(engine, modTestEngine)
	resp, err := client.Moderate(context.Background(), ModerationRequest{
		TenantID: "t", AuthorGCID: "g", ManaTier: "basic",
		PostText: "Scrum always wins.",
	})
	if err != nil {
		t.Fatalf("Moderate: %v", err)
	}
	if resp.FinalVerdict != VerdictRefine {
		t.Errorf("FinalVerdict = %q; want refine", resp.FinalVerdict)
	}
}

func TestModerationEngineClient_Moderate_CriticDisagreesWithModerator(t *testing.T) {
	// Moderator says pass; Critic overrules to reject. Critic is authoritative.
	engine := &fakeModerationEngine{
		sessionID: "sess",
		streamEvents: []agentengine.StreamEvent{
			{Author: "moderator", Text: `{"verdict": "pass", "reason": "looks fine"}`,
				Partial: false, FinishReason: "STOP"},
			{Author: "critic",
				Text:    `{"final_verdict": "reject", "agreement": "revise", "reason": "Moderator missed the PII"}`,
				Partial: false, FinishReason: "STOP"},
		},
	}
	client := NewModerationEngineClient(engine, modTestEngine)
	resp, err := client.Moderate(context.Background(), ModerationRequest{
		TenantID: "t", AuthorGCID: "g", ManaTier: "basic", PostText: "x",
	})
	if err != nil {
		t.Fatalf("Moderate: %v", err)
	}
	if resp.FinalVerdict != VerdictReject {
		t.Errorf("FinalVerdict = %q; want reject (Critic authoritative)", resp.FinalVerdict)
	}
	if resp.ModeratorVerdict != VerdictPass {
		t.Errorf("ModeratorVerdict = %q; want pass (advisory)", resp.ModeratorVerdict)
	}
	if resp.Agreement != "revise" {
		t.Errorf("Agreement = %q; want revise", resp.Agreement)
	}
}

func TestModerationEngineClient_Moderate_MalformedCriticIsUnknown(t *testing.T) {
	engine := &fakeModerationEngine{
		sessionID: "sess",
		streamEvents: []agentengine.StreamEvent{
			{Author: "moderator", Text: `{"verdict": "pass", "reason": "ok"}`,
				Partial: false, FinishReason: "STOP"},
			{Author: "critic", Text: "not-json", Partial: false, FinishReason: "STOP"},
		},
	}
	client := NewModerationEngineClient(engine, modTestEngine)
	resp, err := client.Moderate(context.Background(), ModerationRequest{
		TenantID: "t", AuthorGCID: "g", ManaTier: "basic", PostText: "x",
	})
	// Malformed critic = ErrStreamAborted (fail-loud); but the response
	// MUST carry VerdictUnknown so the handler can fail-safe to reject.
	if !errors.Is(err, agentengine.ErrStreamAborted) {
		t.Errorf("err = %v; want ErrStreamAborted", err)
	}
	if resp.FinalVerdict != VerdictUnknown {
		t.Errorf("FinalVerdict = %q; want unknown (fail-safe)", resp.FinalVerdict)
	}
}

func TestModerationEngineClient_Moderate_MissingCriticIsStreamAborted(t *testing.T) {
	engine := &fakeModerationEngine{
		sessionID: "sess",
		streamEvents: []agentengine.StreamEvent{
			{Author: "moderator", Text: `{"verdict": "pass", "reason": "ok"}`,
				Partial: false, FinishReason: "STOP"},
			// No critic event.
		},
	}
	client := NewModerationEngineClient(engine, modTestEngine)
	_, err := client.Moderate(context.Background(), ModerationRequest{
		TenantID: "t", AuthorGCID: "g", ManaTier: "basic", PostText: "x",
	})
	if !errors.Is(err, agentengine.ErrStreamAborted) {
		t.Errorf("err = %v; want ErrStreamAborted on missing critic", err)
	}
}

func TestModerationEngineClient_Moderate_PropagatesStateToEngine(t *testing.T) {
	engine := &fakeModerationEngine{
		sessionID: "s",
		streamEvents: []agentengine.StreamEvent{
			{Author: "moderator", Text: `{"verdict": "pass", "reason": ""}`, Partial: false, FinishReason: "STOP"},
			{Author: "critic", Text: `{"final_verdict": "pass", "agreement": "confirm", "reason": ""}`, Partial: false, FinishReason: "STOP"},
		},
	}
	client := NewModerationEngineClient(engine, modTestEngine)
	_, err := client.Moderate(context.Background(), ModerationRequest{
		TenantID: "tenant-y", AuthorGCID: "gcid-y", ManaTier: "premium",
		PostText: "Hello world", PostID: "post-001",
	})
	if err != nil {
		t.Fatalf("Moderate: %v", err)
	}
	state := engine.gotCreateReq.State
	if state["tenant_id"] != "tenant-y" {
		t.Errorf("state.tenant_id = %v; want tenant-y", state["tenant_id"])
	}
	if state["author_gcid"] != "gcid-y" {
		t.Errorf("state.author_gcid = %v; want gcid-y", state["author_gcid"])
	}
	if state["user_gcid"] != "gcid-y" {
		t.Errorf("state.user_gcid = %v; want gcid-y (manaplugin contract)", state["user_gcid"])
	}
	if state["mana_tier"] != "premium" {
		t.Errorf("state.mana_tier = %v; want premium", state["mana_tier"])
	}
	if state["post_text"] != "Hello world" {
		t.Errorf("state.post_text = %v; want Hello world", state["post_text"])
	}
	if state["post_id"] != "post-001" {
		t.Errorf("state.post_id = %v; want post-001", state["post_id"])
	}
}

func TestModerationEngineClient_Moderate_DeletesSessionAfter(t *testing.T) {
	engine := &fakeModerationEngine{
		sessionID: "cleanup-s",
		streamEvents: []agentengine.StreamEvent{
			{Author: "moderator", Text: `{"verdict": "pass"}`, Partial: false, FinishReason: "STOP"},
			{Author: "critic", Text: `{"final_verdict": "pass", "agreement": "confirm", "reason": ""}`, Partial: false, FinishReason: "STOP"},
		},
	}
	client := NewModerationEngineClient(engine, modTestEngine)
	_, _ = client.Moderate(context.Background(), ModerationRequest{
		TenantID: "t", AuthorGCID: "g", ManaTier: "basic", PostText: "x",
	})
	if !engine.deleteCalled {
		t.Error("DeleteSession not called; sessions leak")
	}
}

func TestModerationEngineClient_Moderate_FailsLoudOnMissingFields(t *testing.T) {
	client := NewModerationEngineClient(&fakeModerationEngine{}, modTestEngine)
	cases := []ModerationRequest{
		{AuthorGCID: "g", ManaTier: "basic", PostText: "x"},  // missing TenantID
		{TenantID: "t", ManaTier: "basic", PostText: "x"},    // missing AuthorGCID
		{TenantID: "t", AuthorGCID: "g", PostText: "x"},      // missing ManaTier
		{TenantID: "t", AuthorGCID: "g", ManaTier: "basic"},  // missing PostText
	}
	for i, req := range cases {
		_, err := client.Moderate(context.Background(), req)
		if !errors.Is(err, agentengine.ErrInvalidRequest) {
			t.Errorf("case %d: err = %v; want ErrInvalidRequest", i, err)
		}
	}
}

func TestModerationEngineClient_Moderate_FailsLoudOnEmptyEngineResource(t *testing.T) {
	client := NewModerationEngineClient(&fakeModerationEngine{}, "")
	_, err := client.Moderate(context.Background(), ModerationRequest{
		TenantID: "t", AuthorGCID: "g", ManaTier: "basic", PostText: "x",
	})
	if !errors.Is(err, agentengine.ErrEngineNotConfigured) {
		t.Errorf("err = %v; want ErrEngineNotConfigured", err)
	}
}

func TestNormaliseVerdict(t *testing.T) {
	cases := map[string]Verdict{
		"pass":            VerdictPass,
		"Pass":            VerdictPass,
		"approve":         VerdictPass,
		"refine":          VerdictRefine,
		"NEEDS_REFINEMENT": VerdictRefine,
		"reject":          VerdictReject,
		"block":           VerdictReject,
		"":                VerdictUnknown,
		"made-up":         VerdictUnknown,
	}
	for in, want := range cases {
		if got := normaliseVerdict(in); got != want {
			t.Errorf("normaliseVerdict(%q) = %q; want %q", in, got, want)
		}
	}
}
