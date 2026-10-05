package clients

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-common/agentengine"
)

// Error/edge paths for the three agentengine-backed clients. The happy
// paths live in the per-client test files; these cover the remaining
// fail-loud branches so the guards can't regress silently.

func TestDuelAtomSmithEngineClient_NilEngineAndReceiver(t *testing.T) {
	req := DuelAtomSmithRequest{TenantID: "t", UserGCID: "g"}
	var nilC *DuelAtomSmithEngineClient
	if _, err := nilC.ConjureDuelAtoms(context.Background(), req); err == nil || !strings.Contains(err.Error(), "engine not configured") {
		t.Errorf("nil receiver: err = %v, want engine-not-configured", err)
	}
	c := NewDuelAtomSmithEngineClient(nil, smithTestEngine)
	if _, err := c.ConjureDuelAtoms(context.Background(), req); err == nil || !strings.Contains(err.Error(), "engine not configured") {
		t.Errorf("nil engine: err = %v, want engine-not-configured", err)
	}
}

func TestDuelAtomSmithEngineClient_CreateSessionError(t *testing.T) {
	engine := &fakeSmithAgentEngine{sessionID: "s", createErr: errors.New("create boom")}
	c := NewDuelAtomSmithEngineClient(engine, smithTestEngine)
	_, err := c.ConjureDuelAtoms(context.Background(), DuelAtomSmithRequest{TenantID: "t", UserGCID: "g"})
	if err == nil || !strings.Contains(err.Error(), "create session") {
		t.Errorf("err = %v, want create session wrap", err)
	}
}

func TestDuelAtomSmithEngineClient_StreamEventError(t *testing.T) {
	engine := &fakeSmithAgentEngine{
		sessionID: "s",
		streamEvents: []agentengine.StreamEvent{
			{Author: "smith", Err: errors.New("mid-stream failure")},
		},
	}
	c := NewDuelAtomSmithEngineClient(engine, smithTestEngine)
	_, err := c.ConjureDuelAtoms(context.Background(), DuelAtomSmithRequest{TenantID: "t", UserGCID: "g"})
	if err == nil || !strings.Contains(err.Error(), "duel atom smith: stream") {
		t.Errorf("err = %v, want stream wrap", err)
	}
}

func TestDuelAtomSmithEngineClient_AnonymousEventsSkipped(t *testing.T) {
	// Events without an author are transport noise — skipped; only the
	// smith's terminal text should be parsed.
	engine := &fakeSmithAgentEngine{
		sessionID: "s",
		streamEvents: []agentengine.StreamEvent{
			{Text: `{"picks": [], "generated": []}`, Partial: false}, // no author
			{Author: "smith", Text: `{"picks": [0], "generated": []}`, Partial: false},
		},
	}
	c := NewDuelAtomSmithEngineClient(engine, smithTestEngine)
	resp, err := c.ConjureDuelAtoms(context.Background(), DuelAtomSmithRequest{TenantID: "t", UserGCID: "g"})
	if err != nil {
		t.Fatalf("ConjureDuelAtoms: %v", err)
	}
	if len(resp.Picks) != 1 {
		t.Errorf("Picks = %v, want [0] (anonymous event must not clobber final text)", resp.Picks)
	}
}

func TestModerationEngineClient_CreateSessionError(t *testing.T) {
	engine := &fakeModerationEngine{sessionID: "s", createErr: errors.New("create boom")}
	c := NewModerationEngineClient(engine, modTestEngine)
	_, err := c.Moderate(context.Background(), ModerationRequest{TenantID: "t", AuthorGCID: "g", ManaTier: "basic", PostText: "x"})
	if err == nil || !strings.Contains(err.Error(), "moderation create session") {
		t.Errorf("err = %v, want create session wrap", err)
	}
}

func TestModerationEngineClient_StreamQueryError(t *testing.T) {
	engine := &fakeModerationEngine{sessionID: "s", streamErr: errors.New("stream down")}
	c := NewModerationEngineClient(engine, modTestEngine)
	_, err := c.Moderate(context.Background(), ModerationRequest{TenantID: "t", AuthorGCID: "g", ManaTier: "basic", PostText: "x"})
	if err == nil || !strings.Contains(err.Error(), "moderation stream query") {
		t.Errorf("err = %v, want stream query wrap", err)
	}
}

func TestModerationEngineClient_StreamEventError(t *testing.T) {
	engine := &fakeModerationEngine{
		sessionID: "s",
		streamEvents: []agentengine.StreamEvent{
			{Author: "moderator", Err: errors.New("mid-stream failure")},
		},
	}
	c := NewModerationEngineClient(engine, modTestEngine)
	_, err := c.Moderate(context.Background(), ModerationRequest{TenantID: "t", AuthorGCID: "g", ManaTier: "basic", PostText: "x"})
	if err == nil || !strings.Contains(err.Error(), "moderation stream") {
		t.Errorf("err = %v, want stream wrap", err)
	}
}

func TestModerationEngineClient_AnonymousEventsSkipped(t *testing.T) {
	engine := &fakeModerationEngine{
		sessionID: "s",
		streamEvents: []agentengine.StreamEvent{
			{Text: `{"verdict": "reject"}`, Partial: false}, // no author — skipped
			{Author: "moderator", Text: `{"verdict": "pass", "reason": "ok"}`, Partial: false},
			{Author: "critic", Text: `{"final_verdict": "pass", "agreement": "confirm", "reason": ""}`, Partial: false},
		},
	}
	c := NewModerationEngineClient(engine, modTestEngine)
	resp, err := c.Moderate(context.Background(), ModerationRequest{TenantID: "t", AuthorGCID: "g", ManaTier: "basic", PostText: "x"})
	if err != nil {
		t.Fatalf("Moderate: %v", err)
	}
	if resp.FinalVerdict != VerdictPass || resp.ModeratorVerdict != VerdictPass {
		t.Errorf("verdicts = critic:%q moderator:%q, want pass/pass (anonymous event skipped)", resp.FinalVerdict, resp.ModeratorVerdict)
	}
}

func TestProfileConjurerClient_NilEngineAndReceiver(t *testing.T) {
	var nilC *ProfileConjurerClient
	if _, err := nilC.Conjure(context.Background(), "t", "g", "bio", nil); err == nil || !strings.Contains(err.Error(), "engine not configured") {
		t.Errorf("nil receiver: err = %v, want engine-not-configured", err)
	}
	c := NewProfileConjurerClient(nil, conjurerTestEngine)
	if _, err := c.Conjure(context.Background(), "t", "g", "bio", nil); err == nil || !strings.Contains(err.Error(), "engine not configured") {
		t.Errorf("nil engine: err = %v, want engine-not-configured", err)
	}
}

func TestProfileConjurerClient_CreateSessionError(t *testing.T) {
	engine := &fakeConjurerEngine{sessionID: "s", createErr: errors.New("create boom")}
	c := NewProfileConjurerClient(engine, conjurerTestEngine)
	_, err := c.Conjure(context.Background(), "t", "g", "bio", nil)
	if err == nil || !strings.Contains(err.Error(), "profile conjurer: create session") {
		t.Errorf("err = %v, want create session wrap", err)
	}
}

func TestProfileConjurerClient_StreamEventError(t *testing.T) {
	engine := &fakeConjurerEngine{
		sessionID: "s",
		streamEvents: []agentengine.StreamEvent{
			{Author: "conjurer", Err: errors.New("mid-stream failure")},
		},
	}
	c := NewProfileConjurerClient(engine, conjurerTestEngine)
	_, err := c.Conjure(context.Background(), "t", "g", "bio", nil)
	if err == nil || !strings.Contains(err.Error(), "profile conjurer: stream") {
		t.Errorf("err = %v, want stream wrap", err)
	}
}

func TestProfileConjurerClient_AnonymousEventsSkipped(t *testing.T) {
	envelope := `{"tags": {"science": ["astronomy"]}, "proficiency": {"per_category": {"science": "intermediate"}}}`
	engine := &fakeConjurerEngine{
		sessionID: "s",
		streamEvents: []agentengine.StreamEvent{
			{Text: envelope, Partial: false}, // no author — skipped
			{Author: "conjurer", Text: envelope, Partial: false},
		},
	}
	c := NewProfileConjurerClient(engine, conjurerTestEngine)
	res, err := c.Conjure(context.Background(), "t", "g", "bio", nil)
	if err != nil {
		t.Fatalf("Conjure: %v", err)
	}
	if len(res.Tags) != 1 {
		t.Errorf("Tags len = %d, want 1 (anonymous event must not clobber final text)", len(res.Tags))
	}
}
