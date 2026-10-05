package clients

import (
	"context"
	"errors"
	"testing"

	"github.com/apollo-chora/chora-common/agentengine"
)

const smithTestEngine = "http://localhost:8102"

// fakeSmithAgentEngine implements agentengine.Client for smith engine tests.
type fakeSmithAgentEngine struct {
	sessionID    string
	createErr    error
	streamErr    error
	streamEvents []agentengine.StreamEvent
	deleteCalled bool

	gotCreateReq agentengine.CreateSessionRequest
	gotStreamReq agentengine.StreamQueryRequest
}

func (f *fakeSmithAgentEngine) CreateSession(_ context.Context, req agentengine.CreateSessionRequest) (string, error) {
	f.gotCreateReq = req
	if f.createErr != nil {
		return "", f.createErr
	}
	return f.sessionID, nil
}

func (f *fakeSmithAgentEngine) StreamQuery(_ context.Context, req agentengine.StreamQueryRequest) (<-chan agentengine.StreamEvent, error) {
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

func (f *fakeSmithAgentEngine) DeleteSession(_ context.Context, _ agentengine.DeleteSessionRequest) error {
	f.deleteCalled = true
	return nil
}

func TestDuelAtomSmithEngineClient_HappyPath(t *testing.T) {
	engine := &fakeSmithAgentEngine{
		sessionID: "sess-smith",
		streamEvents: []agentengine.StreamEvent{
			{Author: "smith",
				Text: `{"picks": [0, 1], "generated": [{"question": "Gen Q", "options": ["A", "B", "C", "D"], "correct_answer": "A"}]}`,
				Partial: false, FinishReason: "STOP", Model: "gemini-3.5-flash"},
		},
	}
	client := NewDuelAtomSmithEngineClient(engine, smithTestEngine)
	resp, err := client.ConjureDuelAtoms(context.Background(), DuelAtomSmithRequest{
		TenantID:      "tenant-x",
		UserGCID:      "gcid-a",
		Candidates:    []DuelAtomCandidate{{Index: 0, Question: "Q1", Options: []string{"A", "B"}}},
		SharedTags:    []string{"physics"},
		Proficiencies: []int{1200, 1100},
		Profiles:      map[string]string{"gcid-a": "intermediate"},
		Count:         3,
	})
	if err != nil {
		t.Fatalf("ConjureDuelAtoms: %v", err)
	}
	if len(resp.Picks) != 2 {
		t.Errorf("Picks len = %d, want 2", len(resp.Picks))
	}
	if resp.Picks[0] != 0 || resp.Picks[1] != 1 {
		t.Errorf("Picks = %v, want [0 1]", resp.Picks)
	}
	if len(resp.Generated) != 1 {
		t.Fatalf("Generated len = %d, want 1", len(resp.Generated))
	}
	if resp.Generated[0].Question != "Gen Q" {
		t.Errorf("Generated[0].Question = %q, want Gen Q", resp.Generated[0].Question)
	}
	if !engine.deleteCalled {
		t.Error("DeleteSession not called after ConjureDuelAtoms")
	}
}

func TestDuelAtomSmithEngineClient_StateKeysPresent(t *testing.T) {
	engine := &fakeSmithAgentEngine{
		sessionID:    "sess",
		streamEvents: []agentengine.StreamEvent{{Author: "smith", Text: `{"picks": [], "generated": []}`, Partial: false}},
	}
	client := NewDuelAtomSmithEngineClient(engine, smithTestEngine)
	_, _ = client.ConjureDuelAtoms(context.Background(), DuelAtomSmithRequest{
		TenantID:      "tenant-x",
		UserGCID:      "gcid-a",
		Candidates:    []DuelAtomCandidate{{Index: 0, Question: "Q", Options: []string{"A"}}},
		SharedTags:    []string{"math"},
		Proficiencies: []int{1200},
		Profiles:      map[string]string{"gcid-a": "intermediate"},
		Count:         1,
	})

	state := engine.gotCreateReq.State
	for _, key := range []string{
		"tenant_id", "user_gcid", "mana_tier",
		"candidates_json", "shared_tags_json", "proficiencies_json",
		"profiles_json", "count",
	} {
		if _, ok := state[key]; !ok {
			t.Errorf("state missing key %q", key)
		}
	}
	if v, _ := state["mana_tier"].(string); v != "standard" {
		t.Errorf("mana_tier = %q, want standard", v)
	}
	if v, _ := state["count"].(int); v != 1 {
		t.Errorf("count = %v, want 1", v)
	}
}

func TestDuelAtomSmithEngineClient_MalformedJSON(t *testing.T) {
	engine := &fakeSmithAgentEngine{
		sessionID:    "sess",
		streamEvents: []agentengine.StreamEvent{{Author: "smith", Text: "not json", Partial: false}},
	}
	client := NewDuelAtomSmithEngineClient(engine, smithTestEngine)
	_, err := client.ConjureDuelAtoms(context.Background(), DuelAtomSmithRequest{
		TenantID: "tenant-x", UserGCID: "gcid-a",
	})
	if err == nil {
		t.Fatal("expected error on malformed JSON")
	}
	if !errors.Is(err, agentengine.ErrStreamAborted) {
		t.Errorf("err = %v, want ErrStreamAborted wrap", err)
	}
}

func TestDuelAtomSmithEngineClient_EmptyTerminalText(t *testing.T) {
	engine := &fakeSmithAgentEngine{
		sessionID:    "sess",
		streamEvents: []agentengine.StreamEvent{{Author: "smith", Partial: true, Text: "chunk"}},
	}
	client := NewDuelAtomSmithEngineClient(engine, smithTestEngine)
	_, err := client.ConjureDuelAtoms(context.Background(), DuelAtomSmithRequest{
		TenantID: "tenant-x", UserGCID: "gcid-a",
	})
	if err == nil {
		t.Fatal("expected error on no terminal text")
	}
	if !errors.Is(err, agentengine.ErrStreamAborted) {
		t.Errorf("err = %v, want ErrStreamAborted wrap", err)
	}
}

func TestDuelAtomSmithEngineClient_StreamError(t *testing.T) {
	engine := &fakeSmithAgentEngine{
		sessionID: "sess",
		streamErr: errors.New("transport down"),
	}
	client := NewDuelAtomSmithEngineClient(engine, smithTestEngine)
	_, err := client.ConjureDuelAtoms(context.Background(), DuelAtomSmithRequest{
		TenantID: "tenant-x", UserGCID: "gcid-a",
	})
	if err == nil {
		t.Fatal("expected error on stream error")
	}
}

func TestDuelAtomSmithEngineClient_EmptyEngineResource(t *testing.T) {
	client := NewDuelAtomSmithEngineClient(&fakeSmithAgentEngine{}, "")
	_, err := client.ConjureDuelAtoms(context.Background(), DuelAtomSmithRequest{
		TenantID: "tenant-x", UserGCID: "gcid-a",
	})
	if !errors.Is(err, agentengine.ErrEngineNotConfigured) {
		t.Errorf("err = %v, want ErrEngineNotConfigured", err)
	}
}

func TestDuelAtomSmithEngineClient_MissingIdentity(t *testing.T) {
	client := NewDuelAtomSmithEngineClient(&fakeSmithAgentEngine{}, smithTestEngine)
	_, err := client.ConjureDuelAtoms(context.Background(), DuelAtomSmithRequest{
		TenantID: "", UserGCID: "gcid-a",
	})
	if !errors.Is(err, agentengine.ErrInvalidRequest) {
		t.Errorf("err = %v, want ErrInvalidRequest", err)
	}
}
