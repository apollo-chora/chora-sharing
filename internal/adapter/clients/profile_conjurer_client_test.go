package clients

import (
	"context"
	"errors"
	"testing"

	"github.com/apollo-chora/chora-common/agentengine"
	"github.com/apollo-chora/chora-sharing/internal/domain/profiler"
)

const conjurerTestEngine = "http://localhost:8101"

// fakeConjurerEngine implements agentengine.Client for tests.
type fakeConjurerEngine struct {
	sessionID    string
	createErr    error
	streamErr    error
	streamEvents []agentengine.StreamEvent
	deleteCalled bool

	gotCreateReq agentengine.CreateSessionRequest
	gotStreamReq agentengine.StreamQueryRequest
}

func (f *fakeConjurerEngine) CreateSession(_ context.Context, req agentengine.CreateSessionRequest) (string, error) {
	f.gotCreateReq = req
	if f.createErr != nil {
		return "", f.createErr
	}
	return f.sessionID, nil
}

func (f *fakeConjurerEngine) StreamQuery(_ context.Context, req agentengine.StreamQueryRequest) (<-chan agentengine.StreamEvent, error) {
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

func (f *fakeConjurerEngine) DeleteSession(_ context.Context, _ agentengine.DeleteSessionRequest) error {
	f.deleteCalled = true
	return nil
}

func TestProfileConjurerClient_HappyPath(t *testing.T) {
	engine := &fakeConjurerEngine{
		sessionID: "sess-conjurer",
		streamEvents: []agentengine.StreamEvent{
			{Author: "conjurer",
				Text: `{"tags": {"science": ["astronomy", "physics"], "mathematics": ["calculus"]},
"proficiency": {"per_category": {"science": "advanced", "mathematics": "intermediate"}}}`,
				Partial: false, FinishReason: "STOP", Model: "gemini-3.5-flash"},
		},
	}
	client := NewProfileConjurerClient(engine, conjurerTestEngine)
	result, err := client.Conjure(context.Background(),
		"tenant-x", "gcid-phyllis",
		"I love astronomy, planets and space exploration",
		[]string{"Intro to Physics", "Calculus I", "Calculus II", "Linear Algebra"})
	if err != nil {
		t.Fatalf("Conjure: %v", err)
	}
	if len(result.Tags) != 3 {
		t.Fatalf("Tags len = %d, want 3 (astronomy+physics+calculus)", len(result.Tags))
	}
	wantTags := map[string]bool{
		"science/astronomy":   true,
		"science/physics":     true,
		"mathematics/calculus": true,
	}
	for _, it := range result.Tags {
		key := string(it.Category) + "/" + it.Tag
		if !wantTags[key] {
			t.Errorf("unexpected tag %s", key)
		}
	}
	if got := result.Proficiency.PerCategory["science"]; got != profiler.ProficiencyAdvanced {
		t.Errorf("PerCategory[science] = %q, want advanced", got)
	}
	if got := result.Proficiency.PerCategory["mathematics"]; got != profiler.ProficiencyIntermediate {
		t.Errorf("PerCategory[mathematics] = %q, want intermediate", got)
	}
	if len(result.Proficiency.PerCategory) != 2 {
		t.Errorf("PerCategory len = %d, want 2 (science + mathematics)", len(result.Proficiency.PerCategory))
	}
	if !engine.deleteCalled {
		t.Error("DeleteSession not called after Conjure")
	}
}

func TestProfileConjurerClient_StateKeysPresent(t *testing.T) {
	engine := &fakeConjurerEngine{
		sessionID: "sess",
		streamEvents: []agentengine.StreamEvent{
			{Author: "conjurer", Text: `{"tags": {}, "proficiency": {"per_category": {}}}`, Partial: false},
		},
	}
	client := NewProfileConjurerClient(engine, conjurerTestEngine)
	_, _ = client.Conjure(context.Background(), "tenant-x", "gcid-phyllis", "bio", []string{"Calculus I"})

	state := engine.gotCreateReq.State
	for _, key := range []string{"tenant_id", "user_gcid", "mana_tier", "bio", "course_titles_json"} {
		if _, ok := state[key]; !ok {
			t.Errorf("state missing key %q", key)
		}
	}
	if v, _ := state["mana_tier"].(string); v != "standard" {
		t.Errorf("mana_tier = %q, want standard", v)
	}
	if v, _ := state["tenant_id"].(string); v != "tenant-x" {
		t.Errorf("tenant_id = %q, want tenant-x", v)
	}
	if v, _ := state["user_gcid"].(string); v != "gcid-phyllis" {
		t.Errorf("user_gcid = %q, want gcid-phyllis", v)
	}
}

func TestProfileConjurerClient_MalformedJSON(t *testing.T) {
	engine := &fakeConjurerEngine{
		sessionID: "sess",
		streamEvents: []agentengine.StreamEvent{
			{Author: "conjurer", Text: "not json at all", Partial: false},
		},
	}
	client := NewProfileConjurerClient(engine, conjurerTestEngine)
	_, err := client.Conjure(context.Background(), "tenant-x", "gcid", "bio", nil)
	if err == nil {
		t.Fatal("expected error on malformed JSON")
	}
	if !errors.Is(err, agentengine.ErrStreamAborted) {
		t.Errorf("err = %v, want ErrStreamAborted wrap", err)
	}
}

// TestProfileConjurerClient_EmptyProficiencyIsValid asserts the new
// contract: an empty per_category map (or a missing proficiency object
// entirely) is valid — a new user with no signal gets proficiency: {}.
// The smith agent treats empty as "default to beginner-level atoms".
// This replaces the old TestProfileConjurerClient_MissingOverall +
// TestProfileConjurerClient_InvalidOverall tests (the overall field +
// its fail-loud validation were removed).
func TestProfileConjurerClient_EmptyProficiencyIsValid(t *testing.T) {
	cases := []struct {
		name string
		json string
	}{
		{
			name: "empty per_category map",
			json: `{"tags": {}, "proficiency": {"per_category": {}}}`,
		},
		{
			name: "missing proficiency object entirely",
			json: `{"tags": {}}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			engine := &fakeConjurerEngine{
				sessionID: "sess",
				streamEvents: []agentengine.StreamEvent{
					{Author: "conjurer", Text: tc.json, Partial: false},
				},
			}
			client := NewProfileConjurerClient(engine, conjurerTestEngine)
			result, err := client.Conjure(context.Background(), "tenant-x", "gcid", "bio", nil)
			if err != nil {
				t.Fatalf("Conjure: %v (empty proficiency must NOT error)", err)
			}
			if len(result.Proficiency.PerCategory) != 0 {
				t.Errorf("PerCategory len = %d, want 0 (empty proficiency)", len(result.Proficiency.PerCategory))
			}
		})
	}
}

func TestProfileConjurerClient_UnknownTaxonomyDropped(t *testing.T) {
	engine := &fakeConjurerEngine{
		sessionID: "sess",
		streamEvents: []agentengine.StreamEvent{
			{Author: "conjurer", Text: `{
"tags": {
  "science": ["astronomy", "black_holes", "wormholes"],
  "fictional": ["warp_drive"]
},
"proficiency": {"per_category": {"science": "expert", "fictional": "advanced"}}
}`, Partial: false},
		},
	}
	client := NewProfileConjurerClient(engine, conjurerTestEngine)
	result, err := client.Conjure(context.Background(), "tenant-x", "gcid", "bio", nil)
	if err != nil {
		t.Fatalf("Conjure: %v", err)
	}
	// Tag strings are FREE-FORM within a valid category: science/astronomy,
	// science/black_holes, + science/wormholes ALL survive (the closed
	// vocabulary was loosened so a bio about "golang" no longer yields
	// zero tags). The "fictional" category is unknown → dropped entirely.
	if len(result.Tags) != 3 {
		t.Fatalf("Tags len = %d, want 3 (science: astronomy + black_holes + wormholes — free-form tags accepted)", len(result.Tags))
	}
	// The fictional category is dropped — the 6-category structure is the
	// matchmaking signal + stays enforced even though tag strings are free.
	for _, tag := range result.Tags {
		if tag.Category == "fictional" {
			t.Errorf("fictional category must be dropped; got tag %+v", tag)
		}
	}
	// PerCategory: science/expert dropped (invalid level), fictional dropped
	// (unknown category). PerCategory map is empty.
	if len(result.Proficiency.PerCategory) != 0 {
		t.Errorf("PerCategory len = %d, want 0 (all invalid dropped)", len(result.Proficiency.PerCategory))
	}
}

func TestProfileConjurerClient_EmptyTerminalText(t *testing.T) {
	engine := &fakeConjurerEngine{
		sessionID:    "sess",
		streamEvents: []agentengine.StreamEvent{{Author: "conjurer", Partial: true, Text: "chunk"}},
	}
	client := NewProfileConjurerClient(engine, conjurerTestEngine)
	_, err := client.Conjure(context.Background(), "tenant-x", "gcid", "bio", nil)
	if err == nil {
		t.Fatal("expected error on no terminal text")
	}
	if !errors.Is(err, agentengine.ErrStreamAborted) {
		t.Errorf("err = %v, want ErrStreamAborted wrap", err)
	}
}

func TestProfileConjurerClient_StreamError(t *testing.T) {
	engine := &fakeConjurerEngine{
		sessionID: "sess",
		streamErr: errors.New("transport down"),
	}
	client := NewProfileConjurerClient(engine, conjurerTestEngine)
	_, err := client.Conjure(context.Background(), "tenant-x", "gcid", "bio", nil)
	if err == nil {
		t.Fatal("expected error on stream error")
	}
}

func TestProfileConjurerClient_EmptyEngineResource(t *testing.T) {
	client := NewProfileConjurerClient(&fakeConjurerEngine{}, "")
	_, err := client.Conjure(context.Background(), "tenant-x", "gcid", "bio", nil)
	if !errors.Is(err, agentengine.ErrEngineNotConfigured) {
		t.Errorf("err = %v, want ErrEngineNotConfigured", err)
	}
}

func TestProfileConjurerClient_MissingIdentity(t *testing.T) {
	client := NewProfileConjurerClient(&fakeConjurerEngine{}, conjurerTestEngine)
	_, err := client.Conjure(context.Background(), "", "gcid", "bio", nil)
	if !errors.Is(err, agentengine.ErrInvalidRequest) {
		t.Errorf("err = %v, want ErrInvalidRequest", err)
	}
}

func TestProfileConjurerClient_EmptyBioAndCourses(t *testing.T) {
	client := NewProfileConjurerClient(&fakeConjurerEngine{sessionID: "sess"}, conjurerTestEngine)
	_, err := client.Conjure(context.Background(), "tenant-x", "gcid", "   ", nil)
	if !errors.Is(err, profiler.ErrInvalidArgument) {
		t.Errorf("err = %v, want ErrInvalidArgument", err)
	}
}
