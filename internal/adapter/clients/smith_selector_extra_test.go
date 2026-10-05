package clients

import (
	"context"
	"errors"
	"fmt"
	"testing"

	httpadapter "github.com/apollo-chora/chora-sharing/internal/adapter/http"
	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
	"github.com/apollo-chora/chora-sharing/internal/domain/profiler"
)

func TestDuelAtomSmithSelector_NilEngineAndReceiver(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	seedMCQProjection(t, repo, "atom-1", "Q1", "A", []string{"A", "B"})
	inner := NewProjectionAtomSelector(repo)
	req := httpadapter.AtomSelectionRequest{TenantID: "t", ExcludeGCIDs: []string{"g"}, Count: 1}

	var nilSel *DuelAtomSmithSelector
	if _, err := nilSel.SelectAtoms(context.Background(), req); !errors.Is(err, duel.ErrNoAtomsAvailable) {
		t.Errorf("nil receiver: err = %v, want ErrNoAtomsAvailable", err)
	}
	sel := NewDuelAtomSmithSelector(repo, inner, nil, nil)
	if _, err := sel.SelectAtoms(context.Background(), req); !errors.Is(err, duel.ErrNoAtomsAvailable) {
		t.Errorf("nil engine: err = %v, want ErrNoAtomsAvailable", err)
	}
}

func TestDuelAtomSmithSelector_NilReader(t *testing.T) {
	engine := &fakeSmithEngine{resp: DuelAtomSmithResponse{Picks: []int{0}}}
	sel := NewDuelAtomSmithSelector(nil, nil, nil, engine)
	_, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID: "t", ExcludeGCIDs: []string{"g"}, Count: 1,
	})
	if !errors.Is(err, duel.ErrNoAtomsAvailable) {
		t.Errorf("err = %v, want ErrNoAtomsAvailable", err)
	}
}

func TestDuelAtomSmithSelector_DefaultCountIsFive(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	for i := 1; i <= 5; i++ {
		seedMCQProjection(t, repo, fmt.Sprintf("atom-c%d", i), fmt.Sprintf("MCQ %d", i), "A", []string{"A", "B"})
	}
	// Engine picks every valid candidate — with Count=0 the selector must
	// default to 5 and all 5 projections survive.
	engine := &fakeSmithEngine{resp: DuelAtomSmithResponse{Picks: []int{0, 1, 2, 3, 4}}}
	sel := newSmithSelector(repo, engine, nil)

	picks, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID: "t", ExcludeGCIDs: []string{"g"},
	})
	if err != nil {
		t.Fatalf("SelectAtoms: %v", err)
	}
	if len(picks) != 5 {
		t.Fatalf("picks len = %d, want 5 (Count 0 defaults to 5)", len(picks))
	}
}

func TestDuelAtomSmithSelector_ListRandomError(t *testing.T) {
	reader := &errListRandomReader{err: errListRandomFake}
	sel := NewDuelAtomSmithSelector(reader, NewProjectionAtomSelector(reader), nil, &fakeSmithEngine{})
	_, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID: "t", ExcludeGCIDs: []string{"g"}, Count: 1,
	})
	if !errors.Is(err, errListRandomFake) {
		t.Errorf("err = %v, want wrapped ListRandom error", err)
	}
}

func TestDuelAtomSmithSelector_FiltersThinOptionsAndEmptyAnswer(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	seedProjection(t, repo, "atom-thin", "o1", "thin", atom_projection.QuestionTypeMCQ, []string{"A"}, "A")
	seedProjection(t, repo, "atom-noanswer", "o2", "noanswer", atom_projection.QuestionTypeMCQ, []string{"A", "B"}, "  ")
	seedMCQProjection(t, repo, "atom-valid", "valid", "A", []string{"A", "B"})
	engine := &fakeSmithEngine{resp: DuelAtomSmithResponse{Picks: []int{0}}}
	sel := newSmithSelector(repo, engine, nil)

	picks, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID: "t", ExcludeGCIDs: []string{"g"}, Count: 5,
	})
	if err != nil {
		t.Fatalf("SelectAtoms: %v", err)
	}
	if len(picks) != 1 || picks[0].AtomID != "atom-valid" {
		t.Fatalf("picks = %+v, want exactly [atom-valid] (thin-option + blank-answer filtered)", picks)
	}
}

func TestDuelAtomSmithSelector_ProfileErrorIsBestEffort(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	seedMCQProjection(t, repo, "atom-1", "Q1", "A", []string{"A", "B"})
	engine := &fakeSmithEngine{resp: DuelAtomSmithResponse{Picks: []int{0}}}
	profiles := &fakeProfileReader{err: errors.New("profiler down")}
	sel := newSmithSelector(repo, engine, profiles)

	picks, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID: "t", ExcludeGCIDs: []string{"g-a", "g-b"}, Count: 1,
	})
	if err != nil {
		t.Fatalf("SelectAtoms: %v (profile errors are best-effort, not fatal)", err)
	}
	if len(picks) != 1 {
		t.Fatalf("picks len = %d, want 1", len(picks))
	}
	if len(engine.gotReq.Profiles) != 0 {
		t.Errorf("engine got Profiles %v, want empty (all lookups failed)", engine.gotReq.Profiles)
	}
	if engine.gotReq.UserGCID != "g-a" {
		t.Errorf("UserGCID = %q, want g-a (first excluded GCID)", engine.gotReq.UserGCID)
	}
}

func TestDuelAtomSmithSelector_GeneratedTruncatedOnceFull(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	seedMCQProjection(t, repo, "atom-1", "Q1", "A", []string{"A", "B"})
	engine := &fakeSmithEngine{resp: DuelAtomSmithResponse{
		Picks: []int{0},
		Generated: []DuelAtomSmithGenerated{
			{Question: "Gen 1", Options: []string{"A", "B", "C", "D"}, CorrectAnswer: "A"},
			{Question: "Gen 2", Options: []string{"A", "B", "C", "D"}, CorrectAnswer: "A"},
		},
	}}
	sel := newSmithSelector(repo, engine, nil)

	picks, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID: "t", ExcludeGCIDs: []string{"g"}, Count: 1,
	})
	if err != nil {
		t.Fatalf("SelectAtoms: %v", err)
	}
	if len(picks) != 1 || picks[0].Question != "Q1" {
		t.Fatalf("picks = %+v, want the single pick (generated loop must stop once Count reached)", picks)
	}
}

func TestDuelAtomSmithSelector_GeneratedEmptyQuestionDropped(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	engine := &fakeSmithEngine{resp: DuelAtomSmithResponse{
		Generated: []DuelAtomSmithGenerated{
			{Question: "   ", Options: []string{"A", "B", "C", "D"}, CorrectAnswer: "A"},
			{Question: "Good generated", Options: []string{"A", "B", "C", "D"}, CorrectAnswer: "A"},
		},
	}}
	sel := newSmithSelector(repo, engine, nil)

	picks, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID: "t", ExcludeGCIDs: []string{"g"}, Count: 2,
	})
	if err != nil {
		t.Fatalf("SelectAtoms: %v", err)
	}
	if len(picks) != 1 || picks[0].Question != "Good generated" {
		t.Fatalf("picks = %+v, want only the non-blank generated", picks)
	}
}

func TestDuelAtomSmithSelector_GetAtomForRoundNilInner(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	sel := NewDuelAtomSmithSelector(repo, nil, nil, &fakeSmithEngine{})
	_, err := sel.GetAtomForRound(context.Background(), &duel.Duel{Rounds: []duel.RoundSnapshot{{RoundNumber: 1}}}, 1)
	if !errors.Is(err, duel.ErrRoundOutOfRange) {
		t.Errorf("err = %v, want ErrRoundOutOfRange (nil inner)", err)
	}
}

func TestMatchOption(t *testing.T) {
	options := []string{"Water", "Salt", "Sugar", "Acid"}
	cases := []struct {
		name string
		want string
		exp  string
	}{
		{"exact", "Water", "Water"},
		{"case-insensitive", "water", "Water"},
		{"trimmed", "  Acid ", "Acid"},
		{"trimmed case-insensitive", " acid", "Acid"},
		{"no match", "Zinc", ""},
		{"empty want", "", ""},
		{"blank want", "   ", ""},
	}
	for _, tc := range cases {
		if got := matchOption(options, tc.want); got != tc.exp {
			t.Errorf("matchOption(%v, %q) = %q, want %q", options, tc.want, got, tc.exp)
		}
	}
	if got := matchOption(nil, "Water"); got != "" {
		t.Errorf("matchOption(nil, Water) = %q, want empty", got)
	}
}

func TestProfileSummary(t *testing.T) {
	if got := profileSummary(nil); got != "" {
		t.Errorf("profileSummary(nil) = %q, want empty", got)
	}

	p := &profiler.Profile{
		Tags: []profiler.InterestTag{
			{Category: profiler.TagCategory("science"), Tag: "clean_architecture"},
			{Category: profiler.TagCategory("science"), Tag: "design_patterns"},
		},
		Proficiency: profiler.Proficiency{
			PerCategory: map[string]profiler.ProficiencyLevel{
				"science": profiler.ProficiencyAdvanced,
			},
		},
	}
	got := profileSummary(p)
	if got != "tags: clean_architecture, design_patterns; science:advanced" {
		t.Errorf("profileSummary = %q, want tags + per-category summary", got)
	}
}
