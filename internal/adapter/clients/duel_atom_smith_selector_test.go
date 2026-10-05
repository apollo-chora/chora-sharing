package clients

import (
	"context"
	"errors"
	"testing"

	httpadapter "github.com/apollo-chora/chora-sharing/internal/adapter/http"
	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
	"github.com/apollo-chora/chora-sharing/internal/domain/profiler"
)

// fakeSmithEngine implements DuelAtomSmithEngine for tests.
type fakeSmithEngine struct {
	resp   DuelAtomSmithResponse
	err    error
	gotReq DuelAtomSmithRequest
	called bool
}

func (f *fakeSmithEngine) ConjureDuelAtoms(_ context.Context, req DuelAtomSmithRequest) (DuelAtomSmithResponse, error) {
	f.called = true
	f.gotReq = req
	if f.err != nil {
		return DuelAtomSmithResponse{}, f.err
	}
	return f.resp, nil
}

// fakeProfileReader implements ProfileReader for tests.
type fakeProfileReader struct {
	profiles map[string]*profiler.Profile
	err      error
}

func (f *fakeProfileReader) GetProfile(_ context.Context, gcid string) (*profiler.Profile, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.profiles[gcid], nil
}

func seedMCQProjection(t *testing.T, repo *inmem.ProjectionRepo, atomID, stem, answer string, options []string) {
	t.Helper()
	if err := repo.Upsert(context.Background(), atom_projection.Projection{
		AtomID:        atomID,
		RevisionID:    atomID + "-rev",
		OwnerGCID:     "owner-gcid",
		Stem:          stem,
		QuestionType:  atom_projection.QuestionTypeMCQ,
		Options:       options,
		CorrectAnswer: answer,
	}); err != nil {
		t.Fatalf("seed projection %s: %v", atomID, err)
	}
}

func newSmithSelector(repo *inmem.ProjectionRepo, engine DuelAtomSmithEngine, profiles ProfileReader) *DuelAtomSmithSelector {
	return NewDuelAtomSmithSelector(repo, NewProjectionAtomSelector(repo), profiles, engine)
}

// orderedProjectionReader is a candidate-pool reader that returns projections
// in INSERTION order.
//
// The two tests below assert filtered POSITIONS ("the first VALID MCQ resolves
// to index 0"), which needs a deterministic pool. inmem.ProjectionRepo.
// ListRandom ranges over a map (inmem/projection.go:86), so Go randomises the
// order and those assertions failed roughly one run in five. The repo is
// behaving exactly as its name promises; the tests were the ones assuming an
// order it never offered. Only the POOL is swapped: the inner selector still
// reads the real repo, so nothing else about the pipeline changes.
type orderedProjectionReader struct {
	projs []atom_projection.Projection
}

// Get satisfies atom_projection.AtomProjectionReader. The pool reader exists to
// control ORDER, so Get simply serves the same slice by id.
func (r *orderedProjectionReader) Get(_ context.Context, atomID string) (atom_projection.Projection, error) {
	for _, p := range r.projs {
		if p.AtomID == atomID {
			return p, nil
		}
	}
	return atom_projection.Projection{}, atom_projection.ErrNotFound
}

func (r *orderedProjectionReader) ListRandom(_ context.Context, _ string, excludeGCIDs []string, limit int) ([]atom_projection.Projection, error) {
	exclude := make(map[string]bool, len(excludeGCIDs))
	for _, g := range excludeGCIDs {
		exclude[g] = true
	}
	out := make([]atom_projection.Projection, 0, limit)
	for _, p := range r.projs {
		if p.Archived || exclude[p.OwnerGCID] {
			continue
		}
		out = append(out, p)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// seedOrdered puts a projection in BOTH the ordered pool and the repo, so
// position assertions are deterministic while the inner selector still
// resolves rounds from the repo.
func seedOrdered(t *testing.T, pool *orderedProjectionReader, repo *inmem.ProjectionRepo, p atom_projection.Projection) {
	t.Helper()
	if err := repo.Upsert(context.Background(), p); err != nil {
		t.Fatalf("seed projection %s: %v", p.AtomID, err)
	}
	pool.projs = append(pool.projs, p)
}

func mcqProjection(atomID, stem, answer string, options []string) atom_projection.Projection {
	return atom_projection.Projection{
		AtomID:        atomID,
		RevisionID:    atomID + "-rev",
		OwnerGCID:     "owner-gcid",
		Stem:          stem,
		QuestionType:  atom_projection.QuestionTypeMCQ,
		Options:       options,
		CorrectAnswer: answer,
	}
}

func newSmithSelectorOrdered(pool *orderedProjectionReader, repo *inmem.ProjectionRepo, engine DuelAtomSmithEngine, profiles ProfileReader) *DuelAtomSmithSelector {
	return NewDuelAtomSmithSelector(pool, NewProjectionAtomSelector(repo), profiles, engine)
}

func TestDuelAtomSmithSelector_PicksOnly(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	pool := &orderedProjectionReader{}
	seedOrdered(t, pool, repo, mcqProjection("atom-1", "What is 2+2?", "4", []string{"3", "4", "5", "6"}))
	seedOrdered(t, pool, repo, mcqProjection("atom-2", "What is the capital of France?", "Paris", []string{"London", "Berlin", "Paris", "Madrid"}))

	engine := &fakeSmithEngine{resp: DuelAtomSmithResponse{
		Picks: []int{0, 1},
	}}
	sel := newSmithSelectorOrdered(pool, repo, engine, nil)

	picks, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID:     "tenant-x",
		ExcludeGCIDs: []string{"gcid-a", "gcid-b"},
		Count:        2,
	})
	if err != nil {
		t.Fatalf("SelectAtoms: %v", err)
	}
	if len(picks) != 2 {
		t.Fatalf("picks len = %d, want 2", len(picks))
	}
	if picks[0].AtomID != "atom-1" {
		t.Errorf("picks[0].AtomID = %q, want atom-1", picks[0].AtomID)
	}
	if picks[0].RevisionID != "atom-1-rev" {
		t.Errorf("picks[0].RevisionID = %q, want atom-1-rev", picks[0].RevisionID)
	}
	if picks[1].AtomID != "atom-2" {
		t.Errorf("picks[1].AtomID = %q, want atom-2", picks[1].AtomID)
	}
}

func TestDuelAtomSmithSelector_GenerateOnly(t *testing.T) {
	// Empty pool — agent generates all atoms.
	repo := inmem.NewProjectionRepo()
	engine := &fakeSmithEngine{resp: DuelAtomSmithResponse{
		Generated: []DuelAtomSmithGenerated{
			{Question: "What is H2O?", Options: []string{"Water", "Salt", "Sugar", "Acid"}, CorrectAnswer: "Water"},
			{Question: "What is the speed of light?", Options: []string{"3e8 m/s", "3e6 m/s", "3e10 m/s", "3e4 m/s"}, CorrectAnswer: "3e8 m/s"},
		},
	}}
	sel := newSmithSelector(repo, engine, nil)

	picks, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID:     "tenant-x",
		ExcludeGCIDs: []string{"gcid-a", "gcid-b"},
		Count:        2,
	})
	if err != nil {
		t.Fatalf("SelectAtoms: %v", err)
	}
	if len(picks) != 2 {
		t.Fatalf("picks len = %d, want 2", len(picks))
	}
	for i, p := range picks {
		if p.AtomID == "" {
			t.Errorf("picks[%d].AtomID empty, want UUID", i)
		}
		if p.RevisionID != "" {
			t.Errorf("picks[%d].RevisionID = %q, want empty (generated)", i, p.RevisionID)
		}
		if p.Question == "" {
			t.Errorf("picks[%d].Question empty", i)
		}
		if len(p.Options) != 4 {
			t.Errorf("picks[%d].Options len = %d, want 4", i, len(p.Options))
		}
		if p.Answer == "" {
			t.Errorf("picks[%d].Answer empty", i)
		}
	}
	if picks[0].Answer != "Water" {
		t.Errorf("picks[0].Answer = %q, want Water", picks[0].Answer)
	}
}

func TestDuelAtomSmithSelector_MalformedEnvelope(t *testing.T) {
	// Engine returns error — selector propagates.
	repo := inmem.NewProjectionRepo()
	seedMCQProjection(t, repo, "atom-1", "Q", "A", []string{"A", "B"})
	engine := &fakeSmithEngine{err: errors.New("agent transport down")}
	sel := newSmithSelector(repo, engine, nil)

	_, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID:     "tenant-x",
		ExcludeGCIDs: []string{"gcid-a"},
		Count:        1,
	})
	if err == nil {
		t.Fatal("expected error on engine failure")
	}
}

func TestDuelAtomSmithSelector_OutOfRangeIndicesDropped(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	seedMCQProjection(t, repo, "atom-1", "Q1", "A", []string{"A", "B"})
	engine := &fakeSmithEngine{resp: DuelAtomSmithResponse{
		Picks: []int{0, 5, -1}, // 0 valid, 5 out of range, -1 out of range
		Generated: []DuelAtomSmithGenerated{
			{Question: "Gen Q", Options: []string{"A", "B", "C", "D"}, CorrectAnswer: "A"},
		},
	}}
	sel := newSmithSelector(repo, engine, nil)

	picks, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID:     "tenant-x",
		ExcludeGCIDs: []string{"gcid-a"},
		Count:        5,
	})
	if err != nil {
		t.Fatalf("SelectAtoms: %v", err)
	}
	// 1 pick (index 0) + 1 generated = 2
	if len(picks) != 2 {
		t.Fatalf("picks len = %d, want 2 (1 valid pick + 1 generated)", len(picks))
	}
}

func TestDuelAtomSmithSelector_DuplicateIndicesDropped(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	seedMCQProjection(t, repo, "atom-1", "Q1", "A", []string{"A", "B"})
	engine := &fakeSmithEngine{resp: DuelAtomSmithResponse{
		Picks: []int{0, 0, 0}, // all same — only first counts
	}}
	sel := newSmithSelector(repo, engine, nil)

	picks, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID:     "tenant-x",
		ExcludeGCIDs: []string{"gcid-a"},
		Count:        3,
	})
	if err != nil {
		t.Fatalf("SelectAtoms: %v", err)
	}
	if len(picks) != 1 {
		t.Fatalf("picks len = %d, want 1 (duplicates dropped)", len(picks))
	}
}

func TestDuelAtomSmithSelector_GeneratedWith3OptionsDropped(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	engine := &fakeSmithEngine{resp: DuelAtomSmithResponse{
		Generated: []DuelAtomSmithGenerated{
			{Question: "Bad Q", Options: []string{"A", "B", "C"}, CorrectAnswer: "A"}, // only 3 options
			{Question: "Good Q", Options: []string{"A", "B", "C", "D"}, CorrectAnswer: "A"},
		},
	}}
	sel := newSmithSelector(repo, engine, nil)

	picks, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID:     "tenant-x",
		ExcludeGCIDs: []string{"gcid-a"},
		Count:        2,
	})
	if err != nil {
		t.Fatalf("SelectAtoms: %v", err)
	}
	if len(picks) != 1 {
		t.Fatalf("picks len = %d, want 1 (3-option generated dropped)", len(picks))
	}
	if picks[0].Question != "Good Q" {
		t.Errorf("picks[0].Question = %q, want Good Q", picks[0].Question)
	}
}

func TestDuelAtomSmithSelector_GeneratedOffListAnswerDropped(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	engine := &fakeSmithEngine{resp: DuelAtomSmithResponse{
		Generated: []DuelAtomSmithGenerated{
			{Question: "Q", Options: []string{"A", "B", "C", "D"}, CorrectAnswer: "Z"}, // Z not in options
		},
	}}
	sel := newSmithSelector(repo, engine, nil)

	_, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID:     "tenant-x",
		ExcludeGCIDs: []string{"gcid-a"},
		Count:        1,
	})
	if !errors.Is(err, duel.ErrNoAtomsAvailable) {
		t.Errorf("err = %v, want ErrNoAtomsAvailable", err)
	}
}

func TestDuelAtomSmithSelector_GeneratedAnswerCaseInsensitive(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	engine := &fakeSmithEngine{resp: DuelAtomSmithResponse{
		Generated: []DuelAtomSmithGenerated{
			{Question: "Q", Options: []string{"Water", "Salt", "Sugar", "Acid"}, CorrectAnswer: "water"},
		},
	}}
	sel := newSmithSelector(repo, engine, nil)

	picks, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID:     "tenant-x",
		ExcludeGCIDs: []string{"gcid-a"},
		Count:        1,
	})
	if err != nil {
		t.Fatalf("SelectAtoms: %v", err)
	}
	// Answer should be the verbatim option "Water", not the lowercase "water"
	if picks[0].Answer != "Water" {
		t.Errorf("Answer = %q, want Water (verbatim option)", picks[0].Answer)
	}
}

func TestDuelAtomSmithSelector_ZeroValidAtoms(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	// No projections seeded, engine returns nothing
	engine := &fakeSmithEngine{resp: DuelAtomSmithResponse{}}
	sel := newSmithSelector(repo, engine, nil)

	_, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID:     "tenant-x",
		ExcludeGCIDs: []string{"gcid-a"},
		Count:        3,
	})
	if !errors.Is(err, duel.ErrNoAtomsAvailable) {
		t.Errorf("err = %v, want ErrNoAtomsAvailable", err)
	}
}

func TestDuelAtomSmithSelector_TruncatesToCount(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	seedMCQProjection(t, repo, "atom-1", "Q1", "A", []string{"A", "B"})
	seedMCQProjection(t, repo, "atom-2", "Q2", "B", []string{"A", "B"})
	seedMCQProjection(t, repo, "atom-3", "Q3", "A", []string{"A", "B"})
	engine := &fakeSmithEngine{resp: DuelAtomSmithResponse{
		Picks: []int{0, 1, 2},
	}}
	sel := newSmithSelector(repo, engine, nil)

	picks, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID:     "tenant-x",
		ExcludeGCIDs: []string{"gcid-a"},
		Count:        2,
	})
	if err != nil {
		t.Fatalf("SelectAtoms: %v", err)
	}
	if len(picks) != 2 {
		t.Fatalf("picks len = %d, want 2 (truncated to Count)", len(picks))
	}
}

func TestDuelAtomSmithSelector_ProfilesPassedToEngine(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	seedMCQProjection(t, repo, "atom-1", "Q1", "A", []string{"A", "B"})
	engine := &fakeSmithEngine{resp: DuelAtomSmithResponse{Picks: []int{0}}}
	profiles := &fakeProfileReader{profiles: map[string]*profiler.Profile{
		"gcid-a": {Proficiency: profiler.Proficiency{PerCategory: map[string]profiler.ProficiencyLevel{"science": profiler.ProficiencyAdvanced}}},
		"gcid-b": {Proficiency: profiler.Proficiency{}},
	}}
	sel := newSmithSelector(repo, engine, profiles)

	_, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID:     "tenant-x",
		ExcludeGCIDs: []string{"gcid-a", "gcid-b"},
		Count:        1,
	})
	if err != nil {
		t.Fatalf("SelectAtoms: %v", err)
	}
	if len(engine.gotReq.Profiles) != 2 {
		t.Fatalf("Profiles len = %d, want 2", len(engine.gotReq.Profiles))
	}
	if v := engine.gotReq.Profiles["gcid-a"]; v == "" || v == "advanced" {
		t.Errorf("Profiles[gcid-a] = %q, want 'science:advanced' (per-category only, no overall prefix)", v)
	}
	if v := engine.gotReq.Profiles["gcid-b"]; v != "beginner" {
		t.Errorf("Profiles[gcid-b] = %q, want beginner (empty PerCategory defaults to beginner)", v)
	}
}

// TestDuelAtomSmithSelector_PickIndexMatchesFilteredPosition is the
// regression test for the index-mismatch bug: when a non-MCQ/invalid
// projection is mixed into the pool, the agent's pick index must resolve
// against the FILTERED list, not the original projs slice. Before the
// fix, Index was the unfiltered loop position, so a pick of 0 resolved
// to the wrong atom once any projection was filtered out.
func TestDuelAtomSmithSelector_PickIndexMatchesFilteredPosition(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	pool := &orderedProjectionReader{}
	// First projection is OE (open-ended) — filtered out.
	seedOrdered(t, pool, repo, atom_projection.Projection{
		AtomID:        "atom-oe",
		RevisionID:    "rev-oe",
		OwnerGCID:     "owner",
		Stem:          "Open question (should be filtered)",
		QuestionType:  atom_projection.QuestionTypeOE,
		Options:       []string{"A", "B"},
		CorrectAnswer: "A",
	})
	// Second projection is the first VALID MCQ — must be index 0 in the
	// filtered list, not index 1 (its position in the unfiltered pool).
	seedOrdered(t, pool, repo, mcqProjection("atom-mcq-1", "First MCQ", "A", []string{"A", "B", "C", "D"}))
	// Third projection is the second VALID MCQ — index 1 in filtered list.
	seedOrdered(t, pool, repo, mcqProjection("atom-mcq-2", "Second MCQ", "B", []string{"A", "B", "C", "D"}))

	engine := &fakeSmithEngine{resp: DuelAtomSmithResponse{
		Picks: []int{0, 1}, // agent picks both valid MCQs
	}}
	sel := newSmithSelectorOrdered(pool, repo, engine, nil)

	picks, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID:     "tenant-x",
		ExcludeGCIDs: []string{"gcid-a"},
		Count:        2,
	})
	if err != nil {
		t.Fatalf("SelectAtoms: %v", err)
	}
	if len(picks) != 2 {
		t.Fatalf("picks len = %d, want 2", len(picks))
	}
	// Pick 0 must resolve to atom-mcq-1 (first VALID), NOT atom-oe
	// (which was filtered). Before the fix, Index=1 (unfiltered position
	// of atom-mcq-1) would have been out of range or resolved to the
	// wrong atom.
	if picks[0].AtomID != "atom-mcq-1" {
		t.Errorf("picks[0].AtomID = %q, want atom-mcq-1 (filtered index 0)", picks[0].AtomID)
	}
	if picks[0].Question != "First MCQ" {
		t.Errorf("picks[0].Question = %q, want 'First MCQ'", picks[0].Question)
	}
	if picks[1].AtomID != "atom-mcq-2" {
		t.Errorf("picks[1].AtomID = %q, want atom-mcq-2 (filtered index 1)", picks[1].AtomID)
	}
	if picks[1].Question != "Second MCQ" {
		t.Errorf("picks[1].Question = %q, want 'Second MCQ'", picks[1].Question)
	}

	// Verify the candidates sent to the engine also use filtered indices.
	if len(engine.gotReq.Candidates) != 2 {
		t.Fatalf("Candidates len = %d, want 2 (OE filtered)", len(engine.gotReq.Candidates))
	}
	if engine.gotReq.Candidates[0].Index != 0 {
		t.Errorf("Candidates[0].Index = %d, want 0 (filtered position)", engine.gotReq.Candidates[0].Index)
	}
	if engine.gotReq.Candidates[1].Index != 1 {
		t.Errorf("Candidates[1].Index = %d, want 1 (filtered position)", engine.gotReq.Candidates[1].Index)
	}
}

func TestDuelAtomSmithSelector_NonMCQProjectionsFiltered(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	// OE (open-ended) projection — should be filtered out
	if err := repo.Upsert(context.Background(), atom_projection.Projection{
		AtomID:        "atom-oe",
		RevisionID:    "rev-oe",
		OwnerGCID:     "owner",
		Stem:          "Open question",
		QuestionType:  atom_projection.QuestionTypeOE,
		Options:       []string{"A", "B"},
		CorrectAnswer: "A",
	}); err != nil {
		t.Fatalf("seed OE projection: %v", err)
	}
	// MCQ projection — should pass
	seedMCQProjection(t, repo, "atom-mcq", "MCQ question", "A", []string{"A", "B"})
	engine := &fakeSmithEngine{resp: DuelAtomSmithResponse{Picks: []int{0}}}
	sel := newSmithSelector(repo, engine, nil)

	picks, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID:     "tenant-x",
		ExcludeGCIDs: []string{"gcid-a"},
		Count:        1,
	})
	if err != nil {
		t.Fatalf("SelectAtoms: %v", err)
	}
	// The OE projection was filtered; only the MCQ one is in validProjs.
	// Pick index 0 maps to the MCQ projection.
	if picks[0].AtomID != "atom-mcq" {
		t.Errorf("picks[0].AtomID = %q, want atom-mcq (OE filtered)", picks[0].AtomID)
	}
}

func TestDuelAtomSmithSelector_GetAtomForRoundDelegates(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	seedMCQProjection(t, repo, "atom-1", "Embedded Q", "A", []string{"A", "B"})
	sel := newSmithSelector(repo, &fakeSmithEngine{}, nil)

	d := &duel.Duel{
		Rounds: []duel.RoundSnapshot{{
			RoundNumber:    1,
			AtomID:         "atom-1",
			AtomRevisionID: "atom-1-rev",
			Question:       "Embedded Q",
			Options:        []string{"A", "B"},
			CorrectAnswer:  "A",
		}},
	}
	pick, err := sel.GetAtomForRound(context.Background(), d, 1)
	if err != nil {
		t.Fatalf("GetAtomForRound: %v", err)
	}
	if pick.Question != "Embedded Q" {
		t.Errorf("Question = %q, want Embedded Q", pick.Question)
	}
}
