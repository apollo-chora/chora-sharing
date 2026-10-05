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
)

// errListRandomReader is an AtomProjectionReader whose ListRandom always
// fails — the inmem repo never errors, so the error wrap elsewhere in the
// package needs a dedicated double. Get is unused by the paths under test.
type errListRandomReader struct {
	atom_projection.AtomProjectionReader
	err error
}

func (r *errListRandomReader) ListRandom(_ context.Context, _ string, _ []string, _ int) ([]atom_projection.Projection, error) {
	return nil, r.err
}

var errListRandomFake = errors.New("read model down")

func seedProjection(t *testing.T, repo *inmem.ProjectionRepo, atomID, owner, stem string, qtype atom_projection.QuestionType, options []string, answer string) {
	t.Helper()
	if err := repo.Upsert(context.Background(), atom_projection.Projection{
		AtomID:        atomID,
		RevisionID:    atomID + "-rev",
		OwnerGCID:     owner,
		Stem:          stem,
		QuestionType:  qtype,
		Options:       options,
		CorrectAnswer: answer,
	}); err != nil {
		t.Fatalf("seed projection %s: %v", atomID, err)
	}
}

func TestProjectionAtomSelector_SelectAtoms_Success(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	for i := 1; i <= 5; i++ {
		seedProjection(t, repo, fmt.Sprintf("atom-%d", i), "owner", fmt.Sprintf("Q%d", i), atom_projection.QuestionTypeMCQ, []string{"A", "B"}, "A")
	}
	sel := NewProjectionAtomSelector(repo)

	picks, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID:     "tenant-x",
		ExcludeGCIDs: []string{"other-player"},
		Count:        3,
	})
	if err != nil {
		t.Fatalf("SelectAtoms: %v", err)
	}
	if len(picks) != 3 {
		t.Fatalf("picks len = %d, want 3", len(picks))
	}
	for _, p := range picks {
		if p.AtomID == "" || p.Question == "" {
			t.Errorf("pick missing identity: %+v", p)
		}
		if len(p.Options) != 2 || p.Answer != "A" {
			t.Errorf("pick fields wrong: %+v", p)
		}
	}
}

func TestProjectionAtomSelector_SelectAtoms_DefaultCount(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	for i := 1; i <= 6; i++ {
		seedProjection(t, repo, fmt.Sprintf("atom-d%d", i), "owner", "QD", atom_projection.QuestionTypeMCQ, []string{"A", "B"}, "A")
	}
	sel := NewProjectionAtomSelector(repo)

	// Count 0 → defaults to 5 (and breaks once full).
	picks, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{TenantID: "t"})
	if err != nil {
		t.Fatalf("SelectAtoms: %v", err)
	}
	if len(picks) != 5 {
		t.Fatalf("picks len = %d, want 5 (default count)", len(picks))
	}
}

func TestProjectionAtomSelector_SelectAtoms_ExcludesOwners(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	seedProjection(t, repo, "atom-self", "self-gcid", "mine", atom_projection.QuestionTypeMCQ, []string{"A", "B"}, "A")
	seedProjection(t, repo, "atom-other-1", "owner-a", "Q1", atom_projection.QuestionTypeMCQ, []string{"A", "B"}, "A")
	seedProjection(t, repo, "atom-other-2", "owner-b", "Q2", atom_projection.QuestionTypeMCQ, []string{"A", "B"}, "A")
	seedProjection(t, repo, "atom-other-3", "owner-c", "Q3", atom_projection.QuestionTypeMCQ, []string{"A", "B"}, "A")
	sel := NewProjectionAtomSelector(repo)

	picks, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{
		TenantID:     "t",
		ExcludeGCIDs: []string{"self-gcid"},
		Count:        3,
	})
	if err != nil {
		t.Fatalf("SelectAtoms: %v", err)
	}
	if len(picks) != 3 {
		t.Fatalf("picks len = %d, want 3", len(picks))
	}
	for _, p := range picks {
		if p.AtomID == "atom-self" {
			t.Error("own atom leaked into the pool despite ExcludeGCIDs")
		}
	}
}

func TestProjectionAtomSelector_SelectAtoms_FiltersAndDedupes(t *testing.T) {
	repo := inmem.NewProjectionRepo()
	seedProjection(t, repo, "atom-oe", "owner-e", "open", atom_projection.QuestionTypeOE, []string{"A", "B"}, "A")
	seedProjection(t, repo, "atom-thin", "owner-f", "thin", atom_projection.QuestionTypeMCQ, []string{"A"}, "A")
	seedProjection(t, repo, "atom-valid", "owner-g", "valid", atom_projection.QuestionTypeMCQ, []string{"A", "B"}, "A")
	sel := NewProjectionAtomSelector(repo)

	// Exactly one publishable candidate survives the filter regardless of
	// pool iteration order, so the chosen atom is deterministic.
	picks, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{TenantID: "t", Count: 5})
	if err != nil {
		t.Fatalf("SelectAtoms: %v", err)
	}
	if len(picks) != 1 {
		t.Fatalf("picks len = %d, want 1 (OE + thin-option filtered)", len(picks))
	}
	if picks[0].AtomID != "atom-valid" {
		t.Errorf("picks[0].AtomID = %q, want atom-valid", picks[0].AtomID)
	}
}

func TestProjectionAtomSelector_SelectAtoms_NoAtomsAvailable(t *testing.T) {
	sel := NewProjectionAtomSelector(inmem.NewProjectionRepo())
	if _, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{TenantID: "t"}); !errors.Is(err, duel.ErrNoAtomsAvailable) {
		t.Errorf("empty repo: err = %v, want ErrNoAtomsAvailable", err)
	}

	repo := inmem.NewProjectionRepo()
	seedProjection(t, repo, "atom-oe", "owner", "open", atom_projection.QuestionTypeOE, []string{"A", "B"}, "A")
	sel2 := NewProjectionAtomSelector(repo)
	if _, err := sel2.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{TenantID: "t"}); !errors.Is(err, duel.ErrNoAtomsAvailable) {
		t.Errorf("only-OE repo: err = %v, want ErrNoAtomsAvailable", err)
	}
}

func TestProjectionAtomSelector_SelectAtoms_ListRandomError(t *testing.T) {
	reader := &errListRandomReader{err: errListRandomFake}
	sel := NewProjectionAtomSelector(reader)
	_, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{TenantID: "t"})
	if !errors.Is(err, errListRandomFake) {
		t.Errorf("err = %v, want wrapped read-model error", err)
	}
}

func TestProjectionAtomSelector_SelectAtoms_NilReaderAndReceiver(t *testing.T) {
	var nilSel *ProjectionAtomSelector
	if _, err := nilSel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{}); !errors.Is(err, duel.ErrNoAtomsAvailable) {
		t.Errorf("nil receiver: err = %v, want ErrNoAtomsAvailable", err)
	}
	sel := NewProjectionAtomSelector(nil)
	if _, err := sel.SelectAtoms(context.Background(), httpadapter.AtomSelectionRequest{}); !errors.Is(err, duel.ErrNoAtomsAvailable) {
		t.Errorf("nil reader: err = %v, want ErrNoAtomsAvailable", err)
	}
}
