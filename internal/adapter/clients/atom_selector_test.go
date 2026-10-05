package clients

import (
	"context"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
)

// TestProjectionAtomSelector_GetAtomForRound_PopulatesQuestion is the
// regression test for the "Duel Arena Round: 0/0, no atom" bug: the WS
// round-start frame and the HTTP answer-correctness check both call
// GetAtomForRound and depend on Question/Options/Answer being populated
// from the projection read-model. Before the fix, GetAtomForRound
// discarded the reader and returned only atom_id + revision_id, so the
// FE rendered an empty question with no options and no answer was ever
// scored correct.
func TestProjectionAtomSelector_GetAtomForRound_PopulatesQuestion(t *testing.T) {
	const atomID = "11111111-2222-3333-4444-555555555555"
	const revisionID = "22222222-3333-4444-5555-666666666666"
	const stem = "Which planet in our solar system has the most known moons?"
	options := []string{"Mars", "Jupiter", "Saturn", "Neptune"}
	const answer = "Saturn"

	repo := inmem.NewProjectionRepo()
	if err := repo.Upsert(context.Background(), atom_projection.Projection{
		AtomID:        atomID,
		RevisionID:    revisionID,
		OwnerGCID:     "33333333-4444-5555-6666-777777777777",
		Stem:          stem,
		QuestionType:  atom_projection.QuestionTypeMCQ,
		Options:       options,
		CorrectAnswer: answer,
	}); err != nil {
		t.Fatalf("seed projection: %v", err)
	}

	selector := NewProjectionAtomSelector(repo)

	d := &duel.Duel{
		Rounds: []duel.RoundSnapshot{{
			RoundNumber:    1,
			AtomID:         atomID,
			AtomRevisionID: revisionID,
		}},
	}

	pick, err := selector.GetAtomForRound(context.Background(), d, 1)
	if err != nil {
		t.Fatalf("GetAtomForRound: unexpected error: %v", err)
	}
	if pick.AtomID != atomID {
		t.Errorf("AtomID = %q, want %q", pick.AtomID, atomID)
	}
	if pick.Question != stem {
		t.Errorf("Question = %q, want %q (the bug: empty question rendered as 0/0 in the FE)", pick.Question, stem)
	}
	if len(pick.Options) != len(options) {
		t.Fatalf("Options len = %d, want %d", len(pick.Options), len(options))
	}
	for i, opt := range options {
		if pick.Options[i] != opt {
			t.Errorf("Options[%d] = %q, want %q", i, pick.Options[i], opt)
		}
	}
	if pick.Answer != answer {
		t.Errorf("Answer = %q, want %q (the bug: no correct answer meant rounds were never scorable)", pick.Answer, answer)
	}
}

// TestProjectionAtomSelector_GetAtomForRound_MissingProjectionStillReturnsIDs
// guards the non-fatal ErrNotFound path: if the projection has been
// archived or never cached, the round is still addressable by atom_id —
// callers already treat an empty Answer as "not scorable". This must not
// error, or the WS round-delivery would hard-fail an entire duel on one
// missing projection row.
func TestProjectionAtomSelector_GetAtomForRound_MissingProjectionStillReturnsIDs(t *testing.T) {
	repo := inmem.NewProjectionRepo() // empty — no projection cached
	selector := NewProjectionAtomSelector(repo)

	const atomID = "99999999-9999-4999-9999-999999999999"
	d := &duel.Duel{
		Rounds: []duel.RoundSnapshot{{
			RoundNumber: 1,
			AtomID:      atomID,
		}},
	}

	pick, err := selector.GetAtomForRound(context.Background(), d, 1)
	if err != nil {
		t.Fatalf("GetAtomForRound on missing projection: unexpected error: %v", err)
	}
	if pick.AtomID != atomID {
		t.Errorf("AtomID = %q, want %q", pick.AtomID, atomID)
	}
	if pick.Question != "" {
		t.Errorf("Question = %q, want empty (no projection cached)", pick.Question)
	}
	if pick.Answer != "" {
		t.Errorf("Answer = %q, want empty (no projection cached)", pick.Answer)
	}
}

// TestProjectionAtomSelector_GetAtomForRound_BoundsChecks verifies the
// round-number bounds + nil-duel guards the original method enforced —
// the fix must preserve them.
func TestProjectionAtomSelector_GetAtomForRound_BoundsChecks(t *testing.T) {
	selector := NewProjectionAtomSelector(inmem.NewProjectionRepo())

	if _, err := selector.GetAtomForRound(context.Background(), nil, 1); err != duel.ErrRoundOutOfRange {
		t.Errorf("nil duel: err = %v, want %v", err, duel.ErrRoundOutOfRange)
	}
	d := &duel.Duel{Rounds: []duel.RoundSnapshot{{RoundNumber: 1}}}
	if _, err := selector.GetAtomForRound(context.Background(), d, 0); err != duel.ErrRoundOutOfRange {
		t.Errorf("round 0: err = %v, want %v", err, duel.ErrRoundOutOfRange)
	}
	if _, err := selector.GetAtomForRound(context.Background(), d, 2); err != duel.ErrRoundOutOfRange {
		t.Errorf("round 2 of 1: err = %v, want %v", err, duel.ErrRoundOutOfRange)
	}
}

// TestProjectionAtomSelector_GetAtomForRound_EmbeddedDataReturnedWithoutProjection
// verifies the embedded-first path: when the round snapshot carries
// Question/Options/CorrectAnswer (the post-embedded-fields shape, or a
// generated atom with no projection row), GetAtomForRound returns the
// embedded values directly without touching the projection repo.
func TestProjectionAtomSelector_GetAtomForRound_EmbeddedDataReturnedWithoutProjection(t *testing.T) {
	// Empty projection repo — the embedded path must not need it.
	selector := NewProjectionAtomSelector(inmem.NewProjectionRepo())

	const atomID = "55555555-6666-7777-8888-999999999999"
	question := "Which gas makes up ~78% of Earth's atmosphere?"
	options := []string{"Oxygen", "Nitrogen", "Carbon dioxide", "Hydrogen"}
	const answer = "Nitrogen"

	d := &duel.Duel{
		Rounds: []duel.RoundSnapshot{{
			RoundNumber:    1,
			AtomID:         atomID,
			AtomRevisionID: "", // generated atom — no revision
			Question:       question,
			Options:        options,
			CorrectAnswer:  answer,
		}},
	}

	pick, err := selector.GetAtomForRound(context.Background(), d, 1)
	if err != nil {
		t.Fatalf("GetAtomForRound embedded: unexpected error: %v", err)
	}
	if pick.AtomID != atomID {
		t.Errorf("AtomID = %q, want %q", pick.AtomID, atomID)
	}
	if pick.RevisionID != "" {
		t.Errorf("RevisionID = %q, want empty (generated atom)", pick.RevisionID)
	}
	if pick.Question != question {
		t.Errorf("Question = %q, want %q", pick.Question, question)
	}
	if len(pick.Options) != len(options) {
		t.Fatalf("Options len = %d, want %d", len(pick.Options), len(options))
	}
	for i, opt := range options {
		if pick.Options[i] != opt {
			t.Errorf("Options[%d] = %q, want %q", i, pick.Options[i], opt)
		}
	}
	if pick.Answer != answer {
		t.Errorf("Answer = %q, want %q", pick.Answer, answer)
	}
}
