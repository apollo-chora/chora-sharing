// Package clients — ProjectionAtomSelector is the embedded-first
// round-delivery component for duel atoms. It reads the round snapshot's
// embedded Question/Options/CorrectAnswer (the post-embedded-fields
// shape, or a generated atom with no projection row), falling back to
// the atom_projection read-model for legacy rows persisted before
// embedding.
//
// Atom selection for new duels (AI pick+generate) is owned by the
// DuelAtomSmithSelector in duel_atom_smith_selector.go. When the smith
// agent is not wired (local dev), ProjectionAtomSelector also serves as
// the standalone SelectAtoms implementation — it picks random MCQ
// projections from the read-model (no AI).
package clients

import (
	"context"
	"fmt"

	"github.com/apollo-chora/chora-sharing/internal/adapter/http"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
)

// ProjectionAtomSelector is the embedded-first round-delivery component.
// It no longer selects atoms for new duels — that path is owned by the
// DuelAtomSmithSelector (AI pick+generate). This type stays as the
// round-delivery reader: GetAtomForRound reads the round snapshot's
// embedded Question/Options/CorrectAnswer (falling back to the
// projection read-model for legacy rows persisted before embedding).
type ProjectionAtomSelector struct {
	reader atom_projection.AtomProjectionReader
}

// NewProjectionAtomSelector constructs a ProjectionAtomSelector.
func NewProjectionAtomSelector(reader atom_projection.AtomProjectionReader) *ProjectionAtomSelector {
	return &ProjectionAtomSelector{reader: reader}
}

// GetAtomForRound returns the atom pick for a specific round. It is
// embedded-first: if the round snapshot already carries Question text
// (the path for generated atoms and for rounds persisted after the
// embedded-fields change), the embedded Question/Options/CorrectAnswer
// are returned directly with no projection fetch.
//
// For legacy rounds persisted before the embedded-fields change (empty
// Question), it falls back to the projection read-model and fetches
// Stem/Options/CorrectAnswer by atom_id. The projection read-model stays
// the source of truth for projection-backed rounds; embedding only
// removes the per-round-delivery fetch.
func (s *ProjectionAtomSelector) GetAtomForRound(ctx context.Context, d *duel.Duel, roundNo int) (duel.AtomPick, error) {
	if d == nil || roundNo < 1 || roundNo > len(d.Rounds) {
		return duel.AtomPick{}, duel.ErrRoundOutOfRange
	}
	rd := d.Rounds[roundNo-1]
	pick := duel.AtomPick{
		AtomID:     rd.AtomID,
		RevisionID: rd.AtomRevisionID,
	}
	if rd.Question != "" {
		pick.Question = rd.Question
		pick.Options = append([]string(nil), rd.Options...)
		pick.Answer = rd.CorrectAnswer
		return pick, nil
	}
	// Legacy path: round persisted before embedded fields — fetch from
	// the projection read-model.
	if s != nil && s.reader != nil {
		p, err := s.reader.Get(ctx, rd.AtomID)
		if err == nil {
			pick.Question = p.Stem
			pick.Options = append([]string(nil), p.Options...)
			pick.Answer = p.CorrectAnswer
		}
		// ErrNotFound is non-fatal: the round is still addressable by
		// atom_id, but the question text + correctness check will be
		// empty (as before this fix). Don't mask other errors either —
		// the callers already treat an empty Answer as "not scorable".
	}
	return pick, nil
}

// SelectAtoms picks random MCQ projections from the read-model for a new
// duel. This is the local-dev fallback when the duel_atom_smith AI agent
// is not wired — no AI generation, just random selection from published
// atoms. Mirrors the candidate-pool logic in DuelAtomSmithSelector.
func (s *ProjectionAtomSelector) SelectAtoms(ctx context.Context, req httpadapter.AtomSelectionRequest) ([]duel.AtomPick, error) {
	if s == nil || s.reader == nil {
		return nil, duel.ErrNoAtomsAvailable
	}
	if req.Count <= 0 {
		req.Count = 5
	}
	projs, err := s.reader.ListRandom(ctx, req.TenantID, req.ExcludeGCIDs, req.Count*2)
	if err != nil {
		return nil, fmt.Errorf("projection atom selector: list random: %w", err)
	}
	picks := make([]duel.AtomPick, 0, req.Count)
	for _, p := range projs {
		if p.QuestionType != atom_projection.QuestionTypeMCQ {
			continue
		}
		if len(p.Options) < 2 {
			continue
		}
		picks = append(picks, duel.AtomPick{
			AtomID:     p.AtomID,
			RevisionID: p.RevisionID,
			Question:   p.Stem,
			Options:    append([]string(nil), p.Options...),
			Answer:     p.CorrectAnswer,
		})
		if len(picks) >= req.Count {
			break
		}
	}
	if len(picks) == 0 {
		return nil, duel.ErrNoAtomsAvailable
	}
	return picks, nil
}
