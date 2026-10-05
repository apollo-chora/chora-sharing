// Package clients — DuelAtomSmithSelector implements the AtomSelectorPort
// over the duel_atom_smith ADK agent (GKE web-mode, ADR-169).
//
// Pipeline:
//  1. ListRandom on the atom_projection read-model (25 candidates, both
//     players' GCIDs excluded — never duel on your own atom).
//  2. Filter to MCQ atoms with ≥2 options + a non-empty correct answer.
//  3. Best-effort GetProfile for each player (nil on error — the agent
//     proceeds without profile context if the profiler is unavailable).
//  4. ConjureDuelAtoms on the smith engine with candidates + shared tags
//     + proficiencies + profile summaries + count.
//  5. Validate the agent's envelope: pick indices in range + deduped →
//     AtomPick from the candidate; generated atoms need exactly 4 options
//     + a correct_answer matching one option (case-insensitive trim, the
//     matched option stored verbatim as Answer). Generated atoms get a
//     plain UUID atom_id + empty revision_id (NULL revision marks
//     generated — no cross-domain write).
//  6. Truncate to req.Count; zero valid → ErrNoAtomsAvailable; partial
//     accepted (StartBattle sets RoundCount = len).
//
// Fail-loud (per plan): engine/LLM errors propagate to processMatch
// Fail-loud (per plan): engine/LLM errors propagate to processMatch
// which restores both searchers to the pool.
package clients

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"

	httpadapter "github.com/apollo-chora/chora-sharing/internal/adapter/http"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
	"github.com/apollo-chora/chora-sharing/internal/domain/profiler"
)

// ProfileReader is the narrow profile-read port the selector needs (the
// full ProfileStore from handlers.go satisfies this). Best-effort: errors
// are logged + nil profile passed to the agent.
type ProfileReader interface {
	GetProfile(ctx context.Context, gcid string) (*profiler.Profile, error)
}

// DuelAtomSmithSelector implements httpadapter.AtomSelectorPort by
// delegating atom selection to the duel_atom_smith agent. The inner
// ProjectionAtomSelector handles GetAtomForRound (embedded-first round
// delivery — no AI).
type DuelAtomSmithSelector struct {
	reader   atom_projection.AtomProjectionReader
	inner    *ProjectionAtomSelector
	profiles ProfileReader
	engine   DuelAtomSmithEngine
}

// NewDuelAtomSmithSelector constructs the selector. reader feeds the
// candidate pool; inner handles GetAtomForRound; profiles is best-effort
// per-player context; engine is the smith agent (test seam).
func NewDuelAtomSmithSelector(
	reader atom_projection.AtomProjectionReader,
	inner *ProjectionAtomSelector,
	profiles ProfileReader,
	engine DuelAtomSmithEngine,
) *DuelAtomSmithSelector {
	return &DuelAtomSmithSelector{reader: reader, inner: inner, profiles: profiles, engine: engine}
}

// candidatePoolSize is the over-fetch factor — the agent picks the best
// `count` from a larger pool so it has room to personalise.
const candidatePoolSize = 25

// SelectAtoms runs the pick+generate pipeline. Fail-loud: engine errors
// propagate; zero valid atoms → ErrNoAtomsAvailable.
func (s *DuelAtomSmithSelector) SelectAtoms(ctx context.Context, req httpadapter.AtomSelectionRequest) ([]duel.AtomPick, error) {
	if s == nil || s.engine == nil {
		return nil, duel.ErrNoAtomsAvailable
	}
	if req.Count <= 0 {
		req.Count = 5
	}
	if s.reader == nil {
		return nil, duel.ErrNoAtomsAvailable
	}

	// 1. Candidate pool from the projection read-model.
	projs, err := s.reader.ListRandom(ctx, req.TenantID, req.ExcludeGCIDs, candidatePoolSize)
	if err != nil {
		return nil, fmt.Errorf("duel atom smith: list random: %w", err)
	}

	// 2. Filter to valid MCQ atoms. Index is the position in the FILTERED
	// list (not the original projs slice) so the agent's pick indices resolve
	// against validProjs below — a non-MCQ/invalid projection mixed into the
	// pool would otherwise shift every subsequent index by one.
	candidates := make([]DuelAtomCandidate, 0, len(projs))
	validProjs := make([]atom_projection.Projection, 0, len(projs))
	for _, p := range projs {
		if p.QuestionType != atom_projection.QuestionTypeMCQ {
			continue
		}
		if len(p.Options) < 2 {
			continue
		}
		if strings.TrimSpace(p.CorrectAnswer) == "" {
			continue
		}
		candidates = append(candidates, DuelAtomCandidate{
			Index:    len(candidates),
			Question: p.Stem,
			Options:  append([]string(nil), p.Options...),
		})
		validProjs = append(validProjs, p)
	}

	// 3. Best-effort profile summaries for both players.
	profiles := make(map[string]string, len(req.ExcludeGCIDs))
	if s.profiles != nil {
		for _, gcid := range req.ExcludeGCIDs {
			p, err := s.profiles.GetProfile(ctx, gcid)
			if err != nil || p == nil {
				continue
			}
			profiles[gcid] = profileSummary(p)
		}
	}

	// 4. Conjure atoms via the smith agent.
	userGCID := ""
	if len(req.ExcludeGCIDs) > 0 {
		userGCID = req.ExcludeGCIDs[0]
	}
	resp, err := s.engine.ConjureDuelAtoms(ctx, DuelAtomSmithRequest{
		TenantID:      req.TenantID,
		UserGCID:      userGCID,
		Candidates:    candidates,
		SharedTags:    req.Tags,
		Proficiencies: req.Proficiencies,
		Profiles:      profiles,
		Count:         req.Count,
	})
	if err != nil {
		return nil, fmt.Errorf("duel atom smith: conjure: %w", err)
	}

	// 5. Validate picks + generated atoms.
	picks := make([]duel.AtomPick, 0, req.Count)
	seenIdx := make(map[int]bool, len(resp.Picks))
	for _, idx := range resp.Picks {
		if idx < 0 || idx >= len(validProjs) {
			continue // out of range — dropped
		}
		if seenIdx[idx] {
			continue // duplicate — dropped
		}
		seenIdx[idx] = true
		p := validProjs[idx]
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

	for _, g := range resp.Generated {
		if len(picks) >= req.Count {
			break
		}
		q := strings.TrimSpace(g.Question)
		if q == "" {
			continue
		}
		if len(g.Options) != 4 {
			continue // generated MCQs must have exactly 4 options
		}
		answer := matchOption(g.Options, g.CorrectAnswer)
		if answer == "" {
			continue // correct_answer doesn't match any option — dropped
		}
		picks = append(picks, duel.AtomPick{
			AtomID:     uuid.Must(uuid.NewV7()).String(),
			RevisionID: "", // NULL revision marks generated — no cross-domain write
			Question:   q,
			Options:    append([]string(nil), g.Options...),
			Answer:     answer,
		})
	}

	if len(picks) == 0 {
		return nil, duel.ErrNoAtomsAvailable
	}
	return picks, nil
}

// GetAtomForRound delegates to the inner ProjectionAtomSelector
// (embedded-first round delivery — no AI).
func (s *DuelAtomSmithSelector) GetAtomForRound(ctx context.Context, d *duel.Duel, roundNo int) (duel.AtomPick, error) {
	if s == nil || s.inner == nil {
		return duel.AtomPick{}, duel.ErrRoundOutOfRange
	}
	return s.inner.GetAtomForRound(ctx, d, roundNo)
}

// matchOption returns the option string that matches want (case-insensitive,
// trimmed), or "" if none match. The matched option is returned verbatim
// so the stored Answer is exactly one of Options (not the agent's possibly
// differently-cased correct_answer).
func matchOption(options []string, want string) string {
	want = strings.TrimSpace(strings.ToLower(want))
	if want == "" {
		return ""
	}
	for _, opt := range options {
		if strings.TrimSpace(strings.ToLower(opt)) == want {
			return opt
		}
	}
	return ""
}

// profileSummary renders a one-line summary for the agent combining the
// player's interest tags + proficiency. Format:
// "tags: clean_architecture, design_patterns; science:advanced, mathematics:intermediate".
// An empty PerCategory map yields "beginner" (default difficulty — the smith
// agent picks entry-level atoms when there's no signal).
// The tags let the agent personalise across BOTH players' interests, not
// just the shared intersection.
func profileSummary(p *profiler.Profile) string {
	if p == nil {
		return ""
	}
	var parts []string
	// Interest tags first so the agent sees each player's unique topics.
	if len(p.Tags) > 0 {
		tags := make([]string, 0, len(p.Tags))
		for _, t := range p.Tags {
			tags = append(tags, t.Tag)
		}
		parts = append(parts, "tags: "+strings.Join(tags, ", "))
	}
	// Proficiency.
	if len(p.Proficiency.PerCategory) == 0 {
		parts = append(parts, string(profiler.ProficiencyBeginner))
	} else {
		for cat, lvl := range p.Proficiency.PerCategory {
			parts = append(parts, cat+":"+string(lvl))
		}
	}
	return strings.Join(parts, "; ")
}
