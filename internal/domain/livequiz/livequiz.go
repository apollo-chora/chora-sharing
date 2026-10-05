// Package livequiz is the pure-domain bridge for the LiveQuiz feature of the
// Content Sharing domain.
//
// This package holds ONLY the bridge ports + the arm-time gate helper logic.
// The actual LiveQuiz aggregate lives in chora-delivery (the Content Delivery
// domain); chora-sharing exposes two bridge RPCs — AuthorizeLiveQuizAtoms
// (§7.8, the arm-time gate) + GenerateQuizFromTopic (§7.9, the QGen draft
// with HITL) — that delegate to chora-model-gateway. This package defines the
// ports those adapters implement + the pure arm-time gate logic.
//
// Owning domain : Content Sharing (chora_sharing DB) — bridge only.
// Owning team   : Team 1 (Content).
//
// Hard rules (per .claude/rules/ddd-enforcement.md):
//   - This file holds the domain only — NO HTTP, NO persistence.
//   - atom_id / revision_id are opaque UUID refs to chora_creation — NO FK.
//   - The QGen output is UNTRUSTED — re-validated against the entitled set
//     before use (FR-020). This package does NOT re-validate (it only defines
//     the port); the caller (the http adapter §7.9) does.
package livequiz

import (
	"context"
	"errors"
	"fmt"
)

// ErrInvalidArgument is returned for guard-clause failures.
var ErrInvalidArgument = errors.New("invalid argument")

// ErrQuizGeneratorNotConfigured is returned when QuizGenerator is nil at the
// call site — fail-loud on misconfiguration, never a fake draft (§7.9).
var ErrQuizGeneratorNotConfigured = errors.New("quiz generator not configured")

// GeneratedQuestion is a single QGen-produced question in a LiveQuiz draft
// (§7.9). The output is UNTRUSTED — the caller re-validates every atom_id
// against the instructor's entitled set (FR-020) before surfacing the draft.
//
// Cross-domain references (all opaque UUID, no FK):
//   - AtomID     → chora_creation.LearningAtom
//   - RevisionID → chora_creation.AtomRevision (append-only)
type GeneratedQuestion struct {
	AtomID          string   `json:"atom_id"`
	RevisionID      string   `json:"atom_revision_id"`
	Stem            string   `json:"stem"`
	Options         []string `json:"options"`
	CorrectOptionID string   `json:"correct_option_id"`
	TimerSeconds    int      `json:"timer_seconds"`
	Points          int      `json:"points"`
}

// Validate enforces the generated-question invariants: atom_id + revision_id
// required, stem non-empty, at least one option, correct_option_id
// references a listed option, timer + points > 0. Used by the adapter before
// surfacing the draft to HITL.
func (q GeneratedQuestion) Validate() error {
	if q.AtomID == "" {
		return fmt.Errorf("%w: atom_id required", ErrInvalidArgument)
	}
	if q.RevisionID == "" {
		return fmt.Errorf("%w: atom_revision_id required", ErrInvalidArgument)
	}
	if q.Stem == "" {
		return fmt.Errorf("%w: stem required", ErrInvalidArgument)
	}
	if len(q.Options) == 0 {
		return fmt.Errorf("%w: at least one option required", ErrInvalidArgument)
	}
	found := false
	for _, opt := range q.Options {
		if opt == q.CorrectOptionID {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("%w: correct_option_id %q not in options", ErrInvalidArgument, q.CorrectOptionID)
	}
	if q.TimerSeconds <= 0 {
		return fmt.Errorf("%w: timer_seconds must be > 0", ErrInvalidArgument)
	}
	if q.Points <= 0 {
		return fmt.Errorf("%w: points must be > 0", ErrInvalidArgument)
	}
	return nil
}

// QuizGenerator is the hexagonal port for the QGen draft (§7.9). Implemented
// by adapter/clients/qgen_quiz.go → chora-model-gateway (QGen crew). The
// output is UNTRUSTED — the caller re-validates against the entitled set
// (FR-020) + ALWAYS sets review_required="true" (HITL before ARM, FR-032).
//
// Contract:
//   - GenerateQuizQuestions produces `questionCount` questions for the given
//     topic + topic_tags. Returns ErrQuizGeneratorNotConfigured when nil
//     (fail-loud — never a fake draft).
type QuizGenerator interface {
	GenerateQuizQuestions(ctx context.Context, instructorGCID, tenantID, topic string, topicTags []string, questionCount int) ([]GeneratedQuestion, error)
}

// IsLiveQuizAtomUsable is the pure arm-time gate logic (§7.8 step 4). It
// decides whether an atom is usable for a LiveQuiz ARM without touching any
// infrastructure — the caller (the §7.8 adapter) supplies the three boolean
// inputs resolved from the projection + grant lookups.
//
// The disjunct mirrors the ADR-229 WS-2 picker (AuthorizeAtomUse) leg-for-leg:
//   - own-atom (isOwn=true) → usable (the author always reuses their own atom).
//   - tenant-visible (tenantVisible=true) → usable: the projection's
//     reuse_visibility='tenant' IS the author's audience consent (ADR-229 D2).
//     The projection only resolves when the atom is published + not archived,
//     so "tenant-visible" already implies "published" — no explicit grant is
//     needed (a v1 tenant reuse is free-license).
//   - granted (hasGrant=true) → usable: an active grant (live_quiz or
//     unlimited) confers permission + freezes the license snapshot (R-20).
//   - otherwise → not usable (the ARM is blocked loud, SC-009).
//
// This is a pure function — no side effects, no I/O. The inputs are the
// resolved booleans; the decision is deterministic.
func IsLiveQuizAtomUsable(isOwn bool, tenantVisible bool, hasGrant bool) bool {
	if isOwn {
		return true
	}
	return tenantVisible || hasGrant
}
