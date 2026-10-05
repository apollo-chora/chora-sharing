// Package clients — Duel atom smith engine client over the GKE web-mode
// ADK agent (duel_atom_smith crew, ADR-169).
//
// The engine client is the thin transport layer: it creates a session
// with the candidate pool + shared tags + proficiencies + profiles,
// streams the agent's terminal JSON envelope, and returns it raw. The
// DuelAtomSmithSelector (duel_atom_smith_selector.go) wraps this to
// validate picks + generated atoms and build duel.AtomPick values.
//
// Fail-loud (per plan): agent/transport errors propagate to processMatch
// which restores both searchers to the pool.
package clients

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apollo-chora/chora-common/agentengine"
)

// DuelAtomSmithRequest is the input to the smith agent. Candidates is the
// shared-atom pool (filtered to MCQ + valid options); SharedTags +
// Proficiencies + Profiles personalize the pick + generation.
type DuelAtomSmithRequest struct {
	TenantID      string
	UserGCID      string
	Candidates    []DuelAtomCandidate
	SharedTags    []string
	Proficiencies []int
	Profiles      map[string]string // gcid → "overall (+per-category)" summary
	Count         int
}

// DuelAtomCandidate is one row of the shared-atom pool passed to the agent.
type DuelAtomCandidate struct {
	Index   int      `json:"index"`
	Question string  `json:"question"`
	Options []string `json:"options"`
}

// DuelAtomSmithResponse is the raw parsed envelope from the agent. The
// selector validates picks (in-range, deduped) + generated atoms
// (4 options, correct_answer matches an option) before building AtomPicks.
type DuelAtomSmithResponse struct {
	Picks     []int                  `json:"picks"`
	Generated []DuelAtomSmithGenerated `json:"generated"`
}

// DuelAtomSmithGenerated is one agent-generated MCQ atom.
type DuelAtomSmithGenerated struct {
	Question      string   `json:"question"`
	Options       []string `json:"options"`
	CorrectAnswer string   `json:"correct_answer"`
}

// DuelAtomSmithEngine is the test seam over the agent call. The
// production implementation is DuelAtomSmithEngineClient below; tests
// substitute a fake to drive validation logic without an agent.
type DuelAtomSmithEngine interface {
	ConjureDuelAtoms(ctx context.Context, req DuelAtomSmithRequest) (DuelAtomSmithResponse, error)
}

// DuelAtomSmithEngineClient is the production implementation over a GKE
// web-mode ADK agent.
type DuelAtomSmithEngineClient struct {
	engine         agentengine.Client
	engineResource string
}

// NewDuelAtomSmithEngineClient constructs a client bound to a GKE web-mode
// endpoint (http://host:port). Cluster-local cleartext per ADR-169.
func NewDuelAtomSmithEngineClient(engine agentengine.Client, engineResource string) *DuelAtomSmithEngineClient {
	return &DuelAtomSmithEngineClient{engine: engine, engineResource: engineResource}
}

// smithEnvelope mirrors the agent's terminal JSON output.
//
//	{"picks": [<indices>],
//	 "generated": [{"question","options","correct_answer"}]}
type smithEnvelope struct {
	Picks     []int                  `json:"picks"`
	Generated []DuelAtomSmithGenerated `json:"generated"`
}

// ConjureDuelAtoms invokes the duel_atom_smith agent and returns the raw
// parsed envelope. Fail-loud: agent/transport/parse errors propagate.
func (c *DuelAtomSmithEngineClient) ConjureDuelAtoms(
	ctx context.Context,
	req DuelAtomSmithRequest,
) (DuelAtomSmithResponse, error) {
	if c == nil || c.engine == nil {
		return DuelAtomSmithResponse{}, fmt.Errorf("duel atom smith: engine not configured")
	}
	if c.engineResource == "" {
		return DuelAtomSmithResponse{}, agentengine.ErrEngineNotConfigured
	}
	if req.TenantID == "" || req.UserGCID == "" {
		return DuelAtomSmithResponse{}, fmt.Errorf("%w: tenant_id + user_gcid required", agentengine.ErrInvalidRequest)
	}

	candidatesJSON, err := json.Marshal(req.Candidates)
	if err != nil {
		return DuelAtomSmithResponse{}, fmt.Errorf("duel atom smith: marshal candidates: %w", err)
	}
	tagsJSON, err := json.Marshal(req.SharedTags)
	if err != nil {
		return DuelAtomSmithResponse{}, fmt.Errorf("duel atom smith: marshal shared tags: %w", err)
	}
	profJSON, err := json.Marshal(req.Proficiencies)
	if err != nil {
		return DuelAtomSmithResponse{}, fmt.Errorf("duel atom smith: marshal proficiencies: %w", err)
	}
	profilesJSON, err := json.Marshal(req.Profiles)
	if err != nil {
		return DuelAtomSmithResponse{}, fmt.Errorf("duel atom smith: marshal profiles: %w", err)
	}

	state := map[string]any{
		"tenant_id":          req.TenantID,
		"user_gcid":          req.UserGCID,
		"mana_tier":          "standard",
		"candidates_json":    string(candidatesJSON),
		"shared_tags_json":   string(tagsJSON),
		"proficiencies_json": string(profJSON),
		"profiles_json":      string(profilesJSON),
		"count":              req.Count,
	}

	sessionID, err := c.engine.CreateSession(ctx, agentengine.CreateSessionRequest{
		EngineResource: c.engineResource,
		UserID:         req.UserGCID,
		State:          state,
	})
	if err != nil {
		return DuelAtomSmithResponse{}, fmt.Errorf("duel atom smith: create session: %w", err)
	}
	defer func() {
		_ = c.engine.DeleteSession(ctx, agentengine.DeleteSessionRequest{
			EngineResource: c.engineResource,
			UserID:         req.UserGCID,
			SessionID:      sessionID,
		})
	}()

	stream, err := c.engine.StreamQuery(ctx, agentengine.StreamQueryRequest{
		EngineResource: c.engineResource,
		UserID:         req.UserGCID,
		SessionID:      sessionID,
		Message:        "Select + generate duel atoms per the instruction. Emit JSON only.",
	})
	if err != nil {
		return DuelAtomSmithResponse{}, fmt.Errorf("duel atom smith: stream query: %w", err)
	}

	var finalText string
	for ev := range stream {
		if ev.Err != nil {
			return DuelAtomSmithResponse{}, fmt.Errorf("duel atom smith: stream: %w", ev.Err)
		}
		if ev.Author == "" {
			continue
		}
		if ev.Text != "" && !ev.Partial {
			finalText = ev.Text
		}
	}
	if finalText == "" {
		return DuelAtomSmithResponse{}, fmt.Errorf("%w: smith emitted no terminal text", agentengine.ErrStreamAborted)
	}

	var env smithEnvelope
	if err := parseJSONEnvelope(finalText, &env); err != nil {
		return DuelAtomSmithResponse{}, fmt.Errorf("%w: parse smith envelope: %v", agentengine.ErrStreamAborted, err)
	}
	return DuelAtomSmithResponse{Picks: env.Picks, Generated: env.Generated}, nil
}
