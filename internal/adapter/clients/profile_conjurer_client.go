// Package clients — Profile conjurer client over the GKE web-mode ADK
// agent (profile_conjurer crew, ADR-169).
//
// Replaces the deleted StaticTagExtractor (tags-only, deterministic) and
// the unused in-process TagExtractor (model-gateway Invoke, tags-only).
// The conjurer agent emits BOTH interest tags (from bio) AND proficiency
// (from completed course titles) in a single terminal JSON envelope.
//
// Fail-loud (per plan): LLM/agent errors propagate to the profiler
// handler which returns 500. There is no static fallback.
package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apollo-chora/chora-common/agentengine"
	"github.com/apollo-chora/chora-sharing/internal/domain/profiler"
)

// ProfileConjurerClient implements httpadapter.ConjurerPort over a GKE
// web-mode ADK agent. The agent's single sub-agent (conjurer) emits a
// terminal JSON envelope with tags + proficiency.
type ProfileConjurerClient struct {
	engine         agentengine.Client
	engineResource string
}

// NewProfileConjurerClient constructs a client bound to a GKE web-mode
// endpoint (http://host:port). The endpoint is cluster-local cleartext
// per ADR-169 — no ADC token.
func NewProfileConjurerClient(engine agentengine.Client, engineResource string) *ProfileConjurerClient {
	return &ProfileConjurerClient{engine: engine, engineResource: engineResource}
}

// conjurerEnvelope mirrors the agent's terminal JSON output.
//
//	{"tags": {"<category>": ["<tag>"]},
//	 "proficiency": {"per_category": {"<category>": "<level>"}}}
//
// The "overall" field was removed — "beginner in what?" was meaningless
// without category context. An empty per_category map is valid (a new
// user with no signal gets proficiency: {}).
type conjurerEnvelope struct {
	Tags        map[string][]string             `json:"tags"`
	Proficiency conjurerEnvelopeProficiency     `json:"proficiency"`
}

type conjurerEnvelopeProficiency struct {
	PerCategory map[string]string `json:"per_category,omitempty"`
}
// Conjure invokes the profile_conjurer agent and returns the conjured
// tags + per-category proficiency. Fail-loud: any agent/transport/parse
// error returns an error. An empty proficiency map is valid (new user,
// no signal) — the smith agent treats empty as "default to beginner".
func (c *ProfileConjurerClient) Conjure(
	ctx context.Context,
	tenantID, gcid, bio string,
	courseTitles []string,
) (*profiler.ConjuredProfile, error) {
	if c == nil || c.engine == nil {
		return nil, fmt.Errorf("profile conjurer: engine not configured")
	}
	if c.engineResource == "" {
		return nil, agentengine.ErrEngineNotConfigured
	}
	if tenantID == "" || gcid == "" {
		return nil, fmt.Errorf("%w: tenant_id + user_gcid required", agentengine.ErrInvalidRequest)
	}
	if strings.TrimSpace(bio) == "" && len(courseTitles) == 0 {
		return nil, profiler.ErrInvalidArgument
	}

	courses := courseTitles
	if courses == nil {
		courses = []string{}
	}
	coursesJSON, err := json.Marshal(courses)
	if err != nil {
		return nil, fmt.Errorf("profile conjurer: marshal course titles: %w", err)
	}

	state := map[string]any{
		"tenant_id":          tenantID,
		"user_gcid":          gcid,
		"mana_tier":          "standard",
		"bio":                bio,
		"course_titles_json": string(coursesJSON),
	}

	sessionID, err := c.engine.CreateSession(ctx, agentengine.CreateSessionRequest{
		EngineResource: c.engineResource,
		UserID:          gcid,
		State:           state,
	})
	if err != nil {
		return nil, fmt.Errorf("profile conjurer: create session: %w", err)
	}
	defer func() {
		_ = c.engine.DeleteSession(ctx, agentengine.DeleteSessionRequest{
			EngineResource: c.engineResource,
			UserID:         gcid,
			SessionID:      sessionID,
		})
	}()

	stream, err := c.engine.StreamQuery(ctx, agentengine.StreamQueryRequest{
		EngineResource: c.engineResource,
		UserID:         gcid,
		SessionID:      sessionID,
		Message:        "Conjure the learner's interest tags + proficiency per the instruction. Emit JSON only.",
	})
	if err != nil {
		return nil, fmt.Errorf("profile conjurer: stream query: %w", err)
	}

	// P1 single-agent crew: aggregate the conjurer author's terminal text.
	var finalText string
	for ev := range stream {
		if ev.Err != nil {
			return nil, fmt.Errorf("profile conjurer: stream: %w", ev.Err)
		}
		if ev.Author == "" {
			continue
		}
		if ev.Text != "" && !ev.Partial {
			finalText = ev.Text
		}
	}
	if finalText == "" {
		return nil, fmt.Errorf("%w: conjurer emitted no terminal text", agentengine.ErrStreamAborted)
	}

	var env conjurerEnvelope
	if err := parseJSONEnvelope(finalText, &env); err != nil {
		return nil, fmt.Errorf("%w: parse conjurer envelope: %v", agentengine.ErrStreamAborted, err)
	}

	// Build per-category proficiency. An empty map is valid (new user,
	// no signal). Unknown categories + invalid levels are dropped
	// (advisory — the agent may hallucinate). No overall field —
	// "beginner in what?" was meaningless without category context.
	prof := profiler.Proficiency{
		PerCategory: make(map[string]profiler.ProficiencyLevel, len(env.Proficiency.PerCategory)),
	}
	for cat, lvl := range env.Proficiency.PerCategory {
		// Drop unknown categories (not in taxonomy) + invalid levels.
		if _, ok := profiler.ValidCategories[profiler.TagCategory(cat)]; !ok {
			continue
		}
		if !profiler.ValidProficiencyLevel(lvl) {
			continue // invalid per-category levels dropped (advisory)
		}
		prof.PerCategory[cat] = profiler.ProficiencyLevel(lvl)
	}

	// Validate tags — keep only tags whose CATEGORY is in the taxonomy.
	// The tag string itself is free-form (e.g. "golang", "rust",
	// "system_design") — IsValidTag checks the category + non-empty tag,
	// NOT a closed vocabulary. This lets the agent emit interests the
	// taxonomy author didn't foresee, while still dropping off-category
	// noise (e.g. {"sports": ["basketball"]} → dropped, "sports" isn't a
	// valid category).
	tags := make([]profiler.InterestTag, 0, len(env.Tags))
	for catStr, tagList := range env.Tags {
		cat := profiler.TagCategory(catStr)
		if _, ok := profiler.ValidCategories[cat]; !ok {
			continue
		}
		for _, tag := range tagList {
			if profiler.IsValidTag(cat, tag) {
				tags = append(tags, profiler.InterestTag{Category: cat, Tag: tag})
			}
		}
	}

	return &profiler.ConjuredProfile{Tags: tags, Proficiency: prof}, nil
}
