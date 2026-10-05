// TemplateResolver loads `chora-contracts/yaml/agent-guardrail-mapping.yaml`
// and resolves agent_id → full Cloud Model Armor template resource name.
//
// Per ADR-152 §"YAML config migration" the mapping table lives in
// chora-contracts and the LLM-issuing service constructs the resource
// name at runtime by combining `template_tier` with env-derived
// project + location + environment:
//
//	projects/{project}/locations/{location}/templates/chora-guardrail-{tier}-{env}
//
// All inputs come from env vars per `feedback_no_inline_config` and the
// `secrets-and-env` skill — no defaults are baked into the loader.
//
// This resolver mirrors services/chora-agent-executor/internal/adapter/modelarmor/resolver.go
// to keep template-tier resolution identical across the two LLM-issuing
// services that screen via Cloud Model Armor.

package modelarmor

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// mappingFile mirrors the on-disk schema of agent-guardrail-mapping.yaml.
type mappingFile struct {
	Version int                   `yaml:"version"`
	Schema  string                `yaml:"schema"`
	Agents  map[string]agentEntry `yaml:"agents"`
	Default agentEntry            `yaml:"default"`
}

// agentEntry mirrors a single agents.<agent_id> row.
type agentEntry struct {
	TemplateTier string `yaml:"template_tier"`
	Rationale    string `yaml:"rationale"`
}

// allowedTiers enumerates the canonical tier strings the YAML may carry.
// Mismatches surface at load time per the fail-loud posture.
var allowedTiers = map[string]struct{}{
	"strict":     {},
	"balanced":   {},
	"permissive": {},
}

// TemplateResolver maps agent_id → full Cloud Model Armor template
// resource name. Project + location + environment come from env vars at
// construction time; the YAML payload is loaded once at boot.
type TemplateResolver struct {
	project     string
	location    string
	environment string
	agents      map[string]string // agent_id → tier
	defaultTier string
}

// PermissiveTemplates returns the full template resource names that map
// to the `permissive` tier — the caller registers these as INSPECT_ONLY
// on the Cloud Model Armor Client so MATCH_FOUND verdicts on those
// templates become VerdictInspectOnly (audit) rather than VerdictBlock
// (default).
//
// strict + balanced map to INSPECT_AND_BLOCK (the Client default) — no
// registration call required.
func (r *TemplateResolver) PermissiveTemplates() []string {
	out := []string{}
	seen := map[string]struct{}{}
	add := func(name string) {
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	for agent, tier := range r.agents {
		if tier == "permissive" {
			name, err := r.Resolve(agent)
			if err == nil {
				add(name)
			}
		}
	}
	if r.defaultTier == "permissive" {
		add(r.templateName("permissive"))
	}
	return out
}

// LoadResolverFromFile loads the mapping file and returns a resolver
// wired with the supplied project / location / environment.
func LoadResolverFromFile(path, project, location, environment string) (*TemplateResolver, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("modelarmor resolver: read %s: %w", path, err)
	}
	return LoadResolverFromBytes(data, project, location, environment)
}

// LoadResolverFromBytes parses raw YAML bytes + project/location/env and
// returns a validated resolver.
func LoadResolverFromBytes(data []byte, project, location, environment string) (*TemplateResolver, error) {
	if strings.TrimSpace(project) == "" {
		return nil, errors.New("modelarmor resolver: project required")
	}
	if strings.TrimSpace(location) == "" {
		return nil, errors.New("modelarmor resolver: location required")
	}
	if strings.TrimSpace(environment) == "" {
		return nil, errors.New("modelarmor resolver: environment required")
	}

	var mf mappingFile
	if err := yaml.Unmarshal(data, &mf); err != nil {
		return nil, fmt.Errorf("modelarmor resolver: yaml parse: %w", err)
	}

	agents := make(map[string]string, len(mf.Agents))
	for id, entry := range mf.Agents {
		tier := strings.TrimSpace(entry.TemplateTier)
		if _, ok := allowedTiers[tier]; !ok {
			return nil, fmt.Errorf("modelarmor resolver: agent %q has unknown tier %q (allowed: strict / balanced / permissive)", id, tier)
		}
		agents[id] = tier
	}

	defaultTier := strings.TrimSpace(mf.Default.TemplateTier)
	if defaultTier == "" {
		// No default declared — fall back to strict (safe fail-loud).
		defaultTier = "strict"
	} else if _, ok := allowedTiers[defaultTier]; !ok {
		return nil, fmt.Errorf("modelarmor resolver: default tier %q unknown", defaultTier)
	}

	return &TemplateResolver{
		project:     project,
		location:    location,
		environment: environment,
		agents:      agents,
		defaultTier: defaultTier,
	}, nil
}

// Resolve returns the full Cloud Model Armor template resource name for
// agent_id. Unknown agents fall back to the file's default tier
// (typically `strict`) per the file's own fail-loud guidance.
func (r *TemplateResolver) Resolve(agentID string) (string, error) {
	if r == nil {
		return "", errors.New("modelarmor resolver: nil receiver")
	}
	if strings.TrimSpace(agentID) == "" {
		return "", errors.New("modelarmor resolver: agent_id required")
	}
	tier, ok := r.agents[agentID]
	if !ok {
		tier = r.defaultTier
	}
	return r.templateName(tier), nil
}

// templateName composes the full Cloud Model Armor template resource
// name from the configured project / location / environment + the
// resolved tier.
func (r *TemplateResolver) templateName(tier string) string {
	return fmt.Sprintf("projects/%s/locations/%s/templates/chora-guardrail-%s-%s",
		r.project, r.location, tier, r.environment)
}
