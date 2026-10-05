// Package post_test holds RED-phase TDD specs for Post visibility scoping
// (Phase 60.x extension — atomic-post propagation per Comic Ch5 P11).
package post_test

import (
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/post"
)

// Visibility defaults to PUBLIC when caller passes empty string.
func TestPost_Visibility_DefaultsToPublic(t *testing.T) {
	t.Parallel()
	p, err := post.NewPostWithVisibility(tenantA, gcidA, "x", atomA, nil, "")
	if err != nil {
		t.Fatalf("NewPostWithVisibility: %v", err)
	}
	if p.Visibility != post.VisibilityPublic {
		t.Fatalf("expected default VisibilityPublic, got %q", p.Visibility)
	}
}

func TestPost_Visibility_AcceptsAllValidValues(t *testing.T) {
	t.Parallel()
	cases := []post.Visibility{
		post.VisibilityPublic,
		post.VisibilityTenant,
		post.VisibilityPrivate,
	}
	for _, v := range cases {
		v := v
		t.Run(string(v), func(t *testing.T) {
			t.Parallel()
			p, err := post.NewPostWithVisibility(tenantA, gcidA, "x", atomA, nil, v)
			if err != nil {
				t.Fatalf("NewPostWithVisibility(%q): %v", v, err)
			}
			if p.Visibility != v {
				t.Fatalf("expected %q, got %q", v, p.Visibility)
			}
		})
	}
}

func TestPost_Visibility_RejectsUnknown(t *testing.T) {
	t.Parallel()
	if _, err := post.NewPostWithVisibility(tenantA, gcidA, "x", atomA, nil, "secret"); err == nil {
		t.Fatalf("expected error for unknown visibility")
	}
}

func TestPost_VisibleTo_PublicAlwaysVisible(t *testing.T) {
	t.Parallel()
	p, _ := post.NewPostWithVisibility(tenantA, gcidA, "x", atomA, nil, post.VisibilityPublic)
	if !p.VisibleTo(tenantA, gcidB) {
		t.Fatalf("public post must be visible to anyone in tenant")
	}
	if !p.VisibleTo("other-tenant", gcidB) {
		t.Fatalf("public post must be visible across tenants")
	}
}

func TestPost_VisibleTo_TenantOnlySameTenant(t *testing.T) {
	t.Parallel()
	p, _ := post.NewPostWithVisibility(tenantA, gcidA, "x", atomA, nil, post.VisibilityTenant)
	if !p.VisibleTo(tenantA, gcidB) {
		t.Fatalf("tenant post must be visible within tenant")
	}
	if p.VisibleTo("other-tenant", gcidB) {
		t.Fatalf("tenant post must NOT be visible across tenants")
	}
}

func TestPost_VisibleTo_PrivateAuthorOnly(t *testing.T) {
	t.Parallel()
	p, _ := post.NewPostWithVisibility(tenantA, gcidA, "x", atomA, nil, post.VisibilityPrivate)
	if !p.VisibleTo(tenantA, gcidA) {
		t.Fatalf("private post must be visible to author")
	}
	if p.VisibleTo(tenantA, gcidB) {
		t.Fatalf("private post must NOT be visible to non-author")
	}
}
