// Package post_test holds the remaining coverage specs for the Post
// aggregate (visibility default branch).
package post_test

import (
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/post"
)

// A visibility value that is not one of public|tenant|private must be refused
// by VisibleTo regardless of who asks — the switch's default branch.
func TestPost_VisibleTo_UnknownVisibilityNotVisible(t *testing.T) {
	t.Parallel()
	p := &post.Post{
		TenantID:   tenantA,
		AuthorGCID: gcidA,
		Visibility: post.Visibility("secret"),
	}
	if p.VisibleTo(tenantA, gcidA) {
		t.Fatal("an unrecognised visibility must not be visible even to the author")
	}
}