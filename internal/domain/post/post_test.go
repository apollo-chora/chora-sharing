// Package post_test holds RED-phase TDD specs for the Post aggregate.
package post_test

import (
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/domain/post"
)

const (
	tenantA = "01970000-0000-7000-8000-000000000001"
	gcidA   = "01970000-0000-7000-9000-000000000001"
	gcidB   = "01970000-0000-7000-9000-000000000002"
	atomA   = "01970000-0000-7000-a000-000000000001"
)

func TestPost_New_AssignsUUIDv7AndDefaults(t *testing.T) {
	t.Parallel()

	p, err := post.NewPost(tenantA, gcidA, "hello chora", atomA, []string{"intro", "test"})
	if err != nil {
		t.Fatalf("NewPost: unexpected error: %v", err)
	}
	if p.ID == "" {
		t.Fatalf("expected non-empty ID")
	}
	if p.TenantID != tenantA {
		t.Fatalf("tenant_id mismatch: got %q want %q", p.TenantID, tenantA)
	}
	if p.AuthorGCID != gcidA {
		t.Fatalf("author_gcid mismatch")
	}
	if p.Body != "hello chora" {
		t.Fatalf("body mismatch")
	}
	if p.AtomID != atomA {
		t.Fatalf("atom_id mismatch")
	}
	if len(p.Tags) != 2 {
		t.Fatalf("expected 2 tags, got %d", len(p.Tags))
	}
	if p.PostedAt.IsZero() {
		t.Fatalf("expected posted_at set")
	}
	if p.DeletedAt != nil {
		t.Fatalf("expected DeletedAt nil on fresh post")
	}
}

func TestPost_New_RejectsEmptyTenant(t *testing.T) {
	t.Parallel()
	if _, err := post.NewPost("", gcidA, "x", atomA, nil); err == nil {
		t.Fatalf("expected error for empty tenant_id")
	}
}

func TestPost_New_RejectsEmptyAuthor(t *testing.T) {
	t.Parallel()
	if _, err := post.NewPost(tenantA, "", "x", atomA, nil); err == nil {
		t.Fatalf("expected error for empty author_gcid")
	}
}

func TestPost_New_RejectsEmptyBody(t *testing.T) {
	t.Parallel()
	if _, err := post.NewPost(tenantA, gcidA, "   ", atomA, nil); err == nil {
		t.Fatalf("expected error for whitespace-only body")
	}
}

func TestPost_New_RejectsTooLongBody(t *testing.T) {
	t.Parallel()
	body := strings.Repeat("a", post.MaxBodyLen+1)
	if _, err := post.NewPost(tenantA, gcidA, body, atomA, nil); err == nil {
		t.Fatalf("expected error for body exceeding max length")
	}
}

func TestPost_New_AllowsEmptyAtomID(t *testing.T) {
	t.Parallel()
	// atom_id is optional — Post can be a free-form social post.
	p, err := post.NewPost(tenantA, gcidA, "untethered thought", "", nil)
	if err != nil {
		t.Fatalf("expected ok with empty atom_id, got: %v", err)
	}
	if p.AtomID != "" {
		t.Fatalf("expected empty atom_id passthrough")
	}
}

func TestPost_SoftDelete_IsIdempotent(t *testing.T) {
	t.Parallel()
	p, _ := post.NewPost(tenantA, gcidA, "x", atomA, nil)
	if p.DeletedAt != nil {
		t.Fatalf("expected fresh post not soft-deleted")
	}
	p.SoftDelete()
	if p.DeletedAt == nil {
		t.Fatalf("expected DeletedAt after first SoftDelete")
	}
	first := *p.DeletedAt
	time.Sleep(2 * time.Millisecond)
	p.SoftDelete()
	if !p.DeletedAt.Equal(first) {
		t.Fatalf("SoftDelete should be idempotent")
	}
}

func TestPost_TagsAreCopied(t *testing.T) {
	t.Parallel()
	tags := []string{"a", "b"}
	p, _ := post.NewPost(tenantA, gcidA, "x", atomA, tags)
	tags[0] = "mutated"
	if p.Tags[0] == "mutated" {
		t.Fatalf("expected tags to be copied, not aliased")
	}
}
