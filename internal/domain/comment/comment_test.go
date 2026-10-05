package comment

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// --- stubCommentRepo: in-memory CommentRepo double ---

type stubCommentRepo struct {
	comments map[string]*Comment
	byPost   map[string][]string // postID -> []commentID (insertion order)
}

func newStubCommentRepo() *stubCommentRepo {
	return &stubCommentRepo{
		comments: make(map[string]*Comment),
		byPost:   make(map[string][]string),
	}
}

func (s *stubCommentRepo) Create(_ context.Context, c *Comment) error {
	if _, exists := s.comments[c.ID]; exists {
		return ErrConflict
	}
	cp := *c
	s.comments[c.ID] = &cp
	s.byPost[c.PostID] = append(s.byPost[c.PostID], c.ID)
	return nil
}

func (s *stubCommentRepo) ListByPost(_ context.Context, postID string, limit int, cursor string) ([]Comment, string, error) {
	if limit <= 0 {
		limit = 20
	}
	ids := s.byPost[postID]
	var result []Comment
	started := cursor == ""
	for _, id := range ids {
		if !started {
			if id == cursor {
				started = true
			}
			continue
		}
		result = append(result, *s.comments[id])
		if len(result) >= limit {
			break
		}
	}
	if len(result) == 0 {
		return nil, "", nil
	}
	return result, result[len(result)-1].ID, nil
}

// ErrConflict for the stub.
var ErrConflict = errors.New("comment conflict")

// --- NewComment tests ---

func TestNewComment_AcceptsTopLevelComment(t *testing.T) {
	c, err := NewComment("t", "post-1", "author-1", "body text", "")
	if err != nil {
		t.Fatalf("top-level comment: %v", err)
	}
	if c.IsReply() {
		t.Error("expected top-level comment (no parent)")
	}
	if c.ParentCommentID != "" {
		t.Errorf("expected empty parent, got %s", c.ParentCommentID)
	}
}

func TestNewComment_AcceptsReplyWithUUIDParent(t *testing.T) {
	parent := "00000000-0000-7000-8000-000000000001"
	c, err := NewComment("t", "post-1", "author-1", "reply body", parent)
	if err != nil {
		t.Fatalf("reply with UUID parent: %v", err)
	}
	if !c.IsReply() {
		t.Error("expected reply (has parent)")
	}
	if c.ParentCommentID != parent {
		t.Errorf("expected parent %s, got %s", parent, c.ParentCommentID)
	}
}

func TestNewComment_RejectsInvalidParentFormat(t *testing.T) {
	cases := []string{"not-a-uuid", "123", "gggggggg-0000-0000-0000-000000000000"}
	for _, parent := range cases {
		_, err := NewComment("t", "post-1", "author-1", "body", parent)
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("invalid parent %q: expected ErrInvalidArgument, got %v", parent, err)
		}
	}
}

func TestNewComment_RejectsMissingRequiredFields(t *testing.T) {
	cases := []struct {
		name   string
		tenant string
		post   string
		author string
	}{
		{"missing tenant", "", "p", "a"},
		{"missing post", "t", "", "a"},
		{"missing author", "t", "p", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewComment(tc.tenant, tc.post, tc.author, "body", "")
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("%s: expected ErrInvalidArgument, got %v", tc.name, err)
			}
		})
	}
}

func TestNewComment_RejectsEmptyBody(t *testing.T) {
	_, err := NewComment("t", "p", "a", "", "")
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("empty body: expected ErrInvalidArgument, got %v", err)
	}
	// Whitespace-only body is also empty.
	_, err = NewComment("t", "p", "a", "   \n\t  ", "")
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("whitespace body: expected ErrInvalidArgument, got %v", err)
	}
}

func TestNewComment_RejectsBodyTooLong(t *testing.T) {
	long := strings.Repeat("x", MaxBodyLen+1)
	_, err := NewComment("t", "p", "a", long, "")
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("long body: expected ErrInvalidArgument, got %v", err)
	}
}

func TestNewComment_TrimsBody(t *testing.T) {
	c, err := NewComment("t", "p", "a", "  hello  ", "")
	if err != nil {
		t.Fatalf("trim body: %v", err)
	}
	if c.Body != "hello" {
		t.Errorf("expected trimmed 'hello', got %q", c.Body)
	}
}

func TestNewComment_GeneratesID(t *testing.T) {
	c, _ := NewComment("t", "p", "a", "body", "")
	if c.ID == "" {
		t.Error("expected non-empty ID")
	}
	c2, _ := NewComment("t", "p", "a", "body", "")
	if c.ID == c2.ID {
		t.Error("expected unique IDs")
	}
}

func TestIsReply(t *testing.T) {
	top, _ := NewComment("t", "p", "a", "body", "")
	if top.IsReply() {
		t.Error("top-level should not be a reply")
	}
	reply, _ := NewComment("t", "p", "a", "body", "00000000-0000-7000-8000-000000000001")
	if !reply.IsReply() {
		t.Error("reply should be a reply")
	}
}

// --- CommentRepo port tests ---

func TestCommentRepo_CreateAndListByPost(t *testing.T) {
	repo := newStubCommentRepo()
	c1, _ := NewComment("t", "post-1", "a1", "first", "")
	c2, _ := NewComment("t", "post-1", "a2", "second", "")
	if err := repo.Create(context.Background(), c1); err != nil {
		t.Fatalf("Create c1: %v", err)
	}
	if err := repo.Create(context.Background(), c2); err != nil {
		t.Fatalf("Create c2: %v", err)
	}
	comments, _, err := repo.ListByPost(context.Background(), "post-1", 10, "")
	if err != nil {
		t.Fatalf("ListByPost: %v", err)
	}
	if len(comments) != 2 {
		t.Fatalf("expected 2 comments, got %d", len(comments))
	}
}

func TestCommentRepo_ListByPostPagination(t *testing.T) {
	repo := newStubCommentRepo()
	for i := 0; i < 5; i++ {
		c, _ := NewComment("t", "post-1", "a", "body", "")
		_ = repo.Create(context.Background(), c)
	}
	// Page 1: limit 2.
	page1, cursor1, err := repo.ListByPost(context.Background(), "post-1", 2, "")
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("expected 2 in page 1, got %d", len(page1))
	}
	// Page 2: cursor from page 1.
	page2, _, err := repo.ListByPost(context.Background(), "post-1", 2, cursor1)
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if len(page2) != 2 {
		t.Fatalf("expected 2 in page 2, got %d", len(page2))
	}
	// No overlap.
	if page1[0].ID == page2[0].ID || page1[1].ID == page2[0].ID {
		t.Error("pages should not overlap")
	}
}

func TestCommentRepo_ListByPostEmpty(t *testing.T) {
	repo := newStubCommentRepo()
	comments, _, err := repo.ListByPost(context.Background(), "no-posts", 10, "")
	if err != nil {
		t.Fatalf("ListByPost empty: %v", err)
	}
	if len(comments) != 0 {
		t.Fatalf("expected 0 comments, got %d", len(comments))
	}
}

func TestUUIDRegex_AcceptsValidUUIDs(t *testing.T) {
	valid := []string{
		"00000000-0000-7000-8000-000000000001",
		"abcdef12-3456-7890-abcd-ef1234567890",
		"ABCDEF12-3456-7890-ABCD-EF1234567890", // uppercase OK
	}
	for _, u := range valid {
		if !uuidRe.MatchString(u) {
			t.Errorf("expected %q to match UUID regex", u)
		}
	}
}

func TestUUIDRegex_RejectsInvalidUUIDs(t *testing.T) {
	invalid := []string{
		"not-a-uuid",
		"00000000-0000-0000-0000-00000000000",  // too short
		"gggggggg-0000-0000-0000-000000000000", // non-hex
	}
	for _, u := range invalid {
		if uuidRe.MatchString(u) {
			t.Errorf("expected %q to NOT match UUID regex", u)
		}
	}
}
