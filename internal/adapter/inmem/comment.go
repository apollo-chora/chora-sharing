package inmem

import (
	"context"
	"sort"
	"sync"

	"github.com/apollo-chora/chora-sharing/internal/domain/comment"
	"github.com/apollo-chora/chora-sharing/internal/keyset"
)

// CommentRepo is an in-memory adapter implementing comment.CommentRepo.
// Comments are keyed by id and indexed by post_id for keyset pagination.
type CommentRepo struct {
	mu     sync.RWMutex
	byID   map[string]*comment.Comment   // key = comment_id
	byPost map[string][]*comment.Comment // key = post_id
}

// NewCommentRepo returns an empty CommentRepo.
func NewCommentRepo() *CommentRepo {
	return &CommentRepo{
		byID:   make(map[string]*comment.Comment),
		byPost: make(map[string][]*comment.Comment),
	}
}

// Create inserts a comment. The adapter enforces the 1-level-reply
// constraint: a reply's parent must itself be a top-level comment.
func (r *CommentRepo) Create(_ context.Context, c *comment.Comment) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if c.ParentCommentID != "" {
		parent, ok := r.byID[c.ParentCommentID]
		if !ok {
			return comment.ErrNotFound
		}
		if parent.ParentCommentID != "" {
			return comment.ErrInvalidArgument // 1-level only
		}
		if parent.PostID != c.PostID {
			return comment.ErrInvalidArgument
		}
	}

	r.byID[c.ID] = c
	r.byPost[c.PostID] = append(r.byPost[c.PostID], c)
	return nil
}

// ListByPost is a keyset-paginated read, newest-first by created_at with id
// as the tie-break, mirroring the pg adapter. The cursor is the same opaque
// internal/keyset token; an empty or unparseable cursor starts at the first
// page. Returns the comments + the next cursor (empty when there are no more
// pages).
func (r *CommentRepo) ListByPost(_ context.Context, postID string, limit int, cursor string) ([]comment.Comment, string, error) {
	if limit <= 0 {
		limit = 50
	}

	r.mu.RLock()
	src := r.byPost[postID]
	snap := make([]comment.Comment, 0, len(src))
	for _, c := range src {
		snap = append(snap, *c)
	}
	r.mu.RUnlock()

	// Newest-first by created_at, id descending as the stable tie-break.
	sort.Slice(snap, func(i, j int) bool {
		return keyset.Less(snap[j].CreatedAt, snap[j].ID, snap[i].CreatedAt, snap[i].ID)
	})

	// Keyset: skip the rows at or newer than the cursor tuple.
	start := 0
	if curTS, curID := keyset.Decode(cursor); curID != "" {
		for start < len(snap) && !keyset.Less(snap[start].CreatedAt, snap[start].ID, curTS, curID) {
			start++
		}
	}
	if start >= len(snap) {
		return []comment.Comment{}, "", nil
	}
	end := start + limit
	if end > len(snap) {
		end = len(snap)
	}
	page := snap[start:end]
	next := ""
	if end < len(snap) {
		last := page[len(page)-1]
		next = keyset.Encode(last.CreatedAt, last.ID)
	}
	return page, next, nil
}

// Update updates the body of a comment. Only the author can update.
func (r *CommentRepo) Update(_ context.Context, commentID, authorGCID, body string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.byID[commentID]
	if !ok || c.AuthorGCID != authorGCID {
		return comment.ErrNotFound
	}
	c.Body = body
	return nil
}

// Delete deletes a comment. Only the author can delete.
func (r *CommentRepo) Delete(_ context.Context, commentID, authorGCID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.byID[commentID]
	if !ok || c.AuthorGCID != authorGCID {
		return comment.ErrNotFound
	}
	delete(r.byID, commentID)
	post := r.byPost[c.PostID]
	for i, pc := range post {
		if pc.ID == commentID {
			r.byPost[c.PostID] = append(post[:i], post[i+1:]...)
			break
		}
	}
	return nil
}
