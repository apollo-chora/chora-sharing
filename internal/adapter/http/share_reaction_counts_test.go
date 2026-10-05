package httpadapter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
	"github.com/apollo-chora/chora-sharing/internal/domain/reaction"
)

func TestListSharedAtoms_ReactionCountsPerType(t *testing.T) {
	t.Parallel()

	shares := inmem.NewShareRepo()
	graph := inmem.NewSocialGraph()
	posts := inmem.NewPostRepo()
	tenant := "00000000-0000-7000-8000-000000000001"

	// Seed a published share directly.
	share := &atom_share.Share{
		FeedEntryID:  "019700aa-0000-7000-8000-000000000001",
		OwnerGCID:    "gcid-phyllis",
		AtomID:       "atom-001",
		RevisionID:   "rev-001",
		StemPreview:  "Bayesian inference",
		QuestionType: "multiple_choice",
		License:      atom_share.LicenseFree,
		TenantID:     tenant,
	}
	_ = shares.SaveShare(context.Background(), share)

	// Verify the share is visible via the repo directly.
	direct, _, _ := shares.ListSharedAtoms(context.Background(), tenant, "", 10, "", "", "tenant", nil, nil)
	if len(direct) != 1 {
		t.Fatalf("expected 1 share in repo, got %d", len(direct))
	}

	// Add reactions of different types.
	reg := reaction.NewRegistry()
	_, _, _ = reg.React(context.Background(), tenant, "gcid-a", share.FeedEntryID, reaction.TypeLike)
	_, _, _ = reg.React(context.Background(), tenant, "gcid-b", share.FeedEntryID, reaction.TypeLike)
	_, _, _ = reg.React(context.Background(), tenant, "gcid-c", share.FeedEntryID, reaction.TypeCurious)

	h := NewHandler(Deps{
		Shares:    shares,
		Reactions: reg,
		Graph:     graph,
		Posts:     posts,
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/feed/shared-atoms?scope=tenant", nil)
	req.Header.Set("gcid", "gcid-phyllis")
	req.Header.Set("X-Tenant-Id", tenant)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	body := w.Body.String()
	t.Logf("HTTP %d — response: %s", w.Code, body)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !strings.Contains(body, `"reaction_counts"`) {
		t.Fatal("expected reaction_counts field in response")
	}
	if !strings.Contains(body, `"like":2`) {
		t.Fatal("expected like:2 in reaction_counts")
	}
	if !strings.Contains(body, `"curious":1`) {
		t.Fatal("expected curious:1 in reaction_counts")
	}
	if strings.Contains(body, `"insightful"`) {
		t.Fatal("did not expect insightful in reaction_counts (0-count types should be omitted)")
	}
}

func TestReactToPost_ToggleAndReturnsCounts(t *testing.T) {
	t.Parallel()

	shares := inmem.NewShareRepo()
	graph := inmem.NewSocialGraph()
	posts := inmem.NewPostRepo()
	tenant := "00000000-0000-7000-8000-000000000001"

	share := &atom_share.Share{
		FeedEntryID:  "019700aa-0000-7000-8000-000000000002",
		OwnerGCID:    "gcid-author",
		AtomID:       "atom-002",
		RevisionID:   "rev-002",
		StemPreview:  "Graph theory",
		QuestionType: "short_answer",
		License:      atom_share.LicenseFree,
		TenantID:     tenant,
	}
	_ = shares.SaveShare(context.Background(), share)

	reg := reaction.NewRegistry()
	h := NewHandler(Deps{
		Shares:    shares,
		Reactions: reg,
		Graph:     graph,
		Posts:     posts,
	})

	// 1. POST a "like" reaction — should add it and return counts.
	req := httptest.NewRequest(http.MethodPost, "/v1/posts/"+share.FeedEntryID+"/reactions", strings.NewReader(`{"kind":"like"}`))
	req.Header.Set("gcid", "gcid-reactor")
	req.Header.Set("X-Tenant-Id", tenant)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	// POST /reactions is a TOGGLE, and it reports which half it performed:
	//   reacted    -> 201 Created + Location (a reaction resource now exists)
	//   toggled off -> 200 OK      (nothing was created; resp.Reaction is null)
	// This assertion previously expected a flat 200 and had been failing since
	// the handler was rewritten — the duplicate-route panic in NewHandler
	// (handlers.go:233/:236) meant this package never ran, so nobody saw it.
	// The handler is right; the expectation was stale.
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 on react (a reaction was created), got %d: %s", w.Code, w.Body.String())
	}
	if loc := w.Header().Get("Location"); loc == "" {
		t.Fatal("201 on react must carry a Location header pointing at the new reaction")
	}
	body := w.Body.String()
	if !strings.Contains(body, `"like":1`) {
		t.Fatalf("expected like:1 in reaction_counts, got: %s", body)
	}
	if !strings.Contains(body, `"kind":"like"`) {
		t.Fatalf("expected active reaction kind=like, got: %s", body)
	}

	// 2. POST "like" again — should toggle OFF (reaction=null, like:0).
	req2 := httptest.NewRequest(http.MethodPost, "/v1/posts/"+share.FeedEntryID+"/reactions", strings.NewReader(`{"kind":"like"}`))
	req2.Header.Set("gcid", "gcid-reactor")
	req2.Header.Set("X-Tenant-Id", tenant)
	req2.Header.Set("Content-Type", "application/json")
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, req2)

	body2 := w2.Body.String()
	if !strings.Contains(body2, `"reaction":null`) {
		t.Fatalf("expected reaction:null on toggle-off, got: %s", body2)
	}
	if !strings.Contains(body2, `"reaction_counts":{}`) {
		t.Fatalf("expected empty reaction_counts after toggle-off, got: %s", body2)
	}

	// 3. POST "curious" — should add it.
	req3 := httptest.NewRequest(http.MethodPost, "/v1/posts/"+share.FeedEntryID+"/reactions", strings.NewReader(`{"kind":"curious"}`))
	req3.Header.Set("gcid", "gcid-reactor")
	req3.Header.Set("X-Tenant-Id", tenant)
	req3.Header.Set("Content-Type", "application/json")
	w3 := httptest.NewRecorder()
	h.ServeHTTP(w3, req3)

	body3 := w3.Body.String()
	if !strings.Contains(body3, `"curious":1`) {
		t.Fatalf("expected curious:1 after switch, got: %s", body3)
	}
	if strings.Contains(body3, `"like":1`) {
		t.Fatalf("expected like:0 after switching to curious, got: %s", body3)
	}
}
