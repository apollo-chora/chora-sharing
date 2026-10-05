// social_rpc_test.go — statement coverage for the social surface RPCs:
// Follow, Unfollow, CreatePost, ReactToPost, GetLeaderboard (the inmem-backed
// feed endpoints). Hand-written fakes per the package convention (no mocks).
package grpcadapter

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sharingv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/sharing/v1"

	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
	"github.com/apollo-chora/chora-sharing/internal/domain/post"
	"github.com/apollo-chora/chora-sharing/internal/domain/reaction"
	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

// -----------------------------------------------------------------------------
// Fakes
// -----------------------------------------------------------------------------

// fakeGraph is a configurable SocialGraph double.
type fakeGraph struct {
	followEdge   *social.Edge
	followErr    error
	unfollowErr  error
	unfollowed   bool
	gotTenant    string
	gotFollower  string
	gotFollowee  string
}

func (f *fakeGraph) Follow(ctx context.Context, tenantID, follower, followee string) (*social.Edge, bool, error) {
	f.gotTenant, f.gotFollower, f.gotFollowee = tenantID, follower, followee
	if f.followErr != nil {
		return nil, false, f.followErr
	}
	if f.followEdge != nil {
		return f.followEdge, true, nil
	}
	return &social.Edge{
		TenantID:     tenantID,
		FollowerGCID: follower,
		FolloweeGCID: followee,
		CreatedAt:    time.Now().UTC(),
	}, true, nil
}

func (f *fakeGraph) Unfollow(ctx context.Context, tenantID, follower, followee string) (bool, error) {
	f.gotTenant, f.gotFollower, f.gotFollowee = tenantID, follower, followee
	if f.unfollowErr != nil {
		return false, f.unfollowErr
	}
	f.unfollowed = true
	return true, nil
}

// fakePostRepo is an in-memory post.PostRepo. Save stamps a stable ID when the
// domain factory left it empty so the proto mapping is assertable.
type fakePostRepo struct {
	posts   map[string]*post.Post
	getErr  error
	saveErr error
}

func newFakePostRepo() *fakePostRepo {
	return &fakePostRepo{posts: make(map[string]*post.Post)}
}

func (f *fakePostRepo) Save(ctx context.Context, p *post.Post) error {
	if f.saveErr != nil {
		return f.saveErr
	}
	if p.ID == "" {
		p.ID = "post-1"
	}
	f.posts[p.ID] = p
	return nil
}

func (f *fakePostRepo) Get(ctx context.Context, id string) (*post.Post, bool, error) {
	if f.getErr != nil {
		return nil, false, f.getErr
	}
	p, ok := f.posts[id]
	return p, ok, nil
}

func (f *fakePostRepo) ListByTenant(ctx context.Context, tenantID string, offset, limit int) ([]*post.Post, int, error) {
	return nil, 0, nil
}

// fakeReactionRepo is an in-memory reaction.ReactionRepo with failure knobs.
type fakeReactionRepo struct {
	reactErr  error
	listErr   error
	reacted   *reaction.Reaction
	reactions []*reaction.Reaction
}

func (f *fakeReactionRepo) React(ctx context.Context, tenantID, gcid, postID string, t reaction.Type) (*reaction.Reaction, bool, error) {
	if f.reactErr != nil {
		return nil, false, f.reactErr
	}
	f.reacted = &reaction.Reaction{
		ID:        "rx-1",
		TenantID:  tenantID,
		GCID:      gcid,
		PostID:    postID,
		Type:      t,
		CreatedAt: time.Now().UTC(),
	}
	return f.reacted, true, nil
}

func (f *fakeReactionRepo) Unreact(ctx context.Context, gcid, postID string, t reaction.Type) (bool, error) {
	return false, nil
}

func (f *fakeReactionRepo) UnreactByID(ctx context.Context, reactionID, gcid string) (bool, error) {
	return false, nil
}

func (f *fakeReactionRepo) ListByPost(ctx context.Context, postID string) ([]*reaction.Reaction, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.reactions, nil
}

// fakeLeaderboardReader is a configurable leaderboard.LeaderboardReader.
type fakeLeaderboardReader struct {
	entries    []leaderboard.Entry
	err        error
	gotKind    leaderboard.ScopeKind
	gotScopeID string
	gotPeriod  leaderboard.Period
	gotLimit   int
}

func (f *fakeLeaderboardReader) ReadTop(ctx context.Context, scopeKind leaderboard.ScopeKind, scopeID, tenantID string, period leaderboard.Period, limit int) ([]leaderboard.Entry, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.gotKind, f.gotScopeID, f.gotPeriod, f.gotLimit = scopeKind, scopeID, period, limit
	return f.entries, nil
}

// -----------------------------------------------------------------------------
// Follow
// -----------------------------------------------------------------------------

func TestFollow_NotWired_Unimplemented(t *testing.T) {
	t.Parallel()
	_, err := New(Deps{}).Follow(context.Background(), &sharingv1.FollowRequest{
		FollowerGcid: "a", FolloweeGcid: "b", TenantId: "t",
	})
	statusIs(t, err, codes.Unimplemented)
}

func TestFollow_InvalidInput(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Graph: &fakeGraph{}})
	tests := map[string]*sharingv1.FollowRequest{
		"nil request":  nil,
		"all missing":  {},
		"no follower":  {FolloweeGcid: "b", TenantId: "t"},
		"no followee":  {FollowerGcid: "a", TenantId: "t"},
		"no tenant":    {FollowerGcid: "a", FolloweeGcid: "b"},
		"whitespace":   {FollowerGcid: "  ", FolloweeGcid: "b", TenantId: "t"},
	}
	for name, req := range tests {
		if _, err := srv.Follow(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: expected InvalidArgument, got %v", name, err)
		}
	}
}

func TestFollow_HappyPath(t *testing.T) {
	t.Parallel()
	graph := &fakeGraph{}
	srv := New(Deps{Graph: graph})
	resp, err := srv.Follow(context.Background(), &sharingv1.FollowRequest{
		FollowerGcid: "a", FolloweeGcid: "b", TenantId: "t",
	})
	if err != nil {
		t.Fatalf("Follow: %v", err)
	}
	f := resp.GetFollow()
	if f == nil {
		t.Fatal("response Follow nil")
	}
	if f.GetFollowerGcid() != "a" || f.GetFolloweeGcid() != "b" || f.GetTenantId() != "t" {
		t.Errorf("edge mismatch: %+v", f)
	}
	if f.GetFollowedAt() == nil || !f.GetFollowedAt().IsValid() {
		t.Error("followed_at missing")
	}
	if graph.gotTenant != "t" || graph.gotFollower != "a" || graph.gotFollowee != "b" {
		t.Errorf("graph got (%q,%q,%q), want (t,a,b)", graph.gotTenant, graph.gotFollower, graph.gotFollowee)
	}
}

func TestFollow_SelfFollow_InvalidArgument(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Graph: &fakeGraph{followErr: social.ErrSelfFollow}})
	_, err := srv.Follow(context.Background(), &sharingv1.FollowRequest{
		FollowerGcid: "a", FolloweeGcid: "a", TenantId: "t",
	})
	statusIs(t, err, codes.InvalidArgument)
}

func TestFollow_InvalidArgumentFromGraph(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Graph: &fakeGraph{followErr: social.ErrInvalidArgument}})
	_, err := srv.Follow(context.Background(), &sharingv1.FollowRequest{
		FollowerGcid: "a", FolloweeGcid: "b", TenantId: "t",
	})
	statusIs(t, err, codes.InvalidArgument)
}

func TestFollow_GraphError_Internal(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Graph: &fakeGraph{followErr: errors.New("db down")}})
	_, err := srv.Follow(context.Background(), &sharingv1.FollowRequest{
		FollowerGcid: "a", FolloweeGcid: "b", TenantId: "t",
	})
	statusIs(t, err, codes.Internal)
}

// -----------------------------------------------------------------------------
// Unfollow
// -----------------------------------------------------------------------------

func TestUnfollow_NotWired_Unimplemented(t *testing.T) {
	t.Parallel()
	_, err := New(Deps{}).Unfollow(context.Background(), &sharingv1.UnfollowRequest{
		FollowerGcid: "a", FolloweeGcid: "b", TenantId: "t",
	})
	statusIs(t, err, codes.Unimplemented)
}

func TestUnfollow_InvalidInput(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Graph: &fakeGraph{}})
	for name, req := range map[string]*sharingv1.UnfollowRequest{
		"nil request": nil,
		"all missing": {},
		"no tenant":   {FollowerGcid: "a", FolloweeGcid: "b"},
	} {
		if _, err := srv.Unfollow(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: expected InvalidArgument, got %v", name, err)
		}
	}
}

func TestUnfollow_HappyPath(t *testing.T) {
	t.Parallel()
	graph := &fakeGraph{}
	srv := New(Deps{Graph: graph})
	resp, err := srv.Unfollow(context.Background(), &sharingv1.UnfollowRequest{
		FollowerGcid: "a", FolloweeGcid: "b", TenantId: "t",
	})
	if err != nil {
		t.Fatalf("Unfollow: %v", err)
	}
	if !resp.GetWasFollowing() {
		t.Error("was_following=false, want true")
	}
	if !graph.unfollowed {
		t.Error("graph.Unfollow not called")
	}
}

func TestUnfollow_GraphError_Internal(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Graph: &fakeGraph{unfollowErr: errors.New("db down")}})
	_, err := srv.Unfollow(context.Background(), &sharingv1.UnfollowRequest{
		FollowerGcid: "a", FolloweeGcid: "b", TenantId: "t",
	})
	statusIs(t, err, codes.Internal)
}

// -----------------------------------------------------------------------------
// CreatePost
// -----------------------------------------------------------------------------

func TestCreatePost_NotWired_Unimplemented(t *testing.T) {
	t.Parallel()
	_, err := New(Deps{}).CreatePost(context.Background(), &sharingv1.CreatePostRequest{
		AuthorGcid: "a", TenantId: "t", Body: "hi",
	})
	statusIs(t, err, codes.Unimplemented)
}

func TestCreatePost_InvalidInput(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Posts: newFakePostRepo()})
	for name, req := range map[string]*sharingv1.CreatePostRequest{
		"nil request":   nil,
		"all missing":   {},
		"no author":     {TenantId: "t", Body: "hi"},
		"no tenant":     {AuthorGcid: "a", Body: "hi"},
	} {
		if _, err := srv.CreatePost(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: expected InvalidArgument, got %v", name, err)
		}
	}
}

func TestCreatePost_EmptyBody_InvalidArgument(t *testing.T) {
	t.Parallel()
	// NewPostWithVisibility refuses an empty body → ErrInvalidArgument →
	// InvalidArgument (a domain validation, not our fault).
	srv := New(Deps{Posts: newFakePostRepo()})
	_, err := srv.CreatePost(context.Background(), &sharingv1.CreatePostRequest{
		AuthorGcid: "a", TenantId: "t", Body: "   ",
	})
	statusIs(t, err, codes.InvalidArgument)
}

func TestCreatePost_HappyPath(t *testing.T) {
	t.Parallel()
	posts := newFakePostRepo()
	srv := New(Deps{Posts: posts})
	resp, err := srv.CreatePost(context.Background(), &sharingv1.CreatePostRequest{
		AuthorGcid:     "a",
		TenantId:       "t",
		Body:           "hello world",
		AtomId:         "atom-1",
		Visibility:     sharingv1.PostVisibility_POST_VISIBILITY_FOLLOWERS,
		IdempotencyKey: "idem-post-1",
	})
	if err != nil {
		t.Fatalf("CreatePost: %v", err)
	}
	p := resp.GetPost()
	if p == nil {
		t.Fatal("response post nil")
	}
	if p.GetPostId() == "" {
		t.Error("post_id empty (NewPost stamps a UUIDv7)")
	}
	if p.GetBody() != "hello world" || p.GetAtomId() != "atom-1" {
		t.Errorf("post fields mismatch: %+v", p)
	}
	if p.GetKind() != sharingv1.PostKind_POST_KIND_FEEDBACK {
		t.Errorf("kind=%v want FEEDBACK", p.GetKind())
	}
	if p.GetVisibility() != sharingv1.PostVisibility_POST_VISIBILITY_FOLLOWERS {
		t.Errorf("visibility=%v want FOLLOWERS (tenant maps back to FOLLOWERS)", p.GetVisibility())
	}
	if p.GetIsRemoved() {
		t.Error("is_removed=true for a fresh post")
	}
}

func TestCreatePost_SaveError_Internal(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Posts: &fakePostRepo{saveErr: errors.New("disk full")}})
	_, err := srv.CreatePost(context.Background(), &sharingv1.CreatePostRequest{
		AuthorGcid: "a", TenantId: "t", Body: "hi",
	})
	statusIs(t, err, codes.Internal)
}

// -----------------------------------------------------------------------------
// ReactToPost
// -----------------------------------------------------------------------------

func TestReactToPost_NotWired_Unimplemented(t *testing.T) {
	t.Parallel()
	t.Run("nilReactions", func(t *testing.T) {
		_, err := New(Deps{Posts: newFakePostRepo()}).ReactToPost(context.Background(), &sharingv1.ReactToPostRequest{
			PostId: "p", ReactorGcid: "g", TenantId: "t",
			Kind: sharingv1.ReactionKind_REACTION_KIND_LIKE,
		})
		statusIs(t, err, codes.Unimplemented)
	})
	t.Run("nilPosts", func(t *testing.T) {
		_, err := New(Deps{Reactions: &fakeReactionRepo{}}).ReactToPost(context.Background(), &sharingv1.ReactToPostRequest{
			PostId: "p", ReactorGcid: "g", TenantId: "t",
			Kind: sharingv1.ReactionKind_REACTION_KIND_LIKE,
		})
		statusIs(t, err, codes.Unimplemented)
	})
}

func TestReactToPost_InvalidInput(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Posts: newFakePostRepo(), Reactions: &fakeReactionRepo{}})
	for name, req := range map[string]*sharingv1.ReactToPostRequest{
		"nil request":  nil,
		"all missing":  {},
		"no post_id":   {ReactorGcid: "g", TenantId: "t"},
		"no reactor":   {PostId: "p", TenantId: "t"},
		"no tenant":    {PostId: "p", ReactorGcid: "g"},
	} {
		if _, err := srv.ReactToPost(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: expected InvalidArgument, got %v", name, err)
		}
	}
}

func TestReactToPost_PostNotFound_NotFound(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Posts: newFakePostRepo(), Reactions: &fakeReactionRepo{}})
	_, err := srv.ReactToPost(context.Background(), &sharingv1.ReactToPostRequest{
		PostId: "missing", ReactorGcid: "g", TenantId: "t",
		Kind: sharingv1.ReactionKind_REACTION_KIND_LIKE,
	})
	statusIs(t, err, codes.NotFound)
}

func TestReactToPost_LookupError_Internal(t *testing.T) {
	t.Parallel()
	posts := newFakePostRepo()
	posts.getErr = errors.New("db down")
	srv := New(Deps{Posts: posts, Reactions: &fakeReactionRepo{}})
	_, err := srv.ReactToPost(context.Background(), &sharingv1.ReactToPostRequest{
		PostId: "p", ReactorGcid: "g", TenantId: "t",
		Kind: sharingv1.ReactionKind_REACTION_KIND_LIKE,
	})
	statusIs(t, err, codes.Internal)
}

func TestReactToPost_UnknownKind_InvalidArgument(t *testing.T) {
	t.Parallel()
	posts := newFakePostRepo()
	posts.Save(context.Background(), &post.Post{ID: "p", TenantID: "t", AuthorGCID: "a", Body: "b"})
	srv := New(Deps{Posts: posts, Reactions: &fakeReactionRepo{}})
	_, err := srv.ReactToPost(context.Background(), &sharingv1.ReactToPostRequest{
		PostId: "p", ReactorGcid: "g", TenantId: "t",
		Kind: sharingv1.ReactionKind_REACTION_KIND_UNSPECIFIED,
	})
	statusIs(t, err, codes.InvalidArgument)
}

func TestReactToPost_ReactInvalidArgument(t *testing.T) {
	t.Parallel()
	posts := newFakePostRepo()
	posts.Save(context.Background(), &post.Post{ID: "p", TenantID: "t", AuthorGCID: "a", Body: "b"})
	srv := New(Deps{Posts: posts, Reactions: &fakeReactionRepo{reactErr: reaction.ErrInvalidArgument}})
	_, err := srv.ReactToPost(context.Background(), &sharingv1.ReactToPostRequest{
		PostId: "p", ReactorGcid: "g", TenantId: "t",
		Kind: sharingv1.ReactionKind_REACTION_KIND_LIKE,
	})
	statusIs(t, err, codes.InvalidArgument)
}

func TestReactToPost_ReactError_Internal(t *testing.T) {
	t.Parallel()
	posts := newFakePostRepo()
	posts.Save(context.Background(), &post.Post{ID: "p", TenantID: "t", AuthorGCID: "a", Body: "b"})
	srv := New(Deps{Posts: posts, Reactions: &fakeReactionRepo{reactErr: errors.New("db down")}})
	_, err := srv.ReactToPost(context.Background(), &sharingv1.ReactToPostRequest{
		PostId: "p", ReactorGcid: "g", TenantId: "t",
		Kind: sharingv1.ReactionKind_REACTION_KIND_LIKE,
	})
	statusIs(t, err, codes.Internal)
}

func TestReactToPost_ListError_Internal(t *testing.T) {
	t.Parallel()
	posts := newFakePostRepo()
	posts.Save(context.Background(), &post.Post{ID: "p", TenantID: "t", AuthorGCID: "a", Body: "b"})
	srv := New(Deps{Posts: posts, Reactions: &fakeReactionRepo{listErr: errors.New("db down")}})
	_, err := srv.ReactToPost(context.Background(), &sharingv1.ReactToPostRequest{
		PostId: "p", ReactorGcid: "g", TenantId: "t",
		Kind: sharingv1.ReactionKind_REACTION_KIND_LIKE,
	})
	statusIs(t, err, codes.Internal)
}

func TestReactToPost_HappyPath(t *testing.T) {
	t.Parallel()
	posts := newFakePostRepo()
	posts.Save(context.Background(), &post.Post{
		ID: "p", TenantID: "t", AuthorGCID: "a", Body: "b", PostedAt: time.Now().UTC(),
	})
	rx := &fakeReactionRepo{}
	rx.reactions = []*reaction.Reaction{{
		ID: "rx-1", TenantID: "t", GCID: "g", PostID: "p",
		Type: reaction.TypeLike, CreatedAt: time.Now().UTC(),
	}}
	srv := New(Deps{Posts: posts, Reactions: rx})
	resp, err := srv.ReactToPost(context.Background(), &sharingv1.ReactToPostRequest{
		PostId: "p", ReactorGcid: "g", TenantId: "t",
		Kind: sharingv1.ReactionKind_REACTION_KIND_LIKE,
	})
	if err != nil {
		t.Fatalf("ReactToPost: %v", err)
	}
	if resp.GetReaction() == nil || resp.GetReaction().GetPostId() != "p" {
		t.Errorf("reaction missing/mismatched: %+v", resp.GetReaction())
	}
	if resp.GetPost() == nil {
		t.Fatal("post nil")
	}
	if resp.GetPost().GetReactionCount() != 1 {
		t.Errorf("reaction_count=%d want 1", resp.GetPost().GetReactionCount())
	}
	if resp.GetReaction().GetKind() != sharingv1.ReactionKind_REACTION_KIND_LIKE {
		t.Errorf("reaction kind=%v want LIKE", resp.GetReaction().GetKind())
	}
}

// -----------------------------------------------------------------------------
// GetLeaderboard
// -----------------------------------------------------------------------------

func TestGetLeaderboard_NotWired_Unimplemented(t *testing.T) {
	t.Parallel()
	_, err := New(Deps{}).GetLeaderboard(context.Background(), &sharingv1.GetLeaderboardRequest{
		Scope: sharingv1.LeaderboardScope_LEADERBOARD_SCOPE_GLOBAL,
	})
	statusIs(t, err, codes.Unimplemented)
}

func TestGetLeaderboard_InvalidInput(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Leaderboards: &fakeLeaderboardReader{}, Rules: testRules()})
	tests := map[string]*sharingv1.GetLeaderboardRequest{
		"nil request":     nil,
		"unknown scope":   {Scope: sharingv1.LeaderboardScope_LEADERBOARD_SCOPE_UNSPECIFIED},
		"cohort no id":    {Scope: sharingv1.LeaderboardScope_LEADERBOARD_SCOPE_COURSE},
		"class no id":     {Scope: sharingv1.LeaderboardScope_LEADERBOARD_SCOPE_CLASS},
		"tenant no tena":  {Scope: sharingv1.LeaderboardScope_LEADERBOARD_SCOPE_TENANT},
	}
	for name, req := range tests {
		if _, err := srv.GetLeaderboard(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: expected InvalidArgument, got %v", name, err)
		}
	}
}

func TestGetLeaderboard_Global_DefaultLimit_PeriodAllTime(t *testing.T) {
	t.Parallel()
	lb := &fakeLeaderboardReader{entries: []leaderboard.Entry{
		{GCID: "g1", Score: 100, Rank: 1},
		{GCID: "g2", Score: 90, Rank: 2},
	}}
	srv := New(Deps{Leaderboards: lb, Rules: testRules()})
	resp, err := srv.GetLeaderboard(context.Background(), &sharingv1.GetLeaderboardRequest{
		Scope: sharingv1.LeaderboardScope_LEADERBOARD_SCOPE_GLOBAL,
		// limit 0 → default 20; no season → all-time.
	})
	if err != nil {
		t.Fatalf("GetLeaderboard: %v", err)
	}
	if len(resp.GetEntries()) != 2 {
		t.Fatalf("entries=%d want 2", len(resp.GetEntries()))
	}
	if resp.GetEntries()[0].GetGcid() != "g1" || resp.GetEntries()[0].GetScore() != 100 || resp.GetEntries()[0].GetRank() != 1 {
		t.Errorf("entry[0] mismatch: %+v", resp.GetEntries()[0])
	}
	if resp.GetComputedAt() == nil || !resp.GetComputedAt().IsValid() {
		t.Error("computed_at missing")
	}
	if lb.gotKind != leaderboard.ScopeGlobal || lb.gotLimit != 20 || lb.gotPeriod != leaderboard.PeriodAllTime {
		t.Errorf("ReadTop args mismatch: kind=%v limit=%d period=%v", lb.gotKind, lb.gotLimit, lb.gotPeriod)
	}
}

func TestGetLeaderboard_Cohort_LimitClamp_PeriodMonthly(t *testing.T) {
	t.Parallel()
	lb := &fakeLeaderboardReader{}
	srv := New(Deps{Leaderboards: lb, Rules: testRules()})
	// limit 10000 → clamped to 100; season window set → monthly period.
	_, err := srv.GetLeaderboard(context.Background(), &sharingv1.GetLeaderboardRequest{
		Scope:         sharingv1.LeaderboardScope_LEADERBOARD_SCOPE_COURSE,
		ScopeTargetId: "cohort-1",
		TenantId:      "t",
		Limit:         10000,
		SeasonStart:   timestamppbNow(),
	})
	if err != nil {
		t.Fatalf("GetLeaderboard: %v", err)
	}
	if lb.gotKind != leaderboard.ScopeCohort || lb.gotScopeID != "cohort-1" || lb.gotLimit != 100 || lb.gotPeriod != leaderboard.PeriodMonthly {
		t.Errorf("ReadTop args mismatch: kind=%v scopeID=%q limit=%d period=%v", lb.gotKind, lb.gotScopeID, lb.gotLimit, lb.gotPeriod)
	}
}

func TestGetLeaderboard_ReadError_Internal(t *testing.T) {
	t.Parallel()
	srv := New(Deps{Leaderboards: &fakeLeaderboardReader{err: errors.New("db down")}, Rules: testRules()})
	_, err := srv.GetLeaderboard(context.Background(), &sharingv1.GetLeaderboardRequest{
		Scope: sharingv1.LeaderboardScope_LEADERBOARD_SCOPE_TENANT,
		TenantId: "t",
	})
	statusIs(t, err, codes.Internal)
}