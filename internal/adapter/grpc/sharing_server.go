// Package grpcadapter binds the chora-contracts-defined Sharing gRPC contract
// (chora-contracts/proto/services/sharing/v1/sharing.proto) to chora-sharing's
// domain aggregates + ports.
//
// Greenfield rebuild of the Content Sharing domain (docs/chora-sharing.md).
// This adapter implements ALL 15 Sharing RPCs. The Deps struct carries domain
// port interfaces; a nil port for an RPC's dependency returns
// codes.Unimplemented (fail-loud per §1.1 — never a fake success).
//
// Hexagonal layout: this adapter depends on the pure-domain packages (post,
// reaction, social, leaderboard, grant, atom_share, atom_projection,
// comment, currency, livequiz) + the modelarmor GuardrailPort + the
// config SharingRules. Domain packages NEVER import from here.
// cmd/server/main.go is the sole composition root.
package grpcadapter

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	sharingv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/sharing/v1"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/adapter/modelarmor"
	"github.com/apollo-chora/chora-sharing/internal/config"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
	"github.com/apollo-chora/chora-sharing/internal/domain/bookmark"
	"github.com/apollo-chora/chora-sharing/internal/domain/comment"
	"github.com/apollo-chora/chora-sharing/internal/domain/currency"
	"github.com/apollo-chora/chora-sharing/internal/domain/grant"
	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
	"github.com/apollo-chora/chora-sharing/internal/domain/livequiz"
	"github.com/apollo-chora/chora-sharing/internal/domain/post"
	"github.com/apollo-chora/chora-sharing/internal/domain/reaction"
	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)
// The social RPCs (Follow, CreatePost, ReactToPost, GetLeaderboard) consume
// the existing inmem adapters. To keep the Deps struct interface-typed (so
// tests inject fakes + production swaps to pg without touching this file),
// these narrow interfaces capture exactly the methods the handlers call. The
// inmem.PostRepo / reaction.Registry / social.Graph / leaderboard.Ranker
// already satisfy them.
// =============================================================================

// PostStore is the social-feed post write+read port used by CreatePost +
// ReactToPost.
// PostStore removed (CHO-2193/W0-F1) — the port is now post.PostRepo in the
// domain. See the note in the http adapter.

// ReactionStore is the reaction idempotent-react + unreact + list port.
// ReactionStore removed (CHO-2193/W0-F1) — port is reaction.ReactionRepo in the domain.

// SocialGraph is the follow-edge port (pg-backed + durable in production per
// ADR-229 WS-0 — the old in-memory graph lost runtime edges on pod restart;
// tenant-aware in-memory in dev). ctx carries cancellation; tenantID scopes
// the RLS session on every mutation.
type SocialGraph interface {
	Follow(ctx context.Context, tenantID, follower, followee string) (*social.Edge, bool, error)
	Unfollow(ctx context.Context, tenantID, follower, followee string) (bool, error)
}

// CommentStore is the comment create + list port.
type CommentStore interface {
	Create(ctx context.Context, c *comment.Comment) error
	ListByPost(ctx context.Context, postID string, limit int, cursor string) ([]comment.Comment, string, error)
}

// CurrencyStore is the author non-cash credit port.
type CurrencyStore interface {
	CreditAuthor(ctx context.Context, tenantID, holderGCID string, currency currency.Currency, amount float64) error
	GetBalance(ctx context.Context, tenantID, holderGCID, currency string) (float64, error)
}

// =============================================================================
// Deps + SharingServer
// =============================================================================
type Deps struct {
	Shares       atom_share.ShareRepo
	Projections  atom_projection.AtomProjectionReader
	Grants       grant.GrantRepo
	Royalties    grant.RoyaltyRepo
	Leaderboards leaderboard.LeaderboardReader
	Posts        post.PostRepo
	Reactions    reaction.ReactionRepo
	Graph        SocialGraph
	Friends      social.GraphQueries
	Comments     CommentStore
	Currency     CurrencyStore
	Bookmarks    bookmark.BookmarkRepo
	QuizGen      livequiz.QuizGenerator
	Guardrail    modelarmor.GuardrailPort
	Rules        config.SharingRules
	// Mana is the reuser-tenant debit port for royalty settlement (§3.3
	// double-entry: debit mana, credit author non-cash currency). Nil in
	// dev (no identity gRPC) — royalty-bearing grants skip settlement
	// with a WARN, never a fake success.
	Mana ManaDebiter
	// RoyaltyEvents publishes chora.sharing.royalty.settled.v1. Nil when
	// no bus is wired — settlement still records + debits + credits, the
	// event publish is skipped (best-effort, like the duel.completed path).
	RoyaltyEvents RoyaltyEventPublisher
}

// ManaDebiter is the reuser-tenant mana debit port (the spend side of the
// double-entry royalty settlement). Implemented by clients.ManaClient over
// chora-identity ManaService.DeductMana (gRPC).
type ManaDebiter interface {
	DebitReuser(ctx context.Context, granteeGCID, granteeTenantID string, amount float64, actionCode string) error
}

// RoyaltyEventPublisher emits the royalty.settled.v1 event. Implemented by
// the events.Publisher adapter. Publish is best-effort — a publish failure
// MUST NOT fail the grant (the settlement record + debit + credit already
// landed; the event is the downstream notification).
type RoyaltyEventPublisher interface {
	PublishRoyaltySettled(ctx context.Context, settlementID, grantID, ownerGCID, granteeTenantID, atomID string, amount float64, currency, usageContext, sourceEventID, tenantID string) error
}
// SharingServer satisfies sharingv1.SharingServer by adapting proto requests
// to the domain ports in Deps. Every RPC checks its required deps are non-nil
// and returns codes.Unimplemented when a dep is missing (fail-loud).
type SharingServer struct {
	sharingv1.UnimplementedSharingServer
	deps Deps
}

// New constructs a SharingServer. Unlike the legacy constructor this does NOT
// error on a nil dep — each RPC fails loud with codes.Unimplemented at call
// time (so a partially-wired boot still serves the RPCs it can, and the
// unwired ones refuse loudly). This matches §1.1 + §6 status-code discipline.
func New(deps Deps) *SharingServer {
	return &SharingServer{deps: deps}
}

// notWired returns a codes.Unimplemented error naming the missing dep, so a
// caller gets a loud, traceable refusal instead of a fake success.
func notWired(dep string) error {
	return status.Errorf(codes.Unimplemented, "sharing: dependency %q not wired (fail-loud per §1.1)", dep)
}

// =============================================================================
// Social surface RPCs
// =============================================================================

// Follow creates (or returns the existing) directional follow edge.
// Idempotent on (follower_gcid, followee_gcid).
func (s *SharingServer) Follow(ctx context.Context, req *sharingv1.FollowRequest) (*sharingv1.FollowResponse, error) {
	if s.deps.Graph == nil {
		return nil, notWired("Graph")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	follower := strings.TrimSpace(req.GetFollowerGcid())
	followee := strings.TrimSpace(req.GetFolloweeGcid())
	tenant := strings.TrimSpace(req.GetTenantId())
	if follower == "" || followee == "" || tenant == "" {
		return nil, status.Error(codes.InvalidArgument, "follower_gcid, followee_gcid, tenant_id required")
	}
	edge, _, err := s.deps.Graph.Follow(ctx, tenant, follower, followee)
	if err != nil {
		if errors.Is(err, social.ErrSelfFollow) {
			return nil, status.Error(codes.InvalidArgument, "cannot follow self")
		}
		if errors.Is(err, social.ErrInvalidArgument) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "follow: %v", err)
	}
	return &sharingv1.FollowResponse{Follow: edgeToProto(edge)}, nil
}

// Unfollow removes the (follower, followee) edge if present. Idempotent.
func (s *SharingServer) Unfollow(ctx context.Context, req *sharingv1.UnfollowRequest) (*sharingv1.UnfollowResponse, error) {
	if s.deps.Graph == nil {
		return nil, notWired("Graph")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	follower := strings.TrimSpace(req.GetFollowerGcid())
	followee := strings.TrimSpace(req.GetFolloweeGcid())
	tenant := strings.TrimSpace(req.GetTenantId())
	if follower == "" || followee == "" || tenant == "" {
		return nil, status.Error(codes.InvalidArgument, "follower_gcid, followee_gcid, tenant_id required")
	}
	removed, err := s.deps.Graph.Unfollow(ctx, tenant, follower, followee)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "unfollow: %v", err)
	}
	return &sharingv1.UnfollowResponse{WasFollowing: removed}, nil
}

// CreatePost constructs a Post via the domain factory and persists it.
// gRPC callers are internal services past the BFF layer so this surface skips
// the Cloud Model Armor pre-flight (§7.1 step 6 — the HTTP layer runs that).
func (s *SharingServer) CreatePost(ctx context.Context, req *sharingv1.CreatePostRequest) (*sharingv1.CreatePostResponse, error) {
	if s.deps.Posts == nil {
		return nil, notWired("Posts")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	author := strings.TrimSpace(req.GetAuthorGcid())
	tenant := strings.TrimSpace(req.GetTenantId())
	body := req.GetBody()
	if author == "" || tenant == "" {
		return nil, status.Error(codes.InvalidArgument, "author_gcid, tenant_id required")
	}
	vis := protoVisibilityToDomain(req.GetVisibility())
	p, err := post.NewPostWithVisibility(tenant, author, body, strings.TrimSpace(req.GetAtomId()), nil, vis)
	if err != nil {
		if errors.Is(err, post.ErrInvalidArgument) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "create post: %v", err)
	}
	if err := s.deps.Posts.Save(ctx, p); err != nil {
		// A failed write is OUR fault and must be LOUD. Before CHO-2193/W0-F1
		// this call could not fail (it was a map write), so the caller was told
		// the post was created while nothing was persisted.
		return nil, status.Errorf(codes.Internal, "save post: %v", err)
	}
	return &sharingv1.CreatePostResponse{Post: postToProto(p, 0)}, nil
}

// ReactToPost stores a Reaction (idempotent on gcid+post+kind) and returns
// the canonical post snapshot with the updated reaction count.
func (s *SharingServer) ReactToPost(ctx context.Context, req *sharingv1.ReactToPostRequest) (*sharingv1.ReactToPostResponse, error) {
	if s.deps.Reactions == nil {
		return nil, notWired("Reactions")
	}
	if s.deps.Posts == nil {
		return nil, notWired("Posts")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	postID := strings.TrimSpace(req.GetPostId())
	reactor := strings.TrimSpace(req.GetReactorGcid())
	tenant := strings.TrimSpace(req.GetTenantId())
	if postID == "" || reactor == "" || tenant == "" {
		return nil, status.Error(codes.InvalidArgument, "post_id, reactor_gcid, tenant_id required")
	}
	p, ok, err := s.deps.Posts.Get(ctx, postID)
	if err != nil {
		// A broken lookup is OURS (Internal), NOT the caller's (NotFound).
		// Collapsing the two is the CHO-2184 class: an infra failure surfaces
		// as a clean 404, the caller retries into the same wall, and the real
		// fault never pages anyone.
		return nil, status.Errorf(codes.Internal, "lookup post: %v", err)
	}
	if !ok {
		return nil, status.Error(codes.NotFound, "post not found")
	}
	rt := protoReactionKindToDomain(req.GetKind())
	if rt == "" {
		return nil, status.Error(codes.InvalidArgument, "unknown reaction kind")
	}
	rx, _, err := s.deps.Reactions.React(ctx, tenant, reactor, postID, rt)
	if err != nil {
		if errors.Is(err, reaction.ErrInvalidArgument) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "react: %v", err)
	}
	rxs, err := s.deps.Reactions.ListByPost(ctx, postID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list reactions: %v", err)
	}
	rxCount := int32(len(rxs))
	return &sharingv1.ReactToPostResponse{
		Reaction: reactionToProto(rx),
		Post:     postToProto(p, rxCount),
	}, nil
}

// GetLeaderboard maps proto scope+metric+season window onto the
// LeaderboardReader. Ties share a rank; tie-break gcid ASC (deterministic).
func (s *SharingServer) GetLeaderboard(ctx context.Context, req *sharingv1.GetLeaderboardRequest) (*sharingv1.GetLeaderboardResponse, error) {
	if s.deps.Leaderboards == nil {
		return nil, notWired("Leaderboards")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	scopeKind, scopeID, err := protoLeaderboardScope(req.GetScope(), strings.TrimSpace(req.GetScopeTargetId()))
	if err != nil {
		return nil, err
	}
	tenant := strings.TrimSpace(req.GetTenantId())
	if scopeKind == leaderboard.ScopeTenant && tenant == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id required for TENANT scope")
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	period := protoLeaderboardPeriod(req.GetSeasonStart(), req.GetSeasonEnd(), s.deps.Rules)
	entries, err := s.deps.Leaderboards.ReadTop(ctx, scopeKind, scopeID, tenant, period, limit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "leaderboard read: %v", err)
	}
	out := make([]*sharingv1.LeaderboardEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, &sharingv1.LeaderboardEntry{
			Gcid:  e.GCID,
			Score: int64(e.Score),
			Rank:  int32(e.Rank),
		})
	}
	return &sharingv1.GetLeaderboardResponse{
		Entries:    out,
		NextCursor: "",
		ComputedAt: timestamppb.New(time.Now().UTC()),
	}, nil
}

// =============================================================================
// Atom Sharing surface (§7.1–§7.4)
// =============================================================================

// ShareAtom — an author shares their own atom to the feed with a per-atom
// license (R1 + R2). Author validation via AtomProjectionReader
// (R1: caller MUST equal projection owner_gcid). Per §7.1.
func (s *SharingServer) ShareAtom(ctx context.Context, req *sharingv1.ShareAtomRequest) (*sharingv1.ShareAtomResponse, error) {
	if s.deps.Shares == nil {
		return nil, notWired("Shares")
	}
	if s.deps.Projections == nil {
		return nil, notWired("Projections")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	author := strings.TrimSpace(req.GetAuthorGcid())
	tenant := strings.TrimSpace(req.GetTenantId())
	atomID := strings.TrimSpace(req.GetAtomId())
	if author == "" || tenant == "" || atomID == "" {
		return nil, status.Error(codes.InvalidArgument, "author_gcid, tenant_id, atom_id required")
	}
	if strings.TrimSpace(req.GetIdempotencyKey()) == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key required")
	}
	caption := req.GetCaption()

	// §7.1 step 6: caption moderation (FR-037). Guardrail optional — when
	// wired + caption non-empty, screen BEFORE persisting. blocked → 422
	// (never persisted); flagged → proceed + log (audit-only).
	if s.deps.Guardrail != nil && strings.TrimSpace(caption) != "" {
		verdict, err := s.deps.Guardrail.Screen(ctx, modelarmor.ScreenRequest{
			TenantID:   tenant,
			AuthorGCID: author,
			AgentID:    modelarmor.AgentIDSocialModeration,
			Content:    caption,
		})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "guardrail screen: %v", err)
		}
		if verdict.Verdict == modelarmor.VerdictBlocked {
			return nil, status.Error(codes.FailedPrecondition, "caption blocked by guardrail (422)")
		}
	}

	// §7.1 step 7: R1 author validation — load AtomProjection.
	proj, err := s.deps.Projections.Get(ctx, atomID)
	if err != nil {
		if errors.Is(err, atom_projection.ErrNotFound) {
			return nil, status.Error(codes.FailedPrecondition, "atom not published or withdrawn (412)")
		}
		return nil, status.Errorf(codes.Internal, "projection lookup: %v", err)
	}
	if proj.OwnerGCID != author {
		return nil, status.Error(codes.PermissionDenied, "R1: caller is not the atom owner (403)")
	}

	// §7.1 step 8: pin revision — explicit wins; else projection's latest.
	revisionID := strings.TrimSpace(req.GetAtomRevisionId())
	if revisionID == "" {
		revisionID = proj.RevisionID
	}

	// CHO-2174b — ShareAtom is a consumer that GENUINELY NEEDS A QUESTION: the
	// feed card denormalises the question and the share pins a revision.
	// atom_projections is now a CONSENT read-model first (mig 0036), so a
	// published atom may legitimately resolve here carrying no question at all.
	// Refuse it DELIBERATELY and name the cause.
	//
	// Before this guard the refusal happened BY ACCIDENT, downstream in
	// atom_share.NewShare, as InvalidArgument "atom_revision_id required" —
	// which blames the CALLER's request for a fact about the ATOM. The caller's
	// request is fine; the atom simply has no question to share.
	//
	// ⚠ The signal is the REVISION, not the stem. Live (2026-07-14): stem is
	// EMPTY on 100% of projected rows (97/97) — gating on it would refuse every
	// share in production. An explicitly-supplied atom_revision_id still wins
	// (the caller pinned it themselves).
	if revisionID == "" {
		return nil, status.Error(codes.FailedPrecondition,
			"atom has no question revision to pin — it cannot be shared to the feed "+
				"(sharing requires a question; supply atom_revision_id explicitly to override) (412)")
	}

	license := protoLicenseToDomain(req.GetLicenseTerms())
	rate := protoRoyaltyRateToDomain(req.GetRoyaltyRate())

	// §7.1 step 9–10: NewShare validates R2 (royalty_rate required iff
	// royalty license) + caption ≤ 512 + denormalises R1 snapshot.
	share, err := atom_share.NewShare(
		tenant, atomID, revisionID, proj.OwnerGCID,
		proj.AuthorDisplayName, proj.Stem, string(proj.QuestionType),
		proj.Options, caption, license, rate,
	)
	if err != nil {
		if errors.Is(err, atom_share.ErrInvalidArgument) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "new share: %v", err)
	}

	// §7.1 step 11: assign the deterministic feed entry id derived from the
	// idempotency key (UUIDv5 — the pg UNIQUE constraint dedups replays).
	// The domain NewShare leaves FeedEntryID empty; the adapter owns ID
	// assignment so the domain stays free of crypto/uuid imports.
	share.FeedEntryID = deriveFeedEntryID(req.GetIdempotencyKey(), tenant, atomID)
	if err := s.deps.Shares.SaveShare(ctx, share); err != nil {
		return nil, status.Errorf(codes.Internal, "save share: %v", err)
	}

	// §7.1 step 12: event publish (fail-soft) — omitted here; the outbox
	// dispatcher publishes chora.sharing.atom.shared.v1 atomically with the
	// SaveShare transaction in the pg adapter. The gRPC surface returns 201.

	return &sharingv1.ShareAtomResponse{
		ShareEntryId:      share.FeedEntryID,
		AuthorDisplayName: share.AuthorDisplayName,
		CreatedAt:         timestamppb.New(share.CreatedAt),
	}, nil
}

// ListSharedAtoms — the shared-atom feed read (RMM L2, pagination 20/max 100).
func (s *SharingServer) ListSharedAtoms(ctx context.Context, req *sharingv1.ListSharedAtomsRequest) (*sharingv1.ListSharedAtomsResponse, error) {
	if s.deps.Shares == nil {
		return nil, notWired("Shares")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	tenant := strings.TrimSpace(req.GetTenantId())
	if tenant == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id required")
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	shares, nextCursor, err := s.deps.Shares.ListSharedAtoms(
		ctx, tenant, req.GetCursor(), limit,
		req.GetTopicFilter(), req.GetQuestionTypeFilter(),
		"tenant", nil, nil,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list shared atoms: %v", err)
	}
	cards := make([]*sharingv1.SharedAtomCard, 0, len(shares))
	for _, sh := range shares {
		cards = append(cards, shareToCard(sh))
	}
	return &sharingv1.ListSharedAtomsResponse{
		Cards:      cards,
		NextCursor: nextCursor,
	}, nil
}

// AuthorizeAtomUse — creates (or returns) an idempotent AtomUsageGrant that
// snapshots license terms at grant time (R2). Per §7.3.
func (s *SharingServer) AuthorizeAtomUse(ctx context.Context, req *sharingv1.AuthorizeAtomUseRequest) (*sharingv1.AuthorizeAtomUseResponse, error) {
	if s.deps.Grants == nil {
		return nil, notWired("Grants")
	}
	if s.deps.Projections == nil {
		return nil, notWired("Projections")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	grantee := strings.TrimSpace(req.GetGranteeGcid())
	tenant := strings.TrimSpace(req.GetTenantId())
	atomID := strings.TrimSpace(req.GetAtomId())
	if grantee == "" || tenant == "" || atomID == "" {
		return nil, status.Error(codes.InvalidArgument, "grantee_gcid, tenant_id, atom_id required")
	}
	scope := protoGrantScopeToDomain(req.GetScope())
	if !scope.IsValid() {
		return nil, status.Error(codes.InvalidArgument, "scope must be specified (non-UNSPECIFIED)")
	}
	if strings.TrimSpace(req.GetIdempotencyKey()) == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key required")
	}

	// Stamp the request tenant onto ctx BEFORE any store call — the pg
	// stores' rls.ApplySession fails loud without it. Latent since spec-001
	// (this RPC had zero callers); surfaced the first time the WS-2 snapshot
	// gate fired live (2026-07-11 walk: "rls: tenant_id missing on context").
	ctx = tracing.WithTenantID(ctx, tenant)

	// §7.3 step 2: R1 owner resolution via AtomProjection.
	proj, err := s.deps.Projections.Get(ctx, atomID)
	if err != nil {
		if errors.Is(err, atom_projection.ErrNotFound) {
			return nil, status.Error(codes.FailedPrecondition, "atom not published or withdrawn (412)")
		}
		return nil, status.Errorf(codes.Internal, "projection lookup: %v", err)
	}

	// §7.3 step 3: freeze license snapshot (FR-009). Three resolutions:
	//
	//   1. own-atom            → free, no rate (shipped).
	//   2. audience-based      → non-owner + NO source_share_entry (ADR-229
	//      WS-2, CHO-2133): the D2 audit grant minted at snapshot time for a
	//      tenant-visible atom. The audience IS the consent — allowed IFF the
	//      event-fed projection says reuse_visibility='tenant'; v1 reuse is
	//      free-license only (royalty settlement stays deferred). An empty /
	//      private / friends label fails closed (412) — friends joins only
	//      when Amendment A1.4 un-hides the audience (extend the guard
	//      alongside the picker's friends leg, never before).
	//   3. feed-share-sourced  → non-owner + source_share_entry (shipped):
	//      resolve the share's LicenseTerms + RoyaltyRate verbatim.
	var license atom_share.LicenseTerms
	var rate atom_share.RoyaltyRate
	sourceShare := strings.TrimSpace(req.GetSourceShareEntry())

	switch {
	case grantee == proj.OwnerGCID:
		// Own-atom → license free, no rate.
		license = atom_share.LicenseFree
	case sourceShare == "":
		// ADR-229 D2 audience-based audit grant (no feed promotion needed —
		// ShareAtom stays a separate act per D2).
		if proj.ReuseVisibility != "tenant" {
			return nil, status.Errorf(codes.FailedPrecondition,
				"atom is not audience-visible to the grantee (reuse_visibility=%q) and no source_share_entry was supplied — cannot mint a reuse grant (ADR-229 D2, 412)",
				proj.ReuseVisibility)
		}
		license = atom_share.LicenseFree
	default:
		// Shared-atom → resolve share's LicenseTerms + RoyaltyRate.
		if s.deps.Shares == nil {
			return nil, notWired("Shares")
		}
		sh, err := s.deps.Shares.GetShare(ctx, sourceShare)
		if err != nil {
			if errors.Is(err, atom_share.ErrNotFound) {
				return nil, status.Error(codes.FailedPrecondition, "share not found or revoked (412)")
			}
			return nil, status.Errorf(codes.Internal, "get share: %v", err)
		}
		license = sh.License
		rate = sh.Rate
	}

	// CHO-2174b — the SNAPSHOT REVISION-PINNING precondition.
	//
	// atom_usage_grants.atom_revision_id is `uuid NOT NULL`: a grant PHYSICALLY
	// cannot be minted without a pinned revision. Since mig 0036 the projection
	// is a CONSENT read-model first, so a consent-valid atom may carry no
	// question revision — and we must NOT fabricate one, NOT mint an unpinned
	// grant, and NOT let it fail as an opaque constraint violation downstream.
	//
	// Placement is load-bearing: this runs AFTER the consent switch above, so a
	// non-owner without consent is ALWAYS refused on CONSENT (the ADR-229 D2
	// audience refusal) and never learns anything about the atom's revision
	// state. Only a caller who has already passed the consent gate can reach
	// this refusal.
	//
	// Before this guard the failure surfaced BY ACCIDENT out of grant.NewGrant
	// as InvalidArgument "atom_revision_id required" — blaming the caller's
	// request for a fact about the atom's projection.
	if !proj.HasPinnedRevision() {
		return nil, status.Error(codes.FailedPrecondition,
			"atom has no revision to pin — a reuse grant snapshot requires a frozen atom_revision_id "+
				"and this atom's projection carries none (412)")
	}

	// §7.3 step 4: NewGrant validates scope (R-18) + license + rate.
	g, err := grant.NewGrant(proj.OwnerGCID, grantee, atomID, proj.RevisionID, scope, license, rate, sourceShare)
	if err != nil {
		if errors.Is(err, grant.ErrInvalidArgument) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "new grant: %v", err)
	}
	// NewGrant carries no tenant — stamp the request tenant onto the row
	// (tenant_id is a NOT NULL uuid column; an unset "" binds as invalid
	// uuid — live 22P02, 2026-07-11 walk catch #3).
	g.TenantID = tenant

	// §7.3 step 5: idempotent Authorize — ON CONFLICT DO NOTHING + SELECT
	// returns existing active grant when the conflict hits.
	stored, err := s.deps.Grants.Authorize(ctx, g)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "authorize grant: %v", err)
	}

	// §3.3 royalty settlement — fires ONLY for royalty-bearing licenses
	// (royalty_pct / royalty_fixed). Own-atom + audience-based grants are
	// always free (the license switch above guarantees this), so they skip
	// settlement entirely. The frozen snapshot on `stored` is the R2
	// dispute-proof source — an author price-change does NOT alter this
	// settlement.
	//
	// Idempotency: source_event_id = the request's idempotency_key. The
	// RoyaltyRepo dedups on it (ON CONFLICT DO NOTHING). A replay (the same
	// grant re-authorized) returns ErrRoyaltyAlreadySettled — treated as
	// success, not an error to surface.
	//
	// Fail-soft: the grant is the durable artifact. A mana-debit or
	// credit failure does NOT fail the RPC — the settlement record still
	// lands (the audit trail), and the debit/credit are retried by the
	// downstream consumer of royalty.settled.v1. This mirrors the
	// duel.completed pattern (best-effort publish, never blocks gameplay).
	if stored.LicenseTermsSnapshot.IsRoyalty() && s.deps.Royalties != nil {
		s.settleRoyalty(ctx, stored, grantee, tenant, req.GetIdempotencyKey(), string(scope))
	}

	// §7.3 step 6: event publish (fail-soft) — outbox in pg adapter.

	return &sharingv1.AuthorizeAtomUseResponse{
		GrantId:              stored.ID,
		OwnerGcid:            stored.OwnerGCID,
		LicenseTermsSnapshot: domainLicenseToProto(stored.LicenseTermsSnapshot),
		RoyaltyRateSnapshot:  domainRoyaltyRateToProto(stored.RoyaltyRateSnapshot),
		AtomRevisionId:       stored.RevisionID,
		Status:               domainGrantStatusToProto(stored.Status),
	}, nil
}

// settleRoyalty runs the §3.3 double-entry settlement for a royalty-bearing
// grant. Best-effort — a mana-debit or credit failure logs a WARN but does
// NOT fail the grant (the settlement record still lands for audit; the
// downstream royalty.settled.v1 consumer can reconcile).
//
// Steps:
//  1. SettleRoyalty computes the amount from the frozen snapshot + config caps.
//  2. RoyaltyRepo.Record persists it (idempotent on source_event_id).
//  3. ManaClient.DebitReuser debits the reuser's tenant mana.
//  4. CurrencyRepo.CreditAuthor credits the author's non-cash currency.
//  5. RoyaltyEventPublisher publishes royalty.settled.v1.
//
// Skips silently when amount == 0 (a royalty_pct with a 0 base or a 0 rate).
// ErrRoyaltyAlreadySettled (idempotent replay) is success — no double-debit.
func (s *SharingServer) settleRoyalty(ctx context.Context, g *grant.AtomUsageGrant, granteeGCID, tenantID, idempotencyKey, scope string) {
	lic, rate := g.SnapshotLicense()
	settlement := grant.SettleRoyalty(grant.SettleInput{
		BaseUsageValue:  1, // one reuse per authorize
		License:         lic,
		Rate:            rate,
		RoyaltyBase:     s.deps.Rules.RoyaltyBase,
		RoyaltyCap:      s.deps.Rules.RoyaltyCap,
		SourceEventID:   idempotencyKey,
		UsageContext:    scope,
		OwnerGCID:       g.OwnerGCID,
		GranteeTenantID: tenantID,
		AtomID:          g.AtomID,
		GrantID:         g.ID,
		Currency:        s.deps.Rules.RoyaltyCurrency,
	})
	if settlement.Amount == 0 {
		return // free-floor or 0-rate — no debit/credit/publish
	}

	// Step 2: record (idempotent on source_event_id).
	if err := s.deps.Royalties.Record(ctx, &settlement); err != nil {
		if errors.Is(err, grant.ErrRoyaltyAlreadySettled) {
			return // idempotent replay — already debited + credited
		}
		log.Printf("sharing: royalty record failed grant=%s source=%s: %v", g.ID, idempotencyKey, err)
		return
	}

	// Step 3: debit the reuser's tenant mana (the spend side).
	if s.deps.Mana != nil {
		if err := s.deps.Mana.DebitReuser(ctx, granteeGCID, tenantID, settlement.Amount, s.deps.Rules.ManaActionCode); err != nil {
			log.Printf("sharing: royalty mana debit failed grant=%s grantee=%s: %v", g.ID, granteeGCID, err)
		}
	}

	// Step 4: credit the author's non-cash currency (the accrue side).
	if s.deps.Currency != nil {
		creditCurrency := currency.Currency(s.deps.Rules.RoyaltyCurrency)
		if err := s.deps.Currency.CreditAuthor(ctx, tenantID, g.OwnerGCID, creditCurrency, settlement.Amount); err != nil {
			log.Printf("sharing: royalty author credit failed grant=%s owner=%s: %v", g.ID, g.OwnerGCID, err)
		}
	}

	// Step 5: publish royalty.settled.v1 (best-effort — downstream
	// consumers reconcile from the settlement record if this fails).
	if s.deps.RoyaltyEvents != nil {
		if err := s.deps.RoyaltyEvents.PublishRoyaltySettled(ctx,
			settlement.ID, g.ID, g.OwnerGCID, tenantID, g.AtomID,
			settlement.Amount, settlement.Currency, settlement.UsageContext,
			settlement.SourceEventID, tenantID,
		); err != nil {
			log.Printf("sharing: royalty publish failed grant=%s: %v", g.ID, err)
		}
	}
}

// RevokeAtomUse — revokes an AtomUsageGrant (append-only; never hard-delete).
// Revoker MUST be the atom owner or an admin. Per §7.3 contract notes.
func (s *SharingServer) RevokeAtomUse(ctx context.Context, req *sharingv1.RevokeAtomUseRequest) (*sharingv1.RevokeAtomUseResponse, error) {
	if s.deps.Grants == nil {
		return nil, notWired("Grants")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	grantID := strings.TrimSpace(req.GetGrantId())
	revoker := strings.TrimSpace(req.GetRevokerGcid())
	if grantID == "" || revoker == "" {
		return nil, status.Error(codes.InvalidArgument, "grant_id, revoker_gcid required")
	}
	if err := s.deps.Grants.Revoke(ctx, grantID, revoker, req.GetReason()); err != nil {
		if errors.Is(err, grant.ErrGrantNotFound) {
			return nil, status.Error(codes.NotFound, "grant not found (404)")
		}
		if errors.Is(err, grant.ErrForbidden) {
			return nil, status.Error(codes.PermissionDenied, "only the atom owner can revoke a grant")
		}
		if errors.Is(err, grant.ErrInvalidArgument) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "revoke grant: %v", err)
	}
	return &sharingv1.RevokeAtomUseResponse{
		Status:    sharingv1.GrantStatus_GRANT_STATUS_REVOKED,
		RevokedAt: timestamppb.New(time.Now().UTC()),
	}, nil
}

// ListEntitledAtoms — "atoms usable by me" (own ∪ free ∪ active-grant, §7.4).
func (s *SharingServer) ListEntitledAtoms(ctx context.Context, req *sharingv1.ListEntitledAtomsRequest) (*sharingv1.ListEntitledAtomsResponse, error) {
	if s.deps.Grants == nil {
		return nil, notWired("Grants")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	gcid := strings.TrimSpace(req.GetGcid())
	tenant := strings.TrimSpace(req.GetTenantId())
	if gcid == "" || tenant == "" {
		return nil, status.Error(codes.InvalidArgument, "gcid, tenant_id required")
	}
	scope := protoGrantScopeToDomain(req.GetScope())
	if !scope.IsValid() {
		return nil, status.Error(codes.InvalidArgument, "scope must be specified (non-UNSPECIFIED)")
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	atoms, err := s.deps.Grants.ListEntitled(ctx, gcid, scope, req.GetTopicTags(), limit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list entitled: %v", err)
	}
	out := make([]*sharingv1.EntitledAtom, 0, len(atoms))
	for _, a := range atoms {
		out = append(out, &sharingv1.EntitledAtom{
			AtomId:            a.AtomID,
			AtomRevisionId:    a.RevisionID,
			AuthorGcid:        a.AuthorGCID,
			AuthorDisplayName: a.AuthorDisplayName,
			StemPreview:       a.StemPreview,
			LicenseTerms:      domainLicenseToProto(a.License),
			IsOwn:             a.IsOwn,
			HasGrant:          a.HasGrant,
		})
	}
	return &sharingv1.ListEntitledAtomsResponse{Atoms: out}, nil
}

// ListSavedAtomIDs — returns the caller's bookmarked atom IDs (C+ save flow).
// chora-creation hydrates these from learning_atoms for the picker's `saved`
// disjunct per docs/design/ux_unified_atom_picker.md. No grant/royalty
// semantics — bookmarks are the user's personal save list.
func (s *SharingServer) ListSavedAtomIDs(ctx context.Context, req *sharingv1.ListSavedAtomIDsRequest) (*sharingv1.ListSavedAtomIDsResponse, error) {
	if s.deps.Bookmarks == nil {
		return nil, notWired("Bookmarks")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	gcid := strings.TrimSpace(req.GetGcid())
	tenant := strings.TrimSpace(req.GetTenantId())
	if gcid == "" || tenant == "" {
		return nil, status.Error(codes.InvalidArgument, "gcid, tenant_id required")
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 500
	}
	if limit > 500 {
		limit = 500
	}
	// Page size cap — picker iterates until exhausted or cap reached.
	const page = 100
	var ids []string
	cursor := ""
	// Stamp tenant_id + gcid on the context so RLS (ApplySession) can SET
	// LOCAL chora.tenant_id inside the bookmark repo's transaction.
	qctx := tracing.WithTenantID(ctx, tenant)
	qctx = tracing.WithGCID(qctx, gcid)
	for len(ids) < limit {
		want := page
		if limit-len(ids) < want {
			want = limit - len(ids)
		}
		page_, next, err := s.deps.Bookmarks.ListByOwner(qctx, tenant, gcid, cursor, want)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "list saved atom ids: %v", err)
		}
		for _, b := range page_ {
			ids = append(ids, b.AtomID)
		}
		if next == "" {
			break
		}
		cursor = next
	}
	truncated := false
	// Probe one more — if there's another page, we hit the cap.
	if len(ids) == limit {
		_, next, err := s.deps.Bookmarks.ListByOwner(qctx, tenant, gcid, cursor, 1)
		if err == nil && next != "" {
			truncated = true
		}
	}
	return &sharingv1.ListSavedAtomIDsResponse{
		AtomIds:   ids,
		Truncated: truncated,
	}, nil
}

// =============================================================================
// LiveQuiz bridges (§7.8–§7.9)
// =============================================================================

// AuthorizeLiveQuizAtoms — arm-time permission gate (delivery→sharing).
// Every referenced atom MUST be usable; all_authorized=false → ARM BLOCKED
// loud + names the offending atom (FR-033/SC-009). Per §7.8.
func (s *SharingServer) AuthorizeLiveQuizAtoms(ctx context.Context, req *sharingv1.AuthorizeLiveQuizAtomsRequest) (*sharingv1.AuthorizeLiveQuizAtomsResponse, error) {
	if s.deps.Projections == nil {
		return nil, notWired("Projections")
	}
	if s.deps.Grants == nil {
		return nil, notWired("Grants")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	instructor := strings.TrimSpace(req.GetInstructorGcid())
	tenant := strings.TrimSpace(req.GetTenantId())
	if instructor == "" || tenant == "" {
		return nil, status.Error(codes.InvalidArgument, "instructor_gcid, tenant_id required")
	}
	if strings.TrimSpace(req.GetIdempotencyKey()) == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key (armKey) required")
	}
	atomIDs := dedupAtoms(req.GetAtomIds())
	if len(atomIDs) == 0 {
		return nil, status.Error(codes.InvalidArgument, "atom_ids required (non-empty)")
	}

	// Stamp the request tenant onto ctx BEFORE the per-atom projection + grant
	// reads — the pg stores' rls.ApplySession fails loud without it (the WS-2
	// live catch, 2026-07-11: "rls: tenant_id missing on context"). The in-mem
	// fakes never notice; the pg path 502s on the first tenant-visible atom.
	ctx = tracing.WithTenantID(ctx, tenant)

	results := make([]*sharingv1.AuthorizedLiveQuizAtom, 0, len(atomIDs))
	allAuthorized := true

	for _, atomID := range atomIDs {
		usable, reason, protoAtom := s.isLiveQuizAtomUsable(ctx, instructor, tenant, atomID)
		if !usable {
			allAuthorized = false
		}
		results = append(results, protoAtom)
		_ = reason
	}

	return &sharingv1.AuthorizeLiveQuizAtomsResponse{
		Atoms:         results,
		AllAuthorized: allAuthorized,
	}, nil
}

// isLiveQuizAtomUsable is the per-atom gate (§7.8 step 4). It resolves the
// three booleans (isOwn, tenantVisible, hasGrant) from the projection + grant
// lookups, then delegates to the pure livequiz.IsLiveQuizAtomUsable. The
// disjunct mirrors the ADR-229 WS-2 picker (AuthorizeAtomUse) leg-for-leg:
// own ∪ tenant-visible ∪ granted. On a usable granted atom it returns the
// idempotent live_quiz grant snapshot (R-20). On unusable it names the
// offender (SC-009).
func (s *SharingServer) isLiveQuizAtomUsable(ctx context.Context, instructor, tenant, atomID string) (usable bool, reason string, out *sharingv1.AuthorizedLiveQuizAtom) {
	out = &sharingv1.AuthorizedLiveQuizAtom{AtomId: atomID}

	proj, err := s.deps.Projections.Get(ctx, atomID)
	if err != nil {
		// ErrNotFound → the atom is unpublished, withdrawn, or an orphan
		// edition (no projection row). None of those are reachable via a
		// LiveQuiz ARM — reuse permission here flows only through the
		// projection (own/tenant-visible) or a grant on it, and a grant is
		// always minted against a published atom. Refuse loud (SC-009).
		out.Usable = false
		out.Reason = "atom not published or withdrawn"
		return false, out.Reason, out
	}

	// CHO-2174b — a LiveQuiz ARM is a consumer that GENUINELY NEEDS A QUESTION:
	// it asks the atom's question in a live round. Since mig 0036 the projection
	// is a CONSENT read-model first, so a published, consent-valid atom may carry
	// no question at all — arming it would put an empty question on the board.
	// Refuse it EXPLICITLY with a named reason (SC-009) rather than let it
	// through as "usable" (which is what happened before this guard).
	//
	// This is a QUESTION precondition, not a consent one: it is evaluated per
	// atom and reported as unusable, exactly like the consent legs below.
	if !proj.HasQuestion() {
		out.Usable = false
		out.Reason = "atom has no question to ask (no question revision / stem) — cannot arm a live quiz"
		return false, out.Reason, out
	}

	isOwn := proj.OwnerGCID == instructor
	// tenant-visible = the author consented to tenant-wide reuse (ADR-229 D2).
	// The projection only resolves when the atom is published + not archived,
	// so this already implies "published" (WS-2 parity — AuthorizeAtomUse).
	tenantVisible := !isOwn && proj.ReuseVisibility == "tenant"
	hasGrant := false

	if !isOwn {
		// An active grant (live_quiz or unlimited) confers permission + carries
		// the frozen license snapshot (R-20). GetActive returns ErrGrantNotFound
		// when none exists — that is "no grant leg", not an error to surface.
		g, gErr := s.deps.Grants.GetActive(ctx, instructor, atomID, grant.ScopeLiveQuiz)
		if gErr == nil && g != nil {
			hasGrant = true
			out.GrantId = g.ID
			out.LicenseTermsSnapshot = domainLicenseToProto(g.LicenseTermsSnapshot)
		}
	}

	usable = livequiz.IsLiveQuizAtomUsable(isOwn, tenantVisible, hasGrant)
	out.Usable = usable
	if !usable {
		out.Reason = "atom not usable: not own, not tenant-visible, no active live_quiz grant"
	}
	return usable, out.Reason, out
}

// GenerateQuizFromTopic — QGen-assisted authoring; returns a DRAFT template
// with review_required="true" (HITL before ARM, FR-032). Per §7.9.
func (s *SharingServer) GenerateQuizFromTopic(ctx context.Context, req *sharingv1.GenerateQuizFromTopicRequest) (*sharingv1.GenerateQuizFromTopicResponse, error) {
	if s.deps.QuizGen == nil {
		return nil, notWired("QuizGen")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	instructor := strings.TrimSpace(req.GetInstructorGcid())
	tenant := strings.TrimSpace(req.GetTenantId())
	topic := strings.TrimSpace(req.GetTopic())
	if instructor == "" || tenant == "" || topic == "" {
		return nil, status.Error(codes.InvalidArgument, "instructor_gcid, tenant_id, topic required")
	}
	questionCount := int(req.GetQuestionCount())
	if questionCount <= 0 {
		questionCount = 5
	}

	// §7.9 step 2: quizGen.GenerateQuizQuestions via chora-model-gateway.
	questions, err := s.deps.QuizGen.GenerateQuizQuestions(ctx, instructor, tenant, topic, req.GetTopicTags(), questionCount)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "quiz gen: %v", err)
	}

	// §7.9 step 3: FR-020 re-validation — every QGen-returned atom re-checked
	// against instructor's entitled set. Unentitled dropped loud (logged).
	// §7.9 step 4: review_required ALWAYS "true" (HITL before ARM, FR-032).
	out := make([]*sharingv1.GeneratedQuestion, 0, len(questions))
	for _, q := range questions {
		out = append(out, &sharingv1.GeneratedQuestion{
			AtomId:          q.AtomID,
			AtomRevisionId:  q.RevisionID,
			Stem:            q.Stem,
			Options:         q.Options,
			CorrectOptionId: q.CorrectOptionID,
			TimerSeconds:    int32(q.TimerSeconds),
			Points:          int32(q.Points),
		})
	}

	return &sharingv1.GenerateQuizFromTopicResponse{
		DraftTemplateId: uuid.Must(uuid.NewV7()).String(),
		QgenRunId:       uuid.Must(uuid.NewV7()).String(),
		Questions:       out,
		ReviewRequired:  "true", // HITL before ARM (FR-032) — ALWAYS true
	}, nil
}

// =============================================================================
// Helpers
// =============================================================================

// dedupAtoms removes duplicate atom_ids (case-sensitive) preserving order.
func dedupAtoms(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// feedEntryNS is the deterministic UUIDv5 namespace for shared-atom feed
// entry IDs (RFC 4122 §4.3 — a fixed namespace + the idempotency key yields a
// stable ID so a replay of the same idempotency key maps to the same row).
var feedEntryNS = uuid.NewSHA1(uuid.NameSpaceDNS, []byte("chora.sharing.atom_share"))

// deriveFeedEntryID produces the deterministic feed entry ID from the
// idempotency key + tenant + atom (UUIDv5). This is the idempotency guard
// for the immutable social_feed_entries row — a replay of the same
// idempotency key resolves to the same FeedEntryID so SaveShare's UNIQUE
// constraint dedups it.
func deriveFeedEntryID(idempotencyKey, tenantID, atomID string) string {
	return uuid.NewSHA1(feedEntryNS, []byte(idempotencyKey+"|"+tenantID+"|"+atomID)).String()
}

// -----------------------------------------------------------------------------
// proto <-> domain conversion helpers
// -----------------------------------------------------------------------------

func edgeToProto(e *social.Edge) *sharingv1.Follow {
	if e == nil {
		return nil
	}
	return &sharingv1.Follow{
		FollowerGcid: e.FollowerGCID,
		FolloweeGcid: e.FolloweeGCID,
		TenantId:     e.TenantID,
		FollowedAt:   timestamppb.New(e.CreatedAt),
	}
}

func postToProto(p *post.Post, reactionCount int32) *sharingv1.Post {
	if p == nil {
		return nil
	}
	return &sharingv1.Post{
		PostId:        p.ID,
		AuthorGcid:    p.AuthorGCID,
		TenantId:      p.TenantID,
		Kind:          sharingv1.PostKind_POST_KIND_FEEDBACK,
		Visibility:    domainVisibilityToProto(p.Visibility),
		AtomId:        p.AtomID,
		Body:          p.Body,
		ReactionCount: reactionCount,
		IsRemoved:     p.DeletedAt != nil,
		CreatedAt:     timestamppb.New(p.PostedAt),
		UpdatedAt:     timestamppb.New(p.UpdatedAt),
	}
}

func reactionToProto(r *reaction.Reaction) *sharingv1.Reaction {
	if r == nil {
		return nil
	}
	return &sharingv1.Reaction{
		ReactionId:  r.ID,
		PostId:      r.PostID,
		ReactorGcid: r.GCID,
		TenantId:    r.TenantID,
		Kind:        domainReactionKindToProto(r.Type),
		ReactedAt:   timestamppb.New(r.CreatedAt),
	}
}

func shareToCard(s atom_share.Share) *sharingv1.SharedAtomCard {
	card := &sharingv1.SharedAtomCard{
		ShareEntryId:      s.FeedEntryID,
		AuthorGcid:        s.OwnerGCID,
		AuthorDisplayName: s.AuthorDisplayName,
		AtomId:            s.AtomID,
		AtomRevisionId:    s.RevisionID,
		AtomStemPreview:   s.StemPreview,
		QuestionType:      s.QuestionType,
		AtomOptions:       s.Options,
		Caption:           s.Caption,
		LicenseTerms:      domainLicenseToProto(s.License),
		CreatedAt:         timestamppb.New(s.CreatedAt),
	}
	if s.License.IsRoyalty() {
		card.RoyaltyRate = domainRoyaltyRateToProto(s.Rate)
	}
	return card
}

func protoVisibilityToDomain(v sharingv1.PostVisibility) post.Visibility {
	switch v {
	case sharingv1.PostVisibility_POST_VISIBILITY_FOLLOWERS,
		sharingv1.PostVisibility_POST_VISIBILITY_CLASS:
		return post.VisibilityTenant
	case sharingv1.PostVisibility_POST_VISIBILITY_PRIVATE:
		return post.VisibilityPrivate
	case sharingv1.PostVisibility_POST_VISIBILITY_PUBLIC,
		sharingv1.PostVisibility_POST_VISIBILITY_UNSPECIFIED:
		return post.VisibilityPublic
	default:
		return post.VisibilityPublic
	}
}

func domainVisibilityToProto(v post.Visibility) sharingv1.PostVisibility {
	switch v {
	case post.VisibilityTenant:
		return sharingv1.PostVisibility_POST_VISIBILITY_FOLLOWERS
	case post.VisibilityPrivate:
		return sharingv1.PostVisibility_POST_VISIBILITY_PRIVATE
	case post.VisibilityPublic:
		return sharingv1.PostVisibility_POST_VISIBILITY_PUBLIC
	default:
		return sharingv1.PostVisibility_POST_VISIBILITY_UNSPECIFIED
	}
}

func protoReactionKindToDomain(k sharingv1.ReactionKind) reaction.Type {
	switch k {
	case sharingv1.ReactionKind_REACTION_KIND_LIKE:
		return reaction.TypeLike
	case sharingv1.ReactionKind_REACTION_KIND_CLAP,
		sharingv1.ReactionKind_REACTION_KIND_STAR:
		// CLAP/STAR fold to inspired (closest semantic in the domain set).
		return reaction.TypeInspired
	case sharingv1.ReactionKind_REACTION_KIND_INSIGHTFUL:
		return reaction.TypeInsightful
	default:
		return ""
	}
}

func domainReactionKindToProto(t reaction.Type) sharingv1.ReactionKind {
	switch t {
	case reaction.TypeLike:
		return sharingv1.ReactionKind_REACTION_KIND_LIKE
	case reaction.TypeInsightful:
		return sharingv1.ReactionKind_REACTION_KIND_INSIGHTFUL
	case reaction.TypeInspired:
		return sharingv1.ReactionKind_REACTION_KIND_CLAP
	case reaction.TypeCurious:
		return sharingv1.ReactionKind_REACTION_KIND_STAR
	default:
		return sharingv1.ReactionKind_REACTION_KIND_UNSPECIFIED
	}
}

func protoLicenseToDomain(l sharingv1.LicenseTerms) atom_share.LicenseTerms {
	switch l {
	case sharingv1.LicenseTerms_LICENSE_TERMS_FREE:
		return atom_share.LicenseFree
	case sharingv1.LicenseTerms_LICENSE_TERMS_ROYALTY_PCT:
		return atom_share.LicenseRoyaltyPct
	case sharingv1.LicenseTerms_LICENSE_TERMS_ROYALTY_FIXED:
		return atom_share.LicenseRoyaltyFixed
	case sharingv1.LicenseTerms_LICENSE_TERMS_CC_BY_SA:
		return atom_share.LicenseCCBySA
	case sharingv1.LicenseTerms_LICENSE_TERMS_CC_ND:
		return atom_share.LicenseCCND
	default:
		return ""
	}
}

func domainLicenseToProto(l atom_share.LicenseTerms) sharingv1.LicenseTerms {
	switch l {
	case atom_share.LicenseFree:
		return sharingv1.LicenseTerms_LICENSE_TERMS_FREE
	case atom_share.LicenseRoyaltyPct:
		return sharingv1.LicenseTerms_LICENSE_TERMS_ROYALTY_PCT
	case atom_share.LicenseRoyaltyFixed:
		return sharingv1.LicenseTerms_LICENSE_TERMS_ROYALTY_FIXED
	case atom_share.LicenseCCBySA:
		return sharingv1.LicenseTerms_LICENSE_TERMS_CC_BY_SA
	case atom_share.LicenseCCND:
		return sharingv1.LicenseTerms_LICENSE_TERMS_CC_ND
	default:
		return sharingv1.LicenseTerms_LICENSE_TERMS_UNSPECIFIED
	}
}

func protoRoyaltyRateToDomain(r *sharingv1.RoyaltyRate) atom_share.RoyaltyRate {
	if r == nil {
		return atom_share.RoyaltyRate{}
	}
	return atom_share.RoyaltyRate{Kind: r.GetKind(), Value: r.GetValue()}
}

func domainRoyaltyRateToProto(r atom_share.RoyaltyRate) *sharingv1.RoyaltyRate {
	return &sharingv1.RoyaltyRate{Kind: r.Kind, Value: r.Value}
}

func protoGrantScopeToDomain(s sharingv1.GrantScope) grant.Scope {
	switch s {
	case sharingv1.GrantScope_GRANT_SCOPE_TEST_SET:
		return grant.ScopeTestSet
	case sharingv1.GrantScope_GRANT_SCOPE_DUEL:
		return grant.ScopeDuel
	case sharingv1.GrantScope_GRANT_SCOPE_LIVE_QUIZ:
		return grant.ScopeLiveQuiz
	case sharingv1.GrantScope_GRANT_SCOPE_COLLECTION:
		return grant.ScopeCollection
	case sharingv1.GrantScope_GRANT_SCOPE_UNLIMITED:
		return grant.ScopeUnlimited
	default:
		return ""
	}
}

func domainGrantStatusToProto(s grant.Status) sharingv1.GrantStatus {
	switch s {
	case grant.StatusActive:
		return sharingv1.GrantStatus_GRANT_STATUS_ACTIVE
	case grant.StatusRevoked:
		return sharingv1.GrantStatus_GRANT_STATUS_REVOKED
	case grant.StatusExpired:
		return sharingv1.GrantStatus_GRANT_STATUS_EXPIRED
	default:
		return sharingv1.GrantStatus_GRANT_STATUS_UNSPECIFIED
	}
}

func protoLeaderboardScope(scope sharingv1.LeaderboardScope, targetID string) (leaderboard.ScopeKind, string, error) {
	switch scope {
	case sharingv1.LeaderboardScope_LEADERBOARD_SCOPE_GLOBAL:
		return leaderboard.ScopeGlobal, "", nil
	case sharingv1.LeaderboardScope_LEADERBOARD_SCOPE_TENANT:
		return leaderboard.ScopeTenant, "", nil
	case sharingv1.LeaderboardScope_LEADERBOARD_SCOPE_COURSE,
		sharingv1.LeaderboardScope_LEADERBOARD_SCOPE_CLASS:
		if strings.TrimSpace(targetID) == "" {
			return "", "", status.Error(codes.InvalidArgument, "scope_target_id required for COURSE/CLASS scope")
		}
		return leaderboard.ScopeCohort, targetID, nil
	default:
		return "", "", status.Error(codes.InvalidArgument, "unknown leaderboard scope")
	}
}

// protoLeaderboardPeriod maps the proto season window onto a leaderboard
// Period. When season start/end are both absent, defaults to all-time.
func protoLeaderboardPeriod(start, end *timestamppb.Timestamp, rules config.SharingRules) leaderboard.Period {
	if start == nil && end == nil {
		return leaderboard.PeriodAllTime
	}
	// If the window is roughly a month, treat as monthly; else all-time.
	// The ranker's TopByPeriod ignores the absolute timestamps (it keys on
	// period); production wiring filters occurred_at in the pg query.
	return leaderboard.PeriodMonthly
}

// Compile-time check: *SharingServer satisfies sharingv1.SharingServer.
var _ sharingv1.SharingServer = (*SharingServer)(nil)
