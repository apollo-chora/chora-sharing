// Package httpadapter binds the §6 REST routes to chora-sharing's domain
// ports. It is the BFF-facing mirror of the gRPC SharingServer: identity
// headers (gcid + X-Tenant-Id) are gateway-stamped and validated at this
// boundary, Idempotency-Key is required on writes, and every write runs the
// Cloud Model Armor guardrail pre-flight (the gRPC surface skips that — its
// callers are internal services past the BFF).
//
// Greenfield rebuild of the Content Sharing domain (docs/chora-sharing.md).
// The Deps struct carries the SAME domain port interfaces as the gRPC
// SharingServerDeps; a nil port for an RPC's dependency returns 501 (fail-loud
// per §1.1 — never a fake success). The adapter delegates to domain logic;
// it owns only HTTP decoding, identity extraction, status-code mapping, and
// JSON shaping.
//
// Hexagonal layout: this adapter depends on the pure-domain packages (post,
// reaction, social, leaderboard, duel, grant, atom_share, atom_projection,
// comment, currency, livequiz) + the modelarmor GuardrailPort + the
// config SharingRules. Domain packages NEVER import from here.
// cmd/server/main.go is the sole composition root.
package httpadapter

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/adapter/modelarmor"
	"github.com/apollo-chora/chora-sharing/internal/adapter/subscribers"
	"github.com/apollo-chora/chora-sharing/internal/config"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
	"github.com/apollo-chora/chora-sharing/internal/domain/bookmark"
	"github.com/apollo-chora/chora-sharing/internal/domain/comment"
	"github.com/apollo-chora/chora-sharing/internal/domain/currency"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
	"github.com/apollo-chora/chora-sharing/internal/domain/grant"
	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
	"github.com/apollo-chora/chora-sharing/internal/domain/livequiz"
	"github.com/apollo-chora/chora-sharing/internal/domain/post"
	"github.com/apollo-chora/chora-sharing/internal/domain/profiler"
	"github.com/apollo-chora/chora-sharing/internal/domain/reaction"
	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

// =============================================================================
// Port wrappers for the social-surface inmem adapters.
//
// Mirrors the gRPC SharingServerDeps: the social REST routes (reactions,
// comments, leaderboard) consume the inmem adapters. These narrow interfaces
// capture exactly the methods the handlers call, keeping the Deps struct
// interface-typed so tests inject fakes + production swaps to pg without
// touching this file.
// =============================================================================

// PostStore is the social-feed post write+read port used by ReactToPost +
// comment (existence check).
// PostStore removed (CHO-2193/W0-F1). The port now lives in the domain as
// post.PostRepo, alongside comment.CommentRepo / bookmark.BookmarkRepo /
// atom_share.ShareRepo. It was declared HERE and again in the grpc adapter —
// two copies of one port, which is how ReactionStore silently drifted (grpc's
// copy is missing UnreactByID). One port, in the domain, like every other
// aggregate in this service.

// ReactionStore is the reaction idempotent-react + unreact + list port.
// Unreact removes by (gcid, postID, type) — used by the toggle path.
// UnreactByID is the ownership-checked removal backing
// DELETE /v1/posts/{post_id}/reactions/{reaction_id}.
// ReactionStore removed (CHO-2193/W0-F1) — the port is now reaction.ReactionRepo
// in the domain. There were TWO copies of this interface (here and in the grpc
// adapter) and they had ALREADY DRIFTED: grpc's copy is missing UnreactByID.
// That is what a duplicated port does. One port, in the domain.

// SocialGraph is the follow-edge port. Follow/Unfollow/Block/Unblock mutate
// edges; FollowingGCIDs/FollowersGCIDs/BlockedBy list them for the feed scope
// + connections page. pg-backed + durable in production (ADR-229 WS-0 — the
// old in-memory graph lost runtime edges on restart and mixed tenants on
// reads); tenant-aware in-memory in dev. ctx carries cancellation; tenantID
// scopes the RLS session on every call.
type SocialGraph interface {
	Follow(ctx context.Context, tenantID, follower, followee string) (*social.Edge, bool, error)
	Unfollow(ctx context.Context, tenantID, follower, followee string) (bool, error)
	FollowingGCIDs(ctx context.Context, tenantID, gcid string) ([]string, error)
	FollowersGCIDs(ctx context.Context, tenantID, gcid string) ([]string, error)
	Block(ctx context.Context, tenantID, blocker, blocked string) error
	Unblock(ctx context.Context, tenantID, blocker, blocked string) (bool, error)
	BlockedBy(ctx context.Context, tenantID, gcid string) ([]string, error)
}

// CommentStore is the comment create + list + update + delete port.
type CommentStore interface {
	Create(ctx context.Context, c *comment.Comment) error
	ListByPost(ctx context.Context, postID string, limit int, cursor string) ([]comment.Comment, string, error)
	Update(ctx context.Context, commentID, authorGCID, body string) error
	Delete(ctx context.Context, commentID, authorGCID string) error
}

// BookmarkStore is the atom bookmark save/delete/list port.
type BookmarkStore interface {
	Save(ctx context.Context, b *bookmark.Bookmark) error
	Delete(ctx context.Context, tenantID, gcid, atomID string) error
	ListByOwner(ctx context.Context, tenantID, gcid, cursor string, limit int) ([]bookmark.Bookmark, string, error)
}

// ConnectionStore persists social connections (follows + blocks) to the DB.
// The in-memory SocialGraph handles fast read paths (feed filtering); this
// port ensures all mutations survive service restarts.
type ConnectionStore interface {
	SaveFollow(ctx context.Context, tenantID, follower, followee string) error
	DeleteFollow(ctx context.Context, follower, followee string) error
	SaveBlock(ctx context.Context, tenantID, blocker, blocked string) error
	DeleteBlock(ctx context.Context, blocker, blocked string) error
}

// CurrencyStore is the author non-cash credit port (kept for parity; not on
// a §6 REST route today).
type CurrencyStore interface {
	CreditAuthor(ctx context.Context, tenantID, holderGCID string, c currency.Currency, amount float64) error
	GetBalance(ctx context.Context, tenantID, holderGCID, c string) (float64, error)
}

// =============================================================================
// Deps + Handler
// =============================================================================

// Deps wires every collaborator the §6 REST routes need. Fields are domain
// port interfaces (or the narrow social wrappers above) — the SAME types the
// gRPC SharingServerDeps carries, so one wiring at the composition root feeds
// both transports. A nil port for a given route's dependency makes that route
// return 501 — fail-loud per §1.1 (never a fake success).
type Deps struct {
	Shares           atom_share.ShareRepo
	Projections      atom_projection.AtomProjectionReader
	Grants           grant.GrantRepo
	Royalties        grant.RoyaltyRepo
	Duels            DuelStore
	Leaderboards     leaderboard.LeaderboardReader
	Posts            post.PostRepo
	Reactions        reaction.ReactionRepo
	Graph            SocialGraph
	Suggestions      social.SuggestionQueries
	Comments         CommentStore
	Currency         CurrencyStore
	Bookmarks        BookmarkStore
	Connections      ConnectionStore
	QuizGen          livequiz.QuizGenerator
	Guardrail        modelarmor.GuardrailPort
	Rules            config.SharingRules
	WSHandler        http.Handler
	Profiles         ProfileStore
	Conjurer         ConjurerPort
	DisplayNameResolver DisplayNameResolver
	DuelEvents       DuelEventPublisher
	Matchmaker       MatchmakerPort
	MatchmakingQueue MatchmakingQueueRepo
	AtomSelector     AtomSelectorPort
	ProfileBroker    ProfileMessagePublisher
	ProfileWSHandler http.Handler
	// RoundSweeperRegistrar registers a tenant with the WS3 round-timer
	// sweeper when a duel transitions to in_progress (StartBattle). The
	// sweeper polls per-tenant (RLS) so it must know which tenants have
	// active duels. nil in dev (no sweeper wired) — the sweep just doesn't
	// run, same as before.
	RoundSweeperRegistrar RoundSweeperRegistrar

	// Familiar-milestone drafts + share preference (CHO-2258). Ports declared
	// in milestone_drafts_handlers.go; both satisfied by the pg milestone
	// stores that already back the subscriber. nil in the in-memory dev
	// fallback -> those routes 501 (the Friendships convention above).
	MilestoneDrafts MilestoneDrafts
	MilestonePrefs  MilestonePrefs
	// MilestoneDefaultPolicy is the policy in force for a learner with no
	// user_preferences row. The composition root feeds the SAME value here and
	// to FamiliarMilestoneSubscriber.Config.DefaultPolicy: the read API must
	// report the policy the subscriber actually applies, and two independently
	// hardcoded defaults would drift silently. Empty -> subscribers.PolicyDraft.
	MilestoneDefaultPolicy subscribers.Policy

	// Event-push inboxes. chora-gateway resolves the owning service from its
	// routing table and forwards /api/internal/pubsub/{inbox} VERBATIM, so each
	// of these must be mounted at exactly inboxPath(inbox).
	//
	// A nil handler leaves its route UNREGISTERED (404) rather than answering
	// 200 — a 200 from an unwired route would ack the message and discard the
	// event permanently. But an unregistered route is itself a silent killer
	// (the broker retries then dead-letters, and nothing logs it), so main()
	// refuses to boot when any assigned inbox is left unmounted. See
	// (*Handler).MissingInboxes.
	//
	// Build with NewCoursePublishedPushHandler / NewWeaknessGrownPushHandler /
	// NewLiveQuizScorePushHandler.
	CoursePublishedPushHandler http.Handler // chora.delivery.course.published.v1 → C+ discovery feed
	WeaknessGrownPushHandler   http.Handler // chora.consumption.weakness.grown.v1 → leaderboard XP (ADR-196 B3)
	LiveQuizScorePushHandler   http.Handler // chora.delivery.live_quiz_session.score_awarded.v1 → leaderboard.Ranker (ADR-168 #8)
}

// Inbox names owned by chora-sharing. These are the gateway's routing keys;
// the mounted path is inboxPathPrefix + inbox.
const (
	InboxCoursePublished = "course-published"
	InboxWeaknessGrown   = "weakness-grown"
	InboxLiveQuizScores  = "live-quiz-scores"
)

// inboxPathPrefix is the canonical event-push route prefix. The gateway
// forwards the full path unchanged, so downstreams mount exactly
// inboxPathPrefix + inbox.
const inboxPathPrefix = "/api/internal/pubsub/"

// SharingInboxes is every inbox assigned to chora-sharing. main() refuses to
// boot when any of these is not mounted.
var SharingInboxes = []string{InboxCoursePublished, InboxWeaknessGrown, InboxLiveQuizScores}

// InboxPath returns the full mount path for inbox.
func InboxPath(inbox string) string { return inboxPathPrefix + inbox }

// mountInbox mounts an event-push route and records it in a single step, so
// the record cannot disagree with the mux.
//
// A nil handler is NOT mounted: answering 200 from a route with nothing behind
// it would ack the message and destroy the event. The route is left absent, and
// MissingInboxes below turns that absence into a boot failure.
func (h *Handler) mountInbox(inbox string, handler http.Handler) {
	if handler == nil {
		return
	}
	h.mux.Handle(InboxPath(inbox), handler)
	h.mounted[inbox] = struct{}{}
}

// MissingInboxes names every inbox assigned to chora-sharing that this Handler
// does not actually serve.
//
// main() refuses to boot when it is non-empty. That is deliberate and it is the
// whole guard: a service that fails to mount an inbox the gateway routes to it
// 404s every message, the broker retries five times and dead-letters, and
// NOTHING anywhere reports it — which is how CHO-2195 ran for over two weeks
// behind a green test suite. A pod that will not start is loud. A pod that
// quietly drops every event on a lane is not.
func (h *Handler) MissingInboxes() []string {
	var missing []string
	for _, inbox := range SharingInboxes {
		if _, ok := h.mounted[inbox]; !ok {
			missing = append(missing, inbox)
		}
	}
	return missing
}

type ProfileStore interface {
	SaveProfile(ctx context.Context, p *profiler.Profile) error
	GetProfile(ctx context.Context, gcid string) (*profiler.Profile, error)
	// ResolveDisplayNames batch-resolves display_name for a set of gcids
	// from profiler_profiles. Returns a map gcid→display_name; gcids with
	// no profile or empty display_name are absent. Used by the leaderboard
	// handler to enrich entries with human-readable names.
	ResolveDisplayNames(ctx context.Context, gcids []string) (map[string]string, error)
}

// DisplayNameResolver resolves a GCID to a human-readable display name via
// chora-identity's GetMe gRPC RPC. Used by the profiler generate handler to
// populate profiler_profiles.display_name at generation time. Nil when
// identity gRPC is unwired (dev/tests — display_name stays empty, FE falls
// back to shortGcid()).
type DisplayNameResolver interface {
	ResolveDisplayName(ctx context.Context, gcid string) (string, error)
}

// ConjurerPort conjures a user's interest tags (from free-text bio) AND
// proficiency band (from completed course titles) via the interest_profiler
// ADK agent. Replaces the old TagExtractorPort (tags-only, static/LLM).
// The conjured profile feeds profiler_profiles + duel matchmaking.
type ConjurerPort interface {
	Conjure(ctx context.Context, tenantID, gcid, bio string, courseTitles []string) (*profiler.ConjuredProfile, error)
}

// ProfileMessageKind matches ws.ProfileMessageKind — string-typed here so
// the http package need not import the ws adapter (hexagonal boundary:
// the handler publishes via the narrow ProfileMessagePublisher port, the
// ws adapter owns the concrete broker + WS frame shape).
type ProfileMessageKind string

const (
	// ProfileKindReady — payload is the profilerResponse JSON.
	ProfileKindReady ProfileMessageKind = "profile_ready"
	// ProfileKindError — payload is {"error": "<message>"}.
	ProfileKindError ProfileMessageKind = "profile_error"
)

// ProfileMessagePublisher fans a profile-generation completion frame out
// to the user's /v1/me/profile/ws subscriber. Implemented by
// *ws.ProfileBroker; the async generateProfile goroutine publishes after
// the Conjure call resolves.
type ProfileMessagePublisher interface {
	PublishProfile(gcid string, kind ProfileMessageKind, payload []byte)
}

// DuelStore is the persistence port for duels.
type DuelStore interface {
	SaveDuel(ctx context.Context, d *duel.Duel) error
	GetDuel(ctx context.Context, duelID string) (*duel.Duel, error)
	GetDuelForUpdate(ctx context.Context, duelID string) (*duel.Duel, error)
	ListDuels(ctx context.Context, tenantID, gcid, status string, limit int, cursor string) ([]*duel.Duel, string, error)
	ResolveRound(ctx context.Context, d *duel.Duel, roundNo int, gcid string, res duel.RoundResolution) error
	ApplyELO(ctx context.Context, d *duel.Duel, kFactor int) error
	GetRating(ctx context.Context, tenantID, gcid string) (int, error)
	GetRatingStats(ctx context.Context, tenantID, gcid string) (duel.RatingStats, error)
	TopRatings(ctx context.Context, tenantID string, limit int) ([]duel.RatingStats, error)
	// WS1: per-category rating methods.
	GetRatingForCategory(ctx context.Context, tenantID, gcid, category string) (int, error)
	GetRatingStatsForCategory(ctx context.Context, tenantID, gcid, category string) (duel.RatingStats, error)
	TopRatingsForCategory(ctx context.Context, tenantID, category string, limit int) ([]duel.RatingStats, error)
}

// DuelEventPublisher emits the duel.completed.v1 event. Implemented by
// the events.Publisher adapter.
type DuelEventPublisher interface {
	PublishDuelCompleted(ctx context.Context, scope, duelID, tenantID, challengerGCID, opponentGCID, winnerGCID string, scoreChallenger, scoreOpponent int) error
}

// Handler is the http.Handler serving every §6 REST route. Construction is
// via NewHandler; the bootstrap injects Deps. Health endpoints (/healthz,
// /readyz) are always wired. The WebSocket route delegates to deps.WSHandler
// when set (else 501).
type Handler struct {
	deps Deps
	mux  *http.ServeMux
	// mounted records which Pub/Sub inboxes actually reached the mux. Written
	// only by mountInbox, in the same statement as the mux.Handle call, so the
	// record cannot drift from the routing table — a second, independently
	// maintained copy of a routing fact is precisely what caused CHO-2195.
	mounted map[string]struct{}
	// pendingMatches holds in-flight matchmaking pairings between the
	// Matchmaker pairing two searchers and the duel-creation consumer
	// committing them.
	pendingMatches pendingMatchStore
}

func NewHandler(deps Deps) *Handler {
	mux := http.NewServeMux()

	// Health endpoints — no identity required (kubelet probes).
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, healthResp{Status: "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, healthResp{Status: "ready"})
	})

	h := &Handler{deps: deps, mux: mux, mounted: make(map[string]struct{}, 3), pendingMatches: pendingMatchStore{matches: make(map[string]string)}}

	// Start the background match consumer that creates duels when the
	// Matchmaker pairs two searchers.
	h.startMatchConsumer()

	// Event-push inboxes. The gateway forwards the FULL
	// /api/internal/pubsub/{inbox} path (it picks a base URL, it does not
	// rewrite), so the mounted path must match inboxPath() exactly. No
	// requireIdentity: the caller is the broker, authenticated by the bearer
	// token each push handler's Verifier checks.
	//
	// These three ARE the CHO-2195 regression. The greenfield rewrite (470ec0ef9)
	// deleted the push wiring; for two weeks the gateway routed weakness-grown and
	// live-quiz-scores here to a 404 while course-published — the one route that
	// was mounted — was one the gateway never routed. Perfectly inverted, and
	// entirely silent.
	h.mountInbox(InboxCoursePublished, deps.CoursePublishedPushHandler)
	h.mountInbox(InboxWeaknessGrown, deps.WeaknessGrownPushHandler)
	h.mountInbox(InboxLiveQuizScores, deps.LiveQuizScorePushHandler)

	// §6 REST routes — each wrapped with requireIdentity (gcid +
	// X-Tenant-Id required on every /v1 route; missing → 400 per §6).
	mux.HandleFunc("POST /v1/atoms/{atom_id}/share", requireIdentity(h.shareAtom))
	mux.HandleFunc("DELETE /v1/atoms/{atom_id}/share", requireIdentity(h.revokeShare))
	mux.HandleFunc("POST /v1/atoms/{atom_id}/bookmark", requireIdentity(h.bookmarkAtom))
	mux.HandleFunc("DELETE /v1/atoms/{atom_id}/bookmark", requireIdentity(h.unbookmarkAtom))
	mux.HandleFunc("GET /v1/feed/shared-atoms", requireIdentity(h.listSharedAtoms))
	mux.HandleFunc("GET /v1/connections", requireIdentity(h.listConnections))
	// Relationship writes + pending reads (ADR-230 B-lite.2). Relationship
	// ops are naturally idempotent per pair (duplicate follow / re-block /
	// repeat delete are no-ops in the aggregate), so no Idempotency-Key is
	// required — unlike content-bearing writes. No guardrail pre-flight
	// either: the payloads carry GCIDs only, never user content.
	mux.HandleFunc("POST /v1/connections/follows", requireIdentity(h.followMember))
	mux.HandleFunc("DELETE /v1/connections/follows/{gcid}", requireIdentity(h.unfollowMember))
	mux.HandleFunc("POST /v1/connections/blocks", requireIdentity(h.blockMember))
	mux.HandleFunc("DELETE /v1/connections/blocks/{gcid}", requireIdentity(h.unblockMember))
	mux.HandleFunc("GET /v1/connections/suggestions", requireIdentity(h.listFriendSuggestions))
	mux.HandleFunc("GET /v1/me/profile", requireIdentity(h.getProfile))
	mux.HandleFunc("GET /v1/me/profile/ws", requireIdentity(h.profileWS))
	mux.HandleFunc("POST /v1/me/profile/generate", requireIdentity(h.generateProfile))
	mux.HandleFunc("PUT /v1/me/profile/tags", requireIdentity(h.updateProfileTags))
	mux.HandleFunc("GET /v1/me/bookmarks", requireIdentity(h.listBookmarks))
	// Familiar-milestone drafts + share preference (CHO-2258). These three
	// paths are ALREADY allowlisted in the ns/sharing Istio AuthorizationPolicy
	// (live-verified 2026-07-17) — that allowlist is a strict per-path match, so
	// these registrations must keep matching it byte-for-byte. Publish/discard
	// are POST rather than PUT/DELETE because Cloud Armor rule 1005 denies those
	// methods at the edge; see milestone_drafts_handlers.go's header.
	mux.HandleFunc("GET /v1/me/post-drafts", requireIdentity(h.listPostDrafts))
	mux.HandleFunc("POST /v1/me/post-drafts/{draft_id}/publish", requireIdentity(h.publishPostDraft))
	mux.HandleFunc("POST /v1/me/post-drafts/{draft_id}/discard", requireIdentity(h.discardPostDraft))
	mux.HandleFunc("GET /v1/me/preferences/familiar-milestone-share", requireIdentity(h.getSharePref))
	mux.HandleFunc("POST /v1/me/preferences/familiar-milestone-share", requireIdentity(h.setSharePref))
	mux.HandleFunc("POST /v1/posts/{post_id}/reactions", requireIdentity(h.reactToPost))
	mux.HandleFunc("DELETE /v1/posts/{post_id}/reactions/{reaction_id}", requireIdentity(h.removeReaction))
	mux.HandleFunc("POST /v1/posts/{post_id}/comments", requireIdentity(h.createComment))
	mux.HandleFunc("GET /v1/posts/{post_id}/comments", requireIdentity(h.listComments))
	mux.HandleFunc("PATCH /v1/posts/{post_id}/comments/{comment_id}", requireIdentity(h.updateComment))
	mux.HandleFunc("DELETE /v1/posts/{post_id}/comments/{comment_id}", requireIdentity(h.deleteComment))
	mux.HandleFunc("GET /v1/duels", requireIdentity(h.listDuels))
	mux.HandleFunc("GET /v1/duels/{duel_id}/ws", requireIdentity(h.duelWS))
	mux.HandleFunc("POST /v1/duels/{duel_id}/answer", requireIdentity(h.submitDuelAnswer))
	mux.HandleFunc("GET /v1/duels/my-rating", requireIdentity(h.getMyRating))
	mux.HandleFunc("GET /v1/duels/leaderboard", requireIdentity(h.getDuelLeaderboard))
	mux.HandleFunc("GET /v1/duels/{duel_id}", requireIdentity(h.getDuel))
	mux.HandleFunc("POST /v1/duels/queue", requireIdentity(h.enterQueue))
	mux.HandleFunc("DELETE /v1/duels/queue", requireIdentity(h.cancelQueue))
	mux.HandleFunc("POST /v1/duels/queue/heartbeat", requireIdentity(h.heartbeatQueue))
	mux.HandleFunc("GET /v1/duels/queue/status", requireIdentity(h.queueStatus))
	mux.HandleFunc("GET /v1/leaderboard", requireIdentity(h.getLeaderboard))

	return h
}

// ServeHTTP dispatches to the registered mux, then logs any server-side fault.
//
// The log is the ONLY durable record of a 5xx: the error detail travels in the
// response body, and the BFF discards that body when it normalises 500 -> 502.
// Without this, a fault is invisible the moment it leaves the process — C+
// bookmarks 500'd on every request for 15 days without emitting a line
// (CHO-2177). Faults only; success stays quiet so health probes cannot bury it.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	h.mux.ServeHTTP(rec, r)
	if rec.status >= http.StatusInternalServerError {
		log.Printf("sharing: %s %s -> %d: %s",
			r.Method, r.URL.Path, rec.status, bytes.TrimSpace(rec.body))
	}
}

// maxLoggedFaultBody caps how much of a fault body reaches the log — enough for
// the SQLSTATE and the failing relation, not enough to flood on a large payload.
const maxLoggedFaultBody = 512

// statusRecorder wraps http.ResponseWriter to capture the status code and a
// bounded copy of a fault body, so ServeHTTP can log what actually went wrong.
// Only 5xx bodies are retained; nothing else is buffered.
//
// The duel WS route (/v1/duels/{duel_id}/ws) upgrades via Hijacker, so the
// wrapper MUST forward Hijack (and Flush for streaming handlers) to the
// underlying ResponseWriter — otherwise the websocket handshake panics with
// "statusRecorder is not http.Hijacker".
type statusRecorder struct {
	http.ResponseWriter
	status int
	body   []byte
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status >= http.StatusInternalServerError && len(s.body) < maxLoggedFaultBody {
		s.body = append(s.body, b[:min(len(b), maxLoggedFaultBody-len(s.body))]...)
	}
	return s.ResponseWriter.Write(b)
}

// Hijack forwards the hijack to the underlying ResponseWriter so the duel
// WS route can upgrade the connection through this wrapper.
func (s *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := s.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("statusRecorder: wrapped ResponseWriter is not http.Hijacker")
	}
	return hj.Hijack()
}

// Flush forwards flushing for streaming handlers.
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// =============================================================================
// Identity middleware
// =============================================================================

// requireIdentity wraps a handler so the gateway-stamped identity headers
// `gcid` (lowercase per §2.3) + `X-Tenant-Id` are required. Missing → 400
// (§6: "missing → 400"). On success the values are injected into the request
// context via the canonical tracing.WithTenantID / tracing.WithGCID so the
// RLS layer (rls.ApplySession) and downstream repo code can read them
// through tracing.TenantIDFromContext / tracing.GCIDFromContext. Health
// endpoints bypass this (registered unwrapped above).
func requireIdentity(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		gcid := strings.TrimSpace(r.Header.Get("gcid"))
		tenant := strings.TrimSpace(r.Header.Get("X-Tenant-Id"))
		if gcid == "" || tenant == "" {
			writeJSON(w, http.StatusBadRequest, errResp{
				Error: "gcid + X-Tenant-Id headers required",
			})
			return
		}
		ctx := tracing.WithTenantID(r.Context(), tenant)
		ctx = tracing.WithGCID(ctx, gcid)
		r = r.WithContext(ctx)
		next(w, r)
	}
}

// gcidFrom extracts the gateway-stamped caller GCID from the request context.
func gcidFrom(r *http.Request) string {
	return tracing.GCIDFromContext(r.Context())
}

// tenantFrom extracts the gateway-stamped tenant ID from the request context.
func tenantFrom(r *http.Request) string {
	return tracing.TenantIDFromContext(r.Context())
}

// idempotencyKey extracts the Idempotency-Key header, returning "" when
// absent (callers enforce presence on writes).
func idempotencyKey(r *http.Request) string {
	return strings.TrimSpace(r.Header.Get("Idempotency-Key"))
}

// pathParam extracts a named path segment from the request URL.
func pathParam(r *http.Request, name string) string {
	return r.PathValue(name)
}

// =============================================================================
// HTTP helpers
// =============================================================================

// errResp is the canonical JSON error envelope: {"error":"..."}.
type errResp struct {
	Error string `json:"error"`
}

// healthResp is the health-endpoint payload.
type healthResp struct {
	Status string `json:"status"`
}

// writeJSON encodes v as JSON with the given status code. A marshal failure
// (impossible for our DTOs) falls back to a plain-text 500.
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Best-effort; the header is already sent.
		_, _ = w.Write([]byte(`{"error":"internal"}`))
	}
}

// writeErr is shorthand for writeJSON with an errResp.
func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, errResp{Error: msg})
}

// readJSON decodes the request body into v. An empty or malformed body → 400.
func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if r.Body == nil || r.ContentLength == 0 {
		writeErr(w, http.StatusBadRequest, "request body required")
		return false
	}
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "malformed JSON body")
		return false
	}
	return true
}

// pathUUID reads a path parameter that MUST be a UUID — any caller-supplied id
// that becomes a `uuid`-typed column in a query (an atom_id, a feed-entry id).
// It returns the trimmed value and true on success; on an empty or malformed
// value it writes a 400 and returns "", false.
//
// This is the boundary that stops Postgres from being our input validator.
// Without it a caller typo ("banana") flows straight into a `WHERE … = $1`
// against a uuid column, dies with SQLSTATE 22P02, and reaches the caller as a
// 500 — paging the on-call for a client mistake, and pasting a raw driver error
// into the body. 400 is the honest verdict: it is THEIR fault and a real id
// fixes it. It is deliberately NOT 404 — a 404 claims we looked and found
// nothing, but a malformed id is unsearchable; there was nothing well-formed to
// look for. Only the parse-failure path becomes 400; a genuine repository fault
// on a well-formed id still returns 500 and still alerts.
func pathUUID(w http.ResponseWriter, r *http.Request, name string) (string, bool) {
	v := strings.TrimSpace(r.PathValue(name))
	if v == "" {
		writeErr(w, http.StatusBadRequest, name+" path segment required")
		return "", false
	}
	if _, err := uuid.Parse(v); err != nil {
		writeErr(w, http.StatusBadRequest, name+" must be a valid UUID")
		return "", false
	}
	return v, true
}

// parseLimit clamps a query limit: default def when <=0, hard cap maxN.
func parseLimit(q string, def, maxN int) int {
	n, err := strconv.Atoi(q)
	if err != nil || n <= 0 {
		return def
	}
	if n > maxN {
		return maxN
	}
	return n
}

// notWiredMsg returns the 501 message naming the missing dep (fail-loud).
func notWiredMsg(dep string) string {
	return "dependency " + dep + " not wired (fail-loud per §1.1)"
}

// =============================================================================
// Shared request/response DTOs + port-interface conversion helpers
// =============================================================================

// --- License + royalty (mirror proto field names for BFF parity) ---

type royaltyRateJSON struct {
	Kind  string  `json:"kind,omitempty"`
	Value float64 `json:"value,omitempty"`
}

func protoRoyaltyRateToDomain(r *royaltyRateJSON) atom_share.RoyaltyRate {
	if r == nil {
		return atom_share.RoyaltyRate{}
	}
	return atom_share.RoyaltyRate{Kind: r.Kind, Value: r.Value}
}

func domainRoyaltyRateToJSON(r atom_share.RoyaltyRate) *royaltyRateJSON {
	return &royaltyRateJSON{Kind: r.Kind, Value: r.Value}
}

// licenseStringToDomain maps the wire license_terms label (proto UPPER_CASE
// or lowercase snake — both accepted at the BFF seam) to the domain
// LicenseTerms. Empty/unspecified → "" (caller rejects).
func licenseStringToDomain(s string) atom_share.LicenseTerms {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "free", "license_terms_free":
		return atom_share.LicenseFree
	case "royalty_pct", "license_terms_royalty_pct":
		return atom_share.LicenseRoyaltyPct
	case "royalty_fixed", "license_terms_royalty_fixed":
		return atom_share.LicenseRoyaltyFixed
	case "cc_by_sa", "license_terms_cc_by_sa":
		return atom_share.LicenseCCBySA
	case "cc_nd", "license_terms_cc_nd":
		return atom_share.LicenseCCND
	default:
		return ""
	}
}

// domainLicenseToString returns the lowercase snake_case wire label for a
// domain LicenseTerms (the DB enum label — BFF parity with gRPC JSON).
func domainLicenseToString(l atom_share.LicenseTerms) string {
	return string(l)
}

// reactionKindStringToDomain maps the wire reaction kind label to the domain
// reaction.Type. Accepts proto UPPER_CASE + lowercase. Empty → "" (rejected).
func reactionKindStringToDomain(s string) reaction.Type {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "like", "reaction_kind_like":
		return reaction.TypeLike
	case "inspired", "clap", "star", "reaction_kind_clap", "reaction_kind_star":
		return reaction.TypeInspired
	case "insightful", "reaction_kind_insightful":
		return reaction.TypeInsightful
	case "curious", "reaction_kind_curious":
		return reaction.TypeCurious
	default:
		return ""
	}
}

// domainReactionKindToString returns the lowercase wire label for a domain
// reaction.Type.
func domainReactionKindToString(t reaction.Type) string {
	return string(t)
}

// =============================================================================
// Feed-entry ID derivation (shared with gRPC adapter)

// leaderboardScopeFromQuery maps the ?scope= query value onto a
// leaderboard.ScopeKind + scopeID. Accepts global / tenant / class / course
// (class + course map to cohort). Empty defaults to global.
func leaderboardScopeFromQuery(scope, targetID string) (leaderboard.ScopeKind, string) {
	switch strings.ToLower(strings.TrimSpace(scope)) {
	case "", "global":
		return leaderboard.ScopeGlobal, ""
	case "tenant":
		return leaderboard.ScopeTenant, ""
	case "class", "course", "cohort":
		return leaderboard.ScopeCohort, targetID
	default:
		return leaderboard.ScopeGlobal, ""
	}
}

// leaderboardMetricFromQuery maps the ?metric= query value. Defaults to xp.
func leaderboardMetricFromQuery(m string) string {
	switch strings.ToLower(strings.TrimSpace(m)) {
	case "duel_wins", "duel_elo", "reputation", "streak_days":
		return strings.ToLower(strings.TrimSpace(m))
	default:
		return "xp"
	}
}

// leaderboardPeriodFromQuery maps the ?period= query value onto a
// leaderboard.Period. Empty defaults to all-time.
func leaderboardPeriodFromQuery(p string) leaderboard.Period {
	per, err := leaderboard.ParsePeriod(strings.ToLower(strings.TrimSpace(p)))
	if err != nil {
		return leaderboard.PeriodAllTime
	}
	return per
}

// =============================================================================
// Feed-entry ID derivation (shared with gRPC adapter)
// =============================================================================

// feedEntryNS is the deterministic UUIDv5 namespace for shared-atom feed
// entry IDs — identical to the gRPC adapter's so both transports derive the
// SAME feed entry id for a given (idempotency_key, tenant, atom) triple.
var feedEntryNS = uuid.NewSHA1(uuid.NameSpaceDNS, []byte("chora.sharing.atom_share"))

// deriveFeedEntryID produces the deterministic feed entry ID from the
// idempotency key + tenant + atom (UUIDv5). Replay of the same key resolves
// to the same FeedEntryID so SaveShare's UNIQUE constraint dedups it.
func deriveFeedEntryID(idempotencyKey, tenantID, atomID string) string {
	return uuid.NewSHA1(feedEntryNS, []byte(idempotencyKey+"|"+tenantID+"|"+atomID)).String()
}

// nowUTC returns the current UTC time. Extracted as a helper so tests can
// reason about timestamps without importing time directly at call sites.
func nowUTC() time.Time { return time.Now().UTC() }

// Compile-time check: *Handler satisfies http.Handler.
var _ http.Handler = (*Handler)(nil)
