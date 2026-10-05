// Package main wires the chora-sharing Go service.
//
// Per CLAUDE.md §6 the production stack is OTLP-everywhere via
// OTEL_EXPORTER_OTLP_ENDPOINT. This skeleton emits structured log lines with
// W3C trace context propagation; the full OTel SDK lands alongside the
// Postgres + NATS JetStream adapters.
//
// Hexagonal layout assembled here:
//
//	internal/domain/post          (pure domain — Post aggregate)
//	internal/domain/reaction      (pure domain — Reaction + Registry)
//	internal/domain/social        (pure domain — SocialGraph + Edge)
//	internal/domain/duel          (pure domain — Duel + state machine; post-MVP)
//	internal/domain/leaderboard   (pure domain — Leaderboard ranker; post-MVP)
//	internal/domain/discovery     (pure domain — discovery feed projections)
//	internal/adapter/inmem        (in-memory repos)
//	internal/adapter/http         (HTTP/JSON adapter)
//	internal/adapter/events       (event publisher adapter — chora-common)
//	internal/observability        (log + traceparent shim)
//
// Event-bus wiring (S4.4 — A-Content-Sharing):
//
//	The service uses chora-common/eventbus (NATS JetStream). When NATS_URL is
//	set we wire a JetStream bus; otherwise we fall back to the in-process bus
//	(eventbus.NewInMemoryBus) so the service still emits envelope-validating
//	events for traces + subscriber wiring without standing up the broker.
//
//	Subscribers wired here for cross-domain population of the discovery feed:
//	  - chora.delivery.course.published.v1
//	  - chora.consumption.atom_session.completed.v1
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthgrpc "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"github.com/apollo-chora/chora-common/agentengine"
	"github.com/apollo-chora/chora-common/durabilityguard"
	"github.com/apollo-chora/chora-common/eventbus"
	cgcmodelarmor "github.com/apollo-chora/chora-common/modelarmor"
	// pgx stdlib driver — registered for sql.Open("pgx", dsn) used by
	// the per-domain outbox PostgresStore in bootstrap.go.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/apollo-chora/chora-common/tracing"

	sharingv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/sharing/v1"
	identityv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/identity/v1"
	"github.com/apollo-chora/chora-sharing/internal/adapter/clients"
	"github.com/apollo-chora/chora-sharing/internal/adapter/duelevent"
	eventsadapter "github.com/apollo-chora/chora-sharing/internal/adapter/events"
	"github.com/apollo-chora/chora-sharing/internal/adapter/eventpush"
	sharinggrpc "github.com/apollo-chora/chora-sharing/internal/adapter/grpc"
	httpadapter "github.com/apollo-chora/chora-sharing/internal/adapter/http"
	"github.com/apollo-chora/chora-sharing/internal/adapter/inmem"
	mmadapter "github.com/apollo-chora/chora-sharing/internal/adapter/matchmaking"
	sharingmodelarmor "github.com/apollo-chora/chora-sharing/internal/adapter/modelarmor"
	sharingoutbox "github.com/apollo-chora/chora-sharing/internal/adapter/outbox"
	sharingpg "github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/adapter/subscribers"
	"github.com/apollo-chora/chora-sharing/internal/adapter/ws"
	"github.com/apollo-chora/chora-sharing/internal/config"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_projection"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
	"github.com/apollo-chora/chora-sharing/internal/domain/bookmark"
	"github.com/apollo-chora/chora-sharing/internal/domain/comment"
	"github.com/apollo-chora/chora-sharing/internal/domain/currency"
	"github.com/apollo-chora/chora-sharing/internal/domain/discovery"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
	"github.com/apollo-chora/chora-sharing/internal/domain/grant"
	"github.com/apollo-chora/chora-sharing/internal/domain/leaderboard"
	"github.com/apollo-chora/chora-sharing/internal/domain/post"
	tmpl "github.com/apollo-chora/chora-sharing/internal/domain/post_template"
	"github.com/apollo-chora/chora-sharing/internal/domain/profiler"
	"github.com/apollo-chora/chora-sharing/internal/domain/reaction"
	"github.com/apollo-chora/chora-sharing/internal/domain/social"
	"github.com/apollo-chora/chora-sharing/internal/observability"
)

// registerMilestoneSubscriber binds the FamiliarMilestoneSubscriber's four
// topic handlers onto the in-process bus. Each handler decodes the JSON
// envelope payload and forwards to the typed Handle* method.
//
// This is the DEV fallback; in production the durable JetStream consumers
// (registerFamiliarMilestoneSubscriber) dispatch to the same Handle* methods.
func registerMilestoneSubscriber(ctx context.Context, bus *eventbus.InMemoryBus, sub *subscribers.FamiliarMilestoneSubscriber) {
	pairs := []struct {
		topic   string
		handler eventbus.Handler
	}{
		{
			topic: subscribers.TopicFamiliarStageUp,
			handler: func(c context.Context, msg eventbus.Message) error {
				env, err := subscribers.DecodeStageUpWithAttrs(msg.Payload, attrsFromBusEnvelope(msg))
				if err != nil {
					return err
				}
				return sub.HandleStageUp(c, env)
			},
		},
		{
			topic: subscribers.TopicFamiliarBreedRevealed,
			handler: func(c context.Context, msg eventbus.Message) error {
				env, err := subscribers.DecodeBreedRevealedWithAttrs(msg.Payload, attrsFromBusEnvelope(msg))
				if err != nil {
					return err
				}
				return sub.HandleBreedRevealed(c, env)
			},
		},
		{
			topic: subscribers.TopicFamiliarHatched,
			handler: func(c context.Context, msg eventbus.Message) error {
				env, err := subscribers.DecodeHatchedWithAttrs(msg.Payload, attrsFromBusEnvelope(msg))
				if err != nil {
					return err
				}
				return sub.HandleHatched(c, env)
			},
		},
		{
			topic: subscribers.TopicFamiliarSourceRevelation,
			handler: func(c context.Context, msg eventbus.Message) error {
				env, err := subscribers.DecodeSourceRevelationWithAttrs(msg.Payload, attrsFromBusEnvelope(msg))
				if err != nil {
					return err
				}
				return sub.HandleSourceRevelation(c, env)
			},
		},
	}
	for _, p := range pairs {
		if _, err := bus.Subscribe(ctx, p.topic, p.handler); err != nil {
			log.Printf("sharing: subscribe %s: %v", p.topic, err)
		}
	}
	log.Printf("sharing: FamiliarMilestoneSubscriber bound to %d topics", len(pairs))
}

// registerLiveQuizScoreSubscriber binds the LiveQuizScoreSubscriber's single
// topic handler onto the in-process bus (ADR-168 Task #8). Each message is
// decoded (attrs-aware) and forwarded to HandleScoreAwarded, which credits the
// awarded points to the durable Leaderboard.
//
// This is the DEV fallback; the push inbox (live-quiz-scores) is the
// production transport.
func registerLiveQuizScoreSubscriber(ctx context.Context, bus *eventbus.InMemoryBus, sub *subscribers.LiveQuizScoreSubscriber) {
	handler := func(c context.Context, msg eventbus.Message) error {
		env, err := subscribers.DecodeScoreAwardedWithAttrs(msg.Payload, attrsFromBusEnvelope(msg))
		if err != nil {
			return err
		}
		return sub.HandleScoreAwarded(c, env)
	}
	if _, err := bus.Subscribe(ctx, subscribers.TopicLiveQuizScoreAwarded, handler); err != nil {
		log.Printf("sharing: subscribe %s: %v", subscribers.TopicLiveQuizScoreAwarded, err)
	}
	log.Printf("sharing: LiveQuizScoreSubscriber bound to %s", subscribers.TopicLiveQuizScoreAwarded)
}

// registerDuelCompletedSubscriber binds the DuelCompletedSubscriber to the
// in-process bus. In production the same handler is reached via the outbox
// dispatcher, which publishes duel.completed.v1 onto the bus.
func registerDuelCompletedSubscriber(ctx context.Context, bus *eventbus.InMemoryBus, sub *subscribers.DuelCompletedSubscriber) {
	handler := func(c context.Context, msg eventbus.Message) error {
		env, err := subscribers.DecodeDuelCompleted(msg.Payload, attrsFromBusEnvelope(msg))
		if err != nil {
			return err
		}
		return sub.HandleDuelCompleted(c, env)
	}
	if _, err := bus.Subscribe(ctx, subscribers.TopicDuelCompleted, handler); err != nil {
		log.Printf("sharing: subscribe %s: %v", subscribers.TopicDuelCompleted, err)
	}
	log.Printf("sharing: DuelCompletedSubscriber bound to %s", subscribers.TopicDuelCompleted)
}

// attrsFromBusEnvelope projects an eventbus.Message's parsed envelope back
// into the attribute shape expected by the protodecode helpers. The bus
// reconstructs Envelope from the wire headers on receive; this is the inverse
// so the Decode*WithAttrs helpers can merge envelope fields into the JSON
// fallback path.
func attrsFromBusEnvelope(msg eventbus.Message) map[string]string {
	env := msg.Envelope
	attrs := map[string]string{
		"topic":     msg.Subject,
		"event_id":  env.EventID,
		"tenant_id": env.TenantID,
		"gcid":      env.GCID,
	}
	if env.Traceparent != "" {
		attrs["traceparent"] = env.Traceparent
	}
	if env.Tracestate != "" {
		attrs["tracestate"] = env.Tracestate
	}
	if !env.OccurredAt.IsZero() {
		attrs["occurred_at"] = env.OccurredAt.UTC().Format(time.RFC3339Nano)
	}
	if !env.PublishedAt.IsZero() {
		attrs["published_at"] = env.PublishedAt.UTC().Format(time.RFC3339Nano)
	}
	if env.ChoraImdaDimension != "" {
		attrs["chora_imda_dimension"] = env.ChoraImdaDimension
	}
	if env.ImdaLifecycleStage != "" {
		attrs["imda_lifecycle_stage"] = env.ImdaLifecycleStage
	}
	return attrs
}

// pushVerifier builds the bearer-token verifier for ONE push inbox.
//
// The audience is DIFFERENT FOR EVERY INBOX (it is the subscription's own push
// endpoint URL):
//
//	https://api.chora.site/api/internal/pubsub/weakness-grown
//	https://api.chora.site/api/internal/pubsub/live-quiz-scores
//	https://api.chora.site/api/internal/pubsub/course-published
//
// A single CHORA_PUBSUB_PUSH_AUDIENCE value can therefore only ever be correct
// for one of the three: set it, and the other two would 401 every message
// straight into the dead-letter topic. Derive the audience per inbox instead,
// so turning verification ON is safe.
//
// Base unset ⇒ verification disabled, exactly as today. The enforcing gate then
// remains the mesh AuthorizationPolicy restricting /api/internal/pubsub/* to
// the chora-gateway service account.
func pushVerifier(inbox string) *eventpush.Verifier {
	base := strings.TrimRight(strings.TrimSpace(os.Getenv("CHORA_PUBSUB_PUSH_AUDIENCE_BASE")), "/")
	if base == "" {
		return eventpush.NewVerifier(eventpush.VerifierConfig{})
	}
	return eventpush.NewVerifier(eventpush.VerifierConfig{
		Audience: base + httpadapter.InboxPath(inbox),
	})
}

// splitCSV trims + filters a comma-separated env-var into a slice.
func splitCSV(s string) []string {
	if s = strings.TrimSpace(s); s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// identityInterceptor is a gRPC unary interceptor that extracts the
// gateway-stamped identity metadata (x-tenant-id + gcid) and injects them
// into the request context via tracing.WithTenantID / tracing.WithGCID.
// This makes the tenant_id + gcid available to rls.ApplySession inside the
// pg-backed repos — the SAME context key system the HTTP middleware uses
// (tracing ctxKeyTenantID / ctxKeyGCID). Without this interceptor, gRPC
// handlers calling pg repos fail with rls.ErrNoTenantContext.
//
// The metadata keys mirror the HTTP header names (lowercase per gRPC
// metadata convention): "x-tenant-id" + "gcid".
func identityInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("x-tenant-id"); len(vals) > 0 && vals[0] != "" {
			ctx = tracing.WithTenantID(ctx, vals[0])
		}
		if vals := md.Get("gcid"); len(vals) > 0 && vals[0] != "" {
			ctx = tracing.WithGCID(ctx, vals[0])
		}
	}
	return handler(ctx, req)
}

// buildModerationEngine constructs the agentengine.Client for Moderation
// invocations via ADC. ADC failure (local dev without us-central1 creds)
// returns an error; caller logs + skips, leaving the handler to 503
// moderation_engine_not_wired.
func buildModerationEngine(ctx context.Context) (*agentengine.AgentEngineClient, error) {
	tokens, err := agentengine.NewADCTokenSource(ctx)
	if err != nil {
		return nil, err
	}
	return agentengine.New(
		agentengine.WithHTTPDoer(http.DefaultClient),
		agentengine.WithTokenSource(tokens),
		agentengine.WithCrewKind("content_moderation"),
	)
}

// buildModerationGKEClient constructs the Moderation engine client backed by
// the GKE web-mode ADK server (ADR-169 / CHO-1631 — Vertex AI Agent Engine
// decommissioned; the content_moderation crew now runs on GKE, gateway-
// metered). No ADC token — the call is cluster-local cleartext to the
// sidecar-less agent pod. endpoint is the http(s):// or gke:// base URL,
// passed through to the GKE client as the engine "resource".
func buildModerationGKEClient(endpoint string) (clients.ModerationEnginePort, error) {
	gkeClient, err := agentengine.NewGKEClient(
		agentengine.WithGKEHTTPDoer(http.DefaultClient),
		agentengine.WithGKECrewKind("content_moderation"),
	)
	if err != nil {
		return nil, fmt.Errorf("moderation GKE engine: %w", err)
	}
	return clients.NewModerationEngineClient(gkeClient, endpoint), nil
}

// buildGuardrail constructs the guardrail adapter for the Moderation crew
// pre-flight per ADR-152. The screener is the LOCAL, deterministic
// substitute in chora-common/modelarmor (the managed guardrail adapter was
// removed with the cloud exit) — it returns real FilterHits and an honest
// Verdict, but production traffic MUST front it with a managed guardrail.
// Returns nil + nil error when env config is incomplete in dev (the handler
// skips the pre-flight in that case and relies on the Moderation engine gate).
//
// Env contract (no inline config per feedback_no_inline_config):
//
//   - CHORA_MODELARMOR_PROJECT     (default: chora-local)
//   - CHORA_MODELARMOR_LOCATION    (default: local)
//   - CHORA_ENVIRONMENT            (default: dev)
//   - CHORA_GUARDRAIL_MAPPING_PATH (default: chora-contracts/yaml/agent-guardrail-mapping.yaml)
//
// The YAML ships as part of the container image (Dockerfile COPYs
// chora-contracts/yaml/) so the default path resolves;
// CHORA_GUARDRAIL_MAPPING_PATH overrides for local dev runs from a
// different cwd.
func buildGuardrail(ctx context.Context) (sharingmodelarmor.GuardrailPort, func() error, error) {
	project := envOrDefault("CHORA_MODELARMOR_PROJECT", "chora-local")
	location := envOrDefault("CHORA_MODELARMOR_LOCATION", "local")
	environment := envOrDefault("CHORA_ENVIRONMENT", "dev")
	// Canonical container-friendly path; aligned with chora-agent-executor's
	// CHORA_AGENT_GUARDRAIL_MAPPING env name. Dockerfile COPYs the YAML to
	// /etc/chora/agent-guardrail-mapping.yaml so the resolver works inside
	// a distroless final image (no monorepo root at runtime).
	mappingPath := envOrDefault("CHORA_AGENT_GUARDRAIL_MAPPING", "/etc/chora/agent-guardrail-mapping.yaml")

	resolver, err := sharingmodelarmor.LoadResolverFromFile(mappingPath, project, location, environment)
	if err != nil {
		return nil, nil, fmt.Errorf("guardrail resolver: %w", err)
	}
	screener, err := cgcmodelarmor.NewScreener(ctx, project, location)
	if err != nil {
		return nil, nil, fmt.Errorf("guardrail screener: %w", err)
	}
	// Register permissive templates as INSPECT_ONLY so MATCH_FOUND on
	// audit-only tiers becomes VerdictInspectOnly rather than Block.
	if c, ok := screener.(*cgcmodelarmor.LocalScreener); ok {
		for _, name := range resolver.PermissiveTemplates() {
			c.SetTemplateMode(name, cgcmodelarmor.TemplateModeInspectOnly)
		}
	}
	adapter, err := sharingmodelarmor.NewAdapter(screener, resolver)
	if err != nil {
		_ = screener.Close()
		return nil, nil, fmt.Errorf("guardrail adapter: %w", err)
	}
	return adapter, screener.Close, nil
}

// envOrDefault returns os.Getenv(key) when non-empty (after trim) else
// the supplied default. Mirrors the helper pattern in other Chora
// services (chora-creation/bootstrap.go).
func envOrDefault(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

const (
	serviceName    = "chora-sharing"
	serviceVersion = "0.2.0"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// ----------------------------------------------------------------------
	// Pub/Sub bus wiring (Wave-B + M12.3 W1d + W1.5d).
	//
	// ctx is cancelled on SIGINT/SIGTERM so the outbox dispatcher goroutine
	// exits cleanly during Cloud Run scale-to-zero.
	// ----------------------------------------------------------------------
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// OTLP wiring per Tier 3 D13 — direct to Cloud Trace in prod.
	//
	// Per C(a).S1 path (b) — tracker #151 — OTLP init runs in its own
	// goroutine with its own (env-tunable, default 15s) deadline + fail-
	// soft semantics. Timeout / init-error degrade to a no-op shutdown,
	// so the rest of bootstrap (pgx pool, Pub/Sub clients) gets the FULL
	// CHORA_BOOTSTRAP_TIMEOUT_SECONDS budget. Previously a slow Cloud
	// Trace TLS handshake could swallow the shared budget and crash-
	// loop the pod under PgBouncer 4-container cold-start.
	otlpHandle := observability.InitAsync(ctx)
	defer func() {
		// WaitContext blocks until init settles — usually a no-op by
		// shutdown time because pgx-pool init below already gave OTLP
		// best-effort wall-clock to land.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		res := otlpHandle.WaitContext(shutdownCtx)
		if err := res.Shutdown(shutdownCtx); err != nil {
			log.Printf("trace shutdown error: %v", err)
		}
	}()

	pool, poolShutdown := bootstrapDBPool(ctx)
	if poolShutdown != nil {
		defer poolShutdown()
	}
	jetBus, busShutdown := bootstrapBus(ctx)
	if busShutdown != nil {
		defer busShutdown()
	}

	// inmemBus remains for in-process Subscribe calls (cross-domain
	// projections to the discovery feed). Outbound publishes are routed
	// through the outbox-backed Publisher when DB pool is wired so they
	// survive pod death + can be replayed.
	inmemBus := eventbus.NewInMemoryBus()
	defer inmemBus.Close()

	// ----------------------------------------------------------------------
	// D6.2 producer-side outbox wiring (M12.3 W1d + W1.5d bootstrap).
	//
	// Per `feedback_d6_resilience_first_class` + `agentic-resilience-d6`
	// skill Pillar 2. The per-domain Publisher satisfies events.Bus by
	// writing every Publish call to sharing_outbox_events; the Dispatcher
	// drains to the event bus on a background goroutine.
	//
	// Fallback ladder (same as chora-creation W1.5a):
	//   1. pool nil                   → inmemBus direct (dev)
	//   2. pool set + outboxDB nil    → outbox.InMemoryStore (dev w/ pool)
	//   3. pool set + outboxDB set    → outbox.PostgresStore (prod)
	// ----------------------------------------------------------------------
	var outboxStore sharingoutbox.Store
	var outboxDispatcher *sharingoutbox.Dispatcher
	var dispatcherDone chan struct{}

	outboxDB, outboxDBShutdown := bootstrapOutboxDB(ctx)
	if outboxDBShutdown != nil {
		defer outboxDBShutdown()
	}

	if pool != nil {
		if outboxDB != nil {
			outboxStore = sharingoutbox.NewPostgresStore(
				sqlDBAdapter{db: outboxDB},
				sharingoutbox.PostgresStoreOptions{WorkerID: outboxWorkerID()},
			)
			log.Printf("sharing: outbox PostgresStore wired (worker_id=%s)", outboxWorkerID())
		} else {
			outboxStore = sharingoutbox.NewInMemoryStore()
			log.Printf("sharing: outbox InMemoryStore wired (CHORA_OUTBOX_DSN unset; NOT durable across restart)")
		}
	if outboxDB != nil {
		// Dispatch drained rows to the JetStream bus when wired so
		// cross-service consumers receive them; otherwise fall back to the
		// in-process bus so local subscribers (DuelCompletedSubscriber, etc.)
		// actually receive events. The outbox store stays PG-backed (durable);
		// only the dispatch target changes.
		var bus sharingoutbox.Bus
		if jetBus != nil {
			bus = jetBus
			log.Printf("sharing: outbox dispatcher dispatching to NATS JetStream (url=%s)", os.Getenv("NATS_URL"))
		} else {
			bus = inmemBus
			log.Printf("sharing: outbox dispatcher dispatching to inmemBus (NATS_URL unset; NOT durable)")
		}
		outboxDispatcher = sharingoutbox.NewDispatcher(sharingoutbox.DispatcherConfig{
			Store:    outboxStore,
			Bus:      bus,
			WorkerID: outboxWorkerID(),
		})
		dispatcherDone = make(chan struct{})
		go func() {
			defer close(dispatcherDone)
			if err := outboxDispatcher.Run(ctx, 100); err != nil &&
				!errors.Is(err, context.Canceled) &&
				!errors.Is(err, context.DeadlineExceeded) {
				log.Printf("sharing: outbox dispatcher exited: %v", err)
			}
		}()
		log.Printf("sharing: outbox dispatcher goroutine started (batch=100)")
	} else {
		log.Printf("sharing: CHORA_OUTBOX_DSN unset — outbox dispatcher NOT started; rows accumulate")
	}
	}
	_ = jetBus

	// Federated closure-saga subscriber (CHO-1719 / Tier 3 D11): consumes
	// chora.sharing.pii.pseudonymise.requested.v1, applies the per-domain
	// PII_Closure_Map.yaml duty, and acks on
	// chora.sharing.account.pseudonymised.v1.
	//
	// Repo seam (CHO-2198, W0-F1 durability + W0-F5 error-honesty): pg on a
	// healthy pool (durable ack/dedup — migration 0038,
	// closure_pseudonymisation_state), in-memory ONLY when the pool is
	// absent, mirroring the chora-payments else-branch shape. Real
	// per-table pg tokenisation (actually redacting posts / reactions /
	// comments / ... columns) remains separate, deeper debt — this fix is
	// durability of the ack/dedup SIGNAL only, not the redaction itself. The
	// durable consumer is created by the event bus; override the name via env.
	//
	// closureRepo is hoisted to function scope so the W0-F1 durability guard
	// (below, after Deps) can classify it alongside the other repos. It stays
	// nil when the bus is absent (closure subscriber unwired) — the guard
	// reports nil as UNKNOWN, never a violation (mirrors chora-delivery D5).
	var closureRepo eventsadapter.ClosureRepository
	if jetBus != nil {
		piiPath := os.Getenv("CHORA_PII_CLOSURE_MAP_PATH")
		if piiPath == "" {
			piiPath = "config/PII_Closure_Map.yaml"
		}
		closureAckPub := eventbus.NewClosureAckPublisher(
			jetBus,
			envOrDefault("CHORA_SOURCE_PROJECT", "chora-local"),
			"chora-sharing",
		)
		if pool != nil {
			closureRepo = sharingpg.NewClosureRepository(newPgxTxRunner(pool))
			log.Printf("sharing: pg ClosureRepository wired (table=closure_pseudonymisation_state)")
		} else {
			closureRepo = eventsadapter.NewInMemoryClosureRepo()
			log.Printf("sharing: CHORA_DB_DSN unset — closure repo uses in-memory store (NOT durable across restart)")
		}
		if closureSub, err := eventsadapter.BootstrapClosureSubscriber(piiPath, closureRepo, closureAckPub, nil); err != nil {
			log.Printf("sharing: closure subscriber DISABLED (PII map load: %v)", err)
		} else {
			closureSubName := os.Getenv("CHORA_CLOSURE_SUBSCRIPTION")
			if closureSubName == "" {
				closureSubName = "chora-sharing.closure-pseudonymise"
			}
			go func() {
				log.Printf("sharing: closure subscriber binding %s -> %s", closureSubName, eventsadapter.TopicPseudonymiseRequested)
				if err := jetBus.Subscribe(ctx, consumerConfig(closureSubName, eventsadapter.TopicPseudonymiseRequested), eventsadapter.ClosurePullHandler(closureSub)); err != nil && !errors.Is(err, context.Canceled) {
					log.Printf("sharing: closure subscriber exited: %v", err)
				}
			}()
		}
	}
	discoveryReg := discovery.NewRegistry()

	// Subscribe the discovery registry to cross-domain events. These
	// subscriptions are best-effort — if a subscriber returns an error
	// the bus retries up to MaxDeliveryAttempts and then DLQs.
	//
	// subCtx is the subscription lifecycle ctx — separate from the
	// SIGTERM-aware top-level ctx so we can cancel subscriptions
	// independently if needed. The outer ctx remains the dispatcher's
	// cancellation signal.
	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()

	// Subscribe to both course.published.v1 (forward-compat preferred name)
	// and course.created.v1 (current chora-delivery emit signature) — the
	// projection is idempotent on course_id so duplicate flow is safe.
	// The production transport for these two is the course-published push
	// inbox; the in-process binding below serves local dev. Subscribers are
	// NOT outbox-bound (subscribers consume, outbox is the publish path).
	courseHandler := discovery.HandleCoursePublished(discoveryReg)
	if _, err := inmemBus.Subscribe(subCtx, discovery.TopicCoursePublished, courseHandler); err != nil {
		log.Printf("subscribe %s: %v", discovery.TopicCoursePublished, err)
	}
	if _, err := inmemBus.Subscribe(subCtx, discovery.TopicCourseCreated, courseHandler); err != nil {
		log.Printf("subscribe %s: %v", discovery.TopicCourseCreated, err)
	}
	if _, err := inmemBus.Subscribe(subCtx,
		discovery.TopicAtomSessionCompleted,
		discovery.HandleAtomSessionCompleted(discoveryReg),
	); err != nil {
		log.Printf("subscribe %s: %v", discovery.TopicAtomSessionCompleted, err)
	}

	// ----------------------------------------------------------------------
	// PROD-G (ADR-149 Iter G.7) — Familiar milestone subscriber wiring.
	//
	// Subscribes to 4 chora.consumption.familiar.*.v1 topics and either
	// auto-posts / drafts / suppresses per-user preference. In dev (no
	// pgxpool wired) all four ports use in-memory variants so subscriber
	// behaviour is observable end-to-end via inmemBus.
	//
	// Template strings load from CHORA_FAMILIAR_TEMPLATES_PATH or fall
	// back to the canonical embedded defaults (per
	// feedback_no_inline_config — the YAML file is the override channel,
	// not the only path).
	// ----------------------------------------------------------------------
	// milestoneDefaultPolicy is the policy in force for a learner who has never
	// set one (no user_preferences row) — the overwhelmingly common case, since
	// the table starts empty. It is declared ONCE here and handed to both the
	// subscriber (which applies it) and the REST Deps (which reports it), so the
	// API cannot tell a learner they are on a policy the subscriber is not using.
	milestoneDefaultPolicy := subscribers.PolicyDraft

	milestoneDrafts := subscribers.NewInMemoryDraftStore()
	milestonePrefs := subscribers.NewInMemoryPreferenceStore()
	milestoneIdem := subscribers.NewInMemoryIdempotencyStore()
	milestonePosts := subscribers.NewInMemoryPostPublisher()

	templates := tmpl.DefaultTemplates()
	if path := strings.TrimSpace(os.Getenv("CHORA_FAMILIAR_TEMPLATES_PATH")); path != "" {
		blob, err := os.ReadFile(path)
		if err != nil {
			log.Printf("sharing: failed reading CHORA_FAMILIAR_TEMPLATES_PATH=%s — using defaults: %v", path, err)
		} else if parsed, err := tmpl.LoadFromBytes(blob); err != nil {
			log.Printf("sharing: failed parsing %s — using defaults: %v", path, err)
		} else {
			templates = parsed
			log.Printf("sharing: loaded Familiar milestone templates from %s", path)
		}
	}
	composer := tmpl.NewComposer(templates)

	// PROD-G milestone subscriber is now wired AS A DURABLE LANE (CHO-2203),
	// further down — after the pg TxRunner is constructed. The in-memory stores
	// above (milestoneDrafts / milestonePrefs / milestonePosts / milestoneIdem)
	// remain the dev fallback (no DB pool); in prod the four ports are pg-backed
	// (post_drafts / user_preferences / subscriber_idempotency + posts +
	// sharing_outbox_events) and a Cloud PULL receiver replaces the dead
	// in-process bus binding. See registerFamiliarMilestoneSubscriber below.
	//
	// (milestoneIdem is ALSO shared by the live-quiz + weakness-grown push
	// subscribers below; those keep the in-memory store — making them durable is
	// separate follow-up debt, not CHO-2203.)

	// ADR-168 Task #8 — Leaderboard Ranker is shared between the gRPC/HTTP
	// query adapters (below) AND the LiveQuizScoreSubscriber, so live-quiz
	// score_awarded events roll into the same durable board the read paths
	// serve. Created here (before the subscriber) so the in-memory aggregator
	// is the single source of truth across transport + ingest.
	leaderboardsRanker := leaderboard.NewRanker()

	// LiveQuizScoreSubscriber consumes
	// chora.delivery.live_quiz_session.score_awarded.v1 and credits the
	// awarded points to the tenant-scoped weekly + all-time boards. Shares the
	// milestone idempotency store (same chora_sharing.subscriber_idempotency
	// table in prod) for at-least-once dedup keyed by event_id.
	liveQuizScoreSub := subscribers.NewLiveQuizScoreSubscriber(subscribers.LiveQuizScoreConfig{
		Ranker:      leaderboardsRanker,
		Idempotency: milestoneIdem,
	})
	registerLiveQuizScoreSubscriber(subCtx, inmemBus, liveQuizScoreSub)

	// WeaknessGrownSubscriber credits leaderboard XP for a real weakness grow
	// (ADR-196 B3). It was NEVER CONSTRUCTED here: NewWeaknessGrownPushHandler
	// existed, its unit test was green — because the test called the handler
	// directly — and nothing in the composition root ever built the subscriber it
	// needed. chora-gateway routed weakness-grown to this service regardless, and
	// every message 404'd and dead-lettered for two weeks. (CHO-2195.)
	weaknessGrownSub := subscribers.NewWeaknessGrownSubscriber(subscribers.WeaknessGrownConfig{
		Ranker:      leaderboardsRanker,
		Idempotency: milestoneIdem,
	})

	// §6 REST surface — the greenfield http adapter (NewHandler). The legacy
	// milestone-drafts + pub/sub-push http handlers were deleted in the
	// greenfield rebuild (not §6 routes); their subscriber-side wiring
	// (familiarSub, liveQuizScoreSub) stays bound to the in-mem bus for the
	// durable side effects — only the HTTP push endpoints are gone.
	moderationResource := strings.TrimSpace(os.Getenv("MODERATION_ENGINE_RESOURCE"))
	var moderationEngine clients.ModerationEnginePort

	// Moderation → GKE web-mode (ADR-169 / CHO-1631 — Vertex AI Agent Engine
	// decommissioned; content_moderation crew on GKE, gateway-metered). When
	// MODERATION_ENGINE_GKE_ENDPOINT is set, dial the cluster-local plaintext
	// GKE Service (sidecar-less, no ADC token) and use the endpoint as the
	// handler's 503-guard resource. This OVERRIDES the legacy dead-Vertex
	// MODERATION_ENGINE_RESOURCE path below.
	if gke := strings.TrimSpace(os.Getenv("MODERATION_ENGINE_GKE_ENDPOINT")); gke != "" {
		if me, err := buildModerationGKEClient(gke); err == nil {
			moderationEngine = me
			moderationResource = gke
			log.Printf("sharing: moderation engine = GKE web-mode @ %s (ADR-169)", gke)
		} else {
			log.Printf("sharing: moderation GKE wiring failed: %v", err)
		}
	}
	if moderationEngine == nil && moderationResource != "" {
		if eng, err := buildModerationEngine(ctx); err == nil {
			moderationEngine = clients.NewModerationEngineClient(eng, moderationResource)
		} else {
			log.Printf("sharing: skipping Moderation engine wiring (%v)", err)
		}
	}
	_ = moderationEngine // retained for the guardrail-adapter wiring below

	// ADR-152 — Cloud Model Armor pre-flight wiring (supersedes the
	// retired chora-guardrail Cloud Run service). On wiring failure
	// (e.g. ADC unavailable in local dev) the handler skips the
	// pre-flight and falls back to the Moderation engine gate alone.
	var guardrail sharingmodelarmor.GuardrailPort
	if gr, closer, err := buildGuardrail(ctx); err == nil {
		guardrail = gr
		if closer != nil {
			defer func() {
				if cerr := closer(); cerr != nil {
					log.Printf("sharing: guardrail screener close error: %v", cerr)
				}
			}()
		}
		log.Printf("sharing: Cloud Model Armor guardrail wired (agent_id=%s, ADR-152)",
			sharingmodelarmor.AgentIDSocialModeration)
	} else {
		log.Printf("sharing: skipping Cloud Model Armor guardrail wiring (%v)", err)
	}
	// Domain repos: pg-backed when the DB pool is wired (production / local
	// dev with CHORA_DB_DSN set), in-memory otherwise (unit tests, quick
	// dev). Per greenfield §3-§4 + pg/doc.go: the pg adapters are RLS-aware
	// (every method calls rls.ApplySession via the TxRunner). The inmem
	// adapters are hermetic + share state across the HTTP + gRPC Deps so a
	// share created via one transport is visible on the other.
	txRunner := newPgxTxRunner(pool)

	// ----------------------------------------------------------------------
	// CHO-2203 (parent CHO-1889) — DURABLE Familiar-milestone lane.
	//
	// The four milestone ports are pg-backed when the pool is wired (durable:
	// post_drafts / user_preferences / subscriber_idempotency + posts +
	// sharing_outbox_events, migrations 0039-0041), and fall back to the
	// in-memory stores created above in dev. A Cloud PULL receiver
	// (registerFamiliarMilestoneSubscriber) subscribes to the four live
	// chora.consumption.familiar.*.v1 topics when a real Pub/Sub client is
	// wired; only in dev (no pubsub client) does it fall back to the
	// in-process bus binding. This REPLACES the dead in-memory-only lane.
	// ----------------------------------------------------------------------
	var (
		milestoneDraftStore subscribers.DraftStore       = milestoneDrafts
		milestonePrefStore  subscribers.PreferenceStore  = milestonePrefs
		milestonePostPub    subscribers.PostPublisher    = milestonePosts
		milestoneIdemStore  subscribers.IdempotencyStore = milestoneIdem
	)
	// The owner-facing C+ drafts API (CHO-2258) consumes the same pg stores, but
	// ONLY the pg ones: a learner must never be shown drafts from an in-memory
	// map that dies with the pod, nor be told a share preference was saved when
	// it was written to a map. Left nil in dev, the five routes 501 (fail-loud —
	// the Friendships convention).
	var (
		milestoneAPIDrafts httpadapter.MilestoneDrafts
		milestoneAPIPrefs  httpadapter.MilestonePrefs
	)
	if txRunner != nil {
		pgDrafts := sharingpg.NewMilestoneDraftStore(txRunner)
		pgPrefs := sharingpg.NewMilestonePreferenceStore(txRunner)
		milestoneDraftStore = pgDrafts
		milestonePrefStore = pgPrefs
		milestoneAPIDrafts = pgDrafts
		milestoneAPIPrefs = pgPrefs
		milestonePostPub = sharingpg.NewMilestonePostPublisher(txRunner)
		milestoneIdemStore = sharingpg.NewMilestoneIdempotencyStore(txRunner)
		log.Printf("sharing: pg-backed Familiar-milestone lane wired (durable: post_drafts/user_preferences/subscriber_idempotency + posts/outbox, CHO-2203) + owner drafts/prefs API (CHO-2258)")
	} else {
		log.Printf("sharing: in-memory Familiar-milestone lane (no DB pool; NOT durable across restart); drafts/prefs API routes will 501")
	}
	familiarSub := subscribers.NewFamiliarMilestoneSubscriber(subscribers.Config{
		Drafts:        milestoneDraftStore,
		Posts:         milestonePostPub,
		Preferences:   milestonePrefStore,
		Idempotency:   milestoneIdemStore,
		Composer:      composer,
		DefaultPolicy: milestoneDefaultPolicy,
	})
	if jetBus != nil {
		if err := registerFamiliarMilestoneSubscriber(subCtx, jetBus, familiarSub); err != nil {
			log.Fatalf("sharing: familiar-milestone subscriber wiring: %v", err)
		}
	} else {
		// Dev fallback only — the in-process bus has no external publisher, so
		// this drains real traffic only under a test that publishes to inmemBus.
		registerMilestoneSubscriber(subCtx, inmemBus, familiarSub)
	}

	// Posts — pg-backed when the pool is wired, exactly like the ten repos in
	// the gate below and like the social graph (ADR-229 WS-0, whose own comment
	// already records that "the old in-memory graph lost runtime edges on pod
	// restart"). Posts sat in memory NEXT TO that fix for two weeks.
	//
	// The greenfield rewrite 470ec0ef9 DELETED internal/adapter/pg/post.go (plus
	// its unit + integration tests) and left the in-memory repo as the only
	// implementation, so every post the C+ surface accepted was written to a map
	// and died with the pod — while the correct, RLS-ready `posts` table sat in
	// chora_sharing holding 16 rows nobody wrote to any more. Restored here.
	var postsRepo post.PostRepo
	var reactionsReg reaction.ReactionRepo
	if txRunner != nil {
		postsRepo = sharingpg.NewPostRepository(txRunner)
		// Reactions had NO adapter at all — reaction.Registry is a pair of maps
		// inside the DOMAIN package, so there was no seam to substitute into and
		// reactions have NEVER persisted. Registry's own comment promised this
		// swap at "M12+", and the UNIQUE (gcid, post_id, reaction_type) key it
		// said the idempotency invariant would move to ALREADY EXISTS in the live
		// table. The table was built for this adapter; the adapter was never
		// written. It is now.
		reactionsReg = sharingpg.NewReactionRepo(txRunner)
		log.Printf("sharing: pg-backed post + reaction repos wired (durable)")
	} else {
		postsRepo = inmem.NewPostRepo()
		reactionsReg = reaction.NewRegistry()
		log.Printf("sharing: in-memory post + reaction repos wired (no DB pool; NOT durable)")
	}

	var (
		sharesRepo      atom_share.ShareRepo
		projectionsRepo atom_projection.AtomProjectionReader
		grantsRepo      grant.GrantRepo
		royaltiesRepo   grant.RoyaltyRepo
		duelsRepo       duel.DuelRepo
		commentsRepo    comment.CommentRepo
		currencyRepo    currency.CurrencyRepo
		bookmarksRepo   bookmark.BookmarkRepo
		connStore       httpadapter.ConnectionStore
		profileStore    profiler.ProfileRepo
	)
	if txRunner != nil {
		pgProj := sharingpg.NewProjectionRepo(txRunner)
		sharesRepo = sharingpg.NewShareRepo(txRunner).WithOutboxBus(sharingpg.NewDefaultOutboxWriter())
		projectionsRepo = pgProj
		grantsRepo = sharingpg.NewGrantRepo(txRunner)
		royaltiesRepo = sharingpg.NewRoyaltyRepo(txRunner)
		duelsRepo = sharingpg.NewDuelRepo(txRunner)
		commentsRepo = sharingpg.NewCommentRepo(txRunner)
		currencyRepo = sharingpg.NewCurrencyRepo(txRunner)
		bookmarksRepo = sharingpg.NewBookmarkRepo(txRunner)
		connStore = sharingpg.NewConnectionStore(txRunner)
		profileStore = sharingpg.NewProfilerRepo(txRunner)
		log.Printf("sharing: pg-backed repos wired (pool set); profiler=pg")
	} else {
		inShares := inmem.NewShareRepo()
		inProjections := inmem.NewProjectionRepo()
		sharesRepo = inShares
		projectionsRepo = inProjections
		grantsRepo = inmem.NewGrantRepo(inProjections, inShares)
		royaltiesRepo = inmem.NewRoyaltyRepo()
		duelsRepo = inmem.NewDuelRepo()
		commentsRepo = inmem.NewCommentRepo()
		currencyRepo = inmem.NewCurrencyRepo()
		bookmarksRepo = inmem.NewBookmarkRepo()
		profileStore = inmem.NewProfilerRepo()
		log.Printf("sharing: in-memory repos wired (no DB pool)")
	}

	// Social graph — the ADR-230 Relationship aggregate fronts every social
	// mutation in pg mode: block-refusal guards, block-severance, and the
	// relationship.*.v1 outbox events written in the SAME transaction as the
	// state change (B-lite.1, CHO-2119). Reads flow through the
	// social.GraphQueries port (pg SocialGraphRepo — the same object serves
	// the friend set behind GetReuseContext). Tenant-aware in-memory in dev
	// (no events — dev fallback only). The friendship plane (friend requests
	// + friendships) has been removed; FriendSet + FriendSuggestions stay on
	// GraphQueries for the atom_reuse audience tier + GetReuseContext gRPC
	// contract (both return empty).
	var (
		socialGraph       socialGraphPort
		friendsReader     social.GraphQueries
		suggestionsReader social.SuggestionQueries
	)
	if txRunner != nil {
		socialRepo := sharingpg.NewSocialGraphRepo(txRunner)
		relationshipStore := sharingpg.NewRelationshipRepo(txRunner, sharingpg.RelationshipRepoOptions{
			SourceProject: envOrDefault("CHORA_SOURCE_PROJECT", "chora-local"),
		})
		relationships, err := social.NewRelationshipService(social.RelationshipServiceConfig{
			Store: relationshipStore,
			Reads: socialRepo,
		})
		if err != nil {
			log.Fatalf("sharing: relationship service wiring: %v", err)
		}
		socialGraph, friendsReader, suggestionsReader = relationships, socialRepo, socialRepo
		log.Printf("sharing: pg-backed social graph wired (relationship aggregate + severance + outbox spine, ADR-230 B-lite.1)")
	} else {
		memGraph := inmem.NewSocialGraph()
		socialGraph, friendsReader, suggestionsReader = memGraph, memGraph, memGraph
		log.Printf("sharing: in-memory social graph wired (no DB pool; NOT durable; friend set empty)")
	}

	// ADR-229 WS-0 / CHO-1972 — start the (previously built-but-unwired)
	// AtomProjectionSubscriber so atom_projections stays fresh from
	// chora.creation.atom.{published,archived}.v1. pg writer only — the
	// in-memory dev mode has no cross-service surface, and
	// registerAtomProjectionSubscriber itself no-ops loudly when NATS_URL is
	// unset (nil bus).
	if txRunner != nil {
		atomProjSub := subscribers.NewAtomProjectionSubscriber(subscribers.AtomProjectionConfig{
			Writer:      sharingpg.NewAtomProjectionStore(txRunner),
			Idempotency: milestoneIdem,
		})
		// ADR-229 Amendment A1 (CHO-2132) — the stranding detector + repoint
		// legs of the orphan-edition saga. One pg repo implements every port
		// (grant reads, edition map, repoint sweep, orphan_required outbox,
		// any-state projection reads); the friend set rides the same
		// SocialGraphRepo that powers GetReuseContext (structural match on
		// atom_reuse.FriendReads).
		atomReuseRepo := sharingpg.NewAtomReuseRepo(txRunner, sharingpg.AtomReuseRepoOptions{
			SourceProject: envOrDefault("CHORA_SOURCE_PROJECT", "chora-local"),
		})
		strandingSub := subscribers.NewAtomReuseStrandingSubscriber(subscribers.AtomReuseStrandingConfig{
			Grants:      atomReuseRepo,
			Editions:    atomReuseRepo,
			Repointer:   atomReuseRepo,
			Requirer:    atomReuseRepo,
			Projections: atomReuseRepo,
			Friends:     sharingpg.NewSocialGraphRepo(txRunner),
			Idempotency: milestoneIdem,
		})
		if err := registerAtomProjectionSubscriber(subCtx, jetBus, atomProjSub, strandingSub); err != nil {
			log.Fatalf("sharing: atom-projection subscriber wiring: %v", err)
		}
	}

	// SharingRules — config-driven ELO + combo tiers + royalty caps. Loaded
	// loud (fail-fast on malformed env per feedback_no_inline_config); a zero
	// SharingRules is INVALID so we never wire the handler with defaults.
	rules, err := config.LoadSharingRules()
	if err != nil {
		log.Fatalf("sharing: load SharingRules: %v", err)
	}

	// §6 REST handler — the greenfield httpadapter. Deps mirror the gRPC
	// SharingServerDeps (same domain port interfaces). Nil ports fail loud
	// with 501 at call time per §1.1.
	// Duel matchmaking uses the pool-based Matchmaker (no AI, no Meilisearch).
	// Atom selection + profile conjuring use the profile_conjurer +
	// duel_atom_smith ADK agents (GKE web-mode, ADR-169). Unset env → nil
	// ports → 501 (profile_conjurer) / restore-searchers (matchmaking) = fail-loud.
	var conjurer httpadapter.ConjurerPort
	var atomSelector httpadapter.AtomSelectorPort
	var wsHandler http.Handler
	var profileBroker *ws.ProfileBroker
	var profileWSHandler http.Handler
	var roundSweeperRegistrar httpadapter.RoundSweeperRegistrar
	// PROFILE_CONJURER_GKE_ENDPOINT — GKE web-mode ADK endpoint
	// (ADR-169); cluster-local cleartext, no ADC.
	if ep := strings.TrimSpace(os.Getenv("PROFILE_CONJURER_GKE_ENDPOINT")); ep != "" {
		gke, err := agentengine.NewGKEClient(agentengine.WithGKEHTTPDoer(http.DefaultClient), agentengine.WithGKECrewKind("profile_conjurer"))
		if err != nil {
			log.Fatalf("sharing: profile_conjurer GKE client: %v", err)
		}
		conjurer = clients.NewProfileConjurerClient(gke, ep)
		log.Printf("sharing: profile_conjurer agent wired (%s)", ep)
	} else {
		log.Printf("sharing: profile_conjurer agent NOT wired (PROFILE_CONJURER_GKE_ENDPOINT unset — profile generate will 501)")
	}

	// Identity gRPC client — used to resolve display_name via GetMe at
	var displayNameResolver httpadapter.DisplayNameResolver
	var manaClient *clients.ManaClient
	if ep := strings.TrimSpace(os.Getenv("SVC_IDENTITY_GRPC_URL")); ep != "" {
		identityConn, err := grpc.NewClient(ep, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("sharing: chora-identity gRPC dial failed (%s): %v", ep, err)
		} else {
			displayNameResolver = clients.NewIdentityDisplayNameClient(identityConn)
			manaClient = clients.NewManaClient(identityv1.NewManaServiceClient(identityConn))
			log.Printf("sharing: DisplayNameResolver + ManaClient wired (chora-identity at %s)", ep)
		}
	} else {
		log.Printf("sharing: SVC_IDENTITY_GRPC_URL unset — display_name stays empty + royalty mana debit skipped (FE falls back to shortGcid)")
	}

	// DUEL_ATOM_SMITH_GKE_ENDPOINT — GKE web-mode ADK endpoint for the
	// duel_atom_smith crew. Requires projectionsRepo (candidate pool) +
	// profileStore (per-player proficiency context). When unset, falls back
	// to the projection-based selector (no AI — reads the projection
	// read-model directly) so local dev without the smith agent can still
	// run duels.
	if ep := strings.TrimSpace(os.Getenv("DUEL_ATOM_SMITH_GKE_ENDPOINT")); ep != "" && projectionsRepo != nil {
		gke, err := agentengine.NewGKEClient(agentengine.WithGKEHTTPDoer(http.DefaultClient), agentengine.WithGKECrewKind("duel_atom_smith"))
		if err != nil {
			log.Fatalf("sharing: duel atom smith GKE client: %v", err)
		}
		engine := clients.NewDuelAtomSmithEngineClient(gke, ep)
		atomSelector = clients.NewDuelAtomSmithSelector(projectionsRepo, clients.NewProjectionAtomSelector(projectionsRepo), profileStore, engine)
		log.Printf("sharing: duel_atom_smith agent wired (%s)", ep)
	} else if projectionsRepo != nil {
		atomSelector = clients.NewProjectionAtomSelector(projectionsRepo)
		log.Printf("sharing: duel_atom_smith NOT wired — using projection-based atom selector (no AI; local dev fallback)")
	} else {
		log.Printf("sharing: duel_atom_smith agent NOT wired (no projectionsRepo — matchmaking will restore-searchers)")
	}
	// WebSocket handler for real-time duel play. The WS adapter carries
	// the ELO applier + event publisher so ranked duels that complete
	// over the real-time path apply ELO + emit duel.completed.v1.
	var duelEventPub *duelevent.PublisherAdapter
	// The duel event publisher publishes chora.sharing.duel.completed.v1.
	// The DuelCompletedSubscriber (line ~905) is registered on inmemBus.
	// Use the outbox-backed publisher when an outbox dispatcher is running
	// to drain rows — the dispatcher (line ~483) dispatches to inmemBus in
	// local dev (or Cloud Pub/Sub when CHORA_OUTBOX_DISPATCH_PUBSUB=1), so
	// the in-process subscriber receives the event. When no dispatcher is
	// wired (CHORA_PUBSUB_PROJECT unset), publish directly to inmemBus so
	// the subscriber still receives events without the outbox durability.
	if outboxStore != nil && outboxDispatcher != nil {
		outboxBus := sharingoutbox.NewPublisher(sharingoutbox.PublisherConfig{Store: outboxStore})
		eventsPub := eventsadapter.NewPublisher(eventsadapter.Config{Bus: outboxBus})
		duelEventPub = duelevent.New(eventsPub)
		log.Printf("sharing: duel event publisher wired (outbox-backed)")
	} else {
		eventsPub := eventsadapter.NewPublisher(eventsadapter.Config{Bus: inmemBus})
		duelEventPub = duelevent.New(eventsPub)
		log.Printf("sharing: duel event publisher wired (inmem bus)")
	}

	if duelsRepo != nil {
		broker := ws.NewBroker()
		wsAdapter := ws.NewDuelSessionAdapter(duelsRepo, atomSelector, rules.ComboTiers).
			WithRoundTimerSec(rules.RoundTimerSec).
			WithELO(duelsRepo, rules.ELOKFactor).
			WithEventPublisher(duelEventPub)
		wsHandler = ws.NewHandler(broker, wsAdapter, wsAdapter,
			ws.WithWriteGrace(rules.WebSocketGrace))
		log.Printf("sharing: WebSocket duel handler wired (elo + event publish)")

		// WS3: round-timer sweep — auto-resolves expired rounds so a duel
		// advances instead of hanging when a player disconnects. The sweeper
		// reuses the broker + ELO applier + event publisher so timeout
		// completion mirrors the answer-completion path. Per-tenant loop
		// (mirrors the matchmaker) — RLS forbids a cross-tenant sweep.
		roundSweeper := ws.NewRoundSweeper(duelsRepo, broker, 1*time.Second,
			ws.WithRoundSweeperELO(duelsRepo, rules.ELOKFactor),
			ws.WithRoundSweeperEventPublisher(duelEventPub),
			ws.WithRoundSweeperRoundStarter(wsAdapter),
			ws.WithRoundSweeperTimerSec(rules.RoundTimerSec),
		)
		roundSweeper.Start(ctx)
		roundSweeperRegistrar = roundSweeper
		log.Printf("sharing: round-timer sweeper wired (1s tick, per-tenant)")
	}

	// Profile WebSocket — /v1/me/profile/ws. The broker is always wired
	// (even when the conjurer agent is not) so a frontend that connects
	// to the WS while a generation is in-flight still receives the
	// profile_ready / profile_error frame once the async goroutine
	// publishes. Without the broker the 202 path would silently drop the
	// completion + the FE would hang on "generating" until the 5s poll
	// fallback kicks in. When the conjurer is unset (PROFILE_CONJURER_GKE_ENDPOINT
	// missing) generateProfile returns 501 before publishing, so the
	// broker sees no traffic — safe.
	profileBroker = ws.NewProfileBroker()
	profileWSHandler = ws.NewProfileHandler(profileBroker)
	log.Printf("sharing: WebSocket profile handler wired")

	// DuelCompletedSubscriber consumes chora.sharing.duel.completed.v1
	// and credits participation Coins + leaderboard XP to both players.
	if duelsRepo != nil && currencyRepo != nil {
		duelCompletedSub := subscribers.NewDuelCompletedSubscriber(subscribers.DuelCompletedConfig{
			Currency:    currencyRepo,
			Ranker:      leaderboardsRanker,
			Idempotency: milestoneIdem,
			Rewards:     duel.DefaultRewardConfig(),
		})
		registerDuelCompletedSubscriber(subCtx, inmemBus, duelCompletedSub)
		log.Printf("sharing: duel completed subscriber wired")
	}

	// Matchmaker — PG-backed duel matchmaking (multi-pod safe, no in-memory pool).
	var matchmaker httpadapter.MatchmakerPort
	var matchmakingQueue httpadapter.MatchmakingQueueRepo
	var matchmakerStop func()
	if txRunner != nil {
		mmRepo := sharingpg.NewMatchmakingQueueRepo(txRunner)
		matchmakingQueue = mmRepo
		mm := mmadapter.NewMatchmaker(mmadapter.Config{
			Timeout:           rules.MatchmakingTimeout,
			HeartbeatStale:    rules.MatchmakingHeartbeatStale,
			MatchTickInterval: rules.MatchmakingMatchTick,
			SweepTickInterval: rules.MatchmakingSweepTick,
		}, mmRepo)
		matchmaker = mm
		matchmakerStop = mm.Stop
		// Stale finding rows from crashed instances are abandoned by the
		// matchmaker's sweeper loop (per-tenant — a cross-tenant sweep at
		// startup is impossible: RLS forbids it and there is no tenant in
		// the startup context).
		log.Printf("sharing: matchmaker wired (PG-backed, multi-pod, timeout=%s)", rules.MatchmakingTimeout)
	} else {
		log.Printf("sharing: matchmaker NOT wired (no txRunner — matchmaking disabled)")
	}

	restHandler := httpadapter.NewHandler(httpadapter.Deps{
		Shares:           sharesRepo,
		Projections:      projectionsRepo,
		Grants:           grantsRepo,
		Royalties:        royaltiesRepo,
		Duels:            duelsRepo,
		Posts:            postsRepo,

		Reactions:        reactionsReg,
		Graph:            socialGraph,
		Suggestions:      suggestionsReader,
		Comments:         commentsRepo,
		Currency:         currencyRepo,
		Bookmarks:        bookmarksRepo,
		Connections:      connStore,
		Leaderboards:     leaderboard.NewRankerReader(leaderboardsRanker),
		Guardrail:        guardrail,
		Rules:            rules,
		WSHandler:        wsHandler,
		Profiles:         profileStore,
		Conjurer:              conjurer,
		DisplayNameResolver:   displayNameResolver,
		DuelEvents:       duelEventPub,
		Matchmaker:       matchmaker,
		MatchmakingQueue: matchmakingQueue,
		AtomSelector:     atomSelector,
		ProfileBroker:    profileBrokerAdapter{broker: profileBroker},
		ProfileWSHandler: profileWSHandler,
		RoundSweeperRegistrar: roundSweeperRegistrar,

		// Familiar-milestone drafts + share preference (CHO-2258). These are the
		// SAME pg stores the subscriber writes through — one adapter over
		// post_drafts / user_preferences, two consumers with different ports.
		//
		// nil in the in-memory dev fallback, so the five routes 501 rather than
		// serving a learner drafts out of a map that dies with the pod (the
		// Friendships convention).
		MilestoneDrafts: milestoneAPIDrafts,
		MilestonePrefs:  milestoneAPIPrefs,
		// One value, two consumers: the read API must report the policy the
		// SUBSCRIBER actually applies to a learner with no user_preferences row.
		// Two independently hardcoded defaults would drift, and the UI would show
		// a setting that is not the one in force.
		MilestoneDefaultPolicy: milestoneDefaultPolicy,

		// Event-push inboxes (CHO-2195). chora-gateway resolves the owning
		// service and forwards /api/internal/pubsub/{inbox} verbatim. All three
		// assigned inboxes MUST be wired here — main() refuses to boot otherwise
		// (see the MissingInboxes gate below), because an unmounted route 404s
		// and the broker silently dead-letters every message on the lane.
		//
		// Until now only course-published was wired, and the gateway did not route
		// it; the two the gateway DID route (weakness-grown, live-quiz-scores)
		// were mounted nowhere. The subscribers existed but were bound only to the
		// in-process InMemoryBus, which nothing outside the process ever publishes
		// to — a Subscribe() call is not evidence that anything is consumed.
		CoursePublishedPushHandler: httpadapter.NewCoursePublishedPushHandler(httpadapter.CoursePublishedPushDeps{
			Registry: discoveryReg,
			Verifier: pushVerifier(httpadapter.InboxCoursePublished),
		}),
		WeaknessGrownPushHandler: httpadapter.NewWeaknessGrownPushHandler(httpadapter.WeaknessGrownPushDeps{
			Subscriber: weaknessGrownSub,
			Verifier:   pushVerifier(httpadapter.InboxWeaknessGrown),
		}),
		LiveQuizScorePushHandler: httpadapter.NewLiveQuizScorePushHandler(httpadapter.LiveQuizScorePushDeps{
			Subscriber: liveQuizScoreSub,
			Verifier:   pushVerifier(httpadapter.InboxLiveQuizScores),
		}),
	})

	// W0-F1 durability gate (CHO-2198): classify every wired domain repo by SHAPE
	// (holds a live *pgxpool.Pool ⇒ DURABLE; a data map ⇒ IN_MEMORY) and log a
	// structured, greppable report at boot. Report-only unless
	// CHORA_DURABILITY_GUARD=enforce AND the binding is allow-listed — nil
	// allow-list matches the chora-payments / chora-identity / chora-delivery
	// wirings. On a healthy pool this is the clean 11/11-style reference: every
	// port resolves DURABLE (pg) or UNKNOWN (nil connections/friendships/closure
	// in dev). The always-in-memory read-models (leaderboard ranker, discovery
	// registry) + the dead-in-prod milestone subscriber stores are intentionally
	// OMITTED — this gate covers the durable domain-repo ports.
	durabilityguard.Guard("chora-sharing", []durabilityguard.Binding{
		{Port: "posts", Adapter: postsRepo},
		{Port: "reactions", Adapter: reactionsReg},
		{Port: "shares", Adapter: sharesRepo},
		{Port: "projections", Adapter: projectionsRepo},
		{Port: "grants", Adapter: grantsRepo},
		{Port: "royalties", Adapter: royaltiesRepo},
		{Port: "duels", Adapter: duelsRepo},
		{Port: "comments", Adapter: commentsRepo},
		{Port: "currency", Adapter: currencyRepo},
		{Port: "bookmarks", Adapter: bookmarksRepo},
		{Port: "connections", Adapter: connStore},
		{Port: "graph", Adapter: socialGraph},
		{Port: "closure", Adapter: closureRepo},
	}, nil)

	// FAIL-LOUD BOOT GATE (CHO-2195). Refuse to start if any event-push inbox
	// assigned to chora-sharing was not actually mounted.
	//
	// This is the guard whose absence let the regression run for two weeks. Every
	// other signal was green: the handlers compiled, their unit tests passed (they
	// called the handler directly, bypassing the mux), the pod was Ready, and the
	// service returned 200 on every route anyone looked at. Meanwhile chora-gateway
	// forwarded each weakness-grown and live-quiz-scores push to a 404, and the
	// broker retried it five times and dead-lettered it. Nothing, anywhere, said so.
	//
	// A pod that will not start is loud. A pod that silently drops every event on a
	// lane is not. Prefer the crash.
	if missing := restHandler.MissingInboxes(); len(missing) > 0 {
		log.Fatalf("sharing: REFUSING TO BOOT — %d event-push inbox(es) assigned to chora-sharing "+
			"are NOT MOUNTED: %s. chora-gateway routes these here; an unmounted route 404s and "+
			"the broker dead-letters every message on the lane, silently. Wire the handler into "+
			"httpadapter.Deps (internal/adapter/http/handlers.go).",
			len(missing), strings.Join(missing, ", "))
	}
	log.Printf("sharing: event-push inboxes MOUNTED (%d): %s",
		len(httpadapter.SharingInboxes),
		strings.Join(httpadapter.SharingInboxes, ", "))

	// ----------------------------------------------------------------------
	// gRPC server (Wave-1 S-FULL, 2026-05-16) per
	// docs/m13/grpc-mass-remediation-2026-05-16.md §3.a.
	//
	// chora-gateway BFF + chora-consumption call Sharing over gRPC mesh DNS
	// (chora-sharing.sharing.svc.cluster.local:9090) post Wave-2 cutover.
	// mTLS via Cloud Service Mesh PeerAuth; AuthorizationPolicy
	// authz-allow-sharing-grpc.yaml scopes inbound to gateway + consumption
	// principals on port 9090.
	//
	// Per feedback_no_stubs_real_wiring: register unconditionally — no
	// "if env unset { skip }" shim. If a dependent is not wired yet, the
	// consumer fails fast.
	// ----------------------------------------------------------------------
	grpcPort := strings.TrimSpace(os.Getenv("CHORA_GRPC_PORT"))
	if grpcPort == "" {
		grpcPort = "9090"
	}
	grpcLis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalf("sharing: gRPC net.Listen :%s: %v", grpcPort, err)
	}
	grpcSrv := grpc.NewServer(grpc.UnaryInterceptor(identityInterceptor))
	// SharingServer wires domain port interfaces. Social RPCs (Follow,
	// CreatePost, ReactToPost, GetLeaderboard) are backed by inmem adapters;
	// atom-sharing + duel RPCs are backed by pg-backed adapters when the DB
	// pool is wired, or inmem adapters in dev/test (fail-loud per §1.1).
	sharingGRPCSrv := sharinggrpc.New(sharinggrpc.Deps{
		Shares:       sharesRepo,
		Projections:  projectionsRepo,
		Grants:       grantsRepo,
		Royalties:    royaltiesRepo,
		Posts:        postsRepo,
		Reactions:    reactionsReg,
		Graph:        socialGraph,
		Friends:      friendsReader,
		Comments:     commentsRepo,
		Currency:     currencyRepo,
		Bookmarks:    bookmarksRepo,
		Leaderboards: leaderboard.NewRankerReader(leaderboardsRanker),
		Guardrail:    guardrail,
		Rules:        rules,
		Mana:         manaClient,
		RoyaltyEvents: duelEventPub,
	})
	sharingv1.RegisterSharingServer(grpcSrv, sharingGRPCSrv)

	// gRPC health check — required for Cloud Service Mesh probe routing.
	grpcHealthSrv := healthgrpc.NewServer()
	grpcHealthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	grpcHealthSrv.SetServingStatus("chora.services.sharing.v1.Sharing", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcSrv, grpcHealthSrv)
	reflection.Register(grpcSrv)

	grpcErrCh := make(chan error, 1)
	go func() {
		log.Printf("sharing: gRPC server listening on :%s (Sharing bound)", grpcPort)
		if err := grpcSrv.Serve(grpcLis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			grpcErrCh <- err
		}
	}()

	// §6 REST handler is the sole HTTP entry point (the legacy milestone +
	// pub/sub-push http routes were deleted in the greenfield rebuild — not
	// §6 routes). Health endpoints (/healthz, /readyz) live on the same mux.
	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           restHandler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      45 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	addr := ":" + port
	log.Printf("service=%s version=%s listening on %s", serviceName, serviceVersion, addr)

	// Run server in a goroutine so we can intercept SIGTERM cleanly —
	// Cloud Run sends SIGTERM during scale-to-zero and rolling updates.
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		log.Fatalf("server error: %v", err)
	case err := <-grpcErrCh:
		log.Fatalf("sharing: gRPC server error: %v", err)
	case <-ctx.Done():
		log.Printf("received SIGTERM — shutting down")

		// Drain gRPC first — in-flight Sharing RPCs finish + clients see
		// EOF cleanly before the HTTP path drains. Mirrors the
		// chora-identity pattern at services/chora-identity/cmd/server/main.go.
		grpcShutdownDone := make(chan struct{})
		go func() {
			grpcSrv.GracefulStop()
			close(grpcShutdownDone)
		}()
		select {
		case <-grpcShutdownDone:
			log.Printf("sharing: gRPC server drained")
		case <-time.After(10 * time.Second):
			log.Printf("sharing: gRPC graceful-stop deadline exceeded — forcing stop")
			grpcSrv.Stop()
		}

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown error: %v", err)
		}

		// Final outbox drain — flush in-flight pending rows before exit.
		if outboxDispatcher != nil {
			finalDrain, fcancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer fcancel()
			if n, derr := outboxDispatcher.DrainOnce(finalDrain, 200); derr != nil {
				log.Printf("sharing: final outbox drain error: %v (drained %d)", derr, n)
			} else {
				log.Printf("sharing: final outbox drain published %d rows", n)
			}
		}
		if dispatcherDone != nil {
			select {
			case <-dispatcherDone:
			case <-time.After(5 * time.Second):
				log.Printf("sharing: outbox dispatcher shutdown timed out (5s)")
			}
		}

		// Stop the matchmaker background goroutine (§2.6 — goroutine leak).
		// The matchmaker's sweep+match loop must exit cleanly so it
		// doesn't leak across redeploys holding stale pool state.
		if matchmakerStop != nil {
			matchmakerStop()
			log.Printf("sharing: matchmaker stopped")
		}
	}
}

// socialGraphPort is the union of the http + grpc SocialGraph ports so one
// concrete graph (pg repo in prod, tenant-aware in-memory in dev) serves both
// transports; structural typing keeps the adapters' local interfaces
// authoritative. (Replaces seedSocialGraph — the durable pg graph reads
// straight from social_follows/social_blocks under RLS, so there is no
// in-memory copy to hydrate; ADR-229 WS-0.)
type socialGraphPort interface {
	Follow(ctx context.Context, tenantID, follower, followee string) (*social.Edge, bool, error)
	Unfollow(ctx context.Context, tenantID, follower, followee string) (bool, error)
	FollowingGCIDs(ctx context.Context, tenantID, gcid string) ([]string, error)
	FollowersGCIDs(ctx context.Context, tenantID, gcid string) ([]string, error)
	Block(ctx context.Context, tenantID, blocker, blocked string) error
	Unblock(ctx context.Context, tenantID, blocker, blocked string) (bool, error)
	BlockedBy(ctx context.Context, tenantID, gcid string) ([]string, error)
}

// firstNonEmptyEnv returns the first non-empty env var among names ("" when
// none is set — the callee applies its documented default).
func firstNonEmptyEnv(names ...string) string {
	for _, n := range names {
		if v := strings.TrimSpace(os.Getenv(n)); v != "" {
			return v
		}
	}
	return ""
}

// profileBrokerAdapter bridges *ws.ProfileBroker (concrete, in the ws
// adapter package) to httpadapter.ProfileMessagePublisher (the narrow
// port the REST handler publishes through). Defined here in the
// composition root so neither package imports the other — the ws
// adapter stays hexagonally pure (knows nothing of httpadapter), and
// httpadapter stays free of the ws import.
//
// The kind↔ws.ProfileMessageKind mapping is literal (same string
// values: "profile_ready" / "profile_error") so the FE sees one stable
// frame shape regardless of which layer emitted it.
type profileBrokerAdapter struct {
	broker *ws.ProfileBroker
}

func (a profileBrokerAdapter) PublishProfile(gcid string, kind httpadapter.ProfileMessageKind, payload []byte) {
	if a.broker == nil {
		return
	}
	a.broker.Publish(gcid, ws.ProfileMessage{
		Kind:    ws.ProfileMessageKind(string(kind)),
		Payload: payload,
	})
}
