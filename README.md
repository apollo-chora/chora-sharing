# chora-sharing

## About

`chora-sharing` is a Go service that owns Chora's content-sharing and social features. It provides atom sharing and grants, posts with reactions and comments, social connections, bookmarks, discovery projections, profiles, leaderboards, and real-time duels with matchmaking and ELO ratings. The service exposes REST and gRPC APIs and uses PostgreSQL and NATS JetStream when those dependencies are configured, with in-memory adapters available for local development.

## Quick start

Prerequisites:

- Go 1.26.1 or newer
- No external services are required for the basic in-memory development mode

Clone the repository and start the server:

```sh
git clone https://github.com/apollo-chora/chora-sharing.git
cd chora-sharing
go run ./cmd/server
```

The server listens on HTTP port `8080` and gRPC port `9090` by default.

For a local environment file:

```sh
cp .env.example .env
```

Set `CHORA_DB_DSN` and `NATS_URL` when you want PostgreSQL persistence and NATS JetStream instead of the in-memory adapters. `CHORA_OUTBOX_DSN` enables the PostgreSQL-backed outbox.

## Usage

The HTTP API is served on `PORT` (default `8080`). Health checks do not require identity headers:

```text
GET /healthz
GET /readyz
```

The versioned REST API is under `/v1`. Requests to `/v1/*` require the gateway-provided `gcid` and `X-Tenant-Id` identity headers.

Main REST routes include:

| Area | Routes |
| --- | --- |
| Atom sharing | `POST /v1/atoms/{atom_id}/share`, `DELETE /v1/atoms/{atom_id}/share`, `GET /v1/feed/shared-atoms` |
| Bookmarks | `POST /v1/atoms/{atom_id}/bookmark`, `DELETE /v1/atoms/{atom_id}/bookmark`, `GET /v1/me/bookmarks` |
| Connections | `GET /v1/connections`, `POST /v1/connections/follows`, `DELETE /v1/connections/follows/{gcid}`, `POST /v1/connections/blocks`, `DELETE /v1/connections/blocks/{gcid}`, `GET /v1/connections/suggestions` |
| Profiles | `GET /v1/me/profile`, `POST /v1/me/profile/generate`, `PUT /v1/me/profile/tags`, `GET /v1/me/profile/ws` |
| Posts and social activity | `POST /v1/posts/{post_id}/reactions`, `DELETE /v1/posts/{post_id}/reactions/{reaction_id}`, `POST /v1/posts/{post_id}/comments`, `GET /v1/posts/{post_id}/comments`, `PATCH /v1/posts/{post_id}/comments/{comment_id}`, `DELETE /v1/posts/{post_id}/comments/{comment_id}`, `GET /v1/leaderboard` |
| Duels | `GET /v1/duels`, `GET /v1/duels/{duel_id}`, `POST /v1/duels/{duel_id}/answer`, `GET /v1/duels/{duel_id}/ws`, `GET /v1/duels/my-rating`, `GET /v1/duels/leaderboard` |
| Matchmaking | `POST /v1/duels/queue`, `DELETE /v1/duels/queue`, `POST /v1/duels/queue/heartbeat`, `GET /v1/duels/queue/status` |
| Familiar-milestone drafts | `GET /v1/me/post-drafts`, `POST /v1/me/post-drafts/{draft_id}/publish`, `POST /v1/me/post-drafts/{draft_id}/discard`, `GET /v1/me/preferences/familiar-milestone-share`, `POST /v1/me/preferences/familiar-milestone-share` |

The service also mounts three internal event-push inboxes:

```text
POST /api/internal/pubsub/course-published
POST /api/internal/pubsub/weakness-grown
POST /api/internal/pubsub/live-quiz-scores
```

The gRPC server listens on `CHORA_GRPC_PORT` (default `9090`) and registers the `chora.services.sharing.v1.Sharing` service, gRPC health checks, and reflection. The Sharing service implements RPCs for social relationships, posts and reactions, atom sharing and authorization, saved and entitled atoms, live-quiz atom authorization, leaderboard access, and quiz generation.

For production-style persistence and event delivery, configure at least:

```sh
export CHORA_DB_DSN='postgres://user:password@host:5432/chora_sharing?sslmode=disable'
export CHORA_OUTBOX_DSN='postgres://user:password@host:5432/chora_sharing?sslmode=disable'
export NATS_URL='nats://127.0.0.1:4222'
```

Apply the SQL migrations from `migrations/` with the shared Chora migration runner. Forward migrations are the `*.up.sql` files; the runner uses `CHORA_MIGRATE_DSN` and the migrations directory mounted at `/migrations`.

Configuration is environment-driven. Common settings are `PORT`, `CHORA_GRPC_PORT`, `CHORA_DB_DSN`, `CHORA_OUTBOX_DSN`, `NATS_URL`, `SVC_IDENTITY_GRPC_URL`, `MODERATION_ENGINE_GKE_ENDPOINT`, `PROFILE_CONJURER_GKE_ENDPOINT`, `DUEL_ATOM_SMITH_GKE_ENDPOINT`, `CHORA_PUBSUB_PUSH_AUDIENCE_BASE`, `CHORA_FAMILIAR_TEMPLATES_PATH`, `CHORA_PII_CLOSURE_MAP_PATH`, `CHORA_AGENT_GUARDRAIL_MAPPING`, `CHORA_SHARING_ROYALTY_CURRENCY`, `CHORA_SHARING_MANA_ACTION_CODE`, and `OTEL_EXPORTER_OTLP_ENDPOINT`. Duel, leaderboard, royalty, matchmaking, round-timer, and Blitz rules are loaded from the `CHORA_SHARING_*` environment variables defined in `internal/config/sharing_rules.go`.

## Development

The repository is a single Go module:

```text
cmd/server/                    service entrypoint and dependency wiring
internal/domain/               domain aggregates and business rules
internal/adapter/http/         REST handlers and event-push handlers
internal/adapter/grpc/         gRPC service implementation
internal/adapter/pg/            PostgreSQL repositories
internal/adapter/inmem/         in-memory development adapters
internal/adapter/events/       event publisher adapters
internal/adapter/outbox/        PostgreSQL outbox support
internal/adapter/ws/            WebSocket duel and profile handlers
internal/adapter/matchmaking/   matchmaking implementation
internal/adapter/subscribers/   inbound event subscribers
internal/config/               environment-driven sharing rules
config/                        YAML configuration used by the service
migrations/                    PostgreSQL schema migrations
```

Run the unit test suite with:

```sh
go test ./...
```

The repository also contains integration tests behind the `integration` build tag. They use real PostgreSQL when `CHORA_TEST_DSN` is set and otherwise skip.

Build the server binary with:

```sh
go build ./cmd/server
```

The included `Dockerfile` builds a static Linux binary and packages it in an Alpine-based runtime image.