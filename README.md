# chora-sharing

> The social layer: atom sharing, bookmarks, connections, profiles, posts and reactions,
> duels and matchmaking, and the leaderboard.

Go, HTTP/JSON plus gRPC. PostgreSQL for durable state, a transactional outbox to NATS
JetStream, WebSockets for the live surfaces.

## What it is

Everything a learner does with *other* learners lives here:

- **Atom sharing** — share or unshare an atom, and the shared-atom feed.
- **Bookmarks** — save an atom, and list your own.
- **Connections** — follow, block, list, and the suggestion surface.
- **Profiles** — read your profile, regenerate it, set tags, and a WebSocket for live updates.
- **Posts and social activity** — reactions, comments (create, list, edit, delete), and the
  leaderboard.
- **Duels** — list, fetch, answer, a WebSocket for live duels, your rating, and the duel
  leaderboard.
- **Matchmaking** — queue, leave, heartbeat, and status.
- **Familiar-milestone drafts** — list drafts, publish, discard, and the share preference.

It serves HTTP on `PORT` (default `8080`) and gRPC on `CHORA_GRPC_PORT` (default `9090`). In the
stack both are `expose`d and neither is published.

### Two features degrade in this stack, and the log says which

Both are cloud-only AI integrations that have no local equivalent:

```text
sharing: profile_conjurer agent NOT wired (PROFILE_CONJURER_GKE_ENDPOINT unset — profile generate will 501)
sharing: duel_atom_smith NOT wired — using projection-based atom selector (no AI; local dev fallback)
```

| Feature | State in the stack | Consequence |
| --- | --- | --- |
| Profile generation | **`501`** | `POST /v1/me/profile/generate` refuses; reading and tagging a profile still work |
| Duel atom selection | **projection fallback** | duels still run, but atoms are chosen from the existing projection rather than generated |
| Social moderation | **wired (local screener)** | logs `Cloud Model Armor guardrail wired`, but runs the in-tree deterministic screener, not the managed service |
| Event-push inboxes | **mounted, unverified** | all three mounted: `course-published`, `weakness-grown`, `live-quiz-scores`; bearer check is off until the audience base is set |

That is a useful split to hold on to: the *social* features are fully live, and the two AI
embellishments are the ones missing. A `501` from profile-generate is configuration, not a bug.

### Identity headers

Health checks need nothing. Every `/v1/*` route requires the gateway-provided `gcid` **and**
`X-Tenant-Id` headers; with either missing you get a `400`, which is easy to mistake for a bad
request body.

## Prerequisites

| Route | What you need |
| --- | --- |
| In the stack | A working `chora-stack` deployment. PostgreSQL, NATS and identity come from it. |
| Standalone | Go **1.26.1 or newer**, PostgreSQL with this repository's schema, and NATS JetStream. |
| Profile generation | A profile-conjurer endpoint (`PROFILE_CONJURER_GKE_ENDPOINT`) — cloud-only, absent locally. |
| Duel atom generation | A duel atom-smith endpoint (`DUEL_ATOM_SMITH_GKE_ENDPOINT`) — otherwise the projection fallback runs. |
| WebSockets | A client that can hold a connection; the duel and profile sockets are the same listener. |

## Credentials

This service holds **no provider credentials**. Its AI dependencies are reached through
platform endpoints, and the guardrail is cloud-provided.

| Value | Where it comes from | Variable |
| --- | --- | --- |
| PostgreSQL password | the stack's `POSTGRES_PASSWORD`, inside both DSNs as `chora_sharing_app_rw` | `CHORA_DB_DSN`, `CHORA_OUTBOX_DSN` |
| NATS endpoint | the stack's NATS service; no credential locally | `NATS_URL` |
| Identity gRPC | the stack's identity service | `SVC_IDENTITY_GRPC_URL` |
| Cloud AI endpoints | **not set in the stack**; supply them to enable profile/duel generation | `PROFILE_CONJURER_GKE_ENDPOINT`, `DUEL_ATOM_SMITH_GKE_ENDPOINT`, `MODERATION_ENGINE_GKE_ENDPOINT` |

Where the stack's shared values come from, and how to obtain the provider keys that *other*
services need, is documented once in
[`chora-stack/README.md`](../chora-stack/README.md#credentials).

## Run it in the stack

`compose.yml` does four things:

1. Builds the image from the sibling checkout at `../chora-sharing`.
2. Runs `sharing-migrate` first — a `postgres:18.6-alpine` one-shot applying this repository's
   migrations to `chora_sharing`.
3. Starts the service once `postgres` and `nats` are healthy, `nats-init` has completed, and the
   migrate one-shot completed successfully.
4. Publishes no host port; it `expose`s `8080` and `9090`.

```sh
cd chora-stack
docker compose up -d sharing
docker compose ps sharing
```

What Compose sets: `PORT`, `CHORA_GRPC_PORT`, the two DSNs, `NATS_URL`,
`CHORA_SOURCE_PROJECT`, `SVC_IDENTITY_GRPC_URL` and `OTEL_EXPORTER_OTLP_ENDPOINT`. Nothing for
the three cloud AI endpoints — hence the two degradations above.

Probe it from inside the network:

```sh
docker compose exec -T nats wget -qO- http://sharing:8080/readyz
```

```json
{"status":"ready"}
```

`readyz` here is bare — `{"status":"ok"}` and `{"status":"ready"}` are the whole bodies. It does
not report the database, the guardrail or the AI endpoints, so the boot log is the better
inventory.

## Run it standalone

```sh
git clone https://github.com/apollo-chora/chora-sharing.git
cd chora-sharing
go build ./cmd/server
./server
```

For durable operation:

```sh
export CHORA_DB_DSN='postgres://user:password@host:5432/chora_sharing?sslmode=disable'
export CHORA_OUTBOX_DSN="$CHORA_DB_DSN"
export NATS_URL='nats://127.0.0.1:4222'
./server
```

Apply the migrations first — the service does not migrate itself. Use the stack's
`scripts/migrate.sh` or an equivalent runner over `migrations/` (the runner reads
`CHORA_MIGRATE_DSN` and expects the directory at `/migrations` in the container form).

```sh
go test ./...
docker build -t chora-sharing .
```

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `8080` | HTTP listen port |
| `CHORA_GRPC_PORT` | `9090` | gRPC listen port |
| `CHORA_DB_DSN` | — | PostgreSQL DSN |
| `CHORA_OUTBOX_DSN` | — | outbox DSN (the same database in this stack) |
| `NATS_URL` | — | NATS JetStream |
| `SVC_IDENTITY_GRPC_URL` | unset (stack: `identity:9090`) | identity lookups |
| `PROFILE_CONJURER_GKE_ENDPOINT` | unset → profile generate `501` | profile-generation agent |
| `DUEL_ATOM_SMITH_GKE_ENDPOINT` | unset → projection fallback | duel atom selection agent |
| `MODERATION_ENGINE_GKE_ENDPOINT` | unset; guardrail still wired in the stack | external moderation engine |
| `CHORA_PUBSUB_PUSH_AUDIENCE_BASE` | unset | base URL the inboxes derive their audience from; unset **disables** bearer verification |
| `CHORA_FAMILIAR_TEMPLATES_PATH` | bundled | Familiar milestone templates |
| `CHORA_PII_CLOSURE_MAP_PATH` | `config/PII_Closure_Map.yaml` | closure map |
| `CHORA_AGENT_GUARDRAIL_MAPPING` | bundled `config/agent-guardrail-mapping.yaml` | agent → guardrail tier |
| `CHORA_SHARING_ROYALTY_CURRENCY` | domain default | royalty currency |
| `CHORA_SHARING_MANA_ACTION_CODE` | domain default | mana action code |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | OTLP/gRPC trace endpoint |

Duel, leaderboard, royalty, matchmaking, round-timer and Blitz rules all come from
`CHORA_SHARING_*` variables. Each has a working domain default, so the stack boots without any
of them — set one only to tune the rule. The complete set, read from the code:

`CHORA_SHARING_ELO_BASELINE`, `CHORA_SHARING_ELO_K_FACTOR`, `CHORA_SHARING_COMBO_TIERS`,
`CHORA_SHARING_ROYALTY_BASE`, `CHORA_SHARING_ROYALTY_CAP`, `CHORA_SHARING_ROYALTY_CURRENCY`,
`CHORA_SHARING_MANA_ACTION_CODE`, `CHORA_SHARING_WEAKNESS_GROWN_XP`,
`CHORA_SHARING_LIVEQUIZ_PER_SESSION_CAP`, `CHORA_SHARING_SEASON_WINDOW`,
`CHORA_SHARING_WS_GRACE`, `CHORA_SHARING_ROUND_TIMER_SEC`, `CHORA_SHARING_MM_TIMEOUT`,
`CHORA_SHARING_MM_HEARTBEAT_STALE`, `CHORA_SHARING_MM_MATCH_TICK`,
`CHORA_SHARING_MM_SWEEP_TICK`, `CHORA_SHARING_BLITZ_TIME_LIMIT_SEC`,
`CHORA_SHARING_BLITZ_RACE_TARGET`.

## Verify it works

### It is up

```sh
docker compose exec -T nats wget -qO- http://sharing:8080/healthz
docker compose exec -T nats wget -qO- http://sharing:8080/readyz
```

```json
{"status":"ok"}
{"status":"ready"}
```

### Which AI features are live

```sh
docker compose logs sharing | grep -E 'conjurer|atom_smith|guardrail|event-push'
```

Expect four lines in this stack: profile conjurer **not wired**, duel atom smith **not wired**
(fallback), guardrail **wired**, and event-push inboxes **mounted (3)**. That quartet is the
fastest capability summary this service offers.

### Its migrations landed

```sh
docker compose exec -T postgres psql -U chora_sharing_migrate -d chora_sharing \
  -c "select count(*) from schema_migrations;"
```

Expect a count at or just under the 40 forward files listed under [Migrations](#migrations).

### The social surface answers

```sh
docker compose exec -T nats wget -qO- -S \
  --header='X-Tenant-Id: 00000000-0000-7000-8000-000000000001' \
  --header='gcid: 1ebe8ad8-218d-5185-ab8a-f64fcf558f8d' \
  http://sharing:8080/v1/me/bookmarks
```

A `2xx` with a list is the pass. A `400` means `gcid` or `X-Tenant-Id` did not arrive — both are
required on every `/v1/*` route.

### Profile generation is correctly disabled

```sh
docker compose exec -T nats wget -qO- -S -X POST \
  --header='X-Tenant-Id: 00000000-0000-7000-8000-000000000001' \
  --header='gcid: 1ebe8ad8-218d-5185-ab8a-f64fcf558f8d' \
  http://sharing:8080/v1/me/profile/generate
```

`501` is the **expected** answer here: no profile conjurer is configured. A `2xx` would mean a
conjurer endpoint is wired.

## Troubleshooting

- **`POST /v1/me/profile/generate` returns `501`** — `PROFILE_CONJURER_GKE_ENDPOINT` is unset.
  Expected in this stack; profile *reads* and tag updates still work.
- **Duel atoms look repetitive** — the atom smith is not wired, so the projection-based selector
  is choosing from atoms that already exist. That is the documented local fallback, not a fault.
- **A `400` on any `/v1/*` route** — `gcid` and `X-Tenant-Id` are both required. Check the header
  names exactly; `X-Chora-GCID` is not accepted here.
- **Profile or duel WebSocket never connects** — the sockets are served on the same HTTP port as
  the REST API. From the host there is no published port, so a browser cannot reach them
  directly; go through the gateway.
- **The event-push inboxes accept unauthenticated pushes** — with
  `CHORA_PUBSUB_PUSH_AUDIENCE_BASE` unset, bearer verification is **off**, not failing closed.
  The inboxes are mounted and serve anything the network lets through; the real gate is the mesh
  `AuthorizationPolicy` restricting `/api/internal/pubsub/*` to the `chora-gateway` service
  account. Outside a mesh, set the base URL to turn verification on. Do not set a single
  `CHORA_PUBSUB_PUSH_AUDIENCE` for all three — each inbox needs its own audience, so the base is
  combined with each inbox path.
- **Moderation behaves unexpectedly** — what logs as `Cloud Model Armor guardrail wired` is the
  *local deterministic screener* from `chora-common/modelarmor`, not the managed service: the
  managed adapter was removed with the cloud exit and production traffic is expected to front
  this with a real guardrail. It runs pre-flight on `agent_id=social_moderation`; an unset
  `MODERATION_ENGINE_GKE_ENDPOINT` removes the external engine, not the screener.
- **Duel or matchmaking rules look wrong** — those are `CHORA_SHARING_*` environment values, not
  code. The full set is listed under Configuration.
- **Traces never reach Tempo** — this service logs
  `invalid target address http://otel-collector:4317 ... too many colons in address`, like the
  other Go services configured with an `http://` prefix. Observed in the running stack; only
  tracing is affected.

## API surface

Health needs no headers; every `/v1/*` route requires `gcid` and `X-Tenant-Id`.

| Area | Routes |
| --- | --- |
| Atom sharing | `POST` / `DELETE /v1/atoms/{atom_id}/share`, `GET /v1/feed/shared-atoms` |
| Bookmarks | `POST` / `DELETE /v1/atoms/{atom_id}/bookmark`, `GET /v1/me/bookmarks` |
| Connections | `GET /v1/connections`, `POST /v1/connections/follows`, `DELETE /v1/connections/follows/{gcid}`, `POST /v1/connections/blocks`, `DELETE /v1/connections/blocks/{gcid}`, `GET /v1/connections/suggestions` |
| Profiles | `GET /v1/me/profile`, `POST /v1/me/profile/generate`, `PUT /v1/me/profile/tags`, `GET /v1/me/profile/ws` |
| Posts and activity | `POST /v1/posts/{post_id}/reactions`, `DELETE .../reactions/{reaction_id}`, `POST` / `GET /v1/posts/{post_id}/comments`, `PATCH` / `DELETE /v1/posts/{post_id}/comments/{comment_id}`, `GET /v1/leaderboard` |
| Duels | `GET /v1/duels`, `GET /v1/duels/{duel_id}`, `POST /v1/duels/{duel_id}/answer`, `GET /v1/duels/{duel_id}/ws`, `GET /v1/duels/my-rating`, `GET /v1/duels/leaderboard` |
| Matchmaking | `POST` / `DELETE /v1/duels/queue`, `POST /v1/duels/queue/heartbeat`, `GET /v1/duels/queue/status` |
| Familiar-milestone drafts | `GET /v1/me/post-drafts`, `POST .../{draft_id}/publish`, `POST .../{draft_id}/discard`, `GET` / `POST /v1/me/preferences/familiar-milestone-share` |

Internal event-push inboxes (mounted, audience-checked):

```text
POST /api/internal/pubsub/course-published
POST /api/internal/pubsub/weakness-grown
POST /api/internal/pubsub/live-quiz-scores
```

gRPC on `CHORA_GRPC_PORT` registers `chora.services.sharing.v1.Sharing`, the standard health
service and **server reflection** — so `grpcurl` can list the contract without a proto file. The
service implements RPCs for social relationships, posts and reactions, atom sharing and
authorization, saved and entitled atoms, live-quiz atom authorization, leaderboard access, and
quiz generation.

## Architecture

```text
cmd/server/                  entrypoint and wiring
internal/domain/             aggregates and business rules
internal/adapter/http/       REST handlers and event-push handlers
internal/adapter/grpc/       Sharing service implementation
internal/adapter/pg/         PostgreSQL repositories
internal/adapter/inmem/      in-memory development adapters
internal/adapter/events/     event publishers
internal/adapter/outbox/     PostgreSQL outbox support
internal/adapter/ws/         WebSocket duel and profile handlers
internal/adapter/matchmaking/ matchmaking implementation
internal/adapter/subscribers/ inbound event subscribers
internal/config/             environment-driven sharing rules
config/                      YAML configuration (guardrail mapping, PII map, templates)
migrations/                  PostgreSQL schema
```

The distinguishing structural feature is `internal/config/sharing_rules.go`: the duel,
matchmaking, Blitz and royalty rules are *configuration*, not code. That is deliberate — it lets
a deployment retune the game without a rebuild — but it also means a surprising matchmaking
behaviour is usually an environment value rather than a bug.

## Events

The service both consumes and publishes.

| Direction | Mechanism |
| --- | --- |
| Inbound | three event-push inboxes over HTTP (bearer check when the audience base is set), plus NATS subscribers |
| Outbound | PostgreSQL transactional outbox → NATS JetStream |
| Streams | `CHORA_EVENTS` for normal subjects, `CHORA_DLQ` for dead letters |

The HTTP inboxes are a compatibility transport for events delivered by push rather than pull. They
are mounted unconditionally; `CHORA_PUBSUB_PUSH_AUDIENCE_BASE` only decides whether the bearer
token is verified. Leave it unset and the mesh policy is your only gate.

## Migrations

**40 forward migration files**, applied by the stack's `scripts/migrate.sh` in the
`sharing-migrate` one-shot against `chora_sharing`. The runner is not part of this repository.

Note the suffix convention, because the previous documentation got it slightly wrong: 39 of the
40 are `*.up.sql`, and **`9999_grant_app_roles.sql` is a plain `.sql`**. The runner's actual rule
is "every `*.sql` except `*.down.sql`", so that grant file is applied like any other forward
migration.

The set covers atom sharing, bookmarks, connections, profiles, posts, reactions, comments,
duels, matchmaking, drafts, royalty and the outbox.

Two carry requirements worth knowing:

- **`9999_grant_app_roles.sql`** carries the `_app_rw` grants; without it the service connects
  and every query fails on permissions.
- The UUIDv7 default migration switches primary keys to PostgreSQL 18's native `uuidv7()`. An
  older server fails the migrate one-shot rather than the service.

The `.down.sql` siblings are destructive rollbacks and are never applied, and the runner records
a SHA-256 checksum per file in `schema_migrations`, refusing a file whose contents changed after
application.
