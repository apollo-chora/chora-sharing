# chora-sharing

Social + engagement service for Chora: the C+ content-sharing plane. It owns
posts, reactions, comments, the social graph (follow / block), atom sharing
with grants + royalties, real-time duels (ELO + matchmaking + WebSocket),
leaderboards, bookmarks, the cross-domain discovery feed, familiar-milestone
auto-posting, and profile generation.

The service is cloud-neutral: PostgreSQL for persistence, NATS JetStream for
events, env-backed configuration for secrets. No cloud account or managed
services (managed SQL, Pub/Sub, Secret Manager, or CI/CD) are required.

## What it does

1. **Social graph** — follow / unfollow / block / unblock / suggestions, with
   relationship events written in the same transaction as the state change.
2. **Posts + reactions + comments** — pg-backed (RLS-aware) or in-memory in
   dev; keyset-paginated comment feeds.
3. **Atom sharing** — share / revoke atoms, grants + royalty tracking,
   bookmarks, and the shared-atom feed.
4. **Duels** — PG-backed matchmaking queue (multi-pod safe), ELO rating,
   real-time WebSocket play (`/v1/duels/{duel_id}/ws`), round-timer sweep,
   blitz mode.
5. **Leaderboards** — weekly + all-time boards fed by live-quiz scores, duel
   completions, and weakness-grown XP.
6. **Discovery feed** — cross-domain projection of
   `chora.delivery.course.published.v1` and
   `chora.consumption.atom_session.completed.v1` into the C+ discovery feed.
7. **Familiar milestones** — consumes the four
   `chora.consumption.familiar.*.v1` topics and auto-posts / drafts /
   suppresses per-user preference (configurable templates + default policy).
8. **Event-push inboxes** — HTTP receivers the gateway forwards
   `/api/internal/pubsub/{course-published, weakness-grown, live-quiz-scores}`
   to; the service refuses to boot if any assigned inbox is unmounted.
9. **Federated closure saga** — consumes
   `chora.sharing.pii.pseudonymise.requested.v1`, applies the per-domain
   `PII_Closure_Map.yaml`, and acks on
   `chora.sharing.account.pseudonymised.v1`.

## Architecture

- **Compute**: any host running the Go binary or the container image.
- **Database**: PostgreSQL (`chora_sharing`). Schema changes live in
  `migrations/` and are applied with the shared migration runner.
- **Event bus**: NATS JetStream via `chora-common/eventbus`. Outbound events
  are written to `sharing_outbox_events` in the same transaction as the
  domain state change; the outbox dispatcher drains pending rows to the bus.
  Inbound cross-service events arrive either on the bus (durable JetStream
  consumers) or via the HTTP push inboxes above.
- **Ports**: HTTP `:8080` (REST + `/healthz` + `/readyz` + push inboxes);
  gRPC `:9090` (`chora.services.sharing.v1.Sharing` + health + reflection).

## Configuration

Copy the example environment file:

```sh
cp .env.example .env
```

Important variables:

| Variable | Purpose | Local default |
| --- | --- | --- |
| `PORT` | HTTP port | `8080` |
| `CHORA_GRPC_PORT` | gRPC port | `9090` |
| `CHORA_DB_DSN` | PostgreSQL connection string (app_rw role) | unset (in-memory repos) |
| `CHORA_OUTBOX_DSN` | Outbox database DSN (unset → no outbox dispatcher) | unset |
| `CHORA_OUTBOX_WORKER_ID` | Outbox dispatcher worker id | `$HOSTNAME` / `chora-sharing-local` |
| `NATS_URL` | NATS JetStream event bus | unset (in-memory bus) |
| `CHORA_SOURCE_PROJECT` | Project label stamped into event envelopes | `chora-local` |
| `SVC_IDENTITY_GRPC_URL` | chora-identity gRPC (display_name + mana) | unset |
| `MODERATION_ENGINE_GKE_ENDPOINT` | Moderation crew (GKE web-mode ADK) | unset |
| `PROFILE_CONJURER_GKE_ENDPOINT` | profile_conjurer crew (GKE web-mode ADK) | unset |
| `DUEL_ATOM_SMITH_GKE_ENDPOINT` | duel_atom_smith crew (GKE web-mode ADK) | unset |
| `CHORA_PUBSUB_PUSH_AUDIENCE_BASE` | Base URL for push-inbox bearer verification | unset (verification off) |
| `CHORA_FAMILIAR_TEMPLATES_PATH` | Familiar-milestone template YAML override | embedded defaults |
| `CHORA_PII_CLOSURE_MAP_PATH` | PII closure map YAML | `config/PII_Closure_Map.yaml` |
| `CHORA_AGENT_GUARDRAIL_MAPPING` | Guardrail template-tier mapping YAML | `/etc/chora/agent-guardrail-mapping.yaml` |
| `CHORA_SHARING_ROYALTY_CURRENCY` | Royalty currency code | (required) |
| `CHORA_SHARING_MANA_ACTION_CODE` | Mana action code for royalty debit | (required) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP/gRPC trace endpoint | unset (stdout) |

Duel / leaderboard / matchmaking tuning lives in `internal/config` and is
env-driven (`CHORA_SHARING_ELO_*`, `CHORA_SHARING_COMBO_TIERS`,
`CHORA_SHARING_MM_*`, `CHORA_SHARING_ROUND_TIMER_SEC`, …) — see
`internal/config/sharing_rules.go` for the full set with defaults.

## Run locally

```sh
go run ./cmd/server
```

With `CHORA_DB_DSN` and `NATS_URL` set, the service wires the pg repos, the
outbox dispatcher, and the durable JetStream subscribers. Unset, it runs on
in-memory adapters (not durable).

## Database migrations

Forward migrations are every `migrations/*.sql` except `*.down.sql`. Apply
them with the shared runner from `chora-stack/scripts/migrate.sh` (mount the
repo's `migrations/` at `/migrations` and set `CHORA_MIGRATE_DSN`).

## Tests

```sh
go test ./...                 # unit tests (hermetic)
```

Integration tests (build-tagged `integration`) run against real PostgreSQL
when `CHORA_TEST_DSN` is set; they skip otherwise.
