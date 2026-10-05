// relationship_repo.go — pgx-backed social.RelationshipStore (ADR-230
// B-lite.1, CHO-2119).
//
// One RunRelationship call = one tenant-scoped transaction: the RLS session
// is applied first (fail-loud on empty tenant), then the service drives the
// RelationshipTx ops — including Enqueue, which INSERTs the event's
// sharing_outbox_events row through the SAME pgx transaction as the state
// writes. That is what makes relationship state-write + event-publish atomic
// (ADR-230 D4); the existing outbox Dispatcher drains the rows to Pub/Sub
// regardless of which writer inserted them.
//
// Pair canonicalisation happens IN SQL (LEAST/GREATEST on ::uuid) so the
// social_friendships CHECK (member_lo_gcid < member_hi_gcid) is authoritative
// under Postgres uuid ordering — never trusting a caller's ordering.
package pg

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"context"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/adapter/events/protomarshal"
	"github.com/apollo-chora/chora-sharing/internal/adapter/outbox"
	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

const (
	// SQLSelectPairState loads the block-either-direction boolean the
	// aggregate decides against, in one round trip. RLS scopes the subquery.
	SQLSelectPairState = `
SELECT
  EXISTS (SELECT 1 FROM social_blocks
          WHERE (blocker_gcid = $1 AND blocked_gcid = $2)
             OR (blocker_gcid = $2 AND blocked_gcid = $1)) AS blocked_either`

	// SQLFriendPartners resolves the friend set (ADR-230 D2): accepted
	// friendships minus pairs blocked in either direction. The friendship
	// write path is gone, so this always returns empty — kept for the
	// atom_reuse audience tier + GetReuseContext gRPC contract.
	SQLFriendPartners = `
SELECT CASE WHEN f.member_lo_gcid = $1 THEN f.member_hi_gcid ELSE f.member_lo_gcid END AS partner
FROM social_friendships f
WHERE (f.member_lo_gcid = $1 OR f.member_hi_gcid = $1)
  AND NOT EXISTS (
      SELECT 1 FROM social_blocks b
      WHERE (b.blocker_gcid = f.member_lo_gcid AND b.blocked_gcid = f.member_hi_gcid)
         OR (b.blocker_gcid = f.member_hi_gcid AND b.blocked_gcid = f.member_lo_gcid)
  )
ORDER BY partner`

	// SQLInsertOutboxEvent writes the event's outbox row through the domain
	// transaction (payload BYTEA = canonical binary protobuf; envelope JSONB).
	// Column list mirrors outbox.PostgresStore.Insert / migration 0007.
	SQLInsertOutboxEvent = `
INSERT INTO sharing_outbox_events (
    id, tenant_id, gcid, aggregate_type, aggregate_id, event_type, topic,
    payload, envelope, idempotency_key, occurred_at, status
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, 'pending'
)`
)

// relationshipTopics maps the domain event kinds onto the canonical
// chora.sharing.relationship.*.v1 spine (ADR-230 D4). Complete by
// construction — RelationshipTopic fails loud on an unknown kind so a new
// kind cannot ship without its topic.
var relationshipTopics = map[social.RelationshipEventKind]string{
	social.RelFollowed:   "chora.sharing.relationship.followed.v1",
	social.RelUnfollowed: "chora.sharing.relationship.unfollowed.v1",
	social.RelBlocked:    "chora.sharing.relationship.blocked.v1",
	social.RelUnblocked:  "chora.sharing.relationship.unblocked.v1",
}

// relationshipPayloadKeys names the per-topic payload fields (actor, subject,
// timestamp) matching proto/events/sharing/relationship.proto — field numbers
var relationshipPayloadKeys = map[social.RelationshipEventKind][3]string{
	social.RelFollowed:   {"follower_gcid", "followee_gcid", "followed_at"},
	social.RelUnfollowed: {"follower_gcid", "followee_gcid", "unfollowed_at"},
	social.RelBlocked:    {"blocker_gcid", "blocked_gcid", "blocked_at"},
	social.RelUnblocked:  {"unblocker_gcid", "unblocked_gcid", "unblocked_at"},
}

// RelationshipTopic resolves the Pub/Sub topic for a relationship event kind.
func RelationshipTopic(kind social.RelationshipEventKind) (string, error) {
	topic, ok := relationshipTopics[kind]
	if !ok {
		return "", fmt.Errorf("pg: no topic registered for relationship event kind %q", kind)
	}
	return topic, nil
}

// RelationshipRepoOptions tunes the repo at construction time.
type RelationshipRepoOptions struct {
	// SourceProject / SourceService stamp the event envelopes. Defaults:
	// chora-local / chora-sharing.
	SourceProject string
	SourceService string

	// Now is injectable for tests; defaults to time.Now().UTC.
	Now func() time.Time
}

// RelationshipRepo is the pgx-backed social.RelationshipStore.
type RelationshipRepo struct {
	tx   TxRunner
	opts RelationshipRepoOptions
}

// NewRelationshipRepo constructs the repo around a TxRunner.
func NewRelationshipRepo(tx TxRunner, opts RelationshipRepoOptions) *RelationshipRepo {
	if opts.SourceProject == "" {
		opts.SourceProject = "chora-local"
	}
	if opts.SourceService == "" {
		opts.SourceService = "chora-sharing"
	}
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Now().UTC() }
	}
	return &RelationshipRepo{tx: tx, opts: opts}
}

// RunRelationship opens one tenant-scoped transaction: stamps the tenant,
// applies the RLS session (fail-loud on empty tenant), then hands the
// transactional op surface to fn.
func (r *RelationshipRepo) RunRelationship(ctx context.Context, tenantID string, fn func(ctx context.Context, tx social.RelationshipTx) error) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	ctx = tracing.WithTenantID(ctx, strings.TrimSpace(tenantID))
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		return fn(ctx, &relationshipTx{q: q, repo: r})
	})
}


// relationshipTx implements social.RelationshipTx over one open transaction.
type relationshipTx struct {
	q    Querier
	repo *RelationshipRepo
}

// PairState loads the block-either-direction boolean in one query.
func (t *relationshipTx) PairState(ctx context.Context, caller, other string) (social.PairState, error) {
	var st social.PairState
	if err := t.q.QueryRow(ctx, SQLSelectPairState, caller, other).
		Scan(&st.BlockedEither); err != nil {
		return social.PairState{}, fmt.Errorf("pg: pair state: %w", err)
	}
	return st, nil
}

// InsertFollow inserts the edge; a conflict loads + returns the ORIGINAL edge
// unchanged with created=false (WS-0 semantics preserved).
func (t *relationshipTx) InsertFollow(ctx context.Context, e *social.Edge) (bool, *social.Edge, error) {
	tag, err := t.q.Exec(ctx, SQLInsertSocialFollow,
		e.ID, e.TenantID, e.FollowerGCID, e.FolloweeGCID, e.CreatedAt)
	if err != nil {
		return false, nil, fmt.Errorf("pg: insert social follow: %w", err)
	}
	if tag.RowsAffected == 1 {
		return true, e, nil
	}
	existing := &social.Edge{FollowerGCID: e.FollowerGCID, FolloweeGCID: e.FolloweeGCID}
	var createdAt time.Time
	if err := t.q.QueryRow(ctx, SQLSelectSocialFollow, e.FollowerGCID, e.FolloweeGCID).
		Scan(&existing.ID, &existing.TenantID, &createdAt); err != nil {
		return false, nil, fmt.Errorf("pg: select social follow after conflict: %w", err)
	}
	existing.CreatedAt = createdAt
	return false, existing, nil
}

// DeleteFollow removes one directional edge.
func (t *relationshipTx) DeleteFollow(ctx context.Context, follower, followee string) (bool, error) {
	tag, err := t.q.Exec(ctx, SQLDeleteSocialFollow, follower, followee)
	if err != nil {
		return false, fmt.Errorf("pg: delete social follow: %w", err)
	}
	return tag.RowsAffected > 0, nil
}

// InsertBlock writes the block edge; created=false on conflict (re-block).
func (t *relationshipTx) InsertBlock(ctx context.Context, b *social.Block) (bool, error) {
	tag, err := t.q.Exec(ctx, SQLInsertSocialBlock,
		b.ID, b.TenantID, b.BlockerGCID, b.BlockedGCID, b.CreatedAt)
	if err != nil {
		return false, fmt.Errorf("pg: insert social block: %w", err)
	}
	return tag.RowsAffected == 1, nil
}

// DeleteBlock removes the block edge.
func (t *relationshipTx) DeleteBlock(ctx context.Context, blocker, blocked string) (bool, error) {
	tag, err := t.q.Exec(ctx, SQLDeleteSocialBlock, blocker, blocked)
	if err != nil {
		return false, fmt.Errorf("pg: delete social block: %w", err)
	}
	return tag.RowsAffected > 0, nil
}

// Enqueue writes the event's outbox row in THIS transaction (ADR-230 D4):
// canonical binary protobuf payload (Schema Registry BINARY validation),
// full envelope, topic + envelope validated via outbox.BuildRow.
func (t *relationshipTx) Enqueue(ctx context.Context, ev social.RelationshipEvent) error {
	topic, err := RelationshipTopic(ev.Kind)
	if err != nil {
		return err
	}
	keys, ok := relationshipPayloadKeys[ev.Kind]
	if !ok {
		return fmt.Errorf("pg: no payload layout for relationship event kind %q", ev.Kind)
	}

	occurred := ev.OccurredAt
	if occurred.IsZero() {
		occurred = t.repo.opts.Now()
	}
	env := cgcenvelope.Build(ctx, cgcenvelope.BuildOpts{
		SchemaVersion:      1,
		SourceProject:      t.repo.opts.SourceProject,
		SourceService:      t.repo.opts.SourceService,
		ChoraImdaDimension: "accountability",
		ImdaLifecycleStage: "runtime",
		Now:                t.repo.opts.Now,
	})
	env.GCID = ev.ActorGCID
	env.OccurredAt = occurred

	payload := map[string]any{
		keys[0]:                ev.ActorGCID,
		keys[1]:                ev.SubjectGCID,
		keys[2]:                occurred,
		"tenant_id":            ev.TenantID,
		"occurred_at":          occurred.Format(time.RFC3339Nano),
		"chora_imda_dimension": "accountability",
		"imda_lifecycle_stage": "runtime",
	}
	body, err := protomarshal.MarshalPayload(topic, protomarshal.Envelope{
		EventID:        env.EventID,
		IdempotencyKey: env.IdempotencyKey,
		TenantID:       env.TenantID,
		GCID:           env.GCID,
		OccurredAt:     env.OccurredAt,
		PublishedAt:    env.PublishedAt,
		Traceparent:    env.Traceparent,
		Tracestate:     env.Tracestate,
		SourceProject:  env.SourceProject,
		SourceService:  env.SourceService,
		SchemaVersion:  env.SchemaVersion,
	}, payload)
	if err != nil {
		return fmt.Errorf("pg: marshal relationship event %s: %w", topic, err)
	}

	row, err := outbox.BuildRow(topic, env, body)
	if err != nil {
		return fmt.Errorf("pg: build outbox row %s: %w", topic, err)
	}
	envJSON, err := json.Marshal(row.Envelope)
	if err != nil {
		return fmt.Errorf("pg: marshal outbox envelope: %w", err)
	}
	if _, err := t.q.Exec(ctx, SQLInsertOutboxEvent,
		row.ID, row.TenantID, nullableOutboxUUID(row.GCID), row.AggregateType, row.AggregateID,
		row.EventType, row.Topic, row.Payload, string(envJSON), row.IdempotencyKey, row.OccurredAt,
	); err != nil {
		return fmt.Errorf("pg: insert outbox event %s: %w", topic, err)
	}
	return nil
}

// nullableOutboxUUID mirrors outbox.PostgresStore's NULL handling for the
// nullable gcid column.
func nullableOutboxUUID(s string) any {
	if s == "" {
		return nil
	}
	return s
}
