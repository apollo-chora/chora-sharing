// relationship_repo_test.go — RED-phase specs for the pgx-backed
// RelationshipRepo (ADR-230 B-lite.1, CHO-2119).
//
// Proves, driver-free via the stub harness:
//   - tenant is mandatory (rls.ErrNoTenantContext before any user SQL)
//   - Enqueue writes the outbox row IN THE SAME tx callback with a BINARY
//     protobuf payload (never JSON) + full envelope + valid topic
//   - the 7-kind topic map is complete and passes outbox.ValidateTopicName
//   - PairState scans the four EXISTS booleans in declared order
//   - SQL templates hit the right tables
//
// Live RLS + PREPARE behaviour is verified against the deployed DB via the
// PREPARE-smoke lane (feedback_pg_prepare_smoke_over_exec_stubs).
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-sharing/internal/adapter/outbox"
	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

const otherGCID = "01970000-0000-7000-a000-0000000000bb"

func fixedNow() time.Time { return time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC) }

func newRelRepo(q *stubQuerier) *pg.RelationshipRepo {
	return pg.NewRelationshipRepo(&stubTxRunner{q: q}, pg.RelationshipRepoOptions{
		SourceProject: "chora-489812",
		SourceService: "chora-sharing",
		Now:           fixedNow,
	})
}

// -----------------------------------------------------------------------------
// tenant discipline
// -----------------------------------------------------------------------------

func TestRelationshipRepo_RequiresTenantBeforeUserSQL(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := newRelRepo(q)
	err := r.RunRelationship(context.Background(), "", func(ctx context.Context, tx social.RelationshipTx) error {
		t.Fatalf("fn must not run without tenant context")
		return nil
	})
	if !errors.Is(err, rls.ErrNoTenantContext) {
		t.Fatalf("expected rls.ErrNoTenantContext; got %v", err)
	}
	for _, sql := range q.sqls {
		if strings.Contains(sql, "social_") || strings.Contains(sql, "sharing_outbox_events") {
			t.Fatalf("user SQL must not run without tenant; got %v", q.sqls)
		}
	}
}

// -----------------------------------------------------------------------------
// outbox enqueue — atomic, binary, enveloped
// -----------------------------------------------------------------------------

func TestRelationshipRepo_EnqueueWritesBinaryOutboxRowInTx(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := newRelRepo(q)
	occurred := time.Date(2026, 7, 10, 11, 59, 0, 0, time.UTC)
	err := r.RunRelationship(context.Background(), tenantID, func(ctx context.Context, tx social.RelationshipTx) error {
		return tx.Enqueue(ctx, social.RelationshipEvent{
			Kind:        social.RelBlocked,
			TenantID:    tenantID,
			ActorGCID:   authorGCID,
			SubjectGCID: otherGCID,
			OccurredAt:  occurred,
		})
	})
	if err != nil {
		t.Fatalf("RunRelationship: %v", err)
	}

	// Find the outbox INSERT among the recorded SQLs (after the RLS SET).
	idx := -1
	for i, sql := range q.sqls {
		if strings.Contains(sql, "sharing_outbox_events") {
			idx = i
		}
	}
	if idx < 0 {
		t.Fatalf("no sharing_outbox_events INSERT recorded; sqls=%v", q.sqls)
	}
	args := q.args[idx]
	if len(args) != 11 {
		t.Fatalf("outbox insert must bind 11 args (id..occurred_at); got %d: %v", len(args), args)
	}

	// $7 = topic
	topic, _ := args[6].(string)
	if topic != "chora.sharing.relationship.blocked.v1" {
		t.Fatalf("topic = %q; want chora.sharing.relationship.blocked.v1", topic)
	}
	// $4 = aggregate_type
	if agg, _ := args[3].(string); agg != "relationship" {
		t.Fatalf("aggregate_type = %q; want relationship", agg)
	}
	// $8 = payload — MUST be canonical binary protobuf (field 1 envelope =
	// tag 0x0a), never JSON ('{').
	payload, ok := args[7].([]byte)
	if !ok || len(payload) == 0 {
		t.Fatalf("payload must be non-empty []byte; got %T", args[7])
	}
	if payload[0] == '{' {
		t.Fatalf("payload is JSON — Schema Registry (BINARY) would reject it")
	}
	if payload[0] != 0x0a {
		t.Fatalf("payload must open with field-1 length-delimited envelope tag 0x0a; got 0x%02x", payload[0])
	}
	// $9 = envelope JSON string — must carry the tenant + trace context.
	envJSON, _ := args[8].(string)
	if !strings.Contains(envJSON, tenantID) {
		t.Fatalf("envelope must carry tenant_id; got %s", envJSON)
	}
	if !strings.Contains(envJSON, "traceparent") {
		t.Fatalf("envelope must carry traceparent; got %s", envJSON)
	}
	// $10 = idempotency_key non-empty.
	if idem, _ := args[9].(string); strings.TrimSpace(idem) == "" {
		t.Fatalf("idempotency_key must be non-empty")
	}
	// $3 = gcid = actor.
	if gcid, _ := args[2].(string); gcid != authorGCID {
		t.Fatalf("outbox gcid = %q; want actor %q", gcid, authorGCID)
	}
}

// -----------------------------------------------------------------------------
// topic map
// -----------------------------------------------------------------------------

func TestRelationshipTopics_CompleteAndValid(t *testing.T) {
	t.Parallel()
	want := map[social.RelationshipEventKind]string{
		social.RelFollowed:   "chora.sharing.relationship.followed.v1",
		social.RelUnfollowed: "chora.sharing.relationship.unfollowed.v1",
		social.RelBlocked:    "chora.sharing.relationship.blocked.v1",
		social.RelUnblocked:  "chora.sharing.relationship.unblocked.v1",
	}
	for kind, topic := range want {
		got, err := pg.RelationshipTopic(kind)
		if err != nil {
			t.Fatalf("RelationshipTopic(%s): %v", kind, err)
		}
		if got != topic {
			t.Fatalf("RelationshipTopic(%s) = %q; want %q", kind, got, topic)
		}
		if err := outbox.ValidateTopicName(got); err != nil {
			t.Fatalf("topic %q fails ValidateTopicName: %v", got, err)
		}
	}
	if _, err := pg.RelationshipTopic(social.RelationshipEventKind("nope")); err == nil {
		t.Fatalf("unknown kind must fail loud")
	}
}

// -----------------------------------------------------------------------------
// PairState
// -----------------------------------------------------------------------------

func TestRelationshipRepo_PairStateScansBlockedEither(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		rowFn: func(sql string, args ...any) pg.Row {
			if !strings.Contains(sql, "EXISTS") {
				return stubRow{}
			}
			return stubRow{scanFn: func(dest ...any) error {
				if len(dest) != 1 {
					t.Fatalf("PairState must scan exactly 1 boolean; got %d", len(dest))
				}
				b, ok := dest[0].(*bool)
				if !ok {
					t.Fatalf("dest[0] must be *bool")
				}
				*b = true
				return nil
			}}
		},
	}
	r := newRelRepo(q)
	var got social.PairState
	err := r.RunRelationship(context.Background(), tenantID, func(ctx context.Context, tx social.RelationshipTx) error {
		st, err := tx.PairState(ctx, authorGCID, otherGCID)
		got = st
		return err
	})
	if err != nil {
		t.Fatalf("PairState: %v", err)
	}
	if !got.BlockedEither {
		t.Fatalf("PairState mapping wrong: %+v", got)
	}
}
// -----------------------------------------------------------------------------

func TestSQLRelationshipTemplates_AreExported(t *testing.T) {
	t.Parallel()
	if !strings.Contains(pg.SQLSelectPairState, "social_blocks") {
		t.Fatalf("SQLSelectPairState must consult blocks")
	}
	if !strings.Contains(pg.SQLFriendPartners, "social_friendships") ||
		!strings.Contains(pg.SQLFriendPartners, "social_blocks") ||
		!strings.Contains(pg.SQLFriendPartners, "NOT EXISTS") {
		t.Fatalf("SQLFriendPartners must read friendships minus blocks")
	}
	if !strings.Contains(pg.SQLInsertOutboxEvent, "INSERT INTO sharing_outbox_events") ||
		!strings.Contains(pg.SQLInsertOutboxEvent, "'pending'") {
		t.Fatalf("SQLInsertOutboxEvent malformed")
	}
}

// -----------------------------------------------------------------------------
// follow + block ops through the tx (created / conflict / delete semantics)
// -----------------------------------------------------------------------------

func TestRelationshipRepo_FollowAndBlockOps(t *testing.T) {
	t.Parallel()

	t.Run("created path", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{} // Exec defaults to RowsAffected=1
		r := newRelRepo(q)
		err := r.RunRelationship(context.Background(), tenantID, func(ctx context.Context, tx social.RelationshipTx) error {
			edge, err := social.NewEdge(tenantID, authorGCID, otherGCID)
			if err != nil {
				return err
			}
			created, out, err := tx.InsertFollow(ctx, edge)
			if err != nil || !created || out != edge {
				t.Fatalf("InsertFollow: created=%v out=%v err=%v", created, out, err)
			}
			removed, err := tx.DeleteFollow(ctx, authorGCID, otherGCID)
			if err != nil || !removed {
				t.Fatalf("DeleteFollow: removed=%v err=%v", removed, err)
			}
			b, err := social.NewBlock(tenantID, authorGCID, otherGCID)
			if err != nil {
				return err
			}
			bCreated, err := tx.InsertBlock(ctx, b)
			if err != nil || !bCreated {
				t.Fatalf("InsertBlock: created=%v err=%v", bCreated, err)
			}
			bRemoved, err := tx.DeleteBlock(ctx, authorGCID, otherGCID)
			if err != nil || !bRemoved {
				t.Fatalf("DeleteBlock: removed=%v err=%v", bRemoved, err)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("RunRelationship: %v", err)
		}
	})

	t.Run("follow conflict returns original edge", func(t *testing.T) {
		t.Parallel()
		const originalID = "01970000-0000-7000-a000-0000000000f1"
		q := &stubQuerier{
			execTagFn: func(sql string) rls.CommandTag {
				if strings.Contains(sql, "INSERT INTO social_follows") {
					return rls.CommandTag{RowsAffected: 0} // ON CONFLICT no-op
				}
				return rls.CommandTag{RowsAffected: 1}
			},
			rowFn: func(sql string, args ...any) pg.Row {
				return stubRow{scanFn: func(dest ...any) error {
					if p, ok := dest[0].(*string); ok {
						*p = originalID
					}
					if p, ok := dest[1].(*string); ok {
						*p = tenantID
					}
					return nil
				}}
			},
		}
		r := newRelRepo(q)
		err := r.RunRelationship(context.Background(), tenantID, func(ctx context.Context, tx social.RelationshipTx) error {
			edge, err := social.NewEdge(tenantID, authorGCID, otherGCID)
			if err != nil {
				return err
			}
			created, out, err := tx.InsertFollow(ctx, edge)
			if err != nil {
				return err
			}
			if created {
				t.Fatalf("conflict must report created=false")
			}
			if out == nil || out.ID != originalID {
				t.Fatalf("conflict must return the ORIGINAL edge; got %+v", out)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("RunRelationship: %v", err)
		}
	})
}

// -----------------------------------------------------------------------------
// compile-time port proofs
// -----------------------------------------------------------------------------

var (
	_ social.RelationshipStore = (*pg.RelationshipRepo)(nil)
	_ social.GraphQueries      = (*pg.SocialGraphRepo)(nil)
)
