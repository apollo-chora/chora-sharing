// reaction_repo.go — Postgres adapter for the Reaction aggregate (CHO-2193/W0-F1).
//
// SCHEMA: `reactions` (reaction_id, tenant_id, gcid, post_id, reaction_type,
// created_at) with UNIQUE (gcid, post_id, reaction_type) — the idempotency
// arbiter that reaction.Registry's own comment promised would replace its
// in-memory (gcid|post|type) key. The table has been waiting for this adapter.
//
// Hard-delete on Unreact is DELIBERATE, not a soft-delete oversight. It matches
// the house convention for toggles — reactions / atom_bookmarks / atom_votes /
// social_follows carry no deleted_at, while content aggregates (posts) do — and
// mirrors bookmark_repo.go, the closest analogue, which issues a real DELETE.
//
// Resilience:
//   - Idempotency: INSERT ... ON CONFLICT (gcid, post_id, reaction_type) DO
//     NOTHING, then read back. A replay returns created=false, never a 23505.
//   - RLS: every statement calls rls.ApplySession first, inside the tx, so the
//     tenant policy on `reactions` has a tenant to act on. The tenant comes from
//     the VALIDATED ctx (requireIdentity / identityInterceptor), never a param.
//   - Errors are returned, never swallowed. ok=false means ABSENT; err != nil
//     means the operation FAILED and the caller must not report it as absent.
package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-sharing/internal/domain/reaction"
)

// ErrInvalidReaction is the sentinel for a malformed write.
var ErrInvalidReaction = errors.New("pg: reaction is invalid")

// SQLInsertReaction — idempotent on the live UNIQUE (gcid, post_id, reaction_type).
const SQLInsertReaction = `
INSERT INTO reactions (reaction_id, tenant_id, gcid, post_id, reaction_type, created_at)
VALUES ($1, $2, $3, $4, $5, now())
ON CONFLICT (gcid, post_id, reaction_type) DO NOTHING`

// SQLSelectReactionByKey reads back the canonical row for (gcid, post, type).
const SQLSelectReactionByKey = `
SELECT reaction_id, tenant_id, gcid, post_id, reaction_type, created_at
  FROM reactions
 WHERE gcid = $1 AND post_id = $2 AND reaction_type = $3`

// SQLDeleteReaction removes the (gcid, post, type) toggle.
const SQLDeleteReaction = `
DELETE FROM reactions WHERE gcid = $1 AND post_id = $2 AND reaction_type = $3`

// SQLDeleteReactionByID is the ownership-checked removal — gcid is in the
// predicate so a caller can never delete someone else's reaction.
const SQLDeleteReactionByID = `
DELETE FROM reactions WHERE reaction_id = $1 AND gcid = $2`

// SQLListReactionsByPost lists a post's reactions, oldest first.
const SQLListReactionsByPost = `
SELECT reaction_id, tenant_id, gcid, post_id, reaction_type, created_at
  FROM reactions
 WHERE post_id = $1
 ORDER BY created_at ASC`

// ReactionRepo is the pg-backed reaction.ReactionRepo.
type ReactionRepo struct {
	tx TxRunner
}

// NewReactionRepo constructs a pg ReactionRepo around a TxRunner.
func NewReactionRepo(tx TxRunner) *ReactionRepo { return &ReactionRepo{tx: tx} }

// React inserts the reaction, or returns the existing one (idempotent).
func (r *ReactionRepo) React(ctx context.Context, tenantID, gcid, postID string, t reaction.Type) (*reaction.Reaction, bool, error) {
	if r == nil || r.tx == nil {
		return nil, false, ErrNotImplemented
	}
	rx, err := reaction.NewReaction(tenantID, gcid, postID, t)
	if err != nil {
		return nil, false, fmt.Errorf("%w: %v", ErrInvalidReaction, err)
	}

	var (
		out     *reaction.Reaction
		created bool
	)
	err = r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		tag, err := q.Exec(ctx, SQLInsertReaction,
			rx.ID, rx.TenantID, rx.GCID, rx.PostID, string(rx.Type))
		if err != nil {
			return err
		}
		// RowsAffected==0 ⇒ the ON CONFLICT fired ⇒ this is an idempotent repeat.
		created = tag.RowsAffected > 0

		// Always read back the canonical row: on a repeat, the caller must get
		// the ORIGINAL reaction_id/created_at, not the one we just minted and
		// threw away.
		row := q.QueryRow(ctx, SQLSelectReactionByKey, gcid, postID, string(t))
		got, err := scanReaction(row.Scan)
		if err != nil {
			return err
		}
		out = got
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return out, created, nil
}

// Unreact removes the (gcid, post, type) reaction. ok=false ⇒ already absent.
func (r *ReactionRepo) Unreact(ctx context.Context, gcid, postID string, t reaction.Type) (bool, error) {
	if r == nil || r.tx == nil {
		return false, ErrNotImplemented
	}
	var removed bool
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		tag, err := q.Exec(ctx, SQLDeleteReaction, gcid, postID, string(t))
		if err != nil {
			return err
		}
		removed = tag.RowsAffected > 0
		return nil
	})
	if err != nil {
		return false, err
	}
	return removed, nil
}

// UnreactByID removes a reaction by id, ownership-checked on gcid.
func (r *ReactionRepo) UnreactByID(ctx context.Context, reactionID, gcid string) (bool, error) {
	if r == nil || r.tx == nil {
		return false, ErrNotImplemented
	}
	var removed bool
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		tag, err := q.Exec(ctx, SQLDeleteReactionByID, reactionID, gcid)
		if err != nil {
			return err
		}
		removed = tag.RowsAffected > 0
		return nil
	})
	if err != nil {
		return false, err
	}
	return removed, nil
}

// ListByPost returns the post's reactions.
func (r *ReactionRepo) ListByPost(ctx context.Context, postID string) ([]*reaction.Reaction, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	var out []*reaction.Reaction
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		rows, err := q.Query(ctx, SQLListReactionsByPost, postID)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		out = out[:0]
		for rows.Next() {
			rx, err := scanReaction(rows.Scan)
			if err != nil {
				return err
			}
			out = append(out, rx)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// scanReaction maps a row onto the aggregate. Column order matches the SELECT
// lists above.
func scanReaction(scan func(...any) error) (*reaction.Reaction, error) {
	var (
		rx    reaction.Reaction
		rtype string
	)
	if err := scan(&rx.ID, &rx.TenantID, &rx.GCID, &rx.PostID, &rtype, &rx.CreatedAt); err != nil {
		return nil, err
	}
	rx.Type = reaction.Type(rtype)
	return &rx, nil
}
