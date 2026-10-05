// royalty_repo.go — pgx-backed grant.RoyaltyRepo.
//
// Records double-entry royalty settlements into royalty_settlements, idempotent
// on SourceEventID (UNIQUE index royalty_settlements_source_event_unique_idx).
// RLS applied on every method via qToExecer(q) before user queries.
//
// Schema: migrations/0004_atom_sharing.up.sql (royalty_settlements).
//
// Column mapping (per schema §4.2):
//   - tenant_id         = the DEBIT side (reuser tenant) ← RoyaltySettlement.GranteeTenantID
//   - grant_id          = RoyaltySettlement.GrantID
//   - owner_gcid        = the credit side (the AUTHOR) ← RoyaltySettlement.OwnerGCID
//   - grantee_tenant_id = the debit side ← RoyaltySettlement.GranteeTenantID
package pg

import (
	"context"
	"fmt"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-sharing/internal/domain/grant"
)

const sqlRoyaltyInsert = `
INSERT INTO royalty_settlements
    (tenant_id, grant_id, owner_gcid, grantee_tenant_id, atom_id,
     amount, currency, usage_context, source_event_id, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (source_event_id) DO NOTHING`

const sqlRoyaltyExists = `
SELECT 1 FROM royalty_settlements WHERE source_event_id = $1 LIMIT 1`

// RoyaltyRepo implements grant.RoyaltyRepo against Postgres.
type RoyaltyRepo struct {
	tx TxRunner
}

// NewRoyaltyRepo constructs a RoyaltyRepo bound to a TxRunner.
func NewRoyaltyRepo(tx TxRunner) *RoyaltyRepo { return &RoyaltyRepo{tx: tx} }

// Compile-time port assertion.
var _ grant.RoyaltyRepo = (*RoyaltyRepo)(nil)

// Record persists a royalty settlement. Idempotent on SourceEventID: a replay
// (ON CONFLICT DO NOTHING) returns ErrRoyaltyAlreadySettled — the caller
// treats this as success (idempotent), not an error to surface.
//
// The settlement's Amount is persisted as NUMERIC(18,6) per the schema; the
// currency + usage_context values mirror the royalty_currency /
// royalty_usage_context PG enum labels.
func (r *RoyaltyRepo) Record(ctx context.Context, s *grant.RoyaltySettlement) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if s == nil {
		return grant.ErrInvalidArgument
	}
	var inserted bool
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		tag, err := q.Exec(ctx, sqlRoyaltyInsert,
			s.GranteeTenantID, s.GrantID, s.OwnerGCID, s.GranteeTenantID,
			s.AtomID, s.Amount, s.Currency, s.UsageContext, s.SourceEventID, s.CreatedAt,
		)
		if err != nil {
			return fmt.Errorf("pg: insert royalty settlement: %w", err)
		}
		inserted = tag.RowsAffected > 0
		return nil
	})
	if err != nil {
		return err
	}
	if !inserted {
		return grant.ErrRoyaltyAlreadySettled
	}
	return nil
}

// Exists reports whether a settlement has been recorded for the SourceEventID.
func (r *RoyaltyRepo) Exists(ctx context.Context, sourceEventID string) (bool, error) {
	if r == nil || r.tx == nil {
		return false, ErrNotImplemented
	}
	var exists bool
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		var one int
		if err := q.QueryRow(ctx, sqlRoyaltyExists, sourceEventID).Scan(&one); err != nil {
			// No rows → not found. SELECT 1 returns 0 rows when absent.
			return nil
		}
		exists = true
		return nil
	})
	return exists, err
}
