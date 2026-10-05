package pg

import (
	"context"
	"fmt"

	"github.com/apollo-chora/chora-common/rls"
)

type ConnectionStore struct {
	tx TxRunner
}

func NewConnectionStore(tx TxRunner) *ConnectionStore {
	return &ConnectionStore{tx: tx}
}

func (s *ConnectionStore) SaveFollow(ctx context.Context, tenantID, follower, followee string) error {
	return s.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		_, err := q.Exec(ctx, `
			INSERT INTO social_follows (tenant_id, follower_gcid, followee_gcid)
			VALUES ($1, $2, $3)
			ON CONFLICT (follower_gcid, followee_gcid) DO NOTHING
		`, tenantID, follower, followee)
		if err != nil {
			return fmt.Errorf("pg.ConnectionStore.SaveFollow: %w", err)
		}
		return nil
	})
}

func (s *ConnectionStore) DeleteFollow(ctx context.Context, follower, followee string) error {
	return s.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		_, err := q.Exec(ctx, `
			DELETE FROM social_follows WHERE follower_gcid = $1 AND followee_gcid = $2
		`, follower, followee)
		if err != nil {
			return fmt.Errorf("pg.ConnectionStore.DeleteFollow: %w", err)
		}
		return nil
	})
}

func (s *ConnectionStore) SaveBlock(ctx context.Context, tenantID, blocker, blocked string) error {
	return s.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		_, err := q.Exec(ctx, `
			INSERT INTO connections (tenant_id, requester_gcid, addressee_gcid, status)
			VALUES ($1, $2, $3, 'blocked')
			ON CONFLICT DO NOTHING
		`, tenantID, blocker, blocked)
		if err != nil {
			return fmt.Errorf("pg.ConnectionStore.SaveBlock: %w", err)
		}
		return nil
	})
}

func (s *ConnectionStore) DeleteBlock(ctx context.Context, blocker, blocked string) error {
	return s.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		_, err := q.Exec(ctx, `
			DELETE FROM connections
			WHERE requester_gcid = $1 AND addressee_gcid = $2 AND status = 'blocked'
		`, blocker, blocked)
		if err != nil {
			return fmt.Errorf("pg.ConnectionStore.DeleteBlock: %w", err)
		}
		return nil
	})
}
