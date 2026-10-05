package pg

import (
	"context"
	"encoding/json"
	"fmt"
)

// DefaultOutboxWriter is the production OutboxWriter that inserts outbox rows
// via the ShareRepo's transaction Querier. It uses the same SQL as
// sqlOutboxInsert but is callable from within the ShareRepo's RunInTx callback.
type DefaultOutboxWriter struct{}

// NewDefaultOutboxWriter constructs a DefaultOutboxWriter.
func NewDefaultOutboxWriter() *DefaultOutboxWriter { return &DefaultOutboxWriter{} }

// WriteOutboxRow inserts a pending outbox row via the supplied Querier, inside
// the caller's transaction. The row is idempotent on idempotency_key.
func (DefaultOutboxWriter) WriteOutboxRow(ctx context.Context, q Querier, row OutboxRow) error {
	envJSON, err := json.Marshal(row.Envelope)
	if err != nil {
		return fmt.Errorf("marshal outbox envelope: %w", err)
	}
	var gcid any
	if row.GCID != "" {
		gcid = row.GCID
	}
	_, err = q.Exec(ctx, sqlOutboxInsert,
		row.ID, row.TenantID, gcid, row.AggregateType, row.AggregateID,
		row.EventType, row.Topic, row.Payload, string(envJSON),
		row.IdempotencyKey, row.OccurredAt,
	)
	if err != nil {
		return fmt.Errorf("exec outbox insert: %w", err)
	}
	return nil
}
