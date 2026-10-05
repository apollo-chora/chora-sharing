// closure_repository.go — pgx-backed, durable implementation of the
// federated account-closure saga's ClosureRepository port (Tier 3 D11 /
// ADR-184 / ADR-186; W0-F1 durability + W0-F5 error-honesty, CHO-2198).
//
// Replaces events.NewInMemoryClosureRepo — the process-local map whose ack
// state (which (tenant, gcid) pairs this domain has already pseudonymised)
// was lost on every pod restart. That in-memory repo was wired UNGATED:
// gated on `pubsubClient != nil` only, never on pool health (see
// docs/references/w0-f1-inmemory-inventory.md §6 item 4).
//
// PRE-EXISTING SIBLING TABLE (report this loudly): migrations/0007_closure_
// idempotency_outbox.up.sql already defines a `closure_log` table
// (gcid-only PK, RLS intentionally DISABLED — "the closure subscriber runs
// with service-level credentials, not per-tenant RLS"). No Go code
// anywhere references `closure_log`; it is unwired dead schema, and per
// docs/TODO-DEVELOPMENT.md it is one of four repo-defined tables ABSENT
// from prod chora_sharing (never actually applied). This adapter does NOT
// reuse or touch closure_log — it adds the standardised
// closure_pseudonymisation_state shape used by the chora-notifications
// reference (tenant-scoped UNIQUE(tenant_id, gcid), RLS ENABLED), matching
// the mission's explicit migration contract. Reconciling/retiring
// closure_log is a separate decision this fix does not make unilaterally.
//
// DEVIATION FROM THE chora-notifications REFERENCE (report this loudly):
// this package deliberately never imports pgx (see runtime.go) — Querier/
// Row/Rows/TxRunner are pgx-free seams; the concrete pgx bridge lives only
// in cmd/server/pgx_txrunner.go. That bridge's pgxRow.Scan passes pgx's
// raw, UNWRAPPED error straight through (no local ErrNoRows sentinel
// translation), and no existing repo in this package added one (post.go's
// Get treats ANY Scan error as "not found" — the swallowed-error shape
// this whole CHO-2198 fix exists to avoid). Rather than either propagating
// that swallowing habit into a NEW adapter or reaching into the shared
// cmd/server bridge (blast radius beyond this file), Pseudonymise and
// IsPseudonymised both use q.Query(...) (not QueryRow) and the
// Rows.Next()/Rows.Err() idiom already established in
// chora-tenancy/internal/adapter/pg/membership_write_repository.go's
// existence-guard queries: Next()==false + Err()==nil is a CLEAN miss
// (ON CONFLICT fired / no row); Next()==false + Err()!=nil is a GENUINE
// backing-store error that must propagate. This sidesteps the need for an
// ErrNoRows sentinel entirely while preserving the exact W0-F5 fail-loud
// contract the reference established.
//
// SQL contract:
//
//   - Pseudonymise: INSERT ... ON CONFLICT (tenant_id, gcid) DO NOTHING
//     RETURNING id. A returned row means THIS call performed the durable
//     ack (fresh); no row + no error means the conflict fired — another
//     call already acked this (tenant, gcid) pair, so this call is an
//     idempotent no-op (mirrors the in-memory repo's
//     `if r.pseudonymed[key] { return 0, nil }` contract, but race-safe:
//     the uniqueness is enforced by Postgres, not a Go-side check-then-set
//     that two concurrent goroutines/pods could both pass).
//   - IsPseudonymised: SELECT 1 ... LIMIT 1. A real backing-store error is
//     returned as an error — NEVER coerced into `false`. This is the
//     entire point of widening the port's signature (see
//     closure_subscriber.go's ClosureRepository doc comment): the original
//     `IsPseudonymised(gcid string) bool` had no way to report a DB error,
//     so ANY pg-backed implementation of that old signature would have had
//     to swallow a connection error into "not yet pseudonymised" — a
//     guard read that fails OPEN.
//
// What this adapter does NOT do: it does not execute any per-table UPDATE
// against posts / reactions / comments / social_profiles / pvp_duels /
// leaderboard_entries / refer_a_friend_log / territory_conquest_log. Like
// the in-memory repo it replaces, `rows_touched` is a declared-intent
// count derived from the PII_Closure_Map.yaml spec (sum of columns), NOT
// an actual per-row UPDATE-affected count. Real per-table redaction is a
// separate, deeper gap — confirmed identical across all 9 closure-saga
// services (see CHO-2198 durability report "no-op Pseudonymise" finding),
// not something this migration/adapter implements.
//
// RLS: closure_pseudonymisation_state is tenant-scoped (migration 0038).
// Every query runs through r.run(ctx, tenantID, fn), which stamps the
// explicit tenantID onto ctx (tracing.WithTenantID) then calls
// rls.ApplySession BEFORE the user query — matching social_repo.go /
// post.go's established convention for this package (fail-loud on empty
// tenant via rls.ErrNoTenantContext, never a silent zero-row read).
//
// Cross-DB queries forbidden — this repo reads only chora_sharing.
package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/config"
)

// ErrClosureExecutorUnbuilt is returned by Pseudonymise until a real per-table
// executor exists.
//
// ⚠ WHY THIS FAILS CLOSED (disarm, 2026-08-14). Pseudonymise executes no
// per-table UPDATE at all: it writes one durable ack row and reports
// rows_touched as a DECLARED-INTENT count, the sum of columns listed in
// PII_Closure_Map.yaml. Measured across the platform: 9 services carry a closure
// repo, 0 of them contain any UPDATE, over 73 declared tables and 123 columns.
// Measured as never exercised: lifetime inserts on closure_pseudonymisation_state
// are 0.
//
// That is worse than dead code, because closure IS reachable from the admin
// account-lifecycle surface. On the first real closure the subscriber would
// publish status "ok" with a non-zero rows_touched under chora_imda_dimension
// "accountability", and the account would reach ACCOUNT_STATE_PSEUDONYMIZED,
// which the contract defines as "PII fields tokenized per PII_Closure_Map",
// having tokenised nothing. A false compliance attestation; absent code would at
// least have failed loudly.
//
// So the saga fails CLOSED rather than reaching a pseudonymised end state on the
// strength of work nobody did. Real per-table redaction is a separate, deeper
// gap (CHO-2198 "no-op Pseudonymise"); when it lands, this error goes with it.
var ErrClosureExecutorUnbuilt = errors.New(
	"closure: per-table PII_Closure_Map executor is not implemented; " +
		"refusing to report a pseudonymisation that did not happen")

// ClosureRepository is the pgx-backed, durable implementation of the
// events.ClosureRepository port.
type ClosureRepository struct {
	tx TxRunner
}

// NewClosureRepository constructs a pg ClosureRepository around a
// TxRunner. A nil runner (dev-fallback / misuse) makes every method
// return ErrNotImplemented, matching every other repo in this package.
func NewClosureRepository(tx TxRunner) *ClosureRepository {
	return &ClosureRepository{tx: tx}
}

// Pseudonymise durably acks that (tenantID, gcid) has been processed by
// this domain's closure subscriber. Idempotent: a replay (same tenant,
// gcid) is a no-op that returns (0, nil), never a duplicate row or an
// error.
//
// Column order matches
// migrations/0038_closure_pseudonymisation_state.up.sql: id, tenant_id,
// gcid, rows_touched.
func (r *ClosureRepository) Pseudonymise(ctx context.Context, tenantID, gcid string, spec []config.TableSpec) (int, error) {
	// Disarmed until a real executor exists: see ErrClosureExecutorUnbuilt.
	// Placed FIRST so no ack row is written and no success is published.
	return 0, ErrClosureExecutorUnbuilt

	if r == nil || r.tx == nil {
		return 0, ErrNotImplemented
	}
	tenantID = strings.TrimSpace(tenantID)
	gcid = strings.TrimSpace(gcid)
	if tenantID == "" {
		return 0, errors.New("pg.ClosureRepository.Pseudonymise: tenant_id required")
	}
	if gcid == "" {
		return 0, errors.New("pg.ClosureRepository.Pseudonymise: gcid required")
	}

	id, err := uuid.NewV7()
	if err != nil {
		return 0, fmt.Errorf("pg.ClosureRepository.Pseudonymise: uuidv7: %w", err)
	}

	// Declared-intent row count from the PII map — see package doc: this
	// adapter does not itself touch posts/reactions/comments/etc.
	rows := 0
	for _, t := range spec {
		rows += len(t.Columns)
	}

	const q = `
        INSERT INTO closure_pseudonymisation_state (id, tenant_id, gcid, rows_touched)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (tenant_id, gcid) DO NOTHING
        RETURNING id
    `
	conflictFired := false
	err = r.run(ctx, tenantID, func(ctx context.Context, q2 Querier) error {
		qrows, err := q2.Query(ctx, q, id.String(), tenantID, gcid, rows)
		if err != nil {
			return fmt.Errorf("pg: insert closure state: %w", err)
		}
		defer qrows.Close()
		if !qrows.Next() {
			// No RETURNING row: either the ON CONFLICT fired (clean,
			// idempotent no-op) or a genuine backing-store error. Err()
			// is the ONLY honest way to tell them apart — never assume
			// "no row" means "conflict".
			if err := qrows.Err(); err != nil {
				return fmt.Errorf("pg: insert closure state rows.Err: %w", err)
			}
			conflictFired = true
			return nil
		}
		return qrows.Err()
	})
	if err != nil {
		return 0, fmt.Errorf("pg.ClosureRepository.Pseudonymise: %w", err)
	}
	if conflictFired {
		return 0, nil
	}
	return rows, nil
}

// IsPseudonymised reports whether (tenantID, gcid) has already been
// durably acked. A real backing-store error is returned as a non-nil
// error and MUST NOT be treated as `false` by any caller — see the
// ClosureRepository port doc comment in closure_subscriber.go.
func (r *ClosureRepository) IsPseudonymised(ctx context.Context, tenantID, gcid string) (bool, error) {
	if r == nil || r.tx == nil {
		return false, ErrNotImplemented
	}
	tenantID = strings.TrimSpace(tenantID)
	gcid = strings.TrimSpace(gcid)
	if tenantID == "" {
		return false, errors.New("pg.ClosureRepository.IsPseudonymised: tenant_id required")
	}
	if gcid == "" {
		return false, errors.New("pg.ClosureRepository.IsPseudonymised: gcid required")
	}

	const q = `SELECT 1 FROM closure_pseudonymisation_state WHERE tenant_id = $1 AND gcid = $2 LIMIT 1`
	found := false
	err := r.run(ctx, tenantID, func(ctx context.Context, q2 Querier) error {
		qrows, err := q2.Query(ctx, q, tenantID, gcid)
		if err != nil {
			return fmt.Errorf("pg: select closure state: %w", err)
		}
		defer qrows.Close()
		if !qrows.Next() {
			return qrows.Err()
		}
		found = true
		return qrows.Err()
	})
	if err != nil {
		return false, fmt.Errorf("pg.ClosureRepository.IsPseudonymised: %w", err)
	}
	return found, nil
}

// run stamps the explicit tenant onto ctx then applies the RLS session
// before the user query (fail-loud on empty tenant —
// rls.ErrNoTenantContext). Mirrors SocialGraphRepo.run /
// PostRepository's rls.ApplySession convention (each repo in this package
// defines its own copy rather than sharing one across files).
func (r *ClosureRepository) run(ctx context.Context, tenantID string, fn func(ctx context.Context, q Querier) error) error {
	ctx = tracing.WithTenantID(ctx, strings.TrimSpace(tenantID))
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		return fn(ctx, q)
	})
}
