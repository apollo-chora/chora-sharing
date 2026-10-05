package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
	"github.com/jackc/pgx/v5"
)

const (
	sqlDuelUpsert = `
INSERT INTO duel_sessions
    (id, tenant_id, challenger_gcid, opponent_gcid, status, scope, winner_gcid,
     round_count, score_challenger, score_opponent, combo_challenger, combo_opponent,
     interest_tags, created_at, updated_at, completed_at, expires_at, category,
     mode, blitz_variant, blitz_time_limit_sec, blitz_race_target, blitz_started_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
        $19, $20, $21, $22, $23)
ON CONFLICT (id) DO UPDATE SET
    challenger_gcid     = EXCLUDED.challenger_gcid,
    opponent_gcid       = EXCLUDED.opponent_gcid,
    status              = EXCLUDED.status,
    scope               = EXCLUDED.scope,
    winner_gcid         = EXCLUDED.winner_gcid,
    round_count         = EXCLUDED.round_count,
    score_challenger    = EXCLUDED.score_challenger,
    score_opponent      = EXCLUDED.score_opponent,
    combo_challenger    = EXCLUDED.combo_challenger,
    combo_opponent      = EXCLUDED.combo_opponent,
    interest_tags       = EXCLUDED.interest_tags,
    updated_at          = EXCLUDED.updated_at,
    completed_at        = EXCLUDED.completed_at,
    category            = EXCLUDED.category,
    mode                = EXCLUDED.mode,
    blitz_variant       = EXCLUDED.blitz_variant,
    blitz_time_limit_sec = EXCLUDED.blitz_time_limit_sec,
    blitz_race_target   = EXCLUDED.blitz_race_target,
    blitz_started_at    = EXCLUDED.blitz_started_at`

	sqlDuelRoundUpsert = `
INSERT INTO duel_rounds
    (tenant_id, duel_session_id, round_number, atom_id, atom_revision_id,
     question, options, correct_answer, challenger_answered, opponent_answered, deadline_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, false, false, $9)
ON CONFLICT (duel_session_id, round_number) DO NOTHING`

	sqlDuelSelectByID = `
SELECT id, tenant_id, challenger_gcid, opponent_gcid, status, scope, winner_gcid,
       round_count, score_challenger, score_opponent, combo_challenger, combo_opponent,
       interest_tags, created_at, completed_at, expires_at, category,
       mode, blitz_variant, blitz_time_limit_sec, blitz_race_target, blitz_started_at
FROM duel_sessions
WHERE id = $1`

	sqlDuelSelectByIDForUpdate = `
SELECT id, tenant_id, challenger_gcid, opponent_gcid, status, scope, winner_gcid,
       round_count, score_challenger, score_opponent, combo_challenger, combo_opponent,
       interest_tags, created_at, completed_at, expires_at, category,
       mode, blitz_variant, blitz_time_limit_sec, blitz_race_target, blitz_started_at
FROM duel_sessions
WHERE id = $1
FOR UPDATE`

	sqlDuelRoundsBySession = `
SELECT round_number, atom_id, atom_revision_id, question, options, correct_answer,
       challenger_answer, opponent_answer, challenger_time_ms, opponent_time_ms,
       challenger_correct, opponent_correct, combo_multiplier_challenger,
       combo_multiplier_opponent, points_challenger, points_opponent,
       challenger_answered, opponent_answered, winner_gcid, resolved_at, deadline_at
FROM duel_rounds
WHERE duel_session_id = $1
ORDER BY round_number`

	// FCFS: parameterized resolved_at + winner from domain round.
	// NULL when round stays open (wrong answer) so COALESCE keeps existing
	// DB NULL and opponent can still steal.
	sqlDuelRoundResolve = `
UPDATE duel_rounds
SET challenger_answer          = COALESCE($3, challenger_answer),
    opponent_answer            = COALESCE($4, opponent_answer),
    challenger_time_ms         = COALESCE($5, challenger_time_ms),
    opponent_time_ms           = COALESCE($6, opponent_time_ms),
    challenger_correct         = COALESCE($7, challenger_correct),
    opponent_correct           = COALESCE($8, opponent_correct),
    combo_multiplier_challenger = COALESCE($9, combo_multiplier_challenger),
    combo_multiplier_opponent  = COALESCE($10, combo_multiplier_opponent),
    points_challenger          = COALESCE($11, points_challenger),
    points_opponent            = COALESCE($12, points_opponent),
    challenger_answered        = CASE WHEN $3 IS NOT NULL THEN true ELSE challenger_answered END,
    opponent_answered          = CASE WHEN $4 IS NOT NULL THEN true ELSE opponent_answered END,
    winner_gcid                = COALESCE($13, winner_gcid),
    resolved_at               = COALESCE($14, resolved_at)
WHERE duel_session_id = $1 AND round_number = $2
  AND resolved_at IS NULL`

	sqlDuelRoundMarkOpponentAnswered = `
UPDATE duel_rounds
SET challenger_answered = true,
    opponent_answered   = true
WHERE duel_session_id = $1 AND round_number = $2 AND resolved_at IS NOT NULL`

	// sqlDuelRoundStampDeadline stamps the deadline on the first unresolved
	// round (the new current round after the previous one resolved). WS3:
	// deadlines are stamped per-round when a round becomes current, not all
	// at once at StartBattle. Only updates if the deadline is not already set
	// (idempotent — re-stamping an already-stamped round is a no-op).
	sqlDuelRoundStampNextDeadline = `
UPDATE duel_rounds
SET deadline_at = $3
WHERE duel_session_id = $1
  AND round_number = $2
  AND resolved_at IS NULL
  AND deadline_at IS NULL`

	sqlDuelRatingSelect = `
SELECT rating, wins, losses, draws, peak_elo FROM duel_ratings
WHERE tenant_id = $1 AND gcid = $2 AND category = $3 AND deleted_at IS NULL`

	sqlDuelRatingUpsert = `
INSERT INTO duel_ratings (tenant_id, gcid, category, rating, wins, losses, draws, last_duel_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, now())
ON CONFLICT (tenant_id, gcid, category) WHERE deleted_at IS NULL
DO UPDATE SET
    rating       = EXCLUDED.rating,
    wins         = duel_ratings.wins + EXCLUDED.wins,
    losses       = duel_ratings.losses + EXCLUDED.losses,
    draws        = duel_ratings.draws + EXCLUDED.draws,
    peak_elo     = GREATEST(duel_ratings.peak_elo, EXCLUDED.rating),
    last_duel_at = now()`

	sqlDuelRatingTopN = `
SELECT dr.gcid,
       COALESCE(NULLIF(pp.display_name, ''), sfe.display_name, '') AS display_name,
       dr.rating, dr.wins, dr.losses, dr.draws, dr.peak_elo
FROM duel_ratings dr
LEFT JOIN profiler_profiles pp
  ON pp.gcid = dr.gcid AND pp.tenant_id = dr.tenant_id
LEFT JOIN LATERAL (
    SELECT e.content->>'author_display_name' AS display_name
    FROM social_feed_entries e
    WHERE e.entry_type = 'share'
      AND e.actor_gcid = dr.gcid
      AND e.tenant_id = dr.tenant_id
      AND e.content->>'author_display_name' != ''
    ORDER BY e.created_at DESC
    LIMIT 1
) sfe ON true
WHERE dr.tenant_id = $1 AND dr.category = $2 AND dr.deleted_at IS NULL
ORDER BY dr.rating DESC, dr.gcid ASC
LIMIT $3`
)

type DuelRepo struct {
	tx TxRunner
}

func NewDuelRepo(tx TxRunner) *DuelRepo { return &DuelRepo{tx: tx} }

var _ duel.DuelRepo = (*DuelRepo)(nil)

func (r *DuelRepo) SaveDuel(ctx context.Context, d *duel.Duel) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if d == nil {
		return duel.ErrInvalidArgument
	}
	created := d.CreatedAt
	if created.IsZero() {
		created = time.Now().UTC()
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		if _, err := q.Exec(ctx, sqlDuelUpsert,
			d.ID, d.TenantID, d.ChallengerGCID, d.OpponentGCID,
			string(d.Status), string(d.Scope), nullStr(d.WinnerGCID),
			d.RoundCount, d.ScoreChallenger, d.ScoreOpponent,
			d.ComboChallenger, d.ComboOpponent,
			nonNilTags(d.InterestTags), created, d.UpdatedAt, nullTime(d.CompletedAt), nullTime(d.ExpiresAt),
			d.Category,
			string(d.Mode), nullStr(string(d.BlitzConfig.Variant)),
			d.BlitzConfig.TimeLimitSec, d.BlitzConfig.RaceTarget, nullTime(d.BlitzStartedAt),
		); err != nil {
			return fmt.Errorf("pg: upsert duel session: %w", err)
		}
	for _, rd := range d.Rounds {
		if _, err := q.Exec(ctx, sqlDuelRoundUpsert,
			d.TenantID, d.ID, rd.RoundNumber, rd.AtomID, nullStr(rd.AtomRevisionID),
			rd.Question, nonNilTags(rd.Options), rd.CorrectAnswer, nullTime(rd.DeadlineAt),
		); err != nil {
			return fmt.Errorf("pg: upsert duel round %d: %w", rd.RoundNumber, err)
		}
	}
		return nil
	})
}

func (r *DuelRepo) GetDuel(ctx context.Context, duelID string) (*duel.Duel, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	return r.getDuel(ctx, duelID, sqlDuelSelectByID)
}

func (r *DuelRepo) GetDuelForUpdate(ctx context.Context, duelID string) (*duel.Duel, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	return r.getDuel(ctx, duelID, sqlDuelSelectByIDForUpdate)
}

func (r *DuelRepo) getDuel(ctx context.Context, duelID, selectSQL string) (*duel.Duel, error) {
	var d *duel.Duel
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		gg, err := scanDuel(q.QueryRow(ctx, selectSQL, duelID).Scan)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrDuelNotFound
			}
			return fmt.Errorf("pg: scan duel %s: %w", duelID, err)
		}
		rounds, rErr := loadDuelRounds(ctx, q, duelID)
		if rErr != nil {
			return rErr
		}
		gg.Rounds = rounds
		d = gg
		return nil
	})
	if err != nil {
		return nil, err
	}
	return d, nil
}

func (r *DuelRepo) ListDuels(ctx context.Context, tenantID, gcid, status string, limit int, cursor string) ([]*duel.Duel, string, error) {
	if r == nil || r.tx == nil {
		return nil, "", ErrNotImplemented
	}
	if limit <= 0 {
		limit = 20
	}
	args := []any{tenantID, gcid, gcid, limit}
	cursorCond := ""
	if cursor != "" {
		cursorCond = " AND created_at < (SELECT created_at FROM duel_sessions WHERE id = $5)"
		args = append(args, cursor)
	}
	statusCond := ""
	if status != "" {
		idx := len(args) + 1
		statusCond = fmt.Sprintf(" AND status = $%d", idx)
		args = append(args, status)
	}
	query := fmt.Sprintf(`
SELECT id, tenant_id, challenger_gcid, opponent_gcid, status, scope, winner_gcid,
       round_count, score_challenger, score_opponent, combo_challenger, combo_opponent,
       interest_tags, created_at, completed_at, expires_at, category,
       mode, blitz_variant, blitz_time_limit_sec, blitz_race_target, blitz_started_at
FROM duel_sessions
WHERE tenant_id = $1
  AND (challenger_gcid = $2 OR opponent_gcid = $3)%s%s
ORDER BY created_at DESC
LIMIT $4`, statusCond, cursorCond)

	var duels []*duel.Duel
	var nextCursor string
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		rows, err := q.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("pg: list duels: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			d, err := scanDuel(rows.Scan)
			if err != nil {
				return err
			}
			duels = append(duels, d)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(duels) == limit {
			nextCursor = duels[limit-1].ID
			duels = duels[:limit-1]
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	return duels, nextCursor, nil
}

// ListDuelsWithExpiredRounds returns in-progress duels for the given tenant
// that have at least one unresolved round whose deadline_at < now. WS3
// round-timer sweep. Per-tenant (RLS) — the caller must inject the tenant
// via tracing.WithTenantID before calling; rls.ApplySession is applied
// here so the FORCE RLS policy (tenant_id = current_setting(...)) matches.
func (r *DuelRepo) ListDuelsWithExpiredRounds(ctx context.Context, tenantID string, now time.Time) ([]*duel.Duel, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	// DISTINCT because a duel with multiple expired rounds would otherwise
	// appear once per expired round; the sweeper resolves all expired
	// rounds in each duel it picks up.
	const q = `
SELECT DISTINCT ds.id, ds.tenant_id, ds.challenger_gcid, ds.opponent_gcid,
       ds.status, ds.scope, ds.winner_gcid, ds.round_count,
       ds.score_challenger, ds.score_opponent, ds.combo_challenger, ds.combo_opponent,
       ds.interest_tags, ds.created_at, ds.completed_at, ds.expires_at, ds.category,
       ds.mode, ds.blitz_variant, ds.blitz_time_limit_sec, ds.blitz_race_target, ds.blitz_started_at
FROM duel_sessions ds
JOIN duel_rounds dr ON dr.duel_session_id = ds.id
WHERE ds.tenant_id = $1
  AND ds.status = 'in_progress'
  AND dr.resolved_at IS NULL
  AND dr.deadline_at IS NOT NULL
  AND dr.deadline_at < $2`
	var duels []*duel.Duel
	err := r.tx.RunInTx(ctx, func(ctx context.Context, txn Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(txn)); err != nil {
			return err
		}
		rows, err := txn.Query(ctx, q, tenantID, now)
		if err != nil {
			return fmt.Errorf("pg: list duels with expired rounds: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			d, err := scanDuel(rows.Scan)
			if err != nil {
				return err
			}
			duels = append(duels, d)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		// Load rounds for each duel so the sweeper can find the expired
		// round number. loadDuelRounds runs in the same tx via the txn
		// Querier — no nested transaction.
		for _, d := range duels {
			rounds, err := loadDuelRoundsWithQuerier(ctx, txn, d.ID)
			if err != nil {
				return fmt.Errorf("pg: load rounds for sweeper duel %s: %w", d.ID, err)
			}
			d.Rounds = rounds
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return duels, nil
}

// loadDuelRoundsWithQuerier is loadDuelRounds but accepts an explicit
// Querier so callers already inside a RunInTx can reuse their transaction.
func loadDuelRoundsWithQuerier(ctx context.Context, q Querier, duelID string) ([]duel.RoundSnapshot, error) {
	rows, err := q.Query(ctx, sqlDuelRoundsBySession, duelID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDuelRounds(rows)
}

// scanDuelRounds extracts the row-scanning loop from loadDuelRounds so it
// can be reused by loadDuelRoundsWithQuerier.
func scanDuelRounds(rows Rows) ([]duel.RoundSnapshot, error) {
	var out []duel.RoundSnapshot
	for rows.Next() {
		var (
			rd                      duel.RoundSnapshot
			chalAns, oppAns         *string
			chalTime, oppTime       *int64
			chalCorrect, oppCorrect *bool
			chalCombo, oppCombo     *int
			chalPts, oppPts         *int
			atomRev                 *string
			winner                  *string
			resolvedAt              *time.Time
			deadlineAt              *time.Time
		)
		var question string
		var options []string
		var correctAnswer string
		if err := rows.Scan(
			&rd.RoundNumber, &rd.AtomID, &atomRev,
			&question, &options, &correctAnswer,
			&chalAns, &oppAns, &chalTime, &oppTime,
			&chalCorrect, &oppCorrect, &chalCombo, &oppCombo,
			&chalPts, &oppPts,
			&rd.ChallengerAnswered, &rd.OpponentAnswered, &winner, &resolvedAt, &deadlineAt,
		); err != nil {
			return nil, err
		}
		rd.Question = question
		rd.Options = options
		rd.CorrectAnswer = correctAnswer
		if atomRev != nil {
			rd.AtomRevisionID = *atomRev
		}
		if chalAns != nil {
			rd.ChallengerAnswer = *chalAns
		}
		if oppAns != nil {
			rd.OpponentAnswer = *oppAns
		}
		if chalTime != nil {
			rd.ChallengerTimeMs = *chalTime
		}
		if oppTime != nil {
			rd.OpponentTimeMs = *oppTime
		}
		if chalCorrect != nil {
			rd.ChallengerCorrect = *chalCorrect
		}
		if oppCorrect != nil {
			rd.OpponentCorrect = *oppCorrect
		}
		if chalCombo != nil {
			rd.ComboMultiplierChallenger = *chalCombo
		}
		if oppCombo != nil {
			rd.ComboMultiplierOpponent = *oppCombo
		}
		if chalPts != nil {
			rd.PointsChallenger = *chalPts
		}
		if oppPts != nil {
			rd.PointsOpponent = *oppPts
		}
		if winner != nil {
			rd.WinnerGCID = *winner
		}
		if resolvedAt != nil {
			rd.ResolvedAt = resolvedAt
		}
		if deadlineAt != nil {
			rd.DeadlineAt = deadlineAt
		}
		out = append(out, rd)
	}
	return out, rows.Err()
}

func (r *DuelRepo) ResolveRound(ctx context.Context, d *duel.Duel, roundNo int, gcid string, res duel.RoundResolution) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if d == nil {
		return duel.ErrInvalidArgument
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		var (
			chalAns, oppAns         *string
			chalTime, oppTime       *int64
			chalCorrect, oppCorrect *bool
			chalCombo, oppCombo     *int
			chalPts, oppPts         *int
			roundWinner             *string
			roundResolvedAt         *time.Time
		)
		ansPlaceholder := ""
		switch {
		case gcid == "" && res.RoundTimeout:
			// Timeout path: no player answered. Skip answer/correct/combo/
			// points fields — the COALESCE in sqlDuelRoundResolve keeps
			// existing values (all NULL for a fresh round) and only
			// resolved_at + winner are set (both NULL for a timeout).
		case d.ChallengerGCID == gcid:
			chalAns = &ansPlaceholder
			chalCorrect = &res.Correct
			chalCombo = &res.ComboMultiplier
			chalPts = &res.PointsAwarded
		case d.OpponentGCID == gcid:
			oppAns = &ansPlaceholder
			oppCorrect = &res.Correct
			oppCombo = &res.ComboMultiplier
			oppPts = &res.PointsAwarded
		default:
			return duel.ErrNotParticipant
		}
		if roundNo >= 1 && roundNo <= len(d.Rounds) {
			rd := d.Rounds[roundNo-1]
			if rd.ResolvedAt != nil {
				ra := *rd.ResolvedAt
				roundResolvedAt = &ra
				if rd.WinnerGCID != "" {
					w := rd.WinnerGCID
					roundWinner = &w
				}
			}
		}
		if _, err := q.Exec(ctx, sqlDuelRoundResolve,
			d.ID, roundNo,
			chalAns, oppAns,
			chalTime, oppTime,
			chalCorrect, oppCorrect,
			chalCombo, oppCombo,
			chalPts, oppPts,
			roundWinner,
			roundResolvedAt,
		); err != nil {
			return fmt.Errorf("pg: resolve duel round %d: %w", roundNo, err)
		}
		if roundNo >= 1 && roundNo <= len(d.Rounds) && d.Rounds[roundNo-1].ResolvedAt != nil {
			if _, err := q.Exec(ctx, sqlDuelRoundMarkOpponentAnswered, d.ID, roundNo); err != nil {
				return fmt.Errorf("pg: mark opponent answered round %d: %w", roundNo, err)
			}
		}
		// WS3: persist the next round's deadline if the domain stamped it.
		// The domain's ResolveRound / ResolveRoundTimeout calls
		// stampCurrentRoundDeadline on the next unresolved round. The
		// in-memory Duel has the new deadline_at; this UPDATE persists it
		// so the sweeper can find it on the next tick.
		if roundNo < len(d.Rounds) {
			nextRound := d.Rounds[roundNo]
			if nextRound.ResolvedAt == nil && nextRound.DeadlineAt != nil {
				if _, err := q.Exec(ctx, sqlDuelRoundStampNextDeadline, d.ID, nextRound.RoundNumber, nextRound.DeadlineAt); err != nil {
					return fmt.Errorf("pg: stamp next round %d deadline: %w", nextRound.RoundNumber, err)
				}
			}
		}
		if _, err := q.Exec(ctx, sqlDuelUpsert,
			d.ID, d.TenantID, d.ChallengerGCID, d.OpponentGCID,
			string(d.Status), string(d.Scope), nullStr(d.WinnerGCID),
			d.RoundCount, d.ScoreChallenger, d.ScoreOpponent,
			d.ComboChallenger, d.ComboOpponent,
			nonNilTags(d.InterestTags), d.CreatedAt, d.UpdatedAt, nullTime(d.CompletedAt), nullTime(d.ExpiresAt),
			d.Category,
			string(d.Mode), nullStr(string(d.BlitzConfig.Variant)),
			d.BlitzConfig.TimeLimitSec, d.BlitzConfig.RaceTarget, nullTime(d.BlitzStartedAt),
		); err != nil {
			return fmt.Errorf("pg: update duel session after resolve: %w", err)
		}
		return nil
	})
}

func (r *DuelRepo) StampRoundDeadline(ctx context.Context, duelID string, roundNo int, deadline time.Time) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		// Idempotent: sqlDuelRoundStampNextDeadline only updates when the
		// round is unresolved AND deadline_at IS NULL, so a concurrent
		// second serve is a no-op (never resets the timer).
		if _, err := q.Exec(ctx, sqlDuelRoundStampNextDeadline, duelID, roundNo, deadline); err != nil {
			return fmt.Errorf("pg: stamp round %d deadline: %w", roundNo, err)
		}
		return nil
	})
}

func (r *DuelRepo) ApplyELO(ctx context.Context, d *duel.Duel, kFactor int) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if d == nil {
		return duel.ErrInvalidArgument
	}
	if kFactor <= 0 {
		kFactor = 32
	}
	if d.Scope != duel.ScopeRanked {
		return nil
	}
	if d.Status != duel.StatusCompleted && d.Status != duel.StatusForfeited {
		return nil
	}
	// Draw: both players gain/lose a fraction of the K-factor toward the
	// midpoint. This is standard ELO draw handling.
	if d.WinnerGCID == "" {
		return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
			if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
				return err
			}
		rA, err := r.ratingFor(ctx, q, d.TenantID, d.ChallengerGCID, d.Category)
		if err != nil {
			return err
		}
		rB, err := r.ratingFor(ctx, q, d.TenantID, d.OpponentGCID, d.Category)
		if err != nil {
			return err
		}
			// Draw: each player moves half the expected delta toward the other.
			expA := 1.0 / (1.0 + duel.ExpApprox(float64(rB-rA)/400.0))
			drawDelta := float64(kFactor) * (0.5 - expA)
			rANew := rA + int(drawDelta+0.5)
			rBNew := rB - int(drawDelta+0.5)
			if err := r.upsertRating(ctx, q, d.TenantID, d.ChallengerGCID, d.Category, rANew, 0, 0, 1); err != nil {
				return err
			}
			if err := r.upsertRating(ctx, q, d.TenantID, d.OpponentGCID, d.Category, rBNew, 0, 0, 1); err != nil {
				return err
			}
			return nil
		})
	}
	loserGCID := d.OpponentGCID
	if d.WinnerGCID == d.OpponentGCID {
		loserGCID = d.ChallengerGCID
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		winnerRating, err := r.ratingFor(ctx, q, d.TenantID, d.WinnerGCID, d.Category)
		if err != nil {
			return err
		}
		loserRating, err := r.ratingFor(ctx, q, d.TenantID, loserGCID, d.Category)
		if err != nil {
			return err
		}
		winnerNew, loserNew := duel.ComputeELO(winnerRating, loserRating, kFactor)
		if err := r.upsertRating(ctx, q, d.TenantID, d.WinnerGCID, d.Category, winnerNew, 1, 0, 0); err != nil {
			return err
		}
		if err := r.upsertRating(ctx, q, d.TenantID, loserGCID, d.Category, loserNew, 0, 1, 0); err != nil {
			return err
		}
		return nil
	})
}

func (r *DuelRepo) GetRating(ctx context.Context, tenantID, gcid string) (int, error) {
	return r.GetRatingForCategory(ctx, tenantID, gcid, "overall")
}

func (r *DuelRepo) GetRatingStats(ctx context.Context, tenantID, gcid string) (duel.RatingStats, error) {
	return r.GetRatingStatsForCategory(ctx, tenantID, gcid, "overall")
}

// GetRatingForCategory returns the caller's ELO for a specific category (WS1).
func (r *DuelRepo) GetRatingForCategory(ctx context.Context, tenantID, gcid, category string) (int, error) {
	if category == "" {
		category = "overall"
	}
	if r == nil || r.tx == nil {
		return 1200, ErrNotImplemented
	}
	stats, err := r.GetRatingStatsForCategory(ctx, tenantID, gcid, category)
	if err != nil {
		return 1200, err
	}
	return stats.Rating, nil
}

// GetRatingStatsForCategory returns the caller's W/L/D record for a category (WS1).
func (r *DuelRepo) GetRatingStatsForCategory(ctx context.Context, tenantID, gcid, category string) (duel.RatingStats, error) {
	if category == "" {
		category = "overall"
	}
	if r == nil || r.tx == nil {
		return duel.RatingStats{Rating: 1200, PeakELO: 1200}, ErrNotImplemented
	}
	var rs duel.RatingStats
	rs.GCID = gcid
	rs.Rating = 1200
	rs.PeakELO = 1200
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		var rating, wins, losses, draws, peak int
		if err := q.QueryRow(ctx, sqlDuelRatingSelect, tenantID, gcid, category).Scan(
			&rating, &wins, &losses, &draws, &peak,
		); err != nil {
			return nil // no row → fresh player, defaults stay
		}
		rs.Rating = rating
		rs.Wins = wins
		rs.Losses = losses
		rs.Draws = draws
		rs.PeakELO = peak
		return nil
	})
	if err != nil {
		return duel.RatingStats{Rating: 1200, PeakELO: 1200}, err
	}
	if rs.Rating == 0 {
		rs.Rating = 1200
	}
	if rs.PeakELO == 0 {
		rs.PeakELO = 1200
	}
	return rs, nil
}

func (r *DuelRepo) TopRatings(ctx context.Context, tenantID string, limit int) ([]duel.RatingStats, error) {
	return r.TopRatingsForCategory(ctx, tenantID, "overall", limit)
}

// TopRatingsForCategory returns the top-N players for a specific category (WS1).
func (r *DuelRepo) TopRatingsForCategory(ctx context.Context, tenantID, category string, limit int) ([]duel.RatingStats, error) {
	if category == "" {
		category = "overall"
	}
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	var results []duel.RatingStats
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		rows, err := q.Query(ctx, sqlDuelRatingTopN, tenantID, category, limit)
		if err != nil {
			return fmt.Errorf("pg: top ratings: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var rs duel.RatingStats
			if err := rows.Scan(&rs.GCID, &rs.DisplayName, &rs.Rating, &rs.Wins, &rs.Losses, &rs.Draws, &rs.PeakELO); err != nil {
				return err
			}
			results = append(results, rs)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (r *DuelRepo) ratingFor(ctx context.Context, q Querier, tenantID, gcid, category string) (int, error) {
	if category == "" {
		category = "overall"
	}
	var rating int
	err := q.QueryRow(ctx, sqlDuelRatingSelect, tenantID, gcid, category).Scan(
		&rating, new(int), new(int), new(int), new(int),
	)
	if err != nil {
		// No existing rating row → new player starts at 1200. Any other
		// error (RLS denial, connection loss) must propagate — swallowing
		// it silently masks ELO write failures (CHO-336 root cause).
		if errors.Is(err, pgx.ErrNoRows) {
			return 1200, nil
		}
		return 0, fmt.Errorf("pg: read duel rating: %w", err)
	}
	return rating, nil
}

func (r *DuelRepo) upsertRating(ctx context.Context, q Querier, tenantID, gcid, category string, rating, wins, losses, draws int) error {
	if category == "" {
		category = "overall"
	}
	if _, err := q.Exec(ctx, sqlDuelRatingUpsert, tenantID, gcid, category, rating, wins, losses, draws); err != nil {
		return fmt.Errorf("pg: upsert duel rating: %w", err)
	}
	return nil
}

func scanDuel(scan func(...any) error) (*duel.Duel, error) {
	var (
		d               duel.Duel
		winner          *string
		status          string
		scope           string
		completedAt     *time.Time
		expiresAt       *time.Time
		category        string
		mode            string
		blitzVariant    *string
		blitzTimeLimit  int
		blitzRaceTarget int
		blitzStartedAt  *time.Time
	)
	if err := scan(
		&d.ID, &d.TenantID, &d.ChallengerGCID, &d.OpponentGCID,
		&status, &scope, &winner,
		&d.RoundCount, &d.ScoreChallenger, &d.ScoreOpponent,
		&d.ComboChallenger, &d.ComboOpponent,
		&d.InterestTags, &d.CreatedAt, &completedAt, &expiresAt, &category,
		&mode, &blitzVariant, &blitzTimeLimit, &blitzRaceTarget, &blitzStartedAt,
	); err != nil {
		return nil, err
	}
	d.Status = duel.Status(status)
	d.Scope = duel.Scope(scope)
	if winner != nil {
		d.WinnerGCID = *winner
	}
	if completedAt != nil {
		d.CompletedAt = completedAt
	}
	if expiresAt != nil {
		d.ExpiresAt = expiresAt
	}
	if category != "" {
		d.Category = category
	} else {
		d.Category = "overall"
	}
	if mode != "" {
		d.Mode = duel.Mode(mode)
	} else {
		d.Mode = duel.ModeClassic
	}
	if blitzVariant != nil && *blitzVariant != "" {
		d.BlitzConfig = duel.BlitzConfig{
			Variant:      duel.BlitzVariant(*blitzVariant),
			TimeLimitSec: blitzTimeLimit,
			RaceTarget:   blitzRaceTarget,
		}
	}
	if blitzStartedAt != nil {
		d.BlitzStartedAt = blitzStartedAt
	}
	return &d, nil
}

func loadDuelRounds(ctx context.Context, q Querier, duelID string) ([]duel.RoundSnapshot, error) {
	rows, err := q.Query(ctx, sqlDuelRoundsBySession, duelID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDuelRounds(rows)
}

var sqlDuelRatingNearby = `
SELECT gcid, rating, wins, losses, draws, peak_elo
FROM duel_ratings
WHERE tenant_id = $1 AND gcid != $2 AND deleted_at IS NULL
ORDER BY ABS(rating - COALESCE(
    (SELECT rating FROM duel_ratings WHERE tenant_id = $1 AND gcid = $2 AND deleted_at IS NULL),
    1200
)) ASC, gcid ASC
LIMIT $3`

func (r *DuelRepo) FindNearbyRatings(ctx context.Context, tenantID, gcid string, limit int) ([]duel.RatingStats, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if limit <= 0 || limit > 50 {
		limit = 10
	}
	var results []duel.RatingStats
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		rows, err := q.Query(ctx, sqlDuelRatingNearby, tenantID, gcid, limit)
		if err != nil {
			return fmt.Errorf("pg: query nearby ratings: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var rs duel.RatingStats
			if err := rows.Scan(&rs.GCID, &rs.Rating, &rs.Wins, &rs.Losses, &rs.Draws, &rs.PeakELO); err != nil {
				return fmt.Errorf("pg: scan nearby rating: %w", err)
			}
			results = append(results, rs)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}
