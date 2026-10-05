// duel_repo_b_test.go — coverage tests for DuelRepo (second coverage agent):
// SaveDuel upsert+rounds, get/list scans + key branch mapping, ResolveRound
// participant/timeout paths, ApplyELO draw + ladder, ratings read/write.
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
)

const duelID = "01970000-0000-7000-a000-0000000000a1"

func testDuel() *duel.Duel {
	return &duel.Duel{
		ID:              duelID,
		TenantID:        tenantID,
		ChallengerGCID:  authorGCID,
		OpponentGCID:    otherGCID,
		Status:          duel.StatusInProgress,
		Scope:           duel.ScopeRanked,
		Category:        "math",
		Mode:            duel.ModeBlitz,
		BlitzConfig:     duel.BlitzConfig{Variant: duel.BlitzVariantTimed, TimeLimitSec: 60},
		BlitzStartedAt:  bPtr(time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)),
		RoundCount:      1,
		ScoreChallenger: 1,
		ScoreOpponent:   0,
		ComboChallenger: 2,
		ComboOpponent:   0,
		InterestTags:    []string{"math"},
		WinnerGCID:      "",
		CreatedAt:       time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
		UpdatedAt:       time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
		CompletedAt:     nil,
		ExpiresAt:       nil,
		Rounds: []duel.RoundSnapshot{
			{
				RoundNumber:   1,
				AtomID:        "atom-1",
				Question:      "Q",
				Options:       []string{"a", "b"},
				CorrectAnswer: "a",
				DeadlineAt:    bPtr(time.Date(2026, 7, 10, 12, 1, 0, 0, time.UTC)),
			},
		},
	}
}

func TestDuelRepo_NilTxRunner_ReturnsErrNotImplemented(t *testing.T) {
	t.Parallel()
	r := pg.NewDuelRepo(nil)
	if err := r.SaveDuel(context.Background(), testDuel()); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("SaveDuel: expected ErrNotImplemented; got %v", err)
	}
	if _, err := r.GetDuel(context.Background(), duelID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("GetDuel: expected ErrNotImplemented; got %v", err)
	}
	if _, err := r.GetDuelForUpdate(context.Background(), duelID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("GetDuelForUpdate: expected ErrNotImplemented; got %v", err)
	}
	if _, _, err := r.ListDuels(context.Background(), tenantID, authorGCID, "", 10, ""); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("ListDuels: expected ErrNotImplemented; got %v", err)
	}
	if _, err := r.ListDuelsWithExpiredRounds(context.Background(), tenantID, time.Now()); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("ListDuelsWithExpiredRounds: expected ErrNotImplemented; got %v", err)
	}
	if err := r.ResolveRound(context.Background(), testDuel(), 1, authorGCID, duel.RoundResolution{}); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("ResolveRound: expected ErrNotImplemented; got %v", err)
	}
	if err := r.StampRoundDeadline(context.Background(), duelID, 1, time.Now()); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("StampRoundDeadline: expected ErrNotImplemented; got %v", err)
	}
	if err := r.ApplyELO(context.Background(), testDuel(), 32); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("ApplyELO: expected ErrNotImplemented; got %v", err)
	}
	if _, err := r.GetRating(context.Background(), tenantID, authorGCID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("GetRating: expected ErrNotImplemented; got %v", err)
	}
	if _, err := r.TopRatings(context.Background(), tenantID, 10); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("TopRatings: expected ErrNotImplemented; got %v", err)
	}
	if _, err := r.FindNearbyRatings(context.Background(), tenantID, authorGCID, 10); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("FindNearbyRatings: expected ErrNotImplemented; got %v", err)
	}
}

func TestDuelRepo_SaveDuel_RejectsNil(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewDuelRepo(&stubTxRunner{q: q})
	if err := r.SaveDuel(tracing.WithTenantID(context.Background(), tenantID), nil); !errors.Is(err, duel.ErrInvalidArgument) {
		t.Fatalf("expected duel.ErrInvalidArgument; got %v", err)
	}
}

func TestDuelRepo_SaveDuel_UpsertsSessionAndRounds(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewDuelRepo(&stubTxRunner{q: q})
	d := testDuel()
	d.CreatedAt = time.Time{} // force the now() default branch
	if err := r.SaveDuel(tracing.WithTenantID(context.Background(), tenantID), d); err != nil {
		t.Fatalf("SaveDuel: %v", err)
	}
	// SET LOCAL + duel_sessions upsert + 1 round upsert.
	if len(q.sqls) != 3 {
		t.Fatalf("expected 3 SQLs; got %d: %v", len(q.sqls), q.sqls)
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL") {
		t.Fatalf("first SQL must be SET LOCAL; got %q", q.sqls[0])
	}
	if !strings.Contains(q.sqls[1], "INSERT INTO duel_sessions") || !strings.Contains(q.sqls[1], "ON CONFLICT (id) DO UPDATE") {
		t.Fatalf("expected duel_sessions upsert; got %q", q.sqls[1])
	}
	if !strings.Contains(q.sqls[2], "INSERT INTO duel_rounds") {
		t.Fatalf("expected duel_rounds upsert; got %q", q.sqls[2])
	}
	// Session upsert binds 23 args.
	if len(q.args[1]) != 23 {
		t.Fatalf("session upsert binds 23 args; got %d: %v", len(q.args[1]), q.args[1])
	}
}

func TestDuelRepo_SaveDuel_ExecErrors(t *testing.T) {
	t.Parallel()

	t.Run("session upsert error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerierB{execErrFn: func(sql string) error {
			if strings.Contains(sql, "SET LOCAL") {
				return nil
			}
			return errors.New("boom")
		}}
		r := pg.NewDuelRepo(&stubTxRunnerB{q: q})
		err := r.SaveDuel(tracing.WithTenantID(context.Background(), tenantID), testDuel())
		if err == nil || !strings.Contains(err.Error(), "upsert duel session") {
			t.Fatalf("expected wrapped session error; got %v", err)
		}
	})

	t.Run("round upsert error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerierB{execErrFn: func(sql string) error {
			if strings.Contains(sql, "SET LOCAL") {
				return nil
			}
			if strings.Contains(sql, "INSERT INTO duel_sessions") {
				return nil
			}
			return errors.New("boom")
		}}
		r := pg.NewDuelRepo(&stubTxRunnerB{q: q})
		err := r.SaveDuel(tracing.WithTenantID(context.Background(), tenantID), testDuel())
		if err == nil || !strings.Contains(err.Error(), "upsert duel round 1") {
			t.Fatalf("expected wrapped round error; got %v", err)
		}
	})
}

// duelRowScanB feeds the 22-column duel_sessions SELECT shapes.
func duelRowScanB(dest ...any) error {
	bSetInto(dest, 0, duelID)
	bSetInto(dest, 1, tenantID)
	bSetInto(dest, 2, authorGCID)
	bSetInto(dest, 3, otherGCID)
	bSetInto(dest, 4, "in_progress")
	bSetInto(dest, 5, "ranked")
	bSetInto(dest, 6, bPtr(""))
	bSetInto(dest, 7, 1)
	bSetInto(dest, 8, 1)
	bSetInto(dest, 9, 0)
	bSetInto(dest, 10, 2)
	bSetInto(dest, 11, 0)
	bSetInto(dest, 12, []string{"math"})
	bSetInto(dest, 13, time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC))
	bSetInto(dest, 14, bPtr(time.Time{}))
	bSetInto(dest, 15, bPtr(time.Time{}))
	bSetInto(dest, 16, "math")
	bSetInto(dest, 17, "blitz")
	bSetInto(dest, 18, bPtr("timed"))
	bSetInto(dest, 19, 60)
	bSetInto(dest, 20, 0)
	bSetInto(dest, 21, bPtr(time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)))
	return nil
}

// duelRoundScanB feeds scanDuelRounds (21 columns).
func duelRoundScanB(dest ...any) error {
	bSetInto(dest, 0, 1)
	bSetInto(dest, 1, "atom-1")
	bSetInto(dest, 2, bPtr("rev-1"))
	bSetInto(dest, 3, "Q")
	bSetInto(dest, 4, []string{"a", "b"})
	bSetInto(dest, 5, "a")
	bSetInto(dest, 6, bPtr("x"))
	bSetInto(dest, 7, bPtr("y"))
	bSetInto(dest, 8, bPtr(int64(10)))
	bSetInto(dest, 9, bPtr(int64(20)))
	bSetInto(dest, 10, bPtr(true))
	bSetInto(dest, 11, bPtr(false))
	bSetInto(dest, 12, bPtr(2))
	bSetInto(dest, 13, bPtr(3))
	bSetInto(dest, 14, bPtr(5))
	bSetInto(dest, 15, bPtr(7))
	bSetInto(dest, 16, true)
	bSetInto(dest, 17, true)
	bSetInto(dest, 18, bPtr(""))
	bSetInto(dest, 19, bPtr(time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)))
	bSetInto(dest, 20, bPtr(time.Date(2026, 7, 10, 12, 1, 0, 0, time.UTC)))
	return nil
}

func TestDuelRepo_GetDuel_HappyWithRounds(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		rowFn: func(sql string, args ...any) pg.Row {
			if strings.Contains(sql, "FOR UPDATE") {
				t.Fatalf("GetDuel must not use FOR UPDATE")
			}
			return stubRow{scanFn: duelRowScanB}
		},
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{duelRoundScanB}}, nil
		},
	}
	r := pg.NewDuelRepo(&stubTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	d, err := r.GetDuel(ctx, duelID)
	if err != nil {
		t.Fatalf("GetDuel: %v", err)
	}
	if d == nil || d.ID != duelID || len(d.Rounds) != 1 {
		t.Fatalf("scan wrong: %+v", d)
	}
	if d.Mode != duel.ModeBlitz || d.BlitzConfig.Variant != duel.BlitzVariantTimed {
		t.Fatalf("blitz fields wrong: %+v", d)
	}
	if len(d.Rounds[0].Options) != 2 || !d.Rounds[0].ChallengerCorrect {
		t.Fatalf("round scan wrong: %+v", d.Rounds[0])
	}
	// Rounds loaded via sqlDuelRoundsBySession in same tx.
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "FROM duel_rounds") {
		t.Fatalf("expected round load query; got %q", last)
	}
}

func TestDuelRepo_GetDuelForUpdate_UsesForUpdate(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		rowFn: func(sql string, args ...any) pg.Row {
			if !strings.Contains(sql, "FOR UPDATE") {
				t.Fatalf("GetDuelForUpdate must append FOR UPDATE; got %q", sql)
			}
			return stubRow{scanFn: duelRowScanB}
		},
		queryFn: func(sql string, args ...any) (pg.Rows, error) { return &stubRows{}, nil },
	}
	r := pg.NewDuelRepo(&stubTxRunner{q: q})
	d, err := r.GetDuelForUpdate(tracing.WithTenantID(context.Background(), tenantID), duelID)
	if err != nil {
		t.Fatalf("GetDuelForUpdate: %v", err)
	}
	if d == nil {
		t.Fatalf("expected duel")
	}
}

func TestDuelRepo_GetDuel_ErrorMapping(t *testing.T) {
	t.Parallel()

	t.Run("no rows → ErrDuelNotFound", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error { return noRowsB() }}
		}}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		if _, err := r.GetDuel(tracing.WithTenantID(context.Background(), tenantID), duelID); !errors.Is(err, pg.ErrDuelNotFound) {
			t.Fatalf("expected ErrDuelNotFound; got %v", err)
		}
	})

	t.Run("generic scan error wraps", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error { return errors.New("boom") }}
		}}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		_, err := r.GetDuel(tracing.WithTenantID(context.Background(), tenantID), duelID)
		if err == nil || !strings.Contains(err.Error(), "scan duel") {
			t.Fatalf("expected wrapped scan error; got %v", err)
		}
	})

	t.Run("round load error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			rowFn:   func(sql string, args ...any) pg.Row { return stubRow{scanFn: duelRowScanB} },
			queryFn: func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("rounds boom") },
		}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		if _, err := r.GetDuel(tracing.WithTenantID(context.Background(), tenantID), duelID); err == nil {
			t.Fatalf("round load error must propagate")
		}
	})
}

func TestDuelRepo_ListDuels_CursorAndStatus(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			if !strings.Contains(sql, "AND status = $6") {
				t.Fatalf("expected status cond at $6; got %q", sql)
			}
			if !strings.Contains(sql, "AND created_at < (SELECT created_at FROM duel_sessions WHERE id = $5)") {
				t.Fatalf("expected cursor cond at $5; got %q", sql)
			}
			if len(args) != 6 {
				t.Fatalf("expected 6 args; got %v", args)
			}
			return &stubRows{scans: []func(dest ...any) error{duelRowScanB}}, nil
		},
	}
	r := pg.NewDuelRepo(&stubTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	duels, next, err := r.ListDuels(ctx, tenantID, authorGCID, "in_progress", 10, "cursor-1")
	if err != nil {
		t.Fatalf("ListDuels: %v", err)
	}
	if len(duels) != 1 || next != "" {
		t.Fatalf("expected 1 duel, no cursor; got %d, %q", len(duels), next)
	}
	if duels[0].ID != duelID {
		t.Fatalf("scan wrong: %+v", duels[0])
	}
}

func TestDuelRepo_ListDuels_NoFiltersAndFullPage(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			if strings.Contains(sql, "AND status") || strings.Contains(sql, "created_at <") {
				t.Fatalf("no filters expected; got %q", sql)
			}
			if len(args) != 4 {
				t.Fatalf("expected 4 args; got %v", args)
			}
			scans := make([]func(dest ...any) error, 3)
			for i := range scans {
				scans[i] = func(dest ...any) error {
					bSetInto(dest, 0, duelID)
					return nil
				}
			}
			return &stubRows{scans: scans}, nil
		},
	}
	r := pg.NewDuelRepo(&stubTxRunner{q: q})
	duels, next, err := r.ListDuels(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "", 3, "")
	if err != nil {
		t.Fatalf("ListDuels: %v", err)
	}
	if len(duels) != 2 {
		t.Fatalf("full page must trim last row (cursor); got %d", len(duels))
	}
	if next != duelID {
		t.Fatalf("expected next cursor set; got %q", next)
	}
}

func TestDuelRepo_ListDuels_Errors(t *testing.T) {
	t.Parallel()

	t.Run("query error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("q") }}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		if _, _, err := r.ListDuels(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "", 10, ""); err == nil {
			t.Fatalf("query error must propagate")
		}
	})

	t.Run("scan error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{func(dest ...any) error { return errors.New("s") }}}, nil
		}}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		if _, _, err := r.ListDuels(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "", 10, ""); err == nil {
			t.Fatalf("scan error must propagate")
		}
	})

	t.Run("rows.Err", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{}, err: errors.New("r")}, nil
		}}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		if _, _, err := r.ListDuels(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "", 10, ""); err == nil {
			t.Fatalf("rows.Err must propagate")
		}
	})
}

func TestDuelRepo_ListDuelsWithExpiredRounds_Errors(t *testing.T) {
	t.Parallel()

	t.Run("query error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("q") }}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		if _, err := r.ListDuelsWithExpiredRounds(tracing.WithTenantID(context.Background(), tenantID), tenantID, time.Now()); err == nil {
			t.Fatalf("query error must propagate")
		}
	})

	t.Run("round load error", func(t *testing.T) {
		t.Parallel()
		queries := 0
		q := &stubQuerier{
			queryFn: func(sql string, args ...any) (pg.Rows, error) {
				queries++
				if queries == 1 {
					return &stubRows{scans: []func(dest ...any) error{duelRowScanB}}, nil
				}
				return nil, errors.New("rounds boom")
			},
		}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		_, err := r.ListDuelsWithExpiredRounds(tracing.WithTenantID(context.Background(), tenantID), tenantID, time.Now())
		if err == nil || !strings.Contains(err.Error(), "load rounds for sweeper duel") {
			t.Fatalf("expected wrapped round load error; got %v", err)
		}
	})
}

func TestDuelRepo_ResolveRound_ParticipantPaths(t *testing.T) {
	t.Parallel()

	base := func() *duel.Duel { return testDuel() }

	t.Run("challenger answer with resolved round + next deadline", func(t *testing.T) {
		t.Parallel()
		resolved := time.Date(2026, 7, 10, 12, 0, 30, 0, time.UTC)
		nextDeadline := time.Date(2026, 7, 10, 12, 2, 0, 0, time.UTC)
		d := base()
		d.Rounds = []duel.RoundSnapshot{
			{RoundNumber: 1, ResolvedAt: &resolved, WinnerGCID: authorGCID},
			{RoundNumber: 2, DeadlineAt: &nextDeadline},
		}
		q := &stubQuerier{}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		err := r.ResolveRound(tracing.WithTenantID(context.Background(), tenantID), d, 1, authorGCID, duel.RoundResolution{
			Correct: true, ComboMultiplier: 2, PointsAwarded: 5,
		})
		if err != nil {
			t.Fatalf("ResolveRound: %v", err)
		}
		// 4 execs: resolve, mark-opponent-answered, stamp-next-deadline, session upsert.
		if len(q.sqls) != 5 {
			t.Fatalf("expected SET LOCAL + 4 execs; got %d: %v", len(q.sqls), q.sqls)
		}
		if !strings.Contains(q.sqls[1], "UPDATE duel_rounds") || !strings.Contains(q.sqls[1], "resolved_at IS NULL") {
			t.Fatalf("expected round resolve UPDATE; got %q", q.sqls[1])
		}
		if !strings.Contains(q.sqls[2], "opponent_answered   = true") {
			t.Fatalf("expected mark-opponent-answered; got %q", q.sqls[2])
		}
		if !strings.Contains(q.sqls[3], "deadline_at IS NULL") {
			t.Fatalf("expected stamp-next-deadline; got %q", q.sqls[3])
		}
		if !strings.Contains(q.sqls[4], "INSERT INTO duel_sessions") {
			t.Fatalf("expected final session upsert; got %q", q.sqls[4])
		}
	})

	t.Run("opponent answer", func(t *testing.T) {
		t.Parallel()
		d := base()
		q := &stubQuerier{}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		if err := r.ResolveRound(tracing.WithTenantID(context.Background(), tenantID), d, 1, otherGCID, duel.RoundResolution{Correct: false}); err != nil {
			t.Fatalf("ResolveRound: %v", err)
		}
		// opponent path binds opp fields; round unresolved → no mark/stamp.
		if len(q.sqls) != 3 {
			t.Fatalf("expected SET LOCAL + resolve + upsert; got %d", len(q.sqls))
		}
	})

	t.Run("timeout path", func(t *testing.T) {
		t.Parallel()
		d := base()
		q := &stubQuerier{}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		if err := r.ResolveRound(tracing.WithTenantID(context.Background(), tenantID), d, 1, "", duel.RoundResolution{RoundTimeout: true}); err != nil {
			t.Fatalf("ResolveRound: %v", err)
		}
		if len(q.sqls) != 3 {
			t.Fatalf("timeout path = resolve + upsert only; got %d", len(q.sqls))
		}
	})

	t.Run("non-participant rejected", func(t *testing.T) {
		t.Parallel()
		d := base()
		q := &stubQuerier{}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		err := r.ResolveRound(tracing.WithTenantID(context.Background(), tenantID), d, 1, "someone-else", duel.RoundResolution{})
		if !errors.Is(err, duel.ErrNotParticipant) {
			t.Fatalf("expected ErrNotParticipant; got %v", err)
		}
	})

	t.Run("resolve exec error", func(t *testing.T) {
		t.Parallel()
		d := base()
		q := &stubQuerierB{execErrFn: func(sql string) error {
			if strings.Contains(sql, "SET LOCAL") || strings.Contains(sql, "INSERT INTO duel_sessions") {
				return nil
			}
			return errors.New("boom")
		}}
		r := pg.NewDuelRepo(&stubTxRunnerB{q: q})
		err := r.ResolveRound(tracing.WithTenantID(context.Background(), tenantID), d, 1, authorGCID, duel.RoundResolution{Correct: true})
		if err == nil || !strings.Contains(err.Error(), "resolve duel round 1") {
			t.Fatalf("expected wrapped resolve error; got %v", err)
		}
	})

	t.Run("mark-opponent exec error", func(t *testing.T) {
		t.Parallel()
		resolved := time.Date(2026, 7, 10, 12, 0, 30, 0, time.UTC)
		d := base()
		d.Rounds = []duel.RoundSnapshot{{RoundNumber: 1, ResolvedAt: &resolved}}
		q := &stubQuerierB{execErrFn: func(sql string) error {
			if strings.Contains(sql, "SET LOCAL") || strings.Contains(sql, "INSERT INTO duel_sessions") {
				return nil
			}
			if strings.Contains(sql, "opponent_answered   = true") {
				return errors.New("boom")
			}
			return nil
		}}
		r := pg.NewDuelRepo(&stubTxRunnerB{q: q})
		err := r.ResolveRound(tracing.WithTenantID(context.Background(), tenantID), d, 1, authorGCID, duel.RoundResolution{Correct: true})
		if err == nil || !strings.Contains(err.Error(), "mark opponent answered round 1") {
			t.Fatalf("expected wrapped mark-opponent error; got %v", err)
		}
	})

	t.Run("stamp-next-deadline exec error", func(t *testing.T) {
		t.Parallel()
		resolved := time.Date(2026, 7, 10, 12, 0, 30, 0, time.UTC)
		nextDeadline := time.Date(2026, 7, 10, 12, 2, 0, 0, time.UTC)
		d := base()
		d.Rounds = []duel.RoundSnapshot{
			{RoundNumber: 1, ResolvedAt: &resolved},
			{RoundNumber: 2, DeadlineAt: &nextDeadline},
		}
		q := &stubQuerierB{execErrFn: func(sql string) error {
			if strings.Contains(sql, "SET LOCAL") || strings.Contains(sql, "INSERT INTO duel_sessions") {
				return nil
			}
			if strings.Contains(sql, "UPDATE duel_rounds") && !strings.Contains(sql, "opponent_answered") && strings.Contains(sql, "deadline_at") {
				return errors.New("boom")
			}
			return nil
		}}
		r := pg.NewDuelRepo(&stubTxRunnerB{q: q})
		err := r.ResolveRound(tracing.WithTenantID(context.Background(), tenantID), d, 1, authorGCID, duel.RoundResolution{Correct: true})
		if err == nil || !strings.Contains(err.Error(), "stamp next round 2 deadline") {
			t.Fatalf("expected wrapped stamp error; got %v", err)
		}
	})

	t.Run("session upsert error after resolve", func(t *testing.T) {
		t.Parallel()
		d := base()
		q := &stubQuerierB{execErrFn: func(sql string) error {
			if strings.Contains(sql, "SET LOCAL") {
				return nil
			}
			if strings.Contains(sql, "INSERT INTO duel_sessions") {
				return errors.New("boom")
			}
			return nil
		}}
		r := pg.NewDuelRepo(&stubTxRunnerB{q: q})
		err := r.ResolveRound(tracing.WithTenantID(context.Background(), tenantID), d, 1, authorGCID, duel.RoundResolution{Correct: true})
		if err == nil || !strings.Contains(err.Error(), "update duel session after resolve") {
			t.Fatalf("expected wrapped session upsert error; got %v", err)
		}
	})

	t.Run("nil duel rejected", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		if err := r.ResolveRound(context.Background(), nil, 1, authorGCID, duel.RoundResolution{}); !errors.Is(err, duel.ErrInvalidArgument) {
			t.Fatalf("expected duel.ErrInvalidArgument; got %v", err)
		}
	})
}

func TestDuelRepo_StampRoundDeadline(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewDuelRepo(&stubTxRunner{q: q})
	deadline := time.Date(2026, 7, 10, 12, 2, 0, 0, time.UTC)
	if err := r.StampRoundDeadline(tracing.WithTenantID(context.Background(), tenantID), duelID, 2, deadline); err != nil {
		t.Fatalf("StampRoundDeadline: %v", err)
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "deadline_at IS NULL") {
		t.Fatalf("expected idempotent deadline stamp; got %q", last)
	}

	errQ := &stubQuerierB{execErrFn: func(sql string) error {
		if strings.Contains(sql, "SET LOCAL") {
			return nil
		}
		return errors.New("boom")
	}}
	r2 := pg.NewDuelRepo(&stubTxRunnerB{q: errQ})
	err := r2.StampRoundDeadline(tracing.WithTenantID(context.Background(), tenantID), duelID, 2, deadline)
	if err == nil || !strings.Contains(err.Error(), "stamp round 2 deadline") {
		t.Fatalf("expected wrapped stamp error; got %v", err)
	}
}

// ratingRowScanB feeds the 5-column duel_ratings SELECT (rating, wins, losses, draws, peak).
func ratingRowScanB(rating, wins, losses, draws, peak int) func(dest ...any) error {
	return func(dest ...any) error {
		bSetInto(dest, 0, rating)
		bSetInto(dest, 1, wins)
		bSetInto(dest, 2, losses)
		bSetInto(dest, 3, draws)
		bSetInto(dest, 4, peak)
		return nil
	}
}

func TestDuelRepo_ApplyELO_GuardBranches(t *testing.T) {
	t.Parallel()

	t.Run("nil duel rejected", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		if err := r.ApplyELO(context.Background(), nil, 32); !errors.Is(err, duel.ErrInvalidArgument) {
			t.Fatalf("expected duel.ErrInvalidArgument; got %v", err)
		}
	})

	t.Run("non-ranked scope is a no-op", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		d := testDuel()
		d.Scope = duel.ScopeFriendly
		if err := r.ApplyELO(tracing.WithTenantID(context.Background(), tenantID), d, 32); err != nil {
			t.Fatalf("ApplyELO: %v", err)
		}
		if len(q.sqls) != 0 {
			t.Fatalf("friendly scope must issue NO SQL; got %v", q.sqls)
		}
	})

	t.Run("uncompleted status is a no-op", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		d := testDuel()
		d.Status = duel.StatusInProgress
		if err := r.ApplyELO(tracing.WithTenantID(context.Background(), tenantID), d, 32); err != nil {
			t.Fatalf("ApplyELO: %v", err)
		}
		if len(q.sqls) != 0 {
			t.Fatalf("in-progress must issue NO SQL; got %v", q.sqls)
		}
	})

	t.Run("kFactor defaulted", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			rowFn: func(sql string, args ...any) pg.Row { return stubRow{scanFn: ratingRowScanB(1400, 10, 2, 1, 1500)} },
		}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		d := testDuel()
		d.Status = duel.StatusCompleted
		d.WinnerGCID = authorGCID
		if err := r.ApplyELO(tracing.WithTenantID(context.Background(), tenantID), d, 0); err != nil {
			t.Fatalf("ApplyELO: %v", err)
		}
		if len(q.sqls) == 0 {
			t.Fatalf("expected rating ops")
		}
	})
}

func TestDuelRepo_ApplyELO_Draw(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		rowFn: func(sql string, args ...any) pg.Row { return stubRow{scanFn: ratingRowScanB(1400, 10, 2, 1, 1500)} },
	}
	r := pg.NewDuelRepo(&stubTxRunner{q: q})
	d := testDuel()
	d.Status = duel.StatusCompleted
	d.WinnerGCID = ""
	if err := r.ApplyELO(tracing.WithTenantID(context.Background(), tenantID), d, 32); err != nil {
		t.Fatalf("ApplyELO draw: %v", err)
	}
	// SET LOCAL + 2 rating selects + 2 upserts.
	if len(q.sqls) != 5 {
		t.Fatalf("draw path = SET LOCAL + 2 selects + 2 upserts; got %d: %v", len(q.sqls), q.sqls)
	}
}

func TestDuelRepo_ApplyELO_WinnerPathsAndErrors(t *testing.T) {
	t.Parallel()

	t.Run("challenger wins", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			rowFn: func(sql string, args ...any) pg.Row { return stubRow{scanFn: ratingRowScanB(1400, 10, 2, 1, 1500)} },
		}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		d := testDuel()
		d.Status = duel.StatusForfeited
		d.WinnerGCID = authorGCID
		if err := r.ApplyELO(tracing.WithTenantID(context.Background(), tenantID), d, 32); err != nil {
			t.Fatalf("ApplyELO: %v", err)
		}
		if len(q.sqls) != 5 {
			t.Fatalf("ladder path = SET LOCAL + 2 selects + 2 upserts; got %d", len(q.sqls))
		}
	})

	t.Run("opponent wins (loser = challenger)", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			rowFn: func(sql string, args ...any) pg.Row { return stubRow{scanFn: ratingRowScanB(1400, 10, 2, 1, 1500)} },
		}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		d := testDuel()
		d.Status = duel.StatusCompleted
		d.WinnerGCID = otherGCID
		if err := r.ApplyELO(tracing.WithTenantID(context.Background(), tenantID), d, 32); err != nil {
			t.Fatalf("ApplyELO: %v", err)
		}
		if len(q.sqls) != 5 {
			t.Fatalf("expected 5 SQLs; got %d", len(q.sqls))
		}
	})

	t.Run("fresh player rating select (ErrNoRows) defaults", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error { return noRowsB() }}
		}}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		d := testDuel()
		d.Status = duel.StatusCompleted
		d.WinnerGCID = authorGCID
		if err := r.ApplyELO(tracing.WithTenantID(context.Background(), tenantID), d, 32); err != nil {
			t.Fatalf("ApplyELO fresh: %v", err)
		}
	})

	t.Run("rating select generic error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error { return errors.New("conn lost") }}
		}}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		d := testDuel()
		d.Status = duel.StatusCompleted
		d.WinnerGCID = authorGCID
		err := r.ApplyELO(tracing.WithTenantID(context.Background(), tenantID), d, 32)
		if err == nil || !strings.Contains(err.Error(), "read duel rating") {
			t.Fatalf("expected wrapped rating error; got %v", err)
		}
	})

	t.Run("upsert rating error", func(t *testing.T) {
		t.Parallel()
		qB := &stubQuerierB{
			rowFn: func(sql string, args ...any) pg.Row { return stubRow{scanFn: ratingRowScanB(1400, 10, 2, 1, 1500)} },
			execErrFn: func(sql string) error {
				if strings.Contains(sql, "SET LOCAL") {
					return nil
				}
				if strings.Contains(sql, "INSERT INTO duel_ratings") {
					return errors.New("boom")
				}
				return nil
			},
		}
		r := pg.NewDuelRepo(&stubTxRunnerB{q: qB})
		d := testDuel()
		d.Status = duel.StatusCompleted
		d.WinnerGCID = authorGCID
		err := r.ApplyELO(tracing.WithTenantID(context.Background(), tenantID), d, 32)
		if err == nil || !strings.Contains(err.Error(), "upsert duel rating") {
			t.Fatalf("expected wrapped upsert error; got %v", err)
		}
	})
}

func TestDuelRepo_RatingsReads(t *testing.T) {
	t.Parallel()

	t.Run("GetRatingStatsForCategory happy", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: ratingRowScanB(1300, 5, 3, 2, 1400)}
		}}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		rs, err := r.GetRatingStatsForCategory(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "math")
		if err != nil {
			t.Fatalf("GetRatingStatsForCategory: %v", err)
		}
		if rs.Rating != 1300 || rs.Wins != 5 || rs.PeakELO != 1400 {
			t.Fatalf("ratings wrong: %+v", rs)
		}
	})

	t.Run("fresh player defaults 1200", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error { return noRowsB() }}
		}}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		rs, err := r.GetRatingStatsForCategory(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "")
		if err != nil {
			t.Fatalf("GetRatingStatsForCategory: %v", err)
		}
		if rs.Rating != 1200 || rs.PeakELO != 1200 {
			t.Fatalf("expected 1200 defaults; got %+v", rs)
		}
		// empty category → "overall" bound.
		if cat, _ := q.args[len(q.args)-1][2].(string); cat != "overall" {
			t.Fatalf("empty category must default to overall; got %v", q.args[len(q.args)-1][2])
		}
	})

	t.Run("zero rating row re-defaults to 1200", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: ratingRowScanB(0, 0, 0, 0, 0)}
		}}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		rs, err := r.GetRatingStatsForCategory(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, "math")
		if err != nil {
			t.Fatalf("GetRatingStatsForCategory: %v", err)
		}
		if rs.Rating != 1200 || rs.PeakELO != 1200 {
			t.Fatalf("zero row must re-default; got %+v", rs)
		}
	})

	t.Run("GetRatingForCategory error path", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error { return errors.New("boom") }}
		}}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		// No tenant on ctx → rls.ApplySession fails inside the tx, so
		// GetRatingStatsForCategory surfaces the error and GetRatingForCategory
		// must return the 1200 default alongside it.
		rating, err := r.GetRatingForCategory(context.Background(), tenantID, authorGCID, "math")
		if err == nil || rating != 1200 {
			t.Fatalf("expected 1200 + error; got %d, %v", rating, err)
		}
	})

	t.Run("delegates default category", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: ratingRowScanB(1350, 1, 0, 0, 1350)}
		}}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		ctx := tracing.WithTenantID(context.Background(), tenantID)
		rating, err := r.GetRating(ctx, tenantID, authorGCID)
		if err != nil || rating != 1350 {
			t.Fatalf("GetRating: got %d, %v", rating, err)
		}
		if _, err := r.GetRatingStats(ctx, tenantID, authorGCID); err != nil {
			t.Fatalf("GetRatingStats: %v", err)
		}
		// both delegate to "overall".
		if cat, _ := q.args[len(q.args)-1][2].(string); cat != "overall" {
			t.Fatalf("delegates must use overall; got %v", q.args[len(q.args)-1][2])
		}
	})
}

func TestDuelRepo_TopRatings(t *testing.T) {
	t.Parallel()

	t.Run("happy + limit clamp", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			queryFn: func(sql string, args ...any) (pg.Rows, error) {
				return &stubRows{scans: []func(dest ...any) error{func(dest ...any) error {
					bSetInto(dest, 0, authorGCID)
					bSetInto(dest, 1, "Walfa")
					bSetInto(dest, 2, 1500)
					bSetInto(dest, 3, 9)
					bSetInto(dest, 4, 1)
					bSetInto(dest, 5, 0)
					bSetInto(dest, 6, 1600)
					return nil
				}}}, nil
			},
		}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		results, err := r.TopRatingsForCategory(tracing.WithTenantID(context.Background(), tenantID), tenantID, "math", 500)
		if err != nil {
			t.Fatalf("TopRatingsForCategory: %v", err)
		}
		if len(results) != 1 || results[0].Rating != 1500 || results[0].DisplayName != "Walfa" {
			t.Fatalf("top ratings wrong: %+v", results)
		}
		if lim, _ := q.args[len(q.args)-1][2].(int); lim != 20 {
			t.Fatalf("limit 500 must clamp to 20; got %v", q.args[len(q.args)-1][2])
		}
	})

	t.Run("default category on delegate", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		if _, err := r.TopRatings(tracing.WithTenantID(context.Background(), tenantID), tenantID, 5); err != nil {
			t.Fatalf("TopRatings: %v", err)
		}
		if cat, _ := q.args[len(q.args)-1][1].(string); cat != "overall" {
			t.Fatalf("TopRatings must default category; got %v", q.args[len(q.args)-1][1])
		}
	})

	t.Run("errors", func(t *testing.T) {
		t.Parallel()
		errQ := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("q") }}
		r1 := pg.NewDuelRepo(&stubTxRunner{q: errQ})
		if _, err := r1.TopRatingsForCategory(tracing.WithTenantID(context.Background(), tenantID), tenantID, "math", 10); err == nil {
			t.Fatalf("query error must propagate")
		}

		scanQ := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{func(dest ...any) error { return errors.New("s") }}}, nil
		}}
		r2 := pg.NewDuelRepo(&stubTxRunner{q: scanQ})
		if _, err := r2.TopRatingsForCategory(tracing.WithTenantID(context.Background(), tenantID), tenantID, "math", 10); err == nil {
			t.Fatalf("scan error must propagate")
		}

		rowsErrQ := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{}, err: errors.New("r")}, nil
		}}
		r3 := pg.NewDuelRepo(&stubTxRunner{q: rowsErrQ})
		if _, err := r3.TopRatingsForCategory(tracing.WithTenantID(context.Background(), tenantID), tenantID, "math", 10); err == nil {
			t.Fatalf("rows.Err must propagate")
		}
	})
}

func TestDuelRepo_FindNearbyRatings(t *testing.T) {
	t.Parallel()

	t.Run("happy + clamp", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			queryFn: func(sql string, args ...any) (pg.Rows, error) {
				return &stubRows{scans: []func(dest ...any) error{func(dest ...any) error {
					bSetInto(dest, 0, otherGCID)
					bSetInto(dest, 1, 1300)
					bSetInto(dest, 2, 3)
					bSetInto(dest, 3, 1)
					bSetInto(dest, 4, 0)
					bSetInto(dest, 5, 1350)
					return nil
				}}}, nil
			},
		}
		r := pg.NewDuelRepo(&stubTxRunner{q: q})
		results, err := r.FindNearbyRatings(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, 500)
		if err != nil {
			t.Fatalf("FindNearbyRatings: %v", err)
		}
		if len(results) != 1 || results[0].GCID != otherGCID {
			t.Fatalf("nearby wrong: %+v", results)
		}
		if lim, _ := q.args[len(q.args)-1][2].(int); lim != 10 {
			t.Fatalf("limit 500 must clamp to 10; got %v", q.args[len(q.args)-1][2])
		}
	})

	t.Run("errors", func(t *testing.T) {
		t.Parallel()
		errQ := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("q") }}
		r1 := pg.NewDuelRepo(&stubTxRunner{q: errQ})
		if _, err := r1.FindNearbyRatings(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, 5); err == nil {
			t.Fatalf("query error must propagate")
		}

		scanQ := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{func(dest ...any) error { return errors.New("s") }}}, nil
		}}
		r2 := pg.NewDuelRepo(&stubTxRunner{q: scanQ})
		if _, err := r2.FindNearbyRatings(tracing.WithTenantID(context.Background(), tenantID), tenantID, authorGCID, 5); err == nil {
			t.Fatalf("scan error must propagate")
		}
	})
}
