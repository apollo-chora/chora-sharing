// Package ws — duel session adapter that bridges the WebSocket handler to
// the duel domain + HTTP answer-submission flow.
//
// Implements ws.DuelLookup + ws.AnswerProcessor by delegating to the
// duel.DuelRepo and the HTTP handler's answer-submission logic. This
// adapter is constructed in main.go and injected into the WS handler.
package ws

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/domain/duel"
)

type DuelRepoPort interface {
	GetDuel(ctx context.Context, duelID string) (*duel.Duel, error)
	GetDuelForUpdate(ctx context.Context, duelID string) (*duel.Duel, error)
	ResolveRound(ctx context.Context, d *duel.Duel, roundNo int, gcid string, res duel.RoundResolution) error
	// StampRoundDeadline persists the lazy first-serve deadline on an
	// unresolved round. Idempotent — a round that already has a deadline
	// is left untouched (the second player to connect must not reset
	// the timer).
	StampRoundDeadline(ctx context.Context, duelID string, roundNo int, deadline time.Time) error
	// SaveDuel persists the full duel state. Used by blitz mode to save
	// the duel after a blitz answer resolves (blitz answers mutate the
	// duel's score/combo/round state directly, unlike classic which
	// uses ResolveRound for per-round persistence).
	SaveDuel(ctx context.Context, d *duel.Duel) error
}

type AtomPickerPort interface {
	GetAtomForRound(ctx context.Context, d *duel.Duel, roundNo int) (duel.AtomPick, error)
}

type ComboTiersProvider interface {
	ComboTiers() []int
}

// ELOApplier applies ELO rating updates after a ranked duel completes.
// Implemented by duel.DuelRepo (pg + inmem).
type ELOApplier interface {
	ApplyELO(ctx context.Context, d *duel.Duel, kFactor int) error
}

// DuelEventPublisher emits the duel.completed.v1 event when a duel
// reaches a terminal state. Implemented by the events.Publisher adapter.
type DuelEventPublisher interface {
	PublishDuelCompleted(ctx context.Context, scope, duelID, tenantID, challengerGCID, opponentGCID, winnerGCID string, scoreChallenger, scoreOpponent int) error
}

type DuelSessionAdapter struct {
	repo       DuelRepoPort
	picker     AtomPickerPort
	comboTiers []int
	roundTimerSec int

	eloApplier ELOApplier
	eloKFactor int
	eventPub   DuelEventPublisher
}

func NewDuelSessionAdapter(repo DuelRepoPort, picker AtomPickerPort, comboTiers []int) *DuelSessionAdapter {
	return &DuelSessionAdapter{
		repo:       repo,
		picker:     picker,
		comboTiers: comboTiers,
	}
}

// WithRoundTimerSec wires the per-question deadline (WS3). Falls back to 30
// when unset so the frame always carries a non-zero advertised timer.
func (a *DuelSessionAdapter) WithRoundTimerSec(sec int) *DuelSessionAdapter {
	a.roundTimerSec = sec
	return a
}

// WithELO wires the ELO applier + K-factor for ranked duel completion.
func (a *DuelSessionAdapter) WithELO(applier ELOApplier, kFactor int) *DuelSessionAdapter {
	a.eloApplier = applier
	a.eloKFactor = kFactor
	return a
}

// WithEventPublisher wires the duel.completed event publisher.
func (a *DuelSessionAdapter) WithEventPublisher(pub DuelEventPublisher) *DuelSessionAdapter {
	a.eventPub = pub
	return a
}
func (a *DuelSessionAdapter) GetDuel(ctx context.Context, duelID, tenantID string) (*DuelSnapshot, bool) {
	d, err := a.repo.GetDuel(ctx, duelID)
	if err != nil || d == nil {
		return nil, false
	}
	if d.TenantID != tenantID {
		return nil, false
	}
	return toSnapshot(d), true
}

func (a *DuelSessionAdapter) SubmitAnswer(ctx context.Context, duelID, gcid string, roundNo int32, answer string, answerTimeMs int64) (RoundResolvedFrame, bool, error) {
	d, err := a.repo.GetDuelForUpdate(ctx, duelID)
	if err != nil || d == nil {
		return RoundResolvedFrame{}, false, fmt.Errorf("duel not found")
	}
	// Rehydrate the configured round timer (not persisted) so the
	// next-round deadline stamp uses it instead of the 30s fallback.
	d = d.WithRoundTimerSec(a.roundTimerSec)

	var correct bool
	if a.picker != nil {
		pick, err := a.picker.GetAtomForRound(ctx, d, int(roundNo))
		if err == nil && pick.Answer != "" {
			correct = strings.EqualFold(strings.TrimSpace(answer), strings.TrimSpace(pick.Answer))
		}
	}

	res, err := d.ResolveRound(gcid, int(roundNo), correct, answerTimeMs, a.comboTiers)
	if err != nil {
		return RoundResolvedFrame{}, false, err
	}

	if err := a.repo.ResolveRound(ctx, d, int(roundNo), gcid, res); err != nil {
		return RoundResolvedFrame{}, false, err
	}

	completed := res.DuelStatus == duel.StatusCompleted
	frame := RoundResolvedFrame{
		RoundNo:         roundNo,
		GCID:            gcid,
		Correct:         res.Correct,
		ComboMultiplier: int32(res.ComboMultiplier),
		PointsAwarded:   int32(res.PointsAwarded),
		CurrentCombo:    int32(res.CurrentCombo),
		SpeedBonus:      int32(res.SpeedBonus),
		DuelStatus:      string(res.DuelStatus),
		ScoreChallenger: int32(d.ScoreChallenger),
		ScoreOpponent:   int32(d.ScoreOpponent),
		WinnerGCID:      res.WinnerGCID,
	}

	if completed {
		// Apply ELO for ranked duels (best-effort — the frame is already
		// built; a rating failure doesn't break the gameplay response).
		if a.eloApplier != nil && d.Scope == duel.ScopeRanked {
			kFactor := a.eloKFactor
			if kFactor <= 0 {
				kFactor = 32
			}
		if err := a.eloApplier.ApplyELO(ctx, d, kFactor); err != nil {
			log.Printf("ws: ApplyELO failed duel=%s err=%v", d.ID, err)
		}
		}
		// Publish duel.completed.v1 so downstream consumers (reward
		// crediting, leaderboard refresh) fire.
		if a.eventPub != nil {
			_ = a.eventPub.PublishDuelCompleted(ctx,
				string(d.Scope), d.ID, d.TenantID,
				d.ChallengerGCID, d.OpponentGCID, d.WinnerGCID,
				d.ScoreChallenger, d.ScoreOpponent,
			)
		}
	}

	return frame, completed, nil
}

func (a *DuelSessionAdapter) GetCurrentRound(ctx context.Context, duelID string) (RoundStartFrame, bool) {
	d, err := a.repo.GetDuel(ctx, duelID)
	if err != nil || d == nil || d.Status != duel.StatusInProgress {
		return RoundStartFrame{}, false
	}

	// Blitz: all questions are delivered at once via GetBlitzStart, not
	// one at a time via GetCurrentRound. The WS handler calls GetBlitzStart
	// for blitz duels and GetCurrentRound for classic duels.
	if d.IsBlitz() {
		return RoundStartFrame{}, false
	}

	// FCFS: a round is "current" until ResolvedAt is set (a correct
	// answer, or both-wrong). A round where one player answered wrong
	// but the other hasn't answered yet stays current — the opponent
	// can still steal the round with a correct answer.
	for i, r := range d.Rounds {
		if r.ResolvedAt == nil {
			pick, _ := a.picker.GetAtomForRound(ctx, d, i+1)
			timerSec := a.roundTimerSec
			if timerSec <= 0 {
				timerSec = 30
			}
			frame := RoundStartFrame{
				RoundNo:   int32(i + 1),
				AtomID:    r.AtomID,
				Question:  pick.Question,
				Options:   pick.Options,
				TimerSec:  int32(timerSec),
				ServerNow: time.Now().UTC().Format("2006-01-02T15:04:05.000000Z"),
			}
			// WS3: include the server-side deadline so the FE can render a
			// countdown synced to the enforced timer (not just the
			// advertised timer_sec). Round 1 has no stamp yet — StartBattle
			// deliberately leaves it nil because players discover the match
			// via heartbeat polling (up to ~10s late). Stamp it lazily here,
			// on first serve, so the enforced timer starts when a player
			// actually sees the question.
			deadline := r.DeadlineAt
			if deadline == nil {
				stamped := time.Now().UTC().Add(time.Duration(timerSec) * time.Second)
				if err := a.repo.StampRoundDeadline(ctx, d.ID, i+1, stamped); err != nil {
					// FAIL-LOUD: log and serve without a deadline — the
					// stamp is retried on the next serve (reconnect /
					// round advance), so a transient DB error self-heals.
					log.Printf("ws: StampRoundDeadline failed duel=%s round=%d err=%v", d.ID, i+1, err)
				} else {
					deadline = &stamped
				}
			}
			if deadline != nil {
				// UTC() is load-bearing: the layout's Z is a LITERAL, and
				// pgx returns timestamptz in the machine's local zone —
				// formatting a local-zoned time directly mislabels local
				// wall time as UTC and throws the FE countdown off by the
				// tz offset.
				frame.DeadlineAt = deadline.UTC().Format("2006-01-02T15:04:05.000000Z")
			}
			return frame, true
		}
	}
	return RoundStartFrame{}, false
}

// GetBlitzStart builds the BlitzStartFrame for a blitz duel — all questions
// delivered at once so both players can answer in any order. Returns
// (zero, false) for classic duels or non-in-progress duels.
func (a *DuelSessionAdapter) GetBlitzStart(ctx context.Context, duelID string) (BlitzStartFrame, bool) {
	d, err := a.repo.GetDuel(ctx, duelID)
	if err != nil || d == nil || d.Status != duel.StatusInProgress {
		return BlitzStartFrame{}, false
	}
	if !d.IsBlitz() {
		return BlitzStartFrame{}, false
	}

	questions := make([]BlitzQuestion, 0, len(d.Rounds))
	for i, r := range d.Rounds {
		q := BlitzQuestion{
			RoundNo: int32(r.RoundNumber),
			AtomID:  r.AtomID,
		}
		// Use embedded content first (generated atoms have it inline);
		// fall back to the picker for projection-backed rounds.
		if r.Question != "" {
			q.Question = r.Question
			q.Options = r.Options
		} else if a.picker != nil {
			pick, _ := a.picker.GetAtomForRound(ctx, d, i+1)
			q.Question = pick.Question
			q.Options = pick.Options
		}
		questions = append(questions, q)
	}

	frame := BlitzStartFrame{
		Questions:    questions,
		Mode:         string(d.Mode),
		BlitzVariant: string(d.BlitzConfig.Variant),
		ServerNow:    time.Now().UTC().Format("2006-01-02T15:04:05.000000Z"),
	}
	if d.BlitzConfig.Variant == duel.BlitzVariantTimed {
		frame.TimeLimitSec = int32(d.BlitzConfig.TimeLimitSec)
	} else if d.BlitzConfig.Variant == duel.BlitzVariantRace {
		frame.RaceTarget = int32(d.BlitzConfig.RaceTarget)
	}
	if d.BlitzStartedAt != nil {
		frame.StartedAt = d.BlitzStartedAt.UTC().Format("2006-01-02T15:04:05.000000Z")
	}
	return frame, true
}

// SubmitBlitzAnswer processes a blitz answer. Unlike classic SubmitAnswer,
// a blitz answer does NOT resolve the round immediately — the opponent can
// still answer independently. The round resolves only when both players
// have answered. For BlitzVariantRace, the duel completes immediately when
// a player reaches the race target.
func (a *DuelSessionAdapter) SubmitBlitzAnswer(ctx context.Context, duelID, gcid string, roundNo int32, answer string, answerTimeMs int64) (BlitzAnswerResolvedFrame, bool, error) {
	d, err := a.repo.GetDuelForUpdate(ctx, duelID)
	if err != nil || d == nil {
		return BlitzAnswerResolvedFrame{}, false, fmt.Errorf("duel not found")
	}

	var correct bool
	if a.picker != nil {
		pick, err := a.picker.GetAtomForRound(ctx, d, int(roundNo))
		if err == nil && pick.Answer != "" {
			correct = strings.EqualFold(strings.TrimSpace(answer), strings.TrimSpace(pick.Answer))
		}
	}

	res, err := d.ResolveBlitzAnswer(gcid, int(roundNo), correct, answerTimeMs, a.comboTiers)
	if err != nil {
		return BlitzAnswerResolvedFrame{}, false, err
	}

	// Persist the full duel state (blitz mutates score/combo/round state
	// directly, unlike classic which uses ResolveRound for per-round
	// persistence).
	if err := a.repo.SaveDuel(ctx, d); err != nil {
		return BlitzAnswerResolvedFrame{}, false, err
	}

	completed := res.DuelStatus == duel.StatusCompleted
	frame := BlitzAnswerResolvedFrame{
		RoundNo:         roundNo,
		GCID:            gcid,
		Correct:         res.Correct,
		ComboMultiplier: int32(res.ComboMultiplier),
		PointsAwarded:   int32(res.PointsAwarded),
		CurrentCombo:    int32(res.CurrentCombo),
		SpeedBonus:      int32(res.SpeedBonus),
		RoundResolved:   res.RoundResolved,
		DuelStatus:      string(res.DuelStatus),
		ScoreChallenger: int32(d.ScoreChallenger),
		ScoreOpponent:   int32(d.ScoreOpponent),
		WinnerGCID:      res.WinnerGCID,
	}

	if completed {
		if a.eloApplier != nil && d.Scope == duel.ScopeRanked {
			kFactor := a.eloKFactor
			if kFactor <= 0 {
				kFactor = 32
			}
			if err := a.eloApplier.ApplyELO(ctx, d, kFactor); err != nil {
				log.Printf("ws: ApplyELO failed blitz duel=%s err=%v", d.ID, err)
			}
		}
		if a.eventPub != nil {
			_ = a.eventPub.PublishDuelCompleted(ctx,
				string(d.Scope), d.ID, d.TenantID,
				d.ChallengerGCID, d.OpponentGCID, d.WinnerGCID,
				d.ScoreChallenger, d.ScoreOpponent,
			)
		}
	}

	return frame, completed, nil
}

func toSnapshot(d *duel.Duel) *DuelSnapshot {
	rounds := make([]SnapshotRound, 0, len(d.Rounds))
	currentRound := int32(0)
	for i, r := range d.Rounds {
		sr := SnapshotRound{
			RoundNo:            int32(r.RoundNumber),
			AtomID:             r.AtomID,
			ChallengerAnswered: r.ChallengerAnswered,
			OpponentAnswered:   r.OpponentAnswered,
		}
		if r.ChallengerCorrect {
			sr.ChallengerCorrect = true
		}
		if r.OpponentCorrect {
			sr.OpponentCorrect = true
		}
		sr.PointsChallenger = int32(r.PointsChallenger)
		sr.PointsOpponent = int32(r.PointsOpponent)
		rounds = append(rounds, sr)
		// FCFS: current round is the first unresolved one. A round
		// where one player answered wrong but the other hasn't is still
		// current (ResolvedAt == nil).
		if r.ResolvedAt == nil && currentRound == 0 {
			currentRound = int32(i + 1)
		}
	}
	return &DuelSnapshot{
		DuelID:          d.ID,
		Status:          string(d.Status),
		ChallengerGCID:  d.ChallengerGCID,
		OpponentGCID:    d.OpponentGCID,
		ScoreChallenger: int32(d.ScoreChallenger),
		ScoreOpponent:   int32(d.ScoreOpponent),
		ComboChallenger: int32(d.ComboChallenger),
		ComboOpponent:   int32(d.ComboOpponent),
		WinnerGCID:      d.WinnerGCID,
		CurrentRound:    currentRound,
		TotalRounds:     int32(d.RoundCount),
		Rounds:          rounds,
	}
}
