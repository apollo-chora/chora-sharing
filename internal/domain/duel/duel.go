// Package duel is the pure-domain core for the Duel aggregate.
//
// Aggregate root: Duel — real-time PvP quiz battle between two GCIDs.
// Owning domain : Content Sharing (chora_sharing DB).
//
// State machine:
//
//	PENDING ──► ACCEPTED ──► IN_PROGRESS ──► COMPLETED
//	  │            │              │
//	  │            ▼              ▼
//	  └─────► EXPIRED        FORFEITED
//
// Gameplay: an AI-assisted atom picker selects a shared published atom
// (not created by either participant) based on both players' interest
// tags + proficiency. Each round presents the atom's question; both
// players answer simultaneously over WebSocket. Scoring uses a combo
// multiplier (consecutive correct escalates [1,2,3,5]; wrong resets to 0)
// plus a speed bonus for fast answers.
package duel

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidArgument   = errors.New("duel: invalid argument")
	ErrSelfChallenge     = errors.New("duel: challenger cannot challenge self")
	ErrNotInProgress     = errors.New("duel: not in progress")
	ErrNotAccepted       = errors.New("duel: not accepted")
	ErrNotPending        = errors.New("duel: not pending")
	ErrAlreadyResolved   = errors.New("duel: round already resolved")
	ErrIllegalTransition = errors.New("duel: illegal status transition")
	ErrNotParticipant    = errors.New("duel: gcid is not a participant")
	ErrRoundOutOfRange   = errors.New("duel: round number out of range")
	ErrNoAtomsAvailable  = errors.New("duel: no suitable shared atoms found")
)

type Status string

const (
	StatusUnspecified Status = ""
	StatusPending     Status = "pending"
	StatusAccepted    Status = "accepted"
	StatusInProgress  Status = "in_progress"
	StatusCompleted   Status = "completed"
	StatusForfeited   Status = "forfeited"
	StatusExpired     Status = "expired"
)

type Scope string

const (
	ScopeUnspecified Scope = ""
	ScopeFriendly    Scope = "friendly"
	ScopeRanked      Scope = "ranked"
)

// Mode names the duel gameplay model. Classic = sequential FCFS rounds
// (the original model). Blitz = all questions available at once; players
// answer as fast as they can — either most-correct within a time limit
// (BlitzVariantTimed) or first to N correct (BlitzVariantRace).
type Mode string

const (
	ModeUnspecified Mode = ""
	ModeClassic     Mode = "classic"
	ModeBlitz       Mode = "blitz"
)

// BlitzVariant names the blitz win condition.
type BlitzVariant string

const (
	BlitzVariantUnspecified BlitzVariant = ""
	// BlitzVariantTimed — all questions are open for a fixed time limit.
	// When the timer expires, the player with the higher score wins.
	BlitzVariantTimed BlitzVariant = "timed"
	// BlitzVariantRace — first player to reach RaceTarget correct answers
	// wins immediately. The time limit is unused.
	BlitzVariantRace BlitzVariant = "race"
)

// BlitzConfig holds the blitz-mode parameters. Exactly one variant is
// set per duel; the unused field is zero.
type BlitzConfig struct {
	Variant      BlitzVariant
	TimeLimitSec int // BlitzVariantTimed only
	RaceTarget   int // BlitzVariantRace only
}

type DuelConfig struct {
	ELOBaseline   int
	ELOKFactor    int
	ComboTiers    []int
	RoundTimerSec int
}

// Outcome names the result for a participant.
type Outcome string

const (
	OutcomeUnspecified Outcome = ""
	OutcomeWin         Outcome = "win"
	OutcomeLoss        Outcome = "loss"
	OutcomeDraw        Outcome = "draw"
	OutcomeForfeit     Outcome = "forfeit"
)

// RewardConfig holds the participation + performance reward amounts.
// Winner gets WinnerCoins; loser gets LoserCoins (participation reward).
// A draw gives DrawCoins to both. Forfeits give the forfeiting player
// nothing — the winner still gets WinnerCoins.
type RewardConfig struct {
	WinnerCoins int
	LoserCoins  int
	DrawCoins   int
}

// DefaultRewardConfig returns the shipped reward defaults.
func DefaultRewardConfig() RewardConfig {
	return RewardConfig{
		WinnerCoins: 50,
		LoserCoins:  15,
		DrawCoins:   25,
	}
}

// DuelResult is the post-completion view consumed by the event publisher
// + reward crediting. It captures the outcome for each participant.
type DuelResult struct {
	DuelID          string
	TenantID        string
	Scope           Scope
	ChallengerGCID  string
	OpponentGCID    string
	WinnerGCID      string
	Outcome         Outcome
	ScoreChallenger int
	ScoreOpponent   int
}

// Result returns a DuelResult snapshot from the current state.
// Must be called after the duel has reached a terminal state.
func (d *Duel) Result() DuelResult {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.resultLocked()
}

func (d *Duel) resultLocked() DuelResult {
	res := DuelResult{
		DuelID:          d.ID,
		TenantID:        d.TenantID,
		Scope:           d.Scope,
		ChallengerGCID:  d.ChallengerGCID,
		OpponentGCID:    d.OpponentGCID,
		WinnerGCID:      d.WinnerGCID,
		ScoreChallenger: d.ScoreChallenger,
		ScoreOpponent:   d.ScoreOpponent,
	}
	if d.Status == StatusForfeited {
		res.Outcome = OutcomeForfeit
	} else if d.WinnerGCID == "" {
		res.Outcome = OutcomeDraw
	} else {
		res.Outcome = OutcomeWin
	}
	return res
}

// RewardFor returns the coin reward for a participant given the result.
// Returns 0 if the participant forfeited.
func (r DuelResult) RewardFor(gcid string, cfg RewardConfig) int {
	if r.Outcome == OutcomeForfeit {
		if gcid == r.WinnerGCID {
			return cfg.WinnerCoins
		}
		return 0
	}
	if r.Outcome == OutcomeDraw {
		return cfg.DrawCoins
	}
	if gcid == r.WinnerGCID {
		return cfg.WinnerCoins
	}
	return cfg.LoserCoins
}

// OutcomeFor returns the outcome for a specific participant.
func (r DuelResult) OutcomeFor(gcid string) Outcome {
	if r.Outcome == OutcomeForfeit {
		if gcid == r.WinnerGCID {
			return OutcomeWin
		}
		return OutcomeForfeit
	}
	if r.Outcome == OutcomeDraw {
		return OutcomeDraw
	}
	if gcid == r.WinnerGCID {
		return OutcomeWin
	}
	return OutcomeLoss
}

type RoundSnapshot struct {
	RoundNumber               int
	AtomID                    string
	AtomRevisionID            string
	// Question, Options, CorrectAnswer carry the round's content inline so
	// generated atoms (no projection row) can be served without a
	// cross-domain lookup. For projection-backed rounds these mirror the
	// projection's Stem/Options/CorrectAnswer at StartBattle time — the
	// projection read-model stays the source of truth for projection
	// rounds, but embedding avoids a second fetch on every round delivery.
	Question                  string
	Options                   []string
	CorrectAnswer             string
	ChallengerAnswer          string
	OpponentAnswer            string
	ChallengerCorrect         bool
	OpponentCorrect           bool
	ChallengerTimeMs          int64
	OpponentTimeMs            int64
	ComboMultiplierChallenger int
	ComboMultiplierOpponent   int
	PointsChallenger          int
	PointsOpponent            int
	ChallengerAnswered        bool
	OpponentAnswered          bool
	WinnerGCID                string
	ResolvedAt                *time.Time
	// DeadlineAt is the server-side round expiry (now + RoundTimerSec at
	// StartBattle). WS3: a background sweep auto-resolves rounds whose
	// DeadlineAt has passed as both-unanswered (no points, no winner) so
	// the duel advances instead of hanging when a player disconnects.
	DeadlineAt *time.Time
}

type RoundResolution struct {
	Correct         bool
	ComboMultiplier int
	PointsAwarded   int
	CurrentCombo    int
	DuelStatus      Status
	WinnerGCID      string
	SpeedBonus      int
	DuelResult      *DuelResult
	// RoundTimeout is true when the round resolved via ResolveRoundTimeout
	// (the server-side timer fired) rather than a player answer. WS3: the
	// WS adapter uses this to emit a round_timeout frame instead of a
	// round_resolved frame so the FE renders a skip animation.
	RoundTimeout bool
}

type AtomPicker interface {
	PickAtom(ctx context.Context, challengerGCID, opponentGCID, tenantID string, tags []string) (AtomPick, error)
}

type AtomPick struct {
	AtomID     string
	RevisionID string
	Question   string
	Options    []string
	Answer     string
}
type DuelRepo interface {
	SaveDuel(ctx context.Context, d *Duel) error
	GetDuel(ctx context.Context, duelID string) (*Duel, error)
	GetDuelForUpdate(ctx context.Context, duelID string) (*Duel, error)
	ListDuels(ctx context.Context, tenantID, gcid, status string, limit int, cursor string) ([]*Duel, string, error)
	// ListDuelsWithExpiredRounds returns in-progress duels for the given
	// tenant with at least one unresolved round whose deadline has passed.
	// WS3 round-timer sweep. Per-tenant (RLS) — the caller must inject the
	// tenant via tracing.WithTenantID before calling.
	ListDuelsWithExpiredRounds(ctx context.Context, tenantID string, now time.Time) ([]*Duel, error)
	ResolveRound(ctx context.Context, d *Duel, roundNo int, gcid string, res RoundResolution) error
	// StampRoundDeadline persists the lazy first-serve deadline on an
	// unresolved round (WS3). Idempotent — an already-stamped round is
	// left untouched.
	StampRoundDeadline(ctx context.Context, duelID string, roundNo int, deadline time.Time) error
	ApplyELO(ctx context.Context, d *Duel, kFactor int) error
	GetRating(ctx context.Context, tenantID, gcid string) (int, error)
	GetRatingStats(ctx context.Context, tenantID, gcid string) (RatingStats, error)
	TopRatings(ctx context.Context, tenantID string, limit int) ([]RatingStats, error)
	FindNearbyRatings(ctx context.Context, tenantID, gcid string, limit int) ([]RatingStats, error)
	// WS1: per-category rating methods. Category "overall" is the default
	// bucket (backward-compatible with pre-WS1 ratings). These mirror the
	// non-category methods but filter by category.
	GetRatingForCategory(ctx context.Context, tenantID, gcid, category string) (int, error)
	GetRatingStatsForCategory(ctx context.Context, tenantID, gcid, category string) (RatingStats, error)
	TopRatingsForCategory(ctx context.Context, tenantID, category string, limit int) ([]RatingStats, error)
}

// RatingStats carries a player's duel rating + W/L/D record.
// Proficiency = duel ELO + course-completion bonus (design §1).
type RatingStats struct {
	GCID             string
	DisplayName      string
	Rating           int
	Wins             int
	Losses           int
	Draws            int
	PeakELO          int
	CompletedCourses int
	CourseBonus      int
	Proficiency      int
}

// ComputeProficiency fills CourseBonus + Proficiency from the completed
// course count. Per design §1: course_bonus = min(200, courses * 20).
func (rs *RatingStats) ComputeProficiency() {
	rs.CourseBonus = rs.CompletedCourses * 20
	if rs.CourseBonus > 200 {
		rs.CourseBonus = 200
	}
	rs.Proficiency = rs.Rating + rs.CourseBonus
}

type Duel struct {
	mu sync.Mutex

	ID              string
	TenantID        string
	ChallengerGCID  string
	OpponentGCID    string
	Status          Status
	Scope           Scope
	// Mode is the gameplay model (classic sequential vs blitz). Defaults
	// to ModeClassic for duels created via NewDuel/NewPoolDuel. Set to
	// ModeBlitz by NewBlitzPoolDuel.
	Mode            Mode
	// BlitzConfig holds the blitz parameters when Mode==ModeBlitz. Zero
	// value for classic duels.
	BlitzConfig     BlitzConfig
	// BlitzStartedAt is stamped at StartBattle for blitz duels — it is
	// the reference clock for the timed-variant expiry (CompleteBlitzTimed
	// compares now against BlitzStartedAt+TimeLimitSec). Nil for classic.
	BlitzStartedAt  *time.Time
	RoundCount      int
	Rounds          []RoundSnapshot
	ScoreChallenger int
	ScoreOpponent   int
	ComboChallenger int
	ComboOpponent   int
	WinnerGCID      string
	InterestTags    []string
	// Category is the per-category scope for this duel (WS1). Set from the
	// matched Searcher's Category at pool-duel creation. "overall" is the
	// default (un-categorized). ELO writes to the per-category rating bucket.
	Category        string
	CreatedAt       time.Time
	UpdatedAt       time.Time
	CompletedAt     *time.Time
	ExpiresAt       *time.Time
	// roundTimerSec is the per-question deadline duration, stamped at
	// construction from DuelConfig.RoundTimerSec. StartBattle uses it to
	// stamp each round's DeadlineAt (WS3 enforced timer).
	roundTimerSec int
}

func NewDuel(
	cfg DuelConfig,
	challengerGCID, opponentGCID, tenantID string,
	scope Scope,
	roundCount int,
	interestTags []string,
	inviteTTL time.Duration,
) (*Duel, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(challengerGCID) == "" {
		return nil, fmt.Errorf("%w: challenger_gcid required", ErrInvalidArgument)
	}
	if strings.TrimSpace(opponentGCID) == "" {
		return nil, fmt.Errorf("%w: opponent_gcid required", ErrInvalidArgument)
	}
	if challengerGCID == opponentGCID {
		return nil, ErrSelfChallenge
	}
	if scope == ScopeUnspecified {
		scope = ScopeFriendly
	}
	if roundCount <= 0 {
		roundCount = 5
	}
	if roundCount > 20 {
		roundCount = 20
	}
	if inviteTTL <= 0 {
		inviteTTL = 24 * time.Hour
	}

	now := time.Now().UTC()
	d := &Duel{
		ID:             NewUUIDv7(),
		TenantID:       tenantID,
		ChallengerGCID: challengerGCID,
		OpponentGCID:   opponentGCID,
		Status:         StatusPending,
		Scope:          scope,
		RoundCount:     roundCount,
		InterestTags:   append([]string(nil), interestTags...),
		CreatedAt:      now,
		UpdatedAt:      now,
		roundTimerSec:  cfg.RoundTimerSec,
	}
	exp := now.Add(inviteTTL)
	d.ExpiresAt = &exp
	return d, nil
}

// NewPoolDuel constructs a Duel for the pool-based matchmaking model.
// Per spec §4.8: pool-matched duels skip the pending→accepted chain —
// the pool matching IS the acceptance. The duel is created directly as
// StatusAccepted so StartBattle can transition it to in_progress.
// Scope defaults to ranked (pool matchmaking is inherently competitive).
// inviteTTL is unused (no invite phase) but ExpiresAt is still set for
// the duel-level expiry.
func NewPoolDuel(
	cfg DuelConfig,
	challengerGCID, opponentGCID, tenantID string,
	scope Scope,
	roundCount int,
	interestTags []string,
) (*Duel, error) {
	return newPoolDuel(cfg, challengerGCID, opponentGCID, tenantID, scope, roundCount, interestTags, NewUUIDv7())
}

// NewPoolDuelWithID constructs a pool Duel with a pre-generated ID. Used
// when the matchmaker has already claimed the queue rows with a specific
// duel ID and the Duel must be created with that same ID.
func NewPoolDuelWithID(
	cfg DuelConfig,
	challengerGCID, opponentGCID, tenantID string,
	scope Scope,
	roundCount int,
	interestTags []string,
	duelID string,
) (*Duel, error) {
	if strings.TrimSpace(duelID) == "" {
		return nil, fmt.Errorf("%w: duel_id required", ErrInvalidArgument)
	}
	return newPoolDuel(cfg, challengerGCID, opponentGCID, tenantID, scope, roundCount, interestTags, duelID)
}

// NewBlitzPoolDuel constructs a blitz-mode Duel for the pool-based
// matchmaking model. Like NewPoolDuel it skips the pending→accepted chain
// (pool matching IS the acceptance) and starts as StatusAccepted. The
// blitz config selects the win condition: BlitzVariantTimed (most correct
// within TimeLimitSec) or BlitzVariantRace (first to RaceTarget correct).
// Blitz duels are inherently competitive — scope defaults to ranked.
func NewBlitzPoolDuel(
	cfg DuelConfig,
	challengerGCID, opponentGCID, tenantID string,
	scope Scope,
	roundCount int,
	interestTags []string,
	blitzCfg BlitzConfig,
) (*Duel, error) {
	if blitzCfg.Variant == BlitzVariantUnspecified {
		return nil, fmt.Errorf("%w: blitz variant required", ErrInvalidArgument)
	}
	if blitzCfg.Variant == BlitzVariantTimed && blitzCfg.TimeLimitSec <= 0 {
		return nil, fmt.Errorf("%w: blitz timed variant requires time_limit_sec > 0", ErrInvalidArgument)
	}
	if blitzCfg.Variant == BlitzVariantRace && blitzCfg.RaceTarget <= 0 {
		return nil, fmt.Errorf("%w: blitz race variant requires race_target > 0", ErrInvalidArgument)
	}
	d, err := newPoolDuel(cfg, challengerGCID, opponentGCID, tenantID, scope, roundCount, interestTags, NewUUIDv7())
	if err != nil {
		return nil, err
	}
	d.Mode = ModeBlitz
	d.BlitzConfig = blitzCfg
	return d, nil
}

// NewBlitzPoolDuelWithID constructs a blitz pool Duel with a pre-generated
// ID. Used when the matchmaker has already claimed the queue rows with a
// specific duel ID and the Duel must be created with that same ID.
func NewBlitzPoolDuelWithID(
	cfg DuelConfig,
	challengerGCID, opponentGCID, tenantID string,
	scope Scope,
	roundCount int,
	interestTags []string,
	blitzCfg BlitzConfig,
	duelID string,
) (*Duel, error) {
	if strings.TrimSpace(duelID) == "" {
		return nil, fmt.Errorf("%w: duel_id required", ErrInvalidArgument)
	}
	if blitzCfg.Variant == BlitzVariantUnspecified {
		return nil, fmt.Errorf("%w: blitz variant required", ErrInvalidArgument)
	}
	if blitzCfg.Variant == BlitzVariantTimed && blitzCfg.TimeLimitSec <= 0 {
		return nil, fmt.Errorf("%w: blitz timed variant requires time_limit_sec > 0", ErrInvalidArgument)
	}
	if blitzCfg.Variant == BlitzVariantRace && blitzCfg.RaceTarget <= 0 {
		return nil, fmt.Errorf("%w: blitz race variant requires race_target > 0", ErrInvalidArgument)
	}
	d, err := newPoolDuel(cfg, challengerGCID, opponentGCID, tenantID, scope, roundCount, interestTags, duelID)
	if err != nil {
		return nil, err
	}
	d.Mode = ModeBlitz
	d.BlitzConfig = blitzCfg
	return d, nil
}

// IsBlitz reports whether this duel is in blitz mode.
func (d *Duel) IsBlitz() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.Mode == ModeBlitz
}

func newPoolDuel(
	cfg DuelConfig,
	challengerGCID, opponentGCID, tenantID string,
	scope Scope,
	roundCount int,
	interestTags []string,
	duelID string,
) (*Duel, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(challengerGCID) == "" {
		return nil, fmt.Errorf("%w: challenger_gcid required", ErrInvalidArgument)
	}
	if strings.TrimSpace(opponentGCID) == "" {
		return nil, fmt.Errorf("%w: opponent_gcid required", ErrInvalidArgument)
	}
	if challengerGCID == opponentGCID {
		return nil, ErrSelfChallenge
	}
	if scope == ScopeUnspecified {
		scope = ScopeRanked
	}
	if roundCount <= 0 {
		roundCount = 5
	}
	if roundCount > 20 {
		roundCount = 20
	}

	now := time.Now().UTC()
	d := &Duel{
		ID:             duelID,
		TenantID:       tenantID,
		ChallengerGCID: challengerGCID,
		OpponentGCID:   opponentGCID,
		Status:         StatusAccepted,
		Scope:          scope,
		RoundCount:     roundCount,
		InterestTags:   append([]string(nil), interestTags...),
		CreatedAt:      now,
		UpdatedAt:      now,
		roundTimerSec:  cfg.RoundTimerSec,
	}
	return d, nil
}

// WithRoundTimerSec re-applies the configured round timer on a duel that
// was rehydrated from persistence (roundTimerSec is not stored in the DB,
// so a fetched duel has 0 → the next-round deadline stamp would fall back
// to the 30s default instead of the configured value). Callers that fetch
// a duel from the repo and then resolve rounds MUST re-apply the config.
func (d *Duel) WithRoundTimerSec(sec int) *Duel {
	d.mu.Lock()
	defer d.mu.Unlock()
	if sec > 0 {
		d.roundTimerSec = sec
	}
	return d
}

func (d *Duel) Accept(gcid string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.Status != StatusPending {
		return fmt.Errorf("%w: current=%s", ErrNotPending, d.Status)
	}
	if gcid != d.OpponentGCID {
		return ErrNotParticipant
	}
	d.Status = StatusAccepted
	d.UpdatedAt = time.Now().UTC()
	return nil
}

func (d *Duel) StartBattle(atomPicks []AtomPick) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.Status != StatusAccepted {
		return fmt.Errorf("%w: current=%s", ErrNotAccepted, d.Status)
	}
	if len(atomPicks) == 0 {
		return ErrNoAtomsAvailable
	}
	d.Rounds = make([]RoundSnapshot, len(atomPicks))
	for i, ap := range atomPicks {
		// Shuffle the MCQ options so the correct answer lands at a random
		// position — otherwise the atom author's (or smith agent's) original
		// ordering leaks (e.g. if the agent always emits the correct answer
		// as the second option, every duel answer is "B"). The shuffle is
		// per-round-per-duel: both players see the same shuffled order for a
		// given round (the RoundSnapshot is shared), but different rounds +
		// different duels get different orders.
		opts, answer := shuffleOptions(ap.Options, ap.Answer)
		d.Rounds[i] = RoundSnapshot{
			RoundNumber:    i + 1,
			AtomID:         ap.AtomID,
			AtomRevisionID: ap.RevisionID,
			Question:       ap.Question,
			Options:        opts,
			CorrectAnswer:  answer,
		}
	}
	// WS3: round 1's deadline is NOT stamped here. Players discover the
	// match via heartbeat polling (up to ~10s late), so stamping at match
	// time would burn their timer before they ever see the question. The
	// WS adapter stamps round 1 lazily on first serve (GetCurrentRound).
	// Subsequent rounds get their deadline stamped when the previous round
	// resolves (ResolveRound / ResolveRoundTimeout) — stamping all rounds
	// here would make every round share one absolute deadline, so a 5×30s
	// duel would mass-expire rounds 2–5 at t=30s.
	d.RoundCount = len(atomPicks)
	d.Status = StatusInProgress
	d.UpdatedAt = time.Now().UTC()
	// Blitz: stamp the shared start clock so CompleteBlitzTimed can
	// compare now against BlitzStartedAt+TimeLimitSec. Classic duels
	// leave BlitzStartedAt nil (they use per-round deadlines instead).
	if d.Mode == ModeBlitz {
		now := time.Now().UTC()
		d.BlitzStartedAt = &now
	}
	return nil
}

// stampCurrentRoundDeadline stamps DeadlineAt = resolvedAt + roundTimerSec on
// the first unresolved round (the current round). Called on round resolution
// (the next round); round 1 is stamped lazily by the WS adapter on first
// serve instead. No-op when no unresolved round remains or the round is
// already stamped. Caller MUST hold d.mu.
//
// resolvedAt is the instant the PREVIOUS round resolved, passed in rather than
// read here, and both call sites already hold it. It used to read time.Now()
// itself, which was wrong in two ways. On the sweep path it silently discarded
// the `now` the caller supplies precisely so a timeout sweep is deterministic
// and testable without sleeping, stamping a wall-clock deadline unrelated to
// the instant being swept. And on both paths it made one logical event read the
// clock twice, so ResolvedAt and the deadline derived from it could straddle a
// tick and disagree, which left the relationship between them unassertable.
func (d *Duel) stampCurrentRoundDeadline(resolvedAt time.Time, timerSec int) {
	if timerSec <= 0 {
		timerSec = 30
	}
	for i := range d.Rounds {
		if d.Rounds[i].ResolvedAt == nil {
			if d.Rounds[i].DeadlineAt != nil {
				return // already stamped (e.g. re-entry)
			}
			deadline := resolvedAt.UTC().Add(time.Duration(timerSec) * time.Second)
			d.Rounds[i].DeadlineAt = &deadline
			return
		}
	}
}

func (d *Duel) ResolveRound(gcid string, roundNo int, correct bool, answerTimeMs int64, tiers []int) (RoundResolution, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.Status != StatusInProgress {
		return RoundResolution{}, fmt.Errorf("%w: current=%s", ErrNotInProgress, d.Status)
	}
	if roundNo < 1 || roundNo > len(d.Rounds) {
		return RoundResolution{}, fmt.Errorf("%w: round=%d, max=%d", ErrRoundOutOfRange, roundNo, len(d.Rounds))
	}

	isChallenger := gcid == d.ChallengerGCID
	isOpponent := gcid == d.OpponentGCID
	if !isChallenger && !isOpponent {
		return RoundResolution{}, ErrNotParticipant
	}

	round := &d.Rounds[roundNo-1]

	// First-come-first-served: a correct answer resolves the round
	// immediately (the answerer wins the round). A wrong answer marks
	// only that player as answered but keeps the round open for the
	// opponent — UNLESS the opponent has also already answered wrong,
	// in which case the round resolves with no winner.
	//
	// Re-submission by a player who already answered is rejected — but
	// a player who answered wrong is NOT locked out (FCFS only locks on
	// a correct answer, which already resolved the round).
	if round.ResolvedAt != nil {
		return RoundResolution{}, fmt.Errorf("%w: round %d already resolved", ErrAlreadyResolved, roundNo)
	}
	if isChallenger && round.ChallengerAnswered {
		return RoundResolution{}, fmt.Errorf("%w: challenger already answered round %d", ErrAlreadyResolved, roundNo)
	}
	if isOpponent && round.OpponentAnswered {
		return RoundResolution{}, fmt.Errorf("%w: opponent already answered round %d", ErrAlreadyResolved, roundNo)
	}

	if isChallenger {
		round.ChallengerCorrect = correct
		round.ChallengerAnswered = true
		round.ChallengerAnswer = ""
		round.ChallengerTimeMs = answerTimeMs
	} else {
		round.OpponentCorrect = correct
		round.OpponentAnswered = true
		round.OpponentAnswer = ""
		round.OpponentTimeMs = answerTimeMs
	}

	// Combo + points: only the answerer's score is affected this call.
	var combo, points *int
	if isChallenger {
		combo = &d.ComboChallenger
		points = &d.ScoreChallenger
	} else {
		combo = &d.ComboOpponent
		points = &d.ScoreOpponent
	}

	if correct {
		*combo++
	} else {
		*combo = 0
	}

	mult := ComboMultiplierForStreak(*combo, tiers)
	base := 10
	speedBonus := 0
	if correct && answerTimeMs > 0 {
		timerMs := int64(30000)
		if answerTimeMs < timerMs/2 {
			speedBonus = 5
		}
	}
	roundPoints := base*mult + speedBonus
	*points += roundPoints

	if isChallenger {
		round.ComboMultiplierChallenger = mult
		round.PointsChallenger = roundPoints
	} else {
		round.ComboMultiplierOpponent = mult
		round.PointsOpponent = roundPoints
	}

	// FCFS resolution: a correct answer resolves the round immediately
	// (winner = answerer). If the answer is wrong, the round stays open
	// for the opponent — unless the opponent already answered wrong, in
	// which case both-wrong resolves the round with no winner.
	roundResolved := false
	if correct {
		roundResolved = true
		round.WinnerGCID = gcid
		// Mark the opponent as answered too — the round is closed; the
		// opponent does not get to answer a resolved round. This keeps
		// allAnswered consistent for duel-completion logic below.
		if isChallenger {
			round.OpponentAnswered = true
		} else {
			round.ChallengerAnswered = true
		}
	} else {
		// Wrong answer: if the opponent also already answered (wrong),
		// the round resolves with no winner.
		if isChallenger && round.OpponentAnswered {
			roundResolved = true
		} else if isOpponent && round.ChallengerAnswered {
			roundResolved = true
		}
	}

	res := RoundResolution{
		Correct:         correct,
		ComboMultiplier: mult,
		PointsAwarded:   roundPoints,
		CurrentCombo:    *combo,
		DuelStatus:      d.Status,
		SpeedBonus:      speedBonus,
	}

	if roundResolved {
		now := time.Now().UTC()
		round.ResolvedAt = &now
		d.UpdatedAt = now

		// WS3: stamp the next round's deadline now that it has become
		// current. Without this, every round would share round 1's
		// deadline (the StartBattle stamp) and the sweeper would
		// mass-skip rounds 2–N mid-duel.
		d.stampCurrentRoundDeadline(now, d.roundTimerSec)

		// Duel completes when every round is resolved.
		allResolved := true
		for i := range d.Rounds {
			if d.Rounds[i].ResolvedAt == nil {
				allResolved = false
				break
			}
		}
		if allResolved {
			d.completeDuel()
			res.DuelStatus = d.Status
			res.WinnerGCID = d.WinnerGCID
			dr := d.resultLocked()
			res.DuelResult = &dr
		}
	}

	return res, nil
}

// ResolveRoundTimeout auto-resolves a round whose DeadlineAt has passed as
// both-unanswered (no points, no winner) so the duel advances. WS3: called
// by the server-side round-timer sweep. Idempotent against late answers —
// a round already resolved (by a correct answer or a prior timeout) returns
// ErrAlreadyResolved. The caller passes `now` so the sweep is deterministic
// + testable without sleeping. Fail-loud: calling before the deadline is
// an error (the sweep must not prematurely skip a live round).
func (d *Duel) ResolveRoundTimeout(roundNo int, now time.Time) (RoundResolution, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.Status != StatusInProgress {
		return RoundResolution{}, fmt.Errorf("%w: current=%s", ErrNotInProgress, d.Status)
	}
	if roundNo < 1 || roundNo > len(d.Rounds) {
		return RoundResolution{}, fmt.Errorf("%w: round=%d, max=%d", ErrRoundOutOfRange, roundNo, len(d.Rounds))
	}

	round := &d.Rounds[roundNo-1]
	if round.ResolvedAt != nil {
		return RoundResolution{}, fmt.Errorf("%w: round %d already resolved", ErrAlreadyResolved, roundNo)
	}
	if round.DeadlineAt == nil {
		return RoundResolution{}, fmt.Errorf("%w: round %d has no deadline", ErrInvalidArgument, roundNo)
	}
	if now.Before(*round.DeadlineAt) {
		return RoundResolution{}, fmt.Errorf("%w: round %d not yet expired (deadline %s, now %s)", ErrInvalidArgument, roundNo, round.DeadlineAt.Format(time.RFC3339), now.Format(time.RFC3339))
	}

	// Both-unanswered: no points, no winner, no combo change.
	nowUTC := now.UTC()
	round.ResolvedAt = &nowUTC
	round.ChallengerAnswered = false
	round.OpponentAnswered = false
	round.PointsChallenger = 0
	round.PointsOpponent = 0
	d.UpdatedAt = nowUTC

	// WS3: stamp the next round's deadline now that it has become current,
	// from the instant the SWEEP names rather than the wall clock.
	d.stampCurrentRoundDeadline(nowUTC, d.roundTimerSec)

	res := RoundResolution{
		ComboMultiplier: 0,
		PointsAwarded:   0,
		DuelStatus:      d.Status,
		RoundTimeout:    true,
	}

	// Duel completes when every round is resolved.
	allResolved := true
	for i := range d.Rounds {
		if d.Rounds[i].ResolvedAt == nil {
			allResolved = false
			break
		}
	}
	if allResolved {
		d.completeDuel()
		res.DuelStatus = d.Status
		res.WinnerGCID = d.WinnerGCID
		dr := d.resultLocked()
		res.DuelResult = &dr
	}

	return res, nil
}

func (d *Duel) completeDuel() {
	if d.ScoreChallenger > d.ScoreOpponent {
		d.WinnerGCID = d.ChallengerGCID
	} else if d.ScoreOpponent > d.ScoreChallenger {
		d.WinnerGCID = d.OpponentGCID
	}
	d.Status = StatusCompleted
	now := time.Now().UTC()
	d.CompletedAt = &now
	d.UpdatedAt = now
}

// BlitzRoundResolution is the result of a single blitz answer. Unlike
// classic ResolveRound, a blitz answer does NOT necessarily resolve the
// round — the round stays open until BOTH players have answered (or the
// blitz timer expires). RoundResolved is true only when this answer was
// the second of the pair.
type BlitzRoundResolution struct {
	Correct         bool
	ComboMultiplier int
	PointsAwarded   int
	CurrentCombo    int
	SpeedBonus      int
	// RoundResolved is true when this answer was the second of the pair
	// (both players answered) and the round is now closed.
	RoundResolved   bool
	DuelStatus      Status
	WinnerGCID      string
	DuelResult      *DuelResult
}

// ResolveBlitzAnswer records a player's answer to a blitz round. Unlike
// classic FCFS, a correct answer does NOT close the round — the opponent
// can still answer independently. A round resolves only when BOTH players
// have answered. Points are awarded per-answer (combo + speed bonus, same
// as classic). For BlitzVariantRace, the duel completes immediately when
// a player reaches RaceTarget correct answers.
func (d *Duel) ResolveBlitzAnswer(gcid string, roundNo int, correct bool, answerTimeMs int64, tiers []int) (BlitzRoundResolution, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.Mode != ModeBlitz {
		return BlitzRoundResolution{}, fmt.Errorf("%w: not a blitz duel", ErrInvalidArgument)
	}
	if d.Status != StatusInProgress {
		return BlitzRoundResolution{}, fmt.Errorf("%w: current=%s", ErrNotInProgress, d.Status)
	}
	if roundNo < 1 || roundNo > len(d.Rounds) {
		return BlitzRoundResolution{}, fmt.Errorf("%w: round=%d, max=%d", ErrRoundOutOfRange, roundNo, len(d.Rounds))
	}

	isChallenger := gcid == d.ChallengerGCID
	isOpponent := gcid == d.OpponentGCID
	if !isChallenger && !isOpponent {
		return BlitzRoundResolution{}, ErrNotParticipant
	}

	round := &d.Rounds[roundNo-1]
	if round.ResolvedAt != nil {
		return BlitzRoundResolution{}, fmt.Errorf("%w: round %d already resolved", ErrAlreadyResolved, roundNo)
	}
	if isChallenger && round.ChallengerAnswered {
		return BlitzRoundResolution{}, fmt.Errorf("%w: challenger already answered round %d", ErrAlreadyResolved, roundNo)
	}
	if isOpponent && round.OpponentAnswered {
		return BlitzRoundResolution{}, fmt.Errorf("%w: opponent already answered round %d", ErrAlreadyResolved, roundNo)
	}

	if isChallenger {
		round.ChallengerCorrect = correct
		round.ChallengerAnswered = true
		round.ChallengerTimeMs = answerTimeMs
	} else {
		round.OpponentCorrect = correct
		round.OpponentAnswered = true
		round.OpponentTimeMs = answerTimeMs
	}

	var combo, points *int
	if isChallenger {
		combo = &d.ComboChallenger
		points = &d.ScoreChallenger
	} else {
		combo = &d.ComboOpponent
		points = &d.ScoreOpponent
	}

	if correct {
		*combo++
	} else {
		*combo = 0
	}

	mult := ComboMultiplierForStreak(*combo, tiers)
	base := 10
	speedBonus := 0
	if correct && answerTimeMs > 0 {
		timerMs := int64(30000)
		if answerTimeMs < timerMs/2 {
			speedBonus = 5
		}
	}
	// Blitz: only correct answers earn points. A wrong answer resets the
	// combo and awards 0 — this incentivizes accuracy over spamming
	// (unlike classic FCFS where a wrong answer still earns base points).
	roundPoints := 0
	if correct {
		roundPoints = base*mult + speedBonus
	}
	*points += roundPoints

	if isChallenger {
		round.ComboMultiplierChallenger = mult
		round.PointsChallenger = roundPoints
	} else {
		round.ComboMultiplierOpponent = mult
		round.PointsOpponent = roundPoints
	}

	res := BlitzRoundResolution{
		Correct:         correct,
		ComboMultiplier: mult,
		PointsAwarded:   roundPoints,
		CurrentCombo:    *combo,
		SpeedBonus:      speedBonus,
		DuelStatus:      d.Status,
	}

	// Race variant: check if this player reached the target. Count
	// correct answers across all rounds for this player.
	if d.BlitzConfig.Variant == BlitzVariantRace && correct {
		correctCount := 0
		for i := range d.Rounds {
			if isChallenger && d.Rounds[i].ChallengerCorrect {
				correctCount++
			} else if isOpponent && d.Rounds[i].OpponentCorrect {
				correctCount++
			}
		}
		if correctCount >= d.BlitzConfig.RaceTarget {
			// Mark all remaining unresolved rounds as resolved (the
			// duel is over — unanswered rounds are closed).
			now := time.Now().UTC()
			for i := range d.Rounds {
				if d.Rounds[i].ResolvedAt == nil {
					d.Rounds[i].ResolvedAt = &now
				}
			}
			d.WinnerGCID = gcid
			d.completeDuel()
			res.DuelStatus = d.Status
			res.WinnerGCID = d.WinnerGCID
			dr := d.resultLocked()
			res.DuelResult = &dr
			return res, nil
		}
	}

	// Round resolves when both players have answered.
	if round.ChallengerAnswered && round.OpponentAnswered {
		now := time.Now().UTC()
		round.ResolvedAt = &now
		d.UpdatedAt = now
		res.RoundResolved = true

		// Timed variant: check if all rounds are resolved (natural
		// completion before the timer). This is rare but possible.
		if d.BlitzConfig.Variant == BlitzVariantTimed {
			allResolved := true
			for i := range d.Rounds {
				if d.Rounds[i].ResolvedAt == nil {
					allResolved = false
					break
				}
			}
			if allResolved {
				d.completeDuel()
				res.DuelStatus = d.Status
				res.WinnerGCID = d.WinnerGCID
				dr := d.resultLocked()
				res.DuelResult = &dr
			}
		}
	}

	return res, nil
}

// CompleteBlitzTimed ends a timed-blitz duel when the time limit expires.
// All unresolved rounds are closed (no points for unanswered). The winner
// is the player with the higher score. Fail-loud: calling before the time
// limit has elapsed is an error (the sweep must not prematurely end a
// live blitz). Only valid for BlitzVariantTimed.
func (d *Duel) CompleteBlitzTimed(now time.Time) (BlitzRoundResolution, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.Mode != ModeBlitz {
		return BlitzRoundResolution{}, fmt.Errorf("%w: not a blitz duel", ErrInvalidArgument)
	}
	if d.BlitzConfig.Variant != BlitzVariantTimed {
		return BlitzRoundResolution{}, fmt.Errorf("%w: complete timed only valid for timed variant", ErrInvalidArgument)
	}
	if d.Status != StatusInProgress {
		return BlitzRoundResolution{}, fmt.Errorf("%w: current=%s", ErrNotInProgress, d.Status)
	}
	if d.BlitzStartedAt == nil {
		return BlitzRoundResolution{}, fmt.Errorf("%w: blitz not started", ErrInvalidArgument)
	}
	deadline := d.BlitzStartedAt.Add(time.Duration(d.BlitzConfig.TimeLimitSec) * time.Second)
	if now.Before(deadline) {
		return BlitzRoundResolution{}, fmt.Errorf("%w: blitz not yet expired (deadline %s, now %s)", ErrInvalidArgument, deadline.Format(time.RFC3339), now.Format(time.RFC3339))
	}

	// Close all unresolved rounds (no points for unanswered).
	nowUTC := now.UTC()
	for i := range d.Rounds {
		if d.Rounds[i].ResolvedAt == nil {
			d.Rounds[i].ResolvedAt = &nowUTC
		}
	}
	d.UpdatedAt = nowUTC
	d.completeDuel()

	res := BlitzRoundResolution{
		DuelStatus: d.Status,
		WinnerGCID: d.WinnerGCID,
	}
	dr := d.resultLocked()
	res.DuelResult = &dr
	return res, nil
}

func (d *Duel) Forfeit(gcid string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.Status != StatusInProgress && d.Status != StatusAccepted {
		return fmt.Errorf("%w: current=%s", ErrNotInProgress, d.Status)
	}
	isChallenger := gcid == d.ChallengerGCID
	isOpponent := gcid == d.OpponentGCID
	if !isChallenger && !isOpponent {
		return ErrNotParticipant
	}
	if isChallenger {
		d.WinnerGCID = d.OpponentGCID
	} else {
		d.WinnerGCID = d.ChallengerGCID
	}
	d.Status = StatusForfeited
	now := time.Now().UTC()
	d.CompletedAt = &now
	d.UpdatedAt = now
	return nil
}

func ComputeELO(winnerRating, loserRating, kFactor int) (winnerNew, loserNew int) {
	exp := 1.0 / (1.0 + ExpApprox(float64(loserRating-winnerRating)/400.0))
	delta := float64(kFactor) * (1.0 - exp)
	winnerNew = winnerRating + int(delta+0.5)
	loserNew = loserRating - int(delta+0.5)
	totalBefore := winnerRating + loserRating
	totalAfter := winnerNew + loserNew
	if totalAfter != totalBefore {
		winnerNew += totalBefore - totalAfter
	}
	return winnerNew, loserNew
}

// ExpApprox computes e^x via Taylor series, clamped for numerical safety.
// Exported so the pg adapter can reuse the same approximation for draw ELO.
func ExpApprox(x float64) float64 {
	if x > 20 {
		x = 20
	}
	if x < -20 {
		return 0
	}
	result := 1.0
	term := 1.0
	for i := 1; i <= 20; i++ {
		term *= x / float64(i)
		result += term
	}
	return result
}

func ComboMultiplierForStreak(streak int, tiers []int) int {
	if streak < 0 {
		streak = 0
	}
	if len(tiers) == 0 {
		return 1
	}
	if streak >= len(tiers) {
		return tiers[len(tiers)-1]
	}
	return tiers[streak]
}

func NewUUIDv7() string {
	const buflen = 16
	var b [buflen]byte
	_, _ = rand.Read(b[:])
	now := uint64(time.Now().UnixMilli())
	b[0] = byte(now >> 40)
	b[1] = byte(now >> 32)
	b[2] = byte(now >> 24)
	b[3] = byte(now >> 16)
	b[4] = byte(now >> 8)
	b[5] = byte(now)
	b[6] = (b[6] & 0x0F) | 0x70
	b[8] = (b[8] & 0x3F) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// shuffleOptions returns a shuffled copy of options with the correct
// answer tracked. The answer string (already matched case-insensitively
// against an option by matchOption) is located in the shuffled slice +
// returned verbatim so the stored CorrectAnswer is exactly one of the
// shuffled Options. If the answer is empty or not found (shouldn't
// happen — matchOption guarantees it), the options are still shuffled
// but the answer is returned unchanged (the correctness check will
// fail-safe to "incorrect").
//
// Uses math/rand/v2 (Go 1.22+) which auto-seeds from the OS — no global
// Seed call needed. Each call produces a different order, so different
// rounds + different duels get different option orders.
func shuffleOptions(options []string, answer string) ([]string, string) {
	if len(options) <= 1 {
		return append([]string(nil), options...), answer
	}
	shuffled := append([]string(nil), options...)
	mrand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	// The answer is the verbatim option string (matchOption already
	// resolved it). Find it in the shuffled slice + return it so the
	// stored CorrectAnswer matches the new position.
	answerLower := strings.TrimSpace(strings.ToLower(answer))
	for _, opt := range shuffled {
		if strings.TrimSpace(strings.ToLower(opt)) == answerLower {
			return shuffled, opt
		}
	}
	// Fallback: answer not found in options (defensive — matchOption
	// should have guaranteed a match). Return the shuffled options +
	// the original answer string; the correctness check will compare
	// case-insensitively + still match.
	return shuffled, answer
}
