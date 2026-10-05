package ws

type DuelSnapshot struct {
	DuelID            string          `json:"duel_id"`
	Status            string          `json:"status"`
	ChallengerGCID    string          `json:"challenger_gcid"`
	OpponentGCID      string          `json:"opponent_gcid"`
	ScoreChallenger   int32           `json:"score_challenger"`
	ScoreOpponent     int32           `json:"score_opponent"`
	ComboChallenger   int32           `json:"combo_challenger"`
	ComboOpponent     int32           `json:"combo_opponent"`
	WinnerGCID        string          `json:"winner_gcid,omitempty"`
	CurrentRound      int32           `json:"current_round"`
	TotalRounds       int32           `json:"total_rounds"`
	Rounds            []SnapshotRound `json:"rounds"`
}

type SnapshotRound struct {
	RoundNo                   int32  `json:"round_no"`
	AtomID                    string `json:"atom_id"`
	Question                  string `json:"question,omitempty"`
	Options                   []string `json:"options,omitempty"`
	ChallengerAnswered        bool   `json:"challenger_answered"`
	OpponentAnswered          bool   `json:"opponent_answered"`
	ChallengerCorrect         bool   `json:"challenger_correct,omitempty"`
	OpponentCorrect           bool   `json:"opponent_correct,omitempty"`
	PointsChallenger          int32  `json:"points_challenger,omitempty"`
	PointsOpponent            int32  `json:"points_opponent,omitempty"`
}

type RoundStartFrame struct {
	RoundNo    int32     `json:"round_no"`
	AtomID     string    `json:"atom_id"`
	Question   string    `json:"question"`
	Options    []string  `json:"options"`
	TimerSec   int32     `json:"timer_sec"`
	DeadlineAt string    `json:"deadline_at,omitempty"`
	// ServerNow is the server's clock at frame-build time. The FE syncs
	// its countdown offset from it — skew correction must travel with the
	// question itself, not depend on which queue responses carried `now`.
	ServerNow  string    `json:"server_now,omitempty"`
}

type RoundResolvedFrame struct {
	RoundNo                   int32  `json:"round_no"`
	GCID                      string `json:"gcid"`
	Correct                   bool   `json:"correct"`
	ComboMultiplier           int32  `json:"combo_multiplier"`
	PointsAwarded             int32  `json:"points_awarded"`
	CurrentCombo              int32  `json:"current_combo"`
	SpeedBonus                int32  `json:"speed_bonus"`
	DuelStatus                string `json:"duel_status"`
	ScoreChallenger           int32  `json:"score_challenger"`
	ScoreOpponent             int32  `json:"score_opponent"`
	WinnerGCID                string `json:"winner_gcid,omitempty"`
}

type DuelCompletedFrame struct {
	WinnerGCID      string `json:"winner_gcid,omitempty"`
	ScoreChallenger int32  `json:"score_challenger"`
	ScoreOpponent   int32  `json:"score_opponent"`
}

// RoundTimeoutFrame is broadcast when a round auto-resolves via the
// server-side round timer (WS3). The FE uses it to render a skip
// animation instead of a normal round_resolved result.
type RoundTimeoutFrame struct {
	RoundNo         int32  `json:"round_no"`
	DuelStatus      string `json:"duel_status"`
	ScoreChallenger int32  `json:"score_challenger"`
	ScoreOpponent   int32  `json:"score_opponent"`
	WinnerGCID      string `json:"winner_gcid,omitempty"`
}

type InboundAnswerFrame struct {
	RoundNo      int32  `json:"round_no"`
	Answer       string `json:"answer"`
	AnswerTimeMs int64  `json:"answer_time_ms"`
}
type ErrorFrame struct {
	Message string `json:"message"`
}

// --- Blitz mode frames ---

// BlitzStartFrame is sent on connect for blitz duels. Unlike classic mode
// (which sends one RoundStartFrame at a time), blitz sends ALL questions
// at once so both players can answer them in any order as fast as they can.
type BlitzStartFrame struct {
	Questions    []BlitzQuestion `json:"questions"`
	Mode         string          `json:"mode"`
	BlitzVariant string          `json:"blitz_variant"`
	// TimeLimitSec is the time limit for BlitzVariantTimed (0 for race).
	TimeLimitSec int32           `json:"time_limit_sec,omitempty"`
	// RaceTarget is the target correct count for BlitzVariantRace (0 for timed).
	RaceTarget   int32           `json:"race_target,omitempty"`
	// StartedAt is the server-side blitz start clock (RFC3339). The FE
	// syncs its countdown from this for the timed variant.
	StartedAt    string          `json:"started_at,omitempty"`
	ServerNow    string          `json:"server_now,omitempty"`
}

// BlitzQuestion is a single question in a BlitzStartFrame.
type BlitzQuestion struct {
	RoundNo  int32    `json:"round_no"`
	AtomID   string   `json:"atom_id"`
	Question string   `json:"question"`
	Options  []string `json:"options"`
}

// BlitzAnswerResolvedFrame is broadcast after each blitz answer. Unlike
// classic RoundResolvedFrame, a blitz answer does NOT necessarily resolve
// the round — the opponent can still answer independently. RoundResolved
// is true only when this answer was the second of the pair.
type BlitzAnswerResolvedFrame struct {
	RoundNo         int32  `json:"round_no"`
	GCID            string `json:"gcid"`
	Correct         bool   `json:"correct"`
	ComboMultiplier int32  `json:"combo_multiplier"`
	PointsAwarded   int32  `json:"points_awarded"`
	CurrentCombo    int32  `json:"current_combo"`
	SpeedBonus      int32  `json:"speed_bonus"`
	RoundResolved   bool   `json:"round_resolved"`
	DuelStatus      string `json:"duel_status"`
	ScoreChallenger int32  `json:"score_challenger"`
	ScoreOpponent   int32  `json:"score_opponent"`
	WinnerGCID      string `json:"winner_gcid,omitempty"`
}
