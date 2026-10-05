// Package leaderboard is the pure-domain core for the Leaderboard derived
// view of Content Sharing.
//
// Leaderboard is a DERIVED aggregate: production wiring computes ranking
// from AtomAttempt (chora_consumption) + Duel (chora_sharing) events
// streamed via Pub/Sub into a materialised view in chora_sharing.
//
// Hard rules (per .claude/rules/ddd-enforcement.md):
//   - This file holds the domain only — NO HTTP, NO persistence.
package leaderboard

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Period names the rolling-window scopes used by ranked boards.
type Period string

const (
	PeriodWeekly  Period = "weekly"
	PeriodMonthly Period = "monthly"
	PeriodAllTime Period = "all-time"
)

// ErrInvalidPeriod is returned by ParsePeriod for unknown values.
var ErrInvalidPeriod = errors.New("invalid leaderboard period")

// ParsePeriod parses the period string supplied on the URL query:
//
//	"weekly"   → PeriodWeekly
//	"monthly"  → PeriodMonthly
//	"all-time" → PeriodAllTime
//
// Anything else returns ErrInvalidPeriod.
func ParsePeriod(s string) (Period, error) {
	switch s {
	case "weekly":
		return PeriodWeekly, nil
	case "monthly":
		return PeriodMonthly, nil
	case "all-time":
		return PeriodAllTime, nil
	default:
		return "", fmt.Errorf("%w: %q", ErrInvalidPeriod, s)
	}
}

// Cap on entries returned by TopByPeriod (per the brief: top 100).
const maxTopN = 100

// ScopeKind names the three leaderboard scope families.
type ScopeKind string

const (
	ScopeGlobal ScopeKind = "global"
	ScopeTenant ScopeKind = "tenant"
	ScopeCohort ScopeKind = "cohort"
)

// Scope identifies a leaderboard's reach.
//
// For ScopeCohort, ID is the cohort UUID. For Tenant + Global the ID is
// empty (Global = whole platform; Tenant takes its tenant_id from the
// HTTP request header).
type Scope struct {
	Kind ScopeKind
	ID   string
}

// ErrInvalidScope is returned by ParseScope on malformed input.
var ErrInvalidScope = errors.New("invalid leaderboard scope")

// ParseScope parses the scope string supplied on the URL path:
//
//	"global"        → {Global, ""}
//	"tenant"        → {Tenant, ""}
//	"cohort:<id>"   → {Cohort, "<id>"}
//
// Anything else returns ErrInvalidScope.
func ParseScope(s string) (Scope, error) {
	switch {
	case s == "global":
		return Scope{Kind: ScopeGlobal}, nil
	case s == "tenant":
		return Scope{Kind: ScopeTenant}, nil
	case strings.HasPrefix(s, "cohort:"):
		id := strings.TrimPrefix(s, "cohort:")
		if id == "" {
			return Scope{}, fmt.Errorf("%w: cohort scope requires an id", ErrInvalidScope)
		}
		return Scope{Kind: ScopeCohort, ID: id}, nil
	default:
		return Scope{}, fmt.Errorf("%w: %q", ErrInvalidScope, s)
	}
}

// Entry is a single leaderboard row.
type Entry struct {
	GCID  string `json:"gcid"`
	Score int    `json:"score"`
	Rank  int    `json:"rank"`
}

// Ranker computes the top-N for a Scope.
//
// The skeleton supports two paths:
//   - TopN: deterministic stub rows (legacy). Retained so the existing
//     /api/leaderboards/{scope} path keeps working.
//   - TopByPeriod / RankOf / Submit / SubmitTenant: in-memory aggregator
//     keyed by (scope, period, [tenantID,] gcid). M12 replaces this with
//     a Postgres-backed materialised view fed by Pub/Sub events.
type Ranker struct {
	mu sync.RWMutex
	// xpByKey maps a composite scope-period-tenant-gcid key → accumulated XP.
	xpByKey map[string]int
}

// NewRanker returns a Ranker with an empty in-memory aggregator.
func NewRanker() *Ranker {
	return &Ranker{xpByKey: make(map[string]int)}
}

// scopePeriodTenantKey builds a deterministic composite key per board.
//
// Tenant boards segregate by tenantID; global / cohort boards ignore it
// (callers pass empty string). For ScopeCohort the cohort id is folded in
// via Scope.ID itself (already part of the prefix).
func boardKey(scope Scope, period Period, tenantID string) string {
	t := tenantID
	if scope.Kind != ScopeTenant {
		t = ""
	}
	return string(scope.Kind) + "|" + scope.ID + "|" + string(period) + "|" + t
}

// Submit credits xp to (scope, period, gcid) for non-tenant boards.
//
// Use SubmitTenant for tenant-scoped boards (where tenantID participates
// in the key).
func (r *Ranker) Submit(scope Scope, period Period, gcid string, xp int) {
	r.SubmitTenant(scope, period, "", gcid, xp)
}

// SubmitTenant credits xp to (scope, period, tenantID, gcid).
//
// For non-tenant scopes the tenantID parameter is ignored.
func (r *Ranker) SubmitTenant(scope Scope, period Period, tenantID, gcid string, xp int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := boardKey(scope, period, tenantID) + "|" + gcid
	r.xpByKey[key] += xp
}

// TopByPeriod returns up to limit entries for the (scope, period) board,
// optionally filtered by tenantID for tenant-scoped boards.
//
// Tie-breaker is deterministic: xp DESC, gcid ASC. Output ranks are
// monotonic 1-based.
func (r *Ranker) TopByPeriod(scope Scope, period Period, tenantID string, limit int) []Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if limit <= 0 || limit > maxTopN {
		limit = maxTopN
	}
	prefix := boardKey(scope, period, tenantID) + "|"
	entries := make([]Entry, 0)
	for k, xp := range r.xpByKey {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		gcid := strings.TrimPrefix(k, prefix)
		entries = append(entries, Entry{GCID: gcid, Score: xp})
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].Score != entries[j].Score {
			return entries[i].Score > entries[j].Score
		}
		return entries[i].GCID < entries[j].GCID
	})
	if limit > len(entries) {
		limit = len(entries)
	}
	out := make([]Entry, 0, limit)
	for i := 0; i < limit; i++ {
		e := entries[i]
		e.Rank = i + 1
		out = append(out, e)
	}
	return out
}

// RankOf returns the 1-based rank of gcid within the (scope, period[,
// tenantID]) board, or 0 if the gcid has not submitted any XP.
func (r *Ranker) RankOf(scope Scope, period Period, tenantID, gcid string) int {
	out := r.TopByPeriod(scope, period, tenantID, maxTopN)
	for _, e := range out {
		if e.GCID == gcid {
			return e.Rank
		}
	}
	return 0
}

// LeaderboardReader is the hexagonal port (§10.4) the gRPC GetLeaderboard
// handler consumes. The pg adapter implements it against the materialised
// view-backed query; the inmem *Ranker double implements it for tests +
// dev boot via the adapter wrapper below. The read is CQRS — never writes.
//
// Contract:
//   - ReadTop returns up to `limit` ranked entries for (scopeKind, scopeID,
//     metric) within the [start, end] season window. Ties share a rank;
//     tie-break on gcid ASC (deterministic across replays).
//   - scopeID is the tenant_id for ScopeTenant, the cohort UUID for
//     ScopeCohort, empty for ScopeGlobal.
type LeaderboardReader interface {
	ReadTop(ctx context.Context, scopeKind ScopeKind, scopeID, tenantID string, period Period, limit int) ([]Entry, error)
}

// RankerReader wraps *Ranker so it satisfies LeaderboardReader. The Ranker
// already computes rank-on-read; this adapter maps the port's typed scope +
// period onto the Ranker's TopByPeriod call. Production swaps this for the
// pg-backed reader without changing the call site.
type RankerReader struct {
	*Ranker
}

// NewRankerReader wraps a Ranker as a LeaderboardReader.
func NewRankerReader(r *Ranker) *RankerReader { return &RankerReader{Ranker: r} }

// ReadTop delegates to Ranker.TopByPeriod. The metric is XP in the skeleton
// (the only metric the in-memory aggregator tracks); other metrics return an
// empty slice — production wiring maps them to duel_ratings / currency reads.
func (rr *RankerReader) ReadTop(ctx context.Context, scopeKind ScopeKind, scopeID, tenantID string, period Period, limit int) ([]Entry, error) {
	scope := Scope{Kind: scopeKind, ID: scopeID}
	return rr.TopByPeriod(scope, period, tenantID, limit), nil
}

// Compile-time check: *RankerReader satisfies LeaderboardReader.
var _ LeaderboardReader = (*RankerReader)(nil)
