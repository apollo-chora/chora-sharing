package matchmaking

import (
	"testing"
	"time"
)

func TestMatchScore_SharedTagsBoost(t *testing.T) {
	a := Searcher{GCID: "a", Proficiency: 1200, InterestTags: []string{"inheritance", "recursion"}}
	b := Searcher{GCID: "b", Proficiency: 1200, InterestTags: []string{"inheritance", "pointers"}}
	score := MatchScore(a, b)
	if score != 100 {
		t.Errorf("score=%d want 100 (1 shared tag * 100 - 0 delta)", score)
	}
}

func TestMatchScore_NoSharedTags(t *testing.T) {
	a := Searcher{GCID: "a", Proficiency: 1200, InterestTags: []string{"inheritance"}}
	b := Searcher{GCID: "b", Proficiency: 1200, InterestTags: []string{"recursion"}}
	score := MatchScore(a, b)
	if score != 0 {
		t.Errorf("score=%d want 0 (0 shared tags * 100 - 0 delta)", score)
	}
}

func TestMatchScore_ProficiencyDelta(t *testing.T) {
	a := Searcher{GCID: "a", Proficiency: 1200, InterestTags: []string{"inheritance"}}
	b := Searcher{GCID: "b", Proficiency: 1300, InterestTags: []string{"inheritance"}}
	score := MatchScore(a, b)
	if score != 0 {
		t.Errorf("score=%d want 0 (1 shared * 100 - 100 delta)", score)
	}
}

func TestMatchScore_NegativeScore(t *testing.T) {
	a := Searcher{GCID: "a", Proficiency: 1000, InterestTags: []string{"inheritance"}}
	b := Searcher{GCID: "b", Proficiency: 1500, InterestTags: []string{"inheritance"}}
	score := MatchScore(a, b)
	if score != -400 {
		t.Errorf("score=%d want -400 (1 shared * 100 - 500 delta)", score)
	}
}

func TestSharedTags(t *testing.T) {
	tests := []struct {
		name string
		a, b []string
		want []string
	}{
		{"both empty", nil, nil, nil},
		{"a empty", nil, []string{"x"}, nil},
		{"b empty", []string{"x"}, nil, nil},
		{"one shared", []string{"x", "y"}, []string{"y", "z"}, []string{"y"}},
		{"all shared", []string{"x", "y"}, []string{"x", "y"}, []string{"x", "y"}},
		{"none shared", []string{"x"}, []string{"y"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SharedTags(tt.a, tt.b)
			if len(got) != len(tt.want) {
				t.Fatalf("sharedTags(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
			wantSet := make(map[string]bool, len(tt.want))
			for _, w := range tt.want {
				wantSet[w] = true
			}
			for _, g := range got {
				if !wantSet[g] {
					t.Errorf("sharedTags(%v, %v): unexpected tag %s", tt.a, tt.b, g)
				}
			}
		})
	}
}

func TestSearcher_IsExpired(t *testing.T) {
	now := time.Now().UTC()
	s := Searcher{ExpiresAt: now.Add(-1 * time.Minute)}
	if !s.IsExpired(now) {
		t.Error("searcher past expires_at should be expired")
	}
	s2 := Searcher{ExpiresAt: now.Add(5 * time.Minute)}
	if s2.IsExpired(now) {
		t.Error("searcher with future expires_at should not be expired")
	}
}

func TestSearcher_IsHeartbeatStale(t *testing.T) {
	now := time.Now().UTC()
	s := Searcher{LastHeartbeat: now.Add(-31 * time.Second)}
	if !s.IsHeartbeatStale(now, 30*time.Second) {
		t.Error("searcher with heartbeat > 30s ago should be stale")
	}
	s2 := Searcher{LastHeartbeat: now.Add(-10 * time.Second)}
	if s2.IsHeartbeatStale(now, 30*time.Second) {
		t.Error("searcher with heartbeat 10s ago should not be stale")
	}
}

func TestMatchResult_SharedTagsSorted(t *testing.T) {
	mr := MatchResult{
		SearcherA:  Searcher{GCID: "a", InterestTags: []string{"recursion", "inheritance"}},
		SearcherB:  Searcher{GCID: "b", InterestTags: []string{"inheritance", "pointers"}},
		SharedTags: SharedTags([]string{"recursion", "inheritance"}, []string{"inheritance", "pointers"}),
	}
	if len(mr.SharedTags) != 1 || mr.SharedTags[0] != "inheritance" {
		t.Errorf("shared tags = %v, want [inheritance]", mr.SharedTags)
	}
}

func TestIsCompatible_ClassicVsBlitz(t *testing.T) {
	classic := Searcher{GCID: "a", Mode: "classic"}
	blitz := Searcher{GCID: "b", Mode: "blitz", BlitzVariant: "timed"}
	if IsCompatible(classic, blitz) {
		t.Error("classic vs blitz should be incompatible")
	}
}

func TestIsCompatible_BothClassic(t *testing.T) {
	a := Searcher{GCID: "a", Mode: "classic"}
	b := Searcher{GCID: "b", Mode: "classic"}
	if !IsCompatible(a, b) {
		t.Error("both classic should be compatible")
	}
}

func TestIsCompatible_EmptyModeDefaultsClassic(t *testing.T) {
	empty := Searcher{GCID: "a"}
	classic := Searcher{GCID: "b", Mode: "classic"}
	if !IsCompatible(empty, classic) {
		t.Error("empty mode (defaults classic) vs classic should be compatible")
	}
}

func TestIsCompatible_BlitzVariantMismatch(t *testing.T) {
	timed := Searcher{GCID: "a", Mode: "blitz", BlitzVariant: "timed"}
	race := Searcher{GCID: "b", Mode: "blitz", BlitzVariant: "race"}
	if IsCompatible(timed, race) {
		t.Error("timed-blitz vs race-blitz should be incompatible")
	}
}

func TestIsCompatible_BothTimedBlitz(t *testing.T) {
	a := Searcher{GCID: "a", Mode: "blitz", BlitzVariant: "timed"}
	b := Searcher{GCID: "b", Mode: "blitz", BlitzVariant: "timed"}
	if !IsCompatible(a, b) {
		t.Error("both timed-blitz should be compatible")
	}
}
