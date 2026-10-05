// friend_suggestions_test.go — RED specs for the B-lite.3 bounded FoF
// suggestions read (ADR-230 D3, CHO-2126).
//
// FriendSuggestions is a graph-shaped read, so it lives on GraphQueries (the
// D3 upgrade seam) — the service delegates verbatim, exactly like FriendSet.
package social_test

import (
	"context"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/social"
)

func TestRelationship_FriendSuggestions_DelegatesToReads(t *testing.T) {
	t.Parallel()
	reads := &fakeGraphQueries{suggestions: []social.FriendSuggestion{
		{GCID: gcidC, MutualFriends: 2},
		{GCID: gcidB, MutualFriends: 1},
	}}
	svc, err := social.NewRelationshipService(social.RelationshipServiceConfig{
		Store: &fakeRelStore{tx: &fakeRelTx{}},
		Reads: reads,
	})
	if err != nil {
		t.Fatalf("NewRelationshipService: %v", err)
	}

	got, err := svc.FriendSuggestions(context.Background(), tenantA, gcidA, 10)
	if err != nil {
		t.Fatalf("FriendSuggestions: %v", err)
	}
	if len(got) != 2 || got[0].GCID != gcidC || got[0].MutualFriends != 2 {
		t.Fatalf("suggestions = %+v; want the fake's ranked rows verbatim", got)
	}
	if reads.suggestionsCalls != 1 {
		t.Fatalf("delegation calls = %d; want 1", reads.suggestionsCalls)
	}
}
