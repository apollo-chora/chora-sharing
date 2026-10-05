// duel_event_adapter.go — bridges events.Publisher to the DuelEventPublisher
// ports consumed by the WS adapter + HTTP handlers.
//
// The events.Publisher.PublishDuelCompleted method takes a DuelEvent struct;
// the port interfaces in ws/ and http/ use individual params for a cleaner
// call site. This adapter bridges the two.
package duelevent

import (
	"context"
	"time"

	"github.com/apollo-chora/chora-sharing/internal/adapter/events"
)

// PublisherAdapter wraps an *events.Publisher to satisfy the
// DuelEventPublisher ports in ws/ + http/.
type PublisherAdapter struct {
	pub *events.Publisher
}

// New wraps pub as a DuelEventPublisher.
func New(pub *events.Publisher) *PublisherAdapter {
	return &PublisherAdapter{pub: pub}
}

// PublishDuelCompleted maps the flat params to a DuelEvent and delegates
// to the wrapped Publisher.
func (a *PublisherAdapter) PublishDuelCompleted(
	ctx context.Context,
	scope, duelID, tenantID, challengerGCID, opponentGCID, winnerGCID string,
	scoreChallenger, scoreOpponent int,
) error {
	return a.pub.PublishDuelCompleted(ctx, events.DuelEvent{
		DuelID:          duelID,
		TenantID:        tenantID,
		ChallengerGCID:  challengerGCID,
		OpponentGCID:    opponentGCID,
		WinnerGCID:      winnerGCID,
		Scope:           scope,
		ScoreChallenger: scoreChallenger,
		ScoreOpponent:   scoreOpponent,
		OccurredAt:      time.Now().UTC(),
	})
}

// PublishRoyaltySettled maps the flat params to a RoyaltySettledEvent and
// delegates to the wrapped Publisher. Satisfies the grpc.RoyaltyEventPublisher
// port consumed by SharingServer.AuthorizeAtomUse.
func (a *PublisherAdapter) PublishRoyaltySettled(
	ctx context.Context,
	settlementID, grantID, ownerGCID, granteeTenantID, atomID string,
	amount float64,
	currency, usageContext, sourceEventID, tenantID string,
) error {
	return a.pub.PublishRoyaltySettled(ctx, events.RoyaltySettledEvent{
		SettlementID:    settlementID,
		GrantID:         grantID,
		OwnerGCID:       ownerGCID,
		GranteeTenantID: granteeTenantID,
		AtomID:          atomID,
		Amount:          amount,
		Currency:        currency,
		UsageContext:    usageContext,
		SourceEventID:   sourceEventID,
		TenantID:        tenantID,
		OccurredAt:      time.Now().UTC(),
	})
}
