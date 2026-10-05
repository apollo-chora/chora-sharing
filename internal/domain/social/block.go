// block.go — Block edge + the FriendReader port (ADR-229 WS-0, CHO-2102).
//
// A Block is a directional (blocker → blocked) edge that overrides the
// mutual-follow friendship relation: a block in EITHER direction removes the
// pair from both parties' friend sets (ADR-229 D3). Blocks were previously an
// in-memory-only list on Graph (lost on pod restart); they persist to
// chora_sharing.social_blocks from WS-0 on.
//
// Hard rules (per .claude/rules/ddd-enforcement.md):
//   - This file holds the domain only — NO HTTP, NO persistence.
//   - UUIDv7 IDs.
//   - GCIDs travel as opaque cross-domain UUIDs.
package social

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrSelfBlock is returned when blocker_gcid == blocked_gcid.
var ErrSelfBlock = errors.New("cannot block self")

// Block is a single blocker → blocked edge.
type Block struct {
	ID          string
	TenantID    string
	BlockerGCID string
	BlockedGCID string
	CreatedAt   time.Time
}

// NewBlock constructs a Block with guard-clauses.
func NewBlock(tenantID, blocker, blocked string) (*Block, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(blocker) == "" {
		return nil, fmt.Errorf("%w: blocker_gcid required", ErrInvalidArgument)
	}
	if strings.TrimSpace(blocked) == "" {
		return nil, fmt.Errorf("%w: blocked_gcid required", ErrInvalidArgument)
	}
	if blocker == blocked {
		return nil, ErrSelfBlock
	}
	return &Block{
		ID:          NewUUIDv7(),
		TenantID:    tenantID,
		BlockerGCID: blocker,
		BlockedGCID: blocked,
		CreatedAt:   time.Now().UTC(),
	}, nil
}

// NOTE: the WS-0 FriendReader port (MutualFollows) is retired — ADR-230 D2
// replaced derived mutual-follows with explicit friendships behind the
// GraphQueries port (relationship.go); GetReuseContext's RPC contract is
// unchanged.
