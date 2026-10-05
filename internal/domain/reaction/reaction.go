// Package reaction is the pure-domain core for Reaction in Content Sharing.
//
// Aggregate root: Reaction (idempotent on (gcid, post_id, type)).
// Owning domain : Content Sharing (chora_sharing DB).
// Owning team   : Team 1 (Content).
//
// Per the brief, the Reaction registry is idempotent on the triple
// (gcid, post_id, type): a repeat React MUST return the existing record
// (NOT raise 409). Different types from the same gcid on the same post
// are independent records (a learner can react both "curious" AND "like").
//
// Hard rules (per .claude/rules/ddd-enforcement.md):
//
//   - This file holds the domain only — NO HTTP, NO persistence.
//   - UUIDv7 IDs.
//   - Cross-domain references (post_id, gcid) carry as opaque UUIDs.
package reaction

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Type is one of the canonical reaction types per the brief.
//
// We keep the brief's lowercase set (curious / insightful / like / inspired)
// even though the Pub/Sub Protobuf event schema in chora-contracts defines
// an UPPER-case ReactionType enum (LIKE / INSIGHTFUL / CURIOUS / CHEER /
// CELEBRATE). The HTTP layer normalises on the wire; M12 will add a mapper
// when wiring the event publisher.
type Type string

const (
	TypeCurious    Type = "curious"
	TypeInsightful Type = "insightful"
	TypeLike       Type = "like"
	TypeInspired   Type = "inspired"
)

// AllTypes lists the valid reaction types, in display order.
var AllTypes = []Type{TypeCurious, TypeInsightful, TypeLike, TypeInspired}

// ErrInvalidArgument signals a guard-clause failure.
var ErrInvalidArgument = errors.New("invalid argument")

// Reaction is the aggregate.
type Reaction struct {
	ID        string
	TenantID  string
	GCID      string
	PostID    string
	Type      Type
	CreatedAt time.Time
}

// NewReaction constructs a Reaction value.
//
// Used internally by Registry.React and exposed for tests that do not
// need idempotency tracking.
func NewReaction(tenantID, gcid, postID string, t Type) (*Reaction, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(gcid) == "" {
		return nil, fmt.Errorf("%w: gcid required", ErrInvalidArgument)
	}
	if strings.TrimSpace(postID) == "" {
		return nil, fmt.Errorf("%w: post_id required", ErrInvalidArgument)
	}
	if !isValidType(t) {
		return nil, fmt.Errorf("%w: unknown reaction type %q", ErrInvalidArgument, t)
	}
	return &Reaction{
		ID:        NewUUIDv7(),
		TenantID:  tenantID,
		GCID:      gcid,
		PostID:    postID,
		Type:      t,
		CreatedAt: time.Now().UTC(),
	}, nil
}

func isValidType(t Type) bool {
	for _, v := range AllTypes {
		if v == t {
			return true
		}
	}
	return false
}

// Registry tracks reactions in-memory with the (gcid|post|type) idempotency
// key per the brief.
//
// Production wiring (M12+) will swap in a Postgres adapter behind the same
// React + Unreact interface; the idempotency invariant moves to a UNIQUE
// constraint on (gcid, post_id, type).
type Registry struct {
	mu     sync.Mutex
	byKey  map[string]*Reaction // key = gcid|post|type — idempotency
	byPost map[string][]*Reaction
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		byKey:  make(map[string]*Reaction),
		byPost: make(map[string][]*Reaction),
	}
}

// idempotencyKey builds the (gcid|post|type) key.
func idempotencyKey(gcid, postID string, t Type) string {
	return gcid + "|" + postID + "|" + string(t)
}

// React inserts a new reaction, OR returns the existing one if (gcid, post,
// type) is already present (idempotent).
//
// Returns (reaction, created, err).
//   - created=true  → newly inserted record.
//   - created=false → existing record returned (idempotent repeat).
func (r *Registry) React(_ context.Context, tenantID, gcid, postID string, t Type) (*Reaction, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := idempotencyKey(gcid, postID, t)
	if existing, ok := r.byKey[key]; ok {
		return existing, false, nil
	}
	rx, err := NewReaction(tenantID, gcid, postID, t)
	if err != nil {
		return nil, false, err
	}
	r.byKey[key] = rx
	r.byPost[postID] = append(r.byPost[postID], rx)
	return rx, true, nil
}

// Unreact removes an existing reaction; returns true if removed, false
// if no matching record was present.
func (r *Registry) Unreact(_ context.Context, gcid, postID string, t Type) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := idempotencyKey(gcid, postID, t)
	rx, ok := r.byKey[key]
	if !ok {
		return false, nil
	}
	delete(r.byKey, key)

	// Also drop from the byPost slice. The cost is O(n) per post — fine
	// for the in-memory skeleton; M12 swap-in will hit a Postgres index.
	bucket := r.byPost[postID]
	for i, e := range bucket {
		if e.ID == rx.ID {
			r.byPost[postID] = append(bucket[:i], bucket[i+1:]...)
			break
		}
	}
	if len(r.byPost[postID]) == 0 {
		delete(r.byPost, postID)
	}
	return true, nil
}

// UnreactByID removes a reaction by its ID, but ONLY if the caller (gcid)
// owns that reaction. Returns (removed=true, err=nil) on success; returns
// (false, ErrInvalidArgument) when the reaction doesn't exist or the caller
// is not the owner (§10.6 — ownership-checked). This backs the
// DELETE /v1/posts/{post_id}/reactions/{reaction_id} REST route.
func (r *Registry) UnreactByID(_ context.Context, reactionID, gcid string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for key, rx := range r.byKey {
		if rx.ID == reactionID {
			if rx.GCID != gcid {
				return false, ErrInvalidArgument
			}
			delete(r.byKey, key)
			bucket := r.byPost[rx.PostID]
			for i, e := range bucket {
				if e.ID == reactionID {
					r.byPost[rx.PostID] = append(bucket[:i], bucket[i+1:]...)
					break
				}
			}
			if len(r.byPost[rx.PostID]) == 0 {
				delete(r.byPost, rx.PostID)
			}
			return true, nil
		}
	}
	return false, nil
}

// ListByPost returns all live reactions for a post (no soft-delete on
// Reaction — Unreact removes the row outright in the skeleton).
func (r *Registry) ListByPost(_ context.Context, postID string) ([]*Reaction, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*Reaction, 0, len(r.byPost[postID]))
	for _, e := range r.byPost[postID] {
		out = append(out, e)
	}
	return out, nil
}

// NewUUIDv7 — RFC 9562 §5.7 UUIDv7. See post.NewUUIDv7 for rationale.
func NewUUIDv7() string {
	const buflen = 16
	var b [buflen]byte
	now := uint64(time.Now().UnixMilli())
	b[0] = byte(now >> 40)
	b[1] = byte(now >> 32)
	b[2] = byte(now >> 24)
	b[3] = byte(now >> 16)
	b[4] = byte(now >> 8)
	b[5] = byte(now)
	if _, err := rand.Read(b[6:]); err != nil {
		for i := 6; i < buflen; i++ {
			b[i] = byte(now >> uint(8*(i-6)))
		}
	}
	b[6] = (b[6] & 0x0F) | 0x70
	b[8] = (b[8] & 0x3F) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// -----------------------------------------------------------------------------
// ReactionRepo — the hexagonal persistence port for the Reaction aggregate.
//
// This port did not exist until CHO-2193/W0-F1. Reaction had NO adapter at all:
// Registry (above) is a pair of maps living INSIDE this domain package, which is
// infrastructure in the domain layer — so there was no seam to substitute a
// durable adapter into, and reactions have therefore NEVER persisted. Not a
// regression like posts (whose pg adapter was deleted by 470ec0ef9): an original
// omission. Registry's own comment promised "Production wiring (M12+) will swap
// in a Postgres adapter behind the same React + Unreact interface; the
// idempotency invariant moves to a UNIQUE constraint on (gcid, post_id, type)."
// That UNIQUE constraint EXISTS in the live `reactions` table. The table was
// built for the adapter. The adapter was never written.
//
// Hard-delete on Unreact is DELIBERATE and matches the house convention for
// toggles: reactions / atom_bookmarks / atom_votes / social_follows all carry no
// deleted_at, while content aggregates (posts) do. bookmark_repo.go — the closest
// analogue — likewise issues a real DELETE. A reaction is a toggle, not a record
// with an audit lifetime.
type ReactionRepo interface {
	// React is idempotent on (gcid, post_id, type) — the live UNIQUE key.
	// created=false means the reaction already existed.
	React(ctx context.Context, tenantID, gcid, postID string, t Type) (*Reaction, bool, error)
	// Unreact removes the (gcid, post, type) reaction. ok=false means it was
	// already absent; a non-nil error means the delete FAILED and the caller
	// must NOT report it as absent.
	Unreact(ctx context.Context, gcid, postID string, t Type) (bool, error)
	// UnreactByID is the ownership-checked removal backing
	// DELETE /v1/posts/{post_id}/reactions/{reaction_id}.
	UnreactByID(ctx context.Context, reactionID, gcid string) (bool, error)
	// ListByPost returns the post's reactions.
	ListByPost(ctx context.Context, postID string) ([]*Reaction, error)
}
