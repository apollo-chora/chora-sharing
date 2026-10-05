// share_repo_b_test.go — coverage tests for ShareRepo (second coverage
// agent): SaveShare feed-entry + outbox publish, get/list scans + cursor,
// AppendEvent idempotency/conflict matrix, display-name + events reads.
package pg_test

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/domain/atom_share"
)

const shareEntryID = "01970000-0000-7000-a000-0000000000b1"

// stubOutboxB records the row handed to WriteOutboxRow; err injects failure.
type stubOutboxB struct {
	called bool
	row    pg.OutboxRow
	err    error
}

func (s *stubOutboxB) WriteOutboxRow(_ context.Context, _ pg.Querier, row pg.OutboxRow) error {
	s.called = true
	s.row = row
	return s.err
}

func testShare() *atom_share.Share {
	return &atom_share.Share{
		FeedEntryID:       shareEntryID,
		TenantID:          tenantID,
		AtomID:            "atom-1",
		RevisionID:        "rev-1",
		OwnerGCID:         authorGCID,
		AuthorDisplayName: "Walfa",
		StemPreview:       "stem",
		QuestionType:      "multi",
		Options:           []string{"a", "b"},
		Caption:           "cap",
		License:           atom_share.LicenseRoyaltyPct,
		Rate:              atom_share.RoyaltyRate{Kind: "pct", Value: 5},
		CreatedAt:         time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
	}
}

func TestShareRepo_NilTxRunner_ReturnsErrNotImplemented(t *testing.T) {
	t.Parallel()
	r := pg.NewShareRepo(nil)
	if err := r.SaveShare(context.Background(), testShare()); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("SaveShare: expected ErrNotImplemented; got %v", err)
	}
	if _, err := r.GetShare(context.Background(), shareEntryID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("GetShare: expected ErrNotImplemented; got %v", err)
	}
	if _, err := r.GetShareByAtom(context.Background(), "atom-1", authorGCID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("GetShareByAtom: expected ErrNotImplemented; got %v", err)
	}
	if _, _, err := r.ListSharedAtoms(context.Background(), tenantID, "", 10, "", "", "tenant", nil, nil); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("ListSharedAtoms: expected ErrNotImplemented; got %v", err)
	}
	if err := r.AppendEvent(context.Background(), &atom_share.ShareEvent{}); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("AppendEvent: expected ErrNotImplemented; got %v", err)
	}
	if _, err := r.ResolveDisplayName(context.Background(), authorGCID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("ResolveDisplayName: expected ErrNotImplemented; got %v", err)
	}
	if _, err := r.ListEvents(context.Background(), shareEntryID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("ListEvents: expected ErrNotImplemented; got %v", err)
	}
}

func TestShareRepo_SaveShare_RejectsNil(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewShareRepo(&stubTxRunner{q: q})
	if err := r.SaveShare(tracing.WithTenantID(context.Background(), tenantID), nil); !errors.Is(err, atom_share.ErrInvalidArgument) {
		t.Fatalf("expected atom_share.ErrInvalidArgument; got %v", err)
	}
}

func TestShareRepo_SaveShare_InsertsFeedEntry(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewShareRepo(&stubTxRunner{q: q})
	s := testShare()
	s.CreatedAt = time.Time{} // default-now branch
	if err := r.SaveShare(tracing.WithTenantID(context.Background(), tenantID), s); err != nil {
		t.Fatalf("SaveShare: %v", err)
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL") {
		t.Fatalf("first SQL must be SET LOCAL; got %q", q.sqls[0])
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "INSERT INTO social_feed_entries") || !strings.Contains(last, "'share'") {
		t.Fatalf("expected share feed entry insert; got %q", last)
	}
	args := q.args[len(q.args)-1]
	if len(args) != 5 {
		t.Fatalf("insert binds 5 args; got %v", args)
	}
	// $4 = JSON content carrying the attribution snapshot.
	content, ok := args[3].([]byte)
	if !ok || !strings.Contains(string(content), `"atom_id":"atom-1"`) || !strings.Contains(string(content), "royalty_pct") {
		t.Fatalf("content JSON wrong: %v", args[3])
	}
	// $5 = created defaulted to now.
	if created, ok := args[4].(time.Time); !ok || created.IsZero() {
		t.Fatalf("created must default to now; got %v", args[4])
	}
}

func TestShareRepo_SaveShare_WithOutboxBus(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	outbox := &stubOutboxB{}
	r := pg.NewShareRepo(&stubTxRunner{q: q}).WithOutboxBus(outbox)
	if err := r.SaveShare(tracing.WithTenantID(context.Background(), tenantID), testShare()); err != nil {
		t.Fatalf("SaveShare: %v", err)
	}
	if !outbox.called {
		t.Fatalf("WriteOutboxRow must be invoked when bus wired")
	}
	if outbox.row.Topic != "chora.sharing.atom.shared.v1" || outbox.row.AggregateType != "atom" ||
		outbox.row.IdempotencyKey != shareEntryID {
		t.Fatalf("outbox row wrong: %+v", outbox.row)
	}
	if !strings.Contains(string(outbox.row.Payload), `"atom_id":"atom-1"`) {
		t.Fatalf("outbox payload wrong: %s", outbox.row.Payload)
	}
	if outbox.row.Envelope["tenant_id"] != tenantID {
		t.Fatalf("envelope missing tenant: %v", outbox.row.Envelope)
	}
}

func TestShareRepo_SaveShare_ErrorPaths(t *testing.T) {
	t.Parallel()

	t.Run("insert exec error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerierB{execErrFn: func(sql string) error {
			if strings.Contains(sql, "SET LOCAL") {
				return nil
			}
			return errors.New("boom")
		}}
		r := pg.NewShareRepo(&stubTxRunnerB{q: q})
		err := r.SaveShare(tracing.WithTenantID(context.Background(), tenantID), testShare())
		if err == nil || !strings.Contains(err.Error(), "insert share feed entry") {
			t.Fatalf("expected wrapped insert error; got %v", err)
		}
	})

	t.Run("outbox write error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{}
		outbox := &stubOutboxB{err: errors.New("outbox boom")}
		r := pg.NewShareRepo(&stubTxRunner{q: q}).WithOutboxBus(outbox)
		err := r.SaveShare(tracing.WithTenantID(context.Background(), tenantID), testShare())
		if err == nil || !strings.Contains(err.Error(), "write share outbox row") {
			t.Fatalf("expected wrapped outbox error; got %v", err)
		}
	})
}

// shareRowB scans the 4-column (id, tenant_id, content, created_at) share SELECTs.
func shareRowB(dest ...any) error {
	content := []byte(`{"atom_id":"atom-1","atom_revision_id":"rev-1","owner_gcid":"` + authorGCID + `","author_display_name":"Walfa","stem_preview":"stem","question_type":"multi","options":["a","b"],"caption":"cap","license_terms":"royalty_pct","royalty_rate":{"kind":"pct","value":5}}`)
	bSetInto(dest, 0, shareEntryID)
	bSetInto(dest, 1, tenantID)
	bSetInto(dest, 2, content)
	bSetInto(dest, 3, time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC))
	return nil
}

func TestShareRepo_GetShare_HappyAndNotFound(t *testing.T) {
	t.Parallel()

	t.Run("happy", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			if !strings.Contains(sql, "NOT EXISTS") {
				t.Fatalf("revoked exclusion required; got %q", sql)
			}
			return stubRow{scanFn: shareRowB}
		}}
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		s, err := r.GetShare(tracing.WithTenantID(context.Background(), tenantID), shareEntryID)
		if err != nil {
			t.Fatalf("GetShare: %v", err)
		}
		if s == nil || s.AtomID != "atom-1" || s.License != atom_share.LicenseRoyaltyPct || s.Rate.Value != 5 {
			t.Fatalf("scan wrong: %+v", s)
		}
	})

	t.Run("no row → ErrNotFound", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error { return errors.New("no rows") }}
		}}
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		if _, err := r.GetShare(tracing.WithTenantID(context.Background(), tenantID), shareEntryID); !errors.Is(err, atom_share.ErrNotFound) {
			t.Fatalf("expected ErrNotFound; got %v", err)
		}
	})

	t.Run("bad content JSON → unmarshal error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error {
				bSetInto(dest, 0, shareEntryID)
				bSetInto(dest, 2, []byte("not json"))
				return nil
			}}
		}}
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		_, err := r.GetShare(tracing.WithTenantID(context.Background(), tenantID), shareEntryID)
		if err == nil || !strings.Contains(err.Error(), "unmarshal share content") {
			t.Fatalf("expected unmarshal error; got %v", err)
		}
	})
}

func TestShareRepo_GetShareByAtom_Happy(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
		if !strings.Contains(sql, "(e.content->>'atom_id') = $1") {
			t.Fatalf("expected atom-scoped lookup; got %q", sql)
		}
		return stubRow{scanFn: shareRowB}
	}}
	r := pg.NewShareRepo(&stubTxRunner{q: q})
	s, err := r.GetShareByAtom(tracing.WithTenantID(context.Background(), tenantID), "atom-1", authorGCID)
	if err != nil {
		t.Fatalf("GetShareByAtom: %v", err)
	}
	if s == nil || s.AtomID != "atom-1" {
		t.Fatalf("scan wrong: %+v", s)
	}
}

// bEncodeCursor packs (ts,id) like feed_cursor.go so ListSharedAtoms' decode
// produces non-nil cursor params.
func bEncodeCursor(ts time.Time, id string) string {
	raw := ts.UTC().Format(time.RFC3339Nano) + "|" + id
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func TestShareRepo_ListSharedAtoms_Happy(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{
		queryFn: func(sql string, args ...any) (pg.Rows, error) {
			if !strings.Contains(sql, "cardinality") || !strings.Contains(sql, "created_at DESC") {
				t.Fatalf("expected keyset list query; got %q", sql)
			}
			// args: tenantID, filters, limit, scope, followingGCIDs, cursorTS, cursorID, blockedGCIDs
			if len(args) != 8 {
				t.Fatalf("expected 8 args; got %d: %v", len(args), args)
			}
			if scope, _ := args[3].(string); scope != "following" {
				t.Fatalf("scope arg wrong; got %v", args[3])
			}
			following, _ := args[4].([]string)
			if len(following) == 0 || following[0] != otherGCID {
				t.Fatalf("following arg wrong; got %v", args[4])
			}
			return &stubRows{scans: []func(dest ...any) error{shareRowB}}, nil
		},
	}
	r := pg.NewShareRepo(&stubTxRunner{q: q})
	ctx := tracing.WithTenantID(context.Background(), tenantID)
	// limit 10, cursor, filters dedupe, following + blocked lists.
	out, next, err := r.ListSharedAtoms(ctx, tenantID,
		bEncodeCursor(time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC), shareEntryID),
		10, "multi", "multi", "following", []string{otherGCID}, []string{authorGCID})
	if err != nil {
		t.Fatalf("ListSharedAtoms: %v", err)
	}
	if len(out) != 1 || out[0].AtomID != "atom-1" {
		t.Fatalf("scan wrong: %+v", out)
	}
	if next != "" {
		t.Fatalf("1 row < limit → no cursor; got %q", next)
	}
	// filters deduped → single-element array.
	filters, _ := q.args[len(q.args)-1][1].([]string)
	if len(filters) != 1 {
		t.Fatalf("identical filters must dedupe; got %v", filters)
	}
}

func TestShareRepo_ListSharedAtoms_DefaultsAndFullPage(t *testing.T) {
	t.Parallel()

	t.Run("defaults (limit 0, scope empty, empty cursor)", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			queryFn: func(sql string, args ...any) (pg.Rows, error) {
				if lim, _ := args[2].(int); lim != 20 {
					t.Fatalf("limit 0 must default to 20; got %v", args[2])
				}
				if scope, _ := args[3].(string); scope != "tenant" {
					t.Fatalf("empty scope must default to tenant; got %v", args[3])
				}
				if args[5] != nil || args[6] != nil {
					t.Fatalf("empty cursor must bind nil ts/id; got %v, %v", args[5], args[6])
				}
				return &stubRows{}, nil
			},
		}
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		out, next, err := r.ListSharedAtoms(tracing.WithTenantID(context.Background(), tenantID), tenantID, "", 0, "", "", "", nil, nil)
		if err != nil {
			t.Fatalf("ListSharedAtoms: %v", err)
		}
		if out == nil || len(out) != 0 || next != "" {
			t.Fatalf("expected empty slice; got %#v, %q", out, next)
		}
	})

	t.Run("full page cursor", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			queryFn: func(sql string, args ...any) (pg.Rows, error) {
				if lim, _ := args[2].(int); lim != 2 {
					t.Fatalf("expected limit 2; got %v", args[2])
				}
				return &stubRows{scans: []func(dest ...any) error{
					shareRowB, shareRowB,
				}}, nil
			},
		}
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		out, next, err := r.ListSharedAtoms(tracing.WithTenantID(context.Background(), tenantID), tenantID, "", 2, "", "", "", nil, nil)
		if err != nil {
			t.Fatalf("ListSharedAtoms: %v", err)
		}
		if len(out) != 2 || next == "" {
			t.Fatalf("full page must produce cursor; got %d rows, %q", len(out), next)
		}
	})

	t.Run("limit over 100 clamped", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{}
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		if _, _, err := r.ListSharedAtoms(tracing.WithTenantID(context.Background(), tenantID), tenantID, "", 500, "", "", "", nil, nil); err != nil {
			t.Fatalf("ListSharedAtoms: %v", err)
		}
		if lim, _ := q.args[len(q.args)-1][2].(int); lim != 100 {
			t.Fatalf("limit 500 must clamp to 100; got %v", q.args[len(q.args)-1][2])
		}
	})

	t.Run("distinct filters kept", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{}
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		if _, _, err := r.ListSharedAtoms(tracing.WithTenantID(context.Background(), tenantID), tenantID, "", 10, "multi", "choice", "tenant", nil, nil); err != nil {
			t.Fatalf("ListSharedAtoms: %v", err)
		}
		filters, _ := q.args[len(q.args)-1][1].([]string)
		if len(filters) != 2 {
			t.Fatalf("distinct filters must both be kept; got %v", filters)
		}
	})
}

func TestShareRepo_ListSharedAtoms_Errors(t *testing.T) {
	t.Parallel()

	t.Run("query error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("q") }}
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		if _, _, err := r.ListSharedAtoms(tracing.WithTenantID(context.Background(), tenantID), tenantID, "", 10, "", "", "", nil, nil); err == nil {
			t.Fatalf("query error must propagate")
		}
	})

	t.Run("scan error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{func(dest ...any) error { return errors.New("s") }}}, nil
		}}
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		if _, _, err := r.ListSharedAtoms(tracing.WithTenantID(context.Background(), tenantID), tenantID, "", 10, "", "", "", nil, nil); err == nil {
			t.Fatalf("scan error must propagate")
		}
	})

	t.Run("rows.Err", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{}, err: errors.New("r")}, nil
		}}
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		if _, _, err := r.ListSharedAtoms(tracing.WithTenantID(context.Background(), tenantID), tenantID, "", 10, "", "", "", nil, nil); err == nil {
			t.Fatalf("rows.Err must propagate")
		}
	})
}

func TestShareRepo_AppendEvent_HappyAndIdempotent(t *testing.T) {
	t.Parallel()

	baseEvent := func() *atom_share.ShareEvent {
		return &atom_share.ShareEvent{
			FeedEntryID:   shareEntryID,
			ActorGCID:     authorGCID,
			Type:          atom_share.EventRevoked,
			Payload:       map[string]any{"reason": "upstream"},
			SourceEventID: "evt-1",
			CreatedAt:     time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
		}
	}

	t.Run("inserted (1 row affected)", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			rowFn: func(sql string, args ...any) pg.Row {
				return stubRow{scanFn: func(dest ...any) error {
					bSetInto(dest, 0, tenantID)
					return nil
				}}
			},
		}
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		if err := r.AppendEvent(tracing.WithTenantID(context.Background(), tenantID), baseEvent()); err != nil {
			t.Fatalf("AppendEvent: %v", err)
		}
		if len(q.sqls) != 3 {
			t.Fatalf("expected SET LOCAL + tenant resolve + event insert; got %d", len(q.sqls))
		}
		if !strings.Contains(q.sqls[1], "SELECT tenant_id FROM social_feed_entries") {
			t.Fatalf("expected tenant resolve; got %q", q.sqls[1])
		}
		if !strings.Contains(q.sqls[2], "INSERT INTO atom_share_events") {
			t.Fatalf("expected event insert; got %q", q.sqls[2])
		}
	})

	t.Run("conflict with empty source_event_id is a no-op", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			execTagFn: func(sql string) rls.CommandTag {
				if strings.Contains(sql, "INSERT INTO atom_share_events") {
					return rls.CommandTag{RowsAffected: 0}
				}
				return rls.CommandTag{RowsAffected: 1}
			},
			rowFn: func(sql string, args ...any) pg.Row {
				return stubRow{scanFn: func(dest ...any) error {
					bSetInto(dest, 0, tenantID)
					return nil
				}}
			},
		}
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		e := baseEvent()
		e.SourceEventID = ""
		if err := r.AppendEvent(tracing.WithTenantID(context.Background(), tenantID), e); err != nil {
			t.Fatalf("AppendEvent: %v", err)
		}
	})

	t.Run("idempotent replay (same payload) is a no-op", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			execTagFn: func(sql string) rls.CommandTag {
				if strings.Contains(sql, "INSERT INTO atom_share_events") {
					return rls.CommandTag{RowsAffected: 0}
				}
				return rls.CommandTag{RowsAffected: 1}
			},
			rowFn: func(sql string, args ...any) pg.Row {
				return stubRow{scanFn: func(dest ...any) error {
					switch {
					case strings.Contains(sql, "SELECT tenant_id"):
						bSetInto(dest, 0, tenantID)
					default: // sqlShareEventExisting
						bSetInto(dest, 0, authorGCID)
						bSetInto(dest, 1, "revoked")
						bSetInto(dest, 2, []byte(`{"reason":"upstream"}`))
					}
					return nil
				}}
			},
		}
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		if err := r.AppendEvent(tracing.WithTenantID(context.Background(), tenantID), baseEvent()); err != nil {
			t.Fatalf("idempotent replay must be nil; got %v", err)
		}
	})
}

func TestShareRepo_AppendEvent_Conflict(t *testing.T) {
	t.Parallel()

	conflictQ := func(exActor, exType, exPayload string) *stubQuerier {
		return &stubQuerier{
			execTagFn: func(sql string) rls.CommandTag {
				if strings.Contains(sql, "INSERT INTO atom_share_events") {
					return rls.CommandTag{RowsAffected: 0}
				}
				return rls.CommandTag{RowsAffected: 1}
			},
			rowFn: func(sql string, args ...any) pg.Row {
				return stubRow{scanFn: func(dest ...any) error {
					switch {
					case strings.Contains(sql, "SELECT tenant_id"):
						bSetInto(dest, 0, tenantID)
					default:
						bSetInto(dest, 0, exActor)
						bSetInto(dest, 1, exType)
						bSetInto(dest, 2, []byte(exPayload))
					}
					return nil
				}}
			},
		}
	}

	t.Run("actor mismatch → ErrConflict", func(t *testing.T) {
		t.Parallel()
		q := conflictQ(otherGCID, "revoked", `{"reason":"upstream"}`)
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		err := r.AppendEvent(tracing.WithTenantID(context.Background(), tenantID), &atom_share.ShareEvent{
			FeedEntryID: shareEntryID, ActorGCID: authorGCID, Type: atom_share.EventRevoked,
			Payload: map[string]any{"reason": "upstream"}, SourceEventID: "evt-1",
		})
		if !errors.Is(err, atom_share.ErrConflict) {
			t.Fatalf("expected ErrConflict; got %v", err)
		}
	})

	t.Run("payload mismatch → ErrConflict", func(t *testing.T) {
		t.Parallel()
		q := conflictQ(authorGCID, "revoked", `{"reason":"OTHER"}`)
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		err := r.AppendEvent(tracing.WithTenantID(context.Background(), tenantID), &atom_share.ShareEvent{
			FeedEntryID: shareEntryID, ActorGCID: authorGCID, Type: atom_share.EventRevoked,
			Payload: map[string]any{"reason": "upstream"}, SourceEventID: "evt-1",
		})
		if !errors.Is(err, atom_share.ErrConflict) {
			t.Fatalf("expected ErrConflict; got %v", err)
		}
	})

	t.Run("existing row vanished → treated as no-op", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			execTagFn: func(sql string) rls.CommandTag {
				if strings.Contains(sql, "INSERT INTO atom_share_events") {
					return rls.CommandTag{RowsAffected: 0}
				}
				return rls.CommandTag{RowsAffected: 1}
			},
			rowFn: func(sql string, args ...any) pg.Row {
				return stubRow{scanFn: func(dest ...any) error {
					if strings.Contains(sql, "SELECT tenant_id") {
						bSetInto(dest, 0, tenantID)
						return nil
					}
					return errors.New("no rows")
				}}
			},
		}
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		if err := r.AppendEvent(tracing.WithTenantID(context.Background(), tenantID), &atom_share.ShareEvent{
			FeedEntryID: shareEntryID, ActorGCID: authorGCID, Type: atom_share.EventRevoked,
			Payload: map[string]any{}, SourceEventID: "evt-1",
		}); err != nil {
			t.Fatalf("vanished row must be a no-op; got %v", err)
		}
	})
}

func TestShareRepo_AppendEvent_ErrorPaths(t *testing.T) {
	t.Parallel()

	t.Run("tenant resolve error wraps ErrNotFound", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error { return errors.New("no rows") }}
		}}
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		err := r.AppendEvent(tracing.WithTenantID(context.Background(), tenantID), &atom_share.ShareEvent{
			FeedEntryID: shareEntryID, ActorGCID: authorGCID, Type: atom_share.EventRevoked,
			Payload: map[string]any{}, SourceEventID: "evt-1",
		})
		if !errors.Is(err, atom_share.ErrNotFound) {
			t.Fatalf("expected wrapped ErrNotFound; got %v", err)
		}
	})

	t.Run("event insert exec error", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerierB{
			rowFn: func(sql string, args ...any) pg.Row {
				return stubRow{scanFn: func(dest ...any) error {
					bSetInto(dest, 0, tenantID)
					return nil
				}}
			},
			execErrFn: func(sql string) error {
				if strings.Contains(sql, "SET LOCAL") {
					return nil
				}
				return errors.New("boom")
			},
		}
		r := pg.NewShareRepo(&stubTxRunnerB{q: q})
		err := r.AppendEvent(tracing.WithTenantID(context.Background(), tenantID), &atom_share.ShareEvent{
			FeedEntryID: shareEntryID, ActorGCID: authorGCID, Type: atom_share.EventRevoked,
			Payload: map[string]any{}, SourceEventID: "evt-1",
		})
		if err == nil || !strings.Contains(err.Error(), "insert share event") {
			t.Fatalf("expected wrapped insert error; got %v", err)
		}
	})

	t.Run("unserializable payload degraded to {}", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			rowFn: func(sql string, args ...any) pg.Row {
				return stubRow{scanFn: func(dest ...any) error {
					bSetInto(dest, 0, tenantID)
					return nil
				}}
			},
		}
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		e := &atom_share.ShareEvent{
			FeedEntryID: shareEntryID, ActorGCID: authorGCID, Type: atom_share.EventRevoked,
			Payload: map[string]any{"fn": func() {}}, SourceEventID: "evt-1",
		}
		if err := r.AppendEvent(tracing.WithTenantID(context.Background(), tenantID), e); err != nil {
			t.Fatalf("AppendEvent: %v", err)
		}
		// Insert args: $5 = payload → must be the degraded `{}`.
		args := q.args[len(q.args)-1]
		if pay, ok := args[4].([]byte); !ok || string(pay) != "{}" {
			t.Fatalf("unserializable payload must degrade to {}; got %v", args[4])
		}
	})
}

func TestShareRepo_ResolveDisplayName(t *testing.T) {
	t.Parallel()

	t.Run("happy", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error {
				bSetInto(dest, 0, "Walfa")
				return nil
			}}
		}}
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		name, err := r.ResolveDisplayName(tracing.WithTenantID(context.Background(), tenantID), authorGCID)
		if err != nil || name != "Walfa" {
			t.Fatalf("expected Walfa; got %q, %v", name, err)
		}
	})

	t.Run("no row → empty name", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error { return errors.New("no rows") }}
		}}
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		name, err := r.ResolveDisplayName(tracing.WithTenantID(context.Background(), tenantID), authorGCID)
		if err != nil || name != "" {
			t.Fatalf("expected empty, nil; got %q, %v", name, err)
		}
	})
}

func TestShareRepo_ListEvents(t *testing.T) {
	t.Parallel()

	t.Run("happy with nullable source + empty payload", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			queryFn: func(sql string, args ...any) (pg.Rows, error) {
				return &stubRows{scans: []func(dest ...any) error{
					func(dest ...any) error {
						bSetInto(dest, 0, "e-1")
						bSetInto(dest, 1, authorGCID)
						bSetInto(dest, 2, "hidden")
						bSetInto(dest, 3, []byte(`{"why":"x"}`))
						bSetInto(dest, 4, bPtr("src-1"))
						bSetInto(dest, 5, time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC))
						return nil
					},
					func(dest ...any) error {
						bSetInto(dest, 0, "e-2")
						bSetInto(dest, 1, authorGCID)
						bSetInto(dest, 2, "revoked")
						// dest[3] left zero-length, dest[4] nil
						return nil
					},
				}}, nil
			},
		}
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		events, err := r.ListEvents(tracing.WithTenantID(context.Background(), tenantID), shareEntryID)
		if err != nil {
			t.Fatalf("ListEvents: %v", err)
		}
		if len(events) != 2 {
			t.Fatalf("expected 2 events; got %d", len(events))
		}
		if events[0].SourceEventID != "src-1" || events[0].Type != atom_share.EventHidden {
			t.Fatalf("first event scan wrong: %+v", events[0])
		}
		if events[1].SourceEventID != "" {
			t.Fatalf("nil source must stay empty; got %q", events[1].SourceEventID)
		}
	})

	t.Run("no rows → empty slice", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{}
		r := pg.NewShareRepo(&stubTxRunner{q: q})
		events, err := r.ListEvents(tracing.WithTenantID(context.Background(), tenantID), shareEntryID)
		if err != nil || events == nil || len(events) != 0 {
			t.Fatalf("expected non-nil empty slice; got %#v, %v", events, err)
		}
	})

	t.Run("errors", func(t *testing.T) {
		t.Parallel()
		errQ := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("q") }}
		r1 := pg.NewShareRepo(&stubTxRunner{q: errQ})
		if _, err := r1.ListEvents(tracing.WithTenantID(context.Background(), tenantID), shareEntryID); err == nil {
			t.Fatalf("query error must propagate")
		}

		scanQ := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{func(dest ...any) error { return errors.New("s") }}}, nil
		}}
		r2 := pg.NewShareRepo(&stubTxRunner{q: scanQ})
		if _, err := r2.ListEvents(tracing.WithTenantID(context.Background(), tenantID), shareEntryID); err == nil {
			t.Fatalf("scan error must propagate")
		}

		rowsErrQ := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{}, err: errors.New("r")}, nil
		}}
		r3 := pg.NewShareRepo(&stubTxRunner{q: rowsErrQ})
		if _, err := r3.ListEvents(tracing.WithTenantID(context.Background(), tenantID), shareEntryID); err == nil {
			t.Fatalf("rows.Err must propagate")
		}
	})
}
