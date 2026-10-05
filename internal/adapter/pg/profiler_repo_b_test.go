// profiler_repo_b_test.go — coverage tests for ProfilerRepo (second coverage
// agent): SaveProfile upsert + JSON marshalling, GetProfile scan/unmarshal
// branches, ResolveDisplayNames unnest query + empty-input shortcut.
package pg_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-sharing/internal/adapter/pg"
	"github.com/apollo-chora/chora-sharing/internal/domain/profiler"
)

func TestProfilerRepo_NilTxRunner_ReturnsErrNotImplemented(t *testing.T) {
	t.Parallel()
	r := pg.NewProfilerRepo(nil)
	if err := r.SaveProfile(context.Background(), &profiler.Profile{}); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("SaveProfile: expected ErrNotImplemented; got %v", err)
	}
	if _, err := r.GetProfile(context.Background(), authorGCID); !errors.Is(err, pg.ErrNotImplemented) {
		t.Fatalf("GetProfile: expected ErrNotImplemented; got %v", err)
	}
}

func TestProfilerRepo_SaveProfile_RejectsNil(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewProfilerRepo(&stubTxRunner{q: q})
	if err := r.SaveProfile(tracing.WithTenantID(context.Background(), tenantID), nil); !errors.Is(err, profiler.ErrInvalidArgument) {
		t.Fatalf("expected profiler.ErrInvalidArgument; got %v", err)
	}
}

func TestProfilerRepo_SaveProfile_UpsertsWithJSON(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewProfilerRepo(&stubTxRunner{q: q})
	p := &profiler.Profile{
		GCID:         authorGCID,
		TenantID:     tenantID,
		Bio:          "Hello",
		DisplayName:  "Walfa",
		Tags:         []profiler.InterestTag{{Category: profiler.CategoryProgramming, Tag: "go"}},
		Proficiency:  profiler.Proficiency{PerCategory: map[string]profiler.ProficiencyLevel{"programming": profiler.ProficiencyAdvanced}},
		CourseTitles: []string{"course-1"},
		CreatedAt:    time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
		UpdatedAt:    time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
	}
	if err := r.SaveProfile(tracing.WithTenantID(context.Background(), tenantID), p); err != nil {
		t.Fatalf("SaveProfile: %v", err)
	}
	if !strings.Contains(q.sqls[0], "SET LOCAL") {
		t.Fatalf("first SQL must be SET LOCAL; got %q", q.sqls[0])
	}
	last := q.sqls[len(q.sqls)-1]
	if !strings.Contains(last, "INSERT INTO profiler_profiles") || !strings.Contains(last, "ON CONFLICT (tenant_id, gcid) DO UPDATE") {
		t.Fatalf("expected profile upsert; got %q", last)
	}
	args := q.args[len(q.args)-1]
	// $4 = tags JSON string, $6 = proficiency JSON string.
	if s, ok := args[3].(string); !ok || !strings.Contains(s, `"tag":"go"`) {
		t.Fatalf("tags JSON wrong: %v", args[3])
	}
	if s, ok := args[5].(string); !ok || !strings.Contains(s, "advanced") {
		t.Fatalf("proficiency JSON wrong: %v", args[5])
	}
}

func TestProfilerRepo_SaveProfile_DefaultsAndNilCourses(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewProfilerRepo(&stubTxRunner{q: q})
	p := &profiler.Profile{GCID: authorGCID, TenantID: tenantID} // zero times + nil courses
	if err := r.SaveProfile(tracing.WithTenantID(context.Background(), tenantID), p); err != nil {
		t.Fatalf("SaveProfile: %v", err)
	}
	args := q.args[len(q.args)-1]
	courses, ok := args[4].([]string)
	if !ok || len(courses) != 0 {
		t.Fatalf("nil courses must bind empty []string; got %#v", args[4])
	}
	if created, ok := args[7].(time.Time); !ok || created.IsZero() {
		t.Fatalf("created must default to now; got %v", args[7])
	}
	if updated, ok := args[8].(time.Time); !ok || updated.IsZero() {
		t.Fatalf("updated must default to now; got %v", args[8])
	}
}

func TestProfilerRepo_SaveProfile_ExecError(t *testing.T) {
	t.Parallel()
	q := &stubQuerierB{execErrFn: func(sql string) error {
		if strings.Contains(sql, "SET LOCAL") {
			return nil
		}
		return errors.New("boom")
	}}
	r := pg.NewProfilerRepo(&stubTxRunnerB{q: q})
	p := &profiler.Profile{GCID: authorGCID, TenantID: tenantID}
	err := r.SaveProfile(tracing.WithTenantID(context.Background(), tenantID), p)
	if err == nil || !strings.Contains(err.Error(), "upsert profiler profile") {
		t.Fatalf("expected wrapped upsert error; got %v", err)
	}
}

func TestProfilerRepo_GetProfile_EmptyGCIDReturnsNil(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	r := pg.NewProfilerRepo(&stubTxRunner{q: q})
	p, err := r.GetProfile(tracing.WithTenantID(context.Background(), tenantID), "")
	if err != nil || p != nil {
		t.Fatalf("expected (nil, nil); got %+v, %v", p, err)
	}
	if len(q.sqls) != 0 {
		t.Fatalf("empty gcid must issue NO SQL; got %v", q.sqls)
	}
}

func TestProfilerRepo_GetProfile_HappyPathUnmarshals(t *testing.T) {
	t.Parallel()
	tagsJSON, _ := json.Marshal([]profiler.InterestTag{{Category: profiler.CategoryScience, Tag: "physics"}})
	profJSON, _ := json.Marshal(profiler.Proficiency{PerCategory: map[string]profiler.ProficiencyLevel{"science": profiler.ProficiencyIntermediate}})
	q := &stubQuerier{
		rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error {
				bSetInto(dest, 0, authorGCID)
				bSetInto(dest, 1, tenantID)
				bSetInto(dest, 2, "bio")
				bSetInto(dest, 3, tagsJSON)
				bSetInto(dest, 4, []string{"c1"})
				bSetInto(dest, 5, profJSON)
				bSetInto(dest, 6, "Display")
				bSetInto(dest, 7, time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC))
				bSetInto(dest, 8, time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC))
				return nil
			}}
		},
	}
	r := pg.NewProfilerRepo(&stubTxRunner{q: q})
	p, err := r.GetProfile(tracing.WithTenantID(context.Background(), tenantID), authorGCID)
	if err != nil {
		t.Fatalf("GetProfile: %v", err)
	}
	if p == nil || p.GCID != authorGCID || p.DisplayName != "Display" {
		t.Fatalf("scan wrong: %+v", p)
	}
	if len(p.Tags) != 1 || p.Tags[0].Tag != "physics" {
		t.Fatalf("tags unmarshal wrong: %+v", p.Tags)
	}
	if len(p.CourseTitles) != 1 || p.CourseTitles[0] != "c1" {
		t.Fatalf("courses wrong: %+v", p.CourseTitles)
	}
	if p.Proficiency.PerCategory["science"] != profiler.ProficiencyIntermediate {
		t.Fatalf("proficiency unmarshal wrong: %+v", p.Proficiency)
	}
}

func TestProfilerRepo_GetProfile_NoRowsAndEmptyJSON(t *testing.T) {
	t.Parallel()

	t.Run("no rows → (nil, nil)", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error { return noRowsB() }}
		}}
		r := pg.NewProfilerRepo(&stubTxRunner{q: q})
		p, err := r.GetProfile(tracing.WithTenantID(context.Background(), tenantID), authorGCID)
		if err != nil || p != nil {
			t.Fatalf("expected (nil, nil); got %+v, %v", p, err)
		}
	})

	t.Run("empty tags JSON defaults", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error {
				bSetInto(dest, 0, authorGCID)
				bSetInto(dest, 6, "")
				return nil
			}}
		}}
		r := pg.NewProfilerRepo(&stubTxRunner{q: q})
		p, err := r.GetProfile(tracing.WithTenantID(context.Background(), tenantID), authorGCID)
		if err != nil || p == nil {
			t.Fatalf("expected profile; got %+v, %v", p, err)
		}
		if p.Tags == nil || len(p.Tags) != 0 {
			t.Fatalf("nil tags must default to empty slice; got %#v", p.Tags)
		}
	})

	t.Run("unmarshal error propagates", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error {
				bSetInto(dest, 0, authorGCID)
				bSetInto(dest, 3, []byte("not json"))
				return nil
			}}
		}}
		r := pg.NewProfilerRepo(&stubTxRunner{q: q})
		if _, err := r.GetProfile(tracing.WithTenantID(context.Background(), tenantID), authorGCID); err == nil {
			t.Fatalf("bad tags JSON must propagate error")
		}
	})

	t.Run("bad proficiency JSON propagates", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error {
				bSetInto(dest, 0, authorGCID)
				bSetInto(dest, 5, []byte("not json"))
				return nil
			}}
		}}
		r := pg.NewProfilerRepo(&stubTxRunner{q: q})
		if _, err := r.GetProfile(tracing.WithTenantID(context.Background(), tenantID), authorGCID); err == nil {
			t.Fatalf("bad proficiency JSON must propagate error")
		}
	})

	t.Run("generic scan error propagates", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{rowFn: func(sql string, args ...any) pg.Row {
			return stubRow{scanFn: func(dest ...any) error { return errors.New("conn lost") }}
		}}
		r := pg.NewProfilerRepo(&stubTxRunner{q: q})
		_, err := r.GetProfile(tracing.WithTenantID(context.Background(), tenantID), authorGCID)
		if err == nil || !strings.Contains(err.Error(), "select profiler profile") {
			t.Fatalf("generic scan error must wrap; got %v", err)
		}
	})
}

func TestProfilerRepo_ResolveDisplayNames(t *testing.T) {
	t.Parallel()

	t.Run("empty input returns empty map without SQL", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{}
		r := pg.NewProfilerRepo(&stubTxRunner{q: q})
		out, err := r.ResolveDisplayNames(tracing.WithTenantID(context.Background(), tenantID), nil)
		if err != nil || out == nil || len(out) != 0 {
			t.Fatalf("expected empty map; got %v, %v", out, err)
		}
		if len(q.sqls) != 0 {
			t.Fatalf("empty gcids must issue NO SQL; got %v", q.sqls)
		}
	})

	t.Run("nil runner returns empty map", func(t *testing.T) {
		t.Parallel()
		var r *pg.ProfilerRepo
		out, err := r.ResolveDisplayNames(tracing.WithTenantID(context.Background(), tenantID), []string{authorGCID})
		if err != nil || out == nil || len(out) != 0 {
			t.Fatalf("expected empty map from nil receiver; got %v, %v", out, err)
		}
	})

	t.Run("scans names skipping empties", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{
			queryFn: func(sql string, args ...any) (pg.Rows, error) {
				if !strings.Contains(sql, "unnest") {
					t.Fatalf("expected unnest query; got %q", sql)
				}
				return &stubRows{scans: []func(dest ...any) error{
					func(dest ...any) error {
						bSetInto(dest, 0, authorGCID)
						bSetInto(dest, 1, "Walfa")
						return nil
					},
					func(dest ...any) error {
						bSetInto(dest, 0, otherGCID)
						bSetInto(dest, 1, "")
						return nil
					},
				}}, nil
			},
		}
		r := pg.NewProfilerRepo(&stubTxRunner{q: q})
		out, err := r.ResolveDisplayNames(tracing.WithTenantID(context.Background(), tenantID), []string{authorGCID, otherGCID})
		if err != nil {
			t.Fatalf("ResolveDisplayNames: %v", err)
		}
		if out[authorGCID] != "Walfa" {
			t.Fatalf("display name wrong: %v", out)
		}
		if _, ok := out[otherGCID]; ok {
			t.Fatalf("empty display name must be omitted; got %v", out)
		}
	})

	t.Run("query error wraps", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) { return nil, errors.New("q") }}
		r := pg.NewProfilerRepo(&stubTxRunner{q: q})
		_, err := r.ResolveDisplayNames(tracing.WithTenantID(context.Background(), tenantID), []string{authorGCID})
		if err == nil || !strings.Contains(err.Error(), "resolve display names") {
			t.Fatalf("expected wrapped query error; got %v", err)
		}
	})

	t.Run("scan error wraps", func(t *testing.T) {
		t.Parallel()
		q := &stubQuerier{queryFn: func(sql string, args ...any) (pg.Rows, error) {
			return &stubRows{scans: []func(dest ...any) error{func(dest ...any) error { return errors.New("s") }}}, nil
		}}
		r := pg.NewProfilerRepo(&stubTxRunner{q: q})
		_, err := r.ResolveDisplayNames(tracing.WithTenantID(context.Background(), tenantID), []string{authorGCID})
		if err == nil || !strings.Contains(err.Error(), "scan display name") {
			t.Fatalf("expected wrapped scan error; got %v", err)
		}
	})
}
