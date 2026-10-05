// familiar_milestone_extra_test.go — internals + fault branches of the
// FamiliarMilestoneSubscriber not covered by the behavioural specs:
//
//   - processOnce: Seen failure and Mark failure leave the key unmarked and
//     return the error (CHO-2263 ordering),
//   - dispatch: the unreachable-policy guard, PostPublisher failure,
//     DraftStore.Insert failure, and the Insert-duplicate skip,
//   - resolvePolicy: PreferenceStore.Get failure propagation,
//   - the composer render-error wraps on all four handlers,
//   - coercion helpers (asFloat64 int/int64, asStringSlice []any with
//     non-string elements, firstNonEmpty all-empty),
//   - InMemoryDraftStore.Insert duplicate-key behaviour.
package subscribers

import (
	"context"
	"errors"
	"testing"

	tmpl "github.com/apollo-chora/chora-sharing/internal/domain/post_template"
)

// errIdempotencyStore is an IdempotencyStore whose peek/commit halves fail,
// proving processOnce's ordering does not swallow store errors.
type errIdempotencyStore struct {
	seenErr error
	markErr error
}

func (e errIdempotencyStore) Seen(_ context.Context, _, _ string) (bool, error) { return false, e.seenErr }
func (e errIdempotencyStore) Mark(_ context.Context, _, _ string) error         { return e.markErr }

func TestProcessOnce_SeenError_Propagates(t *testing.T) {
	err := processOnce(context.Background(), errIdempotencyStore{seenErr: errors.New("peek down")}, "h", "id", nil, func() error {
		t.Fatalf("fn must not run when the peek fails")
		return nil
	})
	if err == nil {
		t.Fatalf("Seen failure must propagate")
	}
}

func TestProcessOnce_MarkError_PropagatesUnmarked(t *testing.T) {
	ran := false
	err := processOnce(context.Background(), errIdempotencyStore{markErr: errors.New("commit down")}, "h", "id", nil, func() error {
		ran = true
		return nil
	})
	if err == nil {
		t.Fatalf("Mark failure must propagate")
	}
	if !ran {
		t.Fatalf("fn must have run before the Mark attempt")
	}
}

// errPostPublisher is a PostPublisher whose publish always fails.
type errPostPublisher struct{}

func (errPostPublisher) Publish(_ context.Context, _ PostRecord) error {
	return errors.New("post bus down")
}

// errDraftStore is a DraftStore whose insert always fails.
type errDraftStore struct{}

func (errDraftStore) Insert(_ context.Context, _ Draft) (bool, error) {
	return false, errors.New("drafts down")
}

// errPreferenceStore is a PreferenceStore whose read fails.
type errPreferenceStore struct{}

func (errPreferenceStore) Get(_ context.Context, _, _ string) (Policy, bool, error) {
	return "", false, errors.New("preferences down")
}
func (errPreferenceStore) Upsert(_ context.Context, _, _ string, _ Policy) error { return nil }

func dispatchSub(t *testing.T, cfg Config) *FamiliarMilestoneSubscriber {
	t.Helper()
	return NewFamiliarMilestoneSubscriber(cfg)
}

func TestDispatch_EmptyComposedBody_NewPostError(t *testing.T) {
	// An auto-post whose composed body is empty must fail at the Post
	// aggregate (body required), never publish an empty post.
	sub := dispatchSub(t, Config{
		Drafts:      NewInMemoryDraftStore(),
		Posts:       NewInMemoryPostPublisher(),
		Preferences: NewInMemoryPreferenceStore(),
		Idempotency: NewInMemoryIdempotencyStore(),
		Composer:    tmpl.NewComposer(tmpl.DefaultTemplates()),
	})
	err := sub.dispatch(context.Background(), PolicyAuto, dispatchInput{
		Handler: HandlerStageUp, Topic: TopicFamiliarStageUp, EventID: "e",
		TenantID: "t", AuthorGCID: "g", // Body deliberately empty
		Metadata: map[string]interface{}{},
	})
	if err == nil {
		t.Fatalf("empty-body post must fail loud")
	}
}

func TestDispatch_UnreachablePolicy_FailsLoud(t *testing.T) {
	sub := dispatchSub(t, Config{
		Drafts:      NewInMemoryDraftStore(),
		Posts:       NewInMemoryPostPublisher(),
		Preferences: NewInMemoryPreferenceStore(),
		Idempotency: NewInMemoryIdempotencyStore(),
		Composer:    tmpl.NewComposer(tmpl.DefaultTemplates()),
	})
	err := sub.dispatch(context.Background(), Policy("bogus"), dispatchInput{
		Handler: HandlerStageUp, TenantID: "t", AuthorGCID: "g", Body: "x",
	})
	if err == nil {
		t.Fatalf("unreachable policy must fail loud")
	}
}

func TestDispatch_AutoPublishError_Propagates(t *testing.T) {
	sub := dispatchSub(t, Config{
		Drafts:      NewInMemoryDraftStore(),
		Posts:       errPostPublisher{},
		Preferences: NewInMemoryPreferenceStore(),
		Idempotency: NewInMemoryIdempotencyStore(),
		Composer:    tmpl.NewComposer(tmpl.DefaultTemplates()),
	})
	err := sub.dispatch(context.Background(), PolicyAuto, dispatchInput{
		Handler: HandlerStageUp, Topic: TopicFamiliarStageUp, EventID: "e",
		TenantID: "t", AuthorGCID: "g", Body: "x",
		Metadata: map[string]interface{}{},
	})
	if err == nil {
		t.Fatalf("PostPublisher failure must propagate")
	}
}

func TestDispatch_DraftInsertError_Propagates(t *testing.T) {
	sub := dispatchSub(t, Config{
		Drafts:      errDraftStore{},
		Posts:       NewInMemoryPostPublisher(),
		Preferences: NewInMemoryPreferenceStore(),
		Idempotency: NewInMemoryIdempotencyStore(),
		Composer:    tmpl.NewComposer(tmpl.DefaultTemplates()),
	})
	err := sub.dispatch(context.Background(), PolicyDraft, dispatchInput{
		Handler: HandlerStageUp, Topic: TopicFamiliarStageUp, EventID: "e",
		TenantID: "t", AuthorGCID: "g", Body: "x",
		Metadata: map[string]interface{}{},
	})
	if err == nil {
		t.Fatalf("DraftStore.Insert failure must propagate")
	}
}

func TestDispatch_DraftInsertDuplicate_Skips(t *testing.T) {
	drafts := NewInMemoryDraftStore()
	sub := dispatchSub(t, Config{
		Drafts:      drafts,
		Posts:       NewInMemoryPostPublisher(),
		Preferences: NewInMemoryPreferenceStore(),
		Idempotency: NewInMemoryIdempotencyStore(),
		Composer:    tmpl.NewComposer(tmpl.DefaultTemplates()),
	})
	in := dispatchInput{
		Handler: HandlerStageUp, Topic: TopicFamiliarStageUp, EventID: "e",
		TenantID: "t", AuthorGCID: "g", Body: "x",
		Metadata: map[string]interface{}{},
	}
	if err := sub.dispatch(context.Background(), PolicyDraft, in); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	// An already-drafted (topic, event_id) — Insert returns (false, nil) and the
	// subscriber treats it as a no-op rather than an error.
	if err := sub.dispatch(context.Background(), PolicyDraft, in); err != nil {
		t.Fatalf("duplicate insert must be a no-op, got err: %v", err)
	}
	if got := drafts.All(); len(got) != 1 {
		t.Fatalf("drafts = %d, want 1 (no duplicate row)", len(got))
	}
}

func TestResolvePolicy_PreferenceError_Propagates(t *testing.T) {
	sub := dispatchSub(t, Config{
		Drafts:      NewInMemoryDraftStore(),
		Posts:       NewInMemoryPostPublisher(),
		Preferences: errPreferenceStore{},
		Idempotency: NewInMemoryIdempotencyStore(),
		Composer:    tmpl.NewComposer(tmpl.DefaultTemplates()),
	})
	if _, err := sub.resolvePolicy(context.Background(), "t", "g"); err == nil {
		t.Fatalf("PreferenceStore.Get failure must propagate")
	}
}

// brokenComposer is a Composer with NO templates configured — every milestone
// render bricks, exercising the handler render-error wraps.
func brokenComposer() *tmpl.Composer {
	return tmpl.NewComposer(tmpl.Templates{})
}

func TestFamiliarHandlers_ComposerError_Wraps(t *testing.T) {
	cases := []struct {
		name string
		run  func(sub *FamiliarMilestoneSubscriber, ctx context.Context) error
	}{
		{"stage_up", func(sub *FamiliarMilestoneSubscriber, ctx context.Context) error {
			return sub.HandleStageUp(ctx, StageUpEnvelope{EventID: "e", TenantID: "t", OwnerGCID: "g", StageToName: "Teen"})
		}},
		{"breed_revealed", func(sub *FamiliarMilestoneSubscriber, ctx context.Context) error {
			return sub.HandleBreedRevealed(ctx, BreedRevealedEnvelope{EventID: "e", TenantID: "t", OwnerGCID: "g", Species: "fox"})
		}},
		{"hatched", func(sub *FamiliarMilestoneSubscriber, ctx context.Context) error {
			return sub.HandleHatched(ctx, HatchedEnvelope{EventID: "e", TenantID: "t", OwnerGCID: "g", Species: "fox"})
		}},
		{"source_revelation", func(sub *FamiliarMilestoneSubscriber, ctx context.Context) error {
			return sub.HandleSourceRevelation(ctx, SourceRevelationEnvelope{EventID: "e", TenantID: "t", OwnerGCID: "g"})
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sub := dispatchSub(t, Config{
				Drafts:      NewInMemoryDraftStore(),
				Posts:       NewInMemoryPostPublisher(),
				Preferences: NewInMemoryPreferenceStore(),
				Idempotency: NewInMemoryIdempotencyStore(),
				Composer:    brokenComposer(),
			})
			if err := c.run(sub, context.Background()); err == nil {
				t.Fatalf("composer render failure must NACK")
			}
		})
	}
}

func TestFamiliarHandlers_PreferenceError_Wraps(t *testing.T) {
	cases := []struct {
		name string
		run  func(sub *FamiliarMilestoneSubscriber, ctx context.Context) error
	}{
		{"stage_up", func(sub *FamiliarMilestoneSubscriber, ctx context.Context) error {
			return sub.HandleStageUp(ctx, StageUpEnvelope{EventID: "e", TenantID: "t", OwnerGCID: "g", StageToName: "Teen"})
		}},
		{"breed_revealed", func(sub *FamiliarMilestoneSubscriber, ctx context.Context) error {
			return sub.HandleBreedRevealed(ctx, BreedRevealedEnvelope{EventID: "e", TenantID: "t", OwnerGCID: "g"})
		}},
		{"hatched", func(sub *FamiliarMilestoneSubscriber, ctx context.Context) error {
			return sub.HandleHatched(ctx, HatchedEnvelope{EventID: "e", TenantID: "t", OwnerGCID: "g"})
		}},
		{"source_revelation", func(sub *FamiliarMilestoneSubscriber, ctx context.Context) error {
			return sub.HandleSourceRevelation(ctx, SourceRevelationEnvelope{EventID: "e", TenantID: "t", OwnerGCID: "g"})
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sub := dispatchSub(t, Config{
				Drafts:      NewInMemoryDraftStore(),
				Posts:       NewInMemoryPostPublisher(),
				Preferences: errPreferenceStore{},
				Idempotency: NewInMemoryIdempotencyStore(),
				Composer:    tmpl.NewComposer(tmpl.DefaultTemplates()),
			})
			if err := c.run(sub, context.Background()); err == nil {
				t.Fatalf("preference read failure must NACK")
			}
		})
	}
}

func TestFamiliarHandlers_PreflightError_Wraps(t *testing.T) {
	// No deps configured — every handler must fail closed on preflight.
	sub := NewFamiliarMilestoneSubscriber(Config{})
	ctx := context.Background()
	cases := []struct {
		name string
		run  func(ctx context.Context) error
	}{
		{"stage_up", func(ctx context.Context) error {
			return sub.HandleStageUp(ctx, StageUpEnvelope{EventID: "e", TenantID: "t", OwnerGCID: "g", StageToName: "Teen"})
		}},
		{"breed_revealed", func(ctx context.Context) error {
			return sub.HandleBreedRevealed(ctx, BreedRevealedEnvelope{EventID: "e", TenantID: "t", OwnerGCID: "g"})
		}},
		{"hatched", func(ctx context.Context) error {
			return sub.HandleHatched(ctx, HatchedEnvelope{EventID: "e", TenantID: "t", OwnerGCID: "g"})
		}},
		{"source_revelation", func(ctx context.Context) error {
			return sub.HandleSourceRevelation(ctx, SourceRevelationEnvelope{EventID: "e", TenantID: "t", OwnerGCID: "g"})
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := c.run(ctx); err == nil {
				t.Fatalf("missing dependencies must fail closed")
			}
		})
	}
}

// --- coercion helpers ------------------------------------------------------------

func TestAsFloat64_Coercions(t *testing.T) {
	if got := asFloat64(float64(1.5)); got != 1.5 {
		t.Fatalf("asFloat64(float64) = %v", got)
	}
	if got := asFloat64(int(7)); got != 7 {
		t.Fatalf("asFloat64(int) = %v", got)
	}
	if got := asFloat64(int64(8)); got != 8 {
		t.Fatalf("asFloat64(int64) = %v", got)
	}
	if got := asFloat64("nope"); got != 0 {
		t.Fatalf("asFloat64(string) = %v, want 0", got)
	}
}

func TestAsStringSlice_StringSlicePassthrough(t *testing.T) {
	// The proto projector path yields []string; it must pass through as-is.
	got := asStringSlice([]string{"a", "b"})
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("asStringSlice([]string) = %v, want [a b]", got)
	}
}

func TestAsStringSlice_MixedJSONElements(t *testing.T) {
	// json.Unmarshal yields []any; non-string elements are skipped.
	got := asStringSlice([]any{"a", 1.0, "b"})
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("asStringSlice([]any) = %v, want [a b]", got)
	}
}

func TestFirstNonEmpty_AllEmpty(t *testing.T) {
	if got := firstNonEmpty("", "  ", ""); got != "" {
		t.Fatalf("firstNonEmpty(all empty) = %q, want \"\"", got)
	}
}

// --- in-memory doubles ------------------------------------------------------------

func TestInMemoryDraftStore_InsertDuplicate(t *testing.T) {
	s := NewInMemoryDraftStore()
	d := Draft{
		DraftID: "d-1", TenantID: "t", AuthorGCID: "g",
		ComposedFromTopic: "topic", ComposedFromEventID: "evt", Body: "x",
	}
	created, err := s.Insert(context.Background(), d)
	if err != nil || !created {
		t.Fatalf("first insert: created=%v err=%v", created, err)
	}
	created2, err := s.Insert(context.Background(), d)
	if err != nil {
		t.Fatalf("duplicate insert: %v", err)
	}
	if created2 {
		t.Fatalf("duplicate insert must report created=false")
	}
	if got := s.All(); len(got) != 1 {
		t.Fatalf("duplicate insert must not grow the store; got %d drafts", len(got))
	}
}