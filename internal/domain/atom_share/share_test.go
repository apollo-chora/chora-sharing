package atom_share

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// --- stubShareRepo: in-memory ShareRepo double for port-contract tests ---

type stubShareRepo struct {
	shares map[string]*Share
	events map[string][]ShareEvent // feedEntryID -> events
}

func newStubShareRepo() *stubShareRepo {
	return &stubShareRepo{
		shares: make(map[string]*Share),
		events: make(map[string][]ShareEvent),
	}
}

func (s *stubShareRepo) SaveShare(_ context.Context, sh *Share) error {
	if _, exists := s.shares[sh.FeedEntryID]; exists {
		return ErrConflict
	}
	s.shares[sh.FeedEntryID] = sh
	return nil
}

func (s *stubShareRepo) GetShare(_ context.Context, feedEntryID string) (*Share, error) {
	sh, ok := s.shares[feedEntryID]
	if !ok {
		return nil, ErrNotFound
	}
	return sh, nil
}

func (s *stubShareRepo) ListSharedAtoms(_ context.Context, tenantID string, cursor string, limit int, topicFilter, questionTypeFilter string, scope string, followingGCIDs []string, blockedGCIDs []string) ([]Share, string, error) {
	if limit <= 0 {
		limit = 20
	}
	var result []Share
	for _, sh := range s.shares {
		if sh.TenantID != tenantID {
			continue
		}
		if topicFilter != "" && sh.QuestionType != topicFilter {
			continue
		}
		if questionTypeFilter != "" && sh.QuestionType != questionTypeFilter {
			continue
		}
		// Exclude revoked via derived status.
		if DeriveStatus(s.events[sh.FeedEntryID]) == StatusRevoked {
			continue
		}
		result = append(result, *sh)
	}
	if len(result) > limit {
		result = result[:limit]
	}
	if len(result) == 0 {
		return nil, "", nil
	}
	return result, result[len(result)-1].FeedEntryID, nil
}

func (s *stubShareRepo) AppendEvent(_ context.Context, e *ShareEvent) error {
	s.events[e.FeedEntryID] = append(s.events[e.FeedEntryID], *e)
	return nil
}

func (s *stubShareRepo) ListEvents(_ context.Context, feedEntryID string) ([]ShareEvent, error) {
	return s.events[feedEntryID], nil
}

// --- helpers ---

func validShareArgs() (string, string, string, string, string, string, string, []string, string, LicenseTerms, RoyaltyRate) {
	return "tenant-1", "atom-1", "rev-1", "owner-1", "Ada Lovelace", "What is x^2?", "mcq", []string{"2", "4", "6", "8"}, "A fine share", LicenseRoyaltyPct, RoyaltyRate{Kind: "pct", Value: 10}
}

func mustNewShare(t *testing.T, license LicenseTerms, rate RoyaltyRate) *Share {
	t.Helper()
	tenantID, atomID, revID, owner, name, stem, qt, options, caption, _, _ := validShareArgs()
	sh, err := NewShare(tenantID, atomID, revID, owner, name, stem, qt, options, caption, license, rate)
	if err != nil {
		t.Fatalf("NewShare: %v", err)
	}
	return sh
}

// --- LicenseTerms tests ---

func TestLicenseTerms_IsValid(t *testing.T) {
	valid := []LicenseTerms{LicenseFree, LicenseRoyaltyPct, LicenseRoyaltyFixed, LicenseCCBySA, LicenseCCND}
	for _, l := range valid {
		if !l.IsValid() {
			t.Errorf("expected %q to be valid", l)
		}
	}
	if LicenseTerms("bogus").IsValid() {
		t.Error("expected bogus license to be invalid")
	}
	if LicenseTerms("").IsValid() {
		t.Error("expected empty license to be invalid")
	}
}

func TestLicenseTerms_IsRoyalty(t *testing.T) {
	royalty := []LicenseTerms{LicenseRoyaltyPct, LicenseRoyaltyFixed}
	free := []LicenseTerms{LicenseFree, LicenseCCBySA, LicenseCCND}
	for _, l := range royalty {
		if !l.IsRoyalty() {
			t.Errorf("expected %q to be royalty", l)
		}
	}
	for _, l := range free {
		if l.IsRoyalty() {
			t.Errorf("expected %q to NOT be royalty", l)
		}
	}
}

// --- RoyaltyRate.Validate tests ---

func TestRoyaltyRate_Validate_Pct(t *testing.T) {
	cases := []struct {
		val   float64
		valid bool
	}{
		{0, true}, {50, true}, {100, true},
		{-0.1, false}, {100.1, false},
	}
	for _, tc := range cases {
		r := RoyaltyRate{Kind: "pct", Value: tc.val}
		err := r.Validate()
		if tc.valid && err != nil {
			t.Errorf("pct %v: expected nil, got %v", tc.val, err)
		}
		if !tc.valid && !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("pct %v: expected ErrInvalidArgument, got %v", tc.val, err)
		}
	}
}

func TestRoyaltyRate_Validate_FixedPerUse(t *testing.T) {
	cases := []struct {
		val   float64
		valid bool
	}{
		{0, true}, {5.5, true}, {1000, true},
		{-0.01, false},
	}
	for _, tc := range cases {
		r := RoyaltyRate{Kind: "fixed_per_use", Value: tc.val}
		err := r.Validate()
		if tc.valid && err != nil {
			t.Errorf("fixed %v: expected nil, got %v", tc.val, err)
		}
		if !tc.valid && !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("fixed %v: expected ErrInvalidArgument, got %v", tc.val, err)
		}
	}
}

func TestRoyaltyRate_Validate_InvalidKind(t *testing.T) {
	r := RoyaltyRate{Kind: "bogus", Value: 10}
	if !errors.Is(r.Validate(), ErrInvalidArgument) {
		t.Error("expected ErrInvalidArgument for bogus kind")
	}
}

// --- NewShare tests (R2 license rules) ---

func TestNewShare_AcceptsFreeLicenseWithoutRate(t *testing.T) {
	sh, err := NewShare("t", "a", "r", "o", "Ada", "stem", "mcq", nil, "cap", LicenseFree, RoyaltyRate{})
	if err != nil {
		t.Fatalf("free license without rate: %v", err)
	}
	if sh.License != LicenseFree {
		t.Errorf("expected free, got %v", sh.License)
	}
}

func TestNewShare_RejectsMissingRequiredFields(t *testing.T) {
	cases := []struct {
		name   string
		tenant string
		atom   string
		rev    string
		owner  string
	}{
		{"missing tenant_id", "", "a", "r", "o"},
		{"missing atom_id", "t", "", "r", "o"},
		{"missing revision_id", "t", "a", "", "o"},
		{"missing owner_gcid", "t", "a", "r", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewShare(tc.tenant, tc.atom, tc.rev, tc.owner, "Ada", "stem", "mcq", nil, "cap", LicenseFree, RoyaltyRate{})
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("%s: expected ErrInvalidArgument, got %v", tc.name, err)
			}
		})
	}
}

func TestNewShare_RejectsCaptionTooLong(t *testing.T) {
	long := strings.Repeat("x", MaxCaptionLen+1)
	_, err := NewShare("t", "a", "r", "o", "Ada", "stem", "mcq", nil, long, LicenseFree, RoyaltyRate{})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("long caption: expected ErrInvalidArgument, got %v", err)
	}
}

func TestNewShare_AcceptsEmptyCaption(t *testing.T) {
	sh, err := NewShare("t", "a", "r", "o", "Ada", "stem", "mcq", nil, "", LicenseFree, RoyaltyRate{})
	if err != nil {
		t.Fatalf("empty caption: %v", err)
	}
	if sh.Caption != "" {
		t.Errorf("expected empty caption, got %q", sh.Caption)
	}
}

func TestNewShare_TruncatesStemPreview(t *testing.T) {
	longStem := strings.Repeat("数", StemPreviewMax+10) // CJK multi-byte
	sh, err := NewShare("t", "a", "r", "o", "Ada", longStem, "mcq", nil, "cap", LicenseFree, RoyaltyRate{})
	if err != nil {
		t.Fatalf("long stem: %v", err)
	}
	if got := len([]rune(sh.StemPreview)); got != StemPreviewMax {
		t.Errorf("expected stem preview %d runes, got %d", StemPreviewMax, got)
	}
}

func TestNewShare_SetsCreatedAtUTC(t *testing.T) {
	before := time.Now().UTC()
	sh := mustNewShare(t, LicenseFree, RoyaltyRate{})
	if sh.CreatedAt.Before(before.Add(-time.Second)) {
		t.Error("CreatedAt not set to ~now")
	}
	if sh.CreatedAt.Location() != time.UTC {
		t.Errorf("expected UTC, got %v", sh.CreatedAt.Location())
	}
}

// --- DeriveStatus tests (newest-first) ---

func TestDeriveStatus_NoEventsIsVisible(t *testing.T) {
	if got := DeriveStatus(nil); got != StatusVisible {
		t.Errorf("expected visible, got %v", got)
	}
}

func TestDeriveStatus_HiddenByAuthor(t *testing.T) {
	events := []ShareEvent{
		{FeedEntryID: "f1", Type: EventHidden, CreatedAt: time.Now()},
	}
	if got := DeriveStatus(events); got != StatusHiddenByAuthor {
		t.Errorf("expected hidden_by_author, got %v", got)
	}
}

func TestDeriveStatus_Revoked(t *testing.T) {
	events := []ShareEvent{
		{FeedEntryID: "f1", Type: EventRevoked, CreatedAt: time.Now()},
	}
	if got := DeriveStatus(events); got != StatusRevoked {
		t.Errorf("expected revoked, got %v", got)
	}
}

func TestDeriveStatus_ModerationHidden(t *testing.T) {
	events := []ShareEvent{
		{FeedEntryID: "f1", Type: EventModerationHidden, CreatedAt: time.Now()},
	}
	if got := DeriveStatus(events); got != StatusModerationHidden {
		t.Errorf("expected moderation_hidden, got %v", got)
	}
}

func TestDeriveStatus_ModerationBeatsAuthorHide(t *testing.T) {
	// Author hides first, then moderator hides — moderator wins (governance override).
	base := time.Now()
	events := []ShareEvent{
		{FeedEntryID: "f1", Type: EventHidden, CreatedAt: base},
		{FeedEntryID: "f1", Type: EventModerationHidden, CreatedAt: base.Add(time.Second)},
	}
	if got := DeriveStatus(events); got != StatusModerationHidden {
		t.Errorf("expected moderation_hidden to beat author hide, got %v", got)
	}
}

func TestDeriveStatus_RevokedBeatsAuthorHide(t *testing.T) {
	base := time.Now()
	events := []ShareEvent{
		{FeedEntryID: "f1", Type: EventHidden, CreatedAt: base},
		{FeedEntryID: "f1", Type: EventRevoked, CreatedAt: base.Add(time.Second)},
	}
	if got := DeriveStatus(events); got != StatusRevoked {
		t.Errorf("expected revoked to beat author hide, got %v", got)
	}
}

func TestDeriveStatus_NewestFirstFromUnsorted(t *testing.T) {
	// Events inserted oldest-first; DeriveStatus must sort newest-first.
	base := time.Now()
	events := []ShareEvent{
		{FeedEntryID: "f1", Type: EventRevoked, CreatedAt: base.Add(2 * time.Second)},
		{FeedEntryID: "f1", Type: EventHidden, CreatedAt: base}, // oldest
		{FeedEntryID: "f1", Type: EventModerationHidden, CreatedAt: base.Add(time.Second)},
	}
	if got := DeriveStatus(events); got != StatusRevoked {
		t.Errorf("expected revoked (newest), got %v", got)
	}
}

func TestDeriveStatus_PriceChangedDoesNotAffectStatus(t *testing.T) {
	events := []ShareEvent{
		{FeedEntryID: "f1", Type: EventPriceChanged, CreatedAt: time.Now()},
	}
	if got := DeriveStatus(events); got != StatusVisible {
		t.Errorf("expected visible (price_changed irrelevant), got %v", got)
	}
}

func TestDeriveStatus_TiebreakDeterministic(t *testing.T) {
	// Two events with identical timestamp — tie-break on SourceEventID descending.
	base := time.Now()
	events := []ShareEvent{
		{FeedEntryID: "f1", Type: EventHidden, SourceEventID: "aaa", CreatedAt: base},
		{FeedEntryID: "f1", Type: EventRevoked, SourceEventID: "zzz", CreatedAt: base},
	}
	// "zzz" > "aaa" → revoked sorts first.
	if got := DeriveStatus(events); got != StatusRevoked {
		t.Errorf("expected revoked (higher source_event_id wins tie), got %v", got)
	}
}

// --- ShareRepo port contract tests (via stub) ---

func TestShareRepo_SaveAndGetShare(t *testing.T) {
	repo := newStubShareRepo()
	sh := mustNewShare(t, LicenseFree, RoyaltyRate{})
	sh.FeedEntryID = "entry-1"
	if err := repo.SaveShare(context.Background(), sh); err != nil {
		t.Fatalf("SaveShare: %v", err)
	}
	got, err := repo.GetShare(context.Background(), "entry-1")
	if err != nil {
		t.Fatalf("GetShare: %v", err)
	}
	if got.FeedEntryID != "entry-1" {
		t.Errorf("expected entry-1, got %s", got.FeedEntryID)
	}
}

func TestShareRepo_GetShareNotFound(t *testing.T) {
	repo := newStubShareRepo()
	_, err := repo.GetShare(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestShareRepo_SaveShareConflictOnDup(t *testing.T) {
	repo := newStubShareRepo()
	sh := mustNewShare(t, LicenseFree, RoyaltyRate{})
	sh.FeedEntryID = "entry-1"
	_ = repo.SaveShare(context.Background(), sh)
	if err := repo.SaveShare(context.Background(), sh); !errors.Is(err, ErrConflict) {
		t.Errorf("expected ErrConflict on dup, got %v", err)
	}
}

func TestShareRepo_AppendEventAndListEvents(t *testing.T) {
	repo := newStubShareRepo()
	e1 := &ShareEvent{FeedEntryID: "f1", Type: EventHidden, CreatedAt: time.Now()}
	e2 := &ShareEvent{FeedEntryID: "f1", Type: EventRevoked, CreatedAt: time.Now().Add(time.Second)}
	if err := repo.AppendEvent(context.Background(), e1); err != nil {
		t.Fatalf("AppendEvent e1: %v", err)
	}
	if err := repo.AppendEvent(context.Background(), e2); err != nil {
		t.Fatalf("AppendEvent e2: %v", err)
	}
	events, err := repo.ListEvents(context.Background(), "f1")
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

func TestShareRepo_ListSharedAtomsExcludesRevoked(t *testing.T) {
	repo := newStubShareRepo()
	sh1 := mustNewShare(t, LicenseFree, RoyaltyRate{})
	sh1.FeedEntryID = "entry-1"
	sh1.TenantID = "t1"
	sh2 := mustNewShare(t, LicenseFree, RoyaltyRate{})
	sh2.FeedEntryID = "entry-2"
	sh2.TenantID = "t1"
	_ = repo.SaveShare(context.Background(), sh1)
	_ = repo.SaveShare(context.Background(), sh2)
	// Revoke entry-2.
	_ = repo.AppendEvent(context.Background(), &ShareEvent{FeedEntryID: "entry-2", Type: EventRevoked, CreatedAt: time.Now()})

	shares, _, err := repo.ListSharedAtoms(context.Background(), "t1", "", 10, "", "", "tenant", nil, nil)
	if err != nil {
		t.Fatalf("ListSharedAtoms: %v", err)
	}
	if len(shares) != 1 {
		t.Fatalf("expected 1 share (revoked excluded), got %d", len(shares))
	}
	if shares[0].FeedEntryID != "entry-1" {
		t.Errorf("expected entry-1, got %s", shares[0].FeedEntryID)
	}
}

func TestShareRepo_ListSharedAtomsTenantFilter(t *testing.T) {
	repo := newStubShareRepo()
	sh1 := mustNewShare(t, LicenseFree, RoyaltyRate{})
	sh1.FeedEntryID = "entry-1"
	sh1.TenantID = "t1"
	sh2 := mustNewShare(t, LicenseFree, RoyaltyRate{})
	sh2.FeedEntryID = "entry-2"
	sh2.TenantID = "t2"
	_ = repo.SaveShare(context.Background(), sh1)
	_ = repo.SaveShare(context.Background(), sh2)

	shares, _, err := repo.ListSharedAtoms(context.Background(), "t1", "", 10, "", "", "tenant", nil, nil)
	if err != nil {
		t.Fatalf("ListSharedAtoms: %v", err)
	}
	if len(shares) != 1 || shares[0].TenantID != "t1" {
		t.Fatalf("expected 1 share in t1, got %v", shares)
	}
}

func TestShareRepo_ListSharedAtomsQuestionTypeFilter(t *testing.T) {
	repo := newStubShareRepo()
	sh1 := mustNewShare(t, LicenseFree, RoyaltyRate{})
	sh1.FeedEntryID = "entry-1"
	sh1.QuestionType = "mcq"
	sh2 := mustNewShare(t, LicenseFree, RoyaltyRate{})
	sh2.FeedEntryID = "entry-2"
	sh2.QuestionType = "oe"
	_ = repo.SaveShare(context.Background(), sh1)
	_ = repo.SaveShare(context.Background(), sh2)

	shares, _, err := repo.ListSharedAtoms(context.Background(), sh1.TenantID, "", 10, "mcq", "", "tenant", nil, nil)
	if err != nil {
		t.Fatalf("ListSharedAtoms: %v", err)
	}
	if len(shares) != 1 || shares[0].QuestionType != "mcq" {
		t.Fatalf("expected 1 mcq share, got %v", shares)
	}
}
