package bookmark

import (
	"strings"
	"testing"
)

func TestNewBookmark_Validation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		tenant    string
		gcid      string
		atomID    string
		revID     string
		wantErr   string
	}{
		{"empty tenant", "", "g1", "a1", "", "tenant_id required"},
		{"empty gcid", "t1", "", "a1", "", "gcid required"},
		{"empty atom_id", "t1", "g1", "", "", "atom_id required"},
		{"valid minimal", "t1", "g1", "a1", "", ""},
		{"valid with revision", "t1", "g1", "a1", "r1", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bm, err := NewBookmark(tc.tenant, tc.gcid, tc.atomID, tc.revID)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error %q, got nil", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if bm.ID == "" {
				t.Error("ID should be non-empty (UUIDv7)")
			}
			if len(bm.ID) != 36 {
				t.Errorf("ID should be 36-char UUID, got %d chars", len(bm.ID))
			}
			if bm.TenantID != tc.tenant {
				t.Errorf("TenantID = %q, want %q", bm.TenantID, tc.tenant)
			}
			if bm.GCID != tc.gcid {
				t.Errorf("GCID = %q, want %q", bm.GCID, tc.gcid)
			}
			if bm.AtomID != tc.atomID {
				t.Errorf("AtomID = %q, want %q", bm.AtomID, tc.atomID)
			}
			if bm.AtomRevisionID != strings.TrimSpace(tc.revID) {
				t.Errorf("AtomRevisionID = %q, want %q", bm.AtomRevisionID, tc.revID)
			}
			if bm.CreatedAt.IsZero() {
				t.Error("CreatedAt should be set")
			}
		})
	}
}

func TestNewBookmark_UniqueIDs(t *testing.T) {
	t.Parallel()
	bm1, _ := NewBookmark("t", "g", "a", "")
	bm2, _ := NewBookmark("t", "g", "a", "")
	if bm1.ID == bm2.ID {
		t.Error("two bookmarks should have different UUIDv7 IDs")
	}
}

func TestNewUUIDv7_Format(t *testing.T) {
	t.Parallel()
	id := NewUUIDv7()
	// UUIDv7: version nibble at position 14 should be '7', variant at 19 should be '8','9','a','b'.
	if len(id) != 36 {
		t.Fatalf("expected 36-char UUID, got %d", len(id))
	}
	if id[14] != '7' {
		t.Errorf("expected version 7 at position 14, got %q", string(id[14]))
	}
	switch id[19] {
	case '8', '9', 'a', 'b':
	default:
		t.Errorf("expected variant 8/9/a/b at position 19, got %q", string(id[19]))
	}
}
