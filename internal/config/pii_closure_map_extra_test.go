// Additional PII Closure Map loader coverage: the fail-loud read + parse
// error paths that the happy-path TDD specs cannot reach.
package config_test

import (
	"path/filepath"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/config"
)

func TestLoadFromFile_MissingFileFails(t *testing.T) {
	t.Parallel()
	if _, err := config.LoadFromFile(filepath.Join(t.TempDir(), "does-not-exist.yaml")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadFromBytes_MalformedYAMLFails(t *testing.T) {
	t.Parallel()
	if _, err := config.LoadFromBytes([]byte("domain: [unclosed")); err == nil {
		t.Fatal("expected yaml parse error")
	}
}