package clients

import (
	"context"
	"regexp"
	"testing"
)

func TestWithTraceparent(t *testing.T) {
	ctx := context.Background()
	if got := WithTraceparent(ctx, ""); got != ctx {
		t.Error("empty traceparent must return the original context unchanged")
	}
	const tp = "00-11112222333344445555666677778888-9999aaaabbbbcccc-01"
	if got := resolveTraceparent(WithTraceparent(ctx, tp)); got != tp {
		t.Errorf("resolveTraceparent(WithTraceparent) = %q, want %q", got, tp)
	}
}

func TestResolveTraceparent_MintsWhenAbsent(t *testing.T) {
	re := regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-01$`)
	a := resolveTraceparent(context.Background())
	b := resolveTraceparent(context.Background())
	if !re.MatchString(a) {
		t.Errorf("traceparent = %q, want W3C version-00 sampled-01 format", a)
	}
	if a == b {
		t.Error("two minted traceparents must differ (random trace id)")
	}
}

func TestRandHex(t *testing.T) {
	re := regexp.MustCompile(`^[0-9a-f]+$`)
	for _, n := range []int{8, 16} {
		got := randHex(n)
		if len(got) != n*2 {
			t.Errorf("randHex(%d) len = %d, want %d", n, len(got), n*2)
		}
		if !re.MatchString(got) {
			t.Errorf("randHex(%d) = %q, want lowercase hex", n, got)
		}
	}
}

func TestEncodeHex(t *testing.T) {
	cases := map[string]string{
		"":             "",
		"\xab\x12":     "ab12",
		"hello":        "68656c6c6f",
		"\xff\x00\x0f": "ff000f",
	}
	for in, want := range cases {
		if got := encodeHex([]byte(in)); got != want {
			t.Errorf("encodeHex(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNowNanos(t *testing.T) {
	if got := nowNanos(); got <= 0 {
		t.Errorf("nowNanos() = %d, want > 0", got)
	}
	if a, b := nowNanos(), nowNanos(); b < a {
		t.Errorf("nowNanos() went backwards: %d then %d", a, b)
	}
}

func TestReadRand(t *testing.T) {
	buf := make([]byte, 16)
	if _, err := readRand(buf); err != nil {
		t.Fatalf("readRand: %v", err)
	}
	for _, b := range buf {
		if b != 0 {
			// buffer filled with at least one non-zero byte
			return
		}
	}
	t.Error("readRand returned all-zero buffer (astronomically unlikely)")
}
