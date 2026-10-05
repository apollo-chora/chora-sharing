package clients

import (
	"errors"
	"testing"
)

func TestStripCodeFences(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"plain json", `{"a":1}`, `{"a":1}`},
		{"json fence", "```json\n{\"a\":1}\n```", `{"a":1}`},
		{"bare fence", "```\n{\"a\":1}\n```", `{"a":1}`},
		{"fence no trailing newline", "```json\n{\"a\":1}```", `{"a":1}`},
		{"leading whitespace", "  ```json\n{\"a\":1}\n```  ", `{"a":1}`},
		{"empty", "", ""},
		{"fence opener only", "```json", ""},
		{"no fence with whitespace", "  {\"a\":1}  ", `{"a":1}`},
		{"fence with nested backticks in string", "```json\n{\"a\":\"x`y\"}\n```", `{"a":"x` + "`" + `y"}`},
	}
	for _, c := range cases {
		got := stripCodeFences(c.in)
		if got != c.want {
			t.Errorf("%s: stripCodeFences(%q) = %q; want %q", c.name, c.in, got, c.want)
		}
	}
}

func TestParseJSONEnvelope(t *testing.T) {
	t.Parallel()
	type target struct {
		A string `json:"a"`
		B int    `json:"b"`
	}
	cases := []struct {
		name    string
		in      string
		wantErr bool
		wantA   string
		wantB   int
	}{
		{"plain json", `{"a":"hello","b":42}`, false, "hello", 42},
		{"json fenced", "```json\n{\"a\":\"hello\",\"b\":42}\n```", false, "hello", 42},
		{"bare fenced", "```\n{\"a\":\"hello\",\"b\":42}\n```", false, "hello", 42},
		{"empty string", "", true, "", 0},
		{"garbage", "not json at all", true, "", 0},
		{"fenced garbage", "```json\nnot json\n```", true, "", 0},
	}
	for _, c := range cases {
		var got target
		err := parseJSONEnvelope(c.in, &got)
		if c.wantErr {
			if err == nil {
				t.Errorf("%s: expected error, got nil", c.name)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: unexpected error: %v", c.name, err)
			continue
		}
		if got.A != c.wantA || got.B != c.wantB {
			t.Errorf("%s: got {A:%q B:%d}; want {A:%q B:%d}", c.name, got.A, got.B, c.wantA, c.wantB)
		}
	}
}

func TestParseJSONEnvelope_NilTarget(t *testing.T) {
	t.Parallel()
	err := parseJSONEnvelope(`{"a":1}`, nil)
	if err == nil {
		t.Fatal("expected error for nil target")
	}
	if !errors.Is(err, errors.New("json: Unmarshal(nil)") ) {
		// json.Unmarshal returns *json.InvalidUnmarshalError; just verify it's non-nil
	}
}
