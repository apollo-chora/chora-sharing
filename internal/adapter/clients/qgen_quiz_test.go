package clients

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/livequiz"
	modelgatewayv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/model_gateway/v1"
	"google.golang.org/grpc"
)

// fakeModelGatewayClient implements modelgatewayv1.ModelGatewayServiceClient
// for QuizGenClient tests. The embedded interface satisfies the unexercised
// RPCs; Invoke is overridden with scripted behaviour.
type fakeModelGatewayClient struct {
	modelgatewayv1.ModelGatewayServiceClient
	invokeResp *modelgatewayv1.InvokeResponse
	invokeErr  error
	gotReq     *modelgatewayv1.InvokeRequest
}

func (f *fakeModelGatewayClient) Invoke(_ context.Context, in *modelgatewayv1.InvokeRequest, _ ...grpc.CallOption) (*modelgatewayv1.InvokeResponse, error) {
	f.gotReq = in
	if f.invokeErr != nil {
		return nil, f.invokeErr
	}
	return f.invokeResp, nil
}

const validQuestionJSON = `[{"atom_id":"a1","atom_revision_id":"r1","stem":"What is 2+2?","options":["3","4","5","6"],"correct_option_id":"4","timer_seconds":30,"points":10}]`

func TestNewQuizGenClient(t *testing.T) {
	gw := &fakeModelGatewayClient{}
	c := NewQuizGenClient(gw, "gemini-2.5-flash")
	if c == nil {
		t.Fatal("NewQuizGenClient returned nil")
	}
	if c.modelID != "gemini-2.5-flash" {
		t.Errorf("modelID = %q, want gemini-2.5-flash", c.modelID)
	}
}

func TestQuizGenClient_GenerateQuizQuestions_HappyPath(t *testing.T) {
	gw := &fakeModelGatewayClient{invokeResp: &modelgatewayv1.InvokeResponse{
		Completion:   validQuestionJSON,
		FinishReason: modelgatewayv1.FinishReason_FINISH_REASON_COMPLETE,
	}}
	c := NewQuizGenClient(gw, "gemini-2.5-flash")

	qs, err := c.GenerateQuizQuestions(context.Background(), "gcid-instr", "tenant-x", "Probability", []string{"bayes", "poker"}, 2)
	if err != nil {
		t.Fatalf("GenerateQuizQuestions: %v", err)
	}
	if len(qs) != 1 {
		t.Fatalf("questions len = %d, want 1", len(qs))
	}
	if qs[0].AtomID != "a1" || qs[0].Stem != "What is 2+2?" || qs[0].CorrectOptionID != "4" {
		t.Errorf("question = %+v, want seeded a1 question", qs[0])
	}
	// Request plumbing: agent_id + scoping + prompt carrying count/tags.
	req := gw.gotReq
	if req.GetAgentId() != quizAgentID {
		t.Errorf("AgentId = %q, want %q", req.GetAgentId(), quizAgentID)
	}
	if req.GetTenantId() != "tenant-x" || req.GetGcid() != "gcid-instr" {
		t.Errorf("scoping = tenant:%q gcid:%q, want tenant-x/gcid-instr", req.GetTenantId(), req.GetGcid())
	}
	if req.GetLogicalModelId() != "gemini-2.5-flash" {
		t.Errorf("LogicalModelId = %q, want gemini-2.5-flash", req.GetLogicalModelId())
	}
	if req.GetCrewKind() != "qgen" {
		t.Errorf("CrewKind = %q, want qgen", req.GetCrewKind())
	}
	if !strings.Contains(req.GetPrompt(), "exactly 2 LiveQuiz questions") || !strings.Contains(req.GetPrompt(), "bayes, poker") {
		t.Errorf("prompt = %q, want count + tags embedded", req.GetPrompt())
	}
	if req.GetSystemPrompt() != quizSystemPrompt {
		t.Error("SystemPrompt does not match quizSystemPrompt")
	}
	if req.GetInvocationId() == "" {
		t.Error("InvocationId empty, want a run id")
	}
}

func TestQuizGenClient_GenerateQuizQuestions_ForwardsTraceparent(t *testing.T) {
	gw := &fakeModelGatewayClient{invokeResp: &modelgatewayv1.InvokeResponse{
		Completion:   validQuestionJSON,
		FinishReason: modelgatewayv1.FinishReason_FINISH_REASON_COMPLETE,
	}}
	c := NewQuizGenClient(gw, "gemini-2.5-flash")
	ctx := WithTraceparent(context.Background(), "00-11112222333344445555666677778888-9999aaaabbbbcccc-01")
	if _, err := c.GenerateQuizQuestions(ctx, "g", "t", "topic", nil, 1); err != nil {
		t.Fatalf("GenerateQuizQuestions: %v", err)
	}
	if tp := gw.gotReq.GetTraceparent(); tp != "00-11112222333344445555666677778888-9999aaaabbbbcccc-01" {
		t.Errorf("Traceparent = %q, want caller-supplied value", tp)
	}
}

func TestQuizGenClient_GenerateQuizQuestions_MintsTraceparentWhenAbsent(t *testing.T) {
	gw := &fakeModelGatewayClient{invokeResp: &modelgatewayv1.InvokeResponse{
		Completion:   validQuestionJSON,
		FinishReason: modelgatewayv1.FinishReason_FINISH_REASON_COMPLETE,
	}}
	c := NewQuizGenClient(gw, "gemini-2.5-flash")
	if _, err := c.GenerateQuizQuestions(context.Background(), "g", "t", "topic", nil, 1); err != nil {
		t.Fatalf("GenerateQuizQuestions: %v", err)
	}
	re := regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-01$`)
	if !re.MatchString(gw.gotReq.GetTraceparent()) {
		t.Errorf("Traceparent = %q, want W3C format", gw.gotReq.GetTraceparent())
	}
}

func TestQuizGenClient_GenerateQuizQuestions_NotConfigured(t *testing.T) {
	var nilC *QuizGenClient
	if _, err := nilC.GenerateQuizQuestions(context.Background(), "g", "t", "topic", nil, 1); !errors.Is(err, livequiz.ErrQuizGeneratorNotConfigured) {
		t.Errorf("nil receiver: err = %v, want ErrQuizGeneratorNotConfigured", err)
	}
	c := NewQuizGenClient(nil, "gemini-2.5-flash")
	if _, err := c.GenerateQuizQuestions(context.Background(), "g", "t", "topic", nil, 1); !errors.Is(err, livequiz.ErrQuizGeneratorNotConfigured) {
		t.Errorf("nil gateway: err = %v, want ErrQuizGeneratorNotConfigured", err)
	}
}

func TestQuizGenClient_GenerateQuizQuestions_InvalidArgs(t *testing.T) {
	c := NewQuizGenClient(&fakeModelGatewayClient{}, "gemini-2.5-flash")
	cases := []struct {
		name       string
		instructor string
		tenant     string
		topic      string
		count      int
	}{
		{"missing instructor", "", "t", "topic", 1},
		{"missing tenant", "g", "  ", "topic", 1},
		{"missing topic", "g", "t", "", 1},
		{"non-positive count", "g", "t", "topic", 0},
	}
	for _, tc := range cases {
		_, err := c.GenerateQuizQuestions(context.Background(), tc.instructor, tc.tenant, tc.topic, nil, tc.count)
		if !errors.Is(err, livequiz.ErrInvalidArgument) {
			t.Errorf("%s: err = %v, want ErrInvalidArgument", tc.name, err)
		}
	}
}

func TestQuizGenClient_GenerateQuizQuestions_InvokeError(t *testing.T) {
	gw := &fakeModelGatewayClient{invokeErr: errors.New("gateway down")}
	c := NewQuizGenClient(gw, "gemini-2.5-flash")
	_, err := c.GenerateQuizQuestions(context.Background(), "g", "t", "topic", nil, 1)
	if err == nil || !strings.Contains(err.Error(), "quizgen invoke") {
		t.Errorf("err = %v, want quizgen invoke wrap", err)
	}
}

func TestQuizGenClient_GenerateQuizQuestions_NonCompleteFinishReason(t *testing.T) {
	gw := &fakeModelGatewayClient{invokeResp: &modelgatewayv1.InvokeResponse{
		Completion:   validQuestionJSON,
		FinishReason: modelgatewayv1.FinishReason_FINISH_REASON_MAX_TOKENS,
		FinishDetail: "hit ceiling",
	}}
	c := NewQuizGenClient(gw, "gemini-2.5-flash")
	_, err := c.GenerateQuizQuestions(context.Background(), "g", "t", "topic", nil, 1)
	if err == nil || !strings.Contains(err.Error(), "finish_reason") {
		t.Errorf("err = %v, want finish_reason error", err)
	}
}

func TestQuizGenClient_GenerateQuizQuestions_EmptyCompletionIsError(t *testing.T) {
	gw := &fakeModelGatewayClient{invokeResp: &modelgatewayv1.InvokeResponse{
		Completion:   "",
		FinishReason: modelgatewayv1.FinishReason_FINISH_REASON_COMPLETE,
	}}
	c := NewQuizGenClient(gw, "gemini-2.5-flash")
	_, err := c.GenerateQuizQuestions(context.Background(), "g", "t", "topic", nil, 1)
	if err == nil || !strings.Contains(err.Error(), "quizgen parse") {
		t.Errorf("err = %v, want quizgen parse wrap", err)
	}
}

func TestQuizGenClient_GenerateQuizQuestions_ZeroQuestionsReturned(t *testing.T) {
	gw := &fakeModelGatewayClient{invokeResp: &modelgatewayv1.InvokeResponse{
		Completion:   `[]`,
		FinishReason: modelgatewayv1.FinishReason_FINISH_REASON_COMPLETE,
	}}
	c := NewQuizGenClient(gw, "gemini-2.5-flash")
	_, err := c.GenerateQuizQuestions(context.Background(), "g", "t", "topic", nil, 1)
	if err == nil || !strings.Contains(err.Error(), "0 questions") {
		t.Errorf("err = %v, want 0 questions error", err)
	}
}

func TestParseQuizQuestions(t *testing.T) {
	valid := `{"atom_id":"a1","atom_revision_id":"r1","stem":"Q","options":["A","B"],"correct_option_id":"A","timer_seconds":30,"points":10}`
	invalidShape := `{"atom_id":"","atom_revision_id":"","stem":"","options":[],"correct_option_id":"","timer_seconds":0,"points":0}`

	t.Run("valid array", func(t *testing.T) {
		qs, err := parseQuizQuestions("["+valid+"]", "run-1")
		if err != nil {
			t.Fatalf("parseQuizQuestions: %v", err)
		}
		if len(qs) != 1 {
			t.Fatalf("len = %d, want 1", len(qs))
		}
		if qs[0].AtomID != "a1" || qs[0].TimerSeconds != 30 || qs[0].Points != 10 {
			t.Errorf("question = %+v, want seeded values", qs[0])
		}
	})

	t.Run("fenced array", func(t *testing.T) {
		qs, err := parseQuizQuestions("```json\n["+valid+"]\n```", "run-1")
		if err != nil {
			t.Fatalf("parseQuizQuestions fenced: %v", err)
		}
		if len(qs) != 1 {
			t.Fatalf("len = %d, want 1", len(qs))
		}
	})

	t.Run("empty completion", func(t *testing.T) {
		if _, err := parseQuizQuestions("   ", "run-1"); err == nil {
			t.Fatal("expected error on empty completion")
		}
	})

	t.Run("garbage", func(t *testing.T) {
		if _, err := parseQuizQuestions("not json", "run-1"); err == nil || !strings.Contains(err.Error(), "unmarshal") {
			t.Errorf("err = %v, want unmarshal error", err)
		}
	})

	t.Run("mixed valid and malformed dropped", func(t *testing.T) {
		qs, err := parseQuizQuestions("["+valid+","+invalidShape+"]", "run-1")
		if err != nil {
			t.Fatalf("parseQuizQuestions: %v", err)
		}
		if len(qs) != 1 {
			t.Fatalf("len = %d, want 1 (malformed dropped)", len(qs))
		}
		if qs[0].AtomID != "a1" {
			t.Errorf("AtomID = %q, want a1", qs[0].AtomID)
		}
	})

	t.Run("all invalid", func(t *testing.T) {
		_, err := parseQuizQuestions("["+invalidShape+","+invalidShape+"]", "run-1")
		if err == nil || !strings.Contains(err.Error(), "all 2 questions failed validation") {
			t.Errorf("err = %v, want all-failed error", err)
		}
	})
}

func TestBuildQuizPrompt(t *testing.T) {
	withTags := buildQuizPrompt("Probability", []string{"bayes", "poker"}, 3)
	if !strings.Contains(withTags, "exactly 3 LiveQuiz questions") || !strings.Contains(withTags, "bayes, poker") {
		t.Errorf("prompt = %q, want count + tags", withTags)
	}
	noTags := buildQuizPrompt("Probability", nil, 1)
	if !strings.Contains(noTags, "(tags: none)") {
		t.Errorf("prompt = %q, want tags: none fallback", noTags)
	}
}
