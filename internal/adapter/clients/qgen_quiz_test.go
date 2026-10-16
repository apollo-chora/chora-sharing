package clients

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-sharing/internal/domain/livequiz"
	gatewayv1 "github.com/apollo-chora/model-gateway/gen/go/gateway/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// fakeModelGatewayClient implements gatewayv1.ModelGatewayServiceClient for
// QuizGenClient tests. The embedded interface satisfies the unexercised RPCs;
// Invoke is overridden with scripted behaviour.
type fakeModelGatewayClient struct {
	gatewayv1.ModelGatewayServiceClient
	invokeResp *gatewayv1.InvokeResponse
	invokeErr  error
	gotReq     *gatewayv1.InvokeRequest
	gotMD      metadata.MD
}

func (f *fakeModelGatewayClient) Invoke(ctx context.Context, in *gatewayv1.InvokeRequest, _ ...grpc.CallOption) (*gatewayv1.InvokeResponse, error) {
	f.gotReq = in
	f.gotMD, _ = metadata.FromOutgoingContext(ctx)
	if f.invokeErr != nil {
		return nil, f.invokeErr
	}
	return f.invokeResp, nil
}

const validQuestionJSON = `[{"atom_id":"a1","atom_revision_id":"r1","stem":"What is 2+2?","options":["3","4","5","6"],"correct_option_id":"4","timer_seconds":30,"points":10}]`

// quizResponse builds a successful response carrying `text` as the model's
// answer. The gateway returns canonical output items, not a completion string.
func quizResponse(text string) *gatewayv1.InvokeResponse {
	return &gatewayv1.InvokeResponse{
		FinishReason: gatewayv1.FinishReason_FINISH_REASON_STOP,
		Output: []*gatewayv1.OutputItem{{
			Item: &gatewayv1.OutputItem_Content{Content: &gatewayv1.ContentPart{
				Part: &gatewayv1.ContentPart_Text{Text: &gatewayv1.TextPart{Text: text}},
			}},
		}},
	}
}

// userText returns the text of the request's first user message.
func userText(t *testing.T, req *gatewayv1.InvokeRequest) string {
	t.Helper()
	if len(req.GetMessages()) != 1 {
		t.Fatalf("messages len = %d, want 1", len(req.GetMessages()))
	}
	msg := req.GetMessages()[0]
	if msg.GetRole() != gatewayv1.Role_ROLE_USER {
		t.Errorf("role = %v, want ROLE_USER", msg.GetRole())
	}
	if len(msg.GetContent()) != 1 {
		t.Fatalf("content len = %d, want 1", len(msg.GetContent()))
	}
	return msg.GetContent()[0].GetText().GetText()
}

func TestNewQuizGenClient(t *testing.T) {
	gw := &fakeModelGatewayClient{}
	c := NewQuizGenClient(gw)
	if c == nil {
		t.Fatal("NewQuizGenClient returned nil")
	}
}

func TestQuizGenClient_GenerateQuizQuestions_HappyPath(t *testing.T) {
	gw := &fakeModelGatewayClient{invokeResp: quizResponse(validQuestionJSON)}
	c := NewQuizGenClient(gw)

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
	// The gateway resolves the operation from the RPC method, so the caller
	// sends no model ID.
	if req.GetModel() != "" {
		t.Errorf("Model = %q, want empty (the operation's own routes serve it)", req.GetModel())
	}
	// crew_kind has no Invoke field; it rides in metadata_json.
	if !strings.Contains(string(req.GetMetadataJson()), `"crew_kind":"qgen"`) {
		t.Errorf("MetadataJson = %q, want crew_kind qgen", req.GetMetadataJson())
	}
	if prompt := userText(t, req); !strings.Contains(prompt, "exactly 2 LiveQuiz questions") || !strings.Contains(prompt, "bayes, poker") {
		t.Errorf("prompt = %q, want count + tags embedded", prompt)
	}
	if req.GetSystemInstruction() != quizSystemPrompt {
		t.Error("SystemInstruction does not match quizSystemPrompt")
	}
	if req.GetInvocationId() == "" {
		t.Error("InvocationId empty, want a run id")
	}
}

func TestQuizGenClient_GenerateQuizQuestions_ForwardsTraceparent(t *testing.T) {
	gw := &fakeModelGatewayClient{invokeResp: quizResponse(validQuestionJSON)}
	c := NewQuizGenClient(gw)
	ctx := WithTraceparent(context.Background(), "00-11112222333344445555666677778888-9999aaaabbbbcccc-01")
	if _, err := c.GenerateQuizQuestions(ctx, "g", "t", "topic", nil, 1); err != nil {
		t.Fatalf("GenerateQuizQuestions: %v", err)
	}
	// The Invoke RPC has no traceparent field; it travels as gRPC metadata.
	if got := gw.gotMD.Get("traceparent"); len(got) != 1 || got[0] != "00-11112222333344445555666677778888-9999aaaabbbbcccc-01" {
		t.Errorf("traceparent metadata = %v, want caller-supplied value", got)
	}
}

func TestQuizGenClient_GenerateQuizQuestions_MintsTraceparentWhenAbsent(t *testing.T) {
	gw := &fakeModelGatewayClient{invokeResp: quizResponse(validQuestionJSON)}
	c := NewQuizGenClient(gw)
	if _, err := c.GenerateQuizQuestions(context.Background(), "g", "t", "topic", nil, 1); err != nil {
		t.Fatalf("GenerateQuizQuestions: %v", err)
	}
	re := regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-01$`)
	got := gw.gotMD.Get("traceparent")
	if len(got) != 1 || !re.MatchString(got[0]) {
		t.Errorf("traceparent metadata = %v, want one W3C value", got)
	}
}

func TestQuizGenClient_GenerateQuizQuestions_NotConfigured(t *testing.T) {
	var nilC *QuizGenClient
	if _, err := nilC.GenerateQuizQuestions(context.Background(), "g", "t", "topic", nil, 1); !errors.Is(err, livequiz.ErrQuizGeneratorNotConfigured) {
		t.Errorf("nil receiver: err = %v, want ErrQuizGeneratorNotConfigured", err)
	}
	c := NewQuizGenClient(nil)
	if _, err := c.GenerateQuizQuestions(context.Background(), "g", "t", "topic", nil, 1); !errors.Is(err, livequiz.ErrQuizGeneratorNotConfigured) {
		t.Errorf("nil gateway: err = %v, want ErrQuizGeneratorNotConfigured", err)
	}
}

func TestQuizGenClient_GenerateQuizQuestions_InvalidArgs(t *testing.T) {
	c := NewQuizGenClient(&fakeModelGatewayClient{})
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
	c := NewQuizGenClient(gw)
	_, err := c.GenerateQuizQuestions(context.Background(), "g", "t", "topic", nil, 1)
	if err == nil || !strings.Contains(err.Error(), "quizgen invoke") {
		t.Errorf("err = %v, want quizgen invoke wrap", err)
	}
}

func TestQuizGenClient_GenerateQuizQuestions_NonStopFinishReason(t *testing.T) {
	gw := &fakeModelGatewayClient{invokeResp: &gatewayv1.InvokeResponse{
		FinishReason:    gatewayv1.FinishReason_FINISH_REASON_LENGTH,
		RawFinishReason: proto.String("hit ceiling"),
		Output: []*gatewayv1.OutputItem{{
			Item: &gatewayv1.OutputItem_Content{Content: &gatewayv1.ContentPart{
				Part: &gatewayv1.ContentPart_Text{Text: &gatewayv1.TextPart{Text: validQuestionJSON}},
			}},
		}},
	}}
	c := NewQuizGenClient(gw)
	_, err := c.GenerateQuizQuestions(context.Background(), "g", "t", "topic", nil, 1)
	if err == nil || !strings.Contains(err.Error(), "finish_reason") {
		t.Errorf("err = %v, want finish_reason error", err)
	}
}

func TestQuizGenClient_GenerateQuizQuestions_EmptyCompletionIsError(t *testing.T) {
	gw := &fakeModelGatewayClient{invokeResp: quizResponse("")}
	c := NewQuizGenClient(gw)
	_, err := c.GenerateQuizQuestions(context.Background(), "g", "t", "topic", nil, 1)
	if err == nil || !strings.Contains(err.Error(), "quizgen parse") {
		t.Errorf("err = %v, want quizgen parse wrap", err)
	}
}

func TestQuizGenClient_GenerateQuizQuestions_ZeroQuestionsReturned(t *testing.T) {
	gw := &fakeModelGatewayClient{invokeResp: quizResponse(`[]`)}
	c := NewQuizGenClient(gw)
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
