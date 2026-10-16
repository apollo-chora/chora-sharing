// Package clients — LiveQuiz QGen draft client (model-gateway).
//
// QuizGenClient implements livequiz.QuizGenerator by invoking the model
// gateway's Invoke RPC. Used by the GenerateQuizFromTopic bridge (§7.9) to
// draft LiveQuiz questions for HITL.
//
// Per §9 + FR-020: the output is UNTRUSTED — the caller (the http adapter
// §7.9) re-validates every atom_id against the instructor's entitled set
// before surfacing the draft, and ALWAYS sets review_required="true" (HITL
// before ARM, FR-032). This client does NOT re-validate; it only parses the
// model's JSON into []livequiz.GeneratedQuestion and stamps a qgen_run_id.
//
// Per §1.1: fail-loud on misconfiguration (nil client →
// ErrQuizGeneratorNotConfigured). Malformed model output is an error.
package clients

import (
	"context"
	"fmt"
	"strings"

	"github.com/apollo-chora/chora-sharing/internal/domain/livequiz"
	gatewayv1 "github.com/apollo-chora/model-gateway/gen/go/gateway/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// Compile-time check: *QuizGenClient satisfies livequiz.QuizGenerator.
var _ livequiz.QuizGenerator = (*QuizGenClient)(nil)

// quizAgentID is the agent_id the gateway resolves for the LiveQuiz draft
// crew (same QGen crew, different prompt/template than battle questions).
const quizAgentID = "qgen_quiz_draft"

// quizCrewKind rides in the invocation's metadata. The gateway's Invoke RPC
// has no crew_kind field of its own — the operation is named by the RPC
// method, not by the caller — so this goes into metadata_json, which the
// gateway maps onto the invocation's metadata.
const quizCrewKind = "qgen"

// QuizGenClient is the production implementation of livequiz.QuizGenerator.
type QuizGenClient struct {
	gw gatewayv1.ModelGatewayServiceClient
}

// NewQuizGenClient wraps a bound ModelGatewayServiceClient. Pass nil when the
// env is unconfigured; the resulting client fails every call loud so handlers
// emit 501 rather than silently returning a fake draft.
func NewQuizGenClient(gw gatewayv1.ModelGatewayServiceClient) *QuizGenClient {
	return &QuizGenClient{gw: gw}
}

// GenerateQuizQuestions asks the QGen crew to draft `questionCount` questions
// for the LiveQuiz topic + topic_tags. instructorGCID + tenantID scope the
// gateway's RLS + per-tenant budget gate. Returns UNTRUSTED questions — the
// caller re-validates atom_ids against the entitled set (FR-020) before HITL.
func (c *QuizGenClient) GenerateQuizQuestions(ctx context.Context, instructorGCID, tenantID, topic string, topicTags []string, questionCount int) ([]livequiz.GeneratedQuestion, error) {
	if c == nil || c.gw == nil {
		return nil, livequiz.ErrQuizGeneratorNotConfigured
	}
	if strings.TrimSpace(instructorGCID) == "" || strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: instructor_gcid + tenant_id required", livequiz.ErrInvalidArgument)
	}
	if strings.TrimSpace(topic) == "" {
		return nil, fmt.Errorf("%w: topic required", livequiz.ErrInvalidArgument)
	}
	if questionCount <= 0 {
		return nil, fmt.Errorf("%w: question_count must be > 0", livequiz.ErrInvalidArgument)
	}

	runID := uuid.Must(uuid.NewV7()).String()
	prompt := buildQuizPrompt(topic, topicTags, questionCount)

	resp, err := c.gw.Invoke(withTraceparent(ctx), &gatewayv1.InvokeRequest{
		InvocationId: runID,
		TenantId:     tenantID,
		Gcid:         proto.String(instructorGCID),
		AgentId:      quizAgentID,
		// No model ID: the gateway resolves the operation from the RPC method
		// and serves it from the text operation's own configured routes.
		SystemInstruction: quizSystemPrompt,
		Messages: []*gatewayv1.Message{{
			Role:    gatewayv1.Role_ROLE_USER,
			Content: []*gatewayv1.ContentPart{{
				Part: &gatewayv1.ContentPart_Text{Text: &gatewayv1.TextPart{Text: prompt}},
			}},
		}},
		MetadataJson: []byte(fmt.Sprintf(`{"crew_kind":%q}`, quizCrewKind)),
	})
	if err != nil {
		return nil, fmt.Errorf("quizgen invoke: %w", err)
	}
	if rc := resp.GetFinishReason(); rc != gatewayv1.FinishReason_FINISH_REASON_STOP {
		return nil, fmt.Errorf("quizgen invoke: finish_reason=%s detail=%q",
			rc, resp.GetRawFinishReason())
	}

	questions, err := parseQuizQuestions(responseText(resp), runID)
	if err != nil {
		return nil, fmt.Errorf("quizgen parse: %w", err)
	}
	if len(questions) == 0 {
		return nil, fmt.Errorf("quizgen: model returned 0 questions for topic %q", topic)
	}
	return questions, nil
}

// withTraceparent moves the traceparent onto the gRPC metadata. The gateway's
// Invoke RPC has no traceparent field; distributed tracing context travels as
// the `traceparent` metadata key, which is where the OTel interceptor reads it.
func withTraceparent(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "traceparent", resolveTraceparent(ctx))
}

// responseText flattens the response's output items into the text the model
// produced. The gateway returns canonical output items rather than a single
// completion string.
func responseText(resp *gatewayv1.InvokeResponse) string {
	var b strings.Builder
	for _, item := range resp.GetOutput() {
		if part := item.GetContent(); part != nil {
			if text := part.GetText(); text != nil {
				b.WriteString(text.GetText())
			}
		}
	}
	return b.String()
}

// quizEnvelope mirrors the JSON the QGen crew emits for a LiveQuiz draft.
type quizEnvelope struct {
	AtomID          string   `json:"atom_id"`
	RevisionID      string   `json:"atom_revision_id"`
	Stem            string   `json:"stem"`
	Options         []string `json:"options"`
	CorrectOptionID string   `json:"correct_option_id"`
	TimerSeconds    int      `json:"timer_seconds"`
	Points          int      `json:"points"`
}

// parseQuizQuestions decodes the model's completion JSON into GeneratedQuestion
// values + validates each via livequiz.GeneratedQuestion.Validate. A question
// failing Validate is dropped with a best-effort log — the draft is advisory
// (HITL follows), so a single malformed question does not fail the whole draft
// unless ALL are invalid (returning an empty slice is an error above).
func parseQuizQuestions(completion, runID string) ([]livequiz.GeneratedQuestion, error) {
	completion = strings.TrimSpace(completion)
	if completion == "" {
		return nil, fmt.Errorf("empty completion (run %s)", runID)
	}
	var raw []quizEnvelope
	if err := parseJSONEnvelope(completion, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal quiz questions: %w", err)
	}
	questions := make([]livequiz.GeneratedQuestion, 0, len(raw))
	for _, e := range raw {
		q := livequiz.GeneratedQuestion{
			AtomID:          e.AtomID,
			RevisionID:      e.RevisionID,
			Stem:            e.Stem,
			Options:         append([]string(nil), e.Options...),
			CorrectOptionID: e.CorrectOptionID,
			TimerSeconds:    e.TimerSeconds,
			Points:          e.Points,
		}
		if err := q.Validate(); err != nil {
			// Drop malformed; the draft is UNTRUSTED + HITL-gated anyway.
			// Caller re-validates atom_ids against the entitled set (FR-020).
			continue
		}
		questions = append(questions, q)
	}
	if len(questions) == 0 && len(raw) > 0 {
		return nil, fmt.Errorf("all %d questions failed validation (run %s)", len(raw), runID)
	}
	return questions, nil
}

func buildQuizPrompt(topic string, tags []string, count int) string {
	tagsStr := "none"
	if len(tags) > 0 {
		tagsStr = strings.Join(tags, ", ")
	}
	return fmt.Sprintf(`Draft exactly %d LiveQuiz questions for topic %q (tags: %s).
Each question references a published LearningAtom (atom_id + atom_revision_id),
has an MCQ stem, 2-4 options, the correct_option_id, timer_seconds, and points.
Emit a JSON array only.`, count, topic, tagsStr)
}

const quizSystemPrompt = `You are the QGen LiveQuiz draft crew for chora-sharing.
Return ONLY a JSON array of question objects. Each object has fields:
atom_id, atom_revision_id, stem, options (array of strings),
correct_option_id, timer_seconds (int), points (int).
No prose, no markdown fences. The output is UNTRUSTED and will be re-validated
against the instructor's entitled atom set before surfacing to HITL.`
