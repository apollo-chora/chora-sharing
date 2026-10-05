// Package clients — LiveQuiz QGen draft client (chora-model-gateway).
//
// QuizGenClient implements livequiz.QuizGenerator by invoking the QGen crew
// via chora-model-gateway ModelGatewayService.Invoke. Used by the
// GenerateQuizFromTopic bridge (§7.9) to draft LiveQuiz questions for HITL.
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
	modelgatewayv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/model_gateway/v1"
	"github.com/google/uuid"
)

// Compile-time check: *QuizGenClient satisfies livequiz.QuizGenerator.
var _ livequiz.QuizGenerator = (*QuizGenClient)(nil)

// quizAgentID is the agent_id the gateway resolves for the LiveQuiz draft
// crew (same QGen crew, different prompt/template than battle questions).
const quizAgentID = "qgen_quiz_draft"

// QuizGenClient is the production implementation of livequiz.QuizGenerator.
type QuizGenClient struct {
	gw      modelgatewayv1.ModelGatewayServiceClient
	modelID string // logical_model_id (e.g. "gemini-2.5-flash")
}

// NewQuizGenClient wraps a bound ModelGatewayServiceClient. Pass nil when the
// env is unconfigured; the resulting client fails every call loud so handlers
// emit 501 rather than silently returning a fake draft.
func NewQuizGenClient(gw modelgatewayv1.ModelGatewayServiceClient, logicalModelID string) *QuizGenClient {
	return &QuizGenClient{gw: gw, modelID: logicalModelID}
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

	resp, err := c.gw.Invoke(ctx, &modelgatewayv1.InvokeRequest{
		InvocationId:    runID,
		TenantId:        tenantID,
		Gcid:            instructorGCID,
		AgentId:         quizAgentID,
		CrewKind:        "qgen",
		LogicalModelId:  c.modelID,
		Prompt:          prompt,
		SystemPrompt:    quizSystemPrompt,
		Traceparent:     resolveTraceparent(ctx),
	})
	if err != nil {
		return nil, fmt.Errorf("quizgen invoke: %w", err)
	}
	if rc := resp.GetFinishReason(); rc != modelgatewayv1.FinishReason_FINISH_REASON_COMPLETE {
		return nil, fmt.Errorf("quizgen invoke: finish_reason=%s detail=%q",
			rc, resp.GetFinishDetail())
	}

	questions, err := parseQuizQuestions(resp.GetCompletion(), runID)
	if err != nil {
		return nil, fmt.Errorf("quizgen parse: %w", err)
	}
	if len(questions) == 0 {
		return nil, fmt.Errorf("quizgen: model returned 0 questions for topic %q", topic)
	}
	return questions, nil
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
	for i, e := range raw {
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
		_ = i
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
