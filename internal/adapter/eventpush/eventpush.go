// Package eventpush is the cloud-neutral HTTP event-push transport for
// chora-sharing.
//
// HISTORY: this replaces the former chora-common `pubsubpush` package, which
// carried Google Cloud Pub/Sub push-subscription semantics (OIDC ID-token
// verification against Google's JWKS, `projects/<project>/subscriptions/<name>`
// resource names, provider project URLs). That package was deleted when the platform
// went cloud-neutral.
//
// The canonical inbound transport for the platform is now the NATS JetStream
// event bus (chora-common/eventbus); the service binds its subscribers to the
// bus at boot. This package remains as an HONEST LOCAL EQUIVALENT for the
// `/api/internal/pubsub/*` HTTP receivers the gateway still forwards: it decodes
// the same push envelope and dispatches into the same subscriber structs, but
// performs NO cloud-specific authentication. When a verifier audience or token
// validator is configured it enforces a Bearer token through the injected
// ValidateTokenFunc; otherwise it is a no-op and the caller is expected to
// enforce auth at the mesh / network layer.
//
// The JSON envelope shape is retained verbatim:
//
//	{
//	  "message": {
//	    "data":        "<base64 payload>",
//	    "messageId":   "<broker-assigned>",
//	    "publishTime": "<RFC3339>",
//	    "attributes":  {"<key>": "<value>", ...}
//	  },
//	  "subscription": "<subscription name>"
//	}
package eventpush

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/eventbus"
)

// ErrBadRequest is returned by Decode when the inbound HTTP request does not
// conform to the push envelope contract. Handler maps it to a 400 response.
var ErrBadRequest = errors.New("eventpush: bad request")

// ErrUnauthorized is returned by Verifier.Verify when the bearer token is
// missing, malformed, or fails validation. Handler maps it to a 401 response.
var ErrUnauthorized = errors.New("eventpush: unauthorized")

// PushMessage is the decoded form of an event-push delivery.
type PushMessage struct {
	// Data is the base64-decoded payload body — typically the Protobuf wire
	// bytes for the subject's registered schema, or a JSON payload.
	Data []byte

	// MessageID is the broker-assigned identifier (NOT the Chora envelope's
	// event_id — that lives in Attributes).
	MessageID string

	// PublishTime is the broker-side timestamp stamped when the message landed.
	PublishTime time.Time

	// Attributes are the publisher-supplied metadata + the projected Chora
	// envelope fields (event_id, tenant_id, gcid, traceparent ...).
	Attributes map[string]string

	// Subscription is the subscription name the delivery arrived on.
	Subscription string
}

// pushEnvelope is the wire shape of an event-push delivery.
type pushEnvelope struct {
	Message *struct {
		Data        string            `json:"data"`
		MessageID   string            `json:"messageId"`
		PublishTime string            `json:"publishTime"`
		Attributes  map[string]string `json:"attributes,omitempty"`
	} `json:"message"`
	Subscription string `json:"subscription"`
}

// Decode reads the push envelope from r and returns a PushMessage. Returns
// ErrBadRequest (wrapped via %w) for any wire-shape violation.
func Decode(r *http.Request) (PushMessage, error) {
	if r.Method != http.MethodPost {
		return PushMessage{}, fmt.Errorf("%w: method %s (want POST)", ErrBadRequest, r.Method)
	}
	if r.Body == nil {
		return PushMessage{}, fmt.Errorf("%w: empty body", ErrBadRequest)
	}
	// Messages can be up to 10MiB. Cap defensively at 12 MiB so callers can't
	// OOM us with crafted requests.
	body, err := io.ReadAll(io.LimitReader(r.Body, 12*1024*1024))
	if err != nil {
		return PushMessage{}, fmt.Errorf("%w: read body: %v", ErrBadRequest, err)
	}
	var env pushEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return PushMessage{}, fmt.Errorf("%w: unmarshal: %v", ErrBadRequest, err)
	}
	if env.Message == nil {
		return PushMessage{}, fmt.Errorf("%w: missing message", ErrBadRequest)
	}
	var data []byte
	if env.Message.Data != "" {
		data, err = base64.StdEncoding.DecodeString(env.Message.Data)
		if err != nil {
			return PushMessage{}, fmt.Errorf("%w: decode base64 data: %v", ErrBadRequest, err)
		}
	}
	pt := time.Time{}
	if env.Message.PublishTime != "" {
		if parsed, perr := time.Parse(time.RFC3339Nano, env.Message.PublishTime); perr == nil {
			pt = parsed
		}
	}
	return PushMessage{
		Data:         data,
		MessageID:    env.Message.MessageID,
		PublishTime:  pt,
		Attributes:   env.Message.Attributes,
		Subscription: env.Subscription,
	}, nil
}

// FromEventbus projects a NATS JetStream eventbus delivery onto the
// PushMessage shape so the same dispatch logic serves both transports. The
// eventbus subject is exposed under the "topic" attribute (the key the
// existing dispatch callbacks read), and the envelope fields are projected
// under their canonical attribute names.
func FromEventbus(msg eventbus.Message) PushMessage {
	attrs := map[string]string{
		"topic":           msg.Subject,
		"event_id":        msg.Envelope.EventID,
		"idempotency_key": msg.Envelope.IdempotencyKey,
		"tenant_id":       msg.Envelope.TenantID,
		"gcid":            msg.Envelope.GCID,
		"traceparent":     msg.Envelope.Traceparent,
		"tracestate":      msg.Envelope.Tracestate,
		"source_project":  msg.Envelope.SourceProject,
		"source_service":  msg.Envelope.SourceService,
	}
	if !msg.Envelope.OccurredAt.IsZero() {
		attrs["occurred_at"] = msg.Envelope.OccurredAt.Format(time.RFC3339Nano)
	}
	if !msg.Envelope.PublishedAt.IsZero() {
		attrs["published_at"] = msg.Envelope.PublishedAt.Format(time.RFC3339Nano)
	}
	if msg.Envelope.SchemaVersion != 0 {
		attrs["schema_version"] = fmt.Sprintf("%d", msg.Envelope.SchemaVersion)
	}
	return PushMessage{
		Data:       msg.Payload,
		Attributes: attrs,
	}
}

// ---------------------------------------------------------------------------
// Token verifier
// ---------------------------------------------------------------------------

// TokenClaims is the minimal claim set Verifier inspects.
type TokenClaims struct {
	// Subject is the token subject claim.
	Subject string
	// Email is the identity email carried by the token.
	Email string
	// EmailVerified mirrors `email_verified`.
	EmailVerified bool
	// Audience is the `aud` claim — equal to the push endpoint URL.
	Audience string
	// Issuer is the `iss` claim.
	Issuer string
}

// ValidateTokenFunc is the seam Verifier uses to validate raw bearer tokens.
// Callers inject a validator; when none is configured the verifier is a no-op
// (dev / in-process tests).
type ValidateTokenFunc func(ctx context.Context, token, audience string) (TokenClaims, error)

// VerifierConfig wires a Verifier.
type VerifierConfig struct {
	// Audience is the expected `aud` claim — the push endpoint URL. When empty
	// AND ValidateToken is nil, verification is DISABLED (dev / tests only).
	Audience string

	// TrustedServiceAccs is the optional allowlist of acceptable `email`
	// claims. When non-empty, only tokens whose email is in this list pass.
	TrustedServiceAccs []string

	// AllowedIssuers is the optional allowlist of acceptable `iss` claims.
	// When empty, any non-empty issuer is accepted.
	AllowedIssuers []string

	// ValidateToken is the token-validation seam. When Audience is empty AND
	// ValidateToken is nil, Verify becomes a no-op.
	ValidateToken ValidateTokenFunc
}

// Verifier verifies bearer tokens for event-push deliveries. It is safe for
// concurrent use after construction.
type Verifier struct {
	cfg     VerifierConfig
	trusted map[string]struct{}
	issuers map[string]struct{}
}

// NewVerifier constructs a Verifier from cfg.
func NewVerifier(cfg VerifierConfig) *Verifier {
	v := &Verifier{
		cfg:     cfg,
		trusted: make(map[string]struct{}, len(cfg.TrustedServiceAccs)),
		issuers: make(map[string]struct{}, len(cfg.AllowedIssuers)),
	}
	for _, s := range cfg.TrustedServiceAccs {
		if s = strings.TrimSpace(s); s != "" {
			v.trusted[strings.ToLower(s)] = struct{}{}
		}
	}
	for _, s := range cfg.AllowedIssuers {
		if s = strings.TrimSpace(s); s != "" {
			v.issuers[s] = struct{}{}
		}
	}
	return v
}

// Enabled reports whether the verifier will perform any check.
func (v *Verifier) Enabled() bool {
	return v.cfg.Audience != "" || v.cfg.ValidateToken != nil
}

// Verify checks the Authorization header for a valid bearer token. Returns nil
// on success, ErrUnauthorized-wrapped error on failure. When the verifier is
// Disabled, Verify returns nil unconditionally.
func (v *Verifier) Verify(ctx context.Context, r *http.Request) error {
	if !v.Enabled() {
		return nil
	}
	authz := r.Header.Get("Authorization")
	if authz == "" {
		return fmt.Errorf("%w: missing Authorization header", ErrUnauthorized)
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(authz, prefix) {
		return fmt.Errorf("%w: Authorization scheme is not Bearer", ErrUnauthorized)
	}
	token := strings.TrimSpace(authz[len(prefix):])
	if token == "" {
		return fmt.Errorf("%w: empty bearer token", ErrUnauthorized)
	}
	if v.cfg.ValidateToken == nil {
		return fmt.Errorf("%w: no token validator configured", ErrUnauthorized)
	}
	claims, err := v.cfg.ValidateToken(ctx, token, v.cfg.Audience)
	if err != nil {
		return fmt.Errorf("%w: token invalid: %v", ErrUnauthorized, err)
	}
	if len(v.issuers) > 0 {
		if _, ok := v.issuers[claims.Issuer]; !ok {
			return fmt.Errorf("%w: issuer %q not allowed", ErrUnauthorized, claims.Issuer)
		}
	}
	if len(v.trusted) > 0 {
		if _, ok := v.trusted[strings.ToLower(claims.Email)]; !ok {
			return fmt.Errorf("%w: service account %q not in allowlist", ErrUnauthorized, claims.Email)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// HTTP handler
// ---------------------------------------------------------------------------

// DispatchFunc is the per-handler callback the push handler invokes after a
// successful decode + verify. It MUST be idempotent — delivery is
// at-least-once.
type DispatchFunc func(ctx context.Context, msg PushMessage) error

// HandlerConfig wires a push handler.
type HandlerConfig struct {
	// Verifier is optional. Use NewVerifier(VerifierConfig{}) for the no-op
	// verifier in dev.
	Verifier *Verifier
	// Dispatch is mandatory. Panics on nil at NewHandler time.
	Dispatch DispatchFunc
	// Logger receives structured one-liners on auth / decode / dispatch
	// failure. Optional — falls back to the stdlib log package.
	Logger interface {
		Printf(format string, args ...interface{})
	}
}

// handler is the http.Handler returned by NewHandler.
type handler struct {
	verifier *Verifier
	dispatch DispatchFunc
	logger   interface {
		Printf(format string, args ...interface{})
	}
}

// NewHandler builds the http.Handler for an event-push subscription. Panics if
// cfg.Dispatch is nil.
func NewHandler(cfg HandlerConfig) http.Handler {
	if cfg.Dispatch == nil {
		panic("eventpush: Dispatch is required")
	}
	if cfg.Verifier == nil {
		cfg.Verifier = NewVerifier(VerifierConfig{})
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	return &handler{
		verifier: cfg.Verifier,
		dispatch: cfg.Dispatch,
		logger:   cfg.Logger,
	}
}

// ServeHTTP runs verify → decode → dispatch in order. On verifier failure
// returns 401; on decode failure 400; on dispatch failure 500 (retry); on
// success 200.
func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := h.verifier.Verify(r.Context(), r); err != nil {
		h.logger.Printf("eventpush: verify failed: %v", err)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	msg, err := Decode(r)
	if err != nil {
		h.logger.Printf("eventpush: decode failed: %v", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err := h.dispatch(r.Context(), msg); err != nil {
		h.logger.Printf("eventpush: dispatch failed (subscription=%s message_id=%s): %v",
			msg.Subscription, msg.MessageID, err)
		http.Error(w, "dispatch failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
