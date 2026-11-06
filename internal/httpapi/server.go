package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/sidsharma96/rewind/internal/access"
	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/objectstore"
	"github.com/sidsharma96/rewind/internal/record"
	"github.com/sidsharma96/rewind/internal/traceid"
)

// Limits include response preparation/writing, after coordinator admission ends.
// MaxLoginActive must leave at least one operation slot for other requests.
// Bounds are operator settings, not measured capacity or peak-heap guarantees.
type Limits struct {
	MaxActive        int
	MaxLoginActive   int
	MaxRequestBytes  int
	MaxResponseBytes int
	Timeout          time.Duration
}

// Server borrows all operations and auth state. It adds no ingestion queue,
// retry, storage policy or permission cache. Its owner bounds accepted connections
// and HTTP headers, disables HTTP/2 and joins shutdown before closing backends.
type Server struct {
	stateMu  sync.Mutex
	closing  bool
	requests int
	drained  chan struct{}

	credentials *access.Credentials
	sessions    *access.Sessions
	registrar   *ingest.Registrar
	spans       *ingest.Spans
	captures    *ingest.Captures
	traces      *ingest.TraceReader
	metadata    record.MetadataCodec
	origin      string
	host        string
	limits      Limits
	active      chan struct{}
	logins      chan struct{}
}

func New(origin string, limits Limits, credentials *access.Credentials, sessions *access.Sessions,
	registrar *ingest.Registrar, spans *ingest.Spans, captures *ingest.Captures, traces *ingest.TraceReader, metadata record.MetadataCodec) (*Server, error) {
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
		limits.MaxActive < 2 || limits.MaxLoginActive <= 0 || limits.MaxLoginActive >= limits.MaxActive ||
		limits.MaxRequestBytes <= 0 || limits.MaxRequestBytes >= int(^uint(0)>>1) || limits.MaxResponseBytes <= 0 || limits.Timeout <= 0 ||
		credentials == nil || sessions == nil || registrar == nil || spans == nil || captures == nil || traces == nil {
		return nil, ErrProtocol
	}
	if _, err := metadata.Decode([]byte("{}")); err != nil {
		return nil, ErrProtocol
	}
	return &Server{drained: make(chan struct{}), credentials: credentials, sessions: sessions, registrar: registrar, spans: spans, captures: captures, traces: traces,
		metadata: metadata, origin: origin, host: u.Host, limits: limits, active: make(chan struct{}, limits.MaxActive), logins: make(chan struct{}, limits.MaxLoginActive)}, nil
}

func securityHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	// no-referrer makes a native form POST send Origin:null, which our origin
	// check must reject. same-origin retains it only for our own origin.
	w.Header().Set("Referrer-Policy", "same-origin")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'")
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.stateMu.Lock()
	if s.closing {
		s.stateMu.Unlock()
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(s.limits.Timeout))
		s.failure(w, r, 503, "unavailable", "refused")
		return
	}
	s.requests++
	s.stateMu.Unlock()
	defer func() {
		s.stateMu.Lock()
		s.requests--
		if s.closing && s.requests == 0 {
			close(s.drained)
		}
		s.stateMu.Unlock()
	}()

	securityHeaders(w)
	if r.TLS == nil || r.Host != s.host || r.URL.EscapedPath() != r.URL.Path || len(r.URL.RawQuery) > 2048 {
		s.failure(w, r, http.StatusBadRequest, "invalid_request", "refused")
		return
	}
	if r.URL.Path == "/login" {
		select {
		case s.logins <- struct{}{}:
			defer func() { <-s.logins }()
		default:
			s.failure(w, r, 503, "busy", "refused")
			return
		}
	}
	select {
	case s.active <- struct{}{}:
		defer func() { <-s.active }()
	default:
		s.failure(w, r, 503, "busy", "refused")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.limits.Timeout)
	defer cancel()
	r = r.WithContext(ctx)
	controller := http.NewResponseController(w)
	deadline := time.Now().Add(s.limits.Timeout)
	_ = controller.SetReadDeadline(deadline)
	_ = controller.SetWriteDeadline(deadline)
	if !strings.HasPrefix(r.URL.Path, "/api/") {
		s.view(w, r)
		return
	}
	s.api(w, r)
}

func (s *Server) authenticate(r *http.Request, write bool) (context.Context, error) {
	if len(r.Header.Values("Authorization")) != 0 {
		return s.credentials.AuthenticateRequest(r)
	}
	if write {
		return nil, access.ErrUnauthenticated
	}
	cookies := r.CookiesNamed(sessionCookie)
	if len(cookies) != 1 {
		return nil, access.ErrUnauthenticated
	}
	return s.sessions.Authenticate(r.Context(), cookies[0].Value)
}

func (s *Server) api(w http.ResponseWriter, r *http.Request) {
	write := r.Method == http.MethodPut || r.Method == http.MethodPost
	if (r.Method != http.MethodGet && !write) || write && r.Header.Get("Origin") != "" {
		s.failure(w, r, 400, "invalid_request", "refused")
		return
	}
	ctx, err := s.authenticate(r, write)
	if err != nil {
		s.fail(w, r, err, write)
		return
	}
	r = r.WithContext(ctx)
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 6 || parts[0] != "api" || parts[1] != "v1" || parts[2] != "projects" {
		s.failure(w, r, 404, "not_found", "refused")
		return
	}
	project, err := parseOpaque(parts[3])
	if err != nil {
		s.fail(w, r, ErrProtocol, write)
		return
	}
	trace, err := traceid.ParseTrace(parts[5])
	if err != nil {
		s.fail(w, r, ErrProtocol, write)
		return
	}
	e := ingest.Execution{Tenant: project, Trace: trace}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		s.fail(w, r, ErrProtocol, write)
		return
	}
	if parts[4] == "traces" && len(parts) == 6 && r.Method == http.MethodGet {
		if len(query) > 1 || len(query["cursor"]) > 1 || len(query) == 1 && query["cursor"] == nil || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
			s.fail(w, r, ErrProtocol, false)
			return
		}
		page, err := s.readTrace(ctx, e, query.Get("cursor"))
		if err != nil {
			s.fail(w, r, err, false)
			return
		}
		s.respond(w, r, page)
		return
	}
	if len(parts) != 8 || parts[4] != "executions" || len(query) != 0 {
		s.failure(w, r, 404, "not_found", "refused")
		return
	}
	e.Span, err = traceid.ParseSpan(parts[6])
	if err != nil {
		s.fail(w, r, ErrProtocol, write)
		return
	}
	operation := parts[7]
	if r.Method == http.MethodGet {
		if r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
			s.fail(w, r, ErrProtocol, false)
			return
		}
		switch operation {
		case "capture":
			v, err := s.readCapture(ctx, e)
			if err != nil {
				s.fail(w, r, err, false)
				return
			}
			s.respond(w, r, v)
		case "response":
			v, err := s.readResult(ctx, e)
			if err != nil {
				s.fail(w, r, err, false)
				return
			}
			s.respond(w, r, v)
		default:
			s.failure(w, r, 404, "not_found", "refused")
		}
		return
	}
	if !(r.Method == http.MethodPost && operation == "registration" || r.Method == http.MethodPut && (operation == "span" || operation == "capture")) {
		s.failure(w, r, 404, "not_found", "refused")
		return
	}
	data, err := s.readJSON(w, r)
	if err != nil {
		if errors.Is(err, ingest.ErrTooLarge) {
			s.failure(w, r, 413, "too_large", "refused")
		} else {
			s.fail(w, r, err, true)
		}
		return
	}
	switch operation {
	case "registration":
		var in RegisterInput
		if strictJSON(data, &in) != nil {
			s.fail(w, r, ErrProtocol, true)
			return
		}
		run, err := parseOpaque(in.Run)
		if err != nil {
			s.fail(w, r, err, true)
			return
		}
		instance, err := parseOpaque(in.Instance)
		if err != nil {
			s.fail(w, r, err, true)
			return
		}
		v, err := s.registrar.Register(ctx, e, ingest.Owner{Run: run, Instance: instance})
		if err != nil {
			s.fail(w, r, err, true)
			return
		}
		s.respond(w, r, Registration{execution(e), Opaque(v.Registration.Owner.Run), Opaque(v.Registration.Owner.Instance), Opaque(v.Registration.RegisteredBy), v.Duplicate})
	case "span":
		var in SpanInput
		if strictJSON(data, &in) != nil || in.Ended == nil || !*in.Ended {
			s.fail(w, r, ErrProtocol, true)
			return
		}
		packet, err := s.metadata.Decode(in.Metadata)
		if err != nil {
			s.fail(w, r, ErrProtocol, true)
			return
		}
		if len(packet.ResourceSpans) != 1 || packet.ResourceSpans[0] == nil || len(packet.ResourceSpans[0].ScopeSpans) != 1 || packet.ResourceSpans[0].ScopeSpans[0] == nil || len(packet.ResourceSpans[0].ScopeSpans[0].Spans) != 1 {
			s.fail(w, r, ErrProtocol, true)
			return
		}
		view, err := record.SpanAt(packet, 0, 0, 0)
		if err != nil {
			s.fail(w, r, ErrProtocol, true)
			return
		}
		ended, err := view.DeclareEnded()
		if err != nil {
			s.fail(w, r, ErrProtocol, true)
			return
		}
		v, err := s.spans.WriteSpan(ctx, e, ended)
		if err != nil {
			s.fail(w, r, err, true)
			return
		}
		projection, err := receipt(v)
		if err != nil {
			s.fail(w, r, errors.Join(ingest.ErrUncertain, err), true)
			return
		}
		s.respond(w, r, projection)
	case "capture":
		var in CaptureInput
		if strictJSON(data, &in) != nil {
			s.fail(w, r, ErrProtocol, true)
			return
		}
		value, err := in.value(e)
		if err != nil {
			s.fail(w, r, err, true)
			return
		}
		v, err := s.captures.WriteCapture(ctx, e, value)
		if err != nil {
			s.fail(w, r, err, true)
			return
		}
		projection, err := receipt(v)
		if err != nil {
			s.fail(w, r, errors.Join(ingest.ErrUncertain, err), true)
			return
		}
		s.respond(w, r, projection)
	}
}
func (s *Server) readJSON(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	typ, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || typ != "application/json" || len(params) > 0 || r.Header.Get("Content-Encoding") != "" {
		return nil, ErrProtocol
	}
	if r.ContentLength > int64(s.limits.MaxRequestBytes) {
		return nil, ingest.ErrTooLarge
	}
	r.Body = http.MaxBytesReader(w, r.Body, int64(s.limits.MaxRequestBytes))
	data, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, ingest.ErrTooLarge
		}
		return nil, ErrProtocol
	}
	return data, nil
}

func (s *Server) readTrace(ctx context.Context, e ingest.Execution, position string) (Page, error) {
	c, err := parseCursor(e, position)
	if err != nil {
		return Page{}, err
	}
	page, err := s.traces.ReadTrace(ctx, e.Tenant, e.Trace, c)
	if err != nil {
		return Page{}, err
	}
	out := Page{Items: make([]Item, 0, len(page.Items)), Next: cursor(e, page.Next)}
	for _, v := range page.Items {
		item := Item{Execution: execution(v.Execution)}
		switch v.State {
		case ingest.TraceNoSpan:
			item.State = "no_span"
		case ingest.TraceExpired:
			item.State = "expired"
		case ingest.TraceAvailable:
			item.State = "available"
			r, err := receipt(v.Receipt)
			if err != nil {
				return Page{}, err
			}
			item.Receipt = &r
			packet, err := v.Span.OTLP()
			if err != nil {
				return Page{}, err
			}
			item.Metadata, err = s.metadata.Encode(packet)
			if err != nil {
				return Page{}, err
			}
		default:
			return Page{}, ingest.ErrIntegrity
		}
		out.Items = append(out.Items, item)
	}
	return out, nil
}
func (s *Server) readCapture(ctx context.Context, e ingest.Execution) (Capture, error) {
	v, err := s.captures.ReadCapture(ctx, e)
	if err != nil {
		return Capture{}, err
	}
	r, err := receipt(v.Receipt)
	if err != nil {
		return Capture{}, err
	}
	return Capture{execution(e), r, body(v.Capture.Request), body(v.Capture.Response)}, nil
}
func (s *Server) readResult(ctx context.Context, e ingest.Execution) (Result, error) {
	v, err := s.captures.Playback(ctx, e)
	if err != nil {
		return Result{}, err
	}
	r, err := receipt(v.Receipt)
	if err != nil {
		return Result{}, err
	}
	return Result{execution(v.Execution), r, v.RequestState, *body(record.Body{State: record.BodyPresent, Data: v.Bytes}).Data}, nil
}

type boundedBuffer struct {
	bytes.Buffer
	max int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if len(p) > b.max-b.Len() {
		return 0, ingest.ErrTooLarge
	}
	return b.Buffer.Write(p)
}
func (s *Server) respond(w http.ResponseWriter, r *http.Request, v any) {
	b := boundedBuffer{max: s.limits.MaxResponseBytes}
	if err := json.NewEncoder(&b).Encode(v); err != nil {
		if r.Method != http.MethodGet {
			s.failure(w, r, 413, "too_large", "unknown")
		} else {
			s.fail(w, r, err, false)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b.Bytes()) // A failed response never undoes confirmed acceptance.
}
func (s *Server) failure(w http.ResponseWriter, r *http.Request, status int, code, outcome string) {
	r.Close = true
	w.Header().Set("Connection", "close")
	if !strings.HasPrefix(r.URL.Path, "/api/") && r.TLS != nil && r.Host == s.host {
		s.renderFailure(w, r, status, code)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(Failure{code, outcome})
}
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error, write bool) {
	status, code, outcome := 503, "unavailable", "failed"
	if write {
		outcome = "unknown"
	}
	switch {
	case errors.Is(err, access.ErrUnauthenticated):
		status, code, outcome = 401, "unauthenticated", "refused"
	case errors.Is(err, ErrProtocol), errors.Is(err, ingest.ErrInvalid):
		status, code, outcome = 400, "invalid_request", "refused"
	case errors.Is(err, ingest.ErrTooLarge):
		status, code = 413, "too_large"
		outcome = "refused"
		if !write {
			outcome = "failed"
		}
	case errors.Is(err, record.ErrEnvelopeTooLarge):
		status, code = 413, "too_large"
	case errors.Is(err, record.ErrEnvelopeUnsupported):
		code = "unsupported_record"
	case errors.Is(err, ingest.ErrDenied):
		status, code, outcome = 403, "denied", "refused"
	case errors.Is(err, ingest.ErrNotFound):
		status, code, outcome = 404, "not_found", "refused"
	case errors.Is(err, ingest.ErrExpired):
		status, code, outcome = 410, "expired", "refused"
	case errors.Is(err, ingest.ErrConflict), errors.Is(err, ingest.ErrOwnershipConflict):
		status, code, outcome = 409, "conflict", "refused"
	case errors.Is(err, ingest.ErrBusy), errors.Is(err, access.ErrSessionLimit):
		status, code, outcome = 503, "busy", "refused"
	case errors.Is(err, ingest.ErrResultUnavailable):
		status, code, outcome = 409, "result_unavailable", "refused"
	case errors.Is(err, ingest.ErrIntegrity), errors.Is(err, ingest.ErrObjectMissing),
		errors.Is(err, record.ErrSavedSpan), errors.Is(err, record.ErrSavedCapture),
		errors.Is(err, record.ErrMetadataInvalid), errors.Is(err, record.ErrMetadataUnsupported),
		errors.Is(err, record.ErrDuplicateAttribute), errors.Is(err, record.ErrEnvelopeInvalid), errors.Is(err, record.ErrEnvelopeKindMismatch):
		code = "integrity_failure"
	case errors.Is(err, ingest.ErrUncertain), errors.Is(err, ingest.ErrRegistrationUncertain), errors.Is(err, objectstore.ErrUncertain):
		code = "outcome_unknown"
	}
	s.failure(w, r, status, code, outcome)
}

// Seal refuses future operations and returns a completion channel for all
// handlers already owned. The network owner must also stop/close its listeners.
func (s *Server) Seal() <-chan struct{} {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if !s.closing {
		s.closing = true
		if s.drained == nil {
			s.drained = make(chan struct{})
		}
		if s.requests == 0 {
			close(s.drained)
		}
	}
	return s.drained
}
