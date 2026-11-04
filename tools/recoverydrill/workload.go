package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/producer"
	"github.com/sidsharma96/rewind/internal/record"
	"github.com/sidsharma96/rewind/internal/traceid"
	"go.opentelemetry.io/otel/trace"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

type event struct {
	Type         string           `json:"type"`
	Phase        string           `json:"phase,omitempty"`
	Name         string           `json:"name,omitempty"`
	Kind         string           `json:"kind,omitempty"`
	Trace        string           `json:"trace,omitempty"`
	Span         string           `json:"span,omitempty"`
	Receipt      *ingest.Receipt  `json:"receipt,omitempty"`
	RequestState record.BodyState `json:"request_state,omitempty"`
	Response     []byte           `json:"response,omitempty"`
	Methods      *objectMethods   `json:"object_methods,omitempty"`
}

func emit(out io.Writer, value event) error {
	if json.NewEncoder(out).Encode(value) != nil {
		return errors.New("recoverydrill: event output failed")
	}
	return nil
}

func request(ctx context.Context, s *session, token string) (context.Context, error) {
	verified, err := s.credentials.Authenticate(ctx, token)
	if err != nil {
		return nil, errors.New("recoverydrill: fixture credential verification failed")
	}
	return verified, nil
}

func execution(cfg writeConfig, call callPlan) ingest.Execution {
	tid, _ := traceid.ParseTrace(cfg.Trace)
	sid, _ := traceid.ParseSpan(call.Span)
	return ingest.Execution{Tenant: cfg.Project, Trace: tid, Span: sid}
}

func fixtureRecords(cfg writeConfig, index int) (record.EndedSpan, record.Capture, error) {
	call := cfg.Calls[index]
	e := execution(cfg, call)
	code := tracepb.Status_STATUS_CODE_OK
	if index == 1 {
		code = tracepb.Status_STATUS_CODE_ERROR
	}
	metadata := &tracepb.Span{TraceId: append([]byte(nil), e.Trace[:]...), SpanId: append([]byte(nil), e.Span[:]...),
		Name: call.Name, StartTimeUnixNano: 1, EndTimeUnixNano: 2, Flags: 1, Status: &tracepb.Status{Code: code}}
	if index > 0 {
		parent, _ := traceid.ParseSpan(call.Parent)
		metadata.ParentSpanId = append([]byte(nil), parent[:]...)
	}
	ended, err := (record.Span{Metadata: metadata}).DeclareEnded()
	if err != nil {
		return record.EndedSpan{}, record.Capture{}, errors.New("recoverydrill: fixture declaration failed")
	}
	capture := record.Capture{Trace: e.Trace, Span: e.Span,
		Request:  record.Body{State: record.BodyPresent, Data: []byte("calculate")},
		Response: record.Body{State: record.BodyPresent, Data: []byte("delegated")}}
	if index == 1 {
		capture.Response = record.Body{State: record.BodyMissing}
	} else if index == 2 {
		capture.Request = record.Body{State: record.BodyRedacted}
		capture.Response.Data = []byte(`{"answer":42}`)
	}
	return ended, capture, nil
}

// localTool is a controlled byte-result workload, not a tool gateway or live
// application server. Only write constructs it; read and retry have no fallback.
func localTool() (*httptest.Server, *atomic.Int32) {
	calls := new(atomic.Int32)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1025))
		if r.Method != http.MethodPost || err != nil || string(body) != "calculate" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch calls.Add(1) {
		case 1:
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "controlled failure")
		case 2:
			_, _ = io.WriteString(w, `{"answer":42}`)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	server.Config.ReadHeaderTimeout = time.Second
	server.Config.ReadTimeout = 3 * time.Second
	server.Config.WriteTimeout = 3 * time.Second
	server.Config.IdleTimeout = time.Second
	server.Config.MaxHeaderBytes = 4 << 10
	server.Start()
	return server, calls
}

func checkTool(ctx context.Context, client *http.Client, target string, index int, expected record.Capture) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader([]byte("calculate")))
	if err != nil {
		return errors.New("recoverydrill: fixture tool request failed")
	}
	response, err := client.Do(req)
	if err != nil {
		return errors.New("recoverydrill: fixture tool exchange failed")
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 1025))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || len(body) > 1024 {
		return errors.New("recoverydrill: fixture tool body failed")
	}
	if index == 1 && response.StatusCode != http.StatusServiceUnavailable || index == 2 && (response.StatusCode != http.StatusOK || !bytes.Equal(body, expected.Response.Data)) {
		return errors.New("recoverydrill: fixture tool outcome changed")
	}
	return nil
}

func write(ctx context.Context, cfg writeConfig, s *session, out, diagnostics io.Writer, withhold, retry bool) error {
	if !cfg.valid() || withhold && retry {
		return errConfig
	}
	var server *httptest.Server
	var toolCalls *atomic.Int32
	client := &http.Client{Transport: &http.Transport{MaxConnsPerHost: 1, MaxIdleConns: 1, MaxResponseHeaderBytes: 4 << 10, IdleConnTimeout: time.Second},
		Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	if retry {
		// A missing record must fail rather than be silently recreated by retry.
		s.objects.blockCreate.Store(true)
	} else {
		server, toolCalls = localTool()
		defer server.Close()
		if err := registerCalls(ctx, cfg, s); err != nil {
			return err
		}
	}
	for i, call := range cfg.Calls {
		e := execution(cfg, call)
		ended, capture, err := fixtureRecords(cfg, i)
		if err != nil {
			return err
		}
		instance := "bob"
		if i == 0 {
			instance = "alice"
		}
		if !retry {
			if i > 0 {
				if err := checkTool(ctx, client, server.URL, i, capture); err != nil {
					return err
				}
			}
			if err := rejectWrongWriters(ctx, cfg, s, i, ended, capture); err != nil {
				return err
			}
		}
		principal, err := request(ctx, s, cfg.Tokens[instance])
		if err != nil {
			return err
		}
		spanReceipt, err := s.spans.WriteSpan(principal, e, ended)
		if err != nil || !receiptMatches(spanReceipt, e, ingest.SpanRecord, instance, retry) {
			return errors.New("recoverydrill: span acceptance failed")
		}
		eventType := "accepted"
		if retry {
			eventType = "retried"
		}
		if err := emit(out, event{Type: eventType, Name: call.Name, Kind: "span", Trace: cfg.Trace, Span: call.Span, Receipt: &spanReceipt}); err != nil {
			return err
		}
		captureReceipt, err := s.captures.WriteCapture(principal, e, capture)
		if err != nil || !receiptMatches(captureReceipt, e, ingest.CaptureRecord, instance, retry) {
			return errors.New("recoverydrill: capture acceptance failed")
		}
		if withhold && i == 2 {
			// Deliberate application-ack loss after confirmed publication. This
			// is not loss of the PostgreSQL COMMIT reply. No receipt crosses stdout.
			if err := emit(diagnostics, event{Type: "control", Phase: "final-ack-withheld"}); err != nil {
				return err
			}
			<-ctx.Done()
			return errors.New("recoverydrill: withheld acknowledgment deadline reached")
		}
		if err := emit(out, event{Type: eventType, Name: call.Name, Kind: "capture", Trace: cfg.Trace, Span: call.Span, Receipt: &captureReceipt, RequestState: capture.Request.State}); err != nil {
			return err
		}
		if !retry && i == 0 {
			collector, err := request(ctx, s, cfg.Tokens["collector"])
			if err != nil {
				return err
			}
			before := s.objects.snapshot()
			got, err := s.captures.WriteCapture(collector, e, capture)
			want := captureReceipt
			want.Duplicate = true
			if err != nil || got != want || s.objects.snapshot() != before {
				return errors.New("recoverydrill: collector retry changed original acceptance")
			}
		}
	}
	if !retry && toolCalls.Load() != 2 || retry && s.objects.snapshot().Creates != 0 {
		return errors.New("recoverydrill: workload used unexpected new work")
	}
	counts := s.objects.snapshot()
	return emit(out, event{Type: "summary", Phase: map[bool]string{false: "write", true: "retry"}[retry], Trace: cfg.Trace, Methods: &counts})
}

func receiptMatches(receipt ingest.Receipt, execution ingest.Execution, kind ingest.Kind, instance string, duplicate bool) bool {
	return receipt.Reference.Identity == (ingest.Identity{Tenant: execution.Tenant, Trace: execution.Trace, Span: execution.Span, Kind: kind}) && receipt.Attribution == (ingest.Attribution{Owner: ingest.Owner{Run: "run", Instance: instance}, SubmittedBy: instance}) && receipt.Duplicate == duplicate && !receipt.AcceptedAt.IsZero() && receipt.ExpiresAt.After(receipt.AcceptedAt)
}

func registerCalls(ctx context.Context, cfg writeConfig, s *session) error {
	for i, call := range cfg.Calls {
		instance := "bob"
		if i == 0 {
			instance = "alice"
		}
		host, err := producer.NewHost(s.registrar, cfg.Project, ingest.Owner{Run: "run", Instance: instance})
		if err != nil {
			return errConfig
		}
		e := execution(cfg, call)
		sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID(e.Trace), SpanID: trace.SpanID(e.Span), TraceFlags: trace.FlagsSampled})
		for _, p := range []string{"alice", "bob", "collector"} {
			principal, err := request(ctx, s, cfg.Tokens[p])
			if err != nil {
				return err
			}
			before := s.objects.snapshot()
			if _, err := host.Register(principal, sc); !errors.Is(err, ingest.ErrDenied) || s.objects.snapshot() != before {
				return errors.New("recoverydrill: ordinary writer obtained creator authority")
			}
		}
		principal, err := request(ctx, s, cfg.Tokens["host"])
		if err != nil {
			return err
		}
		receipt, err := host.Register(principal, sc)
		if err != nil || receipt.Duplicate || receipt.Registration.Execution != e || receipt.Registration.Owner != (ingest.Owner{Run: "run", Instance: instance}) || receipt.Registration.RegisteredBy != "host" {
			return errors.New("recoverydrill: trusted SDK-ID registration failed")
		}
	}
	return nil
}

func rejectWrongWriters(ctx context.Context, cfg writeConfig, s *session, i int, ended record.EndedSpan, capture record.Capture) error {
	wrong := "alice"
	if i == 0 {
		wrong = "bob"
	}
	principals := []string{wrong, "host"}
	if i > 0 {
		principals = append(principals, "collector")
	}
	e := execution(cfg, cfg.Calls[i])
	for _, p := range principals {
		principal, err := request(ctx, s, cfg.Tokens[p])
		if err != nil {
			return err
		}
		before := s.objects.snapshot()
		if _, err := s.spans.WriteSpan(principal, e, ended); !errors.Is(err, ingest.ErrDenied) {
			return errors.New("recoverydrill: wrong owner span was not denied")
		}
		if _, err := s.captures.WriteCapture(principal, e, capture); !errors.Is(err, ingest.ErrDenied) || s.objects.snapshot() != before {
			return errors.New("recoverydrill: denied writer reached objects")
		}
	}
	return nil
}

func read(ctx context.Context, cfg readConfig, s *session, out io.Writer) error {
	if !cfg.valid() {
		return errConfig
	}
	tid, _ := traceid.ParseTrace(cfg.Trace)
	denied, err := request(ctx, s, cfg.DeniedToken)
	if err != nil {
		return err
	}
	before := s.objects.snapshot()
	if _, err := s.reader.ReadTrace(denied, cfg.Project, tid, ingest.TraceCursor{}); !errors.Is(err, ingest.ErrDenied) || s.objects.snapshot() != before {
		return errors.New("recoverydrill: denied discovery reached objects")
	}
	zero := objectMethods{}
	if err := emit(out, event{Type: "denied", Phase: "trace", Trace: cfg.Trace, Methods: &zero}); err != nil {
		return err
	}
	principal, err := request(ctx, s, cfg.ReaderToken)
	if err != nil {
		return err
	}
	var cursor ingest.TraceCursor
	items := make(map[string]ingest.TraceItem)
	for pages := 0; ; pages++ {
		if pages >= 4 {
			return errors.New("recoverydrill: fixture discovery exceeded page bound")
		}
		page, err := s.reader.ReadTrace(principal, cfg.Project, tid, cursor)
		if err != nil {
			return errors.New("recoverydrill: fresh trace discovery failed")
		}
		for _, item := range page.Items {
			if item.State != ingest.TraceAvailable || item.Span.Metadata == nil || len(items) >= 3 {
				return errors.New("recoverydrill: fixture span unavailable or excessive")
			}
			name := item.Span.Metadata.Name
			if _, duplicate := items[name]; duplicate {
				return errors.New("recoverydrill: repeated fixture operation")
			}
			items[name] = item
		}
		if page.Next.IsZero() {
			break
		}
		cursor = page.Next
	}
	if len(items) != 3 {
		return errors.New("recoverydrill: fixture discovery missing accepted spans")
	}
	alice, found := items[callNames[0]]
	if !found {
		return errors.New("recoverydrill: fixture handoff missing")
	}
	for i, name := range callNames {
		item, found := items[name]
		instance, code := "bob", tracepb.Status_STATUS_CODE_OK
		if i == 0 {
			instance = "alice"
		} else if i == 1 {
			code = tracepb.Status_STATUS_CODE_ERROR
		}
		if !found || !receiptMatches(item.Receipt, item.Execution, ingest.SpanRecord, instance, false) || item.Span.Metadata.GetStatus().GetCode() != code || i > 0 && !bytes.Equal(item.Span.Metadata.ParentSpanId, alice.Execution.Span[:]) {
			return errors.New("recoverydrill: fixture span evidence changed")
		}
		if err := emit(out, event{Type: "recovered", Name: name, Kind: "span", Trace: cfg.Trace, Span: item.Execution.Span.String(), Receipt: &item.Receipt}); err != nil {
			return err
		}
		var receipt ingest.Receipt
		var requestState record.BodyState
		var response []byte
		if i == 1 {
			got, err := s.captures.ReadCapture(principal, item.Execution)
			if err != nil || got.Capture.Request.State != record.BodyPresent || string(got.Capture.Request.Data) != "calculate" || got.Capture.Response.State != record.BodyMissing {
				return errors.New("recoverydrill: failed capture evidence changed")
			}
			if got, err := s.captures.Playback(principal, item.Execution); !errors.Is(err, ingest.ErrResultUnavailable) || got.Bytes != nil || got.Receipt != (ingest.Receipt{}) {
				return errors.New("recoverydrill: unavailable result was fabricated")
			}
			receipt, requestState = got.Receipt, got.Capture.Request.State
		} else {
			got, err := s.captures.Playback(principal, item.Execution)
			want, state := "delegated", record.BodyPresent
			if i == 2 {
				want, state = `{"answer":42}`, record.BodyRedacted
			}
			if err != nil || got.Execution != item.Execution || string(got.Bytes) != want || got.RequestState != state {
				return errors.New("recoverydrill: saved response evidence changed")
			}
			receipt, requestState, response = got.Receipt, got.RequestState, got.Bytes
		}
		if !receiptMatches(receipt, item.Execution, ingest.CaptureRecord, instance, false) {
			return errors.New("recoverydrill: capture acceptance changed")
		}
		if err := emit(out, event{Type: "recovered", Name: name, Kind: "capture", Trace: cfg.Trace, Span: item.Execution.Span.String(), Receipt: &receipt, RequestState: requestState, Response: response}); err != nil {
			return err
		}
	}
	before = s.objects.snapshot()
	if _, err := s.captures.Playback(denied, alice.Execution); !errors.Is(err, ingest.ErrDenied) || s.objects.snapshot() != before {
		return errors.New("recoverydrill: denied known-result read reached objects")
	}
	if err := emit(out, event{Type: "denied", Phase: "known-result", Trace: cfg.Trace, Methods: &zero}); err != nil {
		return err
	}
	counts := s.objects.snapshot()
	if counts.Creates != 0 {
		return errors.New("recoverydrill: fresh reader attempted object creation")
	}
	return emit(out, event{Type: "summary", Phase: "read", Trace: cfg.Trace, Methods: &counts})
}
