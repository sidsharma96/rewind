package httpapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/record"
	"github.com/sidsharma96/rewind/internal/traceid"
)

// Client borrows a trusted TLS transport; it overrides redirect behavior and
// requires a whole-call timeout. It has no retry, disk buffer or storage access.
// Callers own inputs/results and must bound any results they retain.
type Client struct {
	base, token             string
	http                    *http.Client
	maxRequest, maxResponse int
}

func NewClient(endpoint, token string, client *http.Client, maxRequest, maxResponse int) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
		len(token) != 64 || client == nil || client.Timeout <= 0 || maxRequest <= 0 || maxResponse <= 0 || maxResponse == int(^uint(0)>>1) {
		return nil, ErrProtocol
	}
	b, err := hex.DecodeString(token)
	if err != nil || hex.EncodeToString(b) != token {
		return nil, ErrProtocol
	}
	copy := *client
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{endpoint, token, &copy, maxRequest, maxResponse}, nil
}
func (c *Client) call(ctx context.Context, method, path string, input, output any) error {
	write := method != http.MethodGet
	var data []byte
	if input != nil {
		var err error
		data, err = json.Marshal(input)
		if err != nil || len(data) > c.maxRequest {
			return ErrProtocol
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(data))
	if err != nil {
		return ErrProtocol
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept-Encoding", "identity")
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return unconfirmed(write)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Encoding") != "" || resp.Header.Get("Content-Type") != "application/json" {
		return unconfirmed(write)
	}
	data, err = io.ReadAll(io.LimitReader(resp.Body, int64(c.maxResponse)+1))
	if err != nil || len(data) > c.maxResponse {
		return unconfirmed(write)
	}
	if resp.StatusCode != http.StatusOK {
		var fail Failure
		if strictJSON(data, &fail) != nil || failureStatus(fail.Code) != resp.StatusCode || write && fail.Outcome == "failed" || fail.Outcome != "unknown" && fail.Outcome != "refused" && fail.Outcome != "failed" {
			return unconfirmed(write)
		}
		if write && (fail.Code == "outcome_unknown" || fail.Code == "integrity_failure" || fail.Code == "unsupported_record") && fail.Outcome != "unknown" {
			return unconfirmed(true)
		}
		return &fail
	}
	if strictJSON(data, output) != nil {
		return unconfirmed(write)
	}
	return nil
}
func unconfirmed(write bool) error {
	outcome := "failed"
	if write {
		outcome = "unknown"
	}
	return &Failure{Code: "reply_unconfirmed", Outcome: outcome}
}
func failureStatus(code string) int {
	switch code {
	case "invalid_request":
		return 400
	case "unauthenticated":
		return 401
	case "denied":
		return 403
	case "not_found":
		return 404
	case "conflict", "result_unavailable":
		return 409
	case "expired":
		return 410
	case "too_large":
		return 413
	case "busy", "unavailable", "integrity_failure", "unsupported_record", "outcome_unknown":
		return 503
	}
	return 0
}

func canonicalExecution(e Execution) (Execution, error) {
	project, err := parseOpaque(e.Project)
	if err != nil {
		return Execution{}, err
	}
	trace, err := traceid.ParseTrace(e.Trace)
	if err != nil {
		return Execution{}, ErrProtocol
	}
	span, err := traceid.ParseSpan(e.Span)
	if err != nil {
		return Execution{}, ErrProtocol
	}
	return Execution{Opaque(project), trace.String(), span.String()}, nil
}
func path(e Execution) (string, error) {
	if _, err := parseOpaque(e.Project); err != nil {
		return "", err
	}
	if _, err := traceid.ParseTrace(e.Trace); err != nil {
		return "", ErrProtocol
	}
	if _, err := traceid.ParseSpan(e.Span); err != nil {
		return "", ErrProtocol
	}
	return "/api/v1/projects/" + e.Project + "/executions/" + e.Trace + "/" + e.Span, nil
}
func (r Receipt) valid(e Execution, kind string) bool {
	if r.Execution != e || r.Kind != kind || len(r.Digest) != 64 || r.Size < 0 {
		return false
	}
	if _, err := hex.DecodeString(r.Digest); err != nil {
		return false
	}
	for _, s := range []string{r.Run, r.Instance, r.Submitter} {
		if _, err := parseOpaque(s); err != nil {
			return false
		}
	}
	start, err := time.Parse(time.RFC3339Nano, r.Accepted)
	if err != nil {
		return false
	}
	end, err := time.Parse(time.RFC3339Nano, r.Expires)
	return err == nil && !start.IsZero() && end.After(start)
}
func (c *Client) Register(ctx context.Context, e Execution, in RegisterInput) (Registration, error) {
	e, err := canonicalExecution(e)
	if err != nil {
		return Registration{}, err
	}
	p, err := path(e)
	if err != nil {
		return Registration{}, err
	}
	run, err := parseOpaque(in.Run)
	if err != nil {
		return Registration{}, err
	}
	instance, err := parseOpaque(in.Instance)
	if err != nil {
		return Registration{}, err
	}
	in = RegisterInput{Run: Opaque(run), Instance: Opaque(instance)}
	var out Registration
	if err = c.call(ctx, http.MethodPost, p+"/registration", in, &out); err != nil {
		return Registration{}, err
	}
	if out.Execution != e || out.Run != in.Run || out.Instance != in.Instance {
		return Registration{}, unconfirmed(true)
	}
	if _, err := parseOpaque(out.Registrar); err != nil {
		return Registration{}, unconfirmed(true)
	}
	return out, nil
}
func (c *Client) SaveSpan(ctx context.Context, e Execution, in SpanInput) (Receipt, error) {
	e, err := canonicalExecution(e)
	if err != nil {
		return Receipt{}, err
	}
	p, err := path(e)
	if err != nil {
		return Receipt{}, err
	}
	var out Receipt
	if err = c.call(ctx, http.MethodPut, p+"/span", in, &out); err != nil {
		return Receipt{}, err
	}
	if !out.valid(e, "span") {
		return Receipt{}, unconfirmed(true)
	}
	return out, nil
}
func (c *Client) SaveCapture(ctx context.Context, e Execution, in CaptureInput) (Receipt, error) {
	e, err := canonicalExecution(e)
	if err != nil {
		return Receipt{}, err
	}
	p, err := path(e)
	if err != nil {
		return Receipt{}, err
	}
	var out Receipt
	if err = c.call(ctx, http.MethodPut, p+"/capture", in, &out); err != nil {
		return Receipt{}, err
	}
	if !out.valid(e, "capture") {
		return Receipt{}, unconfirmed(true)
	}
	return out, nil
}
func (c *Client) Trace(ctx context.Context, project, trace, position string) (Page, error) {
	projectBytes, err := parseOpaque(project)
	if err != nil {
		return Page{}, err
	}
	traceKey, err := traceid.ParseTrace(trace)
	if err != nil {
		return Page{}, ErrProtocol
	}
	project, trace = Opaque(projectBytes), traceKey.String()
	scope := ingest.Execution{Tenant: projectBytes, Trace: traceKey}
	start, err := parseCursor(scope, position)
	if err != nil {
		return Page{}, err
	}
	if _, err := parseOpaque(project); err != nil {
		return Page{}, err
	}
	if _, err := traceid.ParseTrace(trace); err != nil {
		return Page{}, ErrProtocol
	}
	if len(position) > 565 {
		return Page{}, ErrProtocol
	}
	p := "/api/v1/projects/" + project + "/traces/" + trace
	if position != "" {
		p += "?cursor=" + url.QueryEscape(position)
	}
	var out Page
	if err := c.call(ctx, http.MethodGet, p, nil, &out); err != nil {
		return Page{}, err
	}
	if out.Items == nil {
		return Page{}, ErrProtocol
	}
	previous := start.After().String()
	for _, item := range out.Items {
		if item.Project != project || item.Trace != trace || item.Span <= previous {
			return Page{}, ErrProtocol
		}
		if _, err := traceid.ParseSpan(item.Span); err != nil {
			return Page{}, ErrProtocol
		}
		previous = item.Span
		switch item.State {
		case "available":
			if item.Receipt == nil || !item.Receipt.valid(item.Execution, "span") || len(item.Metadata) == 0 || !json.Valid(item.Metadata) {
				return Page{}, ErrProtocol
			}
		case "no_span", "expired":
			if item.Receipt != nil || len(item.Metadata) != 0 {
				return Page{}, ErrProtocol
			}
		default:
			return Page{}, ErrProtocol
		}
	}
	if out.Next != "" {
		next, err := parseCursor(scope, out.Next)
		if err != nil || len(out.Items) == 0 || next.After().String() != out.Items[len(out.Items)-1].Span {
			return Page{}, ErrProtocol
		}
	}
	return out, nil
}
func (c *Client) Capture(ctx context.Context, e Execution) (Capture, error) {
	e, err := canonicalExecution(e)
	if err != nil {
		return Capture{}, err
	}
	p, err := path(e)
	if err != nil {
		return Capture{}, err
	}
	var out Capture
	if err = c.call(ctx, http.MethodGet, p+"/capture", nil, &out); err != nil {
		return Capture{}, err
	}
	if out.Execution != e || !out.Receipt.valid(e, "capture") {
		return Capture{}, ErrProtocol
	}
	if _, err := out.Request.value(); err != nil {
		return Capture{}, err
	}
	if _, err := out.Response.value(); err != nil {
		return Capture{}, err
	}
	return out, nil
}
func (c *Client) Playback(ctx context.Context, e Execution) (Result, error) {
	e, err := canonicalExecution(e)
	if err != nil {
		return Result{}, err
	}
	p, err := path(e)
	if err != nil {
		return Result{}, err
	}
	var out Result
	if err = c.call(ctx, http.MethodGet, p+"/response", nil, &out); err != nil {
		return Result{}, err
	}
	if out.Execution != e || !out.Receipt.valid(e, "capture") {
		return Result{}, ErrProtocol
	}
	switch out.RequestState {
	case record.BodyMissing, record.BodyRedacted, record.BodyTruncated, record.BodyPresent:
	default:
		return Result{}, ErrProtocol
	}
	if _, err := out.Bytes(); err != nil {
		return Result{}, err
	}
	return out, nil
}
func (r Result) Bytes() ([]byte, error) {
	b, err := base64.StdEncoding.Strict().DecodeString(r.Data)
	if err != nil || base64.StdEncoding.EncodeToString(b) != r.Data {
		return nil, ErrProtocol
	}
	return b, nil
}
