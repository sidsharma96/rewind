package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/sidsharma96/rewind/internal/httpapi"
	"github.com/sidsharma96/rewind/internal/record"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const sessionLimit = 4 << 20
const sessionLineLimit = 128 << 10
const sessionPlanLimit = 16 << 20

// This is an application capture, not a saved-object or receiver schema. Payload
// preserves the selected original CLI line, including its newline. CallID selects
// one block when a line contains several calls/results. It grants no authority.
type sessionCapture struct {
	Version int    `json:"version"`
	Source  string `json:"source"`
	CallID  string `json:"call_id"`
	Payload []byte `json:"payload"`
}

type sessionCall struct {
	id, name, agent, parent string
	request, response       []byte
	failed                  bool
}

type sessionEvent struct {
	Type         string `json:"type"`
	Subtype      string `json:"subtype"`
	Session      string `json:"session_id"`
	Version      string `json:"claude_code_version"`
	Agent        string `json:"agent_id"`
	Parent       string `json:"parent_tool_use_id"`
	TaskID       string `json:"task_id"`
	ToolUseID    string `json:"tool_use_id"`
	Backgrounded *bool  `json:"is_backgrounded"`
	SpawnDepth   int    `json:"spawn_depth"`
	Message      struct {
		Content json.RawMessage `json:"content"`
	} `json:"message"`
	Result  *string `json:"result"`
	IsError *bool   `json:"is_error"`
}

type sessionBlock struct {
	Type    string          `json:"type"`
	ID      string          `json:"id"`
	Name    string          `json:"name"`
	Input   json.RawMessage `json:"input"`
	CallID  string          `json:"tool_use_id"`
	Content json.RawMessage `json:"content"`
	IsError bool            `json:"is_error"`
}

// DecodeDocument rejects duplicate JSON keys and malformed JSON before this
// projection reads known fields. Unknown CLI metadata is retained, not interpreted.
func sessionJSON(raw []byte, dst any) error {
	var fields map[string]json.RawMessage
	if httpapi.DecodeDocument(raw, &fields) != nil || fields == nil || json.Unmarshal(raw, dst) != nil {
		return errors.New("invalid CLI JSON object")
	}
	return nil
}

func sessionID(s string) bool { return s != "" && len(s) <= 256 && !strings.ContainsAny(s, "\x00\r\n") }

// Parse the complete bounded stream before generating identities or contacting
// Rewind. No EOF, timestamp or successful child can stand in for a root result.
func parseSession(raw []byte) ([]sessionCall, []byte, bool, error) {
	bad := func() ([]sessionCall, []byte, bool, error) {
		return nil, nil, false, errors.New("unsupported or inconsistent Claude session stream")
	}
	if len(raw) == 0 || len(raw) > sessionLimit || raw[len(raw)-1] != '\n' {
		return bad()
	}
	lines := bytes.SplitAfter(raw, []byte{'\n'})
	if len(lines) > 513 { // Each independent byte, line and operation cap is enforced.
		return nil, nil, false, errors.New("session exceeds 512 CLI lines")
	}
	var session, child, handoff, startedChild string
	var calls []sessionCall
	index := map[string]int{}
	var final []byte
	failed, initialized := false, false
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		if len(line) > sessionLineLimit {
			return nil, nil, false, errors.New("CLI line exceeds 128 KiB")
		}
		var e sessionEvent
		if sessionJSON(line, &e) != nil || !sessionID(e.Session) || final != nil {
			return bad()
		}
		if session == "" {
			session = e.Session
		}
		if e.Session != session {
			return bad()
		}
		if e.Type == "system" && e.Subtype == "init" {
			if initialized || e.Version != "2.1.318" {
				return bad()
			}
			initialized = true
			continue
		}
		if !initialized {
			return bad()
		}
		if e.Agent != "" {
			if !sessionID(e.Agent) || e.Agent != startedChild || e.Parent != handoff || handoff == "" || (child != "" && e.Agent != child) {
				return bad()
			}
			child = e.Agent
		} else if e.Parent != "" {
			return bad()
		}
		switch e.Type {
		case "system", "rate_limit_event":
			// Only observed non-operation notifications are ignored. New message
			// kinds need an explicit compatibility check, not an end-span guess.
			if e.Type == "system" {
				switch e.Subtype {
				case "task_started":
					// Requested mode is not evidence of the mode that ran. Check
					// the runtime's explicit start report as well as the Agent input.
					if startedChild != "" || !sessionID(e.TaskID) || e.ToolUseID != handoff || handoff == "" || e.Backgrounded == nil || *e.Backgrounded || e.SpawnDepth != 1 {
						return bad()
					}
					startedChild = e.TaskID
				case "task_progress", "task_updated", "task_notification":
				default:
					return bad()
				}
			}
		case "assistant", "user":
			var blocks []json.RawMessage
			if json.Unmarshal(e.Message.Content, &blocks) != nil {
				return bad()
			}
			for _, b := range blocks {
				var block sessionBlock
				if sessionJSON(b, &block) != nil {
					return bad()
				}
				switch block.Type {
				case "text", "thinking", "redacted_thinking":
					continue
				case "tool_use":
					if e.Type != "assistant" || !sessionID(block.ID) || !sessionID(block.Name) || len(block.Input) == 0 || string(block.Input) == "null" || len(calls) >= 32 {
						return bad()
					}
					var input map[string]json.RawMessage
					if sessionJSON(block.Input, &input) != nil {
						return bad()
					}
					if _, exists := index[block.ID]; exists {
						return bad()
					}
					if block.Name == "Agent" {
						var options struct {
							Background *bool `json:"run_in_background"`
						}
						if e.Agent != "" || handoff != "" || json.Unmarshal(block.Input, &options) != nil || options.Background == nil || *options.Background {
							return bad()
						}
						handoff = block.ID
					}
					index[block.ID] = len(calls)
					calls = append(calls, sessionCall{id: block.ID, name: block.Name, agent: e.Agent, parent: e.Parent, request: line})
				case "tool_result":
					var resultFields map[string]json.RawMessage
					if sessionJSON(b, &resultFields) != nil || string(resultFields["is_error"]) == "null" {
						return bad()
					}
					i, ok := index[block.CallID]
					if e.Type != "user" || !ok || calls[i].response != nil || e.Agent != calls[i].agent || e.Parent != calls[i].parent || len(block.Content) == 0 || string(block.Content) == "null" {
						return bad()
					}
					calls[i].response, calls[i].failed = line, block.IsError
				default:
					return bad()
				}
			}
		case "result":
			if e.Agent != "" || e.Result == nil || e.IsError == nil || *e.Result == "" {
				return bad()
			}
			final, failed = line, *e.IsError
		default:
			return bad()
		}
	}
	if final == nil || child == "" || handoff == "" {
		return bad()
	}
	for _, call := range calls {
		if call.response == nil {
			return bad()
		}
	}
	return calls, final, failed, nil
}

func prepareSession(ctx context.Context, raw, prompt []byte, project, run string) ([]plan, error) {
	calls, final, failed, err := parseSession(raw)
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider()
	defer provider.Shutdown(context.Background())
	rootContext, root := provider.Tracer("rewind-claude-session").Start(ctx, "claude.session")
	root.End()
	contexts := map[string]trace.SpanContext{}
	for _, call := range calls {
		_, span := provider.Tracer("rewind-claude-session").Start(rootContext, call.name)
		contexts[call.id] = span.SpanContext()
		span.End()
	}
	makePlan := func(sc trace.SpanContext, parent trace.SpanID, name, owner, id string, request, response []byte, failed bool) (plan, error) {
		status := 1
		if failed {
			status = 2
		}
		metadata := map[string]any{
			"traceId": sc.TraceID().String(), "spanId": sc.SpanID().String(),
			"name": name, "status": map[string]any{"code": status},
		}
		if parent.IsValid() {
			metadata["parentSpanId"] = parent.String()
		}
		packet, err := json.Marshal(map[string]any{
			"resourceSpans": []any{map[string]any{
				"scopeSpans": []any{map[string]any{"spans": []any{metadata}}},
			}},
		})
		if err != nil {
			return plan{}, err
		}
		source := "claude-cli-event"
		if id == "" {
			source = "operator-prompt"
		}
		req, err := json.Marshal(sessionCapture{1, source, id, request})
		if err != nil {
			return plan{}, err
		}
		res, err := json.Marshal(sessionCapture{1, "claude-cli-event", id, response})
		if err != nil {
			return plan{}, err
		}
		a, b := base64.StdEncoding.EncodeToString(req), base64.StdEncoding.EncodeToString(res)
		yes, present := true, record.BodyPresent
		return plan{
			Execution: httpapi.Execution{
				Project: httpapi.Opaque(project), Trace: sc.TraceID().String(), Span: sc.SpanID().String(),
			},
			Owner: httpapi.RegisterInput{Run: httpapi.Opaque(run), Instance: httpapi.Opaque(owner)},
			Span:  httpapi.SpanInput{Ended: &yes, Metadata: packet},
			Capture: httpapi.CaptureInput{
				Request:  &httpapi.Body{State: &present, Data: &a},
				Response: &httpapi.Body{State: &present, Data: &b},
			},
		}, nil
	}
	p, err := makePlan(root.SpanContext(), trace.SpanID{}, "claude.session", "alice", "", prompt, final, failed)
	if err != nil {
		return nil, err
	}
	plans := []plan{p}
	for _, call := range calls {
		parent, owner := root.SpanContext().SpanID(), "alice"
		if call.agent != "" {
			parent, owner = contexts[call.parent].SpanID(), "bob"
		}
		p, err := makePlan(contexts[call.id], parent, "claude.tool."+call.name, owner, call.id, call.request, call.response, call.failed)
		if err != nil {
			return nil, err
		}
		plans = append(plans, p)
	}
	return plans, nil
}

func sessionCommand(ctx context.Context, o taskOptions, streamFile, planFile string, retry bool) error {
	if !sessionID(o.Project) || !sessionID(o.Run) || planFile == "" {
		return errors.New("provide session project, run and plan file")
	}
	var plans []plan
	if retry {
		if streamFile != "" || o.InputFile != "" {
			return errors.New("session-retry takes saved plan and delivery options only")
		}
		raw, err := private(planFile, sessionPlanLimit)
		if err != nil {
			return err
		}
		if httpapi.DecodeDocument(raw, &plans) != nil {
			return errors.New("invalid session plan")
		}
	} else {
		if _, err := os.Lstat(planFile); !errors.Is(err, os.ErrNotExist) {
			return errors.New("session plan must be new; use session-retry")
		}
		raw, err := private(streamFile, sessionLimit)
		if err != nil {
			return err
		}
		prompt, err := private(o.InputFile, 4096)
		if err != nil || len(bytes.TrimSpace(prompt)) == 0 {
			return errors.New("provide private original prompt file")
		}
		plans, err = prepareSession(ctx, raw, prompt, o.Project, o.Run)
		if err != nil {
			return err
		}
	}
	if err := validateSessionPlans(plans, o.Project, o.Run); err != nil {
		return err
	}
	if !retry {
		if err := save(planFile, plans); err != nil {
			return err
		}
	}
	client, cleanup, err := exampleClients(o.Endpoint, o.CAFile)
	if err != nil {
		return err
	}
	defer cleanup()
	host, err := client(o.HostToken)
	if err != nil {
		return err
	}
	alice, err := client(o.AliceToken)
	if err != nil {
		return err
	}
	bob, err := client(o.BobToken)
	if err != nil {
		return err
	}
	for _, p := range plans {
		writer := alice
		if p.Owner.Instance == httpapi.Opaque("bob") {
			writer = bob
		}
		if _, err = host.Register(ctx, p.Execution, p.Owner); err != nil {
			return fmt.Errorf("session registration: %w; use session-retry", err)
		}
		span, err := writer.SaveSpan(ctx, p.Execution, p.Span)
		if err != nil {
			return fmt.Errorf("session span: %w; use session-retry", err)
		}
		capture, err := writer.SaveCapture(ctx, p.Execution, p.Capture)
		if err != nil {
			return fmt.Errorf("session capture: %w; use session-retry", err)
		}
		locator, err := writer.TraceViewURL(p.Execution.Project, p.Execution.Trace)
		if err != nil {
			return err
		}
		u, err := url.Parse(locator)
		if err != nil {
			return err
		}
		q := u.Query()
		q.Set("format", "claude-stream-v1")
		u.RawQuery = q.Encode()
		if err := json.NewEncoder(os.Stdout).Encode(struct {
			TraceURL  string            `json:"trace_url"`
			Execution httpapi.Execution `json:"execution"`
			Span      httpapi.Receipt   `json:"span_receipt"`
			Capture   httpapi.Receipt   `json:"capture_receipt"`
		}{u.String(), p.Execution, span, capture}); err != nil {
			return errors.New("session receipt output failed; use session-retry")
		}
	}
	return nil
}

func validateSessionPlans(plans []plan, project, run string) error {
	if len(plans) < 3 || len(plans) > 33 {
		return errors.New("invalid session plan count")
	}
	seen := map[string]bool{}
	selected := map[string]bool{}
	hasWorker := false
	for i, p := range plans {
		if p.Execution.Project != httpapi.Opaque(project) || p.Execution.Trace != plans[0].Execution.Trace || p.Owner.Run != httpapi.Opaque(run) || (p.Owner.Instance != httpapi.Opaque("alice") && p.Owner.Instance != httpapi.Opaque("bob")) || p.Span.Ended == nil || !*p.Span.Ended || p.Capture.Request == nil || p.Capture.Response == nil || seen[p.Execution.Span] {
			return errors.New("invalid saved session scope or observation")
		}
		tid, te := trace.TraceIDFromHex(p.Execution.Trace)
		sid, se := trace.SpanIDFromHex(p.Execution.Span)
		if te != nil || se != nil || !tid.IsValid() || !sid.IsValid() || i == 0 && p.Owner.Instance != httpapi.Opaque("alice") {
			return errors.New("invalid saved session identity")
		}
		var request, response sessionCapture
		for j, body := range []*httpapi.Body{p.Capture.Request, p.Capture.Response} {
			if body.State == nil || *body.State != record.BodyPresent || body.Data == nil || len(*body.Data) > 256<<10 {
				return errors.New("invalid saved session body")
			}
			raw, err := base64.StdEncoding.Strict().DecodeString(*body.Data)
			v := &request
			if j == 1 {
				v = &response
			}
			if err != nil || httpapi.DecodeDocument(raw, v) != nil || v.Version != 1 || len(v.Payload) == 0 || len(v.Payload) > sessionLineLimit {
				return errors.New("invalid saved session observation")
			}
		}
		if response.Source != "claude-cli-event" || request.CallID != response.CallID || i == 0 && (request.Source != "operator-prompt" || request.CallID != "" || len(request.Payload) > 4096) || i > 0 && (request.Source != "claude-cli-event" || !sessionID(request.CallID)) || selected[request.CallID] {
			return errors.New("invalid saved session selection")
		}
		selected[request.CallID] = true
		hasWorker = hasWorker || p.Owner.Instance == httpapi.Opaque("bob")
		seen[p.Execution.Span] = true
	}
	if !hasWorker {
		return errors.New("session plan has no worker observations")
	}
	return nil
}
