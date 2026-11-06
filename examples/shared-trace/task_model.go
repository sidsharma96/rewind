package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/sidsharma96/rewind/internal/httpapi"
	"github.com/sidsharma96/rewind/internal/record"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type executableInfo struct {
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

// This describes the observed file; the trusted operator must keep the path
// stable through execution. It is not source retention or a hostile-FS defense.
func fingerprintExecutable(path string) (executableInfo, error) {
	if err := trustedExecutable(path); err != nil {
		return executableInfo{}, err
	}
	f, err := os.Open(path)
	if err != nil {
		return executableInfo{}, errors.New("trusted executable unavailable")
	}
	h := sha256.New()
	n, readErr := io.Copy(h, io.LimitReader(f, (512<<20)+1))
	closeErr := f.Close()
	if readErr != nil || closeErr != nil || n > 512<<20 {
		return executableInfo{}, errors.New("trusted executable read failed or exceeds 512 MiB")
	}
	return executableInfo{hex.EncodeToString(h.Sum(nil)), n}, nil
}

func modelEnvironment() []string {
	var out []string
	for _, name := range []string{"PATH", "HOME", "USER", "LOGNAME", "SHELL", "XDG_CONFIG_HOME", "ANTHROPIC_API_KEY"} {
		if value, ok := os.LookupEnv(name); ok {
			out = append(out, name+"="+value)
		}
	}
	return out
}

type modelRequest struct {
	Version    int            `json:"version"`
	Tool       string         `json:"tool"`
	Boundary   string         `json:"boundary"`
	Executable executableInfo `json:"executable"`
	Args       []string       `json:"args"`
	Input      []byte         `json:"stdin"`
}

func prepareModel(ctx context.Context, o taskOptions, info executableInfo, name, owner, system string, input any, traceText, parentText string, links []string) (plan, string, error) {
	stdin, err := json.Marshal(input)
	if err != nil || len(stdin) > 128<<10 {
		return plan{}, "", errors.New("model input exceeds bound")
	}
	args := []string{"-p", "--model", o.Model, "--effort", "low", "--safe-mode", "--restricted", "--strict-mcp-config", "--mcp-config", `{"mcpServers":{}}`, "--no-session-persistence", "--no-chrome", "--disable-slash-commands", "--tools", "", "--permission-mode", "dontAsk", "--system-prompt", system, "--output-format", "json", "--max-budget-usd", "1"}
	request, err := json.Marshal(modelRequest{1, "claude print", "application-visible CLI input/output, not raw provider HTTP", info, args, stdin})
	if err != nil {
		return plan{}, "", err
	}
	parent := ctx
	if traceText != "" {
		tid, e := trace.TraceIDFromHex(traceText)
		if e != nil {
			return plan{}, "", e
		}
		sid, e := trace.SpanIDFromHex(parentText)
		if e != nil {
			return plan{}, "", e
		}
		parent = trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled, Remote: true}))
	}
	provider := sdktrace.NewTracerProvider()
	defer provider.Shutdown(context.Background())
	started := time.Now()
	_, span := provider.Tracer("rewind-task-example").Start(parent, name)
	result, launched, commandErr := runCommand(ctx, o.ModelBinary, args, modelEnvironment(), stdin, 150*time.Second)
	span.End()
	ended := time.Now()
	sc := span.SpanContext()
	var envelope struct {
		Result  *string `json:"result"`
		IsError *bool   `json:"is_error"`
	}
	text := ""
	var workErr error
	if commandErr != nil || !launched || result.Outcome != "exited" || result.ExitCode == nil || *result.ExitCode != 0 || json.Unmarshal(result.Stdout, &envelope) != nil || envelope.IsError == nil || *envelope.IsError || envelope.Result == nil || *envelope.Result == "" {
		workErr = errors.New("model step failed or returned unavailable output; observations saved")
	} else {
		text = *envelope.Result
	}
	status := 1
	if workErr != nil {
		status = 2
	}
	metadata := map[string]any{"traceId": sc.TraceID().String(), "spanId": sc.SpanID().String(), "name": name, "kind": 3, "flags": uint32(sc.TraceFlags()), "startTimeUnixNano": strconv.FormatInt(started.UnixNano(), 10), "endTimeUnixNano": strconv.FormatInt(ended.UnixNano(), 10), "status": map[string]any{"code": status}}
	if parentText != "" {
		metadata["parentSpanId"] = parentText
	}
	if len(links) > 0 {
		var all []any
		for _, id := range links {
			all = append(all, map[string]any{"traceId": sc.TraceID().String(), "spanId": id})
		}
		metadata["links"] = all
	}
	packet, err := json.Marshal(map[string]any{"resourceSpans": []any{map[string]any{"scopeSpans": []any{map[string]any{"scope": map[string]any{"name": "rewind-task-example"}, "spans": []any{metadata}}}}}})
	if err != nil {
		return plan{}, "", err
	}
	state := record.BodyMissing
	var body []byte
	if launched {
		state = record.BodyPresent
		if result.Outcome != "exited" {
			state = record.BodyTruncated
		}
		body, err = json.Marshal(result)
		if err != nil {
			return plan{}, "", err
		}
	}
	yes, present := true, record.BodyPresent
	req, res := base64.StdEncoding.EncodeToString(request), base64.StdEncoding.EncodeToString(body)
	return plan{Execution: httpapi.Execution{Project: httpapi.Opaque(o.Project), Trace: sc.TraceID().String(), Span: sc.SpanID().String()}, Owner: httpapi.RegisterInput{Run: httpapi.Opaque(o.Run), Instance: httpapi.Opaque(owner)}, Span: httpapi.SpanInput{Ended: &yes, Metadata: packet}, Capture: httpapi.CaptureInput{Request: &httpapi.Body{State: &present, Data: &req}, Response: &httpapi.Body{State: &state, Data: &res}}}, text, workErr
}
