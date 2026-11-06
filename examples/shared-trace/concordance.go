package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/sidsharma96/rewind/internal/httpapi"
	"github.com/sidsharma96/rewind/internal/record"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const searchOutputLimit = 64 << 10

// These bodies describe this trusted-host example's command observation. They
// are opaque to Rewind and are not a Concordance result or citation protocol.
type searchInput struct {
	Binary, Target, Edition, Query string
}
type searchRequest struct {
	Version int      `json:"version"`
	Tool    string   `json:"tool"`
	Args    []string `json:"args"`
}
type searchResult struct {
	Version  int    `json:"version"`
	Outcome  string `json:"outcome"`
	ExitCode *int   `json:"exit_code"`
	Signal   string `json:"signal"`
	Stdout   []byte `json:"stdout"`
	Stderr   []byte `json:"stderr"`
}

func (in searchInput) args() ([]string, error) {
	if err := trustedExecutable(in.Binary); err != nil {
		return nil, err
	}
	for i, s := range []string{in.Target, in.Edition, in.Query} {
		limit := 256
		if i == 2 {
			limit = 4096
		}
		if s == "" || len(s) > limit || !utf8.ValidString(s) || strings.ContainsRune(s, 0) {
			return nil, errors.New("provide bounded target, exact edition and query")
		}
	}
	return []string{"search", "--edition", in.Edition, "--limit", "5", "--timeout", "10s", "--max-output-bytes", "65536", "--", in.Target, in.Query}, nil
}

// Storage access stays in the trusted tool host. No environment values are saved
// in either body, and the child receives no ambient agent/service credentials.
func searchEnvironment() []string {
	var out []string
	for _, name := range []string{"PATH", "HOME", "PGSERVICE", "PGSERVICEFILE", "PGPASSFILE", "CONCORDANCE_COLLECTION", "CONCORDANCE_R2_ENDPOINT", "CONCORDANCE_R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"} {
		if value, ok := os.LookupEnv(name); ok {
			out = append(out, name+"="+value)
		}
	}
	return out
}

type searchStream struct {
	data     []byte
	overflow bool
	complete bool
	cancel   context.CancelFunc
}

func (w *searchStream) Write(p []byte) (int, error) {
	n := min(len(p), searchOutputLimit-len(w.data))
	w.data = append(w.data, p[:n]...)
	if n != len(p) {
		w.overflow = true
		w.cancel()
	}
	return len(p), nil
}

// ReadFrom observes the copy's actual EOF, including when Cmd.Wait prefers an
// ExitError over a pipe-copy error. Hide optional copy methods on the inner call
// so this observer cannot recurse or infer completion from a fast-path return.
func (w *searchStream) ReadFrom(r io.Reader) (int64, error) {
	n, err := io.Copy(struct{ io.Writer }{w}, struct{ io.Reader }{r})
	w.complete = err == nil
	return n, err
}

func runSearchCommand(parent context.Context, in searchInput, args []string, timeout time.Duration) (searchResult, bool, error) {
	return runCommand(parent, in.Binary, args, searchEnvironment(), nil, timeout)
}

// Both concrete task callers share bounded process ownership, not a command API.
func runCommand(parent context.Context, binary string, args, env []string, stdin []byte, timeout time.Duration) (searchResult, bool, error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = env
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	cmd.WaitDelay = 250 * time.Millisecond
	cleanup, err := searchProcess(cmd)
	if err != nil {
		return searchResult{}, false, err
	}
	defer cleanup()
	stdout, stderr := &searchStream{data: []byte{}, cancel: cancel}, &searchStream{data: []byte{}, cancel: cancel}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err := cmd.Start(); err != nil {
		// No tool response exists. Do not invent stderr or an exit code.
		return searchResult{}, false, nil
	}
	waitErr := cmd.Wait() // Joins the process and both stream-copy goroutines.
	result := searchResult{Version: 1, Outcome: "exited", Stdout: stdout.data, Stderr: stderr.data}
	if cmd.ProcessState != nil {
		if code := cmd.ProcessState.ExitCode(); code >= 0 {
			result.ExitCode = &code
		}
	}
	result.Signal = searchSignal(cmd.ProcessState)
	switch {
	case stdout.overflow || stderr.overflow:
		result.Outcome = "output_limit"
	case ctx.Err() != nil:
		result.Outcome = "interrupted"
	case errors.Is(waitErr, exec.ErrWaitDelay) || !stdout.complete || !stderr.complete:
		result.Outcome = "incomplete_streams"
	case waitErr != nil:
		var exit *exec.ExitError
		if !errors.As(waitErr, &exit) {
			result.Outcome = "incomplete_streams"
		}
	}
	return result, true, nil
}

func prepareSearch(ctx context.Context, in searchInput, project, run, traceText, parentText string) ([]plan, error) {
	args, err := in.args()
	if err != nil {
		return nil, err
	}
	traceID, err := trace.TraceIDFromHex(traceText)
	if err != nil || !traceID.IsValid() {
		return nil, errors.New("provide Alice's valid trace ID")
	}
	parentID, err := trace.SpanIDFromHex(parentText)
	if err != nil || !parentID.IsValid() {
		return nil, errors.New("provide Alice's valid span ID")
	}
	parent := trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: parentID, TraceFlags: trace.FlagsSampled, Remote: true}))
	provider := sdktrace.NewTracerProvider()
	defer provider.Shutdown(context.Background())
	started := time.Now()
	_, span := provider.Tracer("rewind-shared-example").Start(parent, "concordance.search")
	result, launched, err := runSearchCommand(ctx, in, args, 15*time.Second)
	if err != nil {
		span.End()
		return nil, err
	}
	status := int32(1)
	if !launched || result.Outcome != "exited" || result.ExitCode == nil || *result.ExitCode != 0 {
		status = 2
		message := "search command failed or incomplete"
		if !launched {
			message = "search command did not start"
		}
		span.SetStatus(codes.Error, message)
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
	sc := span.SpanContext()
	metadata := map[string]any{
		"traceId":           sc.TraceID().String(),
		"spanId":            sc.SpanID().String(),
		"parentSpanId":      parentID.String(),
		"name":              "concordance.search",
		"kind":              3,
		"flags":             uint32(sc.TraceFlags()),
		"startTimeUnixNano": strconv.FormatInt(started.UnixNano(), 10),
		"endTimeUnixNano":   strconv.FormatInt(time.Now().UnixNano(), 10),
		"status":            map[string]any{"code": status},
	}
	packet := map[string]any{"resourceSpans": []any{map[string]any{
		"scopeSpans": []any{map[string]any{
			"scope": map[string]any{"name": "rewind-shared-example"},
			"spans": []any{metadata},
		}},
	}}}
	raw, err := json.Marshal(packet)
	if err != nil {
		return nil, err
	}
	request, err := json.Marshal(searchRequest{Version: 1, Tool: "concordance search", Args: args})
	if err != nil {
		return nil, err
	}
	response := record.BodyMissing
	var body []byte
	if launched {
		response = record.BodyPresent
		if result.Outcome != "exited" {
			response = record.BodyTruncated
		}
		body, err = json.Marshal(result)
		if err != nil {
			return nil, err
		}
	}
	yes, present := true, record.BodyPresent
	requestData, responseData := base64.StdEncoding.EncodeToString(request), base64.StdEncoding.EncodeToString(body)
	return []plan{{
		Execution: httpapi.Execution{Project: httpapi.Opaque(project), Trace: sc.TraceID().String(), Span: sc.SpanID().String()},
		Owner:     httpapi.RegisterInput{Run: httpapi.Opaque(run), Instance: httpapi.Opaque("bob")},
		Span:      httpapi.SpanInput{Ended: &yes, Metadata: raw},
		Capture: httpapi.CaptureInput{
			Request:  &httpapi.Body{State: &present, Data: &requestData},
			Response: &httpapi.Body{State: &response, Data: &responseData},
		},
	}}, nil
}

func trustedExecutable(binary string) error {
	if !filepath.IsAbs(binary) || len(binary) > 4096 {
		return errors.New("provide an absolute trusted executable")
	}
	info, err := os.Lstat(binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 || info.Mode()&(os.ModeSetuid|os.ModeSetgid) != 0 {
		return errors.New("executable must be a regular executable file without privilege bits")
	}
	return nil
}
